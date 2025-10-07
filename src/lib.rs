//! NodeMap: a high-performance node hash map using per-bucket seqlock, ported from Go's pb.NodeMapOf.
//! Simplified, Rust-idiomatic API focused on speed.

use std::cell::UnsafeCell;
use std::hash::{BuildHasher, Hash};
use std::marker::PhantomData;
use std::mem::MaybeUninit;
use std::sync::atomic::{
    AtomicBool, AtomicI32, AtomicPtr, AtomicU32, AtomicU64, AtomicUsize, Ordering,
};
use std::sync::LazyLock;
use std::thread;

use ahash::RandomState;

// ================================================================================================
// CONSTANTS AND GLOBAL VARIABLES
// ================================================================================================

/// Number of entries per bucket (op byte = highest, keep 7 data bytes like Go)
const ENTRIES_PER_BUCKET: usize = 6;

/// Mask for identifying occupied slots in meta bytes (6 data bytes, highest byte reserved)
const META_MASK: u64 = 0x0000_8080_8080_8080;

/// Value indicating an empty slot
const EMPTY_SLOT: u8 = 0;

/// Mask for marking a bit in meta byte as slot marker
const SLOT_MASK: u8 = 0x80;

/// Load factor for determining when to resize
const LOAD_FACTOR: f64 = 0.75;

/// Minimum table length
const MIN_TABLE_LEN: usize = 32;

/// Mask for the operation lock (highest meta byte's 0x80 bit acts as root lock)
const OP_LOCK_MASK: u64 = 0x8000_0000_0000_0000;

// Parallel resize constants
/// Minimum buckets per CPU thread
const MIN_BUCKETS_PER_CPU: usize = 16;

/// Over-partition factor for resize to reduce tail latency
const RESIZE_OVER_PARTITION: usize = 8;

/// pure CPU hints before any yield
const SPIN_BEFORE_YIELD: i32 = 128;

// Global cached CPU count to avoid repeated OS queries
static CPU_COUNT: LazyLock<usize> = LazyLock::new(|| {
    thread::available_parallelism()
        .map(|p| p.get())
        .unwrap_or(1)
});

#[inline(always)]
fn cpu_count() -> usize {
    *CPU_COUNT
}

// ================================================================================================
// INTERNAL DATA STRUCTURES
// ================================================================================================

/// Entry in a bucket containing hash, key, and value
struct Entry<K, V> {
    hash: u64, // Highest bit indicates if entry is initialized, remaining 63 bits are actual hash
    key: MaybeUninit<K>,
    val: MaybeUninit<V>,
}

impl<K, V> Default for Entry<K, V> {
    fn default() -> Self {
        Self {
            hash: 0, // 0 indicates empty slot (no HASH_INIT_FLAG)
            key: MaybeUninit::uninit(),
            val: MaybeUninit::uninit(),
        }
    }
}

/// Bucket containing entries and metadata for concurrent access
#[repr(align(64))]
struct Bucket<K, V> {
    meta: AtomicU64,               // Moved first - accessed most frequently in hot paths
    next: AtomicPtr<Bucket<K, V>>, // Least frequently accessed - only for overflow
    entries: [AtomicPtr<Entry<K, V>>; ENTRIES_PER_BUCKET], // Atomic pointer array like Go version
}

// SAFETY: We provide per-bucket seqlock (seq) and atomic meta updates, and never move buckets after creation.
// Concurrent access is coordinated via lock()/unlock() and Acquire/Release fences.
unsafe impl<K: Send, V: Send> Send for Bucket<K, V> {}
unsafe impl<K: Sync, V: Sync> Sync for Bucket<K, V> {}

/// Table containing buckets and size information
struct Table<K, V> {
    buckets: UnsafeCell<*mut Bucket<K, V>>,
    mask: UnsafeCell<usize>,
    size: UnsafeCell<*mut AtomicUsize>,
    size_mask: UnsafeCell<u32>,
    seq: AtomicU32, // seqlock for table (even=stable, odd=write)
}

impl<K, V> Clone for Table<K, V> {
    fn clone(&self) -> Self {
        unsafe {
            Self {
                buckets: UnsafeCell::new(*self.buckets.get()),
                mask: UnsafeCell::new(*self.mask.get()),
                size: UnsafeCell::new(*self.size.get()),
                size_mask: UnsafeCell::new(*self.size_mask.get()),
                seq: AtomicU32::new(self.seq.load(Ordering::Relaxed)),
            }
        }
    }
}

/// Parallel resize state structure (corresponds to Go's nodeResizeState)
struct ResizeState<K, V> {
    started: AtomicBool,                        // Whether resize has started
    chunks: AtomicI32,                          // Set by finalizeResize
    new_table: UnsafeCell<Option<Table<K, V>>>, // Protected by started flag
    process: AtomicI32,                         // Atomic counter for work distribution
    completed: AtomicI32,                       // Atomic counter for completed chunks
}

impl<K, V> ResizeState<K, V> {
    pub fn new() -> Self {
        Self {
            started: AtomicBool::new(false),
            chunks: AtomicI32::new(0),
            new_table: UnsafeCell::new(None),
            process: AtomicI32::new(0),
            completed: AtomicI32::new(0),
        }
    }

    #[inline(always)]
    pub fn is_new_table_ready(&self) -> bool {
        // Use chunks as the publication barrier for new_table.
        // finalize_resize publishes new_table before storing chunks with Release,
        // so an Acquire load of chunks ensures visibility of new_table.
        let chunks = self.chunks.load(Ordering::Acquire);
        if chunks == 0 {
            return false;
        }
        unsafe {
            if let Some(ref new_table) = *self.new_table.get() {
                new_table.seq.load(Ordering::Acquire) == 2
            } else {
                false
            }
        }
    }
}

impl<K, V> Drop for ResizeState<K, V> {
    fn drop(&mut self) {
        // Clean up new table from resize state if it exists
        // UnsafeCell<Option<Table>> doesn't need explicit cleanup
        // as Table will be dropped automatically when Option is dropped
    }
}

// ================================================================================================
// MAIN FLATMAP STRUCTURE
// ================================================================================================

/// High-performance node hash map using per-bucket seqlock
pub struct NodeMap<K, V, S: BuildHasher = RandomState> {
    table: Table<K, V>,
    old_tables: UnsafeCell<Vec<Box<Table<K, V>>>>,
    resize_state: ResizeState<K, V>,
    hasher: S,
}

// SAFETY: NodeMap coordinates concurrent access via per-bucket seqlocks, a root bucket op lock, and a resize_lock guarding table swaps.
// The UnsafeCell around the Arc<Table> is only mutated under resize_lock and never moved; readers clone the Arc which is safe.
unsafe impl<K: Send, V: Send, S: Send + BuildHasher> Send for NodeMap<K, V, S> {}
unsafe impl<K: Sync, V: Sync, S: Sync + BuildHasher> Sync for NodeMap<K, V, S> {}

// ================================================================================================
// FLATMAP CONSTRUCTORS
// ================================================================================================

impl<K: Eq + Hash + Clone + 'static, V: Clone> NodeMap<K, V, RandomState> {
    /// Create a new NodeMap with default settings.
    pub fn new() -> Self {
        Self::with_capacity(0)
    }

    /// Create a new NodeMap with the specified capacity.
    ///
    /// This pre-allocates internal buckets based on the provided size hint. The map may grow
    /// beyond this capacity as elements are inserted.
    pub fn with_capacity(size_hint: usize) -> Self {
        Self::with_capacity_and_hasher(size_hint, RandomState::new())
    }
}

impl<K: Eq + Hash + Clone + 'static, V: Clone, S: BuildHasher> NodeMap<K, V, S> {
    /// Create a new NodeMap using the provided hasher.
    ///
    /// This constructor allows customizing the hashing strategy up front. Changing the hasher
    /// on an existing map is not supported because it would invalidate existing bucket placement.
    pub fn with_hasher(hasher: S) -> Self {
        Self::with_capacity_and_hasher(0, hasher)
    }

    /// Create a new NodeMap with the specified capacity and hasher.
    ///
    /// Pre-allocates internal buckets based on the size hint and uses the provided hasher for key
    /// hashing. This is the recommended way to set a custom hashing strategy.
    pub fn with_capacity_and_hasher(size_hint: usize, hasher: S) -> Self {
        let len = calc_table_len(size_hint);
        let cpus = cpu_count();
        let size_len = calc_size_len(len, cpus);

        // Allocate buckets as raw pointer
        let buckets_layout = std::alloc::Layout::array::<Bucket<K, V>>(len).unwrap();
        let buckets_ptr = unsafe { std::alloc::alloc_zeroed(buckets_layout) as *mut Bucket<K, V> };
        if buckets_ptr.is_null() {
            std::alloc::handle_alloc_error(buckets_layout);
        }

        // Allocate size as raw pointer
        let size_layout = std::alloc::Layout::array::<AtomicUsize>(size_len).unwrap();
        let size_ptr = unsafe { std::alloc::alloc_zeroed(size_layout) as *mut AtomicUsize };
        if size_ptr.is_null() {
            std::alloc::handle_alloc_error(size_layout);
        }

        let table = Table {
            buckets: UnsafeCell::new(buckets_ptr),
            mask: UnsafeCell::new(len - 1),
            size: UnsafeCell::new(size_ptr),
            size_mask: UnsafeCell::new(size_len as u32 - 1),
            seq: AtomicU32::new(2), // Set to 2 to indicate initialization is complete
        };
        Self {
            table,
            old_tables: UnsafeCell::new(Vec::new()),
            resize_state: ResizeState::new(),
            // shrink_on: false,
            hasher,
        }
    }

    // ============================================================================================
    // PUBLIC API METHODS
    // ============================================================================================

    /// Check whether the given key is present.
    ///
    /// This is concurrency-safe. This implementation uses `get` internally and may clone the value
    /// as part of the lookup.
    pub fn contains_key(&self, key: &K) -> bool {
        self.get(key).is_some()
    }

    /// Get the value associated with the given key as a cloned `V`.
    ///
    /// Fast path: read under an even seqlock snapshot. On contention, falls back to bucket-level
    /// lock to guarantee consistency. Requires `V: Clone` to avoid returning an aliased internal
    /// reference.
    /// Returns a cloned value corresponding to the key, if present.
    ///
    /// The internal read path is concurrency-safe and may clone the value before returning it.
    pub fn get(&self, key: &K) -> Option<V>
    where
        V: Clone,
    {
        let table = self.table.seq_load();
        let (hash64, hash_u8) = self.hash_pair(key);
        let idx = self.h1_mask(hash64, table.mask());
        let root = table.get_bucket(idx);
        let h2w = broadcast(hash_u8);

        // Traverse bucket chain (like Go version)
        let mut bucket_opt = Some(root);

        while let Some(b) = bucket_opt {
            let meta = b.meta.load(Ordering::Relaxed);
            let mut marked = mark_zero_bytes(meta ^ h2w);

            // Process each marked entry (like Go version)
            while marked != 0 {
                let j = first_marked_byte_index(marked);
                if let Some(e) = b.get_entry(j) {
                    // Copy only hash first to filter out mismatches without cloning key/val
                    if !e.equal_hash(hash64) {
                        marked &= marked - 1;
                        continue;
                    }

                    // Delay cloning until we know hash matches - first check key equality with unsafe access
                    let entry_key_ref = unsafe { e.key.assume_init_ref() };
                    if entry_key_ref != key {
                        marked &= marked - 1;
                        continue;
                    }
                    // Clone only the value since we already confirmed key match
                    let entry_val = unsafe { e.val.assume_init_ref().clone() };

                    return Some(entry_val);
                }
                // advance to next marked byte regardless of pointer state
                marked &= marked - 1;
            }

            // Successfully processed this bucket, move to next
            bucket_opt = b.get_next_bucket();
        }
        None
    }

    /// Inserts a key-value pair into the map.
    /// If the key already exists, the old value is replaced with the new value.
    ///
    /// # Arguments
    ///
    /// * `key` - The key to insert.
    /// * `val` - The value to insert.
    pub fn insert(&self, key: K, val: V) -> Option<V>
    where
        V: Clone,
    {
        self.alter(key, |_| Some(val))
    }

    /// Removes the key-value pair associated with the given key from the map.
    ///
    /// # Arguments
    ///
    /// * `key` - The key to remove.
    ///
    /// # Returns
    ///
    /// * `Option<V>` - The value associated with the key, if it existed.
    pub fn remove(&self, key: K) -> Option<V>
    where
        V: Clone,
    {
        self.alter(key, |_| None)
    }

    /// Returns the value associated with the given key, or inserts a new value if the key does not exist.
    ///
    /// # Arguments
    ///
    /// * `key` - The key to look up.
    /// * `f` - A closure that takes no arguments and returns the value to insert if the key does not exist.
    ///
    /// # Returns
    ///
    /// * `(V, bool)` - A tuple containing the value associated with the key, and a boolean indicating whether the value was inserted.
    pub fn get_or_insert_with<F: FnOnce() -> V>(&self, key: K, f: F) -> (V, bool)
    where
        V: Clone,
    {
        // Use alter method to implement get_or_insert_with
        let mut f_option = Some(f);
        let mut was_existing = false;
        let mut new_value = None;

        let old_value = self.alter(key, |existing| {
            if let Some(existing_val) = existing {
                // Key exists, return the existing value
                was_existing = true;
                Some(existing_val) // Keep the existing value
            } else {
                // Key doesn't exist, insert new value
                let new_val = f_option.take().unwrap()();
                new_value = Some(new_val.clone());
                Some(new_val) // Insert the new value
            }
        });

        // Return the value and whether it was existing
        if was_existing {
            // If key existed, old_value contains the existing value
            (old_value.unwrap(), true)
        } else {
            // If key didn't exist, return the new value we created
            (new_value.unwrap(), false)
        }
    }

    /// Returns an iterator over the key-value pairs in the map.
    /// The iterator is a clone of the map's contents, so it does not reflect any changes
    /// made to the map after the iterator is created.
    pub fn iter(&self) -> IterIterator<K, V, S>
    where
        K: Clone,
        V: Clone,
    {
        IterIterator::<K, V, S>::new(self)
    }

    /// Returns an iterator over the cloned keys of the map at the moment of call.
    pub fn keys(&self) -> KeysIterator<K, V, S>
    where
        K: Clone,
    {
        KeysIterator::<K, V, S>::new(self)
    }

    /// Returns an iterator over the cloned values of the map at the moment of call.
    pub fn values(&self) -> ValuesIterator<K, V, S>
    where
        V: Clone,
    {
        ValuesIterator::<K, V, S>::new(self)
    }

    /// Returns the number of key-value pairs in the map.
    pub fn len(&self) -> usize {
        let table = self.table.seq_load();
        table.sum_size()
    }

    /// Returns true if the map contains no elements.
    pub fn is_empty(&self) -> bool {
        let table = self.table.seq_load();
        !table.sum_size_exceeds(0)
    }

    /// Removes all key-value pairs from the map.
    pub fn clear(&self) {
        self.retain(|_, _| false);
    }

    /// Apply a transformation to the value for the given key and return the old value.
    ///
    /// The closure receives the current value (if any) and returns the new value to set.
    /// Returning `None` deletes the entry; returning `Some(v)` inserts or updates it.
    ///
    /// This is a full implementation that performs the update/insert/delete under
    /// per-bucket locks and seqlock writes.
    pub fn alter<F>(&self, key: K, f: F) -> Option<V>
    where
        F: FnOnce(Option<V>) -> Option<V>,
    {
        let (hash64, h2) = self.hash_pair(&key);
        let h2w = broadcast(h2);
        loop {
            let table = self.table.seq_load();
            let idx = self.h1_mask(hash64, table.mask());
            let root = table.get_bucket(idx);

            root.lock();

            // Handle ongoing resize if a new table is ready
            if self.resize_state.started.load(Ordering::Relaxed)
                && self.resize_state.is_new_table_ready()
            {
                root.unlock();
                self.help_copy_and_wait();
                continue; // Retry with possibly swapped table
            }

            // If table swapped during lock acquisition, retry
            if unsafe { *table.seq.as_ptr() } != self.table.seq.load(Ordering::Relaxed) {
                root.unlock();
                continue;
            }

            // Search for key and track first empty slot and tail bucket
            let mut found_info: Option<(*mut Bucket<K, V>, usize)> = None;
            let mut empty_slot_info: Option<(*const Bucket<K, V>, usize)> = None;

            let mut b = root;
            let mut last_bucket = b;
            'search_loop: loop {
                let meta = b.meta.load(Ordering::Relaxed);
                let mut marked = mark_zero_bytes(meta ^ h2w);

                while marked != 0 {
                    let slot = first_marked_byte_index(marked);
                    if let Some(entry) = b.get_entry(slot) {
                        if entry.equal_hash(hash64) {
                            if entry.get_key() == &key {
                                found_info = Some((b as *const _ as *mut _, slot));
                                break 'search_loop;
                            }
                        }
                    }
                    marked &= marked - 1;
                }

                if empty_slot_info.is_none() {
                    let empty = (!meta) & META_MASK;
                    if empty != 0 {
                        empty_slot_info = Some((b as *const _, first_marked_byte_index(empty)));
                    }
                }

                let next_ptr = b.next.load(Ordering::Relaxed);
                if !next_ptr.is_null() {
                    last_bucket = unsafe { &*next_ptr };
                    b = last_bucket;
                } else {
                    break;
                }
            }

            // Existing key path
            if let Some((bucket_ptr, slot)) = found_info {
                let bucket = unsafe { &*bucket_ptr };
                if let Some(entry) = bucket.get_entry(slot) {
                    let old_clone = entry.get_value().clone();

                    return match f(Some(old_clone.clone())) {
                        Some(new_v) => {
                            // Update: create new entry and store pointer
                            let new_entry_ptr = unsafe {
                                let layout = std::alloc::Layout::new::<Entry<K, V>>();
                                let ptr = std::alloc::alloc(layout) as *mut Entry<K, V>;
                                if !ptr.is_null() {
                                    let entry = &mut *ptr;
                                    entry.set_hash(hash64);
                                    entry.key.as_mut_ptr().write(key);
                                    entry.val.as_mut_ptr().write(new_v);
                                }
                                ptr
                            };
                            bucket.set_entry_ptr(slot, new_entry_ptr);
                            root.unlock();
                            Some(old_clone)
                        }
                        None => {
                            // Delete entry: clear pointer and update meta
                            bucket.set_entry_ptr(slot, std::ptr::null_mut());
                            let meta = bucket.meta.load(Ordering::Relaxed);
                            let new_meta = set_byte(meta, EMPTY_SLOT, slot);

                            if std::ptr::eq(bucket, root) {
                                root.unlock_with_meta(new_meta);
                            } else {
                                bucket.meta.store(new_meta, Ordering::Relaxed);
                                root.unlock();
                            }

                            table.add_size(idx, -1);
                            Some(old_clone)
                        }
                    };
                } else {
                    root.unlock();
                    return None;
                }
            }

            // Absent key path
            return match f(None) {
                Some(new_v) => {
                    // Insert new entry
                    if let Some((empty_bucket_ptr, empty_slot)) = empty_slot_info {
                        let empty_bucket = unsafe { &*empty_bucket_ptr };

                        // Create new entry and store pointer
                        let new_entry_ptr = unsafe {
                            let layout = std::alloc::Layout::new::<Entry<K, V>>();
                            let ptr = std::alloc::alloc(layout) as *mut Entry<K, V>;
                            if !ptr.is_null() {
                                let entry = &mut *ptr;
                                entry.set_hash(hash64);
                                entry.key.as_mut_ptr().write(key);
                                entry.val.as_mut_ptr().write(new_v);
                            }
                            ptr
                        };

                        // Store pointer first, then update meta (like Go version)
                        empty_bucket.set_entry_ptr(empty_slot, new_entry_ptr);
                        let meta = empty_bucket.meta.load(Ordering::Relaxed);
                        let new_meta = set_byte(meta, h2, empty_slot);

                        if std::ptr::eq(empty_bucket, root) {
                            root.unlock_with_meta(new_meta);
                        } else {
                            empty_bucket.meta.store(new_meta, Ordering::Relaxed);
                            root.unlock();
                        }

                        table.add_size(idx, 1);
                        None
                    } else {
                        // Append overflow bucket
                        let new_bucket_ptr = Bucket::alloc_single(hash64, h2, key, new_v);
                        last_bucket.next.store(new_bucket_ptr, Ordering::Release);
                        root.unlock();
                        table.add_size(idx, 1);
                        self.maybe_grow(&table);
                        None
                    }
                }
                None => {
                    root.unlock();
                    None
                }
            };
        }
    }

    /// Retain entries for which the predicate returns true; allows in-place mutation of values.
    ///
    /// The predicate receives `&K` and `&mut V` and returns `true` to keep the entry or `false` to delete it.
    /// Processing occurs under per-bucket locks with seqlock writes guarding mutations and deletions.
    pub fn retain<F>(&self, mut f: F) -> &Self
    where
        F: FnMut(&K, &mut V) -> bool,
    {
        'restart: loop {
            let table = self.table.seq_load();

            for i in 0..(table.mask() + 1) {
                let root = table.get_bucket(i);
                root.lock();

                // Check if resize is in progress and help complete the copy
                if self.resize_state.started.load(Ordering::Acquire)
                    && self.resize_state.is_new_table_ready()
                {
                    root.unlock();
                    self.help_copy_and_wait();
                    continue 'restart; // Retry with new table
                }

                // Check if table has been swapped during resize
                if unsafe { *table.seq.as_ptr() } != self.table.seq.load(Ordering::Relaxed) {
                    root.unlock();
                    continue 'restart; // Retry with new table
                }

                let mut b = root;
                loop {
                    let mut meta = b.meta.load(Ordering::Relaxed);

                    // Iterate occupied slots based on meta mask
                    let mut marked = meta & META_MASK;
                    while marked != 0 {
                        let j = first_marked_byte_index(marked);
                        if let Some(entry) = b.get_entry(j) {
                            let key_owned = entry.get_key().clone();
                            let mut val_clone = entry.get_value().clone();

                            let keep = f(&key_owned, &mut val_clone);

                            if !keep {
                                // Delete entry: clear pointer and update meta
                                b.set_entry_ptr(j, std::ptr::null_mut());
                                meta = set_byte(meta, EMPTY_SLOT, j);
                                b.meta.store(meta, Ordering::Relaxed);
                                // Decrement size counter using bucket index
                                table.add_size(i, -1);
                            } else {
                                // Value might have been modified, always create new entry
                                // (simpler than comparing values which requires PartialEq)
                                let new_entry_ptr = unsafe {
                                    let layout = std::alloc::Layout::new::<Entry<K, V>>();
                                    let ptr = std::alloc::alloc(layout) as *mut Entry<K, V>;
                                    if !ptr.is_null() {
                                        let new_entry = &mut *ptr;
                                        new_entry.set_hash(entry.hash);
                                        new_entry.key.as_mut_ptr().write(key_owned);
                                        new_entry.val.as_mut_ptr().write(val_clone);
                                    }
                                    ptr
                                };
                                b.set_entry_ptr(j, new_entry_ptr);
                            }
                        }

                        // Clear the lowest set bit: marked &= marked - 1
                        marked &= marked.wrapping_sub(1);
                    }

                    let next_ptr = b.next.load(Ordering::Relaxed);
                    if !next_ptr.is_null() {
                        b = unsafe { &*next_ptr };
                    } else {
                        break;
                    }
                }
                root.unlock();
            }
            break; // Successfully completed
        }
        self
    }

    // ============================================================================================
    // PRIVATE HELPER METHODS
    // ============================================================================================

    #[inline(always)]
    fn maybe_grow(&self, table: &Table<K, V>) {
        if self.resize_state.started.load(Ordering::Relaxed) {
            return;
        }
        let cap = (table.mask() + 1) * ENTRIES_PER_BUCKET;
        let total = table.sum_size();
        let threshold = (cap as f64 * LOAD_FACTOR) as usize;
        if total > threshold {
            self.try_resize(/*ResizeHint::Grow*/);
        }
    }

    fn try_resize(&self /*, hint: ResizeHint*/) {
        // Check if resize is already in progress using the started flag
        // if self.resize_state.started.load(Ordering::Relaxed) {
        //     return;
        // }

        // Try to acquire the resize lock by setting started to true
        match self.resize_state.started.compare_exchange(
            false,
            true,
            Ordering::AcqRel,
            Ordering::Acquire,
        ) {
            Ok(_) => {
                // Reset resize state fields
                unsafe {
                    *self.resize_state.new_table.get() = None;
                }
                self.resize_state.process.store(0, Ordering::Relaxed);
                self.resize_state.completed.store(0, Ordering::Relaxed);
                self.resize_state.chunks.store(0, Ordering::Release);

                // Call finalize_resize which will create the new table and call help_copy_and_wait
                self.finalize_resize();
            }
            Err(_) => {
                // Another thread started resize, help with the current resize
                // self.help_copy_and_wait();
            }
        }
    }

    fn finalize_resize(&self) {
        let state = &self.resize_state;
        let old_table = self.table.seq_load();
        let old_len = old_table.mask() + 1;

        let size = old_table.sum_size();
        let new_len = calc_table_len(size).max(old_len << 1);

        // Calculate parallelism for the copy operation
        let (_, chunks) = calc_parallelism(old_len, MIN_BUCKETS_PER_CPU);

        // Create the new table (this is where the actual allocation happens)
        let buckets_layout = std::alloc::Layout::array::<Bucket<K, V>>(new_len).unwrap();
        let buckets = unsafe { std::alloc::alloc_zeroed(buckets_layout) as *mut Bucket<K, V> };
        if buckets.is_null() {
            std::alloc::handle_alloc_error(buckets_layout);
        }

        // Allocate and initialize size
        let size_len = calc_size_len(new_len, cpu_count());
        let size_layout = std::alloc::Layout::array::<AtomicUsize>(size_len).unwrap();
        let size = unsafe { std::alloc::alloc_zeroed(size_layout) as *mut AtomicUsize };
        if size.is_null() {
            std::alloc::handle_alloc_error(size_layout);
        }

        let new_table = Table {
            buckets: UnsafeCell::new(buckets),
            mask: UnsafeCell::new(new_len - 1),
            size: UnsafeCell::new(size),
            size_mask: UnsafeCell::new((size_len - 1) as u32),
            seq: AtomicU32::new(2), // Set to 2 to indicate initialization is complete
        };

        // Store new table first, then publish via chunks with Release
        unsafe {
            *state.new_table.get() = Some(new_table);
        }
        state.chunks.store(chunks as i32, Ordering::Release);

        // Now call help_copy_and_wait to perform the actual data copying
        // help_copy_and_wait will handle the table swap and cleanup when all chunks are done
        self.help_copy_and_wait();
    }

    fn help_copy_and_wait(&self) {
        let state = &self.resize_state;
        if !state.started.load(Ordering::Acquire) {
            return;
        }

        // Get chunks first as an Acquire barrier to ensure visibility of new_table
        let chunks = state.chunks.load(Ordering::Acquire);

        // If chunks is 0, the resize hasn't been properly initialized yet
        if chunks == 0 {
            return;
        }

        // Get new table from state after the barrier
        let new_table_opt = unsafe { &*state.new_table.get() };
        let new_table = match new_table_opt {
            Some(ref table) => table,
            None => return, // New table not ready yet
        };

        let old_table = &self.table;
        let table_len = old_table.mask() + 1;
        // Calculate chunk size
        let chunk_sz = (table_len + chunks as usize - 1) / chunks as usize;

        // Work loop - similar to Go's for loop
        loop {
            // fetch_add returns the previous value, so we need to add 1 to get the current process number
            let process_prev = state.process.fetch_add(1, Ordering::Relaxed);
            if process_prev >= chunks {
                let mut spins = 0;
                // No more work, wait for completion
                while state.started.load(Ordering::Relaxed) {
                    delay(&mut spins)
                }
                return;
            }

            // Convert to 0-based index (Go uses 1-based then decrements)
            let process0 = process_prev;

            // Calculate chunk boundaries
            let start = process0 as usize * chunk_sz;
            let _end = std::cmp::min(start + chunk_sz, table_len);

            // Copy the chunk - pass is_growth parameter to determine locking strategy
            self.copy_chunk(&old_table, new_table, process0 as usize, chunks as usize);

            // Mark chunk as completed
            let completed = state.completed.fetch_add(1, Ordering::Relaxed) + 1;
            if completed == chunks {
                // All chunks completed, finalize the table swap using seqlock
                let new_table_copy = unsafe { (*state.new_table.get()).as_ref().unwrap().clone() };

                // Store the old table for cleanup
                let old_table_copy = self.table.seq_load();
                unsafe {
                    (&mut *self.old_tables.get()).push(Box::new(old_table_copy));
                }

                // Perform seqlock table swap
                self.table.seq_store(&new_table_copy);

                // Clear resize state
                unsafe {
                    *state.new_table.get() = None;
                }
                state.chunks.store(0, Ordering::Release);
                state.started.store(false, Ordering::Release);
                return;
            }
        }
    }

    fn copy_chunk(
        &self,
        old_table: &Table<K, V>,
        new_table: &Table<K, V>,
        chunk_id: usize,
        total_chunks: usize,
    ) where
        K: Clone + Eq + Hash,
        V: Clone,
    {
        let old_len = old_table.mask() + 1;
        let chunk_size = (old_len + total_chunks - 1) / total_chunks; // Ceiling division
        let start = chunk_id * chunk_size;
        let end = std::cmp::min(start + chunk_size, old_len);
        let total_copied = self.copy_bucket(old_table, start, end, new_table);

        // Update size in batch like Go's AddSize(start, copied)
        if total_copied > 0 {
            new_table.add_size(start, total_copied as isize);
        }
    }

    fn copy_bucket(
        &self,
        old_table: &Table<K, V>,
        start: usize,
        end: usize,
        new_table: &Table<K, V>,
    ) -> usize
    where
        K: Clone + Eq + Hash,
        V: Clone,
    {
        let mut copied = 0;
        for i in start..end {
            let bucket = old_table.get_bucket(i);
            // Lock the source bucket to stabilize the chain
            bucket.lock();

            let mut current = bucket;
            loop {
                let meta = current.meta.load(Ordering::Relaxed);
                let mut marked = meta & META_MASK;

                while marked != 0 {
                    let j = first_marked_byte_index(marked);
                    if let Some(entry) = current.get_entry(j) {
                        let hash64 = entry.hash;
                        // Extract h2 directly from meta instead of recalculating
                        let h2 = ((meta >> (j * 8)) & 0xFF) as u8;

                        // Reuse existing entry pointer to avoid allocation
                        let entry_ptr = current.get_entry_ptr(j);

                        // Direct insertion into destination bucket (like Go's appendTo loop)
                        let idx = self.h1_mask(hash64, new_table.mask());
                        let dest_bucket = new_table.get_bucket(idx);
                        let mut current_dest = dest_bucket;

                        'append_to: loop {
                            let dest_meta = current_dest.meta.load(Ordering::Relaxed);
                            let empty = (!dest_meta) & META_MASK;

                            if empty != 0 {
                                // Found empty slot - store existing pointer and update meta
                                let empty_idx = first_marked_byte_index(empty);
                                let new_meta = set_byte(dest_meta, h2, empty_idx);
                                // Store pointer first, then update meta (like Go version)
                                current_dest.set_entry_ptr(empty_idx, entry_ptr);
                                current_dest.meta.store(new_meta, Ordering::Relaxed);
                                break 'append_to;
                            }

                            // No empty slot, check for next bucket
                            let next_ptr = current_dest.next.load(Ordering::Relaxed);
                            if next_ptr.is_null() {
                                // Create new overflow bucket holding existing pointer
                                let new_ptr = Bucket::alloc_single_with_ptr(h2, entry_ptr);
                                current_dest.next.store(new_ptr, Ordering::Relaxed);
                                break 'append_to;
                            } else {
                                current_dest = unsafe { &*next_ptr };
                            }
                        }

                        copied += 1;
                    }
                    marked &= marked - 1;
                }

                // Move to next bucket in chain
                let next_ptr = current.next.load(Ordering::Relaxed);
                if next_ptr.is_null() {
                    break;
                }
                current = unsafe { &*next_ptr };
            }

            bucket.unlock();
        }
        copied
    }

    #[inline(always)]
    fn hash_pair(&self, key: &K) -> (u64, u8) {
        // Use compile-time type checking for zero-cost optimization
        let h64 = if is_numeric_type::<K>() {
            // For numeric types, use the value directly (zero-cost like Go's intKey)
            if std::any::TypeId::of::<K>() == std::any::TypeId::of::<u64>() {
                unsafe { *(key as *const K as *const u64) }
            } else if std::any::TypeId::of::<K>() == std::any::TypeId::of::<u32>() {
                unsafe { *(key as *const K as *const u32) as u64 }
            } else if std::any::TypeId::of::<K>() == std::any::TypeId::of::<u16>() {
                unsafe { *(key as *const K as *const u16) as u64 }
            } else if std::any::TypeId::of::<K>() == std::any::TypeId::of::<u8>() {
                unsafe { *(key as *const K as *const u8) as u64 }
            } else if std::any::TypeId::of::<K>() == std::any::TypeId::of::<usize>() {
                unsafe { *(key as *const K as *const usize) as u64 }
            } else if std::any::TypeId::of::<K>() == std::any::TypeId::of::<i64>() {
                unsafe { *(key as *const K as *const i64) as u64 }
            } else if std::any::TypeId::of::<K>() == std::any::TypeId::of::<i32>() {
                unsafe { *(key as *const K as *const i32) as u64 }
            } else if std::any::TypeId::of::<K>() == std::any::TypeId::of::<i16>() {
                unsafe { *(key as *const K as *const i16) as u64 }
            } else if std::any::TypeId::of::<K>() == std::any::TypeId::of::<i8>() {
                unsafe { *(key as *const K as *const i8) as u64 }
            } else if std::any::TypeId::of::<K>() == std::any::TypeId::of::<isize>() {
                unsafe { *(key as *const K as *const isize) as u64 }
            } else {
                // Should not reach here if is_numeric_type is correct
                self.hasher.hash_one(key)
            }
        } else if is_string_type::<K>() {
            // For string types, use optimized string hash
            if std::any::TypeId::of::<K>() == std::any::TypeId::of::<String>() {
                let s = unsafe { &*(key as *const K as *const String) };
                if s.len() <= 12 {
                    fast_hash_string(s).0
                } else {
                    self.hasher.hash_one(key)
                }
            } else if std::any::TypeId::of::<K>() == std::any::TypeId::of::<&str>() {
                let s = unsafe { *(key as *const K as *const &str) };
                if s.len() <= 12 {
                    fast_hash_string(s).0
                } else {
                    self.hasher.hash_one(key)
                }
            } else {
                self.hasher.hash_one(key)
            }
        } else {
            // Fallback to standard hasher for other types
            self.hasher.hash_one(key)
        };
        // h64 = h64.max(1);
        let h2 = self.h2(h64);
        (h64, h2)
    }

    #[inline(always)]
    fn h1_mask(&self, hash64: u64, mask: usize) -> usize {
        if is_numeric_type::<K>() {
            // Linear distribution for integers (like Go's intKey=true)
            ((hash64 as usize) / ENTRIES_PER_BUCKET) & mask
        } else {
            // Shift distribution for strings and other types (like Go's intKey=false)
            ((hash64 >> 7) as usize) & mask
        }
    }

    #[inline(always)]
    fn h2(&self, hash64: u64) -> u8 {
        (hash64 as u8) | SLOT_MASK // Use top 7 bits, avoid 0x80 which is used for locking
    }
}

// ================================================================================================
// SHARED FLATMAP (SHARDED)
// ================================================================================================

/// Shared (sharded) NodeMap: fixed-size array of NodeMap shards selected by hashing the key.
/// The shard count is specified via a const generic parameter `N`.
/// For convenience, use `SharedNodeMap<K, V, S>` which aliases to 32 shards.
pub struct NodeMapShared<K, V, S: BuildHasher, const N: usize> {
    shards: [NodeMap<K, V, S>; N],
}

/// Default alias using 32 shards
pub type SharedNodeMap<K, V, S = RandomState> = NodeMapShared<K, V, S, 32>;

impl<K: Eq + Hash + Clone + 'static, V: Clone, const N: usize> NodeMapShared<K, V, RandomState, N> {
    /// Create a new SharedNodeMap with default hasher per shard.
    #[inline(always)]
    pub fn new() -> Self {
        Self::with_capacity(0)
    }

    /// Create a new SharedNodeMap with the specified capacity for each shard.
    #[inline(always)]
    pub fn with_capacity(size_hint: usize) -> Self {
        let shards =
            std::array::from_fn(|_| NodeMap::<K, V, RandomState>::with_capacity(size_hint));
        Self { shards }
    }
}

impl<K: Eq + Hash + Clone + 'static, V: Clone, S: BuildHasher + Clone, const N: usize>
    NodeMapShared<K, V, S, N>
{
    /// Create a new SharedNodeMap using the provided hasher cloned into each shard.
    #[inline(always)]
    pub fn with_hasher(hasher: S) -> Self {
        Self::with_capacity_and_hasher(0, hasher)
    }

    /// Create a new SharedNodeMap with specified capacity and hasher for each shard.
    #[inline(always)]
    pub fn with_capacity_and_hasher(size_hint: usize, hasher: S) -> Self {
        let shards = std::array::from_fn(|_| {
            NodeMap::<K, V, S>::with_capacity_and_hasher(size_hint, hasher.clone())
        });
        Self { shards }
    }

    /// Internal: pick shard index by hashing the key with NodeMap's hash function.
    #[inline(always)]
    fn index_for(&self, key: &K) -> usize {
        let (h64, _h2) = self.shards[0].hash_pair(key);
        (h64 as usize) % N
    }

    // ============================================================================================
    // PUBLIC API WRAPPERS (forced inline)
    // ============================================================================================

    /// Check whether the given key is present.
    #[inline(always)]
    pub fn contains_key(&self, key: &K) -> bool {
        let i = self.index_for(key);
        self.shards[i].contains_key(key)
    }

    /// Get the value associated with the given key as a cloned `V`.
    #[inline(always)]
    pub fn get(&self, key: &K) -> Option<V>
    where
        V: Clone,
    {
        let i = self.index_for(key);
        self.shards[i].get(key)
    }

    /// Insert a key-value pair into the shared map.
    #[inline(always)]
    pub fn insert(&self, key: K, val: V) -> Option<V>
    where
        V: Clone,
    {
        let i = self.index_for(&key);
        self.shards[i].insert(key, val)
    }

    /// Remove a key-value pair from the shared map.
    #[inline(always)]
    pub fn remove(&self, key: K) -> Option<V>
    where
        V: Clone,
    {
        let i = self.index_for(&key);
        self.shards[i].remove(key)
    }

    /// Get or insert with a closure.
    #[inline(always)]
    pub fn get_or_insert_with<F: FnOnce() -> V>(&self, key: K, f: F) -> (V, bool)
    where
        V: Clone,
    {
        let i = self.index_for(&key);
        self.shards[i].get_or_insert_with(key, f)
    }

    /// Iterator over all key-value pairs as a Vec-backed iterator.
    #[inline(always)]
    pub fn iter(&self) -> std::vec::IntoIter<(K, V)>
    where
        K: Clone,
        V: Clone,
        S: BuildHasher + Default,
    {
        let mut items = Vec::new();
        for shard in &self.shards {
            items.extend(shard.into_iter());
        }
        items.into_iter()
    }

    /// Iterator over all keys as a Vec-backed iterator.
    #[inline(always)]
    pub fn keys(&self) -> std::vec::IntoIter<K>
    where
        K: Clone,
        V: Clone,
        S: BuildHasher + Default,
    {
        let mut keys = Vec::new();
        for shard in &self.shards {
            keys.extend(shard.into_iter().map(|(k, _)| k));
        }
        keys.into_iter()
    }

    /// Iterator over all values as a Vec-backed iterator.
    #[inline(always)]
    pub fn values(&self) -> std::vec::IntoIter<V>
    where
        K: Clone,
        V: Clone,
        S: BuildHasher + Default,
    {
        let mut vals = Vec::new();
        for shard in &self.shards {
            vals.extend(shard.into_iter().map(|(_, v)| v));
        }
        vals.into_iter()
    }

    /// Total number of pairs across all shards.
    #[inline(always)]
    pub fn len(&self) -> usize {
        self.shards.iter().map(|s| s.len()).sum()
    }

    /// Returns true if the map contains no elements across all shards.
    #[inline(always)]
    pub fn is_empty(&self) -> bool {
        self.len() == 0
    }

    /// Removes all key-value pairs from all shards.
    #[inline(always)]
    pub fn clear(&self) {
        for shard in &self.shards {
            shard.clear();
        }
    }

    /// Apply a transformation to the value for the given key and return the old value.
    #[inline(always)]
    pub fn alter<F>(&self, key: K, f: F) -> Option<V>
    where
        F: FnOnce(Option<V>) -> Option<V>,
    {
        let i = self.index_for(&key);
        self.shards[i].alter(key, f)
    }
}

// ================================================================================================
// BUCKET IMPLEMENTATION
// ================================================================================================

impl<K, V> Bucket<K, V> {
    // #[inline(always)]
    // fn new() -> Self {
    //     Self {
    //         seq: AtomicU64::new(0),
    //         meta: AtomicU64::new(0),
    //         next: AtomicPtr::new(std::ptr::null_mut()),
    //         entries: UnsafeCell::new(std::array::from_fn(|_| Entry::default())),
    //     }
    // }

    #[inline(always)]
    fn alloc_single(hash: u64, h2: u8, key: K, val: V) -> *mut Bucket<K, V> {
        unsafe {
            let layout = std::alloc::Layout::new::<Bucket<K, V>>();
            let ptr = std::alloc::alloc_zeroed(layout) as *mut Bucket<K, V>;
            if ptr.is_null() {
                std::alloc::handle_alloc_error(layout);
            }

            let bucket = &mut *ptr;

            // Create and store the first entry
            let entry_layout = std::alloc::Layout::new::<Entry<K, V>>();
            let entry_ptr = std::alloc::alloc(entry_layout) as *mut Entry<K, V>;
            if entry_ptr.is_null() {
                std::alloc::handle_alloc_error(entry_layout);
            }

            let entry = &mut *entry_ptr;
            entry.set_hash(hash);
            entry.key.as_mut_ptr().write(key);
            entry.val.as_mut_ptr().write(val);

            // // Initialize atomic pointer array with null pointers
            // for i in 0..ENTRIES_PER_BUCKET {
            //     std::ptr::write(&mut bucket.entries[i], AtomicPtr::new(std::ptr::null_mut()));
            // }

            // Store the entry pointer in the first slot
            bucket.entries[0].store(entry_ptr, Ordering::Relaxed);

            // Set meta to mark the first slot as occupied
            std::ptr::write(&mut bucket.meta, AtomicU64::from(set_byte(0, h2, 0)));

            ptr
        }
    }

    #[inline(always)]
    fn alloc_single_with_ptr(h2: u8, entry_ptr: *mut Entry<K, V>) -> *mut Bucket<K, V> {
        unsafe {
            let layout = std::alloc::Layout::new::<Bucket<K, V>>();
            let ptr = std::alloc::alloc_zeroed(layout) as *mut Bucket<K, V>;
            if ptr.is_null() {
                std::alloc::handle_alloc_error(layout);
            }

            let bucket = &mut *ptr;
            bucket.entries[0].store(entry_ptr, Ordering::Relaxed);
            std::ptr::write(&mut bucket.meta, AtomicU64::from(set_byte(0, h2, 0)));
            ptr
        }
    }

    #[inline(always)]
    fn lock(&self) {
        let mut spins = 0;
        loop {
            // Attempt lock if not held
            let cur = self.meta.load(Ordering::Relaxed);
            if (cur & OP_LOCK_MASK) == 0 {
                if self
                    .meta
                    .compare_exchange_weak(
                        cur,
                        cur | OP_LOCK_MASK,
                        Ordering::Acquire,
                        Ordering::Relaxed,
                    )
                    .is_ok()
                {
                    break;
                }
                continue;
            }
            delay(&mut spins);
        }
    }

    #[inline(always)]
    fn unlock(&self) {
        // Release root bucket lock by clearing the bit via Store
        // Using Store(clear-bit) avoids RMW cost of fetch_and
        let cur = self.meta.load(Ordering::Relaxed);
        self.meta.store(cur & !OP_LOCK_MASK, Ordering::Release);
    }

    #[inline(always)]
    fn unlock_with_meta(&self, meta: u64) {
        // Clear lock a bit while publishing a specific meta value
        self.meta.store(meta & !OP_LOCK_MASK, Ordering::Release);
    }

    /// Get entry pointer at index (may be null)
    #[inline(always)]
    fn get_entry_ptr(&self, index: usize) -> *mut Entry<K, V> {
        self.entries[index].load(Ordering::Relaxed)
    }

    /// Set entry pointer at index
    #[inline(always)]
    fn set_entry_ptr(&self, index: usize, ptr: *mut Entry<K, V>) {
        self.entries[index].store(ptr, Ordering::Relaxed);
    }

    /// Get entry reference at index (returns None if null)
    #[inline(always)]
    fn get_entry(&self, index: usize) -> Option<&Entry<K, V>> {
        let ptr = self.get_entry_ptr(index);
        if ptr.is_null() {
            None
        } else {
            Some(unsafe { &*ptr })
        }
    }

    #[inline(always)]
    fn get_next_bucket(&self) -> Option<&Bucket<K, V>> {
        let next_ptr = self.next.load(Ordering::Acquire);
        if next_ptr.is_null() {
            None
        } else {
            Some(unsafe { &*next_ptr })
        }
    }

    #[inline(always)]
    fn get_next_bucket_relaxed(&self) -> Option<&Bucket<K, V>> {
        let next_ptr = self.next.load(Ordering::Relaxed);
        if next_ptr.is_null() {
            None
        } else {
            Some(unsafe { &*next_ptr })
        }
    }
}

impl<K: Clone, V: Clone> Clone for Bucket<K, V> {
    fn clone(&self) -> Self {
        // Create new bucket with atomic pointer array
        let mut new_entries: [AtomicPtr<Entry<K, V>>; ENTRIES_PER_BUCKET] =
            std::array::from_fn(|_| AtomicPtr::new(std::ptr::null_mut()));

        let meta = self.meta.load(Ordering::Relaxed);
        let mut marked = meta & META_MASK;

        // Clone occupied entries
        while marked != 0 {
            let j = first_marked_byte_index(marked);

            if let Some(src_entry) = self.get_entry(j) {
                unsafe {
                    // Allocate new entry
                    let entry_layout = std::alloc::Layout::new::<Entry<K, V>>();
                    let entry_ptr = std::alloc::alloc(entry_layout) as *mut Entry<K, V>;
                    if !entry_ptr.is_null() {
                        let entry = &mut *entry_ptr;
                        entry.hash = src_entry.hash;
                        entry
                            .key
                            .as_mut_ptr()
                            .write(src_entry.key.assume_init_ref().clone());
                        entry
                            .val
                            .as_mut_ptr()
                            .write(src_entry.val.assume_init_ref().clone());
                        new_entries[j] = AtomicPtr::new(entry_ptr);
                    }
                }
            }
            marked &= marked - 1;
        }

        Bucket {
            meta: AtomicU64::new(meta),
            next: AtomicPtr::new(std::ptr::null_mut()), // reset chain; will be rebuilt on resize path
            entries: new_entries,
        }
    }
}

// ================================================================================================
// TABLE IMPLEMENTATION
// ================================================================================================

impl<K, V> Table<K, V> {
    /// Helper methods for accessing UnsafeCell fields
    #[inline(always)]
    fn buckets(&self) -> *mut Bucket<K, V> {
        unsafe { *self.buckets.get() }
    }

    #[inline(always)]
    fn mask(&self) -> usize {
        unsafe { *self.mask.get() }
    }

    #[inline(always)]
    fn size(&self) -> *mut AtomicUsize {
        unsafe { *self.size.get() }
    }

    #[inline(always)]
    fn size_mask(&self) -> u32 {
        unsafe { *self.size_mask.get() }
    }

    /// Safe bucket access helpers
    #[inline(always)]
    fn get_bucket(&self, index: usize) -> &Bucket<K, V> {
        unsafe { &*self.buckets().add(index) }
    }

    #[inline(always)]
    fn get_bucket_mut(&self, index: usize) -> &mut Bucket<K, V> {
        unsafe { &mut *self.buckets().add(index) }
    }

    #[inline(always)]
    fn get_size_stripe(&self, index: usize) -> &AtomicUsize {
        unsafe { &*self.size().add(index) }
    }

    /// SeqLoad performs a seqlock read of the table, similar to Go's nodeTable.SeqLoad()
    #[inline(always)]
    fn seq_load(&self) -> Table<K, V> {
        loop {
            let s1 = self.seq.load(Ordering::Acquire);
            if s1 & 1 == 0 {
                // Even sequence number means stable
                let table_copy = Table {
                    buckets: UnsafeCell::new(self.buckets()),
                    mask: UnsafeCell::new(self.mask()),
                    size: UnsafeCell::new(self.size()),
                    size_mask: UnsafeCell::new(self.size_mask()),
                    seq: AtomicU32::new(s1),
                };
                let s2 = self.seq.load(Ordering::Acquire);
                if s1 == s2 {
                    return table_copy;
                }
            }
            // Retry if sequence changed or was odd
            std::hint::spin_loop();
        }
    }

    /// SeqStore performs a seqlock write of the table, similar to Go's nodeTable.SeqStore()
    #[inline(always)]
    fn seq_store(&self, new_table: &Table<K, V>) {
        let s = self.seq.load(Ordering::Relaxed);
        self.seq.store(s + 1, Ordering::Relaxed); // Make odd (write in progress)

        // Update table fields
        unsafe {
            *self.buckets.get() = new_table.buckets();
            *self.mask.get() = new_table.mask();
            *self.size.get() = new_table.size();
            *self.size_mask.get() = new_table.size_mask();
        }

        self.seq.store(s + 2, Ordering::Release); // Make even (write complete)
    }

    /// AddSize adds delta to the size counter at the given index
    #[inline(always)]
    fn add_size(&self, idx: usize, delta: isize) {
        let stripe = idx & (self.size_mask() as usize);
        if delta > 0 {
            self.get_size_stripe(stripe)
                .fetch_add(delta as usize, Ordering::Relaxed);
        } else {
            self.get_size_stripe(stripe)
                .fetch_sub((-delta) as usize, Ordering::Relaxed);
        }
    }

    /// SumSize returns the total size across all stripes
    #[inline(always)]
    fn sum_size(&self) -> usize {
        let mut sum = 0usize;
        for i in 0..=(self.size_mask() as usize) {
            sum = sum.wrapping_add(self.get_size_stripe(i).load(Ordering::Relaxed));
        }
        sum
    }

    /// SumSizeExceeds checks if total size exceeds the limit, with early return
    #[inline(always)]
    fn sum_size_exceeds(&self, limit: usize) -> bool {
        let mut sum = 0usize;
        for i in 0..=(self.size_mask() as usize) {
            sum = sum.wrapping_add(self.get_size_stripe(i).load(Ordering::Relaxed));
            if sum > limit {
                return true;
            }
        }
        false
    }
}

// ================================================================================================
// ENTRY IMPLEMENTATION
// ================================================================================================

impl<K, V> Entry<K, V> {
    /// Get the actual hash value (without the init flag)
    #[inline(always)]
    fn equal_hash(&self, hash64: u64) -> bool {
        self.hash == hash64
    }

    /// Set the hash with the init flag
    #[inline(always)]
    fn set_hash(&mut self, hash64: u64) {
        self.hash = hash64
    }

    /// Safe key access for occupied entries
    #[inline(always)]
    fn get_key(&self) -> &K {
        // debug_assert!(self.is_occupied(), "Entry must be occupied to access key");
        unsafe { self.key.assume_init_ref() }
    }

    /// Safe value access for occupied entries
    #[inline(always)]
    fn get_value(&self) -> &V {
        // debug_assert!(self.is_occupied(), "Entry must be occupied to access value");
        unsafe { self.val.assume_init_ref() }
    }

    /// Unsafe clone for concurrent access - caller must ensure entry is occupied
    #[inline(always)]
    unsafe fn unsafe_clone_key_value(&self) -> (K, V)
    where
        K: Clone,
        V: Clone,
    {
        (
            self.key.assume_init_ref().clone(),
            self.val.assume_init_ref().clone(),
        )
    }

    // removed: init_entry (unused)
}

// ================================================================================================
// DROP IMPLEMENTATIONS
// ================================================================================================

impl<K, V, S: BuildHasher> Drop for NodeMap<K, V, S> {
    fn drop(&mut self) {
        // Clean up the main table
        if !self.table.buckets().is_null() {
            let buckets_len = self.table.mask() + 1;

            // Clean up linked buckets first
            for i in 0..buckets_len {
                unsafe {
                    let bucket = self.table.get_bucket(i);
                    let mut ptr = bucket.next.load(Ordering::Relaxed);
                    while !ptr.is_null() {
                        let next_ptr = (*ptr).next.load(Ordering::Relaxed);
                        // Deallocate overflow bucket allocated via raw allocator
                        std::ptr::drop_in_place(ptr);
                        let layout = std::alloc::Layout::new::<Bucket<K, V>>();
                        std::alloc::dealloc(ptr as *mut u8, layout);
                        ptr = next_ptr;
                    }
                }
            }

            // Drop buckets in place
            for i in 0..buckets_len {
                unsafe {
                    std::ptr::drop_in_place(self.table.get_bucket_mut(i));
                }
            }

            // Deallocate buckets memory
            unsafe {
                let buckets_layout =
                    std::alloc::Layout::array::<Bucket<K, V>>(buckets_len).unwrap();
                std::alloc::dealloc(self.table.buckets() as *mut u8, buckets_layout);
            }
        }

        if !self.table.size().is_null() {
            let size_len = self.table.size_mask() as usize + 1;

            // AtomicUsize doesn't need drop_in_place, just deallocate memory
            unsafe {
                let size_layout = std::alloc::Layout::array::<AtomicUsize>(size_len).unwrap();
                std::alloc::dealloc(self.table.size() as *mut u8, size_layout);
            }
        }

        // Clean up old tables
        unsafe {
            let old_tables = &mut *self.old_tables.get();
            old_tables.clear();
        }
    }
}

impl<K, V> Drop for Table<K, V> {
    fn drop(&mut self) {
        // TODO: Fix memory management for seqlock implementation
        // For now, disable automatic cleanup to avoid heap corruption
        // Memory will be cleaned up by NodeMap's Drop implementation
    }
}

// ================================================================================================
// STANDARD TRAIT IMPLEMENTATIONS
// ================================================================================================

// Provide idiomatic trait implementations to integrate with the Rust ecosystem.
impl<K: Eq + Hash + Clone + 'static, V: Clone, S: BuildHasher + Default> Default
    for NodeMap<K, V, S>
{
    fn default() -> Self {
        Self::with_hasher(S::default())
    }
}

impl<'a, K: Eq + Hash + Clone + 'static, V: Clone, S: BuildHasher + Default> IntoIterator
    for &'a NodeMap<K, V, S>
{
    type Item = (K, V);
    type IntoIter = std::vec::IntoIter<(K, V)>;
    fn into_iter(self) -> Self::IntoIter {
        // Collect to a Vec to give a concrete iterator type.
        self.iter().collect::<Vec<_>>().into_iter()
    }
}

impl<K: Eq + Hash + Clone + 'static, V: Clone, S: BuildHasher + Default> FromIterator<(K, V)>
    for NodeMap<K, V, S>
{
    fn from_iter<T: IntoIterator<Item = (K, V)>>(iter: T) -> Self {
        let map = NodeMap::with_hasher(S::default());
        for (k, v) in iter {
            let _ = map.insert(k, v);
        }
        map
    }
}

impl<K: Eq + Hash + Clone + 'static, V: Clone, S: BuildHasher + Default> Extend<(K, V)>
    for NodeMap<K, V, S>
{
    fn extend<T: IntoIterator<Item = (K, V)>>(&mut self, iter: T) {
        for (k, v) in iter {
            let _ = self.insert(k, v);
        }
    }
}

// ================================================================================================
// UTILITY FUNCTIONS
// ================================================================================================

/// Helper functions to check if a type is numeric at compile time
#[inline(always)]
fn is_numeric_type<T: 'static>() -> bool {
    std::any::TypeId::of::<T>() == std::any::TypeId::of::<u8>()
        || std::any::TypeId::of::<T>() == std::any::TypeId::of::<u16>()
        || std::any::TypeId::of::<T>() == std::any::TypeId::of::<u32>()
        || std::any::TypeId::of::<T>() == std::any::TypeId::of::<u64>()
        || std::any::TypeId::of::<T>() == std::any::TypeId::of::<usize>()
        || std::any::TypeId::of::<T>() == std::any::TypeId::of::<i8>()
        || std::any::TypeId::of::<T>() == std::any::TypeId::of::<i16>()
        || std::any::TypeId::of::<T>() == std::any::TypeId::of::<i32>()
        || std::any::TypeId::of::<T>() == std::any::TypeId::of::<i64>()
        || std::any::TypeId::of::<T>() == std::any::TypeId::of::<isize>()
}

#[inline(always)]
fn is_string_type<T: 'static>() -> bool {
    std::any::TypeId::of::<T>() == std::any::TypeId::of::<String>()
        || std::any::TypeId::of::<T>() == std::any::TypeId::of::<&str>()
}

/// Fast string hash similar to Go's hashString
#[inline(always)]
fn fast_hash_string(s: &str) -> (u64, bool) {
    let mut hash = 0u64;
    for byte in s.as_bytes() {
        hash = hash.wrapping_mul(31).wrapping_add(*byte as u64);
    }
    (hash, false) // Use shift distribution for strings
}

/// Calculate the next power of 2 greater than or equal to n
fn next_pow2(mut n: usize) -> usize {
    if n < 2 {
        return 2;
    }
    n -= 1;
    n |= n >> 1;
    n |= n >> 2;
    n |= n >> 4;
    n |= n >> 8;
    n |= n >> 16;
    if usize::BITS == 64 {
        n |= n >> 32;
    }
    n + 1
}

/// Calculate table length based on size hint
fn calc_table_len(size_hint: usize) -> usize {
    let min_cap = (size_hint as f64 * (1.0 / (ENTRIES_PER_BUCKET as f64 * LOAD_FACTOR))) as usize;
    let base = min_cap.max(MIN_TABLE_LEN);
    next_pow2(base)
}

/// Calculate size array length based on table length and CPU count
fn calc_size_len(table_len: usize, cpus: usize) -> usize {
    next_pow2(cpus.min(table_len >> 10))
}

/// Calculate parallelism for resize operations
fn calc_parallelism(table_len: usize, min_buckets_per_cpu: usize) -> (usize, usize) {
    let cpus = cpu_count();
    let max_workers = cpus * RESIZE_OVER_PARTITION; // Use over-partition factor to reduce resize tail latency
    let chunks = (table_len / min_buckets_per_cpu).max(1).min(max_workers);
    (max_workers, chunks)
}

#[inline(always)]
fn broadcast(b: u8) -> u64 {
    (b as u64) * 0x0101_0101_0101_0101
}

#[inline(always)]
fn first_marked_byte_index(w: u64) -> usize {
    (w.trailing_zeros() >> 3) as usize
}

#[inline(always)]
fn mark_zero_bytes(w: u64) -> u64 {
    (w.wrapping_sub(0x0101_0101_0101_0101)) & (!w) & META_MASK
}

#[inline(always)]
fn set_byte(w: u64, b: u8, idx: usize) -> u64 {
    let shift = (idx as u64) << 3;
    (w & !(0xffu64 << shift)) | ((b as u64) << shift)
}

#[inline(always)]
fn try_spin(spins: &mut i32) -> bool {
    if *spins < SPIN_BEFORE_YIELD {
        *spins += *spins + 1;
        std::hint::spin_loop();
        true
    } else {
        false
    }
}

#[inline(always)]
fn delay(spins: &mut i32) {
    if !try_spin(spins) {
        *spins = 0;
        thread::yield_now();
        //thread::sleep(Duration::from_micros(500));
    }
}

// ================================================================================================
// ITERATOR IMPLEMENTATIONS
// ================================================================================================

// Unified per-bucket collector used by KeysIterator/ValuesIterator/IterIterator
// Clones the requested item type lazily, bucket by bucket, avoiding large allocations.
fn collect_next_bucket<K, V, T, F>(
    table: &Table<K, V>,
    bucket_index: &mut usize,
    entries_collected: &mut Vec<T>,
    mut make: F,
) -> bool
where
    K: Clone,
    V: Clone,
    F: FnMut(&Entry<K, V>, u64, usize) -> Option<T>,
{
    while *bucket_index <= table.mask() {
        let root = table.get_bucket(*bucket_index);
        root.lock();

        let mut bucket_opt = Some(root);
        entries_collected.clear();

        while let Some(b) = bucket_opt {
            let meta = b.meta.load(Ordering::Relaxed);
            let mut marked = meta & META_MASK;

            while marked != 0 {
                let j = first_marked_byte_index(marked);
                if let Some(e) = b.get_entry(j) {
                    if let Some(item) = make(e, meta, j) {
                        entries_collected.push(item);
                    }
                }
                marked &= marked - 1;
            }
            bucket_opt = b.get_next_bucket_relaxed();
        }

        root.unlock();
        *bucket_index += 1;

        if !entries_collected.is_empty() {
            return true;
        }
    }
    false
}

/// Iterator over the keys of a NodeMap
pub struct KeysIterator<K, V, S = RandomState>
where
    S: BuildHasher,
{
    table: Table<K, V>,
    bucket_index: usize,
    entries_collected: Vec<K>,
    entries_index: usize,
    _phantom: PhantomData<S>,
}

impl<K, V, S> KeysIterator<K, V, S>
where
    K: Clone,
    V: Clone,
    S: BuildHasher,
{
    fn new(map: &NodeMap<K, V, S>) -> Self {
        Self {
            table: map.table.seq_load(),
            bucket_index: 0,
            entries_collected: Vec::new(),
            entries_index: 0,
            _phantom: PhantomData,
        }
    }

    fn collect_bucket_keys(&mut self) -> bool {
        let ok = collect_next_bucket(
            &self.table,
            &mut self.bucket_index,
            &mut self.entries_collected,
            |e, _meta, _j| {
                let (k, _) = unsafe { e.unsafe_clone_key_value() };
                Some(k)
            },
        );
        if ok {
            self.entries_index = 0;
        }
        ok
    }
}

impl<K, V, S> Iterator for KeysIterator<K, V, S>
where
    K: Clone,
    V: Clone,
    S: BuildHasher,
{
    type Item = K;

    fn next(&mut self) -> Option<Self::Item> {
        if self.entries_index < self.entries_collected.len() {
            let key = self.entries_collected[self.entries_index].clone();
            self.entries_index += 1;
            return Some(key);
        }

        if self.collect_bucket_keys() {
            self.next()
        } else {
            None
        }
    }
}

/// Iterator over the values of a NodeMap
pub struct ValuesIterator<K, V, S = RandomState>
where
    S: BuildHasher,
{
    table: Table<K, V>,
    bucket_index: usize,
    entries_collected: Vec<V>,
    entries_index: usize,
    _phantom: PhantomData<S>,
}

impl<K, V, S> ValuesIterator<K, V, S>
where
    K: Clone,
    V: Clone,
    S: BuildHasher,
{
    fn new(map: &NodeMap<K, V, S>) -> Self {
        Self {
            table: map.table.seq_load(),
            bucket_index: 0,
            entries_collected: Vec::new(),
            entries_index: 0,
            _phantom: PhantomData,
        }
    }

    fn collect_bucket_values(&mut self) -> bool {
        let ok = collect_next_bucket(
            &self.table,
            &mut self.bucket_index,
            &mut self.entries_collected,
            |e, _meta, _j| {
                let (_, v) = unsafe { e.unsafe_clone_key_value() };
                Some(v)
            },
        );
        if ok {
            self.entries_index = 0;
        }
        ok
    }
}

impl<K, V, S> Iterator for ValuesIterator<K, V, S>
where
    K: Clone,
    V: Clone,
    S: BuildHasher,
{
    type Item = V;

    fn next(&mut self) -> Option<Self::Item> {
        if self.entries_index < self.entries_collected.len() {
            let value = self.entries_collected[self.entries_index].clone();
            self.entries_index += 1;
            return Some(value);
        }

        if self.collect_bucket_values() {
            self.next()
        } else {
            None
        }
    }
}

/// Iterator over the key-value pairs of a NodeMap
pub struct IterIterator<K, V, S = RandomState>
where
    S: BuildHasher,
{
    table: Table<K, V>,
    bucket_index: usize,
    entries_collected: Vec<(K, V)>,
    entries_index: usize,
    _phantom: PhantomData<S>,
}

impl<K, V, S> IterIterator<K, V, S>
where
    K: Clone,
    V: Clone,
    S: BuildHasher,
{
    fn new(map: &NodeMap<K, V, S>) -> Self {
        Self {
            table: map.table.seq_load(),
            bucket_index: 0,
            entries_collected: Vec::new(),
            entries_index: 0,
            _phantom: PhantomData,
        }
    }

    fn collect_bucket_pairs(&mut self) -> bool {
        let ok = collect_next_bucket(
            &self.table,
            &mut self.bucket_index,
            &mut self.entries_collected,
            |e, _meta, _j| {
                let (k, v) = unsafe { e.unsafe_clone_key_value() };
                Some((k, v))
            },
        );
        if ok {
            self.entries_index = 0;
        }
        ok
    }
}

impl<K, V, S> Iterator for IterIterator<K, V, S>
where
    K: Clone,
    V: Clone,
    S: BuildHasher,
{
    type Item = (K, V);

    fn next(&mut self) -> Option<Self::Item> {
        if self.entries_index < self.entries_collected.len() {
            let pair = self.entries_collected[self.entries_index].clone();
            self.entries_index += 1;
            return Some(pair);
        }

        if self.collect_bucket_pairs() {
            self.next()
        } else {
            None
        }
    }
}
