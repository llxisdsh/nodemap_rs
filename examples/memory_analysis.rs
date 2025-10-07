use std::mem;

// 复制相关常量和函数
const ENTRIES_PER_BUCKET: usize = 7;
const LOAD_FACTOR: f64 = 0.75;
const MIN_TABLE_LEN: usize = 32;

fn next_pow2(mut n: usize) -> usize {
    if n == 0 {
        return 1;
    }
    n -= 1;
    n |= n >> 1;
    n |= n >> 2;
    n |= n >> 4;
    n |= n >> 8;
    n |= n >> 16;
    n |= n >> 32;
    n + 1
}

fn calc_table_len(size_hint: usize) -> usize {
    let min_cap = (size_hint as f64 * (1.0 / (ENTRIES_PER_BUCKET as f64 * LOAD_FACTOR))) as usize;
    let base = min_cap.max(MIN_TABLE_LEN);
    next_pow2(base)
}

fn calc_size_len(table_len: usize, cpus: usize) -> usize {
    next_pow2(cpus.min(table_len >> 10))
}

// 模拟 Bucket 结构
#[repr(align(8))]
struct MockBucket {
    meta: u64,    // AtomicU64
    seq: u64,     // AtomicU64  
    entries: [MockEntry; ENTRIES_PER_BUCKET], // UnsafeCell<[Entry<K, V>; ENTRIES_PER_BUCKET]>
    next: *mut MockBucket, // AtomicPtr<Bucket<K, V>>
}

struct MockEntry {
    hash: u64,
    key: i32,    // 模拟 i32 key
    val: i32,    // 模拟 i32 value
}

fn main() {
    let capacity = 100_000;
    let cpus = num_cpus::get();
    
    println!("=== NodeMap Memory Analysis for capacity {} ===", capacity);
    println!("CPU count: {}", cpus);
    
    let table_len = calc_table_len(capacity);
    let size_len = calc_size_len(table_len, cpus);
    
    println!("\nTable calculations:");
    println!("  min_cap = {} / ({} * {}) = {}", 
             capacity, ENTRIES_PER_BUCKET, LOAD_FACTOR, 
             (capacity as f64 / (ENTRIES_PER_BUCKET as f64 * LOAD_FACTOR)) as usize);
    println!("  table_len = next_pow2({}) = {}", 
             (capacity as f64 / (ENTRIES_PER_BUCKET as f64 * LOAD_FACTOR)) as usize, table_len);
    println!("  size_len = next_pow2(min({}, {})) = {}", cpus, table_len >> 10, size_len);
    
    // 计算内存使用
    let bucket_size = mem::size_of::<MockBucket>();
    let entry_size = mem::size_of::<MockEntry>();
    let atomic_usize_size = mem::size_of::<std::sync::atomic::AtomicUsize>();
    
    println!("\nMemory layout:");
    println!("  Bucket size: {} bytes", bucket_size);
    println!("  Entry size: {} bytes", entry_size);
    println!("  AtomicUsize size: {} bytes", atomic_usize_size);
    
    let buckets_memory = table_len * bucket_size;
    let size_memory = size_len * atomic_usize_size;
    let total_memory = buckets_memory + size_memory;
    
    println!("\nMemory allocation:");
    println!("  Buckets: {} * {} = {} bytes ({:.2} MB)", 
             table_len, bucket_size, buckets_memory, buckets_memory as f64 / 1024.0 / 1024.0);
    println!("  Size array: {} * {} = {} bytes", 
             size_len, atomic_usize_size, size_memory);
    println!("  Total: {} bytes ({:.2} MB)", total_memory, total_memory as f64 / 1024.0 / 1024.0);
    
    // 分析 alloc_zeroed 的影响
    println!("\nalloc_zeroed analysis:");
    println!("  Zero-initializing {:.2} MB of memory", buckets_memory as f64 / 1024.0 / 1024.0);
    println!("  This includes {} buckets, each with {} entries", table_len, ENTRIES_PER_BUCKET);
    println!("  Total entries to zero: {} entries", table_len * ENTRIES_PER_BUCKET);
    
    // 对比 DashMap 的可能内存使用
    println!("\nComparison with DashMap:");
    println!("  DashMap likely uses lazy allocation or smaller initial size");
    println!("  NodeMap pre-allocates all {} buckets upfront", table_len);
}