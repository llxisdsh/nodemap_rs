use criterion::{black_box, criterion_group, criterion_main, Criterion};
use dashmap::DashMap;
use nodemap_rs::NodeMap;

fn benchmark_nodemap_init_only(c: &mut Criterion) {
    c.bench_function("nodemap_init_only", |b| {
        b.iter(|| {
            let map: NodeMap<i32, i32> = black_box(NodeMap::new());
            // 立即丢弃，只测量初始化开销
            drop(map);
        })
    });
}

fn benchmark_dashmap_init_only(c: &mut Criterion) {
    c.bench_function("dashmap_init_only", |b| {
        b.iter(|| {
            let map: DashMap<i32, i32> = black_box(DashMap::new());
            // 立即丢弃，只测量初始化开销
            drop(map);
        })
    });
}

fn benchmark_nodemap_with_capacity_init(c: &mut Criterion) {
    c.bench_function("nodemap_with_capacity_init", |b| {
        b.iter(|| {
            let map: NodeMap<i32, i32> = black_box(NodeMap::with_capacity(100_000));
            drop(map);
        })
    });
}

fn benchmark_dashmap_with_capacity_init(c: &mut Criterion) {
    c.bench_function("dashmap_with_capacity_init", |b| {
        b.iter(|| {
            let map: DashMap<i32, i32> = black_box(DashMap::with_capacity(100_000));
            drop(map);
        })
    });
}

fn benchmark_nodemap_drop_after_insert(c: &mut Criterion) {
    c.bench_function("nodemap_drop_after_insert", |b| {
        b.iter(|| {
            let map: NodeMap<i32, i32> = NodeMap::new();
            // 插入一些数据
            for i in 0..1000 {
                map.insert(i, i * 2);
            }
            // 测量 drop 开销
            black_box(drop(map));
        })
    });
}

fn benchmark_dashmap_drop_after_insert(c: &mut Criterion) {
    c.bench_function("dashmap_drop_after_insert", |b| {
        b.iter(|| {
            let map: DashMap<i32, i32> = DashMap::new();
            // 插入一些数据
            for i in 0..1000 {
                map.insert(i, i * 2);
            }
            // 测量 drop 开销
            black_box(drop(map));
        })
    });
}

fn benchmark_nodemap_full_lifecycle(c: &mut Criterion) {
    c.bench_function("nodemap_full_lifecycle", |b| {
        b.iter(|| {
            let map: NodeMap<i32, i32> = NodeMap::new();
            // 插入数据
            for i in 0..100 {
                map.insert(i, i * 2);
            }
            // 读取数据
            for i in 0..100 {
                black_box(map.get(&i));
            }
            // 自动 drop
        })
    });
}

fn benchmark_dashmap_full_lifecycle(c: &mut Criterion) {
    c.bench_function("dashmap_full_lifecycle", |b| {
        b.iter(|| {
            let map: DashMap<i32, i32> = DashMap::new();
            // 插入数据
            for i in 0..100 {
                map.insert(i, i * 2);
            }
            // 读取数据
            for i in 0..100 {
                black_box(map.get(&i));
            }
            // 自动 drop
        })
    });
}

criterion_group!(
    benches,
    benchmark_nodemap_init_only,
    benchmark_dashmap_init_only,
    benchmark_nodemap_with_capacity_init,
    benchmark_dashmap_with_capacity_init,
    benchmark_nodemap_drop_after_insert,
    benchmark_dashmap_drop_after_insert,
    benchmark_nodemap_full_lifecycle,
    benchmark_dashmap_full_lifecycle
);
criterion_main!(benches);
