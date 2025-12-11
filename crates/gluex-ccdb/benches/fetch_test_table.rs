#![allow(missing_docs)]

use std::time::Duration;

use criterion::{criterion_group, criterion_main, Criterion};
use gluex_ccdb::{context::Context, database::CCDB};

const TABLE_PATH: &str = "/test/demo/mytable";
const DEFAULT_DB: &str = "ccdb.sqlite";

fn open_table() -> gluex_ccdb::database::TypeTableHandle {
    let db_path = std::env::var("CCDB_BENCH_DB").unwrap_or_else(|_| DEFAULT_DB.to_string());
    let db = CCDB::open(&db_path).expect("failed to open database");
    db.table(TABLE_PATH)
        .expect("failed to open benchmark table")
}

fn bench_fetch_range(c: &mut Criterion) {
    let table = open_table();
    let ctx = Context::default().with_run_range(0..=30_000);

    let mut group = c.benchmark_group("fetch_test_table_range");
    group.sample_size(20);
    group.measurement_time(Duration::from_secs(15));
    group.bench_function("run_range_0_30000", |b| {
        b.iter(|| {
            let data = table.fetch(&ctx).expect("fetch failed");
            std::hint::black_box(&data);
        });
    });
    group.finish();
}

fn bench_fetch_single_run(c: &mut Criterion) {
    let table = open_table();
    let ctx = Context::default().with_run(2);

    let mut group = c.benchmark_group("fetch_test_table_single_run");
    group.sample_size(20);
    group.measurement_time(Duration::from_secs(15));
    group.bench_function("single_run_2", |b| {
        b.iter(|| {
            let data = table.fetch(&ctx).expect("fetch failed");
            std::hint::black_box(&data);
        });
    });
    group.finish();
}

criterion_group!(benches, bench_fetch_range, bench_fetch_single_run);
criterion_main!(benches);
