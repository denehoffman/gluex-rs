use std::time::Duration;

use criterion::{criterion_group, criterion_main, Criterion};
use gluex_ccdb::{context::Context, database::CCDB};

fn bench_fetch_endpoint_energy_range(c: &mut Criterion) {
    let db_path = std::env::var("CCDB_BENCH_DB").unwrap_or_else(|_| "ccdb.sqlite".to_string());
    let db = CCDB::open(&db_path).expect("failed to open database");
    let table = db
        .table("PHOTON_BEAM/endpoint_energy")
        .expect("failed to open endpoint_energy table");
    let ctx = Context::default().with_run_range(30_000..40_000);

    let mut group = c.benchmark_group("fetch_endpoint_energy");
    group.sample_size(20);
    group.measurement_time(Duration::from_secs(15));
    group.bench_function("run_range_30000_40000", |b| {
        b.iter(|| {
            let data = table.fetch(&ctx).expect("fetch failed");
            std::hint::black_box(&data);
        })
    });
    group.finish();
}

fn bench_fetch_endpoint_energy_single_run(c: &mut Criterion) {
    let db_path = std::env::var("CCDB_BENCH_DB").unwrap_or_else(|_| "ccdb.sqlite".to_string());
    let db = CCDB::open(&db_path).expect("failed to open database");
    let table = db
        .table("PHOTON_BEAM/endpoint_energy")
        .expect("failed to open endpoint_energy table");
    let ctx = Context::default().with_run(39_877);

    let mut group = c.benchmark_group("fetch_endpoint_energy_single_run");
    group.sample_size(20);
    group.measurement_time(Duration::from_secs(15));
    group.bench_function("single_run_39877", |b| {
        b.iter(|| {
            let data = table.fetch(&ctx).expect("fetch failed");
            std::hint::black_box(&data);
        })
    });
    group.finish();
}

criterion_group!(
    benches,
    bench_fetch_endpoint_energy_range,
    bench_fetch_endpoint_energy_single_run
);
criterion_main!(benches);
