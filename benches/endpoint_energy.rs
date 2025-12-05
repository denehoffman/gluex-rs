use ccdb_rs::{context::Context, database::Database};
use criterion::{criterion_group, criterion_main, Criterion};

fn bench_fetch_endpoint_energy(c: &mut Criterion) {
    let db_path = std::env::var("CCDB_BENCH_DB").unwrap_or_else(|_| "ccdb.sqlite".to_string());
    let db = Database::open(&db_path).expect("failed to open database");
    let table = db
        .table("PHOTON_BEAM/endpoint_energy")
        .expect("failed to open endpoint_energy table");
    let ctx = Context::default().with_run_range(30_000..40_000);

    c.bench_function("fetch_endpoint_energy", |b| {
        b.iter(|| {
            let data = table.fetch(&ctx).expect("fetch failed");
            std::hint::black_box(&data);
        })
    });
}

criterion_group!(benches, bench_fetch_endpoint_energy);
criterion_main!(benches);
