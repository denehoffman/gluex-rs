//! Criterion benchmarks for RCDB fetch queries.
use std::{hint::black_box, path::PathBuf, time::Duration};

use criterion::{criterion_group, criterion_main, Criterion};
use gluex_core::run_periods::RunPeriod;
use gluex_rcdb::prelude::*;

fn rcdb_path() -> PathBuf {
    if let Ok(path) = std::env::var("RCDB_BENCH_CONNECTION") {
        let candidate = PathBuf::from(&path);
        if candidate.is_absolute() || candidate.exists() {
            return candidate;
        }
    }
    PathBuf::from(env!("CARGO_MANIFEST_DIR")).join("../../rcdb.sqlite")
}

fn bench_polarimeter_fetch(c: &mut Criterion) {
    let conn_path = rcdb_path();
    let rcdb = RCDB::open(&conn_path).expect("failed to open RCDB benchmark database");
    let run_period = RunPeriod::RP2018_08;
    let start_run = run_period.min_run();
    let end_run = start_run + 500;
    let context = gluex_rcdb::context::Context::default()
        .with_run_range(start_run..=end_run)
        .filter(gluex_rcdb::conditions::aliases::approved_production(
            run_period,
        ));

    c.bench_function("rcdb_fetch/polarimeter_converter_rp2018_08", |b| {
        let rcdb = rcdb.clone();
        let context = context.clone();
        b.iter(|| {
            let values = rcdb
                .fetch(["polarimeter_converter"], &context)
                .expect("rcdb fetch failed");
            black_box(values)
        });
    });
}

criterion_group! {
    name = rcdb_fetch_benches;
    config = Criterion::default()
        .sample_size(10)
        .measurement_time(Duration::from_secs(2));
    targets = bench_polarimeter_fetch
}
criterion_main!(rcdb_fetch_benches);
