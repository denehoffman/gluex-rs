#![allow(missing_docs)]

use std::{thread, time::Duration};

use gluex_rs::{CancellationToken, ExecutionOptions, GlueX, RawValue, RunSelection, SourceConfig};

#[path = "fixtures/rust.rs"]
mod fixtures;

const EXPENSIVE_READ: &str = "
    WITH RECURSIVE values_(n) AS (
        SELECT 0
        UNION ALL
        SELECT n + 1 FROM values_ WHERE n < 100000000
    )
    SELECT sum(n) FROM values_
";

#[test]
fn raw_timeout_interrupts_work_and_releases_the_reader() {
    let rcdb = gluex_rs::rcdb::RCDB::open(fixtures::rcdb().path()).unwrap();
    let options = ExecutionOptions::timeout(Duration::ZERO);

    let error = rcdb
        .raw_with_options(EXPENSIVE_READ, &[], &options)
        .expect_err("an expired timeout must interrupt SQLite");
    assert!(error.to_string().contains("interrupted"));

    let result = rcdb.raw("SELECT ?", &[RawValue::Integer(42)]).unwrap();
    assert_eq!(result.rows()[0].values(), &[RawValue::Integer(42)]);
}

#[test]
fn explicit_cancellation_interrupts_before_execution() {
    let rcdb = gluex_rs::rcdb::RCDB::open(fixtures::rcdb().path()).unwrap();
    let token = CancellationToken::new();
    token.cancel();

    let error = rcdb
        .raw_with_options(EXPENSIVE_READ, &[], &ExecutionOptions::cancellable(token))
        .expect_err("a cancelled request must not execute");
    assert!(error.to_string().contains("interrupted"));
}

#[test]
fn domain_queries_honor_expired_deadlines_without_partial_results() {
    let rcdb = fixtures::rcdb();
    let ccdb = fixtures::ccdb();
    let gx = GlueX::open(
        SourceConfig::sqlite(rcdb.path()),
        SourceConfig::sqlite(ccdb.path()),
    )
    .unwrap();

    let run_error = gx
        .runs(RunSelection::range(50_000, 60_000))
        .unwrap()
        .with_timeout(Duration::ZERO)
        .collect()
        .expect_err("run evaluation must honor its deadline");
    assert!(run_error.to_string().contains("interrupted"));

    let calibration_error = gx
        .calibrations()
        .unwrap()
        .get("/TARGET/density")
        .unwrap()
        .for_runs(RunSelection::runs([50_685]))
        .unwrap()
        .with_timeout(Duration::ZERO)
        .collect()
        .expect_err("calibration evaluation must honor its deadline");
    assert!(calibration_error.to_string().contains("interrupted"));
}

#[test]
fn cancellation_interrupts_in_flight_run_evaluation_and_releases_the_reader() {
    let rcdb = fixtures::rcdb();
    rusqlite::Connection::open(rcdb.path())
        .unwrap()
        .execute_batch(
            "WITH RECURSIVE numbers(n) AS (
                 SELECT 200000 UNION ALL SELECT n + 1 FROM numbers WHERE n < 500000
             ) INSERT INTO runs(number) SELECT n FROM numbers;",
        )
        .unwrap();
    let gx = GlueX::open(SourceConfig::sqlite(rcdb.path()), SourceConfig::Disabled).unwrap();
    let token = CancellationToken::new();
    let canceller = token.clone();
    let thread = thread::spawn(move || {
        thread::sleep(Duration::from_millis(2));
        canceller.cancel();
    });

    let error = gx
        .runs(RunSelection::range(200_000, 500_000))
        .unwrap()
        .with_cancellation(token)
        .count()
        .expect_err("cancellation must interrupt an active domain query");
    thread.join().unwrap();
    assert!(error.to_string().contains("interrupted"));
    assert_eq!(
        gx.runs(RunSelection::runs([2])).unwrap().count().unwrap(),
        1
    );
}

#[test]
fn cancellation_interrupts_in_flight_calibration_resolution() {
    let ccdb = fixtures::ccdb();
    let gx = GlueX::open(SourceConfig::Disabled, SourceConfig::sqlite(ccdb.path())).unwrap();
    let token = CancellationToken::new();
    let canceller = token.clone();
    let thread = thread::spawn(move || {
        thread::sleep(Duration::from_millis(2));
        canceller.cancel();
    });

    let error = gx
        .calibrations()
        .unwrap()
        .get("/test/demo/mytable")
        .unwrap()
        .for_runs(RunSelection::range(0, 500_000))
        .unwrap()
        .with_cancellation(token)
        .collect()
        .expect_err("cancellation must interrupt active assignment resolution");
    thread.join().unwrap();
    assert!(error.to_string().contains("interrupted"));
    assert_eq!(
        gx.calibrations()
            .unwrap()
            .get("/test/demo/mytable")
            .unwrap()
            .for_runs(RunSelection::runs([2]))
            .unwrap()
            .count()
            .unwrap(),
        1
    );
}
