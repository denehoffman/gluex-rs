#![allow(missing_docs)]

use std::{thread, time::Duration};

use gluex_rs::{
    CancellationToken, ExecutionError, ExecutionOptions, GlueX, RawValue, RunSelection,
    SourceConfig,
};

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
    assert!(matches!(
        error,
        gluex_rs::RawError::Execution(ExecutionError::Timeout)
    ));

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
    assert!(matches!(
        error,
        gluex_rs::RawError::Execution(ExecutionError::Cancelled)
    ));
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
    assert_eq!(run_error.execution_error(), Some(ExecutionError::Timeout));

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
    assert_eq!(
        calibration_error.execution_error(),
        Some(ExecutionError::Timeout)
    );
}

#[test]
fn timeout_begins_at_each_terminal_evaluation() {
    let rcdb = fixtures::rcdb();
    let gx = GlueX::open(SourceConfig::sqlite(rcdb.path()), SourceConfig::Disabled).unwrap();
    let query = gx
        .runs(RunSelection::range(2, 5))
        .unwrap()
        .with_timeout(Duration::from_millis(100));

    thread::sleep(Duration::from_millis(150));
    assert_eq!(query.count().unwrap(), 4);
    thread::sleep(Duration::from_millis(150));
    assert_eq!(query.count().unwrap(), 4);
}

#[test]
fn stream_timeout_excludes_consumer_idle_time() {
    let rcdb = fixtures::rcdb();
    let gx = GlueX::open(SourceConfig::sqlite(rcdb.path()), SourceConfig::Disabled).unwrap();
    let query = gx
        .runs(RunSelection::range(2, 5))
        .unwrap()
        .with_timeout(Duration::from_millis(100));
    let mut stream = query.stream(1).unwrap();

    thread::sleep(Duration::from_millis(150));
    assert_eq!(stream.next().unwrap().unwrap().numbers(), &[2]);
    thread::sleep(Duration::from_millis(150));
    assert_eq!(stream.next().unwrap().unwrap().numbers(), &[3]);
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
    assert!(error.to_string().contains("cancelled"));
    assert_eq!(
        gx.runs(RunSelection::runs([2])).unwrap().count().unwrap(),
        1
    );
}

#[test]
fn cancelled_run_and_condition_terminals_share_cleanup_and_leave_reader_reusable() {
    let rcdb = fixtures::rcdb();
    let gx = GlueX::open(SourceConfig::sqlite(rcdb.path()), SourceConfig::Disabled).unwrap();
    let token = CancellationToken::new();
    let run_query = gx
        .runs(RunSelection::range(2, 5))
        .unwrap()
        .with_cancellation(token.clone());
    let condition_query = run_query.select(["event_count"]).unwrap();
    token.cancel();

    assert!(
        run_query
            .collect()
            .unwrap_err()
            .to_string()
            .contains("cancelled")
    );
    assert!(
        run_query
            .first()
            .unwrap_err()
            .to_string()
            .contains("cancelled")
    );
    assert!(
        run_query
            .one()
            .unwrap_err()
            .to_string()
            .contains("cancelled")
    );
    assert!(
        run_query
            .count()
            .unwrap_err()
            .to_string()
            .contains("cancelled")
    );
    let mut run_stream = run_query.stream(1).unwrap();
    assert!(
        run_stream
            .next()
            .unwrap()
            .unwrap_err()
            .to_string()
            .contains("cancelled")
    );
    assert!(run_stream.next().is_none());

    assert!(
        condition_query
            .collect()
            .unwrap_err()
            .to_string()
            .contains("cancelled")
    );
    assert!(
        condition_query
            .first()
            .unwrap_err()
            .to_string()
            .contains("cancelled")
    );
    assert!(
        condition_query
            .one()
            .unwrap_err()
            .to_string()
            .contains("cancelled")
    );
    assert!(
        condition_query
            .count()
            .unwrap_err()
            .to_string()
            .contains("cancelled")
    );
    let mut condition_stream = condition_query.stream(1).unwrap();
    assert!(
        condition_stream
            .next()
            .unwrap()
            .unwrap_err()
            .to_string()
            .contains("cancelled")
    );
    assert!(condition_stream.next().is_none());

    let reusable = gx.runs(RunSelection::range(2, 5)).unwrap();
    let mut abandoned = reusable.select(["event_count"]).unwrap().stream(1).unwrap();
    assert!(abandoned.next().unwrap().is_ok());
    drop(abandoned);
    assert_eq!(reusable.count().unwrap(), 4);
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
    assert!(error.to_string().contains("cancelled"));
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
