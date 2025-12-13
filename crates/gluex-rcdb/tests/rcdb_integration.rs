#![allow(missing_docs)]

use std::path::PathBuf;

use gluex_core::parsers::parse_timestamp;
use gluex_rcdb::prelude::*;

fn rcdb_path() -> PathBuf {
    if let Ok(raw) = std::env::var("RCDB_TEST_SQLITE_CONNECTION") {
        let supplied = PathBuf::from(&raw);
        if supplied.is_absolute() || supplied.exists() {
            return supplied;
        }
        return PathBuf::from(env!("CARGO_MANIFEST_DIR"))
            .join("../../")
            .join(raw);
    }
    PathBuf::from(env!("CARGO_MANIFEST_DIR")).join("../../rcdb.sqlite")
}

fn open_db() -> RCDB {
    RCDB::open(rcdb_path()).expect("failed to open RCDB test database")
}

#[test]
fn fetch_single_run_int_condition() -> RCDBResult<()> {
    let db = open_db();
    let values = db.fetch(["event_count"], &Context::default().with_run(2))?;
    let run_entry = values.get(&2).expect("missing run 2");
    let value = run_entry
        .get("event_count")
        .expect("missing event_count value");
    assert_eq!(value.value_type(), ValueType::Int);
    assert_eq!(value.as_int(), Some(2));
    Ok(())
}

#[test]
fn fetch_run_range_collects_multiple_rows() -> RCDBResult<()> {
    let db = open_db();
    let ctx = Context::default().with_run_range(2..=5);
    let values = db.fetch(["event_count"], &ctx)?;
    assert_eq!(values.len(), 4);
    assert_eq!(
        values
            .get(&3)
            .and_then(|row| row.get("event_count"))
            .and_then(Value::as_int),
        Some(1686),
    );
    assert!(values.contains_key(&5));
    Ok(())
}

#[test]
fn fetch_bool_condition() -> RCDBResult<()> {
    let db = open_db();
    let ctx = Context::default().with_runs([2, 3, 4]);
    let values = db.fetch(["is_valid_run_end"], &ctx)?;
    assert_eq!(
        values
            .get(&2)
            .and_then(|row| row.get("is_valid_run_end"))
            .and_then(Value::as_bool),
        Some(false),
    );
    assert_eq!(
        values
            .get(&4)
            .and_then(|row| row.get("is_valid_run_end"))
            .and_then(Value::as_bool),
        Some(true),
    );
    Ok(())
}

#[test]
fn fetch_time_condition() -> RCDBResult<()> {
    let db = open_db();
    let ctx = Context::default().with_run(2);
    let values = db.fetch(["run_start_time"], &ctx)?;
    let run_entry = values.get(&2).expect("missing run");
    let value = run_entry
        .get("run_start_time")
        .expect("missing run_start_time");
    let expected = parse_timestamp("2015-12-08 15:47:20")?;
    assert_eq!(value.value_type(), ValueType::Time);
    assert_eq!(value.as_time(), Some(expected));
    Ok(())
}

#[test]
fn fetch_with_predicates() -> RCDBResult<()> {
    let db = open_db();
    let ctx = Context::default()
        .with_run_range(1000..=1100)
        .filter(conditions::all([
            conditions::string_cond("run_type").isin([
                "hd_all.tsg",
                "hd_all.tsg-m8",
                "hd_all.tsg-m7",
            ]),
            conditions::float_cond("beam_current").gt(0.1),
            conditions::int_cond("event_count").gt(50),
        ]));
    let values = db.fetch(
        [
            "beam_current",
            "event_count",
            "run_type",
            "collimator_diameter",
        ],
        &ctx,
    )?;
    assert!(!values.is_empty());
    for (run, row) in &values {
        let event_count = row
            .get("event_count")
            .and_then(Value::as_int)
            .expect("event_count missing");
        assert!(event_count > 50, "run {run} failed event_count");
    }
    Ok(())
}

#[test]
fn fetch_runs_with_filters() -> RCDBResult<()> {
    let db = open_db();
    let ctx = Context::default()
        .with_run_range(1000..=1100)
        .filter(conditions::all([
            conditions::float_cond("beam_current").gt(0.1),
            conditions::int_cond("event_count").gt(50),
        ]));
    let runs = db.fetch_runs(&ctx)?;
    assert!(!runs.is_empty());
    assert!(runs.iter().all(|run| (1000..=1100).contains(run)));
    Ok(())
}

#[test]
fn fetch_runs_with_alias() -> RCDBResult<()> {
    let db = open_db();
    let alias_expr = conditions::aliases::is_production();
    let ctx = Context::default()
        .with_run_range(10_000..=10_300)
        .filter(alias_expr);
    let runs = db.fetch_runs(&ctx)?;
    assert!(!runs.is_empty());
    Ok(())
}
