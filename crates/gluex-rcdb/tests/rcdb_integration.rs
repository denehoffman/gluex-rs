use std::path::PathBuf;

use gluex_core::parsers::parse_timestamp;
use gluex_rcdb::{Context, RCDBResult, ValueType, RCDB};

fn rcdb_path() -> PathBuf {
    let raw = std::env::var("RCDB_TEST_SQLITE_CONNECTION")
        .expect("set RCDB_TEST_SQLITE_CONNECTION to a RCDB SQLite file");
    let manifest_dir = PathBuf::from(env!("CARGO_MANIFEST_DIR"));
    let cwd_path = PathBuf::from(&raw);
    if cwd_path.is_absolute() || cwd_path.exists() {
        return cwd_path;
    }
    let workspace_path = manifest_dir.join("..").join("..").join(&raw);
    if workspace_path.exists() {
        return workspace_path;
    }
    cwd_path
}

fn open_db() -> RCDB {
    RCDB::open(rcdb_path()).expect("failed to open RCDB test database")
}

#[test]
fn fetch_single_run_int_condition() -> RCDBResult<()> {
    let db = open_db();
    let values = db.fetch("event_count", &Context::default().with_run(2))?;
    let value = values.get(&2).expect("missing event_count run 2");
    assert_eq!(value.value_type(), ValueType::Int);
    assert_eq!(value.as_int(), Some(2));
    Ok(())
}

#[test]
fn fetch_run_range_collects_multiple_rows() -> RCDBResult<()> {
    let db = open_db();
    let ctx = Context::default().with_run_range(2..=5);
    let values = db.fetch("event_count", &ctx)?;
    assert_eq!(values.len(), 4);
    assert_eq!(values.get(&3).and_then(|v| v.as_int()), Some(1686));
    assert!(values.contains_key(&5));
    Ok(())
}

#[test]
fn fetch_bool_condition() -> RCDBResult<()> {
    let db = open_db();
    let ctx = Context::default().with_runs([2, 3, 4]);
    let values = db.fetch("is_valid_run_end", &ctx)?;
    assert_eq!(values.get(&2).and_then(|v| v.as_bool()), Some(false));
    assert_eq!(values.get(&4).and_then(|v| v.as_bool()), Some(true));
    Ok(())
}

#[test]
fn fetch_time_condition() -> RCDBResult<()> {
    let db = open_db();
    let ctx = Context::default().with_run(2);
    let values = db.fetch("run_start_time", &ctx)?;
    let value = values.get(&2).expect("missing run_start_time");
    let expected = parse_timestamp("2015-12-08 15:47:20")?;
    assert_eq!(value.value_type(), ValueType::Time);
    assert_eq!(value.as_time(), Some(expected));
    Ok(())
}
