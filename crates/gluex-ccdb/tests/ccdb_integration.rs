#![allow(missing_docs)]

use chrono::{Datelike, Timelike};
use gluex_ccdb::{CCDBResult, context::CCDBContext, database::CCDB, models::ColumnMeta};
use gluex_core::{GlueXCoreError, parsers::parse_timestamp};

#[path = "../../../tests/fixtures/rust.rs"]
mod fixtures;

const TABLE_PATH: &str = "/test/demo/mytable";

fn open_db() -> (fixtures::TestDatabase, CCDB) {
    let fixture = fixtures::ccdb();
    let db = CCDB::open(fixture.path()).expect("failed to open CCDB test database");
    (fixture, db)
}

#[test]
fn parse_timestamp_infers_and_validates_inputs() -> CCDBResult<()> {
    let ts = parse_timestamp("2013-02-22 19:40:35")?;
    assert_eq!((ts.year(), ts.month(), ts.day()), (2013, 2, 22));
    assert_eq!((ts.hour(), ts.minute(), ts.second()), (19, 40, 35));

    let inferred = parse_timestamp("2013-02")?;
    assert_eq!(
        (inferred.year(), inferred.month(), inferred.day()),
        (2013, 2, 28)
    );
    assert_eq!(
        (inferred.hour(), inferred.minute(), inferred.second()),
        (23, 59, 59)
    );

    let full_year = parse_timestamp("2013")?;
    assert_eq!(
        (full_year.year(), full_year.month(), full_year.day()),
        (2013, 12, 31)
    );
    assert_eq!(
        (full_year.hour(), full_year.minute(), full_year.second()),
        (23, 59, 59)
    );

    let err = parse_timestamp("no digits here").unwrap_err();
    assert!(matches!(err, GlueXCoreError::TimestampNoDigits(msg) if msg == "no digits here"));
    Ok(())
}

#[test]
fn directory_and_table_metadata_can_be_discovered() -> CCDBResult<()> {
    let (_fixture, db) = open_db();

    let root = db.root();
    assert_eq!(root.full_path(), "/");

    let test_dir = db.dir("/test")?;
    assert_eq!(test_dir.full_path(), "/test");

    let demo_dir = test_dir.dir("demo")?;
    assert_eq!(demo_dir.full_path(), "/test/demo");

    let table = demo_dir.table("mytable")?;
    assert_eq!(table.full_path(), TABLE_PATH);
    let meta = table.meta();
    assert_eq!(meta.n_rows(), 2);
    assert_eq!(meta.n_columns(), 3);

    let columns = table.columns()?;
    assert_eq!(columns.len(), 3);
    let names: Vec<&str> = columns.iter().map(ColumnMeta::name).collect();
    assert_eq!(names, ["x", "y", "z"]);
    let types: Vec<&str> = columns.iter().map(|c| c.column_type().as_str()).collect();
    assert_eq!(types, ["double", "double", "double"]);
    Ok(())
}

#[test]
fn fetch_respects_runs_variations_and_timestamps() -> CCDBResult<()> {
    let (_fixture, db) = open_db();
    let before_first = parse_timestamp("2013-02-22 19:40:34")?;
    let first_available = parse_timestamp("2013-02-22 19:40:35")?;
    let updated = parse_timestamp("2020-02-01 00:00:00")?;

    let empty_ctx = CCDBContext::default()
        .with_run_range(0..=3)
        .with_timestamp(before_first);
    let empty = db.fetch(TABLE_PATH, &empty_ctx)?;
    assert!(empty.is_empty());

    let no_runs = db.fetch(
        TABLE_PATH,
        &CCDBContext::default()
            .with_runs([])
            .with_timestamp(first_available),
    )?;
    assert!(no_runs.is_empty());

    let first_ctx = CCDBContext::default()
        .with_run_range(0..=3)
        .with_timestamp(first_available);
    let first = db.fetch(TABLE_PATH, &first_ctx)?;
    assert_eq!(
        first.keys().copied().collect::<Vec<_>>(),
        vec![0i64, 1, 2, 3]
    );
    for data in first.values() {
        assert_eq!(data.n_rows(), 2);
        assert_eq!(
            data.column_names()
                .iter()
                .map(String::as_str)
                .collect::<Vec<_>>(),
            vec!["x", "y", "z"]
        );
        assert_eq!(data.named_double("x", 0), Some(0.0));
        assert_eq!(data.named_double("y", 0), Some(1.0));
        assert_eq!(data.named_double("z", 0), Some(2.0));
        assert_eq!(data.named_double("x", 1), Some(3.0));
        assert_eq!(data.named_double("y", 1), Some(4.0));
        assert_eq!(data.named_double("z", 1), Some(5.0));
    }

    let mc_ctx = CCDBContext::default()
        .with_run(2)
        .with_variation("mc")
        .with_timestamp(first_available);
    let mc_result = db.fetch(TABLE_PATH, &mc_ctx)?;
    let mc_data = mc_result.get(&2).expect("missing mc data for run 2");
    assert_eq!(mc_data.named_double("x", 0), Some(0.0));
    assert_eq!(mc_data.named_double("z", 1), Some(5.0));

    let updated_ctx = CCDBContext::default()
        .with_run_range(0..=3)
        .with_timestamp(updated);
    let updated_data = db.fetch(TABLE_PATH, &updated_ctx)?;
    assert_eq!(
        updated_data.keys().copied().collect::<Vec<_>>(),
        vec![0i64, 1, 2, 3]
    );
    for data in updated_data.values() {
        assert_eq!(data.named_double("x", 0), Some(1.0));
        assert_eq!(data.named_double("y", 0), Some(2.0));
        assert_eq!(data.named_double("z", 0), Some(3.0));
        assert_eq!(data.named_double("x", 1), Some(4.0));
        assert_eq!(data.named_double("y", 1), Some(5.0));
        assert_eq!(data.named_double("z", 1), Some(6.0));
    }
    Ok(())
}
