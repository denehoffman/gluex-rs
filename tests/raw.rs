#![allow(missing_docs)]
use gluex_rs::{GlueX, RawValue, SourceConfig};
#[path = "fixtures/rust.rs"]
mod fixtures;

#[test]
fn raw_reads_bind_parameters_and_preserve_sqlite_value_types() {
    let fixture = fixtures::rcdb();
    let gx = GlueX::open(SourceConfig::sqlite(fixture.path()), SourceConfig::Disabled).unwrap();
    let result = gx.sources().rcdb().unwrap().raw(
        "WITH chosen AS (SELECT number FROM runs WHERE number = ?) SELECT number, NULL, 1.5, 'x', x'00ff' FROM chosen",
        &[RawValue::Integer(2)],
    ).unwrap();
    assert_eq!(result.columns()[0].name(), "number");
    assert_eq!(
        result.rows()[0].values(),
        &[
            RawValue::Integer(2),
            RawValue::Null,
            RawValue::Real(1.5),
            RawValue::Text("x".into()),
            RawValue::Blob(vec![0, 255])
        ]
    );
}

#[test]
fn both_sources_reject_mutation_and_restriction_bypasses() {
    let rcdb = fixtures::rcdb();
    let ccdb = fixtures::ccdb();
    let gx = GlueX::open(
        SourceConfig::sqlite(rcdb.path()),
        SourceConfig::sqlite(ccdb.path()),
    )
    .unwrap();
    for ccdb in [false, true] {
        let raw = |sql: &str| {
            if ccdb {
                gx.sources().ccdb().unwrap().raw(sql, &[])
            } else {
                gx.sources().rcdb().unwrap().raw(sql, &[])
            }
        };
        assert_eq!(
            raw("PRAGMA query_only").unwrap().rows()[0].values(),
            &[RawValue::Integer(1)]
        );
        let before = raw("SELECT name, sql FROM sqlite_schema ORDER BY name").unwrap();
        for sql in [
            "PRAGMA query_only=OFF",
            "PRAGMA writable_schema=ON",
            "PRAGMA journal_mode=WAL",
            "ATTACH DATABASE ':memory:' AS other",
            "CREATE TEMP TABLE side_effect (x)",
            "CREATE TABLE side_effect (x)",
            "DROP TABLE side_effect",
            "SELECT 1; SELECT 2",
            "SELECT load_extension('no-extension')",
            "BEGIN",
            "VACUUM",
            "INSERT INTO schema_versions VALUES (99)",
            "DELETE FROM schema_versions",
            "UPDATE schema_versions SET version=99",
        ] {
            assert!(raw(sql).is_err(), "accepted {sql}");
        }
        assert_eq!(
            raw("SELECT name, sql FROM sqlite_schema ORDER BY name")
                .unwrap()
                .rows(),
            before.rows()
        );
        assert_eq!(
            raw("SELECT 42").unwrap().rows()[0].values(),
            &[RawValue::Integer(42)]
        );
    }
}

#[test]
fn schema_inspection_empty_results_and_parameter_errors_are_explicit() {
    let fixture = fixtures::rcdb();
    let gx = GlueX::open(SourceConfig::sqlite(fixture.path()), SourceConfig::Disabled).unwrap();
    let reader = gx.sources().rcdb().unwrap();
    let result = reader
        .raw(
            "SELECT number AS run FROM runs WHERE number = ?",
            &[RawValue::Integer(-1)],
        )
        .unwrap();
    assert!(result.rows().is_empty());
    assert_eq!(result.columns()[0].name(), "run");
    assert_eq!(result.columns()[0].declared_type(), Some("INTEGER"));
    let columns = reader.raw("PRAGMA table_info(runs)", &[]).unwrap();
    assert_eq!(
        columns.rows()[0].values()[1],
        RawValue::Text("number".into())
    );
    assert!(reader.raw("SELECT ?", &[]).is_err());
    assert!(reader.raw("SELECT 1", &[RawValue::Integer(1)]).is_err());
    assert!(reader.raw("SELECT CAST(x'ff' AS TEXT)", &[]).is_err());
    let before = reader
        .raw("SELECT * FROM runs ORDER BY number", &[])
        .unwrap();
    for sql in [
        "DELETE FROM runs",
        "UPDATE runs SET number=9",
        "INSERT INTO runs(number) VALUES (9)",
        "DROP TABLE runs",
        "PRAGMA query_only=0",
    ] {
        assert!(reader.raw(sql, &[]).is_err());
    }
    assert_eq!(
        reader
            .raw("SELECT * FROM runs ORDER BY number", &[])
            .unwrap()
            .rows(),
        before.rows()
    );
    assert_eq!(
        reader.raw("PRAGMA query_only", &[]).unwrap().rows()[0].values(),
        &[RawValue::Integer(1)]
    );
}
