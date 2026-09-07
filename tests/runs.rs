#![allow(missing_docs)]

use gluex_rs::{GlueX, RunSelection, SourceConfig};

#[path = "fixtures/rust.rs"]
mod fixtures;

#[test]
fn numeric_scope_collects_recorded_runs_without_scientific_cuts() {
    let fixture = fixtures::rcdb();
    let gx = GlueX::open(SourceConfig::sqlite(fixture.path()), SourceConfig::Disabled).unwrap();
    let query = gx.runs(RunSelection::runs([5, 2, 1, 5, 3])).unwrap();
    assert!(format!("{query:?}").contains("RunQuery"));
    let result = query.collect().unwrap();
    assert_eq!(result.numbers(), &[2, 3, 5]);
    assert_eq!(result.provenance().selection(), query.selection());
    assert_eq!(
        result.provenance().source(),
        gx.sources().rcdb().unwrap().connection_path()
    );
    assert_eq!(query.collect().unwrap().numbers(), result.numbers());
    drop(gx);
    assert_eq!(query.collect().unwrap().numbers(), &[2, 3, 5]);
}

#[test]
fn catalog_discovers_database_defined_conditions() {
    let fixture = fixtures::rcdb();
    rusqlite::Connection::open(fixture.path()).unwrap().execute(
        "INSERT INTO condition_types (id, name, value_type, description) VALUES (100, 'custom_monitor', 'float', 'Monitor reading')", []
    ).unwrap();
    let gx = GlueX::open(SourceConfig::sqlite(fixture.path()), SourceConfig::Disabled).unwrap();
    let catalog = gx.conditions().unwrap();
    let definition = &catalog["custom_monitor"];
    assert_eq!(definition.name(), "custom_monitor");
    assert_eq!(definition.value_type(), gluex_rs::rcdb::ValueType::Float);
    assert_eq!(definition.description(), "Monitor reading");
    assert!(catalog.keys().any(|key| key == "custom_monitor"));
    assert!(catalog.items().any(|(key, value)| key == value.name()));
    assert!(catalog.get("absent").is_none());
}

#[test]
fn ranges_periods_and_empty_scopes_resolve_only_recorded_membership() {
    let fixture = fixtures::rcdb();
    let gx = GlueX::open(SourceConfig::sqlite(fixture.path()), SourceConfig::Disabled).unwrap();
    for (selection, expected) in [
        (RunSelection::range(2, 4), vec![2, 3, 4]),
        (RunSelection::range(4, 2), vec![]),
        (RunSelection::runs([]), vec![]),
        (
            RunSelection::period(gluex_rs::RunPeriod::RP2018_08),
            vec![50685, 50697],
        ),
        (
            RunSelection::range(i64::MIN, i64::MAX),
            vec![2, 3, 4, 5, 1100, 10204, 50685, 50697],
        ),
        (RunSelection::runs([i64::MAX, i64::MIN, 2]), vec![2]),
    ] {
        assert_eq!(
            gx.runs(selection).unwrap().collect().unwrap().numbers(),
            expected
        );
    }
    let gx = GlueX::open(SourceConfig::Disabled, SourceConfig::Disabled).unwrap();
    assert!(
        gx.runs(RunSelection::range(1, 9))
            .unwrap_err()
            .to_string()
            .contains("RCDB")
    );
    assert!(gx.conditions().is_err());
}

#[test]
fn query_inspection_does_not_decode_recorded_runs() {
    let fixture = fixtures::rcdb();
    rusqlite::Connection::open(fixture.path()).unwrap().execute_batch(
        "DROP TABLE runs; CREATE TABLE runs (number TEXT); INSERT INTO runs VALUES ('invalid');"
    ).unwrap();
    let gx = GlueX::open(SourceConfig::sqlite(fixture.path()), SourceConfig::Disabled).unwrap();
    let query = gx.runs(RunSelection::All).unwrap();
    assert!(format!("{query:?}").contains("RunQuery"));
    assert!(query.provenance().source().contains("sqlite"));
    assert!(query.collect().is_err());
}
