#![allow(missing_docs)]
use gluex_rs::{GlueX, RunSelection, SourceConfig};
#[path = "fixtures/rust.rs"]
mod fixtures;

#[test]
fn ccdb_only_catalog_and_numeric_series_retain_assignment_inputs() {
    let fixture = fixtures::ccdb();
    let gx = GlueX::open(SourceConfig::Disabled, SourceConfig::sqlite(fixture.path())).unwrap();
    let catalog = gx.calibrations().unwrap();
    assert!(catalog.keys().any(|p| p == "/TARGET/density"));
    let table = catalog.get("/test/demo/mytable").unwrap();
    assert_eq!(table.columns().unwrap()[0].name(), "x");
    let query = table.for_runs(RunSelection::range(2, 3)).unwrap();
    assert_eq!(query.provenance().variation(), "default");
    assert_eq!(
        query.provenance().as_of(),
        gx.sources().ccdb().unwrap().opened_at()
    );
    drop(gx);
    let series = query.collect().unwrap();
    let entry = series.get(2).unwrap();
    assert_eq!(entry.assignment_id(), 230_266);
    assert_eq!(entry.constant_set_id(), 230_302);
    assert_eq!(entry.payload().named_double("x", 0), Some(1.0));
    assert!(series.report().missing_runs().is_empty());
    let missing = catalog
        .get("/TARGET/density")
        .unwrap()
        .for_runs(RunSelection::runs([2, 50685]))
        .unwrap()
        .collect()
        .unwrap();
    assert_eq!(missing.report().missing_runs(), &[2]);
    assert_eq!(
        missing
            .get(50685)
            .unwrap()
            .payload()
            .named_double("density", 0),
        Some(70.92)
    );
}

#[test]
fn calibration_inspection_is_lazy_and_payload_errors_are_not_missing_assignments() {
    let fixture = fixtures::ccdb();
    rusqlite::Connection::open(fixture.path())
        .unwrap()
        .execute(
            "UPDATE constantSets SET vault = 'broken' WHERE id = 230302",
            [],
        )
        .unwrap();
    let gx = GlueX::open(SourceConfig::Disabled, SourceConfig::sqlite(fixture.path())).unwrap();
    let catalog = gx.calibrations().unwrap();
    let table = catalog.get("/test/demo/mytable").unwrap();
    assert_eq!(table.columns().unwrap().len(), 3);
    let query = table
        .for_runs(RunSelection::range(i64::MIN, i64::MAX))
        .unwrap();
    assert_eq!(query.provenance().table(), "/test/demo/mytable");
    assert!(!format!("{query:?}").is_empty());
    assert!(
        table
            .for_runs(RunSelection::runs([2]))
            .unwrap()
            .collect()
            .is_err()
    );
    assert!(table.for_runs(RunSelection::All).is_err());
    assert!(
        table
            .for_runs(RunSelection::range(4, 2))
            .unwrap()
            .collect()
            .unwrap()
            .items()
            .next()
            .is_none()
    );
    let empty = GlueX::open(SourceConfig::Disabled, SourceConfig::Disabled).unwrap();
    assert!(
        empty
            .calibrations()
            .unwrap_err()
            .to_string()
            .contains("CCDB")
    );
}
