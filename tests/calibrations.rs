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

#[test]
fn explicit_historical_selectors_are_immutable_and_include_boundaries() {
    let fixture = fixtures::ccdb();
    let gx = GlueX::open(SourceConfig::Disabled, SourceConfig::sqlite(fixture.path())).unwrap();
    let catalog = gx.calibrations().unwrap();
    let query = catalog
        .get("/test/demo/mytable")
        .unwrap()
        .for_runs(RunSelection::runs([-1, 0, 2, 2_147_483_647, 2_147_483_648]))
        .unwrap();
    let boundary = gluex_rs::parsers::parse_timestamp("2020-01-15 13:08:18").unwrap();
    let historical = query
        .with_variation("mc")
        .as_of(boundary - chrono::Duration::seconds(1));
    let old = historical.collect().unwrap();
    assert_eq!(old.get(0).unwrap().assignment_id(), 76);
    assert_eq!(old.get(2_147_483_647).unwrap().variation(), "default");
    assert_eq!(old.report().missing_runs(), &[-1, 2_147_483_648]);
    assert_eq!(historical.provenance().variation(), "mc");
    assert_eq!(query.provenance().variation(), "default");
    let current = historical.as_of(boundary).collect().unwrap();
    assert_eq!(current.get(2).unwrap().assignment_id(), 230_266);
    assert!(std::ptr::eq(
        current.get(0).unwrap().payload(),
        current.get(2).unwrap().payload()
    ));
    assert!(query.with_variation("missing").collect().is_err());
}

#[test]
fn historical_inheritance_prefers_child_then_highest_eligible_assignment_id() {
    // JeffersonLab CCDB SQLiteDataProvider::GetAssignmentShort orders eligible
    // assignments by id DESC, then tries parents with the same cutoff.
    let fixture = fixtures::ccdb();
    rusqlite::Connection::open(fixture.path())
        .unwrap()
        .execute_batch(
            "
        INSERT INTO variations (id, name, parentId) VALUES (3, 'nested', 2);
        INSERT INTO runRanges (id, runMin, runMax) VALUES (3, 2, 3);
        INSERT INTO assignments (id, created, variationId, runRangeId, constantSetId) VALUES
          (300000, '2014-01-01 00:00:00', 1, 1, 76),
          (300001, '2015-01-01 00:00:00', 2, 3, 230302);
    ",
        )
        .unwrap();
    let gx = GlueX::open(SourceConfig::Disabled, SourceConfig::sqlite(fixture.path())).unwrap();
    let query = gx
        .calibrations()
        .unwrap()
        .get("/test/demo/mytable")
        .unwrap()
        .for_runs(RunSelection::range(1, 4))
        .unwrap()
        .with_variation("nested");
    let result = query.collect().unwrap();
    assert_eq!(result.get(1).unwrap().assignment_id(), 300_000);
    assert_eq!(result.get(2).unwrap().assignment_id(), 300_001);
    assert_eq!(result.get(3).unwrap().variation(), "mc");
    assert_eq!(result.get(4).unwrap().variation(), "default");
    let old = query
        .as_of(gluex_rs::parsers::parse_timestamp("2014-06-01 00:00:00").unwrap())
        .collect()
        .unwrap();
    assert_eq!(old.get(2).unwrap().assignment_id(), 300_000);
}

#[test]
fn malformed_historical_data_is_never_reported_as_missing() {
    for change in [
        "UPDATE assignments SET created = 'not-a-date' WHERE id = 230266",
        "UPDATE assignments SET created = 'garbage 2014 garbage' WHERE id = 230266",
        "UPDATE assignments SET runRangeId = 999 WHERE id = 230266",
        "UPDATE runRanges SET runMin = 9, runMax = 1 WHERE id = 1",
        "UPDATE variations SET parentId = 999 WHERE name = 'mc'",
        "UPDATE variations SET parentId = 2 WHERE name = 'mc'",
        "UPDATE constantSets SET vault = 'broken' WHERE id = 230302",
        "UPDATE columns SET columnType = 'invalid' WHERE id = 641",
    ] {
        let fixture = fixtures::ccdb();
        rusqlite::Connection::open(fixture.path())
            .unwrap()
            .execute_batch(change)
            .unwrap();
        let gx = GlueX::open(SourceConfig::Disabled, SourceConfig::sqlite(fixture.path())).unwrap();
        let query = gx
            .calibrations()
            .unwrap()
            .get("/test/demo/mytable")
            .unwrap()
            .for_runs(RunSelection::runs([2]))
            .unwrap()
            .with_variation("mc");
        let error = query.collect().unwrap_err().to_string();
        assert!(error.contains("/test/demo/mytable"), "{error}");
    }
}

#[test]
fn malformed_boolean_payload_is_an_error() {
    let fixture = fixtures::ccdb();
    rusqlite::Connection::open(fixture.path()).unwrap().execute_batch("UPDATE columns SET columnType = 'bool' WHERE id = 641; UPDATE constantSets SET vault = 'wrong|2|3|true|5|6' WHERE id = 230302;").unwrap();
    let gx = GlueX::open(SourceConfig::Disabled, SourceConfig::sqlite(fixture.path())).unwrap();
    let error = gx
        .calibrations()
        .unwrap()
        .get("/test/demo/mytable")
        .unwrap()
        .for_runs(RunSelection::runs([2]))
        .unwrap()
        .collect()
        .unwrap_err()
        .to_string();
    assert!(
        error.contains("column 0") && error.contains("/test/demo/mytable"),
        "{error}"
    );
}

#[test]
fn historical_cutoffs_preserve_fractional_seconds() {
    let fixture = fixtures::ccdb();
    rusqlite::Connection::open(fixture.path())
        .unwrap()
        .execute_batch(
            "UPDATE assignments SET created = '2020-01-15 13:08:18.500' WHERE id = 230266",
        )
        .unwrap();
    let gx = GlueX::open(SourceConfig::Disabled, SourceConfig::sqlite(fixture.path())).unwrap();
    let query = gx
        .calibrations()
        .unwrap()
        .get("/test/demo/mytable")
        .unwrap()
        .for_runs(RunSelection::runs([2]))
        .unwrap();
    let cutoff = chrono::DateTime::parse_from_rfc3339("2020-01-15T13:08:18.499Z")
        .unwrap()
        .to_utc();
    assert_eq!(
        query
            .as_of(cutoff)
            .collect()
            .unwrap()
            .get(2)
            .unwrap()
            .assignment_id(),
        76
    );
    let exact = cutoff + chrono::Duration::milliseconds(1);
    let result = query.as_of(exact).collect().unwrap();
    assert_eq!(result.get(2).unwrap().assignment_id(), 230_266);
    assert_eq!(result.get(2).unwrap().created(), exact);
}
