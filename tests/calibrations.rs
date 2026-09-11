#![allow(missing_docs)]
use gluex_rs::{
    GlueX, RunSelection, SourceConfig,
    ccdb::{CCDB, CCDBContext},
};
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
fn assignment_oracles_cover_intervals_cutoffs_and_lower_level_equivalence() {
    let fixture = fixtures::ccdb();
    rusqlite::Connection::open(fixture.path())
        .unwrap()
        .execute_batch(
            "
        DELETE FROM assignments WHERE constantSetId IN (76, 230302);
        INSERT INTO runRanges (id, runMin, runMax) VALUES
          (10, 10, 20),
          (11, 15, 25),
          (12, 30, 30);
        INSERT INTO constantSets (id, vault, constantTypeId) VALUES
          (300000, '0|0|0|0|0|0', 81),
          (300001, '1|1|1|1|1|1', 81),
          (300002, '2|2|2|2|2|2', 81),
          (300003, '3|3|3|3|3|3', 81),
          (300004, '4|4|4|4|4|4', 81);
        INSERT INTO assignments (id, created, variationId, runRangeId, constantSetId) VALUES
          (300000, '2019-12-31 23:59:59', 1, 10, 300000),
          (300001, '2020-01-01 00:00:00', 1, 10, 300001),
          (300002, '2020-01-01 00:00:00', 1, 11, 300002),
          (300003, '2020-01-01 00:00:01', 1, 10, 300003),
          (300004, '2020-01-01 00:00:00', 1, 12, 300004);
    ",
        )
        .unwrap();
    let runs = [9, 10, 14, 15, 20, 21, 25, 26, 30, 31];
    let cutoff = gluex_rs::parsers::parse_timestamp("2020-01-01 00:00:00").unwrap();
    let gx = GlueX::open(SourceConfig::Disabled, SourceConfig::sqlite(fixture.path())).unwrap();
    let series = gx
        .calibrations()
        .unwrap()
        .get("/test/demo/mytable")
        .unwrap()
        .for_runs(RunSelection::runs(runs))
        .unwrap()
        .as_of(cutoff)
        .collect()
        .unwrap();

    assert_eq!(series.get(10).unwrap().assignment_id(), 300_001);
    assert_eq!(series.get(14).unwrap().assignment_id(), 300_001);
    assert_eq!(series.get(15).unwrap().assignment_id(), 300_002);
    assert_eq!(series.get(20).unwrap().assignment_id(), 300_002);
    assert_eq!(series.get(21).unwrap().assignment_id(), 300_002);
    assert_eq!(series.get(25).unwrap().assignment_id(), 300_002);
    assert_eq!(series.get(30).unwrap().assignment_id(), 300_004);
    assert_eq!(series.report().missing_runs(), &[9, 26, 31]);

    let lower = CCDB::open(fixture.path())
        .unwrap()
        .fetch(
            "/test/demo/mytable",
            &CCDBContext::default()
                .with_runs(runs)
                .with_timestamp(cutoff),
        )
        .unwrap();
    assert_eq!(
        lower.keys().copied().collect::<Vec<_>>(),
        series.items().map(|(run, _)| *run).collect::<Vec<_>>()
    );
    for (run, entry) in series.items() {
        assert_eq!(
            lower.get(run).unwrap().named_double("x", 0),
            entry.payload().named_double("x", 0)
        );
    }

    let before = gx
        .calibrations()
        .unwrap()
        .get("/test/demo/mytable")
        .unwrap()
        .for_runs(RunSelection::runs([10, 15, 20, 21]))
        .unwrap()
        .as_of(cutoff - chrono::Duration::seconds(1))
        .collect()
        .unwrap();
    assert_eq!(before.get(10).unwrap().assignment_id(), 300_000);
    assert_eq!(before.get(15).unwrap().assignment_id(), 300_000);
    assert_eq!(before.get(20).unwrap().assignment_id(), 300_000);
    assert_eq!(before.report().missing_runs(), &[21]);
}

#[test]
fn malformed_candidate_timestamps_are_validated_when_loaded() {
    let fixture = fixtures::ccdb();
    rusqlite::Connection::open(fixture.path())
        .unwrap()
        .execute_batch(
            "
        INSERT INTO runRanges (id, runMin, runMax) VALUES (10, 5, 5);
        UPDATE assignments
        SET created = 'not-a-date', runRangeId = 10
        WHERE id = 230266;
    ",
        )
        .unwrap();
    let gx = GlueX::open(SourceConfig::Disabled, SourceConfig::sqlite(fixture.path())).unwrap();
    let error = gx
        .calibrations()
        .unwrap()
        .get("/test/demo/mytable")
        .unwrap()
        .for_runs(RunSelection::runs([1, 10]))
        .unwrap()
        .collect()
        .unwrap_err()
        .to_string();
    assert!(error.contains("assignment 230266"), "{error}");
}

#[test]
fn malformed_historical_data_is_never_reported_as_missing() {
    for change in [
        "UPDATE assignments SET created = 'not-a-date' WHERE id = 230266",
        "UPDATE assignments SET created = 'garbage 2014 garbage' WHERE id = 230266",
        "UPDATE assignments SET runRangeId = 999 WHERE id = 230266",
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
        assert!(query.strict().stream(1).unwrap().next().unwrap().is_err());
        assert!(
            query
                .fallback_to(2)
                .stream(1)
                .unwrap()
                .next()
                .unwrap()
                .is_err()
        );
    }
}

#[test]
fn reversed_run_ranges_are_ignored_as_empty_intervals() {
    let fixture = fixtures::ccdb();
    rusqlite::Connection::open(fixture.path())
        .unwrap()
        .execute_batch(
            "
        INSERT INTO runRanges (id, runMin, runMax) VALUES (10, 100900, 100899);
        INSERT INTO assignments (id, created, variationId, runRangeId, constantSetId)
        VALUES (300000, '2024-10-18 12:27:33', 1, 10, 230302);
    ",
        )
        .unwrap();
    let gx = GlueX::open(SourceConfig::Disabled, SourceConfig::sqlite(fixture.path())).unwrap();
    let series = gx
        .calibrations()
        .unwrap()
        .get("/test/demo/mytable")
        .unwrap()
        .for_runs(RunSelection::runs([2]))
        .unwrap()
        .collect()
        .unwrap();
    assert_eq!(series.get(2).unwrap().assignment_id(), 230_266);
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

#[test]
fn run_queries_compose_with_period_specific_reconstruction() {
    let rcdb = fixtures::rcdb();
    let ccdb = fixtures::ccdb();
    let gx = GlueX::open(
        SourceConfig::sqlite(rcdb.path()),
        SourceConfig::sqlite(ccdb.path()),
    )
    .unwrap();
    let runs = gx.runs(RunSelection::range(50_685, 50_697)).unwrap();
    let reconstruction = gluex_rs::ReconstructionSelection::periods([(
        gluex_rs::RunPeriod::RP2018_08,
        gluex_rs::RESTVersionSelection::try_new(gluex_rs::RunPeriod::RP2018_08, 2).unwrap(),
    )]);
    let query = gx
        .calibrations()
        .unwrap()
        .get("/TARGET/density")
        .unwrap()
        .for_query(&runs)
        .unwrap()
        .with_reconstruction(reconstruction);
    let series = query.collect().unwrap();
    assert_eq!(
        series.items().map(|(run, _)| *run).collect::<Vec<_>>(),
        [50_685, 50_697]
    );
    assert!(series.provenance().runs().is_some());
    assert_eq!(series.provenance().resolved_reconstruction().len(), 1);
    assert!(
        query
            .with_variation("default")
            .collect()
            .unwrap_err()
            .to_string()
            .contains("conflict")
    );
    let resolved_runs = runs.collect().unwrap();
    let latest = gx
        .calibrations()
        .unwrap()
        .get("/TARGET/density")
        .unwrap()
        .for_run_set(&resolved_runs)
        .unwrap()
        .with_reconstruction(gluex_rs::ReconstructionSelection::latest())
        .collect()
        .unwrap();
    assert_eq!(latest.provenance().resolved_reconstruction().len(), 1);

    let valid_end = gx.conditions().unwrap()["is_valid_run_end"]
        .eq(true)
        .unwrap();
    let filtered = gx
        .runs(RunSelection::range(2, 4))
        .unwrap()
        .where_predicate(valid_end);
    let composed = gx
        .calibrations()
        .unwrap()
        .get("/test/demo/mytable")
        .unwrap()
        .for_query(&filtered)
        .unwrap()
        .collect()
        .unwrap();
    assert_eq!(
        composed.provenance().run_report().unwrap().unknown_runs(),
        &[3]
    );
}

#[test]
fn calibration_streams_share_payloads_and_apply_missing_policies() {
    let fixture = fixtures::ccdb();
    let gx = GlueX::open(SourceConfig::Disabled, SourceConfig::sqlite(fixture.path())).unwrap();
    let table = gx
        .calibrations()
        .unwrap()
        .get("/test/demo/mytable")
        .unwrap()
        .clone();
    let query = table.for_runs(RunSelection::range(1, 4)).unwrap();
    let chunks = query
        .stream(2)
        .unwrap()
        .collect::<Result<Vec<_>, _>>()
        .unwrap();
    assert_eq!(chunks.len(), 2);
    assert!(std::ptr::eq(
        chunks[0].get(1).unwrap().payload(),
        chunks[1].get(3).unwrap().payload()
    ));
    assert_eq!(query.count().unwrap(), 4);
    assert_eq!(query.first().unwrap().unwrap().items().len(), 1);
    let mut abandoned = query.stream(1).unwrap();
    assert!(abandoned.next().unwrap().is_ok());
    drop(abandoned);
    assert_eq!(query.count().unwrap(), 4);

    let missing = gx
        .calibrations()
        .unwrap()
        .get("/TARGET/density")
        .unwrap()
        .for_runs(RunSelection::runs([2, 50_685]))
        .unwrap();
    assert!(
        missing
            .strict()
            .collect()
            .unwrap_err()
            .to_string()
            .contains("missing")
    );
    let filled = missing.fallback_to(50_685).collect().unwrap();
    assert_eq!(
        filled.get(2).unwrap().constant_set_id(),
        filled.get(50_685).unwrap().constant_set_id()
    );
    assert_eq!(filled.report().substitutions(), &[(2, 50_685)]);
    assert_eq!(
        filled.provenance().policy(),
        gluex_rs::MissingDataPolicy::Fallback
    );
    assert_eq!(filled.provenance().fallback_run(), Some(50_685));
}
