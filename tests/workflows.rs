#![allow(missing_docs)]

use approx::assert_relative_eq;
use gluex_rs::{
    CancellationToken, GlueX, RESTVersionSelection, ReconstructionSelection, RunPeriod,
    RunSelection, SourceConfig, WorkflowError,
    lumi::{AVOGADRO_CONSTANT, TARGET_LENGTH_CM},
};

#[path = "fixtures/rust.rs"]
mod fixtures;

const TAGGED_FLUX: f64 = 690_176_020.926_555_9;
const TAGGED_LUMINOSITY: f64 = 0.000_869_563_919_913_552_8;

fn reconstruction() -> ReconstructionSelection {
    ReconstructionSelection::periods([(
        RunPeriod::RP2018_08,
        RESTVersionSelection::try_new(RunPeriod::RP2018_08, 2).unwrap(),
    )])
}

#[test]
fn root_workflow_uses_the_supplied_run_set_without_hidden_approval() {
    let rcdb = fixtures::rcdb();
    let ccdb = fixtures::ccdb();
    rusqlite::Connection::open(rcdb.path())
        .unwrap()
        .execute(
            "UPDATE conditions SET int_value = 0 WHERE run_number = 50685 AND condition_type_id = 13",
            [],
        )
        .unwrap();
    let gx = GlueX::open(
        SourceConfig::sqlite(rcdb.path()),
        SourceConfig::sqlite(ccdb.path()),
    )
    .unwrap();
    let runs = gx
        .runs(RunSelection::runs([50_685]))
        .unwrap()
        .collect()
        .unwrap();

    let result = gx
        .workflows()
        .luminosity(&runs, reconstruction(), [8.0, 8.5, 9.0])
        .collect()
        .unwrap();

    assert_relative_eq!(result.histograms().tagged_flux.counts()[1], TAGGED_FLUX);
    assert_relative_eq!(
        result.histograms().tagged_luminosity.counts()[1],
        TAGGED_LUMINOSITY
    );
    assert_eq!(result.report().selected_runs(), &[50_685]);
    assert_eq!(result.report().used_runs(), &[50_685]);
    assert!(result.report().excluded_runs().is_empty());
    assert_eq!(
        result.provenance().procedure_version(),
        "gluex-luminosity-v1"
    );
    assert_eq!(result.provenance().resolved_reconstruction().len(), 1);
    assert!(!result.provenance().coherent_peak());
    assert!(!result.provenance().polarized());
    assert_eq!(
        result.provenance().rcdb_source(),
        rcdb.path().to_string_lossy()
    );
    assert_eq!(
        result.provenance().ccdb_source(),
        ccdb.path().to_string_lossy()
    );
    let context = &result.provenance().resolved_reconstruction()[&RunPeriod::RP2018_08];
    assert_eq!(context.variation, "default");
}

#[test]
fn multi_run_workflow_aggregates_independent_run_results() {
    let rcdb = fixtures::rcdb();
    let ccdb = fixtures::ccdb();
    rusqlite::Connection::open(ccdb.path())
        .unwrap()
        .execute_batch(
            "INSERT INTO runRanges (id, name, runMin, runMax) VALUES (3, 'second run', 50697, 50697);
             INSERT INTO constantSets (id, vault, constantTypeId) VALUES (10009, '141.84|0.70', 706);
             INSERT INTO assignments (id, created, variationId, runRangeId, constantSetId)
             VALUES (10009, '2019-01-01 00:00:00', 1, 3, 10009);",
        )
        .unwrap();
    let gx = GlueX::open(
        SourceConfig::sqlite(rcdb.path()),
        SourceConfig::sqlite(ccdb.path()),
    )
    .unwrap();
    let all = gx
        .runs(RunSelection::runs([50_685, 50_697]))
        .unwrap()
        .collect()
        .unwrap();
    let first = gx
        .runs(RunSelection::runs([50_685]))
        .unwrap()
        .collect()
        .unwrap();
    let second = gx
        .runs(RunSelection::runs([50_697]))
        .unwrap()
        .collect()
        .unwrap();
    let workflows = gx.workflows();
    let combined = workflows
        .luminosity(&all, reconstruction(), [8.0, 8.5, 9.0])
        .collect()
        .unwrap();
    let a = workflows
        .luminosity(&first, reconstruction(), [8.0, 8.5, 9.0])
        .collect()
        .unwrap();
    let b = workflows
        .luminosity(&second, reconstruction(), [8.0, 8.5, 9.0])
        .collect()
        .unwrap();

    for bin in 0..combined.histograms().tagged_flux.bins() {
        assert_relative_eq!(
            combined.histograms().tagged_flux.counts()[bin],
            a.histograms().tagged_flux.counts()[bin] + b.histograms().tagged_flux.counts()[bin]
        );
        assert_relative_eq!(
            combined.histograms().tagged_luminosity.counts()[bin],
            a.histograms().tagged_luminosity.counts()[bin]
                + b.histograms().tagged_luminosity.counts()[bin]
        );
        let centers_per_density = 1e-24 * AVOGADRO_CONSTANT * 1e-3 * TARGET_LENGTH_CM;
        assert_relative_eq!(
            a.histograms().tagged_luminosity.counts()[bin],
            a.histograms().tagged_flux.counts()[bin] * 70.92 * centers_per_density / 1e12
        );
        assert_relative_eq!(
            b.histograms().tagged_luminosity.counts()[bin],
            b.histograms().tagged_flux.counts()[bin] * 141.84 * centers_per_density / 1e12
        );
    }
}

#[test]
fn latest_reconstruction_uses_the_source_opening_timestamp() {
    let rcdb = fixtures::rcdb();
    let ccdb = fixtures::ccdb();
    let gx = GlueX::open(
        SourceConfig::sqlite(rcdb.path()),
        SourceConfig::sqlite(ccdb.path()),
    )
    .unwrap();
    let opened_at = gx.sources().ccdb().unwrap().opened_at();
    let runs = gx
        .runs(RunSelection::runs([50_685]))
        .unwrap()
        .collect()
        .unwrap();

    let result = gx
        .workflows()
        .luminosity(&runs, ReconstructionSelection::latest(), [8.0, 8.5, 9.0])
        .collect()
        .unwrap();

    assert_eq!(
        result.provenance().resolved_reconstruction()[&RunPeriod::RP2018_08].timestamp,
        opened_at
    );
}

#[test]
fn strict_workflow_rejects_missing_or_zero_livetime() {
    let rcdb = fixtures::rcdb();
    let ccdb = fixtures::ccdb();
    rusqlite::Connection::open(ccdb.path())
        .unwrap()
        .execute(
            "UPDATE constantSets SET vault = '0|0.96248|1|0|2|0|3|0' WHERE id = 10007",
            [],
        )
        .unwrap();
    let gx = GlueX::open(
        SourceConfig::sqlite(rcdb.path()),
        SourceConfig::sqlite(ccdb.path()),
    )
    .unwrap();
    let runs = gx
        .runs(RunSelection::runs([50_685]))
        .unwrap()
        .collect()
        .unwrap();

    let error = gx
        .workflows()
        .luminosity(&runs, reconstruction(), [8.0, 8.5, 9.0])
        .collect()
        .unwrap_err();
    assert!(error.to_string().contains("livetime"));
}

#[test]
fn multi_period_workflow_resolves_each_period_and_reports_missing_runs() {
    let rcdb = fixtures::rcdb();
    let ccdb = fixtures::ccdb();
    let gx = GlueX::open(
        SourceConfig::sqlite(rcdb.path()),
        SourceConfig::sqlite(ccdb.path()),
    )
    .unwrap();
    let runs = gx
        .runs(RunSelection::runs([10_204, 50_685]))
        .unwrap()
        .collect()
        .unwrap();
    let reconstruction = ReconstructionSelection::periods([
        (RunPeriod::RP2016_02, RESTVersionSelection::Current),
        (
            RunPeriod::RP2018_08,
            RESTVersionSelection::try_new(RunPeriod::RP2018_08, 2).unwrap(),
        ),
    ]);

    let result = gx
        .workflows()
        .luminosity(&runs, reconstruction, [8.0, 8.5, 9.0])
        .report_missing()
        .collect()
        .unwrap();

    assert_eq!(result.report().selected_runs(), &[10_204, 50_685]);
    assert_eq!(result.report().used_runs(), &[50_685]);
    assert_eq!(result.report().excluded_runs().len(), 1);
    assert_eq!(result.report().excluded_runs()[0].0, 10_204);
    assert_eq!(result.provenance().resolved_reconstruction().len(), 2);
    assert!(
        result
            .provenance()
            .resolved_reconstruction()
            .contains_key(&RunPeriod::RP2016_02)
    );
    assert!(
        result
            .provenance()
            .resolved_reconstruction()
            .contains_key(&RunPeriod::RP2018_08)
    );
    assert_relative_eq!(result.histograms().tagged_flux.counts()[1], TAGGED_FLUX);
}

#[test]
fn workflow_cancellation_and_cache_controls_leave_the_session_reusable() {
    let rcdb = fixtures::rcdb();
    let ccdb = fixtures::ccdb();
    let gx = GlueX::open(
        SourceConfig::sqlite(rcdb.path()),
        SourceConfig::sqlite(ccdb.path()),
    )
    .unwrap();
    assert_eq!(gx.cache_info().calibration_payload_capacity(), 128);
    gx.set_calibration_payload_cache_capacity(0);
    assert_eq!(gx.cache_info().calibration_payload_capacity(), 1);

    let runs = gx
        .runs(RunSelection::runs([50_685]))
        .unwrap()
        .collect()
        .unwrap();
    let token = CancellationToken::new();
    token.cancel();
    let error = gx
        .workflows()
        .luminosity(&runs, reconstruction(), [8.0, 8.5, 9.0])
        .with_cancellation(token)
        .collect()
        .unwrap_err();
    assert!(matches!(error, WorkflowError::Interrupted));

    gx.clear_caches();
    assert_eq!(gx.cache_info().ccdb_metadata_entries(), 0);
    assert!(
        gx.workflows()
            .luminosity(&runs, reconstruction(), [8.0, 8.5, 9.0])
            .collect()
            .is_ok()
    );
}
