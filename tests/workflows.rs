#![allow(missing_docs)]

use approx::assert_relative_eq;
use chrono::{Duration, Utc};
use gluex_rs::{
    CancellationToken, GlueX, RESTVersionSelection, ReconstructionSelection, RunPeriod,
    RunSelection, SourceConfig, WorkflowError,
    lumi::{AVOGADRO_CONSTANT, BERILLIUM_RADIATION_LENGTH_METERS, TARGET_LENGTH_CM},
};
use std::time::Duration as StdDuration;

#[path = "fixtures/rust.rs"]
mod fixtures;

const TAGGED_FLUX: f64 = 690_176_020.926_555_9;
const TAGGED_LUMINOSITY: f64 = 0.000_869_563_919_913_552_8;

fn worked_fixture_tagged_flux() -> f64 {
    // Independent worked reference from the fixture's recorded inputs:
    // E = endpoint * (scaled_low + scaled_high) / 2,
    // A(E) = p0 * (1 - 2*p1/E) in this energy region, and
    // flux = tagged_count * (live/total) * 9 / (7*radiation_lengths) / A(E).
    let endpoint = 11.630_002_5;
    let (p0, p1) = (0.798_871, 3.124_48);
    let acceptance = |energy: f64| p0 * (1.0 - 2.0 * p1 / energy);
    let scale = (0.962_48 / 0.968_2) * 9.0 / (7.0 * (75e-6 / BERILLIUM_RADIATION_LENGTH_METERS));
    let microscope_energy = endpoint * (0.765_084_300_13 + 0.765_866_632_806) / 2.0;
    let hodoscope_energy = endpoint * (0.770_565 + 0.772_153) / 2.0;
    1_905.67 * scale / acceptance(microscope_energy)
        + 25_885.5 * scale / acceptance(hodoscope_energy)
}

fn reconstruction() -> ReconstructionSelection {
    ReconstructionSelection::periods([(
        RunPeriod::RP2018_08,
        RESTVersionSelection::try_new(RunPeriod::RP2018_08, 2).unwrap(),
    )])
}

fn fall_2019_reconstruction() -> ReconstructionSelection {
    ReconstructionSelection::periods([(
        RunPeriod::RP2019_11,
        RESTVersionSelection::try_new(RunPeriod::RP2019_11, 1).unwrap(),
    )])
}

#[test]
fn root_workflow_uses_the_supplied_run_set_without_hidden_approval() {
    assert_relative_eq!(worked_fixture_tagged_flux(), TAGGED_FLUX);
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
    assert_eq!(result.provenance().procedure_status(), "canonical");
    assert!(result.provenance().references().iter().any(|reference| {
        reference.contains("RevModPhys.46.815") && reference.contains("pair production")
    }));
    assert!(result.provenance().assumptions().iter().any(|assumption| {
        assumption.contains("target length") && assumption.contains("29.5 cm")
    }));
    assert!(
        result
            .provenance()
            .references()
            .iter()
            .any(|reference| reference.contains("/PHOTON_BEAM/coherent_energy"))
    );
    assert!(
        result
            .provenance()
            .validation_gaps()
            .iter()
            .any(|gap| { gap.contains("REST") && gap.contains("reference") })
    );
    assert!(result.provenance().exceptions().iter().any(|exception| {
        exception.contains("72436") && exception.contains("2021-04-23T00:00:01Z")
    }));
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
fn luminosity_uses_the_calibrated_endpoint_to_scale_tagger_energy() {
    let rcdb = fixtures::rcdb();
    let ccdb = fixtures::ccdb();
    rusqlite::Connection::open(ccdb.path())
        .unwrap()
        .execute_batch(
            "INSERT INTO constantSets (id, vault, constantTypeId) VALUES (10009, '10.0', 662);
             INSERT INTO assignments (id, created, variationId, runRangeId, constantSetId)
             VALUES (10009, '2019-01-01 00:00:00', 1, 2, 10009);",
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
        .luminosity(&runs, reconstruction(), [9.0, 10.0, 11.0])
        .collect()
        .unwrap();

    // Independent values from the established plot_flux_ccdb.py procedure for
    // this fixture: TAGM E=9.28475716468 GeV and TAGH E=9.3435925 GeV.
    assert_relative_eq!(
        result.histograms().tagged_flux.counts()[0],
        632_061_337.838_105_6
    );
    assert_eq!(result.histograms().tagged_flux.counts()[1], 0.0);
    assert_relative_eq!(
        result.histograms().tagm_flux.counts()[0],
        43_863_901.505_601_33
    );
    assert_relative_eq!(
        result.histograms().tagh_flux.counts()[0],
        588_197_436.332_504_3
    );
}

#[test]
fn fall_2019_override_changes_energy_bin_at_run_72436() {
    let rcdb = fixtures::rcdb();
    let ccdb = fixtures::ccdb();
    rusqlite::Connection::open(rcdb.path())
        .unwrap()
        .execute_batch(
            "INSERT INTO runs (number) VALUES (72435), (72436);
             INSERT INTO conditions
                 (id, text_value, int_value, float_value, bool_value, run_number, condition_type_id)
             VALUES
                 (1001, 'Be 75um', 0, 0, 0, 72435, 33),
                 (1002, 'Be 75um', 0, 0, 0, 72436, 33);",
        )
        .unwrap();
    rusqlite::Connection::open(ccdb.path())
        .unwrap()
        .execute_batch(
            "UPDATE runRanges SET runMax = 72436 WHERE id = 2;
             INSERT INTO runRanges (id, name, runMin, runMax)
             VALUES (4, '2019-11 override boundary', 72436, 72436);
             INSERT INTO constantSets (id, vault, constantTypeId) VALUES
                 (10009, '10.0', 662),
                 (10010, '9.0', 662);
             INSERT INTO assignments (id, created, variationId, runRangeId, constantSetId) VALUES
                 (10009, '2019-01-01 00:00:00', 1, 2, 10009),
                 (10010, '2021-04-23 00:00:00', 1, 4, 10010);",
        )
        .unwrap();
    let gx = GlueX::open(
        SourceConfig::sqlite(rcdb.path()),
        SourceConfig::sqlite(ccdb.path()),
    )
    .unwrap();
    let before = gx
        .runs(RunSelection::runs([72_435]))
        .unwrap()
        .collect()
        .unwrap();
    let boundary = gx
        .runs(RunSelection::runs([72_436]))
        .unwrap()
        .collect()
        .unwrap();
    let workflows = gx.workflows();

    let before = workflows
        .luminosity(&before, fall_2019_reconstruction(), [9.0, 9.5, 10.0])
        .collect()
        .unwrap();
    let boundary = workflows
        .luminosity(&boundary, fall_2019_reconstruction(), [9.0, 9.5, 10.0])
        .collect()
        .unwrap();

    assert_relative_eq!(
        before.histograms().tagged_flux.counts()[0],
        632_061_337.838_105_6
    );
    assert_eq!(before.histograms().tagged_flux.counts()[1], 0.0);
    assert_eq!(boundary.histograms().tagged_flux.counts()[0], 0.0);
    assert!(boundary.histograms().tagged_flux.counts()[1] > 0.0);
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
fn coherent_peak_reference_case_excludes_fixture_flux_above_the_period_window() {
    let gx = GlueX::open(
        SourceConfig::sqlite(fixtures::rcdb().path()),
        SourceConfig::sqlite(fixtures::ccdb().path()),
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
        .with_coherent_peak(true)
        .collect()
        .unwrap();

    // The fixture channels lie above the RP2018-08 coherent window (8.2, 8.8) GeV.
    assert_eq!(result.histograms().tagged_flux.counts(), &[0.0, 0.0]);
    assert_eq!(result.histograms().tagged_luminosity.counts(), &[0.0, 0.0]);
    assert!(result.provenance().coherent_peak());
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
    assert!(matches!(
        result.provenance().requested_reconstruction(),
        ReconstructionSelection::Latest
    ));
}

#[test]
fn luminosity_uses_source_opening_time_for_default_calibrations() {
    let rcdb = fixtures::rcdb();
    let ccdb = fixtures::ccdb();
    let later = Utc::now() + Duration::milliseconds(250);
    rusqlite::Connection::open(ccdb.path())
        .unwrap()
        .execute_batch(&format!(
            "INSERT INTO constantSets (id, vault, constantTypeId) VALUES
                 (10010, '0|0|1|0|2|0|3|0', 596);
             INSERT INTO assignments (id, created, variationId, runRangeId, constantSetId)
             VALUES (10010, '{}', 1, 2, 10010);",
            later.to_rfc3339()
        ))
        .unwrap();
    let gx = GlueX::open(
        SourceConfig::sqlite(rcdb.path()),
        SourceConfig::sqlite(ccdb.path()),
    )
    .unwrap();
    let opened_at = gx.sources().ccdb().unwrap().opened_at();
    assert!(opened_at < later);
    std::thread::sleep(StdDuration::from_millis(300));
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
    assert_eq!(result.provenance().calibration_default_as_of(), opened_at);
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
fn report_and_fallback_never_hide_incompatible_calibration_schemas() {
    let rcdb = fixtures::rcdb();
    let ccdb = fixtures::ccdb();
    rusqlite::Connection::open(ccdb.path())
        .unwrap()
        .execute(
            "UPDATE columns SET columnType = 'string' WHERE id = 1012",
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

    let report_error = gx
        .workflows()
        .luminosity(&runs, reconstruction(), [8.0, 8.5, 9.0])
        .report_missing()
        .collect()
        .unwrap_err();
    assert!(!matches!(
        report_error,
        WorkflowError::Luminosity(gluex_rs::lumi::LuminosityError::MissingRunInput { .. })
    ));

    let fallback_error = gx
        .workflows()
        .luminosity(&runs, reconstruction(), [8.0, 8.5, 9.0])
        .fallback_to(50_697)
        .collect()
        .unwrap_err();
    assert!(!matches!(
        fallback_error,
        WorkflowError::Luminosity(gluex_rs::lumi::LuminosityError::MissingRunInput { .. })
    ));
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
fn explicit_luminosity_fallback_is_applied_and_reported() {
    let rcdb = fixtures::rcdb();
    let ccdb = fixtures::ccdb();
    rusqlite::Connection::open(ccdb.path())
        .unwrap()
        .execute("UPDATE runRanges SET runMax = 50685 WHERE id = 2", [])
        .unwrap();
    rusqlite::Connection::open(rcdb.path())
        .unwrap()
        .execute("INSERT INTO runs(number) VALUES (50680)", [])
        .unwrap();
    let gx = GlueX::open(
        SourceConfig::sqlite(rcdb.path()),
        SourceConfig::sqlite(ccdb.path()),
    )
    .unwrap();
    let runs = gx
        .runs(RunSelection::runs([50_680]))
        .unwrap()
        .collect()
        .unwrap();

    let result = gx
        .workflows()
        .luminosity(&runs, reconstruction(), [8.0, 8.5, 9.0])
        .fallback_to(50_685)
        .collect()
        .unwrap();

    assert_eq!(result.report().selected_runs(), &[50_680]);
    assert_eq!(result.report().used_runs(), &[50_680]);
    assert_eq!(result.report().substitutions(), &[(50_680, 50_685)]);
    assert_relative_eq!(result.histograms().tagged_flux.counts()[1], TAGGED_FLUX);
    assert_eq!(result.provenance().missing_policy().as_str(), "fallback");
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
    assert!(matches!(error, WorkflowError::Cancelled));

    gx.clear_caches();
    assert_eq!(gx.cache_info().ccdb_metadata_entries(), 0);
    assert!(
        gx.workflows()
            .luminosity(&runs, reconstruction(), [8.0, 8.5, 9.0])
            .collect()
            .is_ok()
    );
}
