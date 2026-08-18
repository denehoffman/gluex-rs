#![allow(missing_docs)]

use approx::assert_relative_eq;
use gluex_rs::{
    Histogram, RESTVersionSelection, RunPeriod,
    lumi::{FluxHistograms, Luminosity, LuminosityContext},
    run_periods::coherent_peak,
};
use std::collections::HashMap;
use std::process::Command;

#[path = "fixtures/rust.rs"]
mod fixtures;

fn assert_histograms_close(actual: &Histogram, expected: &Histogram) {
    assert_relative_eq!(
        actual.counts(),
        expected.counts(),
        max_relative = 8.0 * f64::EPSILON
    );
    assert_relative_eq!(actual.bin_edges(), expected.bin_edges());
    assert_relative_eq!(
        actual.errors(),
        expected.errors(),
        max_relative = 8.0 * f64::EPSILON
    );
}

#[test]
fn lumi_matches_the_library_histograms_for_fixture_inputs() {
    let ccdb = fixtures::ccdb();
    let rcdb = fixtures::rcdb();
    let output = Command::new(env!("CARGO_BIN_EXE_gluex"))
        .args([
            "lumi", "--run", "f18=2", "--bins", "2", "--min", "8.0", "--max", "9.0", "--rcdb",
        ])
        .arg(rcdb.path())
        .arg("--ccdb")
        .arg(ccdb.path())
        .output()
        .expect("gluex lumi should run");

    assert!(
        output.status.success(),
        "{}",
        String::from_utf8_lossy(&output.stderr)
    );
    let cli_histograms: FluxHistograms =
        serde_json::from_slice(&output.stdout).expect("output should be JSON histograms");
    let context = LuminosityContext::new(
        RunPeriod::RP2018_08.iter_runs().collect(),
        HashMap::from([(
            RunPeriod::RP2018_08,
            RESTVersionSelection::try_new(RunPeriod::RP2018_08, 2)
                .expect("fixture REST version must exist"),
        )]),
    )
    .expect("fixture run period should create a luminosity context");
    let library_histograms = Luminosity::new(rcdb.path(), ccdb.path())
        .fetch(&[8.0, 8.5, 9.0], &context)
        .expect("library luminosity calculation should succeed");
    assert_histograms_close(&cli_histograms.tagged_flux, &library_histograms.tagged_flux);
    assert_histograms_close(&cli_histograms.tagm_flux, &library_histograms.tagm_flux);
    assert_histograms_close(&cli_histograms.tagh_flux, &library_histograms.tagh_flux);
    assert_histograms_close(
        &cli_histograms.tagged_luminosity,
        &library_histograms.tagged_luminosity,
    );
}

#[test]
fn info_runs_matches_core_run_period_metadata() {
    let run_period = RunPeriod::RP2018_08;
    let output = Command::new(env!("CARGO_BIN_EXE_gluex"))
        .args(["info", "runs", "f18"])
        .output()
        .expect("gluex info should run");

    assert!(
        output.status.success(),
        "{}",
        String::from_utf8_lossy(&output.stderr)
    );
    let stdout = String::from_utf8(output.stdout).expect("run metadata should be UTF-8");
    let (peak_min, peak_max) = coherent_peak(run_period.min_run());
    let expected = format!(
        "{run_period:?} ({})\n  runs: {}-{}\n  coherent peak: {peak_min:.1}-{peak_max:.1} GeV\n",
        run_period.short_name(),
        run_period.min_run(),
        run_period.max_run(),
    );

    assert_eq!(stdout, expected);
}

#[test]
fn gen_check_validates_and_compiles_example_channel() {
    let config = concat!(
        env!("CARGO_MANIFEST_DIR"),
        "/examples/generation/piplus-neutron.json"
    );
    let output = Command::new(env!("CARGO_BIN_EXE_gluex"))
        .args(["gen", "check", config, "--json"])
        .output()
        .expect("gluex gen check should run");

    assert!(
        output.status.success(),
        "{}",
        String::from_utf8_lossy(&output.stderr)
    );
    let value: serde_json::Value =
        serde_json::from_slice(&output.stdout).expect("check output should be JSON");
    assert_eq!(value["valid"], true);
    assert_eq!(
        value["config_sha256"]
            .as_str()
            .expect("digest should be a string")
            .len(),
        64
    );
}

#[test]
fn gen_check_rejects_unknown_fields() {
    let directory = tempfile::tempdir().expect("temporary directory should be created");
    let config = directory.path().join("invalid.json");
    std::fs::write(
        &config,
        r#"{
            "version": 1,
            "name": "invalid",
            "unexpected": true,
            "beam": {"energy": {"kind": "uniform", "min": 8.0, "max": 9.0}},
            "production": {
                "products": [
                    {"name": "a", "particle": "PiPlus"},
                    {"name": "b", "particle": "Neutron"}
                ],
                "transfer": {
                    "outgoing": "a",
                    "distribution": {"kind": "uniform"}
                }
            }
        }"#,
    )
    .expect("fixture should be written");

    let output = Command::new(env!("CARGO_BIN_EXE_gluex"))
        .args(["gen", "check"])
        .arg(config)
        .output()
        .expect("gluex gen check should run");

    assert!(!output.status.success());
    assert!(String::from_utf8_lossy(&output.stderr).contains("unknown field `unexpected`"));
}

#[test]
fn gen_run_certifies_an_envelope_and_writes_hddm_transactionally() {
    let source = concat!(
        env!("CARGO_MANIFEST_DIR"),
        "/examples/generation/piplus-neutron.json"
    );
    let directory = tempfile::tempdir().expect("temporary directory should be created");
    let hddm = directory.path().join("events.hddm");
    let report = directory.path().join("report.json");

    let generation = Command::new(env!("CARGO_BIN_EXE_gluex"))
        .args(["gen", "run"])
        .arg(source)
        .args([
            "--events",
            "3",
            "--run-number",
            "90000",
            "--seed",
            "17",
            "--output",
        ])
        .arg(&hddm)
        .arg("--report")
        .arg(&report)
        .output()
        .expect("gluex gen run should run");
    assert!(
        generation.status.success(),
        "{}",
        String::from_utf8_lossy(&generation.stderr)
    );
    assert!(std::fs::metadata(hddm).unwrap().len() > 0);
    let report_json: serde_json::Value =
        serde_json::from_slice(&std::fs::read(report).unwrap()).unwrap();
    assert_eq!(report_json["produced"], 3);
    assert_eq!(report_json["pilot_proposals"], 0);
    assert!(report_json["envelope"].as_f64().unwrap() > 0.0);
    assert_eq!(report_json["envelope_kind"], "ProvenPhaseSpace");
    let interval = report_json["proven_weight_interval"]
        .as_array()
        .expect("certified report should include a weight interval");
    assert_eq!(interval.len(), 2);
    assert!(interval[1].as_f64().unwrap() >= interval[0].as_f64().unwrap());
    assert!(report_json["proven_continuous_dimensions"].is_number());
    assert!(report_json["proven_piecewise_regions"].is_number());
    assert!(report_json["proven_subdivisions"].is_number());
}

#[test]
fn gen_run_defaults_seed_and_output_from_manifest_path() {
    let source = concat!(
        env!("CARGO_MANIFEST_DIR"),
        "/examples/generation/piplus-neutron.json"
    );
    let directory = tempfile::tempdir().expect("temporary directory should be created");
    let config = directory.path().join("myconfig.json");
    let default_output = directory.path().join("myconfig.hddm");
    let explicit_output = directory.path().join("explicit.hddm");
    std::fs::copy(source, &config).expect("generation fixture should be copied");

    let default_generation = Command::new(env!("CARGO_BIN_EXE_gluex"))
        .args(["gen", "run"])
        .arg(&config)
        .args(["--events", "2", "--run-number", "90000"])
        .output()
        .expect("gluex gen run should use defaults");
    assert!(
        default_generation.status.success(),
        "{}",
        String::from_utf8_lossy(&default_generation.stderr)
    );
    assert!(std::fs::metadata(&default_output).unwrap().len() > 0);
    assert!(
        String::from_utf8_lossy(&default_generation.stdout)
            .contains(default_output.to_string_lossy().as_ref())
    );

    let explicit_generation = Command::new(env!("CARGO_BIN_EXE_gluex"))
        .args(["gen", "run"])
        .arg(&config)
        .args([
            "--events",
            "2",
            "--run-number",
            "90000",
            "--seed",
            "0",
            "--output",
        ])
        .arg(&explicit_output)
        .output()
        .expect("gluex gen run should accept explicit defaults");
    assert!(
        explicit_generation.status.success(),
        "{}",
        String::from_utf8_lossy(&explicit_generation.stderr)
    );
    assert_eq!(
        std::fs::read(default_output).unwrap(),
        std::fs::read(explicit_output).unwrap()
    );
}

#[test]
fn gen_run_grows_an_underestimated_manual_envelope_with_a_warning() {
    let source = concat!(
        env!("CARGO_MANIFEST_DIR"),
        "/examples/generation/piplus-neutron.json"
    );
    let directory = tempfile::tempdir().expect("temporary directory should be created");
    let hddm = directory.path().join("events.hddm");
    let report = directory.path().join("report.json");

    let generation = Command::new(env!("CARGO_BIN_EXE_gluex"))
        .args(["gen", "run"])
        .arg(source)
        .args([
            "--events",
            "3",
            "--run-number",
            "90000",
            "--seed",
            "17",
            "--max-weight",
            "1e-300",
            "--output",
        ])
        .arg(&hddm)
        .arg("--report")
        .arg(&report)
        .output()
        .expect("gluex gen run should run");

    assert!(
        generation.status.success(),
        "{}",
        String::from_utf8_lossy(&generation.stderr)
    );
    assert!(
        String::from_utf8_lossy(&generation.stderr)
            .contains("warning: rejection envelope was exceeded")
    );
    let report_json: serde_json::Value =
        serde_json::from_slice(&std::fs::read(report).unwrap()).unwrap();
    assert_eq!(report_json["pilot_proposals"], 0);
    assert!(report_json["envelope_updates"].as_u64().unwrap() > 0);
}
