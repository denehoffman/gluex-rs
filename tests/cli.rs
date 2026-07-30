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
