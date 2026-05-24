#![allow(missing_docs)]

use gluex_lumi::{Luminosity, LuminosityContext, RESTVersionSelection, RunPeriod};
use std::collections::HashMap;

#[path = "../../../tests/fixtures/rust.rs"]
mod fixtures;

const TAGM_FLUX: f64 = 48_116_930.846_010_25;
const TAGH_FLUX: f64 = 642_059_090.080_545_7;
const TAGGED_FLUX: f64 = 690_176_020.926_555_9;
const TAGGED_LUMINOSITY: f64 = 0.000_869_563_919_913_552_8;

fn context(runs: Vec<i64>) -> LuminosityContext {
    LuminosityContext::new(
        runs,
        HashMap::from([(
            RunPeriod::RP2018_08,
            RESTVersionSelection::try_new(RunPeriod::RP2018_08, 2)
                .expect("fixture REST version must exist"),
        )]),
    )
    .expect("fixture runs must be valid")
}

#[test]
fn fetch_computes_detector_flux_and_luminosity() {
    let ccdb = fixtures::ccdb();
    let rcdb = fixtures::rcdb();
    let calculator = Luminosity::new(rcdb.path(), ccdb.path());

    let histograms = calculator
        .fetch(&[8.0, 8.5, 9.0], &context(vec![50685]))
        .expect("luminosity calculation should succeed");

    assert_eq!(histograms.tagged_flux.counts[0], 0.0);
    assert_eq!(histograms.tagm_flux.counts[1], TAGM_FLUX);
    assert_eq!(histograms.tagh_flux.counts[1], TAGH_FLUX);
    assert_eq!(histograms.tagged_flux.counts[1], TAGGED_FLUX);
    assert_eq!(histograms.tagged_luminosity.counts[1], TAGGED_LUMINOSITY);
    assert!(histograms.tagged_luminosity.errors[1] > 0.0);
}

#[test]
fn excluded_runs_are_removed_before_aggregation() {
    let ccdb = fixtures::ccdb();
    let rcdb = fixtures::rcdb();
    let calculator = Luminosity::new(rcdb.path(), ccdb.path());
    let single = calculator
        .fetch(&[8.0, 8.5, 9.0], &context(vec![50685]))
        .expect("single-run luminosity should succeed");
    let excluding = calculator
        .fetch(
            &[8.0, 8.5, 9.0],
            &context(vec![50685, 50697]).with_exclude_runs([50697]),
        )
        .expect("excluded-run luminosity should succeed");

    assert_eq!(excluding.tagged_flux.counts, single.tagged_flux.counts);
    assert_eq!(
        excluding.tagged_luminosity.counts,
        single.tagged_luminosity.counts
    );
}
