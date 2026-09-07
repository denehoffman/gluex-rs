#![allow(missing_docs)]

use approx::assert_relative_eq;
use gluex_rs::lumi::{
    Luminosity, LuminosityContext, LuminosityError, RESTVersionSelection, RunPeriod,
};
use std::collections::HashMap;

#[path = "fixtures/rust.rs"]
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

    assert_relative_eq!(histograms.tagged_flux.counts()[0], 0.0);
    assert_relative_eq!(
        histograms.tagm_flux.counts()[1],
        TAGM_FLUX,
        max_relative = 1e-12
    );
    assert_relative_eq!(
        histograms.tagh_flux.counts()[1],
        TAGH_FLUX,
        max_relative = 1e-12
    );
    assert_relative_eq!(
        histograms.tagged_flux.counts()[1],
        TAGGED_FLUX,
        max_relative = 1e-12
    );
    assert_relative_eq!(
        histograms.tagged_luminosity.counts()[1],
        TAGGED_LUMINOSITY,
        max_relative = 1e-12
    );
    assert!(histograms.tagged_luminosity.errors()[1] > 0.0);
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

    for (excluding, single) in excluding
        .tagged_flux
        .counts()
        .iter()
        .zip(single.tagged_flux.counts())
    {
        assert_relative_eq!(excluding, single);
    }
    for (excluding, single) in excluding
        .tagged_luminosity
        .counts()
        .iter()
        .zip(single.tagged_luminosity.counts())
    {
        assert_relative_eq!(excluding, single);
    }
}

#[test]
fn cloned_calculators_remain_usable_after_original_is_dropped() {
    let ccdb = fixtures::ccdb();
    let rcdb = fixtures::rcdb();
    let calculator = Luminosity::new(rcdb.path(), ccdb.path());
    let first = calculator.clone();
    calculator
        .fetch(&[8.0, 8.5, 9.0], &context(vec![50685]))
        .unwrap();
    let second = calculator.clone();
    drop(calculator);

    std::thread::scope(|scope| {
        for calculator in [first, second] {
            scope.spawn(move || {
                let histograms = calculator
                    .fetch(&[8.0, 8.5, 9.0], &context(vec![50685]))
                    .expect("surviving clones should remain usable concurrently");
                assert_relative_eq!(
                    histograms.tagged_flux.counts()[1],
                    TAGGED_FLUX,
                    max_relative = 1e-12
                );
                assert_relative_eq!(
                    histograms.tagged_luminosity.counts()[1],
                    TAGGED_LUMINOSITY,
                    max_relative = 1e-12
                );
            });
        }
    });
}

#[test]
fn failed_open_can_be_retried_after_missing_source_becomes_available() {
    let rcdb = fixtures::rcdb();
    let ccdb = fixtures::ccdb();
    let directory = tempfile::tempdir().unwrap();
    let missing = directory.path().join("ccdb.sqlite");
    let calculator = Luminosity::new(rcdb.path(), &missing);
    let selection = context(vec![50685]);
    assert!(matches!(
        calculator.fetch(&[8.0, 8.5, 9.0], &selection),
        Err(LuminosityError::CCDBError(_))
    ));

    std::fs::copy(ccdb.path(), &missing).unwrap();
    let histograms = calculator.fetch(&[8.0, 8.5, 9.0], &selection).unwrap();
    assert_relative_eq!(
        histograms.tagged_flux.counts()[1],
        TAGGED_FLUX,
        max_relative = 1e-12
    );
}

#[test]
fn excluded_selection_is_rejected_before_opening_sources() {
    let directory = tempfile::tempdir().unwrap();
    let calculator = Luminosity::new(
        directory.path().join("rcdb.sqlite"),
        directory.path().join("ccdb.sqlite"),
    );
    let selection = context(vec![50685]).with_exclude_runs([50685]);
    assert!(matches!(
        calculator.fetch(&[8.0, 8.5, 9.0], &selection),
        Err(LuminosityError::EmptyRunSelection)
    ));
}
