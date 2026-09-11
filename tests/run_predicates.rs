#![allow(missing_docs)]

use gluex_rs::{GlueX, RunSelection, SourceConfig};

#[path = "fixtures/rust.rs"]
mod fixtures;

#[test]
fn typed_predicates_preserve_unknown_under_negation_and_report_exclusions() {
    let fixture = fixtures::rcdb();
    let gx = GlueX::open(SourceConfig::sqlite(fixture.path()), SourceConfig::Disabled).unwrap();
    let definitions = gx.conditions().unwrap();
    let base = gx.runs(RunSelection::runs([2, 3, 4])).unwrap();
    let valid = definitions["is_valid_run_end"].eq(true).unwrap();
    let result = base.where_predicate(!valid).collect().unwrap();
    assert_eq!(result.numbers(), &[2]);
    assert_eq!(result.report().unknown_runs(), &[3]);
    assert_eq!(base.collect().unwrap().numbers(), &[2, 3, 4]);
    assert_eq!(
        base.where_predicate(definitions["is_valid_run_end"].is_missing())
            .collect()
            .unwrap()
            .numbers(),
        &[3]
    );
    assert!(definitions["event_count"].gt("wrong type").is_err());
}

#[test]
fn composed_predicates_follow_the_three_valued_truth_table() {
    let fixture = fixtures::rcdb();
    let gx = GlueX::open(SourceConfig::sqlite(fixture.path()), SourceConfig::Disabled).unwrap();
    let d = gx.conditions().unwrap();
    let states = [
        d["event_count"].gt(0).unwrap(),
        d["event_count"].lt(0).unwrap(),
        d["is_valid_run_end"].eq(true).unwrap(),
    ];
    // T, F, U: expected conjunction and disjunction, stated independently.
    let cases = [
        (0, 0, Some(true), Some(true)),
        (0, 1, Some(false), Some(true)),
        (0, 2, None, Some(true)),
        (1, 0, Some(false), Some(true)),
        (1, 1, Some(false), Some(false)),
        (1, 2, Some(false), None),
        (2, 0, None, Some(true)),
        (2, 1, Some(false), None),
        (2, 2, None, None),
    ];
    let base = gx.runs(RunSelection::runs([3])).unwrap();
    for (a, b, and, or) in cases {
        for (predicate, expected) in [
            (states[a].clone() & states[b].clone(), and),
            (states[a].clone() | states[b].clone(), or),
        ] {
            let result = base.where_predicate(predicate).collect().unwrap();
            assert_eq!(
                result.numbers(),
                if expected == Some(true) {
                    &[3][..]
                } else {
                    &[]
                }
            );
            assert_eq!(
                result.report().unknown_runs(),
                if expected.is_none() { &[3][..] } else { &[] }
            );
        }
    }
    let present = d["is_valid_run_end"].is_present();
    assert!(
        base.where_predicate(present)
            .collect()
            .unwrap()
            .report()
            .unknown_runs()
            .is_empty()
    );
}

#[test]
fn approval_is_explicit_and_definition_operands_are_parameterized() {
    let fixture = fixtures::rcdb();
    let gx = GlueX::open(SourceConfig::sqlite(fixture.path()), SourceConfig::Disabled).unwrap();
    let base = gx.runs(RunSelection::range(50000, 59999)).unwrap();
    assert_eq!(base.collect().unwrap().numbers(), &[50685, 50697]);
    assert_eq!(
        base.where_predicate(gluex_rs::approved_production(),)
            .collect()
            .unwrap()
            .numbers(),
        &[50685, 50697]
    );
    let d = gx.conditions().unwrap();
    assert!(
        base.where_predicate(d["daq_run"].eq("PHYSICS' OR 1=1 --").unwrap())
            .collect()
            .unwrap()
            .numbers()
            .is_empty()
    );
    assert!(d["beam_current"].gt(f64::NAN).is_err());
    assert!(d["is_valid_run_end"].gt(true).is_err());
    assert_eq!(
        gx.runs(RunSelection::runs([2, 3, 4]))
            .unwrap()
            .where_predicate(
                d["event_count"].ge(1686).unwrap() & d["event_count"].le(5000).unwrap(),
            )
            .collect()
            .unwrap()
            .numbers(),
        &[3, 4]
    );
    let time = "2015-12-08T15:47:20Z"
        .parse::<chrono::DateTime<chrono::Utc>>()
        .unwrap();
    assert_eq!(
        gx.runs(RunSelection::runs([2, 3]))
            .unwrap()
            .where_predicate(d["run_start_time"].eq(time).unwrap())
            .collect()
            .unwrap()
            .numbers(),
        &[2]
    );
}
