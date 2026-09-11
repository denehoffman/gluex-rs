use super::*;
use crate::{RunSelection, SourceIdentity};

#[test]
fn evaluated_runs_are_assembled_without_changing_order_or_accounting() {
    let result = run_set(
        RunProvenance {
            source: SourceIdentity::trusted("fixture".into()),
            selection: RunSelection::range(2, 4),
            predicates: Vec::new(),
            calibration_scopes: Vec::new(),
        },
        EvaluatedRuns {
            numbers: vec![2, 4],
            unknown_runs: vec![3],
            candidates: vec![2, 3, 4],
            complete: true,
        },
    );

    assert_eq!(result.numbers(), &[2, 4]);
    assert_eq!(result.report().unknown_runs(), &[3]);
    assert_eq!(result.report().evaluated_runs(), &[2, 3, 4]);
    assert!(result.report().complete());
}
