//! Assembly of immutable run-domain result chunks.

use super::{
    ConditionProvenance, ConditionReport, ConditionResults, RunProvenance, RunReport, RunSet,
    evaluation::{EvaluatedConditions, EvaluatedRuns},
};

pub(super) fn run_set(provenance: RunProvenance, evaluated: EvaluatedRuns) -> RunSet {
    RunSet {
        numbers: evaluated.numbers,
        provenance,
        report: RunReport {
            unknown_runs: evaluated.unknown_runs,
            evaluated_runs: evaluated.candidates,
            complete: evaluated.complete,
        },
    }
}

pub(super) fn condition_results(
    runs: RunSet,
    provenance: ConditionProvenance,
    evaluated: EvaluatedConditions,
) -> ConditionResults {
    ConditionResults {
        runs,
        columns: evaluated.columns,
        provenance,
        report: ConditionReport {
            missing_values: evaluated.missing_values,
            substitutions: evaluated.substitutions,
        },
    }
}

pub(super) fn extend_conditions(target: &mut ConditionResults, other: ConditionResults) {
    target.runs.numbers.extend(other.runs.numbers);
    target
        .runs
        .report
        .unknown_runs
        .extend(other.runs.report.unknown_runs);
    target
        .runs
        .report
        .evaluated_runs
        .extend(other.runs.report.evaluated_runs);
    target.runs.report.complete = other.runs.report.complete;
    for (name, values) in other.columns {
        target.columns.entry(name).or_default().extend(values);
    }
    target
        .report
        .missing_values
        .extend(other.report.missing_values);
    target
        .report
        .substitutions
        .extend(other.report.substitutions);
}

#[cfg(test)]
mod tests;
