//! Run predicate execution and condition-value evaluation.

use super::{ConditionQuery, ConditionValue, MissingDataPolicy, RunQuery, RunSelection, RunSet};
use crate::{DatabaseResult, RunNumber, rcdb::RCDBContext};

pub(super) struct EvaluatedRuns {
    pub(super) numbers: Vec<RunNumber>,
    pub(super) unknown_runs: Vec<RunNumber>,
    pub(super) candidates: Vec<RunNumber>,
    pub(super) complete: bool,
}

pub(super) fn evaluate_runs(
    query: &RunQuery,
    candidates: Vec<RunNumber>,
    complete: bool,
) -> DatabaseResult<EvaluatedRuns> {
    if query.execution.interrupted() {
        return Err(crate::rcdb::RCDBError::from(crate::execution::interrupted_error()).into());
    }
    let predicate =
        crate::rcdb::conditions::all(query.predicates.iter().map(|predicate| predicate.0.clone()));
    let selection = RunSelection::runs(candidates.iter().copied());
    let context = RCDBContext::from_selection(selection.clone()).filter(predicate.clone());
    let unknown_context = RCDBContext::from_selection(selection).filter(predicate.unknown());
    Ok(EvaluatedRuns {
        numbers: query
            .reader
            .fetch_runs_with_options(&context, &query.execution)?,
        unknown_runs: query
            .reader
            .fetch_runs_with_options(&unknown_context, &query.execution)?,
        candidates,
        complete,
    })
}

pub(super) struct EvaluatedConditions {
    pub(super) columns: std::collections::BTreeMap<String, Vec<Option<ConditionValue>>>,
    pub(super) missing_values: Vec<(RunNumber, String)>,
    pub(super) substitutions: Vec<(RunNumber, String)>,
}

pub(super) fn collect_conditions(
    query: &ConditionQuery,
    runs: &RunSet,
) -> DatabaseResult<EvaluatedConditions> {
    let rows = query.query.reader.fetch_with_options(
        &query.fields,
        &RCDBContext::from_selection(RunSelection::runs(runs.numbers().iter().copied())),
        &query.query.execution,
    )?;
    let mut columns = std::collections::BTreeMap::new();
    let mut missing_values = Vec::new();
    let mut substitutions = Vec::new();
    for name in &query.fields {
        let column = runs
            .numbers()
            .iter()
            .map(|run| {
                let mut value = rows
                    .get(run)
                    .and_then(|row| row.get(name))
                    .cloned()
                    .map(ConditionValue);
                if value.is_none() {
                    missing_values.push((*run, name.clone()));
                    if let Some(fallback) = query.fallbacks.get(name) {
                        value = Some(fallback.clone());
                        substitutions.push((*run, name.clone()));
                    }
                }
                value
            })
            .collect();
        columns.insert(name.clone(), column);
    }
    missing_values.sort();
    substitutions.sort();
    if query.policy == MissingDataPolicy::Strict && !missing_values.is_empty() {
        return Err(crate::rcdb::RCDBError::MissingData(missing_values.len()).into());
    }
    Ok(EvaluatedConditions {
        columns,
        missing_values,
        substitutions,
    })
}
