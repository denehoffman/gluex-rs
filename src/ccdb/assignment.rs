use crate::{
    ExecutionOptions,
    ccdb::{CCDBError, CCDBResult, models::ConstantSetMeta},
    core::{Id, RunNumber},
};
use chrono::{DateTime, Utc};
use std::{
    collections::BinaryHeap,
    collections::{BTreeMap, HashMap, HashSet},
    sync::Arc,
};

/// Candidate metadata decoded once before run-by-run assignment selection.
#[derive(Debug, Clone)]
pub struct AssignmentCandidate {
    id: Id,
    created: DateTime<Utc>,
    constant_set: ConstantSetMeta,
    run_min: RunNumber,
    run_max: RunNumber,
}

impl AssignmentCandidate {
    pub fn try_new(
        id: Id,
        created: &str,
        constant_set: ConstantSetMeta,
        run_min: RunNumber,
        run_max: RunNumber,
    ) -> CCDBResult<Self> {
        if run_min > run_max {
            return Err(CCDBError::InvalidMetadata(format!(
                "assignment {id} has reversed run bounds"
            )));
        }
        let created = crate::core::parsers::parse_database_timestamp(created)
            .map_err(|error| CCDBError::InvalidMetadata(format!("assignment {id}: {error}")))?;
        Ok(Self {
            id,
            created,
            constant_set,
            run_min,
            run_max,
        })
    }
}

#[derive(Debug, Clone)]
pub struct ResolvedAssignment {
    pub(crate) constant_set: Arc<ConstantSetMeta>,
    pub(crate) id: Id,
    pub(crate) created: DateTime<Utc>,
    pub(crate) variation: String,
    pub(crate) run_min: RunNumber,
    pub(crate) run_max: RunNumber,
}

/// Select the highest eligible assignment id for each requested run.
///
/// This intentionally retains the straightforward resolver used before the
/// interval-aware implementation. Candidate decoding and validation are kept
/// outside the run loop so this function is a focused correctness oracle for
/// the optimized resolver introduced separately.
pub fn resolve_candidates(
    runs: &HashSet<RunNumber>,
    candidates: &[AssignmentCandidate],
    variation: &str,
    timestamp: DateTime<Utc>,
    options: &ExecutionOptions,
) -> CCDBResult<BTreeMap<RunNumber, ResolvedAssignment>> {
    resolve_candidates_with_stats(runs, candidates, variation, timestamp, options)
        .map(|(resolved, _)| resolved)
}

fn resolve_candidates_with_stats(
    runs: &HashSet<RunNumber>,
    candidates: &[AssignmentCandidate],
    variation: &str,
    timestamp: DateTime<Utc>,
    options: &ExecutionOptions,
) -> CCDBResult<(BTreeMap<RunNumber, ResolvedAssignment>, usize)> {
    let mut ordered_runs = runs.iter().copied().collect::<Vec<_>>();
    ordered_runs.sort_unstable();
    let mut eligible = candidates
        .iter()
        .enumerate()
        .filter(|(_, candidate)| candidate.created <= timestamp)
        .collect::<Vec<_>>();
    eligible.sort_unstable_by_key(|(_, candidate)| candidate.run_min);

    let mut best = BTreeMap::new();
    let mut constant_set_cache: HashMap<Id, Arc<ConstantSetMeta>> = HashMap::new();
    let mut active = BinaryHeap::new();
    let mut next_candidate = 0;
    let mut inspected = 0;
    for run in ordered_runs {
        inspected += 1;
        if options.interrupted() {
            return Err(crate::execution::interrupted_error().into());
        }
        while next_candidate < eligible.len() && eligible[next_candidate].1.run_min <= run {
            let (index, candidate) = eligible[next_candidate];
            active.push((candidate.id, index));
            next_candidate += 1;
            inspected += 1;
        }
        while active
            .peek()
            .is_some_and(|(_, index)| candidates[*index].run_max < run)
        {
            active.pop();
        }
        let Some(&(_, index)) = active.peek() else {
            continue;
        };
        let candidate = &candidates[index];
        let constant_set = constant_set_cache
            .entry(candidate.constant_set.id)
            .or_insert_with(|| Arc::new(candidate.constant_set.clone()))
            .clone();
        best.insert(
            run,
            ResolvedAssignment {
                constant_set,
                id: candidate.id,
                created: candidate.created,
                variation: variation.to_owned(),
                run_min: candidate.run_min,
                run_max: candidate.run_max,
            },
        );
    }
    Ok((best, inspected))
}

#[cfg(test)]
mod tests {
    use super::*;

    fn candidate(id: Id, run_min: RunNumber, run_max: RunNumber) -> AssignmentCandidate {
        AssignmentCandidate::try_new(
            id,
            "2020-01-01 00:00:00",
            ConstantSetMeta {
                id,
                ..ConstantSetMeta::default()
            },
            run_min,
            run_max,
        )
        .unwrap()
    }

    #[test]
    fn resolution_work_scales_with_runs_plus_candidates() {
        let cutoff = crate::core::parsers::parse_database_timestamp("2021-01-01 00:00:00").unwrap();
        for (run_count, candidate_count) in [(1_000, 10), (10, 1_000)] {
            let runs = (0..run_count).collect::<HashSet<_>>();
            let candidates = (0..candidate_count)
                .map(|id| candidate(id + 1, 0, run_count - 1))
                .collect::<Vec<_>>();
            let (resolved, inspected) = resolve_candidates_with_stats(
                &runs,
                &candidates,
                "default",
                cutoff,
                &ExecutionOptions::default(),
            )
            .unwrap();

            assert_eq!(resolved.len(), runs.len());
            assert_eq!(inspected, runs.len() + candidates.len());
            assert!(inspected < runs.len() * candidates.len());
        }
    }
}
