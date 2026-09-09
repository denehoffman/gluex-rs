use crate::{
    ExecutionOptions,
    ccdb::{CCDBError, CCDBResult, models::ConstantSetMeta},
    core::{Id, RunNumber},
};
use chrono::{DateTime, Utc};
use std::{
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
    let mut best = BTreeMap::new();
    let mut best_id = HashMap::new();
    let mut constant_set_cache: HashMap<Id, Arc<ConstantSetMeta>> = HashMap::new();
    for &run in runs {
        if options.interrupted() {
            return Err(crate::execution::interrupted_error().into());
        }
        for candidate in candidates {
            if run < candidate.run_min || run > candidate.run_max || candidate.created > timestamp {
                continue;
            }
            if best_id.get(&run).is_none_or(|id| candidate.id > *id) {
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
                best_id.insert(run, candidate.id);
            }
        }
    }
    Ok(best)
}
