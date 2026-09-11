use super::{CalibrationQuery, CalibrationSelector, ReconstructionSelection};
use crate::{
    DatabaseResult, RESTVersionContext, RunNumber, RunPeriod,
    ccdb::{CCDBError, assignment::ResolvedAssignment},
};
use std::collections::BTreeMap;

pub(super) fn resolve(
    query: &CalibrationQuery,
    runs: &[RunNumber],
    resolved_reconstruction: &mut BTreeMap<RunPeriod, RESTVersionContext>,
) -> DatabaseResult<BTreeMap<RunNumber, ResolvedAssignment>> {
    let reconstruction = match query.provenance.selector.clone() {
        CalibrationSelector::Defaults | CalibrationSelector::Direct => {
            return Ok(query.table.handle.resolve_assignments_with_options(
                runs,
                &query.provenance.variation,
                query.provenance.as_of,
                &query.execution,
            )?);
        }
        CalibrationSelector::Reconstruction(reconstruction) => reconstruction,
        CalibrationSelector::Conflict => {
            return Err(CCDBError::SelectorConflict(
                "reconstruction selection cannot be combined with direct variation/as-of arguments"
                    .into(),
            )
            .into());
        }
    };
    let mut grouped: BTreeMap<RunPeriod, Vec<RunNumber>> = BTreeMap::new();
    for &run in runs {
        grouped
            .entry(RunPeriod::try_from(run)?)
            .or_default()
            .push(run);
    }
    let mut assignments = BTreeMap::new();
    for (period, period_runs) in grouped {
        let context = match &reconstruction {
            ReconstructionSelection::Latest => RESTVersionContext {
                variation: query.provenance.variation.clone(),
                timestamp: query.provenance.as_of,
            },
            ReconstructionSelection::Periods(selections) => selections
                .get(&period)
                .ok_or_else(|| {
                    CCDBError::InvalidPathError(format!(
                        "missing reconstruction selection for {period:?}"
                    ))
                })?
                .resolve(period)?,
        };
        assignments.extend(query.table.handle.resolve_assignments_with_options(
            &period_runs,
            &context.variation,
            context.timestamp,
            &query.execution,
        )?);
        resolved_reconstruction.insert(period, context);
    }
    Ok(assignments)
}
