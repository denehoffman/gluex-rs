use super::{CalibrationEntry, CalibrationQuery, CalibrationReport, CalibrationSeries};
use crate::{DatabaseResult, Id, RESTVersionContext, RunNumber, RunPeriod, ccdb::CCDBError};
use std::{collections::BTreeMap, sync::Arc};

pub(super) fn assemble(
    query: &CalibrationQuery,
    runs: Vec<RunNumber>,
    complete: bool,
    decoded: &BTreeMap<RunNumber, CalibrationEntry>,
    resolved_reconstruction: &BTreeMap<RunPeriod, RESTVersionContext>,
    run_report: Option<&crate::RunReport>,
) -> DatabaseResult<CalibrationSeries> {
    let missing_runs: Vec<_> = runs
        .iter()
        .copied()
        .filter(|run| !decoded.contains_key(run))
        .collect();
    if query.provenance.policy == crate::MissingDataPolicy::Strict && !missing_runs.is_empty() {
        return Err(CCDBError::MissingData(missing_runs.len()).into());
    }
    let mut entries: BTreeMap<_, _> = runs
        .iter()
        .filter_map(|run| decoded.get(run).cloned().map(|entry| (*run, entry)))
        .collect();
    let mut substitutions = entries
        .iter()
        .filter_map(|(&run, entry)| {
            let (_, run_max) = entry.run_range();
            (run > run_max).then_some((run, run_max))
        })
        .collect::<Vec<_>>();
    if let Some(fallback) = query.provenance.fallback_run {
        let entry = decoded
            .get(&fallback)
            .cloned()
            .ok_or(CCDBError::MissingData(1))?;
        for &run in &missing_runs {
            entries.insert(run, entry.clone());
            substitutions.push((run, fallback));
        }
    }
    let mut provenance = query.provenance.clone();
    provenance.resolved_reconstruction = resolved_reconstruction.clone();
    provenance.run_report = run_report.cloned();
    let payloads: BTreeMap<Id, Arc<_>> = entries
        .values()
        .map(|entry| (entry.constant_set_id(), Arc::clone(&entry.payload)))
        .collect();
    Ok(CalibrationSeries {
        entries,
        payloads,
        provenance,
        report: CalibrationReport {
            missing_runs,
            substitutions,
            evaluated_runs: runs,
            complete,
        },
    })
}
