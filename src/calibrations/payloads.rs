use super::{CalibrationEntry, CalibrationPayload, CalibrationQuery};
use crate::{
    DatabaseResult, Id,
    ccdb::{CCDBError, Data, assignment::ResolvedAssignment},
};
use std::{collections::BTreeMap, sync::Arc};

pub(super) fn decode(
    query: &CalibrationQuery,
    payloads: &mut BTreeMap<Id, Arc<CalibrationPayload>>,
    assignment: ResolvedAssignment,
) -> DatabaseResult<CalibrationEntry> {
    if query.execution.interrupted() {
        return Err(CCDBError::from(crate::execution::interrupted_error()).into());
    }
    let id = assignment.constant_set.id();
    let payload = if let Some(payload) = payloads.get(&id) {
        Arc::clone(payload)
    } else {
        let layout = query.table.handle.column_layout()?;
        let n_rows = usize::try_from(query.table.metadata().n_rows()).map_err(|_| {
            CCDBError::InvalidPathError(format!("{}: negative row count", query.table.path()))
        })?;
        let payload = Arc::new(CalibrationPayload(Data::from_vault(
            assignment.constant_set.vault(),
            layout,
            n_rows,
        )?));
        payloads.insert(id, Arc::clone(&payload));
        if payloads.len() > query.table.handle.payload_cache_capacity()
            && let Some(evicted) = payloads.keys().copied().find(|key| *key != id)
        {
            payloads.remove(&evicted);
        }
        payload
    };
    Ok(CalibrationEntry {
        assignment,
        payload,
    })
}
