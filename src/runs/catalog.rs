//! Construction of immutable condition catalogs from source metadata.

use std::collections::BTreeMap;

use super::ConditionDefinition;

pub(super) fn definitions(
    definitions: impl IntoIterator<Item = (String, crate::rcdb::models::ConditionTypeMeta)>,
) -> BTreeMap<String, ConditionDefinition> {
    definitions
        .into_iter()
        .map(|(name, definition)| (name, ConditionDefinition(definition)))
        .collect()
}
