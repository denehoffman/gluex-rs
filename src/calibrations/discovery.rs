use super::{CalibrationDirectory, CalibrationTable};
use crate::ccdb::CCDB;
use std::collections::BTreeMap;

pub(super) fn discover(
    reader: &CCDB,
) -> (
    BTreeMap<String, CalibrationTable>,
    BTreeMap<String, CalibrationDirectory>,
) {
    let tables: BTreeMap<_, _> = reader
        .catalog_tables()
        .into_iter()
        .map(|handle| {
            (
                handle.full_path(),
                CalibrationTable {
                    handle,
                    source: reader.connection_path().into(),
                },
            )
        })
        .collect();
    let directories = reader
        .catalog_directories()
        .into_iter()
        .map(|dir| {
            let path = dir.full_path();
            let children = dir
                .dirs()
                .into_iter()
                .map(|child| (child.meta().name().to_owned(), child.full_path()))
                .collect();
            let local_tables = dir
                .tables()
                .into_iter()
                .map(|table| (table.name().to_owned(), tables[&table.full_path()].clone()))
                .collect();
            (
                path.clone(),
                CalibrationDirectory {
                    path,
                    directories: children,
                    tables: local_tables,
                },
            )
        })
        .collect();
    (tables, directories)
}
