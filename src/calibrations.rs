//! Source-bound calibration discovery and explicit numeric retrieval.
use crate::{
    Id, RunNumber, RunSelection,
    ccdb::{
        CCDB, CCDBError, CCDBResult, ColumnMeta, Data, TypeTableMeta,
        database::{ResolvedAssignment, TypeTableHandle},
    },
};
use chrono::{DateTime, Utc};
use std::{collections::BTreeMap, sync::Arc};

/// Immutable full-path catalog; discovery never fetches constants.
#[derive(Debug, Clone)]
pub struct CalibrationCatalog {
    tables: BTreeMap<String, CalibrationTable>,
    directories: BTreeMap<String, CalibrationDirectory>,
}
impl CalibrationCatalog {
    pub(crate) fn new(reader: &CCDB) -> Self {
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
                    .map(|d| (d.meta().name().to_owned(), d.full_path()))
                    .collect();
                let local_tables = dir
                    .tables()
                    .into_iter()
                    .map(|t| (t.name().to_owned(), tables[&t.full_path()].clone()))
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
        Self {
            tables,
            directories,
        }
    }
    /// Full table paths in lexical order.
    #[must_use]
    pub fn keys(&self) -> impl ExactSizeIterator<Item = &String> {
        self.tables.keys()
    }
    /// Full paths and table definitions in lexical order.
    #[must_use]
    pub fn items(&self) -> impl ExactSizeIterator<Item = (&String, &CalibrationTable)> {
        self.tables.iter()
    }
    /// Look up an exact absolute path without fetching constants.
    #[must_use]
    pub fn get(&self, path: &str) -> Option<&CalibrationTable> {
        self.tables.get(path)
    }
    /// Directory definitions indexed by full path.
    #[must_use]
    pub const fn directories(&self) -> &BTreeMap<String, CalibrationDirectory> {
        &self.directories
    }
}

/// Immutable directory with named child paths and local table definitions.
#[derive(Debug, Clone)]
pub struct CalibrationDirectory {
    path: String,
    directories: BTreeMap<String, String>,
    tables: BTreeMap<String, CalibrationTable>,
}
impl CalibrationDirectory {
    /// Absolute directory path.
    #[must_use]
    pub fn path(&self) -> &str {
        &self.path
    }
    /// Child directory names mapped to absolute paths.
    #[must_use]
    pub const fn directories(&self) -> &BTreeMap<String, String> {
        &self.directories
    }
    /// Local table names mapped to definitions.
    #[must_use]
    pub const fn tables(&self) -> &BTreeMap<String, CalibrationTable> {
        &self.tables
    }
}

/// A calibration definition bound to its source and exploratory defaults.
#[derive(Debug, Clone)]
pub struct CalibrationTable {
    handle: TypeTableHandle,
    source: String,
}
impl CalibrationTable {
    /// Absolute table path.
    #[must_use]
    pub fn path(&self) -> String {
        self.handle.full_path()
    }
    /// Table metadata without reading assignments or constants.
    #[must_use]
    pub const fn metadata(&self) -> &TypeTableMeta {
        self.handle.meta()
    }
    /// Ordered, named columns without reading constants.
    ///
    /// # Errors
    /// Returns a contextual error for malformed column metadata or database failures.
    pub fn columns(&self) -> CCDBResult<Vec<ColumnMeta>> {
        self.handle.columns()
    }
    /// Build an unevaluated numeric request using default variation and source opening time.
    /// RCDB membership is never consulted. Reversed ranges are empty.
    ///
    /// # Errors
    /// Rejects `RunSelection::All`; calibration requests require explicit numeric scope.
    pub fn for_runs(&self, selection: RunSelection) -> CCDBResult<CalibrationQuery> {
        let selection = match selection {
            RunSelection::All => {
                return Err(CCDBError::InvalidPathError(
                    "calibration requests require explicit numeric Run Selection".into(),
                ));
            }
            RunSelection::Runs(runs) => RunSelection::runs(runs),
            selection @ RunSelection::Range { .. } => selection,
        };
        let defaults = self.handle.default_context([]);
        Ok(CalibrationQuery {
            table: self.clone(),
            provenance: CalibrationProvenance {
                source: self.source.clone(),
                table: self.path(),
                selection,
                variation: defaults.variation,
                as_of: defaults.timestamp,
            },
        })
    }
}

/// Captured calibration inputs; files must remain unchanged while in use.
#[derive(Debug, Clone)]
pub struct CalibrationProvenance {
    source: String,
    table: String,
    selection: RunSelection,
    variation: String,
    as_of: DateTime<Utc>,
}
impl CalibrationProvenance {
    /// Source filesystem identity.
    #[must_use]
    pub fn source(&self) -> &str {
        &self.source
    }
    /// Absolute table path.
    #[must_use]
    pub fn table(&self) -> &str {
        &self.table
    }
    /// Requested numeric scope, without RCDB membership resolution.
    #[must_use]
    pub const fn selection(&self) -> &RunSelection {
        &self.selection
    }
    /// Requested variation captured by the query.
    #[must_use]
    pub fn variation(&self) -> &str {
        &self.variation
    }
    /// Requested cutoff: source opening time unless explicitly overridden.
    #[must_use]
    pub const fn as_of(&self) -> DateTime<Utc> {
        self.as_of
    }
}

/// Reusable, lazy calibration request. Inspection does not read assignments or constants.
#[derive(Debug, Clone)]
pub struct CalibrationQuery {
    table: CalibrationTable,
    provenance: CalibrationProvenance,
}
impl CalibrationQuery {
    /// Return a new query requesting an explicit variation. Validated during collection.
    #[must_use]
    pub fn with_variation(&self, variation: impl Into<String>) -> Self {
        let mut query = self.clone();
        query.provenance.variation = variation.into();
        query
    }
    /// Return a new query with an explicit inclusive UTC assignment cutoff.
    #[must_use]
    pub fn as_of(&self, timestamp: DateTime<Utc>) -> Self {
        let mut query = self.clone();
        query.provenance.as_of = timestamp;
        query
    }

    /// Inspect captured inputs without executing the query.
    #[must_use]
    pub const fn provenance(&self) -> &CalibrationProvenance {
        &self.provenance
    }
    /// Collect per-run assignments and shared immutable payloads; report absent assignments.
    ///
    /// # Errors
    /// Database, metadata and payload decoding failures are errors, not missing assignments.
    pub fn collect(&self) -> CCDBResult<CalibrationSeries> {
        self.collect_inner().map_err(|source| CCDBError::Retrieval {
            table: self.provenance.table.clone(),
            variation: self.provenance.variation.clone(),
            as_of: self.provenance.as_of,
            source: Box::new(source),
        })
    }
    fn collect_inner(&self) -> CCDBResult<CalibrationSeries> {
        let runs: Vec<_> = match &self.provenance.selection {
            RunSelection::Runs(runs) => runs.clone(),
            RunSelection::Range { start, end } => (*start..=*end).collect(),
            RunSelection::All => unreachable!("validated at construction"),
        };
        let assignments = self.table.handle.resolve_assignments(
            &runs,
            &self.provenance.variation,
            self.provenance.as_of,
        )?;
        let mut payloads = BTreeMap::new();
        let mut entries = BTreeMap::new();
        for (run, assignment) in assignments {
            let id = assignment.constant_set.id();
            let payload = if let Some(payload) = payloads.get(&id) {
                Arc::clone(payload)
            } else {
                let layout = self.table.handle.column_layout()?;
                let n_rows = usize::try_from(self.table.metadata().n_rows()).map_err(|_| {
                    CCDBError::InvalidPathError(format!(
                        "{}: negative row count",
                        self.table.path()
                    ))
                })?;
                let payload = Arc::new(Data::from_vault(
                    assignment.constant_set.vault(),
                    layout,
                    n_rows,
                )?);
                payloads.insert(id, payload.clone());
                payload
            };
            entries.insert(
                run,
                CalibrationEntry {
                    assignment,
                    payload,
                },
            );
        }
        let missing_runs = runs
            .into_iter()
            .filter(|run| !entries.contains_key(run))
            .collect();
        Ok(CalibrationSeries {
            entries,
            provenance: self.provenance.clone(),
            report: CalibrationReport { missing_runs },
        })
    }
}

/// One resolved assignment with immutable shared constants.
#[derive(Debug, Clone)]
pub struct CalibrationEntry {
    assignment: ResolvedAssignment,
    payload: Arc<Data>,
}
impl CalibrationEntry {
    /// Effective assignment identifier.
    #[must_use]
    pub const fn assignment_id(&self) -> Id {
        self.assignment.id
    }
    /// Effective constant set identifier.
    #[must_use]
    pub fn constant_set_id(&self) -> Id {
        self.assignment.constant_set.id()
    }
    /// Effective assignment creation time.
    #[must_use]
    pub const fn created(&self) -> DateTime<Utc> {
        self.assignment.created
    }
    /// Effective variation name.
    #[must_use]
    pub fn variation(&self) -> &str {
        &self.assignment.variation
    }
    /// Inclusive assignment run bounds.
    #[must_use]
    pub const fn run_range(&self) -> (RunNumber, RunNumber) {
        (self.assignment.run_min, self.assignment.run_max)
    }
    /// Immutable constants, shared by entries resolving to the same constant set.
    #[must_use]
    pub fn payload(&self) -> &Data {
        &self.payload
    }
}

/// Immutable association of requested numeric runs with available assignments.
#[derive(Debug, Clone)]
pub struct CalibrationSeries {
    entries: BTreeMap<RunNumber, CalibrationEntry>,
    provenance: CalibrationProvenance,
    report: CalibrationReport,
}
impl CalibrationSeries {
    /// Find a resolved run; absent assignments return `None` and appear in the report.
    #[must_use]
    pub fn get(&self, run: RunNumber) -> Option<&CalibrationEntry> {
        self.entries.get(&run)
    }
    /// Resolved runs and entries in ascending run order.
    #[must_use]
    pub fn items(&self) -> impl ExactSizeIterator<Item = (&RunNumber, &CalibrationEntry)> {
        self.entries.iter()
    }
    /// Inputs used by this completed collection.
    #[must_use]
    pub const fn provenance(&self) -> &CalibrationProvenance {
        &self.provenance
    }
    /// Missing-assignment diagnostics.
    #[must_use]
    pub const fn report(&self) -> &CalibrationReport {
        &self.report
    }
}

/// Completed calibration retrieval diagnostics.
#[derive(Debug, Clone)]
pub struct CalibrationReport {
    missing_runs: Vec<RunNumber>,
}
impl CalibrationReport {
    /// Requested numeric runs without an assignment.
    #[must_use]
    pub fn missing_runs(&self) -> &[RunNumber] {
        &self.missing_runs
    }
}
