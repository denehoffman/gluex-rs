//! Source-bound calibration discovery and explicit numeric retrieval.
use crate::{
    Id, RESTVersionContext, RESTVersionSelection, RunNumber, RunPeriod, RunProvenance, RunQuery,
    RunSelection, RunSet,
    ccdb::{
        CCDB, CCDBError, CCDBResult, ColumnMeta, Data, TypeTableMeta,
        database::{ResolvedAssignment, TypeTableHandle},
    },
};
use chrono::{DateTime, Utc};
use std::{collections::BTreeMap, sync::Arc};

/// Explicit reconstruction-aware calibration selector.
#[derive(Debug, Clone)]
pub enum ReconstructionSelection {
    /// Use the source-opening defaults explicitly for every run period.
    Latest,
    /// Resolve the supplied REST selection independently for each run period.
    Periods(BTreeMap<RunPeriod, RESTVersionSelection>),
}
impl ReconstructionSelection {
    /// Explicitly request the latest calibration state captured when the source opened.
    #[must_use]
    pub const fn latest() -> Self {
        Self::Latest
    }
    /// Build an explicit per-period REST selection mapping.
    #[must_use]
    pub fn periods(
        selections: impl IntoIterator<Item = (RunPeriod, RESTVersionSelection)>,
    ) -> Self {
        Self::Periods(selections.into_iter().collect())
    }
}

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
        Ok(self.query(CalibrationInput::Numeric(selection), None, None))
    }

    /// Build a lazy request from a resolved Run Set without reconstructing a numeric list.
    ///
    /// # Errors
    /// Reserved for validation shared with the other calibration constructors.
    pub fn for_run_set(&self, runs: &RunSet) -> CCDBResult<CalibrationQuery> {
        Ok(self.query(
            CalibrationInput::Resolved(runs.clone()),
            Some(runs.provenance().clone()),
            Some(runs.report().clone()),
        ))
    }

    /// Build a lazy composed request. RCDB is consulted only when this query is evaluated.
    ///
    /// # Errors
    /// Reserved for validation shared with the other calibration constructors.
    pub fn for_query(&self, runs: &RunQuery) -> CCDBResult<CalibrationQuery> {
        Ok(self.query(
            CalibrationInput::Query(runs.clone()),
            Some(runs.provenance()),
            None,
        ))
    }

    fn query(
        &self,
        input: CalibrationInput,
        runs: Option<RunProvenance>,
        run_report: Option<crate::RunReport>,
    ) -> CalibrationQuery {
        let defaults = self.handle.default_context([]);
        let selection = input.selection().clone();
        CalibrationQuery {
            table: self.clone(),
            input,
            execution: crate::ExecutionOptions::default(),
            provenance: CalibrationProvenance {
                source: self.source.clone(),
                table: self.path(),
                selection,
                runs,
                run_report,
                variation: defaults.variation,
                as_of: defaults.timestamp,
                selector: CalibrationSelector::Defaults,
                resolved_reconstruction: BTreeMap::new(),
                policy: crate::MissingDataPolicy::Report,
                fallback_run: None,
            },
        }
    }
}

#[derive(Debug, Clone)]
enum CalibrationInput {
    Numeric(RunSelection),
    Resolved(RunSet),
    Query(RunQuery),
}
#[derive(Debug, Clone)]
enum CalibrationSelector {
    Defaults,
    Direct,
    Reconstruction(ReconstructionSelection),
    Conflict,
}

impl CalibrationSelector {
    fn with_direct(self) -> Self {
        match self {
            Self::Reconstruction(_) | Self::Conflict => Self::Conflict,
            Self::Defaults | Self::Direct => Self::Direct,
        }
    }

    fn with_reconstruction(self, selection: ReconstructionSelection) -> Self {
        match self {
            Self::Direct | Self::Conflict => Self::Conflict,
            Self::Defaults | Self::Reconstruction(_) => Self::Reconstruction(selection),
        }
    }
}
impl CalibrationInput {
    const fn selection(&self) -> &RunSelection {
        match self {
            Self::Numeric(selection) => selection,
            Self::Resolved(runs) => runs.provenance().selection(),
            Self::Query(query) => query.selection(),
        }
    }
}

/// Captured calibration inputs; files must remain unchanged while in use.
#[derive(Debug, Clone)]
pub struct CalibrationProvenance {
    source: String,
    table: String,
    selection: RunSelection,
    runs: Option<RunProvenance>,
    run_report: Option<crate::RunReport>,
    variation: String,
    as_of: DateTime<Utc>,
    selector: CalibrationSelector,
    resolved_reconstruction: BTreeMap<RunPeriod, RESTVersionContext>,
    policy: crate::MissingDataPolicy,
    fallback_run: Option<RunNumber>,
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
    /// RCDB membership and predicate provenance for composed requests.
    #[must_use]
    pub const fn runs(&self) -> Option<&RunProvenance> {
        self.runs.as_ref()
    }
    /// Completed or partial upstream RCDB diagnostics for composed requests.
    #[must_use]
    pub const fn run_report(&self) -> Option<&crate::RunReport> {
        self.run_report.as_ref()
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
    /// Explicit reconstruction selector, when present.
    #[must_use]
    pub const fn reconstruction(&self) -> Option<&ReconstructionSelection> {
        match &self.selector {
            CalibrationSelector::Reconstruction(selection) => Some(selection),
            _ => None,
        }
    }
    /// Per-period variation/time selections resolved during evaluation.
    #[must_use]
    pub const fn resolved_reconstruction(&self) -> &BTreeMap<RunPeriod, RESTVersionContext> {
        &self.resolved_reconstruction
    }
    /// Declared missing-data policy.
    #[must_use]
    pub const fn policy(&self) -> crate::MissingDataPolicy {
        self.policy
    }
    /// Run whose resolved assignment supplies explicit fallbacks.
    #[must_use]
    pub const fn fallback_run(&self) -> Option<RunNumber> {
        self.fallback_run
    }
}

/// Reusable, lazy calibration request. Inspection does not read assignments or constants.
#[derive(Debug, Clone)]
pub struct CalibrationQuery {
    table: CalibrationTable,
    input: CalibrationInput,
    execution: crate::ExecutionOptions,
    provenance: CalibrationProvenance,
}
impl CalibrationQuery {
    /// Return a query that stops once `duration` has elapsed during evaluation.
    #[must_use]
    pub fn with_timeout(&self, duration: std::time::Duration) -> Self {
        let mut query = self.clone();
        query.execution = query.execution.with_timeout(duration);
        query
    }

    /// Return a query controlled by the supplied cancellation token.
    #[must_use]
    pub fn with_cancellation(&self, token: crate::CancellationToken) -> Self {
        let mut query = self.clone();
        query.execution = query.execution.with_cancellation(token);
        query
    }

    #[cfg(feature = "python")]
    pub(crate) fn with_execution(&self, execution: crate::ExecutionOptions) -> Self {
        let mut query = self.clone();
        query.execution = execution;
        query
    }

    #[cfg(feature = "python")]
    pub(crate) fn with_interrupt_check(
        &self,
        check: impl Fn() -> bool + Send + Sync + 'static,
    ) -> Self {
        self.with_execution(self.execution.clone().with_interrupt_check(check))
    }

    /// Return a new query requesting an explicit variation. Validated during collection.
    #[must_use]
    pub fn with_variation(&self, variation: impl Into<String>) -> Self {
        let mut query = self.clone();
        query.provenance.variation = variation.into();
        query.provenance.selector = query.provenance.selector.with_direct();
        query
    }
    /// Return a new query with an explicit inclusive UTC assignment cutoff.
    #[must_use]
    pub fn as_of(&self, timestamp: DateTime<Utc>) -> Self {
        let mut query = self.clone();
        query.provenance.as_of = timestamp;
        query.provenance.selector = query.provenance.selector.with_direct();
        query
    }

    /// Return a new query with an explicit reconstruction selector.
    #[must_use]
    pub fn with_reconstruction(&self, selection: ReconstructionSelection) -> Self {
        let mut query = self.clone();
        query.provenance.selector = query.provenance.selector.with_reconstruction(selection);
        query
    }

    /// Reject any requested run lacking an assignment.
    #[must_use]
    pub fn strict(&self) -> Self {
        let mut query = self.clone();
        query.provenance.policy = crate::MissingDataPolicy::Strict;
        query.provenance.fallback_run = None;
        query
    }

    /// Use the assignment resolved for `run` as an explicit fallback for missing runs.
    #[must_use]
    pub fn fallback_to(&self, run: RunNumber) -> Self {
        let mut query = self.clone();
        query.provenance.policy = crate::MissingDataPolicy::Fallback;
        query.provenance.fallback_run = Some(run);
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
        let mut chunks = self.stream_inner(1024)?;
        let mut result = CalibrationSeries {
            entries: BTreeMap::new(),
            payloads: BTreeMap::new(),
            provenance: self.provenance.clone(),
            report: CalibrationReport {
                missing_runs: Vec::new(),
                substitutions: Vec::new(),
                evaluated_runs: Vec::new(),
                complete: true,
            },
        };
        for chunk in &mut chunks {
            result.extend(chunk?);
        }
        result.provenance.resolved_reconstruction = chunks.resolved_reconstruction;
        result.report.complete = true;
        Ok(result)
    }

    /// Iterate bounded assignment chunks while sharing decoded payloads across chunks.
    ///
    /// # Errors
    /// Rejects zero-sized chunks, selector conflicts, and invalid composed inputs.
    pub fn stream(&self, chunk_size: usize) -> CCDBResult<CalibrationStream> {
        self.stream_inner(chunk_size)
            .map_err(|source| CCDBError::Retrieval {
                table: self.provenance.table.clone(),
                variation: self.provenance.variation.clone(),
                as_of: self.provenance.as_of,
                source: Box::new(source),
            })
    }

    fn stream_inner(&self, chunk_size: usize) -> CCDBResult<CalibrationStream> {
        if self.execution.interrupted() {
            return Err(crate::execution::interrupted_error().into());
        }
        if chunk_size == 0 {
            return Err(CCDBError::InvalidPathError(
                "stream chunk size must be positive".into(),
            ));
        }
        if matches!(self.provenance.selector, CalibrationSelector::Conflict) {
            return Err(CCDBError::SelectorConflict(
                "reconstruction selection cannot be combined with direct variation/as-of arguments"
                    .into(),
            ));
        }
        let input = match &self.input {
            CalibrationInput::Numeric(selection) => {
                CalibrationRunStream::Numeric(SelectionCursor::new(selection.clone(), chunk_size))
            }
            CalibrationInput::Resolved(runs) => CalibrationRunStream::Resolved {
                runs: runs.numbers().to_vec(),
                index: 0,
                chunk_size,
            },
            CalibrationInput::Query(query) => CalibrationRunStream::Query(
                query
                    .with_execution(self.execution.clone())
                    .stream(chunk_size)?,
            ),
        };
        Ok(CalibrationStream {
            query: self.clone(),
            input,
            payloads: BTreeMap::new(),
            resolved_reconstruction: BTreeMap::new(),
            run_report: self.provenance.run_report.clone(),
            failed: false,
        })
    }

    /// Return the first requested calibration result chunk without consuming the rest.
    ///
    /// # Errors
    /// Returns a contextual calibration or composed RCDB evaluation error.
    pub fn first(&self) -> CCDBResult<Option<CalibrationSeries>> {
        self.stream(1)?.next().transpose()
    }

    /// Count resolved assignments without retaining a complete series.
    ///
    /// # Errors
    /// Returns a contextual calibration or composed RCDB evaluation error.
    pub fn count(&self) -> CCDBResult<usize> {
        self.stream(1024)?
            .try_fold(0usize, |count, chunk| Ok(count + chunk?.entries.len()))
    }

    /// Return exactly one resolved assignment, rejecting any other cardinality.
    ///
    /// # Errors
    /// Returns a cardinality or contextual calibration evaluation error.
    pub fn one(&self) -> CCDBResult<CalibrationSeries> {
        let result = self.collect()?;
        if result.entries.len() == 1 {
            Ok(result)
        } else {
            Err(CCDBError::InvalidCardinality(result.entries.len()))
        }
    }
}

struct SelectionCursor {
    selection: RunSelection,
    index: usize,
    next: Option<RunNumber>,
    chunk_size: usize,
}
impl SelectionCursor {
    const fn new(selection: RunSelection, chunk_size: usize) -> Self {
        let next = match selection {
            RunSelection::Range { start, end } if start <= end => Some(start),
            _ => None,
        };
        Self {
            selection,
            index: 0,
            next,
            chunk_size,
        }
    }
    fn next_chunk(&mut self) -> Option<(Vec<RunNumber>, bool)> {
        match &self.selection {
            RunSelection::Runs(runs) => {
                if self.index >= runs.len() {
                    return None;
                }
                let end = (self.index + self.chunk_size).min(runs.len());
                let chunk = runs[self.index..end].to_vec();
                self.index = end;
                Some((chunk, end == runs.len()))
            }
            RunSelection::Range { end, .. } => {
                let mut run = self.next?;
                let mut chunk = Vec::with_capacity(self.chunk_size);
                while chunk.len() < self.chunk_size {
                    chunk.push(run);
                    if run == *end {
                        self.next = None;
                        break;
                    }
                    run = run.checked_add(1)?;
                    self.next = Some(run);
                }
                Some((chunk, self.next.is_none()))
            }
            RunSelection::All => None,
        }
    }
}

enum CalibrationRunStream {
    Numeric(SelectionCursor),
    Resolved {
        runs: Vec<RunNumber>,
        index: usize,
        chunk_size: usize,
    },
    Query(crate::RunStream),
}

/// Bounded iterator over calibration series chunks.
pub struct CalibrationStream {
    query: CalibrationQuery,
    input: CalibrationRunStream,
    payloads: BTreeMap<Id, Arc<Data>>,
    resolved_reconstruction: BTreeMap<RunPeriod, RESTVersionContext>,
    run_report: Option<crate::RunReport>,
    failed: bool,
}
struct CalibrationRunChunk {
    runs: Vec<RunNumber>,
    complete: bool,
    report: Option<crate::RunReport>,
}
impl CalibrationStream {
    fn next_runs(&mut self) -> CCDBResult<Option<CalibrationRunChunk>> {
        match &mut self.input {
            CalibrationRunStream::Numeric(cursor) => {
                Ok(cursor
                    .next_chunk()
                    .map(|(runs, complete)| CalibrationRunChunk {
                        runs,
                        complete,
                        report: None,
                    }))
            }
            CalibrationRunStream::Resolved {
                runs,
                index,
                chunk_size,
            } => {
                if *index >= runs.len() {
                    return Ok(None);
                }
                let end = (*index + *chunk_size).min(runs.len());
                let chunk = runs[*index..end].to_vec();
                *index = end;
                Ok(Some(CalibrationRunChunk {
                    runs: chunk,
                    complete: end == runs.len(),
                    report: None,
                }))
            }
            CalibrationRunStream::Query(stream) => stream
                .next()
                .transpose()
                .map(|chunk| {
                    chunk.map(|runs| CalibrationRunChunk {
                        runs: runs.numbers().to_vec(),
                        complete: runs.report().complete(),
                        report: Some(runs.report().clone()),
                    })
                })
                .map_err(Into::into),
        }
    }

    fn resolve_assignments(
        &mut self,
        runs: &[RunNumber],
    ) -> CCDBResult<BTreeMap<RunNumber, ResolvedAssignment>> {
        let selector = self.query.provenance.selector.clone();
        let reconstruction = match selector {
            CalibrationSelector::Defaults | CalibrationSelector::Direct => {
                return self.query.table.handle.resolve_assignments_with_options(
                    runs,
                    &self.query.provenance.variation,
                    self.query.provenance.as_of,
                    &self.query.execution,
                );
            }
            CalibrationSelector::Reconstruction(reconstruction) => reconstruction,
            CalibrationSelector::Conflict => {
                return Err(CCDBError::SelectorConflict(
                    "reconstruction selection cannot be combined with direct variation/as-of arguments"
                        .into(),
                ));
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
                    variation: self.query.provenance.variation.clone(),
                    timestamp: self.query.provenance.as_of,
                },
                ReconstructionSelection::Periods(selections) => selections
                    .get(&period)
                    .ok_or_else(|| {
                        CCDBError::InvalidPathError(format!(
                            "missing reconstruction selection for {period:?}"
                        ))
                    })?
                    .resolve_context(period)?,
            };
            assignments.extend(self.query.table.handle.resolve_assignments_with_options(
                &period_runs,
                &context.variation,
                context.timestamp,
                &self.query.execution,
            )?);
            self.resolved_reconstruction.insert(period, context);
        }
        Ok(assignments)
    }

    fn decode_entry(&mut self, assignment: ResolvedAssignment) -> CCDBResult<CalibrationEntry> {
        if self.query.execution.interrupted() {
            return Err(crate::execution::interrupted_error().into());
        }
        let id = assignment.constant_set.id();
        let payload = if let Some(payload) = self.payloads.get(&id) {
            Arc::clone(payload)
        } else {
            let layout = self.query.table.handle.column_layout()?;
            let n_rows = usize::try_from(self.query.table.metadata().n_rows()).map_err(|_| {
                CCDBError::InvalidPathError(format!(
                    "{}: negative row count",
                    self.query.table.path()
                ))
            })?;
            let payload = Arc::new(Data::from_vault(
                assignment.constant_set.vault(),
                layout,
                n_rows,
            )?);
            self.payloads.insert(id, Arc::clone(&payload));
            if self.payloads.len() > self.query.table.handle.payload_cache_capacity()
                && let Some(evicted) = self.payloads.keys().copied().find(|key| *key != id)
            {
                self.payloads.remove(&evicted);
            }
            payload
        };
        Ok(CalibrationEntry {
            assignment,
            payload,
        })
    }

    fn evaluate(&mut self, runs: Vec<RunNumber>, complete: bool) -> CCDBResult<CalibrationSeries> {
        let mut resolution_runs = runs.clone();
        if let Some(fallback) = self.query.provenance.fallback_run
            && !resolution_runs.contains(&fallback)
        {
            resolution_runs.push(fallback);
        }
        let assignments = self.resolve_assignments(&resolution_runs)?;
        let mut decoded = BTreeMap::new();
        for (run, assignment) in assignments {
            if self.query.execution.interrupted() {
                return Err(crate::execution::interrupted_error().into());
            }
            decoded.insert(run, self.decode_entry(assignment)?);
        }
        let missing_runs: Vec<_> = runs
            .iter()
            .copied()
            .filter(|run| !decoded.contains_key(run))
            .collect();
        if self.query.provenance.policy == crate::MissingDataPolicy::Strict
            && !missing_runs.is_empty()
        {
            return Err(CCDBError::MissingData(missing_runs.len()));
        }
        let mut entries: BTreeMap<_, _> = runs
            .iter()
            .filter_map(|run| decoded.get(run).cloned().map(|entry| (*run, entry)))
            .collect();
        let mut substitutions = Vec::new();
        if let Some(fallback) = self.query.provenance.fallback_run {
            let entry = decoded
                .get(&fallback)
                .cloned()
                .ok_or(CCDBError::MissingData(1))?;
            for &run in &missing_runs {
                entries.insert(run, entry.clone());
                substitutions.push((run, fallback));
            }
        }
        let mut provenance = self.query.provenance.clone();
        provenance.resolved_reconstruction = self.resolved_reconstruction.clone();
        provenance.run_report.clone_from(&self.run_report);
        let payloads = entries
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
}
impl Iterator for CalibrationStream {
    type Item = CCDBResult<CalibrationSeries>;
    fn next(&mut self) -> Option<Self::Item> {
        if self.failed {
            return None;
        }
        if self.query.execution.interrupted() {
            self.failed = true;
            return Some(Err(CCDBError::Retrieval {
                table: self.query.provenance.table.clone(),
                variation: self.query.provenance.variation.clone(),
                as_of: self.query.provenance.as_of,
                source: Box::new(CCDBError::from(crate::execution::interrupted_error())),
            }));
        }
        match self.next_runs() {
            Ok(Some(chunk)) => {
                if let Some(report) = chunk.report {
                    if let Some(combined) = &mut self.run_report {
                        combined.extend(report);
                    } else {
                        self.run_report = Some(report);
                    }
                }
                let result = self.evaluate(chunk.runs, chunk.complete);
                if result.is_err() {
                    self.failed = true;
                }
                Some(result.map_err(|source| CCDBError::Retrieval {
                    table: self.query.provenance.table.clone(),
                    variation: self.query.provenance.variation.clone(),
                    as_of: self.query.provenance.as_of,
                    source: Box::new(source),
                }))
            }
            Ok(None) => None,
            Err(source) => {
                self.failed = true;
                Some(Err(CCDBError::Retrieval {
                    table: self.query.provenance.table.clone(),
                    variation: self.query.provenance.variation.clone(),
                    as_of: self.query.provenance.as_of,
                    source: Box::new(source),
                }))
            }
        }
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
    payloads: BTreeMap<Id, Arc<Data>>,
    provenance: CalibrationProvenance,
    report: CalibrationReport,
}
impl CalibrationSeries {
    fn extend(&mut self, other: Self) {
        for (run, mut entry) in other.entries {
            let id = entry.constant_set_id();
            if let Some(payload) = self.payloads.get(&id) {
                entry.payload = Arc::clone(payload);
            } else {
                self.payloads.insert(id, Arc::clone(&entry.payload));
            }
            self.entries.insert(run, entry);
        }
        self.provenance.resolved_reconstruction = other.provenance.resolved_reconstruction;
        self.provenance.run_report = other.provenance.run_report;
        self.report.missing_runs.extend(other.report.missing_runs);
        self.report.substitutions.extend(other.report.substitutions);
        self.report
            .evaluated_runs
            .extend(other.report.evaluated_runs);
        self.report.complete = other.report.complete;
    }
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
    substitutions: Vec<(RunNumber, RunNumber)>,
    evaluated_runs: Vec<RunNumber>,
    complete: bool,
}
impl CalibrationReport {
    /// Requested numeric runs without an assignment.
    #[must_use]
    pub fn missing_runs(&self) -> &[RunNumber] {
        &self.missing_runs
    }
    /// Missing runs replaced by the assignment from the paired fallback run.
    #[must_use]
    pub fn substitutions(&self) -> &[(RunNumber, RunNumber)] {
        &self.substitutions
    }
    /// Requested runs examined in this chunk or completed series.
    #[must_use]
    pub fn evaluated_runs(&self) -> &[RunNumber] {
        &self.evaluated_runs
    }
    /// Whether the original request has been fully evaluated.
    #[must_use]
    pub const fn complete(&self) -> bool {
        self.complete
    }
}
