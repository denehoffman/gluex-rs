//! Numeric Run Selections and explicitly resolved recorded Run Sets.

use crate::{
    RunNumber, RunPeriod,
    rcdb::{RCDB, RCDBContext, RCDBResult},
};

/// Numeric Run Selection constructed without I/O or scientific criteria.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum RunSelection {
    /// Return conditions for every run stored in RCDB.
    All,
    /// Return conditions only for the exact run numbers in the list.
    Runs(Vec<RunNumber>),
    /// Return conditions for every run within the inclusive range.
    Range {
        /// Inclusive start run number.
        start: RunNumber,
        /// Inclusive end run number.
        end: RunNumber,
    },
}

impl RunSelection {
    /// True when no runs will be returned.
    #[must_use]
    pub const fn is_empty(&self) -> bool {
        matches!(self, Self::Runs(r) if r.is_empty())
            || matches!(self, Self::Range { start, end } if *start > *end)
    }
}

impl RunSelection {
    /// Select explicit numbers, sorted and deduplicated without database access.
    #[must_use]
    pub fn runs(runs: impl IntoIterator<Item = RunNumber>) -> Self {
        let mut runs: Vec<_> = runs.into_iter().collect();
        runs.sort_unstable();
        runs.dedup();
        Self::Runs(runs)
    }

    /// Select an inclusive numeric range without expanding it. Reversed bounds are empty.
    #[must_use]
    pub const fn range(start: RunNumber, end: RunNumber) -> Self {
        Self::Range { start, end }
    }

    /// Select a period's inclusive numeric bounds, without a scientific cut.
    #[must_use]
    pub fn period(period: RunPeriod) -> Self {
        Self::range(period.min_run(), period.max_run())
    }
}

/// Inputs identifying recorded membership resolution. Files must remain unchanged while in use.
#[derive(Debug, Clone)]
pub struct RunProvenance {
    source: String,
    selection: RunSelection,
    predicates: Vec<crate::rcdb::Expr>,
}

impl RunProvenance {
    /// Explicit predicates, combined with conjunction.
    #[must_use]
    pub fn predicates(&self) -> &[crate::rcdb::Expr] {
        &self.predicates
    }

    /// Filesystem identity of the RCDB source; not a historical snapshot.
    #[must_use]
    pub fn source(&self) -> &str {
        &self.source
    }

    /// Numeric scope requested before RCDB membership resolution.
    #[must_use]
    pub const fn selection(&self) -> &RunSelection {
        &self.selection
    }
}

/// Immutable, sorted recorded run numbers and their resolution inputs.
#[derive(Debug, Clone)]
pub struct RunSet {
    numbers: Vec<RunNumber>,
    provenance: RunProvenance,
    report: RunReport,
}

impl RunSet {
    /// Completed evaluation report identifying final-unknown exclusions.
    #[must_use]
    pub const fn report(&self) -> &RunReport {
        &self.report
    }

    /// Sorted, unique recorded run numbers.
    #[must_use]
    pub fn numbers(&self) -> &[RunNumber] {
        &self.numbers
    }

    /// Inputs used to resolve this collection.
    #[must_use]
    pub const fn provenance(&self) -> &RunProvenance {
        &self.provenance
    }
}

/// Reusable lazy recorded-membership query bound to its original source.
#[derive(Clone)]
pub struct RunQuery {
    reader: RCDB,
    selection: RunSelection,
    predicates: Vec<crate::rcdb::Expr>,
}

impl RunQuery {
    /// Project named conditions into a distinct lazy query. Duplicate names are deduplicated.
    ///
    /// # Errors
    /// Rejects empty projections and unknown names using the bound source's catalog.
    pub fn select(
        &self,
        fields: impl IntoIterator<Item = impl AsRef<str>>,
    ) -> RCDBResult<ConditionQuery> {
        let catalog = self.reader.conditions();
        let mut names = Vec::new();
        for field in fields {
            let name = field.as_ref();
            if catalog.get(name).is_none() {
                return Err(crate::rcdb::RCDBError::ConditionTypeNotFound(name.into()));
            }
            if !names.iter().any(|n| n == name) {
                names.push(name.to_owned());
            }
        }
        if names.is_empty() {
            return Err(crate::rcdb::RCDBError::EmptyConditionList);
        }
        Ok(ConditionQuery {
            query: self.clone(),
            fields: names,
        })
    }

    /// Return a new query with an additional predicate; the original is unchanged.
    #[must_use]
    pub fn filter(&self, predicate: crate::rcdb::Expr) -> Self {
        let mut query = self.clone();
        query.predicates.push(predicate);
        query
    }

    pub(crate) fn new(reader: RCDB, selection: RunSelection) -> Self {
        let selection = match selection {
            RunSelection::Runs(runs) => RunSelection::runs(runs),
            selection => selection,
        };
        Self {
            reader,
            selection,
            predicates: Vec::new(),
        }
    }

    /// Inspect the numeric scope without executing the query.
    #[must_use]
    pub const fn selection(&self) -> &RunSelection {
        &self.selection
    }

    /// Inspect source and selection without executing the query.
    #[must_use]
    pub fn provenance(&self) -> RunProvenance {
        RunProvenance {
            source: self.reader.connection_path().to_owned(),
            selection: self.selection.clone(),
            predicates: self.predicates.clone(),
        }
    }

    /// Materialize recorded membership with no implicit approval or production cut.
    ///
    /// # Errors
    /// Returns a contextual RCDB error if execution or run decoding fails.
    pub fn collect(&self) -> RCDBResult<RunSet> {
        let predicate = crate::rcdb::conditions::all(self.predicates.clone());
        let context = RCDBContext::from_selection(self.selection.clone()).filter(predicate.clone());
        let unknown_context =
            RCDBContext::from_selection(self.selection.clone()).filter(predicate.unknown());
        Ok(RunSet {
            numbers: self.reader.fetch_runs(&context)?,
            provenance: self.provenance(),
            report: RunReport {
                unknown_runs: self.reader.fetch_runs(&unknown_context)?,
            },
        })
    }
}

impl std::fmt::Debug for RunQuery {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("RunQuery")
            .field("source", &self.reader.connection_path())
            .field("selection", &self.selection)
            .field("predicates", &self.predicates)
            .finish()
    }
}

/// An immutable Condition Definition with its database-native metadata.
pub type ConditionDefinition = crate::rcdb::models::ConditionTypeMeta;

/// Immutable, name-indexed catalog of dynamic Condition Definitions.
#[derive(Debug, Clone)]
pub struct ConditionCatalog(std::collections::BTreeMap<String, ConditionDefinition>);

impl ConditionCatalog {
    pub(crate) fn new(
        definitions: impl IntoIterator<Item = (String, ConditionDefinition)>,
    ) -> Self {
        Self(definitions.into_iter().collect())
    }

    /// Look up a definition, returning `None` for an unknown name.
    #[must_use]
    pub fn get(&self, name: &str) -> Option<&ConditionDefinition> {
        self.0.get(name)
    }

    /// Iterate condition names in lexical order.
    #[must_use]
    pub fn keys(&self) -> impl ExactSizeIterator<Item = &String> {
        self.0.keys()
    }

    /// Iterate names and definitions in lexical order.
    #[must_use]
    pub fn items(&self) -> impl ExactSizeIterator<Item = (&String, &ConditionDefinition)> {
        self.0.iter()
    }

    /// Number of definitions in the catalog.
    #[must_use]
    pub fn len(&self) -> usize {
        self.0.len()
    }

    /// Whether the catalog contains no definitions.
    #[must_use]
    pub fn is_empty(&self) -> bool {
        self.0.is_empty()
    }
}

impl std::ops::Index<&str> for ConditionCatalog {
    type Output = ConditionDefinition;
    fn index(&self, name: &str) -> &Self::Output {
        &self.0[name]
    }
}

/// Completed run evaluation diagnostics. Only final-unknown predicates are reported here.
#[derive(Debug, Clone)]
pub struct RunReport {
    unknown_runs: Vec<RunNumber>,
}
impl RunReport {
    /// Recorded runs excluded because the complete predicate evaluated to unknown.
    #[must_use]
    pub fn unknown_runs(&self) -> &[RunNumber] {
        &self.unknown_runs
    }
}

/// Lazy named projection of a Run Query; construction and inspection do not read values.
#[derive(Debug, Clone)]
pub struct ConditionQuery {
    query: RunQuery,
    fields: Vec<String>,
}
impl ConditionQuery {
    /// Captured query inputs and ordered projected names, without evaluation.
    #[must_use]
    pub fn provenance(&self) -> ConditionProvenance {
        ConditionProvenance {
            runs: self.query.provenance(),
            fields: self.fields.clone(),
        }
    }
    /// Collect optional condition columns aligned with recorded runs. Missing cells are reported.
    ///
    /// # Errors
    /// Returns contextual errors for malformed values or failed database execution.
    pub fn collect(&self) -> RCDBResult<ConditionResults> {
        let runs = self.query.collect()?;
        let rows = self.query.reader.fetch(
            &self.fields,
            &RCDBContext::from_selection(RunSelection::runs(runs.numbers().iter().copied())),
        )?;
        let mut columns = std::collections::BTreeMap::new();
        let mut missing_values = Vec::new();
        for name in &self.fields {
            let column = runs
                .numbers()
                .iter()
                .map(|run| {
                    let value = rows.get(run).and_then(|row| row.get(name)).cloned();
                    if value.is_none() {
                        missing_values.push((*run, name.clone()));
                    }
                    value
                })
                .collect();
            columns.insert(name.clone(), column);
        }
        missing_values.sort();
        Ok(ConditionResults {
            runs,
            columns,
            provenance: self.provenance(),
            report: ConditionReport { missing_values },
        })
    }
}

/// Inputs used to produce aligned condition columns.
#[derive(Debug, Clone)]
pub struct ConditionProvenance {
    runs: RunProvenance,
    fields: Vec<String>,
}
impl ConditionProvenance {
    /// Recorded-membership inputs, including source, numeric scope and predicates.
    #[must_use]
    pub const fn runs(&self) -> &RunProvenance {
        &self.runs
    }
    /// Ordered projected condition names.
    #[must_use]
    pub fn fields(&self) -> &[String] {
        &self.fields
    }
}

/// Completed diagnostics for missing requested condition cells.
#[derive(Debug, Clone)]
pub struct ConditionReport {
    missing_values: Vec<(RunNumber, String)>,
}
impl ConditionReport {
    /// Missing (run, condition name) pairs in run/name order; null values count as missing.
    #[must_use]
    pub fn missing_values(&self) -> &[(RunNumber, String)] {
        &self.missing_values
    }
}

/// Immutable optional condition columns aligned with a resolved Run Set.
#[derive(Debug, Clone)]
pub struct ConditionResults {
    runs: RunSet,
    columns: std::collections::BTreeMap<String, Vec<Option<crate::rcdb::Value>>>,
    provenance: ConditionProvenance,
    report: ConditionReport,
}
impl ConditionResults {
    /// Recorded runs in column order, with predicate-exclusion diagnostics.
    #[must_use]
    pub const fn runs(&self) -> &RunSet {
        &self.runs
    }
    /// Inputs used for this completed collection.
    #[must_use]
    pub const fn provenance(&self) -> &ConditionProvenance {
        &self.provenance
    }
    /// Missing projected cells.
    #[must_use]
    pub const fn report(&self) -> &ConditionReport {
        &self.report
    }
    /// Read an optional cell by run number and projected name.
    ///
    /// # Errors
    /// Rejects runs outside the result and names outside the projection.
    pub fn get(&self, run: RunNumber, name: &str) -> RCDBResult<Option<&crate::rcdb::Value>> {
        let column = self.column(name)?;
        let index = self
            .runs
            .numbers()
            .binary_search(&run)
            .map_err(|_| crate::rcdb::RCDBError::RunNotInResults(run))?;
        Ok(column[index].as_ref())
    }
    /// Read a named column in the same order as `runs().numbers()`.
    ///
    /// # Errors
    /// Rejects names outside the projection, distinguishing them from missing cells.
    pub fn column(&self, name: &str) -> RCDBResult<&[Option<crate::rcdb::Value>]> {
        self.columns
            .get(name)
            .map(Vec::as_slice)
            .ok_or_else(|| crate::rcdb::RCDBError::ConditionTypeNotFound(name.into()))
    }
}
