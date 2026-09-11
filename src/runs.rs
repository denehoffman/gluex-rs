//! Numeric Run Selections and explicitly resolved recorded Run Sets.

use crate::{
    DatabaseResult, RunNumber, RunPeriod,
    rcdb::{RCDB, RCDBContext},
};

mod catalog;
mod evaluation;
mod results;

macro_rules! validated_text {
    ($name:ident, $description:literal) => {
        #[doc = $description]
        #[derive(Debug, Clone, PartialEq, Eq, PartialOrd, Ord, Hash, serde::Serialize)]
        #[serde(transparent)]
        pub struct $name(String);

        impl $name {
            /// Borrow the validated text.
            #[must_use]
            pub fn as_str(&self) -> &str {
                &self.0
            }

            #[allow(dead_code)]
            pub(crate) fn trusted(value: String) -> Self {
                debug_assert!(!value.trim().is_empty());
                Self(value)
            }
        }

        impl TryFrom<String> for $name {
            type Error = &'static str;

            fn try_from(value: String) -> Result<Self, Self::Error> {
                if value.trim().is_empty() {
                    Err("value must not be empty")
                } else {
                    Ok(Self(value))
                }
            }
        }

        impl<'de> serde::Deserialize<'de> for $name {
            fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
            where
                D: serde::Deserializer<'de>,
            {
                Self::try_from(<String as serde::Deserialize>::deserialize(deserializer)?)
                    .map_err(serde::de::Error::custom)
            }
        }
    };
}

validated_text!(SourceIdentity, "Validated database source identity.");
validated_text!(Variation, "Validated calibration variation name.");

/// Validated absolute calibration path.
#[derive(
    Debug, Clone, PartialEq, Eq, PartialOrd, Ord, Hash, serde::Serialize, serde::Deserialize,
)]
#[serde(try_from = "String", into = "String")]
pub struct CalibrationPath(String);
impl CalibrationPath {
    /// Borrow the absolute path.
    #[must_use]
    pub fn as_str(&self) -> &str {
        &self.0
    }
}
impl TryFrom<String> for CalibrationPath {
    type Error = &'static str;

    fn try_from(value: String) -> Result<Self, Self::Error> {
        let valid = value.starts_with('/')
            && (value == "/"
                || value
                    .split('/')
                    .skip(1)
                    .all(|segment| !segment.is_empty() && segment != "." && segment != ".."));
        valid
            .then_some(Self(value))
            .ok_or("calibration path must be absolute with non-empty canonical segments")
    }
}

impl From<CalibrationPath> for String {
    fn from(path: CalibrationPath) -> Self {
        path.0
    }
}

/// Stability status of a documented scientific procedure.
#[derive(Debug, Clone, Copy, PartialEq, Eq, serde::Serialize, serde::Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum ProcedureStatus {
    /// The procedure remains subject to validation or change.
    Provisional,
    /// The procedure is supported as a stable product contract.
    Stable,
}

/// Structured accounting shared by run-bearing results.
#[derive(Debug, Clone, serde::Serialize, serde::Deserialize)]
pub struct RunAccounting {
    evaluated_runs: Vec<RunNumber>,
    omissions: Vec<RunOmission>,
    complete: bool,
}
impl RunAccounting {
    /// Recorded candidates examined by the terminal operation.
    #[must_use]
    pub fn evaluated_runs(&self) -> &[RunNumber] {
        &self.evaluated_runs
    }
    /// Structured omissions from the result.
    #[must_use]
    pub fn omissions(&self) -> &[RunOmission] {
        &self.omissions
    }
    /// Whether the original request was evaluated completely.
    #[must_use]
    pub const fn complete(&self) -> bool {
        self.complete
    }
}

/// Why a recorded run was omitted from a Run Set.
#[derive(Debug, Clone, Copy, PartialEq, Eq, serde::Serialize, serde::Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum RunOmissionReason {
    /// The complete predicate evaluated to SQL unknown.
    UnknownPredicate,
}

/// One structured run omission.
#[derive(Debug, Clone, Copy, PartialEq, Eq, serde::Serialize, serde::Deserialize)]
pub struct RunOmission {
    run: RunNumber,
    reason: RunOmissionReason,
}
impl RunOmission {
    /// Omitted run number.
    #[must_use]
    pub const fn run(self) -> RunNumber {
        self.run
    }
    /// Structured reason for the omission.
    #[must_use]
    pub const fn reason(self) -> RunOmissionReason {
        self.reason
    }
}

/// Immutable Missing Data Policy configuration retained by a query.
#[derive(Debug, Clone, serde::Serialize, serde::Deserialize)]
pub struct MissingDataConfig {
    policy: MissingDataPolicy,
    fallback_fields: Vec<String>,
}
impl MissingDataConfig {
    /// Declared handling policy.
    #[must_use]
    pub const fn policy(&self) -> MissingDataPolicy {
        self.policy
    }
    /// Fields with explicit caller-provided fallback values.
    #[must_use]
    pub fn fallback_fields(&self) -> &[String] {
        &self.fallback_fields
    }
}

/// One missing condition cell identified without a positional tuple.
#[derive(Debug, Clone, PartialEq, Eq, serde::Serialize, serde::Deserialize)]
pub struct ConditionOmission {
    run: RunNumber,
    condition: String,
}
impl ConditionOmission {
    /// Run containing the missing cell.
    #[must_use]
    pub const fn run(&self) -> RunNumber {
        self.run
    }
    /// Condition name for the missing cell.
    #[must_use]
    pub fn condition(&self) -> &str {
        &self.condition
    }
}

/// Declared treatment of unavailable requested scientific inputs.
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq, serde::Serialize, serde::Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum MissingDataPolicy {
    /// Preserve omissions in the result report.
    #[default]
    Report,
    /// Reject any completed chunk containing an omission.
    Strict,
    /// Apply only caller-supplied substitutions and report each use.
    Fallback,
}
impl MissingDataPolicy {
    /// Stable user-facing policy name.
    #[must_use]
    pub const fn as_str(self) -> &'static str {
        match self {
            Self::Report => "report",
            Self::Strict => "strict",
            Self::Fallback => "fallback",
        }
    }
}

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
    source: SourceIdentity,
    selection: RunSelection,
    predicates: Vec<crate::RunPredicate>,
}

impl RunProvenance {
    /// Explicit predicates, combined with conjunction.
    #[must_use]
    pub fn predicates(&self) -> &[crate::RunPredicate] {
        &self.predicates
    }

    /// Filesystem identity of the RCDB source; not a historical snapshot.
    #[must_use]
    pub fn source(&self) -> &str {
        self.source.as_str()
    }

    /// Validated source identity.
    #[must_use]
    pub const fn source_identity(&self) -> &SourceIdentity {
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
    predicates: Vec<crate::RunPredicate>,
    execution: crate::ExecutionOptions,
}

impl RunQuery {
    /// Project named conditions into a distinct lazy query. Duplicate names are deduplicated.
    ///
    /// # Errors
    /// Rejects empty projections and unknown names using the bound source's catalog.
    pub fn select(
        &self,
        fields: impl IntoIterator<Item = impl AsRef<str>>,
    ) -> DatabaseResult<ConditionQuery> {
        let catalog = self.reader.conditions();
        let mut names = Vec::new();
        for field in fields {
            let name = field.as_ref();
            if catalog.get(name).is_none() {
                return Err(crate::rcdb::RCDBError::ConditionTypeNotFound(name.into()).into());
            }
            if !names.iter().any(|n| n == name) {
                names.push(name.to_owned());
            }
        }
        if names.is_empty() {
            return Err(crate::rcdb::RCDBError::EmptyConditionList.into());
        }
        Ok(ConditionQuery {
            query: self.clone(),
            fields: names,
            policy: MissingDataPolicy::Report,
            fallbacks: std::collections::BTreeMap::new(),
        })
    }

    /// Return a new query with an additional Condition Predicate; the original is unchanged.
    #[must_use]
    pub fn where_predicate(&self, predicate: crate::RunPredicate) -> Self {
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
            execution: crate::ExecutionOptions::default(),
        }
    }

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

    pub(crate) fn with_execution(&self, execution: crate::ExecutionOptions) -> Self {
        let mut query = self.clone();
        query.execution = execution;
        query
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
            source: SourceIdentity::trusted(self.reader.connection_path().to_owned()),
            selection: self.selection.clone(),
            predicates: self.predicates.clone(),
        }
    }

    /// Materialize recorded membership with no implicit approval or production cut.
    ///
    /// # Errors
    /// Returns a contextual RCDB error if execution or run decoding fails.
    pub fn collect(&self) -> DatabaseResult<RunSet> {
        crate::execution::execute_terminal(self, Self::collect_inner)
    }

    fn collect_inner(&self) -> DatabaseResult<RunSet> {
        let mut numbers = Vec::new();
        let mut unknown_runs = Vec::new();
        let mut evaluated_runs = Vec::new();
        for chunk in self.stream(1024)? {
            let chunk = chunk?;
            numbers.extend_from_slice(chunk.numbers());
            unknown_runs.extend_from_slice(chunk.report().unknown_runs());
            evaluated_runs.extend_from_slice(chunk.report().evaluated_runs());
        }
        Ok(RunSet {
            numbers,
            provenance: self.provenance(),
            report: RunReport {
                unknown_runs,
                evaluated_runs,
                complete: true,
            },
        })
    }

    fn evaluate_candidates(
        &self,
        candidates: Vec<RunNumber>,
        complete: bool,
    ) -> DatabaseResult<RunSet> {
        evaluation::evaluate_runs(self, candidates, complete)
            .map(|evaluated| results::run_set(self.provenance(), evaluated))
    }

    /// Iterate bounded chunks of recorded candidates. Dropping the iterator releases its reader.
    ///
    /// # Errors
    /// A chunk size of zero is invalid.
    pub fn stream(&self, chunk_size: usize) -> DatabaseResult<RunStream> {
        if chunk_size == 0 {
            return Err(crate::rcdb::RCDBError::InvalidValue(
                "stream chunk size must be positive".into(),
            )
            .into());
        }
        Ok(RunStream {
            budget: crate::execution::StreamBudget::new(&self.execution),
            query: self.clone(),
            chunk_size,
            offset: 0,
            complete: false,
        })
    }

    /// Return the first matching run without evaluating the remaining candidates.
    ///
    /// # Errors
    /// Returns a contextual RCDB error if candidate or predicate evaluation fails.
    pub fn first(&self) -> DatabaseResult<Option<RunNumber>> {
        crate::execution::execute_terminal(self, Self::first_inner)
    }

    fn first_inner(&self) -> DatabaseResult<Option<RunNumber>> {
        for chunk in self.stream(1)? {
            if let Some(run) = chunk?.numbers().first() {
                return Ok(Some(*run));
            }
        }
        Ok(None)
    }

    /// Count all matching runs without retaining a complete Run Set.
    ///
    /// # Errors
    /// Returns a contextual RCDB error if candidate or predicate evaluation fails.
    pub fn count(&self) -> DatabaseResult<usize> {
        crate::execution::execute_terminal(self, Self::count_inner)
    }

    fn count_inner(&self) -> DatabaseResult<usize> {
        self.stream(1024)?
            .try_fold(0usize, |count, chunk| Ok(count + chunk?.numbers().len()))
    }

    /// Return the only matching run, rejecting zero or multiple matches.
    ///
    /// # Errors
    /// Returns a cardinality error or a contextual RCDB evaluation error.
    pub fn one(&self) -> DatabaseResult<RunNumber> {
        crate::execution::execute_terminal(self, Self::one_inner)
    }

    fn one_inner(&self) -> DatabaseResult<RunNumber> {
        let mut found = Vec::with_capacity(2);
        for chunk in self.stream(2)? {
            found.extend_from_slice(chunk?.numbers());
            if found.len() > 1 {
                return Err(crate::rcdb::RCDBError::InvalidCardinality(found.len()).into());
            }
        }
        found
            .first()
            .copied()
            .ok_or_else(|| crate::rcdb::RCDBError::InvalidCardinality(0).into())
    }
}

impl crate::execution::TerminalQuery for RunQuery {
    type Error = crate::DatabaseError;

    fn execution_options(&self) -> &crate::ExecutionOptions {
        &self.execution
    }

    fn with_execution_options(&self, options: crate::ExecutionOptions) -> Self {
        self.with_execution(options)
    }

    fn interruption_error(&self, failure: crate::ExecutionError) -> crate::DatabaseError {
        crate::rcdb::RCDBError::from(failure).into()
    }
}

/// Bounded iterator over completed portions of a Run Query.
pub struct RunStream {
    query: RunQuery,
    budget: crate::execution::StreamBudget,
    chunk_size: usize,
    offset: usize,
    complete: bool,
}
impl Iterator for RunStream {
    type Item = DatabaseResult<RunSet>;
    fn next(&mut self) -> Option<Self::Item> {
        if self.complete {
            return None;
        }
        let query = self.query.clone();
        let mut budget = std::mem::take(&mut self.budget);
        let result = budget.execute(&query, |active| self.next_inner(active));
        self.budget = budget;
        if result.is_err() {
            self.complete = true;
        }
        result.transpose()
    }
}

impl RunStream {
    pub(crate) fn set_execution(&mut self, execution: crate::ExecutionOptions) {
        self.query.execution = execution;
    }

    fn next_inner(&mut self, query: &RunQuery) -> DatabaseResult<Option<RunSet>> {
        while !self.complete {
            let context = RCDBContext::from_selection(query.selection.clone());
            let page = query.reader.fetch_run_page_with_options(
                &context,
                self.chunk_size,
                self.offset,
                &query.execution,
            );
            let (candidates, consumed, complete) = match page {
                Ok(page) => page,
                Err(error) => {
                    self.complete = true;
                    return Err(error.into());
                }
            };
            self.offset = self.offset.saturating_add(consumed);
            self.complete = complete;
            if candidates.is_empty() {
                if complete {
                    return query.evaluate_candidates(Vec::new(), true).map(Some);
                }
                continue;
            }
            return query.evaluate_candidates(candidates, complete).map(Some);
        }
        Ok(None)
    }
}

impl std::fmt::Debug for RunQuery {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("RunQuery")
            .field("source", &self.reader.connection_path())
            .field("selection", &self.selection)
            .field("predicates", &self.predicates)
            .field("execution", &self.execution)
            .finish()
    }
}

/// Storage-independent value category for a named run condition.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ConditionValueType {
    /// Signed integer.
    Int,
    /// Floating-point number.
    Float,
    /// Boolean.
    Bool,
    /// UTC timestamp.
    Time,
    /// UTF-8 text.
    String,
    /// JSON text.
    Json,
    /// Opaque text/blob payload.
    Blob,
}

/// Composable, backend-neutral three-valued Condition Predicate.
#[derive(Debug, Clone)]
pub struct RunPredicate(pub(crate) crate::rcdb::conditions::Expr);

impl std::fmt::Display for RunPredicate {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        self.0.fmt(formatter)
    }
}

impl std::ops::Not for RunPredicate {
    type Output = Self;
    fn not(self) -> Self::Output {
        Self(!self.0)
    }
}

impl std::ops::BitAnd for RunPredicate {
    type Output = Self;
    fn bitand(self, rhs: Self) -> Self::Output {
        Self(self.0 & rhs.0)
    }
}

impl std::ops::BitOr for RunPredicate {
    type Output = Self;
    fn bitor(self, rhs: Self) -> Self::Output {
        Self(self.0 | rhs.0)
    }
}

impl ConditionValueType {
    /// Stable user-facing type name.
    #[must_use]
    pub const fn as_str(self) -> &'static str {
        match self {
            Self::Int => "int",
            Self::Float => "float",
            Self::Bool => "bool",
            Self::Time => "time",
            Self::String => "string",
            Self::Json => "json",
            Self::Blob => "blob",
        }
    }
}

impl From<crate::rcdb::ValueType> for ConditionValueType {
    fn from(value: crate::rcdb::ValueType) -> Self {
        match value {
            crate::rcdb::ValueType::Int => Self::Int,
            crate::rcdb::ValueType::Float => Self::Float,
            crate::rcdb::ValueType::Bool => Self::Bool,
            crate::rcdb::ValueType::Time => Self::Time,
            crate::rcdb::ValueType::String => Self::String,
            crate::rcdb::ValueType::Json => Self::Json,
            crate::rcdb::ValueType::Blob => Self::Blob,
        }
    }
}

/// Immutable backend-neutral run-condition value.
#[derive(Debug, Clone)]
pub struct ConditionValue(crate::rcdb::Value);

impl ConditionValue {
    /// Declared value category.
    #[must_use]
    pub fn value_type(&self) -> ConditionValueType {
        self.0.value_type().into()
    }
    /// Text payload, when applicable.
    #[must_use]
    pub fn as_string(&self) -> Option<&str> {
        self.0.as_string()
    }
    /// Integer payload, when applicable.
    #[must_use]
    pub fn as_int(&self) -> Option<i64> {
        self.0.as_int()
    }
    /// Floating-point payload, when applicable.
    #[must_use]
    pub fn as_float(&self) -> Option<f64> {
        self.0.as_float()
    }
    /// Boolean payload, when applicable.
    #[must_use]
    pub fn as_bool(&self) -> Option<bool> {
        self.0.as_bool()
    }
    /// UTC timestamp payload, when applicable.
    #[must_use]
    pub fn as_time(&self) -> Option<chrono::DateTime<chrono::Utc>> {
        self.0.as_time()
    }
}

/// Immutable backend-neutral description of a named run condition.
#[derive(Debug, Clone)]
pub struct ConditionDefinition(crate::rcdb::models::ConditionTypeMeta);

impl ConditionDefinition {
    /// Database-native identifier retained for advanced inspection.
    #[must_use]
    pub const fn id(&self) -> crate::Id {
        self.0.id()
    }

    /// Dynamic condition name.
    #[must_use]
    pub fn name(&self) -> &str {
        self.0.name()
    }

    /// Declared value type.
    #[must_use]
    pub fn value_type(&self) -> ConditionValueType {
        self.0.value_type().into()
    }

    /// Native creation timestamp text, or an empty string.
    #[must_use]
    pub fn created(&self) -> String {
        self.0.created()
    }

    /// Available description, or an empty string.
    #[must_use]
    pub fn description(&self) -> &str {
        self.0.description()
    }

    /// Build a predicate matching recorded non-null values.
    #[must_use]
    pub fn is_present(&self) -> crate::RunPredicate {
        crate::RunPredicate(self.0.is_present())
    }

    /// Build a predicate matching absent or null values.
    #[must_use]
    pub fn is_missing(&self) -> crate::RunPredicate {
        crate::RunPredicate(self.0.is_missing())
    }

    /// Build a typed equality predicate.
    ///
    /// # Errors
    /// Rejects values incompatible with this Condition Definition's value type.
    pub fn eq(
        &self,
        value: impl Into<crate::ConditionOperand>,
    ) -> DatabaseResult<crate::RunPredicate> {
        self.0
            .eq(value)
            .map(crate::RunPredicate)
            .map_err(Into::into)
    }

    /// Build a typed inequality predicate.
    ///
    /// # Errors
    /// Rejects values incompatible with this Condition Definition's value type.
    pub fn ne(
        &self,
        value: impl Into<crate::ConditionOperand>,
    ) -> DatabaseResult<crate::RunPredicate> {
        self.0
            .ne(value)
            .map(crate::RunPredicate)
            .map_err(Into::into)
    }

    /// Build a typed greater-than predicate.
    ///
    /// # Errors
    /// Rejects values incompatible with this Condition Definition's value type.
    pub fn gt(
        &self,
        value: impl Into<crate::ConditionOperand>,
    ) -> DatabaseResult<crate::RunPredicate> {
        self.0
            .gt(value)
            .map(crate::RunPredicate)
            .map_err(Into::into)
    }

    /// Build a typed greater-than-or-equal predicate.
    ///
    /// # Errors
    /// Rejects values incompatible with this Condition Definition's value type.
    pub fn ge(
        &self,
        value: impl Into<crate::ConditionOperand>,
    ) -> DatabaseResult<crate::RunPredicate> {
        self.0
            .ge(value)
            .map(crate::RunPredicate)
            .map_err(Into::into)
    }

    /// Build a typed less-than predicate.
    ///
    /// # Errors
    /// Rejects values incompatible with this Condition Definition's value type.
    pub fn lt(
        &self,
        value: impl Into<crate::ConditionOperand>,
    ) -> DatabaseResult<crate::RunPredicate> {
        self.0
            .lt(value)
            .map(crate::RunPredicate)
            .map_err(Into::into)
    }

    /// Build a typed less-than-or-equal predicate.
    ///
    /// # Errors
    /// Rejects values incompatible with this Condition Definition's value type.
    pub fn le(
        &self,
        value: impl Into<crate::ConditionOperand>,
    ) -> DatabaseResult<crate::RunPredicate> {
        self.0
            .le(value)
            .map(crate::RunPredicate)
            .map_err(Into::into)
    }
}

/// Immutable, name-indexed catalog of dynamic Condition Definitions.
#[derive(Debug, Clone)]
pub struct ConditionCatalog(std::collections::BTreeMap<String, ConditionDefinition>);

impl ConditionCatalog {
    pub(crate) fn new(
        definitions: impl IntoIterator<Item = (String, crate::rcdb::models::ConditionTypeMeta)>,
    ) -> Self {
        Self(catalog::definitions(definitions))
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
    evaluated_runs: Vec<RunNumber>,
    complete: bool,
}
impl RunReport {
    pub(crate) fn extend(&mut self, other: Self) {
        self.unknown_runs.extend(other.unknown_runs);
        self.evaluated_runs.extend(other.evaluated_runs);
        self.complete = other.complete;
    }
    /// Recorded runs excluded because the complete predicate evaluated to unknown.
    #[must_use]
    pub fn unknown_runs(&self) -> &[RunNumber] {
        &self.unknown_runs
    }
    /// Recorded candidates examined to produce this result or streamed chunk.
    #[must_use]
    pub fn evaluated_runs(&self) -> &[RunNumber] {
        &self.evaluated_runs
    }
    /// Whether all candidates in the original query have been evaluated.
    #[must_use]
    pub const fn complete(&self) -> bool {
        self.complete
    }

    /// Structured run accounting equivalent to the legacy report accessors.
    #[must_use]
    pub fn accounting(&self) -> RunAccounting {
        RunAccounting {
            evaluated_runs: self.evaluated_runs.clone(),
            omissions: self
                .unknown_runs
                .iter()
                .copied()
                .map(|run| RunOmission {
                    run,
                    reason: RunOmissionReason::UnknownPredicate,
                })
                .collect(),
            complete: self.complete,
        }
    }
}

/// Lazy named projection of a Run Query; construction and inspection do not read values.
#[derive(Debug, Clone)]
pub struct ConditionQuery {
    query: RunQuery,
    fields: Vec<String>,
    policy: MissingDataPolicy,
    fallbacks: std::collections::BTreeMap<String, ConditionValue>,
}
impl ConditionQuery {
    #[cfg(feature = "python")]
    pub(crate) fn query_with_timeout(&self, duration: std::time::Duration) -> Self {
        let mut query = self.clone();
        query.query = query.query.with_timeout(duration);
        query
    }
    pub(crate) fn definition(&self, name: &str) -> DatabaseResult<ConditionDefinition> {
        Ok(self
            .query
            .reader
            .conditions()
            .get(name)
            .cloned()
            .ok_or_else(|| crate::rcdb::RCDBError::ConditionTypeNotFound(name.into()))?)
    }
    /// Captured query inputs and ordered projected names, without evaluation.
    #[must_use]
    pub fn provenance(&self) -> ConditionProvenance {
        ConditionProvenance {
            runs: self.query.provenance(),
            fields: self.fields.clone(),
            policy: self.policy,
            fallback_fields: self.fallbacks.keys().cloned().collect(),
        }
    }
    /// Collect optional condition columns aligned with recorded runs. Missing cells are reported.
    ///
    /// # Errors
    /// Returns contextual errors for malformed values or failed database execution.
    pub fn collect(&self) -> DatabaseResult<ConditionResults> {
        crate::execution::execute_terminal(self, Self::collect_inner)
    }

    fn collect_inner(&self) -> DatabaseResult<ConditionResults> {
        let mut chunks = self.stream(1024)?;
        let Some(first) = chunks.next() else {
            return self.collect_for_runs(RunSet {
                numbers: Vec::new(),
                provenance: self.query.provenance(),
                report: RunReport {
                    unknown_runs: Vec::new(),
                    evaluated_runs: Vec::new(),
                    complete: true,
                },
            });
        };
        let mut result = first?;
        for chunk in chunks {
            result.extend(chunk?);
        }
        result.runs.report.complete = true;
        Ok(result)
    }

    fn collect_for_runs(&self, runs: RunSet) -> DatabaseResult<ConditionResults> {
        evaluation::collect_conditions(self, &runs)
            .map(|evaluated| results::condition_results(runs, self.provenance(), evaluated))
    }

    /// Return a strict query that rejects missing projected values.
    #[must_use]
    pub fn strict(&self) -> Self {
        let mut query = self.clone();
        query.policy = MissingDataPolicy::Strict;
        query.fallbacks.clear();
        query
    }

    /// Supply a typed fallback for one projected field and report every substitution.
    ///
    /// # Errors
    /// Rejects unknown fields and fallback values that do not match the field type.
    pub fn fill(&self, name: &str, operand: crate::ConditionOperand) -> DatabaseResult<Self> {
        if !self.fields.iter().any(|field| field == name) {
            return Err(crate::rcdb::RCDBError::ConditionTypeNotFound(name.into()).into());
        }
        let definition = self.definition(name)?;
        let value = crate::rcdb::Value::from_operand(definition.0.value_type(), operand)
            .map(ConditionValue)?;
        let mut query = self.clone();
        query.policy = MissingDataPolicy::Fallback;
        query.fallbacks.insert(name.into(), value);
        Ok(query)
    }

    /// Iterate aligned projected-result chunks with bounded candidate memory.
    ///
    /// # Errors
    /// Rejects a zero chunk size.
    pub fn stream(&self, chunk_size: usize) -> DatabaseResult<ConditionStream> {
        Ok(ConditionStream {
            budget: crate::execution::StreamBudget::new(&self.query.execution),
            query: self.clone(),
            runs: self.query.stream(chunk_size)?,
        })
    }

    /// Return the first matching projected row without evaluating the rest.
    ///
    /// # Errors
    /// Returns a contextual RCDB evaluation or value-decoding error.
    pub fn first(&self) -> DatabaseResult<Option<ConditionResults>> {
        crate::execution::execute_terminal(self, Self::first_inner)
    }

    fn first_inner(&self) -> DatabaseResult<Option<ConditionResults>> {
        let Some(chunk) = self.query.first()? else {
            return Ok(None);
        };
        let runs = self.query.evaluate_candidates(vec![chunk], false)?;
        self.collect_for_runs(runs).map(Some)
    }

    /// Count matching rows without retaining projected values.
    ///
    /// # Errors
    /// Returns a contextual RCDB evaluation error.
    pub fn count(&self) -> DatabaseResult<usize> {
        crate::execution::execute_terminal(self, |query| query.query.count())
    }

    /// Return exactly one projected row, rejecting any other cardinality.
    ///
    /// # Errors
    /// Returns a cardinality, RCDB evaluation, or value-decoding error.
    pub fn one(&self) -> DatabaseResult<ConditionResults> {
        crate::execution::execute_terminal(self, Self::one_inner)
    }

    fn one_inner(&self) -> DatabaseResult<ConditionResults> {
        let run = self.query.one()?;
        self.collect_for_runs(self.query.evaluate_candidates(vec![run], true)?)
    }
}

impl crate::execution::TerminalQuery for ConditionQuery {
    type Error = crate::DatabaseError;

    fn execution_options(&self) -> &crate::ExecutionOptions {
        &self.query.execution
    }

    fn with_execution_options(&self, options: crate::ExecutionOptions) -> Self {
        let mut query = self.clone();
        query.query = query.query.with_execution(options);
        query
    }

    fn interruption_error(&self, failure: crate::ExecutionError) -> crate::DatabaseError {
        crate::rcdb::RCDBError::from(failure).into()
    }
}

/// Bounded iterator over condition-result chunks.
pub struct ConditionStream {
    query: ConditionQuery,
    runs: RunStream,
    budget: crate::execution::StreamBudget,
}
impl Iterator for ConditionStream {
    type Item = DatabaseResult<ConditionResults>;
    fn next(&mut self) -> Option<Self::Item> {
        if self.runs.complete {
            return None;
        }
        let query = self.query.clone();
        let original_run_options = self.runs.query.execution.clone();
        let result = self.budget.execute(&query, |active| {
            self.runs.query.execution = active.query.execution.clone();
            self.runs
                .next()
                .transpose()?
                .map(|runs| active.collect_for_runs(runs))
                .transpose()
        });
        self.runs.query.execution = original_run_options;
        if result.is_err() {
            self.runs.complete = true;
        }
        result.transpose()
    }
}

/// Inputs used to produce aligned condition columns.
#[derive(Debug, Clone)]
pub struct ConditionProvenance {
    runs: RunProvenance,
    fields: Vec<String>,
    policy: MissingDataPolicy,
    fallback_fields: Vec<String>,
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
    /// Declared missing-data policy.
    #[must_use]
    pub const fn policy(&self) -> MissingDataPolicy {
        self.policy
    }
    /// Projected fields with caller-supplied fallback values.
    #[must_use]
    pub fn fallback_fields(&self) -> &[String] {
        &self.fallback_fields
    }

    /// Structured Missing Data Policy configuration.
    #[must_use]
    pub fn missing_data(&self) -> MissingDataConfig {
        MissingDataConfig {
            policy: self.policy,
            fallback_fields: self.fallback_fields.clone(),
        }
    }
}

/// Completed diagnostics for missing requested condition cells.
#[derive(Debug, Clone)]
pub struct ConditionReport {
    missing_values: Vec<(RunNumber, String)>,
    substitutions: Vec<(RunNumber, String)>,
}
impl ConditionReport {
    /// Missing (run, condition name) pairs in run/name order; null values count as missing.
    #[must_use]
    pub fn missing_values(&self) -> &[(RunNumber, String)] {
        &self.missing_values
    }
    /// Missing cells replaced by caller-supplied values.
    #[must_use]
    pub fn substitutions(&self) -> &[(RunNumber, String)] {
        &self.substitutions
    }

    /// Structured missing condition cells.
    #[must_use]
    pub fn omissions(&self) -> Vec<ConditionOmission> {
        self.missing_values
            .iter()
            .map(|(run, condition)| ConditionOmission {
                run: *run,
                condition: condition.clone(),
            })
            .collect()
    }
}

/// Immutable optional condition columns aligned with a resolved Run Set.
#[derive(Debug, Clone)]
pub struct ConditionResults {
    runs: RunSet,
    columns: std::collections::BTreeMap<String, Vec<Option<ConditionValue>>>,
    column_types: std::collections::BTreeMap<String, ConditionValueType>,
    provenance: ConditionProvenance,
    report: ConditionReport,
}
impl ConditionResults {
    fn extend(&mut self, other: Self) {
        results::extend_conditions(self, other);
    }
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
    pub fn get(&self, run: RunNumber, name: &str) -> DatabaseResult<Option<&ConditionValue>> {
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
    pub fn column(&self, name: &str) -> DatabaseResult<&[Option<ConditionValue>]> {
        Ok(self
            .columns
            .get(name)
            .map(Vec::as_slice)
            .ok_or_else(|| crate::rcdb::RCDBError::ConditionTypeNotFound(name.into()))?)
    }
    /// Return the database-defined type of a projected column.
    ///
    /// # Errors
    /// Rejects names outside the result projection.
    pub fn column_type(&self, name: &str) -> DatabaseResult<ConditionValueType> {
        self.column_types
            .get(name)
            .copied()
            .ok_or_else(|| crate::rcdb::RCDBError::ConditionTypeNotFound(name.into()).into())
    }
}
