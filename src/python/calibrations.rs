use super::dataframe::PolarsDataFrame;
use super::{
    runs::{PyRunProvenance, PyRunQuery, PyRunReport, PyRunSelection, PyRunSet, coerce_run_scopes},
    tuple::{TypedIterator, TypedTuple},
};
use crate::calibrations::*;
use crate::{Id, RESTVersionSelection, RunNumber};
use polars::prelude::{
    Column, DataFrame, DataType, IntoColumn, IntoSeries, ListChunked, NamedFrom, Series,
    SortMultipleOptions, StructChunked,
};
use pyo3::{
    exceptions::{PyKeyError, PyStopIteration, PyValueError},
    prelude::*,
    types::{PyDict, PyTuple},
};
use pyo3_polars::PyDataFrame;

fn error(error: crate::DatabaseError) -> PyErr {
    super::exceptions::map(&error)
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) enum ScopedCalibrationContext {
    Default,
    Reconstruction(crate::RunPeriod, ReconstructionPeriod),
    Direct {
        variation: String,
        as_of: chrono::DateTime<chrono::Utc>,
    },
}

impl ScopedCalibrationContext {
    fn description(&self) -> String {
        match self {
            Self::Default => "the session default calibration context".to_owned(),
            Self::Reconstruction(period, reconstruction) => reconstruction
                .resolve(*period)
                .map(|context| {
                    format!(
                        "{} ({}, as of {})",
                        period.short_name(),
                        context.variation,
                        context.timestamp
                    )
                })
                .unwrap_or_else(|_| format!("{} REST calibration", period.short_name())),
            Self::Direct { variation, as_of } => {
                format!("variation {variation:?}, as of {as_of}")
            }
        }
    }
}

/// Numeric run selection carrying exactly one calibration context.
#[pyclass(
    name = "CalibratedRunSelection",
    module = "gluex",
    frozen,
    skip_from_py_object
)]
#[derive(Clone)]
pub struct PyCalibratedRunSelection {
    pub(crate) selection: crate::RunSelection,
    pub(crate) context: ScopedCalibrationContext,
    excluded: std::collections::BTreeSet<RunNumber>,
}

impl PyCalibratedRunSelection {
    pub(crate) fn direct(
        selection: crate::RunSelection,
        as_of: chrono::DateTime<chrono::Utc>,
        variation: Option<String>,
    ) -> Self {
        Self {
            selection,
            context: ScopedCalibrationContext::Direct {
                variation: variation.unwrap_or_else(|| "default".to_owned()),
                as_of,
            },
            excluded: std::collections::BTreeSet::new(),
        }
    }

    pub(crate) fn reconstruction(
        selection: crate::RunSelection,
        period: crate::RunPeriod,
        reconstruction: ReconstructionPeriod,
    ) -> Self {
        Self {
            selection,
            context: ScopedCalibrationContext::Reconstruction(period, reconstruction),
            excluded: std::collections::BTreeSet::new(),
        }
    }

    pub(crate) fn run_scope(&self) -> PyResult<crate::runs::RunCalibrationScope> {
        let mut runs = selection_numbers(&self.selection)?;
        runs.retain(|run| !self.excluded.contains(run));
        let context = match &self.context {
            ScopedCalibrationContext::Default => {
                return Err(PyValueError::new_err(
                    "a calibrated run selection must have an explicit calibration context",
                ));
            }
            ScopedCalibrationContext::Reconstruction(period, reconstruction) => {
                crate::runs::RunCalibrationContext::Reconstruction(*period, reconstruction.clone())
            }
            ScopedCalibrationContext::Direct { variation, as_of } => {
                crate::runs::RunCalibrationContext::Direct {
                    variation: variation.clone(),
                    as_of: *as_of,
                }
            }
        };
        Ok(crate::runs::RunCalibrationScope {
            selection: crate::RunSelection::runs(runs),
            context,
        })
    }
}

#[pymethods]
impl PyCalibratedRunSelection {
    /// Return a copy that omits explicit run numbers before overlap validation.
    #[pyo3(signature = (*runs))]
    pub(crate) fn excluding(&self, runs: Vec<RunNumber>) -> Self {
        let mut selected = self.clone();
        selected.excluded.extend(runs);
        selected
    }

    fn __repr__(&self) -> String {
        format!(
            "CalibratedRunSelection(context={}, excluded={})",
            self.context.description(),
            self.excluded.len()
        )
    }
}

pub(crate) fn selection_period(selection: &crate::RunSelection) -> PyResult<crate::RunPeriod> {
    let periods = match selection {
        crate::RunSelection::Runs(runs) if !runs.is_empty() => runs
            .iter()
            .map(|run| crate::RunPeriod::try_from(*run))
            .collect::<Result<std::collections::BTreeSet<_>, _>>(),
        crate::RunSelection::Range { start, end } if start <= end => [
            crate::RunPeriod::try_from(*start),
            crate::RunPeriod::try_from(*end),
        ]
        .into_iter()
        .collect::<Result<std::collections::BTreeSet<_>, _>>(),
        crate::RunSelection::Runs(_) | crate::RunSelection::Range { .. } => {
            return Err(PyValueError::new_err(
                "an empty run selection cannot choose a REST version",
            ));
        }
        crate::RunSelection::All => {
            return Err(PyValueError::new_err(
                "an all-runs selection cannot choose one REST version",
            ));
        }
    }
    .map_err(|error| PyValueError::new_err(error.to_string()))?;
    if periods.len() != 1 {
        return Err(PyValueError::new_err(
            "rest(version) requires runs from exactly one run period; split the selection by period",
        ));
    }
    Ok(*periods.first().expect("one period was validated"))
}

fn reconstruction_from_dict(selections: &Bound<'_, PyDict>) -> PyResult<ReconstructionSelection> {
    let mut native = std::collections::BTreeMap::new();
    for (key, value) in selections.iter() {
        let period = super::core::parse_run_period_object(&key)?;
        let selection = if let Ok(selection) =
            value.extract::<PyRef<'_, super::core::PyRESTVersionSelection>>()
        {
            if selection
                .1
                .is_some_and(|selected_period| selected_period != period)
            {
                return Err(PyValueError::new_err(format!(
                    "REST version selection conflicts with mapping key {}",
                    period.short_name()
                )));
            }
            let mut requested = ReconstructionPeriod::new(selection.0);
            if let Some(variation) = &selection.2 {
                requested = requested.with_variation(variation);
            }
            requested
        } else {
            let version = value.extract()?;
            ReconstructionPeriod::new(
                crate::RESTVersionSelection::try_new(period, version)
                    .map_err(|error| PyValueError::new_err(error.to_string()))?,
            )
        };
        if native.insert(period, selection).is_some() {
            return Err(PyValueError::new_err(format!(
                "duplicate reconstruction meaning for run period {}",
                period.short_name()
            )));
        }
    }
    Ok(ReconstructionSelection::periods(native))
}

pub(crate) fn parse_reconstruction(value: &Bound<'_, PyAny>) -> PyResult<ReconstructionSelection> {
    if let Ok(selection) = value.extract::<PyReconstructionSelection>() {
        return Ok(selection.0);
    }
    if let Ok(selections) = value.cast::<PyDict>() {
        return reconstruction_from_dict(selections);
    }
    if let Ok(period) = value.extract::<PyRef<'_, super::core::PyCalibratedRunPeriod>>() {
        return Ok(ReconstructionSelection::periods([(
            period.0.period(),
            period.0.reconstruction().clone(),
        )]));
    }
    if let Ok(selection) = value.extract::<PyRef<'_, super::core::PyRESTVersionSelection>>() {
        let period = selection.1.ok_or_else(|| {
            pyo3::exceptions::PyTypeError::new_err(
                "a direct REST selection must be created by RunPeriod.rest(...) so its period is known",
            )
        })?;
        let mut requested = ReconstructionPeriod::new(selection.0);
        if let Some(variation) = &selection.2 {
            requested = requested.with_variation(variation);
        }
        return Ok(ReconstructionSelection::periods([(period, requested)]));
    }
    Err(pyo3::exceptions::PyTypeError::new_err(
        "reconstruction must be a RunPeriod.rest(...) selection, ReconstructionSelection, or period-to-version mapping",
    ))
}

/// Immutable full-path mapping of calibration definitions; discovery does not fetch constants.
#[pyclass(name = "CalibrationCatalog", module = "gluex", frozen)]
pub struct PyCalibrationCatalog(pub(crate) CalibrationCatalog);
#[pymethods]
impl PyCalibrationCatalog {
    /// Select run scopes for one or more calibration tables without retrieving data.
    #[pyo3(signature = (*run_scopes: "int | Sequence[int] | range | RunPeriod | CalibratedRunPeriod | CalibratedRunSelection | str | RunSelection | RunQuery | RunSet", variation=None, as_of=None))]
    fn select(
        &self,
        run_scopes: &Bound<'_, PyTuple>,
        variation: Option<String>,
        as_of: Option<chrono::DateTime<chrono::Utc>>,
    ) -> PyResult<PyCalibrationSelection> {
        calibration_selection(self.0.clone(), run_scopes, variation, as_of)
    }
    fn keys(&self) -> TypedTuple<String> {
        TypedTuple(self.0.keys().cloned().collect())
    }
    fn items(&self) -> TypedTuple<(String, PyCalibrationTable)> {
        TypedTuple(
            self.0
                .items()
                .map(|(k, v)| (k.clone(), PyCalibrationTable(v.clone())))
                .collect(),
        )
    }
    fn __getitem__(&self, path: &str) -> PyResult<PyCalibrationTable> {
        self.0
            .get(path)
            .cloned()
            .map(PyCalibrationTable)
            .ok_or_else(|| PyKeyError::new_err(path.to_owned()))
    }
    fn __contains__(&self, path: &str) -> bool {
        self.0.get(path).is_some()
    }
    fn __iter__(&self) -> TypedIterator<String> {
        TypedIterator(self.0.keys().cloned().collect())
    }
    fn __len__(&self) -> usize {
        self.0.keys().len()
    }
    /// Independent directory mapping keyed by absolute paths.
    #[getter]
    fn directories(&self) -> std::collections::BTreeMap<String, PyCalibrationDirectory> {
        self.0
            .directories()
            .iter()
            .map(|(k, v)| (k.clone(), PyCalibrationDirectory(v.clone())))
            .collect()
    }
    fn __repr__(&self) -> String {
        format!("CalibrationCatalog(tables={})", self.__len__())
    }
}

#[derive(Clone)]
enum CalibrationRunInput {
    Selection(crate::RunSelection),
    Query(crate::RunQuery),
    Set(crate::RunSet),
}

impl CalibrationRunInput {
    fn query(&self, table: &CalibrationTable) -> PyResult<CalibrationQuery> {
        match self {
            Self::Selection(selection) => table.for_runs(selection.clone()),
            Self::Query(query) => table.for_query(query),
            Self::Set(runs) => table.for_run_set(runs),
        }
        .map_err(error)
    }
}

fn scoped_context(context: &crate::runs::RunCalibrationContext) -> ScopedCalibrationContext {
    match context {
        crate::runs::RunCalibrationContext::Reconstruction(period, reconstruction) => {
            ScopedCalibrationContext::Reconstruction(*period, reconstruction.clone())
        }
        crate::runs::RunCalibrationContext::Direct { variation, as_of } => {
            ScopedCalibrationContext::Direct {
                variation: variation.clone(),
                as_of: *as_of,
            }
        }
    }
}

fn calibrated_run_set_inputs(
    runs: &crate::RunSet,
) -> PyResult<Vec<(CalibrationRunInput, ScopedCalibrationContext)>> {
    let selected = runs
        .numbers()
        .iter()
        .copied()
        .collect::<std::collections::BTreeSet<_>>();
    let mut owners =
        std::collections::BTreeMap::<RunNumber, crate::runs::RunCalibrationContext>::new();
    let mut inputs = Vec::new();
    for scope in runs.provenance().calibration_scopes() {
        let context = scope.context.clone();
        let mut unique = Vec::new();
        let mut conflicts = Vec::new();
        for run in selection_numbers(&scope.selection)? {
            if !selected.contains(&run) {
                continue;
            }
            match owners.get(&run) {
                Some(existing) if existing != &context => conflicts.push(run),
                Some(_) => {}
                None => {
                    owners.insert(run, context.clone());
                    unique.push(run);
                }
            }
        }
        if !conflicts.is_empty() {
            return Err(PyValueError::new_err(format!(
                "runs [{}] have conflicting calibration contexts inherited from run selection",
                format_run_list(&conflicts)
            )));
        }
        if !unique.is_empty() {
            inputs.push((
                CalibrationRunInput::Set(runs.subset(unique)),
                scoped_context(&context),
            ));
        }
    }
    let unscoped = selected
        .into_iter()
        .filter(|run| !owners.contains_key(run))
        .collect::<Vec<_>>();
    if !unscoped.is_empty() {
        inputs.push((
            CalibrationRunInput::Set(runs.subset(unscoped)),
            ScopedCalibrationContext::Default,
        ));
    }
    Ok(inputs)
}

fn calibration_selection(
    catalog: CalibrationCatalog,
    scopes: &Bound<'_, PyTuple>,
    variation: Option<String>,
    as_of: Option<chrono::DateTime<chrono::Utc>>,
) -> PyResult<PyCalibrationSelection> {
    let mut scoped = Vec::new();
    for scope in scopes {
        if let Ok(selection) = scope.extract::<PyRef<'_, PyCalibratedRunSelection>>() {
            scoped.push((
                selection.selection.clone(),
                selection.context.clone(),
                selection.excluded.clone(),
            ));
        } else if let Ok(period) = scope.extract::<PyRef<'_, super::core::PyCalibratedRunPeriod>>()
        {
            scoped.push((
                crate::RunSelection::period(period.0.period()),
                ScopedCalibrationContext::Reconstruction(
                    period.0.period(),
                    period.0.reconstruction().clone(),
                ),
                std::collections::BTreeSet::new(),
            ));
        } else {
            scoped.push((
                super::runs::coerce_run_scope(&scope)?,
                ScopedCalibrationContext::Default,
                std::collections::BTreeSet::new(),
            ));
        }
    }
    let calibrated = scoped
        .iter()
        .any(|(_, context, _)| *context != ScopedCalibrationContext::Default);
    if calibrated && (variation.is_some() || as_of.is_some()) {
        let period = scopes
            .iter()
            .find_map(|scope| {
                scope
                    .extract::<PyRef<'_, super::core::PyCalibratedRunPeriod>>()
                    .ok()
                    .map(|period| period.0.period().short_name().to_ascii_lowercase())
            })
            .unwrap_or_else(|| "the calibrated scope".to_owned());
        let calibrated_runs = scoped
            .iter()
            .filter(|(_, context, _)| *context != ScopedCalibrationContext::Default)
            .flat_map(|(selection, _, excluded)| {
                selection_numbers(selection)
                    .unwrap_or_default()
                    .into_iter()
                    .filter(|run| !excluded.contains(run))
                    .collect::<Vec<_>>()
            })
            .collect::<std::collections::BTreeSet<_>>();
        let plain_runs = scoped
            .iter()
            .filter(|(_, context, _)| *context == ScopedCalibrationContext::Default)
            .flat_map(|(selection, _, _)| selection_numbers(selection).unwrap_or_default())
            .collect::<std::collections::BTreeSet<_>>();
        let overlap = calibrated_runs
            .intersection(&plain_runs)
            .copied()
            .collect::<Vec<_>>();
        let guidance = if overlap.is_empty() {
            "remove the global selectors and attach .at(timestamp, variation=...) to the specific RunSelection that needs them".to_owned()
        } else {
            let runs = format_run_list(&overlap);
            format!(
                "the likely override is RunPeriod({period:?}).rest(...).excluding({runs}) together with RunSelection.runs([{runs}]).at(timestamp, variation='mc')"
            )
        };
        return Err(PyValueError::new_err(format!(
            "variation and as_of would also overwrite {period}'s calibrated context; {guidance}"
        )));
    }
    if scopes.len() == 1
        && let Ok(runs) = scopes.get_item(0)?.extract::<PyRunSet>()
    {
        let inherited = calibrated_run_set_inputs(&runs.0)?;
        if inherited
            .iter()
            .any(|(_, context)| *context != ScopedCalibrationContext::Default)
            && (variation.is_some() || as_of.is_some())
        {
            return Err(PyValueError::new_err(
                "variation and as_of cannot override calibration contexts inherited from the RunSet",
            ));
        }
        return Ok(PyCalibrationSelection {
            scopes: inherited,
            variation,
            as_of,
            catalog,
        });
    } else if scopes.len() == 1
        && let Ok(query) = scopes.get_item(0)?.extract::<PyRunQuery>()
    {
        return Ok(PyCalibrationSelection {
            scopes: vec![(
                CalibrationRunInput::Query(query.0),
                ScopedCalibrationContext::Default,
            )],
            variation,
            as_of,
            catalog,
        });
    }
    let mut owners = std::collections::BTreeMap::<RunNumber, ScopedCalibrationContext>::new();
    let mut normalized = Vec::new();
    for (selection, context, excluded) in scoped {
        let mut runs = selection_numbers(&selection)?;
        runs.retain(|run| !excluded.contains(run));
        let conflicts = runs
            .iter()
            .filter(|run| owners.get(run).is_some_and(|existing| existing != &context))
            .copied()
            .collect::<Vec<_>>();
        if let Some(run) = conflicts.first()
            && let Some(existing) = owners.get(run)
        {
            let displayed = format_run_list(&conflicts);
            return Err(PyValueError::new_err(format!(
                "runs [{displayed}] have conflicting calibration contexts: {} and {}. If the custom runs should override a period, exclude them first: period.rest(...).excluding({displayed}), RunSelection.runs([{displayed}]).at(timestamp, variation='...')",
                existing.description(),
                context.description(),
            )));
        }
        let mut unique = Vec::new();
        for run in runs {
            if let Some(existing) = owners.get(&run) {
                debug_assert_eq!(existing, &context);
            } else {
                owners.insert(run, context.clone());
                unique.push(run);
            }
        }
        if !unique.is_empty() {
            normalized.push((
                CalibrationRunInput::Selection(crate::RunSelection::runs(unique)),
                context,
            ));
        }
    }
    Ok(PyCalibrationSelection {
        catalog,
        scopes: normalized,
        variation,
        as_of,
    })
}

fn format_run_list(runs: &[RunNumber]) -> String {
    let shown = runs
        .iter()
        .take(8)
        .map(ToString::to_string)
        .collect::<Vec<_>>()
        .join(", ");
    if runs.len() > 8 {
        format!("{shown}, ...")
    } else {
        shown
    }
}

fn selection_numbers(selection: &crate::RunSelection) -> PyResult<Vec<RunNumber>> {
    match selection {
        crate::RunSelection::Runs(runs) => Ok(runs.clone()),
        crate::RunSelection::Range { start, end } if start <= end => {
            let length = end.saturating_sub(*start) as u64 + 1;
            if length > 10_000_000 {
                return Err(PyValueError::new_err(
                    "combined calibrated scopes are limited to 10,000,000 runs",
                ));
            }
            Ok((*start..=*end).collect())
        }
        crate::RunSelection::Range { .. } => Ok(Vec::new()),
        crate::RunSelection::All => Err(PyValueError::new_err(
            "all-runs selection is not supported for calibrations",
        )),
    }
}

/// Lazy calibration run selection; project tables with ``tables``.
#[pyclass(name = "CalibrationSelection", module = "gluex", frozen)]
pub struct PyCalibrationSelection {
    catalog: CalibrationCatalog,
    scopes: Vec<(CalibrationRunInput, ScopedCalibrationContext)>,
    variation: Option<String>,
    as_of: Option<chrono::DateTime<chrono::Utc>>,
}

#[pymethods]
impl PyCalibrationSelection {
    /// Project exact absolute table paths; each path becomes an independent result column.
    #[pyo3(signature = (*paths))]
    fn tables(&self, paths: &Bound<'_, PyTuple>) -> PyResult<PyCalibrationTablesQuery> {
        let paths = paths.extract::<Vec<String>>()?;
        if paths.is_empty() {
            return Err(PyValueError::new_err(
                "at least one calibration table is required",
            ));
        }
        let mut seen = std::collections::BTreeSet::new();
        let mut queries = Vec::with_capacity(paths.len());
        for path in paths {
            if !seen.insert(path.clone()) {
                return Err(PyValueError::new_err(format!(
                    "duplicate calibration table {path:?}"
                )));
            }
            let table = self
                .catalog
                .get(&path)
                .ok_or_else(|| PyKeyError::new_err(path.clone()))?;
            let mut table_queries = Vec::with_capacity(self.scopes.len());
            for (input, context) in &self.scopes {
                let mut query = input.query(table)?;
                match context {
                    ScopedCalibrationContext::Default => {}
                    ScopedCalibrationContext::Reconstruction(period, reconstruction) => {
                        query = query.with_reconstruction(ReconstructionSelection::periods([(
                            *period,
                            reconstruction.clone(),
                        )]));
                    }
                    ScopedCalibrationContext::Direct { variation, as_of } => {
                        query = query.with_variation(variation.clone()).as_of(*as_of);
                    }
                }
                if let Some(variation) = &self.variation {
                    query = query.with_variation(variation.clone());
                }
                if let Some(as_of) = self.as_of {
                    query = query.as_of(as_of);
                }
                table_queries.push(query);
            }
            queries.push((path, table_queries));
        }
        Ok(PyCalibrationTablesQuery { queries })
    }

    fn __repr__(&self) -> &'static str {
        "CalibrationSelection(project=tables(*paths))"
    }
}

/// Lazy multi-table calibration request.
#[pyclass(
    name = "CalibrationTablesQuery",
    module = "gluex",
    frozen,
    skip_from_py_object
)]
#[derive(Clone)]
pub struct PyCalibrationTablesQuery {
    queries: Vec<(String, Vec<CalibrationQuery>)>,
}

#[pymethods]
impl PyCalibrationTablesQuery {
    /// Require every requested run/table pair to have an assignment.
    fn strict(&self) -> Self {
        Self {
            queries: self
                .queries
                .iter()
                .map(|(path, queries)| {
                    (
                        path.clone(),
                        queries.iter().map(CalibrationQuery::strict).collect(),
                    )
                })
                .collect(),
        }
    }

    /// Use one run's assignment when a requested run/table pair is missing.
    fn fallback_to(&self, run: RunNumber) -> Self {
        Self {
            queries: self
                .queries
                .iter()
                .map(|(path, queries)| {
                    (
                        path.clone(),
                        queries.iter().map(|query| query.fallback_to(run)).collect(),
                    )
                })
                .collect(),
        }
    }

    /// Collect every projected table into one keyed result.
    fn collect(&self, py: Python<'_>) -> PyResult<PyCalibrationResults> {
        let mut series = Vec::with_capacity(self.queries.len());
        for (path, queries) in &self.queries {
            let mut table_series = Vec::with_capacity(queries.len());
            for query in queries {
                table_series.push(crate::python::execution::PythonExecution::execute(
                    py,
                    query,
                    CalibrationQuery::collect,
                )?);
            }
            series.push((path.clone(), table_series));
        }
        Ok(PyCalibrationResults { series })
    }

    fn __repr__(&self) -> String {
        format!(
            "CalibrationTablesQuery(tables={:?})",
            self.queries.iter().map(|(p, _)| p).collect::<Vec<_>>()
        )
    }
}

/// Collected calibration tables keyed by exact absolute path.
#[pyclass(name = "CalibrationResults", module = "gluex", frozen)]
pub struct PyCalibrationResults {
    series: Vec<(String, Vec<CalibrationSeries>)>,
}

fn payload_series(name: &str, values: &CalibrationColumnValues) -> Series {
    match values {
        CalibrationColumnValues::Int(v) => Series::new(name.into(), *v),
        CalibrationColumnValues::UInt(v) => Series::new(name.into(), *v),
        CalibrationColumnValues::Long(v) => Series::new(name.into(), *v),
        CalibrationColumnValues::ULong(v) => Series::new(name.into(), *v),
        CalibrationColumnValues::Double(v) => Series::new(name.into(), *v),
        CalibrationColumnValues::String(v) => Series::new(name.into(), *v),
        CalibrationColumnValues::Bool(v) => Series::new(name.into(), *v),
    }
}

fn payload_column_for_run<'a>(
    series: &'a [CalibrationSeries],
    run: RunNumber,
    name: &str,
) -> Option<CalibrationColumnValues<'a>> {
    series
        .iter()
        .find_map(|series| series.get(run))
        .and_then(|entry| entry.payload().column(name))
}

fn scalar_payload_field(
    name: &str,
    kind: CalibrationColumnValues<'_>,
    series: &[CalibrationSeries],
    runs: &[RunNumber],
) -> Series {
    macro_rules! scalar_values {
        ($variant:ident) => {{
            let values = runs
                .iter()
                .map(|run| match payload_column_for_run(series, *run, name) {
                    Some(CalibrationColumnValues::$variant(values)) => values.first().cloned(),
                    _ => None,
                })
                .collect::<Vec<_>>();
            Series::new(name.into(), values)
        }};
    }

    match kind {
        CalibrationColumnValues::Int(_) => scalar_values!(Int),
        CalibrationColumnValues::UInt(_) => scalar_values!(UInt),
        CalibrationColumnValues::Long(_) => scalar_values!(Long),
        CalibrationColumnValues::ULong(_) => scalar_values!(ULong),
        CalibrationColumnValues::Double(_) => scalar_values!(Double),
        CalibrationColumnValues::String(_) => scalar_values!(String),
        CalibrationColumnValues::Bool(_) => scalar_values!(Bool),
    }
}

fn nested_table_column(
    path: &str,
    series: &[CalibrationSeries],
    runs: &[RunNumber],
) -> PyResult<Column> {
    let Some((_, first)) = series.iter().find_map(|series| series.items().next()) else {
        return Ok(Column::full_null(path.into(), runs.len(), &DataType::Null));
    };
    let scalar = series
        .iter()
        .flat_map(CalibrationSeries::items)
        .all(|(_, entry)| entry.payload().n_rows() == 1);
    let mut fields = Vec::new();
    for name in first.payload().column_names() {
        if scalar {
            let kind = first
                .payload()
                .column(name)
                .expect("column name came from this payload");
            fields.push(scalar_payload_field(name, kind, series, runs));
            continue;
        }
        let lists: ListChunked = runs
            .iter()
            .map(|run| {
                payload_column_for_run(series, *run, name)
                    .map(|values| payload_series(name, &values))
            })
            .collect();
        fields.push(lists.with_name(name.into()).into_series());
    }
    StructChunked::from_series(path.into(), runs.len(), fields.iter())
        .map(IntoColumn::into_column)
        .map_err(|error| PyValueError::new_err(error.to_string()))
}

#[pymethods]
impl PyCalibrationResults {
    /// Exact table paths in projection order.
    #[getter]
    fn tables(&self) -> TypedTuple<String> {
        TypedTuple(self.series.iter().map(|(path, _)| path.clone()).collect())
    }

    /// All evaluated runs in numeric order, including partial table misses.
    #[getter]
    fn runs(&self) -> TypedTuple<RunNumber> {
        let runs = self
            .series
            .iter()
            .flat_map(|(_, segments)| {
                segments
                    .iter()
                    .flat_map(|series| series.report().evaluated_runs().iter().copied())
            })
            .collect::<std::collections::BTreeSet<_>>();
        TypedTuple(runs.into_iter().collect())
    }

    fn __getitem__(&self, path: &str) -> PyResult<PyCalibrationTableResults> {
        self.series
            .iter()
            .find(|(candidate, _)| candidate == path)
            .map(|(_, series)| PyCalibrationTableResults(series.clone()))
            .ok_or_else(|| PyKeyError::new_err(path.to_owned()))
    }

    /// Convert to one row per run with one collision-safe struct column per table.
    fn to_polars(&self) -> PyResult<PolarsDataFrame> {
        let runs = self.runs().0;
        let run_numbers = runs
            .iter()
            .map(|run| {
                u32::try_from(*run).map_err(|_| {
                    PyValueError::new_err(format!(
                        "run number {run} cannot be represented as Polars UInt32"
                    ))
                })
            })
            .collect::<PyResult<Vec<_>>>()?;
        let mut columns = vec![Column::new("run_number".into(), run_numbers)];
        for (path, segments) in &self.series {
            columns.push(nested_table_column(path, segments, &runs)?);
        }
        DataFrame::new(runs.len(), columns)
            .map(|frame| PolarsDataFrame(PyDataFrame(frame)))
            .map_err(|error| PyValueError::new_err(error.to_string()))
    }

    fn __len__(&self) -> usize {
        self.runs().0.len()
    }

    fn __repr__(&self) -> String {
        format!(
            "CalibrationResults(runs={}, tables={:?})",
            self.__len__(),
            self.tables().0
        )
    }
}

/// One calibration table collected across one or more independently calibrated scopes.
#[pyclass(name = "CalibrationTableResults", module = "gluex", frozen)]
pub struct PyCalibrationTableResults(Vec<CalibrationSeries>);

#[pymethods]
impl PyCalibrationTableResults {
    /// Runs with resolved assignments, sorted and deduplicated.
    #[getter]
    fn runs(&self) -> TypedTuple<RunNumber> {
        TypedTuple(
            self.0
                .iter()
                .flat_map(CalibrationSeries::items)
                .map(|(run, _)| *run)
                .collect::<std::collections::BTreeSet<_>>()
                .into_iter()
                .collect(),
        )
    }

    /// Provenance for each independently calibrated scope.
    #[getter]
    fn contexts(&self) -> TypedTuple<PyCalibrationProvenance> {
        TypedTuple(
            self.0
                .iter()
                .map(|series| PyCalibrationProvenance(series.provenance().clone()))
                .collect(),
        )
    }

    /// One missing-data report for each entry in ``contexts``.
    #[getter]
    fn reports(&self) -> TypedTuple<PyCalibrationReport> {
        TypedTuple(
            self.0
                .iter()
                .map(|series| PyCalibrationReport(series.report().clone()))
                .collect(),
        )
    }

    /// Single-scope provenance; use ``contexts`` when several calibrated scopes were combined.
    #[getter]
    fn provenance(&self) -> PyResult<PyCalibrationProvenance> {
        match self.0.as_slice() {
            [series] => Ok(PyCalibrationProvenance(series.provenance().clone())),
            _ => Err(PyValueError::new_err(
                "this table contains multiple calibration contexts; inspect .contexts instead",
            )),
        }
    }

    /// Single-scope report; multi-context reports remain available on the individual contexts.
    #[getter]
    fn report(&self) -> PyResult<PyCalibrationReport> {
        match self.0.as_slice() {
            [series] => Ok(PyCalibrationReport(series.report().clone())),
            _ => Err(PyValueError::new_err(
                "this table contains multiple calibration contexts; inspect .reports instead",
            )),
        }
    }

    fn __getitem__(&self, run: RunNumber) -> PyResult<PyCalibrationEntry> {
        self.0
            .iter()
            .find_map(|series| series.get(run))
            .cloned()
            .map(PyCalibrationEntry)
            .ok_or_else(|| PyKeyError::new_err(run))
    }

    fn items(&self) -> TypedTuple<(RunNumber, PyCalibrationEntry)> {
        let entries = self
            .0
            .iter()
            .flat_map(CalibrationSeries::items)
            .map(|(run, entry)| (*run, entry.clone()))
            .collect::<std::collections::BTreeMap<_, _>>();
        TypedTuple(
            entries
                .into_iter()
                .map(|(run, entry)| (run, PyCalibrationEntry(entry)))
                .collect(),
        )
    }

    /// Flatten every scope's payload rows into one run-sorted Polars DataFrame.
    fn to_polars(&self) -> PyResult<PolarsDataFrame> {
        let mut frames = self.0.iter().map(|series| {
            PyCalibrationSeries(series.clone())
                .to_polars()
                .map(|frame| frame.0.0)
        });
        let Some(mut frame) = frames.next().transpose()? else {
            return Ok(PolarsDataFrame(PyDataFrame(DataFrame::empty())));
        };
        for next in frames {
            frame
                .vstack_mut(&next?)
                .map_err(|error| PyValueError::new_err(error.to_string()))?;
        }
        frame
            .sort(["run_number"], SortMultipleOptions::default())
            .map(|frame| PolarsDataFrame(PyDataFrame(frame)))
            .map_err(|error| PyValueError::new_err(error.to_string()))
    }

    fn __len__(&self) -> usize {
        self.runs().0.len()
    }

    fn __repr__(&self) -> String {
        format!(
            "CalibrationTableResults(runs={}, contexts={})",
            self.__len__(),
            self.0.len()
        )
    }
}

/// Immutable calibration directory definition with independent child mappings.
#[pyclass(name = "CalibrationDirectory", module = "gluex", frozen)]
pub struct PyCalibrationDirectory(CalibrationDirectory);
#[pymethods]
impl PyCalibrationDirectory {
    #[getter]
    fn path(&self) -> &str {
        self.0.path()
    }
    /// Child names mapped to absolute directory paths; editing the copy cannot change the catalog.
    #[getter]
    fn directories(&self) -> std::collections::BTreeMap<String, String> {
        self.0.directories().clone()
    }
    /// Local names mapped to immutable table definitions.
    #[getter]
    fn tables(&self) -> std::collections::BTreeMap<String, PyCalibrationTable> {
        self.0
            .tables()
            .iter()
            .map(|(k, v)| (k.clone(), PyCalibrationTable(v.clone())))
            .collect()
    }
    fn __repr__(&self) -> String {
        format!("CalibrationDirectory({:?})", self.0.path())
    }
}

/// Source-bound table definition; columns and metadata do not fetch constants.
#[pyclass(name = "CalibrationTable", module = "gluex", frozen)]
pub struct PyCalibrationTable(CalibrationTable);
#[pymethods]
impl PyCalibrationTable {
    #[getter]
    fn path(&self) -> String {
        self.0.path()
    }
    #[getter]
    fn id(&self) -> Id {
        self.0.metadata().id()
    }
    #[getter]
    fn description(&self) -> String {
        self.0.metadata().description().to_owned()
    }
    #[getter]
    fn n_rows(&self) -> i64 {
        self.0.metadata().n_rows()
    }
    /// Ordered named columns; raises RuntimeError for invalid metadata.
    #[getter]
    fn columns(&self, py: Python<'_>) -> PyResult<TypedTuple<PyCalibrationColumn>> {
        py.detach(|| self.0.columns())
            .map(|c| TypedTuple(c.into_iter().map(PyCalibrationColumn).collect()))
            .map_err(error)
    }
    /// Build a lazy query from a Python run scope, with optional keyword selectors.
    #[pyo3(signature = (
        *run_scopes: "int | Sequence[int] | range | RunPeriod | CalibratedRunPeriod | str | RunSelection | RunQuery | RunSet",
        variation=None,
        as_of=None,
        reconstruction: "CalibratedRunPeriod | RESTVersionSelection | ReconstructionSelection | dict[RunPeriod | str, int | RESTVersionSelection] | None"=None,
        missing_policy: "Literal['report', 'strict', 'fallback']"="report",
        fallback_run=None
    ))]
    fn for_runs(
        &self,
        run_scopes: &Bound<'_, PyTuple>,
        variation: Option<String>,
        as_of: Option<chrono::DateTime<chrono::Utc>>,
        reconstruction: Option<&Bound<'_, PyAny>>,
        missing_policy: &str,
        fallback_run: Option<RunNumber>,
    ) -> PyResult<PyCalibrationQuery> {
        let mut period_contexts = std::collections::BTreeMap::new();
        let mut configured = std::collections::BTreeMap::new();
        for scope in run_scopes {
            let context = if let Ok(period) =
                scope.extract::<PyRef<'_, super::core::PyCalibratedRunPeriod>>()
            {
                Some((period.0.period(), Some(period.0.reconstruction().clone())))
            } else if let Ok(period) = scope.extract::<PyRef<'_, super::core::PyRunPeriod>>() {
                Some((period.0, None))
            } else if let Ok(name) = scope.extract::<String>() {
                name.parse::<crate::RunPeriod>()
                    .ok()
                    .map(|period| (period, None))
            } else {
                None
            };
            if let Some((period, reconstruction)) = context {
                if let Some(previous) = period_contexts.insert(period, reconstruction.clone())
                    && previous != reconstruction
                {
                    return Err(PyValueError::new_err(format!(
                        "conflicting calibration contexts for {}",
                        period.short_name()
                    )));
                }
                if let Some(reconstruction) = reconstruction {
                    configured.insert(period, reconstruction);
                }
            }
        }
        let period_reconstruction =
            (!configured.is_empty()).then(|| ReconstructionSelection::periods(configured));
        if (reconstruction.is_some() || period_reconstruction.is_some())
            && (variation.is_some() || as_of.is_some())
        {
            return Err(PyValueError::new_err(
                "reconstruction conflicts with direct variation or as_of selectors",
            ));
        }
        if reconstruction.is_some() && period_reconstruction.is_some() {
            return Err(PyValueError::new_err(
                "a configured RunPeriod already supplies reconstruction; do not repeat it",
            ));
        }
        let mut query = if run_scopes.len() == 1
            && let Ok(runs) = run_scopes.get_item(0)?.extract::<PyRunSet>()
        {
            self.0.for_run_set(&runs.0)
        } else if run_scopes.len() == 1
            && let Ok(query) = run_scopes.get_item(0)?.extract::<PyRunQuery>()
        {
            self.0.for_query(&query.0)
        } else {
            self.0.for_runs(coerce_run_scopes(run_scopes)?)
        }
        .map_err(error)?;
        if let Some(variation) = variation {
            query = query.with_variation(variation);
        }
        if let Some(as_of) = as_of {
            query = query.as_of(as_of);
        }
        if let Some(reconstruction) = reconstruction {
            query = query.with_reconstruction(parse_reconstruction(reconstruction)?);
        } else if let Some(reconstruction) = period_reconstruction {
            query = query.with_reconstruction(reconstruction);
        }
        query = match (missing_policy, fallback_run) {
            ("report", None) => query,
            ("strict", None) => query.strict(),
            ("fallback", Some(run)) => query.fallback_to(run),
            ("fallback", None) => {
                return Err(PyValueError::new_err(
                    "missing_policy='fallback' requires fallback_run",
                ));
            }
            (_, Some(_)) => {
                return Err(PyValueError::new_err(
                    "fallback_run requires missing_policy='fallback'",
                ));
            }
            (policy, None) => {
                return Err(PyValueError::new_err(format!(
                    "missing_policy must be 'report', 'strict', or 'fallback', got {policy:?}"
                )));
            }
        };
        Ok(PyCalibrationQuery(query))
    }
    fn __repr__(&self) -> String {
        format!(
            "CalibrationTable(path={:?}, rows={}, query=for_runs(run_scope, *, selectors))",
            self.path(),
            self.n_rows()
        )
    }
}

/// Explicit latest or per-period REST reconstruction selection.
#[pyclass(
    name = "ReconstructionSelection",
    module = "gluex",
    frozen,
    from_py_object
)]
#[derive(Clone)]
pub struct PyReconstructionSelection(pub(crate) ReconstructionSelection);
#[pymethods]
impl PyReconstructionSelection {
    #[staticmethod]
    fn latest() -> Self {
        Self(ReconstructionSelection::latest())
    }
    #[staticmethod]
    #[pyo3(signature = (*selections: "CalibratedRunPeriod | RESTVersionSelection | dict[RunPeriod | str, int | RESTVersionSelection]"))]
    fn periods(selections: &Bound<'_, PyTuple>) -> PyResult<Self> {
        if selections.len() == 1
            && let Ok(mapping) = selections.get_item(0)?.cast::<PyDict>()
        {
            return reconstruction_from_dict(mapping).map(Self);
        }
        let mut native = std::collections::BTreeMap::new();
        for value in selections.iter() {
            if let Ok(period) = value.extract::<PyRef<'_, super::core::PyCalibratedRunPeriod>>() {
                if native
                    .insert(period.0.period(), period.0.reconstruction().clone())
                    .is_some()
                {
                    return Err(PyValueError::new_err(format!(
                        "duplicate reconstruction meaning for run period {}",
                        period.0.period().short_name()
                    )));
                }
                continue;
            }
            let selection = value.extract::<PyRef<'_, super::core::PyRESTVersionSelection>>()?;
            let period = selection.1.ok_or_else(|| {
                pyo3::exceptions::PyTypeError::new_err(
                    "period selections must be created by RunPeriod.rest(...) so their periods are known",
                )
            })?;
            let mut requested = ReconstructionPeriod::new(selection.0);
            if let Some(variation) = &selection.2 {
                requested = requested.with_variation(variation);
            }
            if native.insert(period, requested).is_some() {
                return Err(PyValueError::new_err(format!(
                    "duplicate reconstruction meaning for run period {}",
                    period.short_name()
                )));
            }
        }
        Ok(Self(ReconstructionSelection::periods(native)))
    }
    /// Resolve a period to the exact CCDB variation and effective timestamp.
    #[pyo3(signature = (period: "RunPeriod | str"))]
    fn resolve(&self, period: &Bound<'_, PyAny>) -> PyResult<(String, String)> {
        let period = super::core::parse_run_period_object(period)?;
        let selection = match &self.0 {
            ReconstructionSelection::Latest => {
                ReconstructionPeriod::new(RESTVersionSelection::Current)
            }
            ReconstructionSelection::Periods(selections) => selections
                .get(&period)
                .ok_or_else(|| {
                    PyKeyError::new_err(format!(
                        "no reconstruction selection for {}",
                        period.short_name()
                    ))
                })?
                .clone(),
        };
        let context = selection
            .resolve(period)
            .map_err(|error| PyValueError::new_err(error.to_string()))?;
        Ok((context.variation, context.timestamp.to_rfc3339()))
    }
    fn __repr__(&self) -> String {
        match &self.0 {
            ReconstructionSelection::Latest => "ReconstructionSelection.latest()".to_owned(),
            ReconstructionSelection::Periods(periods) => format!(
                "ReconstructionSelection.periods(periods={:?})",
                periods
                    .keys()
                    .map(crate::RunPeriod::short_name)
                    .collect::<Vec<_>>()
            ),
        }
    }
}

/// Immutable named calibration column definition.
#[pyclass(name = "CalibrationColumn", module = "gluex", frozen)]
pub struct PyCalibrationColumn(crate::CalibrationColumn);
#[pymethods]
impl PyCalibrationColumn {
    #[getter]
    fn name(&self) -> &str {
        self.0.name()
    }
    #[getter]
    fn value_type(&self) -> String {
        self.0.value_type().as_str().to_owned()
    }
    fn __repr__(&self) -> String {
        format!(
            "CalibrationColumn(name={:?}, type={})",
            self.name(),
            self.value_type()
        )
    }
}

/// Captured source, table, numeric scope, variation and effective time.
#[pyclass(name = "CalibrationProvenance", module = "gluex", frozen)]
pub struct PyCalibrationProvenance(CalibrationProvenance);
#[pymethods]
impl PyCalibrationProvenance {
    #[getter]
    fn source(&self) -> &str {
        self.0.source()
    }
    #[getter]
    fn table(&self) -> &str {
        self.0.table()
    }
    #[getter]
    fn selection(&self) -> PyRunSelection {
        PyRunSelection(self.0.selection().clone())
    }
    #[getter]
    fn runs(&self) -> Option<PyRunProvenance> {
        self.0.runs().cloned().map(PyRunProvenance)
    }
    #[getter]
    fn run_report(&self) -> Option<PyRunReport> {
        self.0.run_report().cloned().map(PyRunReport)
    }
    #[getter]
    fn variation(&self) -> &str {
        self.0.variation()
    }
    #[getter]
    fn as_of(&self) -> chrono::DateTime<chrono::Utc> {
        self.0.as_of()
    }
    #[getter]
    fn resolved_reconstruction(
        &self,
    ) -> std::collections::BTreeMap<String, (String, chrono::DateTime<chrono::Utc>)> {
        self.0
            .resolved_reconstruction()
            .iter()
            .map(|(period, context)| {
                (
                    period.data_name().to_owned(),
                    (context.variation.clone(), context.timestamp),
                )
            })
            .collect()
    }
    #[getter]
    fn missing_policy(&self) -> &'static str {
        self.0.policy().as_str()
    }
    #[getter]
    fn fallback_run(&self) -> Option<RunNumber> {
        self.0.fallback_run()
    }
    fn __repr__(&self) -> String {
        format!("{:?}", self.0)
    }
}

/// Reusable lazy request. Call collect explicitly to retrieve constants.
#[pyclass(name = "CalibrationQuery", module = "gluex", frozen)]
pub struct PyCalibrationQuery(CalibrationQuery);
#[pymethods]
impl PyCalibrationQuery {
    /// Return an immutable query with a timeout in seconds.
    fn timeout(&self, seconds: f64) -> PyResult<Self> {
        Ok(Self(
            self.0
                .with_timeout(crate::python::execution::timeout(seconds)?),
        ))
    }
    /// Return a new query requesting this variation; invalid variations fail on collection.
    fn with_variation(&self, variation: String) -> Self {
        Self(self.0.with_variation(variation))
    }
    /// Return a new query with an inclusive cutoff. Requires a timezone-aware datetime.
    fn as_of(&self, timestamp: chrono::DateTime<chrono::Utc>) -> Self {
        Self(self.0.as_of(timestamp))
    }
    /// Resolve calibration selectors independently for each run period.
    fn with_reconstruction(&self, selection: &PyReconstructionSelection) -> Self {
        Self(self.0.with_reconstruction(selection.0.clone()))
    }
    /// Explicitly use source-opening defaults for all run periods.
    fn latest_reconstruction(&self) -> Self {
        Self(
            self.0
                .with_reconstruction(ReconstructionSelection::latest()),
        )
    }
    fn strict(&self) -> Self {
        Self(self.0.strict())
    }
    fn fallback_to(&self, run: RunNumber) -> Self {
        Self(self.0.fallback_to(run))
    }

    #[getter]
    fn provenance(&self) -> PyCalibrationProvenance {
        PyCalibrationProvenance(self.0.provenance().clone())
    }
    /// Collect numeric assignments and report missing runs. Releases the GIL.
    /// Execution and malformed-payload errors raise RuntimeError.
    fn collect(&self, py: Python<'_>) -> PyResult<PyCalibrationSeries> {
        crate::python::execution::PythonExecution::execute(py, &self.0, CalibrationQuery::collect)
            .map(PyCalibrationSeries)
    }
    #[pyo3(signature = (*, chunk_size=1024))]
    fn stream(&self, chunk_size: usize) -> PyResult<PyCalibrationStream> {
        crate::python::execution::PythonExecution::stream(&self.0, |query| query.stream(chunk_size))
            .map(|(stream, signals)| PyCalibrationStream(stream, signals))
    }
    fn first(&self, py: Python<'_>) -> PyResult<Option<PyCalibrationSeries>> {
        crate::python::execution::PythonExecution::execute(py, &self.0, CalibrationQuery::first)
            .map(|value| value.map(PyCalibrationSeries))
    }
    fn one(&self, py: Python<'_>) -> PyResult<PyCalibrationSeries> {
        crate::python::execution::PythonExecution::execute(py, &self.0, CalibrationQuery::one)
            .map(PyCalibrationSeries)
    }
    fn count(&self, py: Python<'_>) -> PyResult<usize> {
        crate::python::execution::PythonExecution::execute(py, &self.0, CalibrationQuery::count)
    }
    fn __repr__(&self) -> String {
        format!("CalibrationQuery({:?})", self.0.provenance())
    }
}

/// Iterator yielding bounded Calibration Series chunks.
#[pyclass(name = "CalibrationStream", module = "gluex")]
pub struct PyCalibrationStream(CalibrationStream, crate::python::execution::PythonExecution);
#[pymethods]
impl PyCalibrationStream {
    fn __iter__(slf: PyRef<'_, Self>) -> PyRef<'_, Self> {
        slf
    }
    fn __next__(&mut self, py: Python<'_>) -> PyResult<PyCalibrationSeries> {
        self.1
            .finish(py.detach(|| self.0.next().transpose()))?
            .map(PyCalibrationSeries)
            .ok_or_else(|| PyStopIteration::new_err(()))
    }
}

/// Immutable run-to-assignment association. Runs are numeric; RCDB is not consulted.
#[pyclass(name = "CalibrationSeries", module = "gluex", frozen)]
pub struct PyCalibrationSeries(CalibrationSeries);

fn dataframe_column(series: &CalibrationSeries, name: &str, rows: usize) -> PyResult<Column> {
    macro_rules! collect_values {
        ($variant:ident, $ty:ty) => {{
            let mut values = Vec::<$ty>::with_capacity(rows);
            for (_, entry) in series.items() {
                match entry.payload().column(name) {
                    Some(CalibrationColumnValues::$variant(column)) => {
                        values.extend_from_slice(column);
                    }
                    _ => {
                        return Err(PyValueError::new_err(format!(
                            "calibration column {name:?} has inconsistent types"
                        )));
                    }
                }
            }
            Ok(Column::new(name.into(), values))
        }};
    }

    let first = series
        .items()
        .next()
        .expect("dataframe columns require a non-empty calibration series")
        .1
        .payload()
        .column(name)
        .ok_or_else(|| PyKeyError::new_err(name.to_owned()))?;
    match first {
        CalibrationColumnValues::Int(_) => collect_values!(Int, i32),
        CalibrationColumnValues::UInt(_) => collect_values!(UInt, u32),
        CalibrationColumnValues::Long(_) => collect_values!(Long, i64),
        CalibrationColumnValues::ULong(_) => collect_values!(ULong, u64),
        CalibrationColumnValues::Double(_) => collect_values!(Double, f64),
        CalibrationColumnValues::String(_) => collect_values!(String, String),
        CalibrationColumnValues::Bool(_) => collect_values!(Bool, bool),
    }
}

#[pymethods]
impl PyCalibrationSeries {
    #[getter]
    fn runs(&self) -> TypedTuple<RunNumber> {
        TypedTuple(self.0.items().map(|(r, _)| *r).collect())
    }
    fn __len__(&self) -> usize {
        self.0.items().len()
    }
    fn __getitem__(&self, run: RunNumber) -> PyResult<PyCalibrationEntry> {
        self.0
            .get(run)
            .cloned()
            .map(PyCalibrationEntry)
            .ok_or_else(|| PyKeyError::new_err(run))
    }
    fn __iter__(&self) -> TypedIterator<RunNumber> {
        TypedIterator(self.0.items().map(|(r, _)| *r).collect())
    }
    fn items(&self) -> TypedTuple<(RunNumber, PyCalibrationEntry)> {
        TypedTuple(
            self.0
                .items()
                .map(|(r, e)| (*r, PyCalibrationEntry(e.clone())))
                .collect(),
        )
    }
    /// Convert to a Polars DataFrame with one row per run and payload row.
    fn to_polars(&self) -> PyResult<PolarsDataFrame> {
        let rows = self
            .0
            .items()
            .try_fold(0_usize, |total, (_, entry)| {
                total.checked_add(entry.payload().n_rows())
            })
            .ok_or_else(|| PyValueError::new_err("calibration DataFrame is too large"))?;
        let mut runs = Vec::<u32>::with_capacity(rows);
        for (run, entry) in self.0.items() {
            let run = u32::try_from(*run).map_err(|_| {
                PyValueError::new_err(format!(
                    "run number {run} cannot be represented as Polars UInt32"
                ))
            })?;
            runs.extend(std::iter::repeat_n(run, entry.payload().n_rows()));
        }
        let mut columns = vec![Column::new("run_number".into(), runs)];
        if let Some((_, first)) = self.0.items().next() {
            for name in first.payload().column_names() {
                if name == "run_number" {
                    return Err(PyValueError::new_err(
                        "calibration column name 'run_number' is reserved for DataFrame conversion",
                    ));
                }
                columns.push(dataframe_column(&self.0, name, rows)?);
            }
        }
        DataFrame::new(rows, columns)
            .map(|frame| PolarsDataFrame(PyDataFrame(frame)))
            .map_err(|error| PyValueError::new_err(error.to_string()))
    }
    #[getter]
    fn provenance(&self) -> PyCalibrationProvenance {
        PyCalibrationProvenance(self.0.provenance().clone())
    }
    #[getter]
    fn report(&self) -> PyCalibrationReport {
        PyCalibrationReport(self.0.report().clone())
    }
    fn __repr__(&self) -> String {
        format!(
            "CalibrationSeries(runs={}, table={:?})",
            self.__len__(),
            self.0.provenance().table()
        )
    }
}

/// Immutable effective assignment metadata and shared payload.
#[pyclass(name = "CalibrationEntry", module = "gluex", frozen)]
pub struct PyCalibrationEntry(CalibrationEntry);
#[pymethods]
impl PyCalibrationEntry {
    #[getter]
    fn assignment_id(&self) -> Id {
        self.0.assignment_id()
    }
    #[getter]
    fn constant_set_id(&self) -> Id {
        self.0.constant_set_id()
    }
    #[getter]
    fn created(&self) -> chrono::DateTime<chrono::Utc> {
        self.0.created()
    }
    #[getter]
    fn variation(&self) -> &str {
        self.0.variation()
    }
    #[getter]
    fn run_range(&self) -> (RunNumber, RunNumber) {
        self.0.run_range()
    }
    #[getter]
    fn payload(&self) -> PyCalibrationPayload {
        PyCalibrationPayload(self.0.clone())
    }
    fn __repr__(&self) -> String {
        format!(
            "CalibrationEntry(assignment_id={}, constant_set_id={})",
            self.assignment_id(),
            self.constant_set_id()
        )
    }
}

#[derive(IntoPyObject)]
pub enum CalibrationScalar {
    Int(i64),
    UInt(u64),
    Float(f64),
    Bool(bool),
    Text(String),
}

/// Immutable tabular constants shared between entries for the same constant set.
#[pyclass(name = "CalibrationPayload", module = "gluex", frozen)]
pub struct PyCalibrationPayload(CalibrationEntry);
#[pymethods]
impl PyCalibrationPayload {
    #[getter]
    fn columns(&self) -> TypedTuple<String> {
        TypedTuple(self.0.payload().column_names().to_vec())
    }
    fn __len__(&self) -> usize {
        self.0.payload().n_rows()
    }
    /// Return a named column as an immutable tuple; unknown names raise KeyError.
    fn column(&self, name: &str) -> PyResult<TypedTuple<CalibrationScalar>> {
        let c = self
            .0
            .payload()
            .column(name)
            .ok_or_else(|| PyKeyError::new_err(name.to_owned()))?;
        Ok(TypedTuple(match c {
            crate::CalibrationColumnValues::Int(v) => v
                .iter()
                .map(|v| CalibrationScalar::Int(i64::from(*v)))
                .collect(),
            crate::CalibrationColumnValues::UInt(v) => v
                .iter()
                .map(|v| CalibrationScalar::UInt(u64::from(*v)))
                .collect(),
            crate::CalibrationColumnValues::Long(v) => {
                v.iter().map(|v| CalibrationScalar::Int(*v)).collect()
            }
            crate::CalibrationColumnValues::ULong(v) => {
                v.iter().map(|v| CalibrationScalar::UInt(*v)).collect()
            }
            crate::CalibrationColumnValues::Double(v) => {
                v.iter().map(|v| CalibrationScalar::Float(*v)).collect()
            }
            crate::CalibrationColumnValues::Bool(v) => {
                v.iter().map(|v| CalibrationScalar::Bool(*v)).collect()
            }
            crate::CalibrationColumnValues::String(v) => v
                .iter()
                .map(|v| CalibrationScalar::Text(v.clone()))
                .collect(),
        }))
    }
    fn __getitem__(&self, name: &str) -> PyResult<TypedTuple<CalibrationScalar>> {
        self.column(name)
    }
    fn __repr__(&self) -> String {
        format!(
            "CalibrationPayload(rows={}, columns={:?})",
            self.__len__(),
            self.0.payload().column_names()
        )
    }
}

/// Completed missing-assignment diagnostics.
#[pyclass(name = "CalibrationReport", module = "gluex", frozen)]
pub struct PyCalibrationReport(CalibrationReport);
#[pymethods]
impl PyCalibrationReport {
    #[getter]
    fn missing_runs(&self) -> TypedTuple<RunNumber> {
        TypedTuple(self.0.missing_runs().to_vec())
    }
    #[getter]
    fn substitutions(&self) -> TypedTuple<(RunNumber, RunNumber)> {
        TypedTuple(self.0.substitutions().to_vec())
    }
    #[getter]
    fn evaluated_runs(&self) -> TypedTuple<RunNumber> {
        TypedTuple(self.0.evaluated_runs().to_vec())
    }
    #[getter]
    fn complete(&self) -> bool {
        self.0.complete()
    }
    fn __repr__(&self) -> String {
        format!("{:?}", self.0)
    }
}
