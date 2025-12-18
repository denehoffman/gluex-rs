use ::gluex_rcdb::{
    conditions::{self, Expr},
    context::Context,
    data::Value,
    database::RCDB,
    models::ValueType,
    RCDBError,
};
use chrono::{DateTime, Utc};
use gluex_core::{
    constants::{MAX_RUN_NUMBER, MIN_RUN_NUMBER},
    run_periods::RunPeriodError,
    RunNumber,
};
use pyo3::{
    exceptions::PyRuntimeError,
    prelude::*,
    types::{PyDict, PyFloat, PyInt, PyList, PyModule, PyString, PyTuple},
    Bound, IntoPyObject,
};

fn py_rcdb_error(err: RCDBError) -> PyErr {
    PyRuntimeError::new_err(err.to_string())
}

/// Boolean expression used to filter RCDB queries.
///
/// Examples
/// --------
/// >>> import gluex_rcdb as rcdb
/// >>> expr = rcdb.int_cond("event_count").gt(1000)
/// >>> ctx = rcdb.Context(filters=expr)
#[pyclass(name = "Expr", module = "gluex_rcdb")]
#[derive(Clone)]
pub struct PyExpr {
    expr: Expr,
}

impl PyExpr {
    fn new(expr: Expr) -> Self {
        Self { expr }
    }

    fn inner(&self) -> Expr {
        self.expr.clone()
    }
}

#[pymethods]
impl PyExpr {
    fn __repr__(&self) -> String {
        format!("Expr({})", self.expr)
    }

    fn __str__(&self) -> String {
        self.expr.to_string()
    }

    fn __invert__(&self) -> PyExpr {
        PyExpr::new(self.inner().negate())
    }
}

pub fn parse_context(
    py: Python<'_>,
    run_period: Option<String>,
    runs: Option<Vec<RunNumber>>,
    run_min: Option<RunNumber>,
    run_max: Option<RunNumber>,
    filters: Option<Py<PyAny>>,
) -> PyResult<Context> {
    let mut selection_kinds = 0;
    if run_period.is_some() {
        selection_kinds += 1;
    }
    if runs.is_some() {
        selection_kinds += 1;
    }
    if run_min.is_some() || run_max.is_some() {
        selection_kinds += 1;
    }
    if selection_kinds > 1 {
        return Err(PyRuntimeError::new_err(
            "run_period, runs, and run_min/run_max arguments are mutually exclusive",
        ));
    }

    let mut ctx = Context::default();
    if let Some(run_period) = run_period {
        ctx = ctx.with_run_period(
            run_period
                .parse()
                .map_err(|e: RunPeriodError| PyRuntimeError::new_err(e.to_string()))?,
        );
    } else if let Some(run_list) = runs {
        ctx = ctx.with_runs(run_list);
    } else if run_min.is_some() || run_max.is_some() {
        let start = run_min.unwrap_or(MIN_RUN_NUMBER);
        let end = run_max.unwrap_or(MAX_RUN_NUMBER);
        ctx = ctx.with_run_range(start..=end);
    }

    if let Some(filter_obj) = filters {
        let bound = filter_obj.into_bound(py);
        let exprs = exprs_from_object(bound)?;
        ctx = ctx.filter(exprs);
    }

    Ok(ctx)
}

/// Read-only RCDB client.
///
/// Parameters
/// ----------
/// path : str
///     Filesystem path to an RCDB SQLite database.
#[pyclass(name = "RCDB", module = "gluex_rcdb", unsendable)]
pub struct PyRCDB {
    inner: RCDB,
}

#[pymethods]
impl PyRCDB {
    #[new]
    #[pyo3(signature = (path), text_signature = "(path)")]
    /// Create a new RCDB connection.
    ///
    /// Parameters
    /// ----------
    /// path : str
    ///     Path to the RCDB SQLite database file.
    fn new(path: &str) -> PyResult<Self> {
        Ok(Self {
            inner: RCDB::open(path).map_err(py_rcdb_error)?,
        })
    }

    /// str: Filesystem path that was used to open the database.
    #[getter]
    pub fn connection_path(&self) -> &str {
        self.inner.connection_path()
    }

    /// fetch(self, condition_names, context=None)
    ///
    /// Parameters
    /// ----------
    /// condition_names : Sequence[str]
    ///     Condition names to retrieve per run.
    /// run_period : str, optional
    ///     The run period to use (short name, e.g. "S17", "F18").
    /// runs : Sequence[int], optional
    ///     Explicit list of run numbers. Duplicates are ignored.
    /// run_min : int, optional
    ///     Inclusive start of the run range. Defaults to the first run in RCDB
    ///     when only ``run_max`` is provided.
    /// run_max : int, optional
    ///     Inclusive end of the run range. Defaults to the last run in RCDB when
    ///     only ``run_min`` is provided.
    /// filters : Expr or Sequence[Expr], optional
    ///     One or more expressions that must evaluate to true.
    ///
    /// Returns
    /// -------
    /// dict[int, dict[str, object]]
    ///     Mapping of run number to dictionaries of condition values converted
    ///     into native Python scalars.
    ///
    /// Notes
    /// -----
    /// The run_period, runs, and (run_min, run_max) arguments are mutually exclusive.
    #[pyo3(signature = (condition_names, *, run_period=None, runs=None, run_min=None, run_max=None, filters=None))]
    pub fn fetch(
        &self,
        py: Python<'_>,
        condition_names: &Bound<'_, PyAny>,
        run_period: Option<String>,
        runs: Option<Vec<RunNumber>>,
        run_min: Option<RunNumber>,
        run_max: Option<RunNumber>,
        filters: Option<Py<PyAny>>,
    ) -> PyResult<Py<PyDict>> {
        let names = extract_name_list(condition_names)?;
        let ctx =
            parse_context(py, run_period, runs, run_min, run_max, filters).unwrap_or_default();
        let data = self.inner.fetch(names, &ctx).map_err(py_rcdb_error)?;
        let runs_dict = PyDict::new(py);
        for (run, values) in data {
            let value_dict = PyDict::new(py);
            for (name, value) in values {
                let py_value = value_to_python(py, &value)?;
                value_dict.set_item(name, py_value)?;
            }
            runs_dict.set_item(run, value_dict)?;
        }
        Ok(runs_dict.unbind())
    }

    /// fetch_runs(self, context=None)
    ///
    /// Parameters
    /// ----------
    /// run_period : str, optional
    ///     The run period to use (short name, e.g. "S17", "F18").
    /// runs : Sequence[int], optional
    ///     Explicit list of run numbers. Duplicates are ignored.
    /// run_min : int, optional
    ///     Inclusive start of the run range. Defaults to the first run in RCDB
    ///     when only ``run_max`` is provided.
    /// run_max : int, optional
    ///     Inclusive end of the run range. Defaults to the last run in RCDB when
    ///     only ``run_min`` is provided.
    /// filters : Expr or Sequence[Expr], optional
    ///     One or more expressions that must evaluate to true.
    ///
    /// Returns
    /// -------
    /// list[int]
    ///     Sorted run numbers satisfying the run number specifications and filters.
    ///
    /// Notes
    /// -----
    /// The run_period, runs, and (run_min, run_max) arguments are mutually exclusive.
    #[pyo3(signature = (*, run_period=None, runs=None, run_min=None, run_max=None, filters=None))]
    pub fn fetch_runs(
        &self,
        py: Python<'_>,
        run_period: Option<String>,
        runs: Option<Vec<RunNumber>>,
        run_min: Option<RunNumber>,
        run_max: Option<RunNumber>,
        filters: Option<Py<PyAny>>,
    ) -> PyResult<Vec<RunNumber>> {
        let ctx =
            parse_context(py, run_period, runs, run_min, run_max, filters).unwrap_or_default();
        self.inner.fetch_runs(&ctx).map_err(py_rcdb_error)
    }

    fn __repr__(&self) -> String {
        format!("RCDB(path='{}')", self.inner.connection_path())
    }

    fn __str__(&self) -> String {
        self.__repr__()
    }
}

/// Builder used to construct integer condition expressions.
#[pyclass(name = "IntCondition", module = "gluex_rcdb")]
#[derive(Clone)]
pub struct PyIntField(conditions::IntField);

#[pymethods]
impl PyIntField {
    /// eq(self, value)
    ///
    /// Parameters
    /// ----------
    /// value : int
    ///     Integer value the condition must equal.
    ///
    /// Returns
    /// -------
    /// Expr
    ///     Predicate yielding true when the condition equals ``value``.
    fn eq(&self, value: i64) -> PyExpr {
        PyExpr::new(self.0.clone().eq(value))
    }

    /// ne(self, value)
    ///
    /// Parameters
    /// ----------
    /// value : int
    ///     Integer value the condition must differ from.
    ///
    /// Returns
    /// -------
    /// Expr
    ///     Predicate yielding true when the condition is not ``value``.
    fn ne(&self, value: i64) -> PyExpr {
        PyExpr::new(self.0.clone().ne(value))
    }

    /// gt(self, value)
    ///
    /// Parameters
    /// ----------
    /// value : int
    ///     Threshold the condition must be strictly greater than.
    ///
    /// Returns
    /// -------
    /// Expr
    ///     Predicate representing ``condition > value``.
    fn gt(&self, value: i64) -> PyExpr {
        PyExpr::new(self.0.clone().gt(value))
    }

    /// ge(self, value)
    ///
    /// Parameters
    /// ----------
    /// value : int
    ///     Threshold the condition must be greater than or equal to.
    ///
    /// Returns
    /// -------
    /// Expr
    ///     Predicate representing ``condition >= value``.
    fn ge(&self, value: i64) -> PyExpr {
        PyExpr::new(self.0.clone().ge(value))
    }

    /// lt(self, value)
    ///
    /// Parameters
    /// ----------
    /// value : int
    ///     Threshold the condition must be strictly less than.
    ///
    /// Returns
    /// -------
    /// Expr
    ///     Predicate representing ``condition < value``.
    fn lt(&self, value: i64) -> PyExpr {
        PyExpr::new(self.0.clone().lt(value))
    }

    /// le(self, value)
    ///
    /// Parameters
    /// ----------
    /// value : int
    ///     Threshold the condition must be less than or equal to.
    ///
    /// Returns
    /// -------
    /// Expr
    ///     Predicate representing ``condition <= value``.
    fn le(&self, value: i64) -> PyExpr {
        PyExpr::new(self.0.clone().le(value))
    }

    fn __repr__(&self) -> String {
        "IntCondition(..)".to_string()
    }
}

/// Builder used to construct float condition expressions.
#[pyclass(name = "FloatCondition", module = "gluex_rcdb")]
#[derive(Clone)]
pub struct PyFloatField(conditions::FloatField);

#[pymethods]
impl PyFloatField {
    /// eq(self, value)
    ///
    /// Parameters
    /// ----------
    /// value : float
    ///     Floating-point value the condition must equal.
    ///
    /// Returns
    /// -------
    /// Expr
    ///     Predicate yielding true when the condition equals ``value``.
    fn eq(&self, value: f64) -> PyExpr {
        PyExpr::new(self.0.clone().eq(value))
    }

    /// gt(self, value)
    ///
    /// Parameters
    /// ----------
    /// value : float
    ///     Threshold the condition must be strictly greater than.
    ///
    /// Returns
    /// -------
    /// Expr
    ///     Predicate representing ``condition > value``.
    fn gt(&self, value: f64) -> PyExpr {
        PyExpr::new(self.0.clone().gt(value))
    }

    /// ge(self, value)
    ///
    /// Parameters
    /// ----------
    /// value : float
    ///     Threshold the condition must be greater than or equal to.
    ///
    /// Returns
    /// -------
    /// Expr
    ///     Predicate representing ``condition >= value``.
    fn ge(&self, value: f64) -> PyExpr {
        PyExpr::new(self.0.clone().ge(value))
    }

    /// lt(self, value)
    ///
    /// Parameters
    /// ----------
    /// value : float
    ///     Threshold the condition must be strictly less than.
    ///
    /// Returns
    /// -------
    /// Expr
    ///     Predicate representing ``condition < value``.
    fn lt(&self, value: f64) -> PyExpr {
        PyExpr::new(self.0.clone().lt(value))
    }

    /// le(self, value)
    ///
    /// Parameters
    /// ----------
    /// value : float
    ///     Threshold the condition must be less than or equal to.
    ///
    /// Returns
    /// -------
    /// Expr
    ///     Predicate representing ``condition <= value``.
    fn le(&self, value: f64) -> PyExpr {
        PyExpr::new(self.0.clone().le(value))
    }

    fn __repr__(&self) -> String {
        "FloatCondition(..)".to_string()
    }
}

/// Builder used to construct string condition expressions.
#[pyclass(name = "StringCondition", module = "gluex_rcdb")]
#[derive(Clone)]
pub struct PyStringField(conditions::StringField);

#[pymethods]
impl PyStringField {
    /// eq(self, value)
    ///
    /// Parameters
    /// ----------
    /// value : str
    ///     Exact string to match.
    ///
    /// Returns
    /// -------
    /// Expr
    ///     Predicate yielding true when the condition equals ``value``.
    fn eq(&self, value: &str) -> PyExpr {
        PyExpr::new(self.0.clone().eq(value))
    }

    /// ne(self, value)
    ///
    /// Parameters
    /// ----------
    /// value : str
    ///     String the condition must differ from.
    ///
    /// Returns
    /// -------
    /// Expr
    ///     Predicate requiring the condition to be anything except ``value``.
    fn ne(&self, value: &str) -> PyExpr {
        PyExpr::new(self.0.clone().ne(value))
    }

    /// isin(self, values)
    ///
    /// Parameters
    /// ----------
    /// values : Sequence[str]
    ///     Collection of strings that are considered valid.
    ///
    /// Returns
    /// -------
    /// Expr
    ///     Predicate representing membership in ``values``.
    fn isin(&self, values: &Bound<'_, PyAny>) -> PyResult<PyExpr> {
        let list: Vec<String> = values.extract()?;
        Ok(PyExpr::new(self.0.clone().isin(list)))
    }

    /// contains(self, value)
    ///
    /// Parameters
    /// ----------
    /// value : str
    ///     Substring that must appear within the condition value.
    ///
    /// Returns
    /// -------
    /// Expr
    ///     Predicate yielding true when ``value`` is a substring of the condition.
    fn contains(&self, value: &str) -> PyExpr {
        PyExpr::new(self.0.clone().contains(value))
    }

    fn __repr__(&self) -> String {
        "StringCondition(..)".to_string()
    }
}

/// Builder used to construct boolean condition expressions.
#[pyclass(name = "BoolCondition", module = "gluex_rcdb")]
#[derive(Clone)]
pub struct PyBoolField(conditions::BoolField);

#[pymethods]
impl PyBoolField {
    /// is_true(self)
    ///
    /// Returns
    /// -------
    /// Expr
    ///     Predicate requiring the condition to be explicitly true.
    fn is_true(&self) -> PyExpr {
        PyExpr::new(self.0.clone().is_true())
    }

    /// is_false(self)
    ///
    /// Returns
    /// -------
    /// Expr
    ///     Predicate requiring the condition to be explicitly false.
    fn is_false(&self) -> PyExpr {
        PyExpr::new(self.0.clone().is_false())
    }

    /// exists(self)
    ///
    /// Returns
    /// -------
    /// Expr
    ///     Predicate that yields true when the boolean condition is present,
    ///     regardless of its value.
    fn exists(&self) -> PyExpr {
        PyExpr::new(self.0.clone().exists())
    }

    fn __repr__(&self) -> String {
        "BoolCondition(..)".to_string()
    }
}

/// Builder used to construct timestamp condition expressions.
#[pyclass(name = "TimeCondition", module = "gluex_rcdb")]
#[derive(Clone)]
pub struct PyTimeField(conditions::TimeField);

#[pymethods]
impl PyTimeField {
    /// eq(self, value)
    ///
    /// Parameters
    /// ----------
    /// value : datetime
    ///     Timestamp the condition must equal.
    ///
    /// Returns
    /// -------
    /// Expr
    ///     Predicate yielding true when the condition equals ``value``.
    fn eq(&self, value: DateTime<Utc>) -> PyResult<PyExpr> {
        Ok(PyExpr::new(self.0.clone().eq(value)))
    }

    /// gt(self, value)
    ///
    /// Parameters
    /// ----------
    /// value : datetime
    ///     Timestamp that defines the lower bound (exclusive).
    ///
    /// Returns
    /// -------
    /// Expr
    ///     Predicate representing ``condition > value``.
    fn gt(&self, value: DateTime<Utc>) -> PyResult<PyExpr> {
        Ok(PyExpr::new(self.0.clone().gt(value)))
    }

    /// ge(self, value)
    ///
    /// Parameters
    /// ----------
    /// value : datetime
    ///     Timestamp used as a lower bound (inclusive).
    ///
    /// Returns
    /// -------
    /// Expr
    ///     Predicate representing ``condition >= value``.
    fn ge(&self, value: DateTime<Utc>) -> PyResult<PyExpr> {
        Ok(PyExpr::new(self.0.clone().ge(value)))
    }

    /// lt(self, value)
    ///
    /// Parameters
    /// ----------
    /// value : datetime
    ///     Timestamp used as an upper bound (exclusive).
    ///
    /// Returns
    /// -------
    /// Expr
    ///     Predicate representing ``condition < value``.
    fn lt(&self, value: DateTime<Utc>) -> PyResult<PyExpr> {
        Ok(PyExpr::new(self.0.clone().lt(value)))
    }

    /// le(self, value)
    ///
    /// Parameters
    /// ----------
    /// value : datetime
    ///     Timestamp used as an upper bound (inclusive).
    ///
    /// Returns
    /// -------
    /// Expr
    ///     Predicate representing ``condition <= value``.
    fn le(&self, value: DateTime<Utc>) -> PyResult<PyExpr> {
        Ok(PyExpr::new(self.0.clone().le(value)))
    }

    fn __repr__(&self) -> String {
        "TimeCondition(..)".to_string()
    }
}

#[pyfunction(name = "int_cond", text_signature = "(name)")]
/// int_cond(name)
///
/// Parameters
/// ----------
/// name : str
///     Condition name to treat as an integer column.
///
/// Returns
/// -------
/// IntCondition
///     Builder exposing numeric comparison helpers.
fn int_cond(name: &str) -> PyIntField {
    PyIntField(conditions::int_cond(name))
}

#[pyfunction(name = "float_cond", text_signature = "(name)")]
/// float_cond(name)
///
/// Parameters
/// ----------
/// name : str
///     Condition name to treat as a floating-point column.
///
/// Returns
/// -------
/// FloatCondition
///     Builder exposing numeric comparison helpers.
fn float_cond(name: &str) -> PyFloatField {
    PyFloatField(conditions::float_cond(name))
}

#[pyfunction(name = "string_cond", text_signature = "(name)")]
/// string_cond(name)
///
/// Parameters
/// ----------
/// name : str
///     Condition name to treat as textual data.
///
/// Returns
/// -------
/// StringCondition
///     Builder exposing string comparison helpers.
fn string_cond(name: &str) -> PyStringField {
    PyStringField(conditions::string_cond(name))
}

#[pyfunction(name = "bool_cond", text_signature = "(name)")]
/// bool_cond(name)
///
/// Parameters
/// ----------
/// name : str
///     Condition name to treat as a boolean column.
///
/// Returns
/// -------
/// BoolCondition
///     Builder exposing boolean predicates.
fn bool_cond(name: &str) -> PyBoolField {
    PyBoolField(conditions::bool_cond(name))
}

#[pyfunction(name = "time_cond", text_signature = "(name)")]
/// time_cond(name)
///
/// Parameters
/// ----------
/// name : str
///     Condition name storing RFC3339 timestamps.
///
/// Returns
/// -------
/// TimeCondition
///     Builder exposing timestamp comparison helpers.
fn time_cond(name: &str) -> PyTimeField {
    PyTimeField(conditions::time_cond(name))
}

#[pyfunction(name = "all", signature = (*exprs))]
/// all(*exprs)
///
/// Parameters
/// ----------
/// exprs : Sequence[Expr]
///     One or more expressions that must all evaluate to true.
///
/// Returns
/// -------
/// Expr
///     Composite expression equivalent to logical AND.
fn all(exprs: &Bound<'_, PyTuple>) -> PyResult<PyExpr> {
    Ok(PyExpr::new(conditions::all(tuple_to_exprs(exprs)?)))
}

#[pyfunction(name = "any", signature = (*exprs))]
/// any(*exprs)
///
/// Parameters
/// ----------
/// exprs : Sequence[Expr]
///     One or more expressions where at least one must evaluate to true.
///
/// Returns
/// -------
/// Expr
///     Composite expression equivalent to logical OR.
fn any(exprs: &Bound<'_, PyTuple>) -> PyResult<PyExpr> {
    Ok(PyExpr::new(conditions::any(tuple_to_exprs(exprs)?)))
}

/// Common aliases for expressions used in RCDB filters.
#[pyclass(name = "aliases", module = "gluex_rcdb")]
#[derive(Clone)]
pub struct Aliases;

#[pymethods]
impl Aliases {
    /// Expr: Expression selecting GlueX production runs.
    #[getter]
    pub fn is_production(&self) -> PyExpr {
        PyExpr::new(conditions::aliases::is_production())
    }
    /// Expr: Expression selecting 2018 production runs.
    #[getter]
    pub fn is_2018production(&self) -> PyExpr {
        PyExpr::new(conditions::aliases::is_2018production())
    }
    /// Expr: Expression selecting PrimEx production runs.
    #[getter]
    pub fn is_primex_production(&self) -> PyExpr {
        PyExpr::new(conditions::aliases::is_primex_production())
    }
    /// Expr: Expression selecting DIRC production runs.
    #[getter]
    pub fn is_dirc_production(&self) -> PyExpr {
        PyExpr::new(conditions::aliases::is_dirc_production())
    }
    /// Expr: Expression selecting SRC production runs.
    #[getter]
    pub fn is_src_production(&self) -> PyExpr {
        PyExpr::new(conditions::aliases::is_src_production())
    }
    /// Expr: Expression selecting CPP production runs.
    #[getter]
    pub fn is_cpp_production(&self) -> PyExpr {
        PyExpr::new(conditions::aliases::is_cpp_production())
    }
    /// Expr: Expression selecting long-mode production runs.
    #[getter]
    pub fn is_production_long(&self) -> PyExpr {
        PyExpr::new(conditions::aliases::is_production_long())
    }
    /// Expr: Expression selecting cosmic calibration runs.
    #[getter]
    pub fn is_cosmic(&self) -> PyExpr {
        PyExpr::new(conditions::aliases::is_cosmic())
    }
    /// Expr: Expression selecting runs with the empty target configuration.
    #[getter]
    pub fn is_empty_target(&self) -> PyExpr {
        PyExpr::new(conditions::aliases::is_empty_target())
    }
    /// Expr: Expression selecting runs with the amorphous radiator.
    #[getter]
    pub fn is_amorph_radiator(&self) -> PyExpr {
        PyExpr::new(conditions::aliases::is_amorph_radiator())
    }
    /// Expr: Expression selecting coherent-beam running.
    #[getter]
    pub fn is_coherent_beam(&self) -> PyExpr {
        PyExpr::new(conditions::aliases::is_coherent_beam())
    }
    /// Expr: Expression selecting runs with the solenoid field off.
    #[getter]
    pub fn is_field_off(&self) -> PyExpr {
        PyExpr::new(conditions::aliases::is_field_off())
    }
    /// Expr: Expression selecting runs with the solenoid field on.
    #[getter]
    pub fn is_field_on(&self) -> PyExpr {
        PyExpr::new(conditions::aliases::is_field_on())
    }
    /// Expr: Expression matching runs whose status is 'calibration'.
    #[getter]
    pub fn status_calibration(&self) -> PyExpr {
        PyExpr::new(conditions::aliases::status_calibration())
    }
    /// Expr: Expression matching runs approved for long-mode production.
    #[getter]
    pub fn status_approved_long(&self) -> PyExpr {
        PyExpr::new(conditions::aliases::status_approved_long())
    }
    /// Expr: Expression matching runs approved for physics production.
    #[getter]
    pub fn status_approved(&self) -> PyExpr {
        PyExpr::new(conditions::aliases::status_approved())
    }
    /// Expr: Expression matching runs whose status is 'unchecked'.
    #[getter]
    pub fn status_unchecked(&self) -> PyExpr {
        PyExpr::new(conditions::aliases::status_unchecked())
    }
    /// Expr: Expression matching runs that were rejected.
    #[getter]
    pub fn status_reject(&self) -> PyExpr {
        PyExpr::new(conditions::aliases::status_reject())
    }
    pub fn approved_production(&self, run_period: String) -> PyResult<PyExpr> {
        Ok(PyExpr::new(conditions::aliases::approved_production(
            run_period
                .parse()
                .map_err(|e: RunPeriodError| PyRuntimeError::new_err(e.to_string()))?,
        )))
    }
}

fn tuple_to_exprs(exprs: &Bound<'_, PyTuple>) -> PyResult<Vec<Expr>> {
    exprs.iter().map(|item| extract_expr(&item)).collect()
}

fn exprs_from_object(obj: Bound<'_, PyAny>) -> PyResult<Vec<Expr>> {
    if obj.is_instance_of::<PyExpr>() {
        return Ok(vec![extract_expr(&obj)?]);
    }
    if obj.is_instance_of::<PyTuple>() {
        let tuple = obj.cast::<PyTuple>()?;
        return tuple_to_exprs(tuple);
    }
    if obj.is_instance_of::<PyList>() {
        let list = obj.cast::<PyList>()?;
        return list.iter().map(|item| extract_expr(&item)).collect();
    }
    Err(PyRuntimeError::new_err(
        "filters must be an Expr or sequence of Expr objects",
    ))
}

fn extract_expr(obj: &Bound<'_, PyAny>) -> PyResult<Expr> {
    let expr: Py<PyExpr> = obj.extract()?;
    let borrowed = expr.borrow(obj.py());
    Ok(borrowed.inner())
}

fn value_to_python(py: Python<'_>, value: &Value) -> PyResult<Py<PyAny>> {
    let obj = match value.value_type() {
        ValueType::String | ValueType::Json | ValueType::Blob => value
            .as_string()
            .map(|s| PyString::new(py, s).into_any().unbind())
            .unwrap_or_else(|| py.None()),
        ValueType::Int => {
            if let Some(v) = value.as_int() {
                PyInt::new(py, v).into_any().unbind()
            } else {
                py.None()
            }
        }
        ValueType::Float => {
            if let Some(v) = value.as_float() {
                PyFloat::new(py, v).into_any().unbind()
            } else {
                py.None()
            }
        }
        ValueType::Bool => {
            if let Some(v) = value.as_bool() {
                let obj = v.into_pyobject(py)?;
                obj.to_owned().into_any().unbind()
            } else {
                py.None()
            }
        }
        ValueType::Time => {
            if let Some(dt) = value.as_time() {
                PyString::new(py, &dt.to_rfc3339()).into_any().unbind()
            } else {
                py.None()
            }
        }
    };
    Ok(obj)
}

fn extract_name_list(names: &Bound<'_, PyAny>) -> PyResult<Vec<String>> {
    names
        .extract::<Vec<String>>()
        .map_err(|_| PyRuntimeError::new_err("condition_names must be a sequence of strings"))
}

#[pymodule]
/// Python module initializer for gluex_rcdb bindings.
pub fn gluex_rcdb(py: Python<'_>, m: &Bound<'_, PyModule>) -> PyResult<()> {
    m.add_class::<PyRCDB>()?;
    m.add_class::<PyExpr>()?;
    m.add_class::<PyIntField>()?;
    m.add_class::<PyFloatField>()?;
    m.add_class::<PyStringField>()?;
    m.add_class::<PyBoolField>()?;
    m.add_class::<PyTimeField>()?;
    m.add_function(wrap_pyfunction!(int_cond, m)?)?;
    m.add_function(wrap_pyfunction!(float_cond, m)?)?;
    m.add_function(wrap_pyfunction!(string_cond, m)?)?;
    m.add_function(wrap_pyfunction!(bool_cond, m)?)?;
    m.add_function(wrap_pyfunction!(time_cond, m)?)?;
    m.add_function(wrap_pyfunction!(all, m)?)?;
    m.add_function(wrap_pyfunction!(any, m)?)?;
    let aliases = Py::new(py, Aliases)?;
    m.add("aliases", aliases)?;
    Ok(())
}
