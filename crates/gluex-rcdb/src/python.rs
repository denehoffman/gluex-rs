#![cfg(feature = "python")]
use crate::{
    cond::{self, ConditionAlias, Expr},
    context::Context,
    data::Value,
    database::RCDB,
    models::ValueType,
    RCDBError,
};
use chrono::{DateTime, Utc};
use gluex_core::{parsers::parse_timestamp, RunNumber};
use pyo3::{
    conversion::IntoPyObject,
    exceptions::PyRuntimeError,
    prelude::*,
    types::{PyDict, PyFloat, PyInt, PyString, PyTuple},
    Bound,
};

impl From<RCDBError> for PyErr {
    fn from(value: RCDBError) -> Self {
        PyRuntimeError::new_err(value.to_string())
    }
}

/// Lightweight wrapper around RCDB filtering expressions.
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

#[allow(missing_docs)]
#[pymethods]
impl PyExpr {
    fn __repr__(&self) -> String {
        "Expr(...)".to_string()
    }

    fn __str__(&self) -> String {
        self.__repr__()
    }
}

/// Context describing which runs participate in a query.
#[pyclass(name = "Context", module = "gluex_rcdb")]
#[derive(Clone)]
pub struct PyContext {
    inner: Context,
}

#[allow(missing_docs)]
#[pymethods]
impl PyContext {
    #[new]
    fn new() -> Self {
        Self {
            inner: Context::default(),
        }
    }

    /// Copy the context so it can be reused elsewhere.
    fn copy(&self) -> PyResult<Self> {
        Ok(self.clone())
    }

    /// Restrict the context to the single run `run`.
    fn with_run(&mut self, run: RunNumber) -> PyResult<Self> {
        self.inner = self.inner.clone().with_run(run);
        Ok(self.clone())
    }

    /// Restrict the context to the supplied runs.
    fn with_runs(&mut self, runs: Vec<RunNumber>) -> PyResult<Self> {
        self.inner = self.inner.clone().with_runs(runs);
        Ok(self.clone())
    }

    /// Restrict the context to the inclusive run range `[start, end]`.
    fn with_run_range(&mut self, start: RunNumber, end: RunNumber) -> PyResult<Self> {
        self.inner = self.inner.clone().with_run_range(start..=end);
        Ok(self.clone())
    }

    /// Append one or more predicate expressions. All supplied expressions must evaluate to true.
    #[pyo3(signature = (*exprs))]
    fn filter(&mut self, exprs: &Bound<'_, PyTuple>) -> PyResult<Self> {
        let expr_list = tuple_to_exprs(exprs)?;
        self.inner = self.inner.clone().filter(expr_list);
        Ok(self.clone())
    }

    /// list[int]: Runs recorded in the context when it targets explicit runs.
    #[getter]
    fn runs(&self) -> Vec<RunNumber> {
        self.inner.runs().unwrap_or(&[]).to_vec()
    }

    fn __repr__(&self) -> String {
        format!("Context(runs={:?})", self.inner.runs())
    }

    fn __str__(&self) -> String {
        self.__repr__()
    }
}

impl PyContext {
    fn inner(&self) -> Context {
        self.inner.clone()
    }
}

/// Typed value stored in RCDB.
#[pyclass(name = "Value", module = "gluex_rcdb")]
#[derive(Clone)]
pub struct PyValue {
    value: Value,
}

#[allow(missing_docs)]
#[pymethods]
impl PyValue {
    /// str: Name of the RCDB type backing the value.
    #[getter]
    fn value_type(&self) -> &'static str {
        self.value.value_type().as_str()
    }

    /// Returns the value as a string, when possible.
    fn as_string(&self) -> Option<String> {
        self.value.as_string().map(|s| s.to_string())
    }

    /// Returns the value as an integer, when possible.
    fn as_int(&self) -> Option<i64> {
        self.value.as_int()
    }

    /// Returns the value as a float, when possible.
    fn as_float(&self) -> Option<f64> {
        self.value.as_float()
    }

    /// Returns the value as a boolean, when possible.
    fn as_bool(&self) -> Option<bool> {
        self.value.as_bool()
    }

    /// Returns the value as an RFC3339 timestamp, when possible.
    fn as_time(&self) -> Option<String> {
        self.value.as_time().map(|dt| dt.to_rfc3339())
    }

    /// Convert the value into a native Python scalar.
    fn to_python(&self, py: Python<'_>) -> PyResult<Py<PyAny>> {
        value_to_python(py, &self.value)
    }

    fn __repr__(&self) -> String {
        format!("Value(type='{}')", self.value.value_type().as_str())
    }

    fn __str__(&self) -> String {
        self.__repr__()
    }
}

impl From<Value> for PyValue {
    fn from(value: Value) -> Self {
        Self { value }
    }
}

/// Read-only RCDB client.
#[pyclass(name = "RCDB", module = "gluex_rcdb", unsendable)]
pub struct PyRCDB {
    inner: RCDB,
}

#[allow(missing_docs)]
#[pymethods]
impl PyRCDB {
    #[new]
    fn new(path: &str) -> PyResult<Self> {
        Ok(Self {
            inner: RCDB::open(path)?,
        })
    }

    /// Open an RCDB SQLite file at `path`.
    #[staticmethod]
    pub fn open(path: &str) -> PyResult<Self> {
        Self::new(path)
    }

    /// str: Filesystem path that was used to open the database.
    #[getter]
    pub fn connection_path(&self) -> &str {
        self.inner.connection_path()
    }

    /// Fetch one or more conditions for the supplied context.
    ///
    /// Parameters
    /// ----------
    /// condition_names:
    ///     Iterable of condition names to request.
    /// context:
    ///     Optional `Context` limiting the runs inspected. When omitted a default context is used.
    ///
    /// Returns
    /// -------
    /// dict[int, dict[str, Value]]
    ///     Mapping of run numbers to condition values.
    #[pyo3(signature = (condition_names, context=None))]
    pub fn fetch(
        &self,
        py: Python<'_>,
        condition_names: &Bound<'_, PyAny>,
        context: Option<&PyContext>,
    ) -> PyResult<Py<PyDict>> {
        let names = extract_name_list(condition_names)?;
        let ctx = context
            .map(|ctx| ctx.inner())
            .unwrap_or_else(Context::default);
        let data = self.inner.fetch(names, &ctx)?;
        let runs_dict = PyDict::new(py);
        for (run, values) in data {
            let value_dict = PyDict::new(py);
            for (name, value) in values {
                value_dict.set_item(name, PyValue::from(value))?;
            }
            runs_dict.set_item(run, value_dict)?;
        }
        Ok(runs_dict.unbind())
    }

    /// Return the run numbers that satisfy the supplied context filters.
    #[pyo3(signature = (context=None))]
    pub fn fetch_runs(&self, context: Option<&PyContext>) -> PyResult<Vec<RunNumber>> {
        let ctx = context
            .map(|ctx| ctx.inner())
            .unwrap_or_else(Context::default);
        Ok(self.inner.fetch_runs(&ctx)?)
    }
}

/// Builder used to construct integer condition expressions within Python.
#[pyclass(name = "IntCondition", module = "gluex_rcdb")]
#[derive(Clone)]
pub struct PyIntField(cond::IntField);

#[allow(missing_docs)]
#[pymethods]
impl PyIntField {
    fn eq(&self, value: i64) -> PyExpr {
        PyExpr::new(self.0.clone().eq(value))
    }
    fn neq(&self, value: i64) -> PyExpr {
        PyExpr::new(self.0.clone().neq(value))
    }
    fn gt(&self, value: i64) -> PyExpr {
        PyExpr::new(self.0.clone().gt(value))
    }
    fn geq(&self, value: i64) -> PyExpr {
        PyExpr::new(self.0.clone().geq(value))
    }
    fn lt(&self, value: i64) -> PyExpr {
        PyExpr::new(self.0.clone().lt(value))
    }
    fn leq(&self, value: i64) -> PyExpr {
        PyExpr::new(self.0.clone().leq(value))
    }
    fn __repr__(&self) -> String {
        "IntCondition(..)".to_string()
    }
}

/// Builder used to construct float condition expressions.
#[pyclass(name = "FloatCondition", module = "gluex_rcdb")]
#[derive(Clone)]
pub struct PyFloatField(cond::FloatField);

#[allow(missing_docs)]
#[pymethods]
impl PyFloatField {
    fn eq(&self, value: f64) -> PyExpr {
        PyExpr::new(self.0.clone().eq(value))
    }
    fn gt(&self, value: f64) -> PyExpr {
        PyExpr::new(self.0.clone().gt(value))
    }
    fn geq(&self, value: f64) -> PyExpr {
        PyExpr::new(self.0.clone().geq(value))
    }
    fn lt(&self, value: f64) -> PyExpr {
        PyExpr::new(self.0.clone().lt(value))
    }
    fn leq(&self, value: f64) -> PyExpr {
        PyExpr::new(self.0.clone().leq(value))
    }
    fn __repr__(&self) -> String {
        "FloatCondition(..)".to_string()
    }
}

/// Builder used to construct string condition expressions.
#[pyclass(name = "StringCondition", module = "gluex_rcdb")]
#[derive(Clone)]
pub struct PyStringField(cond::StringField);

#[allow(missing_docs)]
#[pymethods]
impl PyStringField {
    fn eq(&self, value: &str) -> PyExpr {
        PyExpr::new(self.0.clone().eq(value))
    }
    fn neq(&self, value: &str) -> PyExpr {
        PyExpr::new(self.0.clone().neq(value))
    }
    fn isin(&self, values: &Bound<'_, PyAny>) -> PyResult<PyExpr> {
        let list: Vec<String> = values.extract()?;
        Ok(PyExpr::new(self.0.clone().isin(list)))
    }
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
pub struct PyBoolField(cond::BoolField);

#[allow(missing_docs)]
#[pymethods]
impl PyBoolField {
    fn is_true(&self) -> PyExpr {
        PyExpr::new(self.0.clone().is_true())
    }
    fn is_false(&self) -> PyExpr {
        PyExpr::new(self.0.clone().is_false())
    }
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
pub struct PyTimeField(cond::TimeField);

#[allow(missing_docs)]
#[pymethods]
impl PyTimeField {
    fn eq(&self, value: &str) -> PyResult<PyExpr> {
        Ok(PyExpr::new(self.0.clone().eq(parse_py_time(value)?)))
    }

    fn gt(&self, value: &str) -> PyResult<PyExpr> {
        Ok(PyExpr::new(self.0.clone().gt(parse_py_time(value)?)))
    }

    fn geq(&self, value: &str) -> PyResult<PyExpr> {
        Ok(PyExpr::new(self.0.clone().geq(parse_py_time(value)?)))
    }

    fn lt(&self, value: &str) -> PyResult<PyExpr> {
        Ok(PyExpr::new(self.0.clone().lt(parse_py_time(value)?)))
    }

    fn lte(&self, value: &str) -> PyResult<PyExpr> {
        Ok(PyExpr::new(self.0.clone().lte(parse_py_time(value)?)))
    }

    fn __repr__(&self) -> String {
        "TimeCondition(..)".to_string()
    }
}

/// Alias metadata exposed to Python callers.
#[pyclass(name = "ConditionAlias", module = "gluex_rcdb")]
pub struct PyConditionAlias {
    alias: &'static ConditionAlias,
}

#[allow(missing_docs)]
#[pymethods]
impl PyConditionAlias {
    /// str: Name of the alias.
    #[getter]
    fn name(&self) -> &'static str {
        self.alias.name
    }

    /// str: Human-readable description of the alias.
    #[getter]
    fn comment(&self) -> &'static str {
        self.alias.comment
    }

    /// Returns the expression represented by this alias.
    fn expression(&self) -> PyExpr {
        PyExpr::new(self.alias.expression())
    }

    fn __repr__(&self) -> String {
        format!("ConditionAlias(name='{}')", self.alias.name)
    }
}

/// Create an integer condition builder.
#[pyfunction]
fn int_cond(name: &str) -> PyIntField {
    PyIntField(cond::int_cond(name))
}

/// Create a floating-point condition builder.
#[pyfunction]
fn float_cond(name: &str) -> PyFloatField {
    PyFloatField(cond::float_cond(name))
}

/// Create a string condition builder.
#[pyfunction]
fn string_cond(name: &str) -> PyStringField {
    PyStringField(cond::string_cond(name))
}

/// Create a boolean condition builder.
#[pyfunction]
fn bool_cond(name: &str) -> PyBoolField {
    PyBoolField(cond::bool_cond(name))
}

/// Create a timestamp condition builder.
#[pyfunction]
fn time_cond(name: &str) -> PyTimeField {
    PyTimeField(cond::time_cond(name))
}

/// Combine expressions with logical AND semantics.
#[pyfunction(signature = (*exprs))]
fn all(exprs: &Bound<'_, PyTuple>) -> PyResult<PyExpr> {
    Ok(PyExpr::new(cond::all(tuple_to_exprs(exprs)?)))
}

/// Combine expressions with logical OR semantics.
#[pyfunction(signature = (*exprs))]
fn any(exprs: &Bound<'_, PyTuple>) -> PyResult<PyExpr> {
    Ok(PyExpr::new(cond::any(tuple_to_exprs(exprs)?)))
}

/// Return the expression associated with an alias name, if any.
#[pyfunction]
fn alias(name: &str) -> Option<PyExpr> {
    cond::alias(name).map(PyExpr::new)
}

/// Returns all built-in aliases.
#[pyfunction]
fn aliases() -> Vec<PyConditionAlias> {
    cond::DEFAULT_ALIASES
        .iter()
        .map(|alias| PyConditionAlias { alias })
        .collect()
}

fn tuple_to_exprs(exprs: &Bound<'_, PyTuple>) -> PyResult<Vec<Expr>> {
    exprs
        .iter()
        .map(|item| -> PyResult<Expr> {
            let expr = item.extract::<PyRef<PyExpr>>()?;
            Ok(expr.inner())
        })
        .collect()
}

fn extract_name_list(names: &Bound<'_, PyAny>) -> PyResult<Vec<String>> {
    if let Ok(list) = names.extract::<Vec<String>>() {
        return Ok(list);
    }
    Err(PyRuntimeError::new_err(
        "condition_names must be a sequence of strings",
    ))
}

fn parse_py_time(value: &str) -> PyResult<DateTime<Utc>> {
    parse_timestamp(value).map_err(|err| PyRuntimeError::new_err(err.to_string()))
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

#[pymodule]
/// Python module initializer for gluex_rcdb bindings.
pub fn gluex_rcdb(_py: Python<'_>, m: &Bound<'_, PyModule>) -> PyResult<()> {
    m.add_class::<PyRCDB>()?;
    m.add_class::<PyContext>()?;
    m.add_class::<PyExpr>()?;
    m.add_class::<PyValue>()?;
    m.add_class::<PyIntField>()?;
    m.add_class::<PyFloatField>()?;
    m.add_class::<PyStringField>()?;
    m.add_class::<PyBoolField>()?;
    m.add_class::<PyTimeField>()?;
    m.add_class::<PyConditionAlias>()?;
    m.add_function(wrap_pyfunction!(int_cond, m)?)?;
    m.add_function(wrap_pyfunction!(float_cond, m)?)?;
    m.add_function(wrap_pyfunction!(string_cond, m)?)?;
    m.add_function(wrap_pyfunction!(bool_cond, m)?)?;
    m.add_function(wrap_pyfunction!(time_cond, m)?)?;
    m.add_function(wrap_pyfunction!(all, m)?)?;
    m.add_function(wrap_pyfunction!(any, m)?)?;
    m.add_function(wrap_pyfunction!(alias, m)?)?;
    m.add_function(wrap_pyfunction!(aliases, m)?)?;
    Ok(())
}
