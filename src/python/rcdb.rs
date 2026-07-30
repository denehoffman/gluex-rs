#[pyo3::pymodule(submodule)]
pub(crate) mod rcdb {
    use std::env;

    use crate::core::{
        RunNumber,
        constants::{MAX_RUN_NUMBER, MIN_RUN_NUMBER},
        utils::resolve_path,
    };
    use crate::rcdb::{
        RCDB, RCDBContext, RCDBError, Value, ValueType,
        conditions::{self, Expr},
    };
    use chrono::{DateTime, Utc};
    use pyo3::{
        IntoPyObject,
        exceptions::PyRuntimeError,
        prelude::*,
        types::{PyDict, PyFloat, PyInt, PyList, PyString, PyTuple},
    };

    use crate::python::core::parse_run_period_object;

    fn py_rcdb_error(err: RCDBError) -> PyErr {
        PyRuntimeError::new_err(err.to_string())
    }

    fn resolve_connection_path(path: Option<String>) -> PyResult<String> {
        let raw_path = match path {
            Some(value) if !value.is_empty() => value,
            _ => env::var("RCDB_CONNECTION").map_err(|_| {
                PyRuntimeError::new_err("RCDB_CONNECTION is not set and no path was provided")
            })?,
        };
        resolve_path(raw_path)
            .map(|path| path.to_string_lossy().to_string())
            .map_err(|err| PyRuntimeError::new_err(err.to_string()))
    }

    #[pyclass(name = "Expr", module = "gluex.rcdb", skip_from_py_object)]
    #[derive(Clone)]
    pub struct PyExpr(Expr);

    #[pymethods]
    impl PyExpr {
        fn __repr__(&self) -> String {
            format!("Expr({})", self.0)
        }

        fn __str__(&self) -> String {
            self.0.to_string()
        }

        fn __invert__(&self) -> Self {
            Self(self.0.clone().negate())
        }
    }

    #[pyclass(name = "RCDB", module = "gluex.rcdb", unsendable)]
    pub struct PyRCDB(RCDB);

    #[pymethods]
    impl PyRCDB {
        #[new]
        #[pyo3(signature = (path=None))]
        fn new(path: Option<String>) -> PyResult<Self> {
            let path = resolve_connection_path(path)?;
            RCDB::open(path).map(Self).map_err(py_rcdb_error)
        }

        #[getter]
        fn connection_path(&self) -> &str {
            self.0.connection_path()
        }

        #[allow(clippy::too_many_arguments)]
        #[pyo3(signature = (condition_names, *, run_period=None, runs=None, run_min=None, run_max=None, filters=None))]
        fn fetch(
            &self,
            py: Python<'_>,
            condition_names: Bound<'_, PyAny>,
            run_period: Option<Bound<'_, PyAny>>,
            runs: Option<Vec<RunNumber>>,
            run_min: Option<RunNumber>,
            run_max: Option<RunNumber>,
            filters: Option<Py<PyAny>>,
        ) -> PyResult<Py<PyDict>> {
            let names = condition_names.extract::<Vec<String>>().map_err(|_| {
                PyRuntimeError::new_err("condition_names must be a sequence of strings")
            })?;
            let context = build_context(py, run_period, runs, run_min, run_max, filters)?;
            let data = self.0.fetch(names, &context).map_err(py_rcdb_error)?;
            let runs = PyDict::new(py);
            for (run, values) in data {
                let conditions = PyDict::new(py);
                for (name, value) in values {
                    conditions.set_item(name, value_to_py(py, &value)?)?;
                }
                runs.set_item(run, conditions)?;
            }
            Ok(runs.unbind())
        }

        #[pyo3(signature = (*, run_period=None, runs=None, run_min=None, run_max=None, filters=None))]
        fn fetch_runs(
            &self,
            py: Python<'_>,
            run_period: Option<Bound<'_, PyAny>>,
            runs: Option<Vec<RunNumber>>,
            run_min: Option<RunNumber>,
            run_max: Option<RunNumber>,
            filters: Option<Py<PyAny>>,
        ) -> PyResult<Vec<RunNumber>> {
            let context = build_context(py, run_period, runs, run_min, run_max, filters)?;
            self.0.fetch_runs(&context).map_err(py_rcdb_error)
        }
    }

    #[pyclass(name = "IntCondition", module = "gluex.rcdb", skip_from_py_object)]
    #[derive(Clone)]
    pub struct PyIntCondition(conditions::IntField);

    #[pymethods]
    impl PyIntCondition {
        fn eq(&self, value: i64) -> PyExpr {
            PyExpr(self.0.clone().eq(value))
        }

        fn ne(&self, value: i64) -> PyExpr {
            PyExpr(self.0.clone().ne(value))
        }

        fn gt(&self, value: i64) -> PyExpr {
            PyExpr(self.0.clone().gt(value))
        }

        fn ge(&self, value: i64) -> PyExpr {
            PyExpr(self.0.clone().ge(value))
        }

        fn lt(&self, value: i64) -> PyExpr {
            PyExpr(self.0.clone().lt(value))
        }

        fn le(&self, value: i64) -> PyExpr {
            PyExpr(self.0.clone().le(value))
        }
    }

    #[pyclass(name = "FloatCondition", module = "gluex.rcdb", skip_from_py_object)]
    #[derive(Clone)]
    pub struct PyFloatCondition(conditions::FloatField);

    #[pymethods]
    impl PyFloatCondition {
        fn eq(&self, value: f64) -> PyExpr {
            PyExpr(self.0.clone().eq(value))
        }

        fn gt(&self, value: f64) -> PyExpr {
            PyExpr(self.0.clone().gt(value))
        }

        fn ge(&self, value: f64) -> PyExpr {
            PyExpr(self.0.clone().ge(value))
        }

        fn lt(&self, value: f64) -> PyExpr {
            PyExpr(self.0.clone().lt(value))
        }

        fn le(&self, value: f64) -> PyExpr {
            PyExpr(self.0.clone().le(value))
        }
    }

    #[pyclass(name = "StringCondition", module = "gluex.rcdb", skip_from_py_object)]
    #[derive(Clone)]
    pub struct PyStringCondition(conditions::StringField);

    #[pymethods]
    impl PyStringCondition {
        fn eq(&self, value: &str) -> PyExpr {
            PyExpr(self.0.clone().eq(value))
        }

        fn ne(&self, value: &str) -> PyExpr {
            PyExpr(self.0.clone().ne(value))
        }

        fn isin(&self, values: Vec<String>) -> PyExpr {
            PyExpr(self.0.clone().isin(values))
        }

        fn contains(&self, value: &str) -> PyExpr {
            PyExpr(self.0.clone().contains(value))
        }
    }

    #[pyclass(name = "BoolCondition", module = "gluex.rcdb", skip_from_py_object)]
    #[derive(Clone)]
    pub struct PyBoolCondition(conditions::BoolField);

    #[pymethods]
    impl PyBoolCondition {
        fn is_true(&self) -> PyExpr {
            PyExpr(self.0.clone().is_true())
        }

        fn is_false(&self) -> PyExpr {
            PyExpr(self.0.clone().is_false())
        }

        fn exists(&self) -> PyExpr {
            PyExpr(self.0.clone().exists())
        }
    }

    #[pyclass(name = "TimeCondition", module = "gluex.rcdb", skip_from_py_object)]
    #[derive(Clone)]
    pub struct PyTimeCondition(conditions::TimeField);

    #[pymethods]
    impl PyTimeCondition {
        fn eq(&self, value: DateTime<Utc>) -> PyExpr {
            PyExpr(self.0.clone().eq(value))
        }

        fn gt(&self, value: DateTime<Utc>) -> PyExpr {
            PyExpr(self.0.clone().gt(value))
        }

        fn ge(&self, value: DateTime<Utc>) -> PyExpr {
            PyExpr(self.0.clone().ge(value))
        }

        fn lt(&self, value: DateTime<Utc>) -> PyExpr {
            PyExpr(self.0.clone().lt(value))
        }

        fn le(&self, value: DateTime<Utc>) -> PyExpr {
            PyExpr(self.0.clone().le(value))
        }
    }

    #[pyfunction]
    fn int_cond(name: &str) -> PyIntCondition {
        PyIntCondition(conditions::int_cond(name))
    }

    #[pyfunction]
    fn float_cond(name: &str) -> PyFloatCondition {
        PyFloatCondition(conditions::float_cond(name))
    }

    #[pyfunction]
    fn string_cond(name: &str) -> PyStringCondition {
        PyStringCondition(conditions::string_cond(name))
    }

    #[pyfunction]
    fn bool_cond(name: &str) -> PyBoolCondition {
        PyBoolCondition(conditions::bool_cond(name))
    }

    #[pyfunction]
    fn time_cond(name: &str) -> PyTimeCondition {
        PyTimeCondition(conditions::time_cond(name))
    }

    #[pyfunction(signature = (*exprs))]
    fn all(exprs: &Bound<'_, PyTuple>) -> PyResult<PyExpr> {
        Ok(PyExpr(conditions::all(tuple_to_exprs(exprs)?)))
    }

    #[pyfunction(signature = (*exprs))]
    fn any(exprs: &Bound<'_, PyTuple>) -> PyResult<PyExpr> {
        Ok(PyExpr(conditions::any(tuple_to_exprs(exprs)?)))
    }

    #[pyclass(name = "Aliases", module = "gluex.rcdb", skip_from_py_object)]
    #[derive(Clone)]
    pub struct PyAliases;

    #[pymethods]
    impl PyAliases {
        #[getter]
        fn is_production(&self) -> PyExpr {
            PyExpr(conditions::aliases::is_production())
        }

        #[getter]
        fn is_2018production(&self) -> PyExpr {
            PyExpr(conditions::aliases::is_2018production())
        }

        #[getter]
        fn is_primex_production(&self) -> PyExpr {
            PyExpr(conditions::aliases::is_primex_production())
        }

        #[getter]
        fn is_dirc_production(&self) -> PyExpr {
            PyExpr(conditions::aliases::is_dirc_production())
        }

        #[getter]
        fn is_src_production(&self) -> PyExpr {
            PyExpr(conditions::aliases::is_src_production())
        }

        #[getter]
        fn is_cpp_production(&self) -> PyExpr {
            PyExpr(conditions::aliases::is_cpp_production())
        }

        #[getter]
        fn is_production_long(&self) -> PyExpr {
            PyExpr(conditions::aliases::is_production_long())
        }

        #[getter]
        fn is_cosmic(&self) -> PyExpr {
            PyExpr(conditions::aliases::is_cosmic())
        }

        #[getter]
        fn is_empty_target(&self) -> PyExpr {
            PyExpr(conditions::aliases::is_empty_target())
        }

        #[getter]
        fn is_amorph_radiator(&self) -> PyExpr {
            PyExpr(conditions::aliases::is_amorph_radiator())
        }

        #[getter]
        fn is_coherent_beam(&self) -> PyExpr {
            PyExpr(conditions::aliases::is_coherent_beam())
        }

        #[getter]
        fn is_field_off(&self) -> PyExpr {
            PyExpr(conditions::aliases::is_field_off())
        }

        #[getter]
        fn is_field_on(&self) -> PyExpr {
            PyExpr(conditions::aliases::is_field_on())
        }

        #[getter]
        fn status_calibration(&self) -> PyExpr {
            PyExpr(conditions::aliases::status_calibration())
        }

        #[getter]
        fn status_approved_long(&self) -> PyExpr {
            PyExpr(conditions::aliases::status_approved_long())
        }

        #[getter]
        fn status_approved(&self) -> PyExpr {
            PyExpr(conditions::aliases::status_approved())
        }

        #[getter]
        fn status_unchecked(&self) -> PyExpr {
            PyExpr(conditions::aliases::status_unchecked())
        }

        #[getter]
        fn status_reject(&self) -> PyExpr {
            PyExpr(conditions::aliases::status_reject())
        }

        fn approved_production(&self, run_period: Bound<'_, PyAny>) -> PyResult<PyExpr> {
            let run_period = parse_run_period_object(&run_period)?;
            conditions::aliases::approved_production(run_period)
                .map(PyExpr)
                .map_err(py_rcdb_error)
        }
    }

    #[pymodule_init]
    fn init(module: &Bound<'_, PyModule>) -> PyResult<()> {
        module.add("aliases", Py::new(module.py(), PyAliases)?)
    }

    fn build_context(
        py: Python<'_>,
        run_period: Option<Bound<'_, PyAny>>,
        runs: Option<Vec<RunNumber>>,
        run_min: Option<RunNumber>,
        run_max: Option<RunNumber>,
        filters: Option<Py<PyAny>>,
    ) -> PyResult<RCDBContext> {
        let selection_count = usize::from(run_period.is_some())
            + usize::from(runs.is_some())
            + usize::from(run_min.is_some() || run_max.is_some());
        if selection_count > 1 {
            return Err(PyRuntimeError::new_err(
                "run_period, runs, and run_min/run_max arguments are mutually exclusive",
            ));
        }

        let mut context = RCDBContext::default();
        if let Some(run_period) = run_period {
            context = context.with_run_period(parse_run_period_object(&run_period)?);
        } else if let Some(runs) = runs {
            context = context.with_runs(runs);
        } else if run_min.is_some() || run_max.is_some() {
            context = context.with_run_range(
                run_min.unwrap_or(MIN_RUN_NUMBER)..=run_max.unwrap_or(MAX_RUN_NUMBER),
            );
        }
        if let Some(filters) = filters {
            context = context.filter(exprs_from_object(filters.into_bound(py))?);
        }
        Ok(context)
    }

    fn tuple_to_exprs(exprs: &Bound<'_, PyTuple>) -> PyResult<Vec<Expr>> {
        exprs.iter().map(|expr| extract_expr(&expr)).collect()
    }

    fn exprs_from_object(object: Bound<'_, PyAny>) -> PyResult<Vec<Expr>> {
        if object.is_instance_of::<PyExpr>() {
            return Ok(vec![extract_expr(&object)?]);
        }
        if object.is_instance_of::<PyTuple>() {
            return tuple_to_exprs(object.cast::<PyTuple>()?);
        }
        if object.is_instance_of::<PyList>() {
            return object
                .cast::<PyList>()?
                .iter()
                .map(|expr| extract_expr(&expr))
                .collect();
        }
        Err(PyRuntimeError::new_err(
            "filters must be an Expr or sequence of Expr objects",
        ))
    }

    fn extract_expr(object: &Bound<'_, PyAny>) -> PyResult<Expr> {
        let expr: Py<PyExpr> = object.extract()?;
        Ok(expr.borrow(object.py()).0.clone())
    }

    fn value_to_py(py: Python<'_>, value: &Value) -> PyResult<Py<PyAny>> {
        Ok(match value.value_type() {
            ValueType::String | ValueType::Json | ValueType::Blob => value
                .as_string()
                .map(|value| PyString::new(py, value).into_any().unbind())
                .unwrap_or_else(|| py.None()),
            ValueType::Int => value
                .as_int()
                .map(|value| PyInt::new(py, value).into_any().unbind())
                .unwrap_or_else(|| py.None()),
            ValueType::Float => value
                .as_float()
                .map(|value| PyFloat::new(py, value).into_any().unbind())
                .unwrap_or_else(|| py.None()),
            ValueType::Bool => match value.as_bool() {
                Some(value) => value.into_pyobject(py)?.to_owned().into_any().unbind(),
                None => py.None(),
            },
            ValueType::Time => value
                .as_time()
                .map(|value| PyString::new(py, &value.to_rfc3339()).into_any().unbind())
                .unwrap_or_else(|| py.None()),
        })
    }
}
