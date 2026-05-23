#[pyo3::pymodule(submodule)]
pub(crate) mod ccdb {
    use std::{collections::BTreeMap, env};

    use ::gluex_ccdb::{
        CCDB, CCDBContext, CCDBError, Column, ColumnMeta, ColumnType, Data, TypeTableMeta, Value,
        database::{DirectoryHandle, TypeTableHandle},
    };
    use chrono::{DateTime, Utc};
    use gluex_core::{
        GlueXCoreError, RESTVersion, RESTVersionSelection, RunNumber, RunPeriod,
        parsers::parse_timestamp, utils::resolve_path,
    };
    use pyo3::{
        conversion::IntoPyObject,
        exceptions::{PyIndexError, PyKeyError, PyRuntimeError, PyTypeError},
        prelude::*,
        types::{PyDict, PyFloat, PyInt, PyString},
    };

    fn py_ccdb_error(err: CCDBError) -> PyErr {
        PyRuntimeError::new_err(err.to_string())
    }

    fn resolve_connection_path(path: Option<String>) -> PyResult<String> {
        let raw_path = match path {
            Some(value) if !value.is_empty() => value,
            _ => env::var("CCDB_CONNECTION").map_err(|_| {
                PyRuntimeError::new_err("CCDB_CONNECTION is not set and no path was provided")
            })?,
        };
        resolve_path(raw_path)
            .map(|path| path.to_string_lossy().to_string())
            .map_err(|err| PyRuntimeError::new_err(err.to_string()))
    }

    #[pyclass(name = "ColumnType", module = "gluex.ccdb", skip_from_py_object)]
    #[derive(Clone)]
    pub struct PyColumnType(ColumnType);

    #[pymethods]
    impl PyColumnType {
        #[getter]
        fn name(&self) -> &'static str {
            self.0.as_str()
        }

        fn __repr__(&self) -> String {
            format!("ColumnType('{}')", self.0.as_str())
        }
    }

    impl From<ColumnType> for PyColumnType {
        fn from(kind: ColumnType) -> Self {
            Self(kind)
        }
    }

    #[pyclass(name = "ColumnMeta", module = "gluex.ccdb", skip_from_py_object)]
    #[derive(Clone)]
    pub struct PyColumnMeta(ColumnMeta);

    #[pymethods]
    impl PyColumnMeta {
        #[getter]
        fn id(&self) -> i64 {
            self.0.id()
        }

        #[getter]
        fn name(&self) -> &str {
            self.0.name()
        }

        #[getter]
        fn column_type(&self) -> PyColumnType {
            self.0.column_type().into()
        }

        #[getter]
        fn order(&self) -> i64 {
            self.0.order()
        }

        #[getter]
        fn comment(&self) -> &str {
            self.0.comment()
        }

        fn __repr__(&self) -> String {
            format!(
                "ColumnMeta(name='{}', type='{}', order={})",
                self.0.name(),
                self.0.column_type().as_str(),
                self.0.order()
            )
        }
    }

    #[pyclass(name = "TypeTableMeta", module = "gluex.ccdb", skip_from_py_object)]
    #[derive(Clone)]
    pub struct PyTypeTableMeta(TypeTableMeta);

    #[pymethods]
    impl PyTypeTableMeta {
        #[getter]
        fn id(&self) -> i64 {
            self.0.id()
        }

        #[getter]
        fn name(&self) -> &str {
            self.0.name()
        }

        #[getter]
        fn n_rows(&self) -> i64 {
            self.0.n_rows()
        }

        #[getter]
        fn n_columns(&self) -> i64 {
            self.0.n_columns()
        }

        #[getter]
        fn comment(&self) -> &str {
            self.0.comment()
        }

        fn __repr__(&self) -> String {
            format!(
                "TypeTableMeta(name='{}', id={})",
                self.0.name(),
                self.0.id()
            )
        }
    }

    #[pyclass(name = "Column", module = "gluex.ccdb", unsendable)]
    pub struct PyColumn(Column, String, ColumnType);

    #[pymethods]
    impl PyColumn {
        #[getter]
        fn name(&self) -> &str {
            &self.1
        }

        #[getter]
        fn column_type(&self) -> PyColumnType {
            PyColumnType(self.2)
        }

        fn values(&self, py: Python<'_>) -> PyResult<Vec<Py<PyAny>>> {
            column_values_to_py(py, &self.0)
        }

        fn row(&self, py: Python<'_>, row: usize) -> PyResult<Py<PyAny>> {
            if row >= self.0.len() {
                return Err(PyIndexError::new_err("row index out of range"));
            }
            value_to_py(py, self.0.row(row))
        }

        fn __len__(&self) -> usize {
            self.0.len()
        }

        fn __getitem__(&self, py: Python<'_>, row: usize) -> PyResult<Py<PyAny>> {
            self.row(py, row)
        }
    }

    #[pyclass(name = "Data", module = "gluex.ccdb", unsendable)]
    pub struct PyData(Data);

    #[pymethods]
    impl PyData {
        #[getter]
        fn n_rows(&self) -> usize {
            self.0.n_rows()
        }

        #[getter]
        fn n_columns(&self) -> usize {
            self.0.n_columns()
        }

        #[getter]
        fn column_names(&self) -> Vec<String> {
            self.0.column_names().to_vec()
        }

        #[getter]
        fn column_types(&self) -> Vec<PyColumnType> {
            self.0
                .column_types()
                .iter()
                .copied()
                .map(PyColumnType::from)
                .collect()
        }

        fn row(&self, row: usize) -> PyResult<PyRowView> {
            self.0.row(row).map_err(py_ccdb_error)?;
            Ok(PyRowView(self.0.clone(), row))
        }

        fn rows(&self) -> Vec<PyRowView> {
            (0..self.0.n_rows())
                .map(|row| PyRowView(self.0.clone(), row))
                .collect()
        }

        fn column(&self, column: Bound<'_, PyAny>) -> PyResult<PyColumn> {
            let column = parse_column_index(&self.0, column)?;
            Ok(PyColumn(
                self.0
                    .column_clone(column)
                    .ok_or_else(|| PyRuntimeError::new_err("column index out of range"))?,
                self.0.column_names()[column].clone(),
                self.0.column_types()[column],
            ))
        }

        fn value(
            &self,
            py: Python<'_>,
            column: Bound<'_, PyAny>,
            row: usize,
        ) -> PyResult<Py<PyAny>> {
            let Some(column) = parse_optional_column_index(&self.0, column)? else {
                return Ok(py.None());
            };
            match self.0.value(column, row) {
                Some(value) => value_to_py(py, value),
                None => Ok(py.None()),
            }
        }

        fn as_dict(&self, py: Python<'_>) -> PyResult<Py<PyDict>> {
            let dict = PyDict::new(py);
            for (name, _, column) in self.0.iter_columns() {
                dict.set_item(name, column_values_to_py(py, column)?)?;
            }
            Ok(dict.unbind())
        }

        fn __len__(&self) -> usize {
            self.0.n_rows()
        }

        fn __getitem__(&self, py: Python<'_>, key: Bound<'_, PyAny>) -> PyResult<Py<PyAny>> {
            if let Ok(row) = key.extract::<usize>() {
                if row >= self.0.n_rows() {
                    return Err(PyIndexError::new_err("row index out of range"));
                }
                return Ok(Py::new(py, PyRowView(self.0.clone(), row))?.into_any());
            }
            if let Ok(name) = key.extract::<String>() {
                let column = self
                    .0
                    .column_names()
                    .iter()
                    .position(|candidate| candidate == &name)
                    .ok_or_else(|| PyKeyError::new_err(name.clone()))?;
                return Ok(Py::new(
                    py,
                    PyColumn(
                        self.0.column_clone(column).expect("indexed column exists"),
                        name,
                        self.0.column_types()[column],
                    ),
                )?
                .into_any());
            }
            Err(PyTypeError::new_err(
                "data indices must be a row number or column name",
            ))
        }

        fn __repr__(&self) -> String {
            let columns: Vec<String> = self
                .0
                .column_names()
                .iter()
                .zip(self.0.column_types())
                .map(|(name, kind)| format!("{}:{}", name, kind.as_str()))
                .collect();
            format!(
                "Data(n_rows={}, n_columns={}, columns=[{}])",
                self.0.n_rows(),
                self.0.n_columns(),
                columns.join(", ")
            )
        }
    }

    // The Rust row view borrows its Data. Python needs an owned backing value.
    #[pyclass(name = "RowView", module = "gluex.ccdb", unsendable)]
    pub struct PyRowView(Data, usize);

    #[pymethods]
    impl PyRowView {
        #[getter]
        fn n_columns(&self) -> usize {
            self.0.n_columns()
        }

        #[getter]
        fn column_types(&self) -> Vec<PyColumnType> {
            self.0
                .column_types()
                .iter()
                .copied()
                .map(PyColumnType::from)
                .collect()
        }

        fn value(&self, py: Python<'_>, column: Bound<'_, PyAny>) -> PyResult<Py<PyAny>> {
            let Some(column) = parse_optional_column_index(&self.0, column)? else {
                return Ok(py.None());
            };
            match self.0.value(column, self.1) {
                Some(value) => value_to_py(py, value),
                None => Ok(py.None()),
            }
        }

        fn columns(&self, py: Python<'_>) -> PyResult<Vec<(String, PyColumnType, Py<PyAny>)>> {
            let row = self.0.row(self.1).map_err(py_ccdb_error)?;
            row.iter_columns()
                .map(|(name, kind, value)| {
                    Ok((
                        name.to_string(),
                        PyColumnType::from(kind),
                        value_to_py(py, value)?,
                    ))
                })
                .collect()
        }

        fn as_dict(&self, py: Python<'_>) -> PyResult<Py<PyDict>> {
            let row = self.0.row(self.1).map_err(py_ccdb_error)?;
            let dict = PyDict::new(py);
            for (name, _, value) in row.iter_columns() {
                dict.set_item(name, value_to_py(py, value)?)?;
            }
            Ok(dict.unbind())
        }

        fn __getitem__(&self, py: Python<'_>, column: Bound<'_, PyAny>) -> PyResult<Py<PyAny>> {
            let column = parse_strict_column_index(&self.0, column)?;
            value_to_py(
                py,
                self.0
                    .value(column, self.1)
                    .expect("row and column were validated"),
            )
        }
    }

    #[pyclass(name = "TypeTableHandle", module = "gluex.ccdb", unsendable)]
    pub struct PyTypeTableHandle(TypeTableHandle);

    #[pymethods]
    impl PyTypeTableHandle {
        #[getter]
        fn name(&self) -> &str {
            self.0.name()
        }

        #[getter]
        fn id(&self) -> i64 {
            self.0.id()
        }

        #[getter]
        fn meta(&self) -> PyTypeTableMeta {
            PyTypeTableMeta(self.0.meta().clone())
        }

        fn full_path(&self) -> String {
            self.0.full_path()
        }

        fn columns(&self) -> PyResult<Vec<PyColumnMeta>> {
            Ok(self
                .0
                .columns()
                .map_err(py_ccdb_error)?
                .into_iter()
                .map(PyColumnMeta)
                .collect())
        }

        #[pyo3(signature = (*, runs=None, variation=None, timestamp=None))]
        fn fetch(
            &self,
            runs: Option<Vec<RunNumber>>,
            variation: Option<String>,
            timestamp: Option<Bound<'_, PyAny>>,
        ) -> PyResult<BTreeMap<RunNumber, PyData>> {
            let context = build_context(runs, variation, timestamp)?;
            Ok(self
                .0
                .fetch(&context)
                .map_err(py_ccdb_error)?
                .into_iter()
                .map(|(run, data)| (run, PyData(data)))
                .collect())
        }

        #[pyo3(signature = (*, run_period, rest_version=None, variation=None, timestamp=None))]
        fn fetch_run_period(
            &self,
            run_period: &str,
            rest_version: Option<Bound<'_, PyAny>>,
            variation: Option<String>,
            timestamp: Option<Bound<'_, PyAny>>,
        ) -> PyResult<BTreeMap<RunNumber, PyData>> {
            let context = build_run_period_context(run_period, rest_version, variation, timestamp)?;
            Ok(self
                .0
                .fetch(&context)
                .map_err(py_ccdb_error)?
                .into_iter()
                .map(|(run, data)| (run, PyData(data)))
                .collect())
        }
    }

    #[pyclass(name = "DirectoryHandle", module = "gluex.ccdb", unsendable)]
    pub struct PyDirectoryHandle(DirectoryHandle);

    #[pymethods]
    impl PyDirectoryHandle {
        fn full_path(&self) -> String {
            self.0.full_path()
        }

        fn parent(&self) -> Option<Self> {
            self.0.parent().map(Self)
        }

        fn dirs(&self) -> Vec<Self> {
            self.0.dirs().into_iter().map(Self).collect()
        }

        fn dir(&self, name: &str) -> PyResult<Self> {
            self.0.dir(name).map(Self).map_err(py_ccdb_error)
        }

        fn tables(&self) -> Vec<PyTypeTableHandle> {
            self.0.tables().into_iter().map(PyTypeTableHandle).collect()
        }

        fn table(&self, name: &str) -> PyResult<PyTypeTableHandle> {
            self.0
                .table(name)
                .map(PyTypeTableHandle)
                .map_err(py_ccdb_error)
        }
    }

    #[pyclass(name = "CCDB", module = "gluex.ccdb", unsendable)]
    pub struct PyCCDB(CCDB);

    #[pymethods]
    impl PyCCDB {
        #[new]
        #[pyo3(signature = (path=None))]
        fn new(path: Option<String>) -> PyResult<Self> {
            let path = resolve_connection_path(path)?;
            CCDB::open(path).map(Self).map_err(py_ccdb_error)
        }

        #[getter]
        fn connection_path(&self) -> &str {
            self.0.connection_path()
        }

        fn root(&self) -> PyDirectoryHandle {
            PyDirectoryHandle(self.0.root())
        }

        fn dir(&self, path: &str) -> PyResult<PyDirectoryHandle> {
            self.0
                .dir(path)
                .map(PyDirectoryHandle)
                .map_err(py_ccdb_error)
        }

        fn table(&self, path: &str) -> PyResult<PyTypeTableHandle> {
            self.0
                .table(path)
                .map(PyTypeTableHandle)
                .map_err(py_ccdb_error)
        }

        #[pyo3(signature = (path, *, runs=None, variation=None, timestamp=None))]
        fn fetch(
            &self,
            path: &str,
            runs: Option<Vec<RunNumber>>,
            variation: Option<String>,
            timestamp: Option<Bound<'_, PyAny>>,
        ) -> PyResult<BTreeMap<RunNumber, PyData>> {
            let context = build_context(runs, variation, timestamp)?;
            Ok(self
                .0
                .fetch(path, &context)
                .map_err(py_ccdb_error)?
                .into_iter()
                .map(|(run, data)| (run, PyData(data)))
                .collect())
        }

        #[pyo3(signature = (path, *, run_period, rest_version=None, variation=None, timestamp=None))]
        fn fetch_run_period(
            &self,
            path: &str,
            run_period: &str,
            rest_version: Option<Bound<'_, PyAny>>,
            variation: Option<String>,
            timestamp: Option<Bound<'_, PyAny>>,
        ) -> PyResult<BTreeMap<RunNumber, PyData>> {
            let context = build_run_period_context(run_period, rest_version, variation, timestamp)?;
            Ok(self
                .0
                .fetch(path, &context)
                .map_err(py_ccdb_error)?
                .into_iter()
                .map(|(run, data)| (run, PyData(data)))
                .collect())
        }
    }

    fn value_to_py(py: Python<'_>, value: Value<'_>) -> PyResult<Py<PyAny>> {
        Ok(match value {
            Value::Int(value) => PyInt::new(py, *value).unbind().into(),
            Value::UInt(value) => PyInt::new(py, *value).unbind().into(),
            Value::Long(value) => PyInt::new(py, *value).unbind().into(),
            Value::ULong(value) => PyInt::new(py, *value).unbind().into(),
            Value::Double(value) => PyFloat::new(py, *value).unbind().into(),
            Value::Bool(value) => {
                let object = (*value).into_pyobject(py)?;
                <pyo3::Bound<'_, _> as Clone>::clone(&object)
                    .into_any()
                    .unbind()
            }
            Value::String(value) => PyString::new(py, value).unbind().into(),
        })
    }

    fn column_values_to_py(py: Python<'_>, column: &Column) -> PyResult<Vec<Py<PyAny>>> {
        (0..column.len())
            .map(|row| value_to_py(py, column.row(row)))
            .collect()
    }

    fn parse_timestamp_object(
        timestamp: Option<Bound<'_, PyAny>>,
    ) -> PyResult<Option<DateTime<Utc>>> {
        let Some(timestamp) = timestamp else {
            return Ok(None);
        };
        if let Ok(timestamp) = timestamp.extract::<DateTime<Utc>>() {
            return Ok(Some(timestamp));
        }
        if let Ok(timestamp) = timestamp.extract::<String>() {
            return parse_timestamp(&timestamp)
                .map(Some)
                .map_err(|err| PyRuntimeError::new_err(err.to_string()));
        }
        Err(PyRuntimeError::new_err("timestamp must be str or datetime"))
    }

    fn parse_rest_version(
        run_period: RunPeriod,
        rest_version: Option<Bound<'_, PyAny>>,
    ) -> PyResult<RESTVersionSelection> {
        let Some(rest_version) = rest_version else {
            return Ok(RESTVersionSelection::Current);
        };
        if let Ok(version) = rest_version.extract::<RESTVersion>() {
            return RESTVersionSelection::try_new(run_period, version)
                .map_err(|err| PyRuntimeError::new_err(err.to_string()));
        }
        if let Ok(timestamp) = rest_version.extract::<DateTime<Utc>>() {
            return Ok(RESTVersionSelection::from_timestamp(timestamp));
        }
        Err(PyRuntimeError::new_err(
            "rest_version must be int, datetime, or None",
        ))
    }

    fn parse_column_index(data: &Data, column: Bound<'_, PyAny>) -> PyResult<usize> {
        if let Ok(index) = column.extract::<usize>() {
            if index < data.n_columns() {
                return Ok(index);
            }
            return Err(PyRuntimeError::new_err("column index out of range"));
        }
        if let Ok(name) = column.extract::<String>() {
            return data
                .column_names()
                .iter()
                .position(|candidate| candidate == &name)
                .ok_or_else(|| PyRuntimeError::new_err("column name not found"));
        }
        Err(PyRuntimeError::new_err("column must be int or str"))
    }

    fn parse_strict_column_index(data: &Data, column: Bound<'_, PyAny>) -> PyResult<usize> {
        if let Ok(index) = column.extract::<usize>() {
            if index < data.n_columns() {
                return Ok(index);
            }
            return Err(PyIndexError::new_err("column index out of range"));
        }
        if let Ok(name) = column.extract::<String>() {
            return data
                .column_names()
                .iter()
                .position(|candidate| candidate == &name)
                .ok_or_else(|| PyKeyError::new_err(name));
        }
        Err(PyTypeError::new_err("column indices must be int or str"))
    }

    fn parse_optional_column_index(
        data: &Data,
        column: Bound<'_, PyAny>,
    ) -> PyResult<Option<usize>> {
        if let Ok(index) = column.extract::<usize>() {
            return Ok((index < data.n_columns()).then_some(index));
        }
        if let Ok(name) = column.extract::<String>() {
            return Ok(data
                .column_names()
                .iter()
                .position(|candidate| candidate == &name));
        }
        Err(PyTypeError::new_err("column indices must be int or str"))
    }

    fn build_context(
        runs: Option<Vec<RunNumber>>,
        variation: Option<String>,
        timestamp: Option<Bound<'_, PyAny>>,
    ) -> PyResult<CCDBContext> {
        let mut context = CCDBContext::default();
        if let Some(runs) = runs {
            context.runs = runs;
        }
        if let Some(variation) = variation {
            context.variation = variation;
        }
        if let Some(timestamp) = parse_timestamp_object(timestamp)? {
            context.timestamp = timestamp;
        }
        Ok(context)
    }

    fn build_run_period_context(
        run_period: &str,
        rest_version: Option<Bound<'_, PyAny>>,
        variation: Option<String>,
        timestamp: Option<Bound<'_, PyAny>>,
    ) -> PyResult<CCDBContext> {
        let run_period = run_period
            .parse()
            .map_err(|err: GlueXCoreError| py_ccdb_error(CCDBError::GlueXCoreError(err)))?;
        let rest_version = parse_rest_version(run_period, rest_version)?;
        let mut context = CCDBContext::default()
            .with_run_period(run_period, rest_version)
            .map_err(py_ccdb_error)?;
        if let Some(variation) = variation {
            context.variation = variation;
        }
        if let Some(timestamp) = parse_timestamp_object(timestamp)? {
            context.timestamp = timestamp;
        }
        Ok(context)
    }
}
