use ::gluex_ccdb::{
    context::Context,
    data::{self, Data, Value},
    database::{DirectoryHandle, TypeTableHandle, CCDB},
    models::{ColumnMeta, ColumnType, TypeTableMeta},
    CCDBError,
};
use chrono::{DateTime, Utc};
use gluex_core::{parsers::parse_timestamp, run_periods::RunPeriodError, RunNumber};
use pyo3::{
    conversion::IntoPyObject,
    exceptions::PyRuntimeError,
    prelude::*,
    types::{PyFloat, PyInt, PyModule, PyString},
};
use std::{collections::BTreeMap, sync::Arc};

fn py_ccdb_error(err: CCDBError) -> PyErr {
    PyRuntimeError::new_err(err.to_string())
}

/// Column type describing how a CCDB column is stored.
///
/// Attributes
/// ----------
/// name : str
///     Short lowercase identifier for the storage type (e.g. "int").
#[pyclass(name = "ColumnType", module = "gluex_ccdb")]
#[derive(Clone)]
pub struct PyColumnType {
    kind: ColumnType,
}

#[pymethods]
impl PyColumnType {
    /// str: Short lowercase identifier for the storage type.
    #[getter]
    pub fn name(&self) -> &'static str {
        self.kind.as_str()
    }
    fn __repr__(&self) -> String {
        format!("ColumnType('{}')", self.kind.as_str())
    }
}

impl From<ColumnType> for PyColumnType {
    fn from(kind: ColumnType) -> Self {
        Self { kind }
    }
}

#[allow(missing_docs)]
#[pyclass(name = "ColumnMeta", module = "gluex_ccdb")]
#[derive(Clone)]
pub struct PyColumnMeta {
    inner: ColumnMeta,
}

#[pymethods]
impl PyColumnMeta {
    #[getter]
    fn id(&self) -> i64 {
        self.inner.id()
    }
    #[getter]
    fn name(&self) -> &str {
        self.inner.name()
    }
    #[getter]
    fn column_type(&self) -> PyColumnType {
        self.inner.column_type().into()
    }
    #[getter]
    fn order(&self) -> i64 {
        self.inner.order()
    }
    #[getter]
    fn comment(&self) -> &str {
        self.inner.comment()
    }

    fn __repr__(&self) -> String {
        format!(
            "ColumnMeta(name='{}', type='{}', order={})",
            self.inner.name(),
            self.inner.column_type().as_str(),
            self.inner.order()
        )
    }
    fn __str__(&self) -> String {
        self.__repr__()
    }
}

/// Single column of a fetched CCDB table.
///
/// Attributes
/// ----------
/// name : str
///     Column name as recorded in CCDB metadata.
/// column_type : ColumnType
///     Storage type of the column values.
#[pyclass(name = "Column", module = "gluex_ccdb", unsendable)]
pub struct PyColumn {
    name: String,
    column_type: ColumnType,
    column: Arc<data::Column>,
}

#[pymethods]
impl PyColumn {
    /// str: Column name as stored in CCDB metadata.
    #[getter]
    pub fn name(&self) -> String {
        self.name.clone()
    }
    /// ColumnType: Declared storage type for the column.
    #[getter]
    pub fn column_type(&self) -> PyColumnType {
        PyColumnType::from(self.column_type)
    }

    /// row(self, row)
    ///
    /// Parameters
    /// ----------
    /// row : int
    ///     Zero-based row index.
    ///
    /// Returns
    /// -------
    /// object
    ///     Value converted to a Python scalar.
    ///
    /// Raises
    /// ------
    /// RuntimeError
    ///     If the requested row is out of range.
    pub fn row(&self, py: Python<'_>, row: usize) -> PyResult<Py<PyAny>> {
        if row >= self.column.len() {
            return Err(PyRuntimeError::new_err("row index out of range"));
        }
        value_to_py(py, self.column.row(row))
    }

    /// values(self)
    ///
    /// Returns
    /// -------
    /// list[object]
    ///     All values converted to Python scalars in row order.
    pub fn values(&self, py: Python<'_>) -> PyResult<Vec<Py<PyAny>>> {
        let vals: Vec<Py<PyAny>> = match self.column.as_ref() {
            data::Column::Int(v) => v
                .iter()
                .map(|x| PyInt::new(py, *x).unbind().into())
                .collect(),
            data::Column::UInt(v) => v
                .iter()
                .map(|x| PyInt::new(py, *x).unbind().into())
                .collect(),
            data::Column::Long(v) => v
                .iter()
                .map(|x| PyInt::new(py, *x).unbind().into())
                .collect(),
            data::Column::ULong(v) => v
                .iter()
                .map(|x| PyInt::new(py, *x).unbind().into())
                .collect(),
            data::Column::Double(v) => v
                .iter()
                .map(|x| PyFloat::new(py, *x).unbind().into())
                .collect(),
            data::Column::Bool(v) => v
                .iter()
                .map(|x| {
                    let obj = (*x).into_pyobject(py).unwrap();
                    <pyo3::Bound<'_, _> as Clone>::clone(&obj)
                        .into_any()
                        .unbind()
                })
                .collect(),
            data::Column::String(v) => v
                .iter()
                .map(|s| PyString::new(py, s).unbind().into())
                .collect(),
        };
        Ok(vals)
    }

    fn __repr__(&self) -> String {
        format!(
            "Column(name='{}', type='{}')",
            self.name(),
            self.column_type().name()
        )
    }
    fn __str__(&self) -> String {
        self.__repr__()
    }
}

#[allow(missing_docs)]
#[pyclass(name = "TypeTableMeta", module = "gluex_ccdb")]
#[derive(Clone)]
pub struct PyTypeTableMeta {
    inner: TypeTableMeta,
}

#[pymethods]
impl PyTypeTableMeta {
    #[getter]
    fn id(&self) -> i64 {
        self.inner.id()
    }
    #[getter]
    fn name(&self) -> &str {
        self.inner.name()
    }
    #[getter]
    fn n_rows(&self) -> i64 {
        self.inner.n_rows()
    }
    #[getter]
    fn n_columns(&self) -> i64 {
        self.inner.n_columns()
    }
    #[getter]
    fn comment(&self) -> &str {
        self.inner.comment()
    }

    fn __repr__(&self) -> String {
        format!(
            "TypeTableMeta(name='{}', id={})",
            self.inner.name(),
            self.inner.id()
        )
    }
}

/// Column-major dataset returned from CCDB fetch operations.
///
/// Attributes
/// ----------
/// n_rows : int
///     Number of rows in the dataset.
/// n_columns : int
///     Number of columns in the dataset.
/// column_names : list[str]
///     Names for each column in positional order.
/// column_types : list[ColumnType]
///     Storage type for each column in positional order.
#[pyclass(name = "Data", module = "gluex_ccdb", unsendable)]
pub struct PyData {
    inner: Arc<Data>,
}

#[pymethods]
impl PyData {
    /// int: Number of rows in the dataset.
    #[getter]
    pub fn n_rows(&self) -> usize {
        self.inner.n_rows()
    }
    /// int: Number of columns in the dataset.
    #[getter]
    pub fn n_columns(&self) -> usize {
        self.inner.n_columns()
    }
    /// list[str]: Column names in positional order.
    #[getter]
    pub fn column_names(&self) -> Vec<String> {
        self.inner.column_names().to_vec()
    }
    /// list[ColumnType]: Column types in positional order.
    #[getter]
    pub fn column_types(&self) -> Vec<PyColumnType> {
        self.inner
            .column_types()
            .iter()
            .copied()
            .map(PyColumnType::from)
            .collect()
    }

    /// column(self, column)
    ///
    /// Parameters
    /// ----------
    /// column : int | str
    ///     Column index or name.
    ///
    /// Returns
    /// -------
    /// Column
    ///     Column wrapper exposing values and metadata.
    ///
    /// Raises
    /// ------
    /// RuntimeError
    ///     If the column cannot be found.
    pub fn column(&self, column: Bound<'_, PyAny>) -> PyResult<PyColumn> {
        let idx = parse_column_index(&self.inner, column)?;
        let name = self.inner.column_names()[idx].clone();
        let column = self
            .inner
            .column_clone(idx)
            .ok_or_else(|| PyRuntimeError::new_err("column index out of range"))?;
        let column_type = self.inner.column_types()[idx];
        Ok(PyColumn {
            name,
            column_type,
            column: Arc::new(column),
        })
    }

    /// row(self, row)
    ///
    /// Parameters
    /// ----------
    /// row : int
    ///     Zero-based row index.
    ///
    /// Returns
    /// -------
    /// RowView
    ///     Lightweight view over the requested row.
    ///
    /// Raises
    /// ------
    /// RuntimeError
    ///     If the row index is out of range.
    pub fn row(&self, row: usize) -> PyResult<PyRowView> {
        self.inner
            .row(row)
            .map_err(|e| py_ccdb_error(CCDBError::from(e)))?;
        Ok(PyRowView {
            data: Arc::clone(&self.inner),
            row,
        })
    }

    /// rows(self)
    ///
    /// Returns
    /// -------
    /// list[RowView]
    ///     View objects for each row in order.
    pub fn rows(&self) -> PyResult<Vec<PyRowView>> {
        let n_rows = self.inner.n_rows();
        let data = Arc::clone(&self.inner);
        Ok((0..n_rows)
            .map(|row| PyRowView {
                data: Arc::clone(&data),
                row,
            })
            .collect())
    }

    /// value(self, column, row)
    ///
    /// Parameters
    /// ----------
    /// column : int | str
    ///     Column index or name.
    /// row : int
    ///     Zero-based row index.
    ///
    /// Returns
    /// -------
    /// object
    ///     Cell value converted to a Python scalar or `None` if missing.
    pub fn value(
        &self,
        py: Python<'_>,
        column: Bound<'_, PyAny>,
        row: usize,
    ) -> PyResult<Py<PyAny>> {
        let col_idx = parse_column_index(&self.inner, column)?;
        match self.inner.value(col_idx, row) {
            Some(v) => value_to_py(py, v),
            None => Ok(py.None()),
        }
    }

    fn __repr__(&self) -> String {
        let cols: Vec<String> = self
            .inner
            .column_names()
            .iter()
            .zip(self.inner.column_types())
            .map(|(n, t)| format!("{}:{}", n, t.as_str()))
            .collect();
        format!(
            "Data(n_rows={}, n_columns={}, columns=[{}])",
            self.inner.n_rows(),
            self.inner.n_columns(),
            cols.join(", ")
        )
    }
}

/// Lightweight view of a single row in a CCDB result set.
///
/// Attributes
/// ----------
/// n_columns : int
///     Number of columns available in the row.
/// column_types : list[ColumnType]
///     Storage type for each column in the row.
#[pyclass(name = "RowView", module = "gluex_ccdb")]
pub struct PyRowView {
    data: Arc<Data>,
    row: usize,
}

#[pymethods]
impl PyRowView {
    /// int: Number of columns available in this row.
    #[getter]
    pub fn n_columns(&self, _py: Python<'_>) -> usize {
        self.data.n_columns()
    }

    /// list[ColumnType]: Column types for this row in positional order.
    #[getter]
    pub fn column_types(&self, _py: Python<'_>) -> Vec<PyColumnType> {
        self.data
            .column_types()
            .iter()
            .copied()
            .map(PyColumnType::from)
            .collect()
    }

    /// value(self, column)
    ///
    /// Parameters
    /// ----------
    /// column : int | str
    ///     Column index or name.
    ///
    /// Returns
    /// -------
    /// object
    ///     Cell value converted to a Python scalar or `None` if missing.
    pub fn value(&self, py: Python<'_>, column: Bound<'_, PyAny>) -> PyResult<Py<PyAny>> {
        let idx = parse_column_index(&self.data, column)?;
        match self.data.value(idx, self.row) {
            Some(v) => value_to_py(py, v),
            None => Ok(py.None()),
        }
    }

    /// columns(self)
    ///
    /// Returns
    /// -------
    /// list[tuple[str, ColumnType, object]]
    ///     Column name, type, and value for each column in the row.
    pub fn columns(&self, py: Python<'_>) -> PyResult<Vec<(String, PyColumnType, Py<PyAny>)>> {
        let row = self
            .data
            .row(self.row)
            .map_err(|e| py_ccdb_error(CCDBError::from(e)))?;
        row.iter_columns()
            .map(|(name, ty, v)| {
                Ok((
                    name.to_string(),
                    PyColumnType::from(ty),
                    value_to_py(py, v)?,
                ))
            })
            .collect()
    }

    fn __repr__(&self) -> String {
        let cols: Vec<String> = self
            .data
            .column_names()
            .iter()
            .zip(self.data.column_types())
            .map(|(n, t)| format!("{}:{}", n, t.as_str()))
            .collect();
        format!("RowView(row={}, columns=[{}])", self.row, cols.join(", "))
    }
}

/// Handle to a CCDB type table, exposing metadata and fetch APIs to Python.
///
/// Attributes
/// ----------
/// name : str
///     Table name without directory components.
/// id : int
///     Unique table identifier in CCDB.
/// meta : TypeTableMeta
///     Metadata describing row/column counts and comments.
#[pyclass(name = "TypeTableHandle", module = "gluex_ccdb", unsendable)]
pub struct PyTypeTableHandle {
    inner: TypeTableHandle,
}

#[pymethods]
impl PyTypeTableHandle {
    /// str: Table name (without directory components).
    #[getter]
    pub fn name(&self) -> &str {
        self.inner.name()
    }
    /// int: Numeric identifier of the table in CCDB.
    #[getter]
    pub fn id(&self) -> i64 {
        self.inner.id()
    }
    /// TypeTableMeta: Metadata such as row counts and comments.
    #[getter]
    pub fn meta(&self) -> PyTypeTableMeta {
        PyTypeTableMeta {
            inner: self.inner.meta().clone(),
        }
    }
    /// str: Absolute path to this table.
    pub fn full_path(&self) -> String {
        self.inner.full_path()
    }
    /// columns(self)
    ///
    /// Returns
    /// -------
    /// list[ColumnMeta]
    ///     Metadata for each column in order.
    pub fn columns(&self) -> PyResult<Vec<PyColumnMeta>> {
        Ok(self
            .inner
            .columns()
            .map_err(py_ccdb_error)?
            .into_iter()
            .map(|m| PyColumnMeta { inner: m })
            .collect())
    }
    /// fetch(self, *, runs=None, variation=None, timestamp=None)
    ///
    /// Parameters
    /// ----------
    /// runs : list[int] | None, optional
    ///     Run numbers to query; defaults to run 0 when omitted.
    /// variation : str | None, optional
    ///     Variation branch to resolve (default "default").
    /// timestamp : datetime | str | None, optional
    ///     Timestamp used to select historical assignments.
    ///
    /// Returns
    /// -------
    /// dict[int, Data]
    ///     Mapping of run number to fetched dataset.
    #[pyo3(signature = (*, runs=None, variation=None, timestamp=None))]
    pub fn fetch(
        &self,
        runs: Option<Vec<RunNumber>>,
        variation: Option<String>,
        timestamp: Option<Bound<'_, PyAny>>,
    ) -> PyResult<BTreeMap<RunNumber, PyData>> {
        let ctx = build_context(runs, variation, timestamp)?;
        Ok(self
            .inner
            .fetch(&ctx)
            .map_err(py_ccdb_error)?
            .into_iter()
            .map(|(run, data)| {
                (
                    run,
                    PyData {
                        inner: Arc::new(data),
                    },
                )
            })
            .collect())
    }

    /// fetch_run_period(self, *, run_period, rest_version=None, variation=None, timestamp=None)
    ///
    /// Parameters
    /// ----------
    /// run_period : str
    ///     The short string of the corresponding GlueX run period (e.g. "S17", "F18")
    /// rest_version : int | None, optional
    ///     The REST version to use when resolving a time stamp.
    /// variation : str | None, optional
    ///     Variation branch to resolve (default "default").
    /// timestamp : datetime | str | None, optional
    ///     Timestamp used to select historical assignments. This will override timestamp from the REST version if provided
    ///
    /// Returns
    /// -------
    /// dict[int, Data]
    ///     Mapping of run number to fetched dataset.
    #[pyo3(signature = (*, run_period, rest_version=None, variation=None, timestamp=None))]
    pub fn fetch_run_period(
        &self,
        run_period: &str,
        rest_version: Option<usize>,
        variation: Option<String>,
        timestamp: Option<Bound<'_, PyAny>>,
    ) -> PyResult<BTreeMap<RunNumber, PyData>> {
        let mut ctx = Context::default()
            .with_run_period(
                run_period
                    .parse()
                    .map_err(|e: RunPeriodError| py_ccdb_error(CCDBError::RunPeriodError(e)))?,
                rest_version,
            )
            .map_err(py_ccdb_error)?;
        if let Some(variation) = variation {
            ctx.variation = variation;
        }
        if let Some(ts) = parse_py_timestamp(timestamp)? {
            ctx.timestamp = ts;
        }
        Ok(self
            .inner
            .fetch(&ctx)
            .map_err(py_ccdb_error)?
            .into_iter()
            .map(|(run, data)| {
                (
                    run,
                    PyData {
                        inner: Arc::new(data),
                    },
                )
            })
            .collect())
    }

    fn __repr__(&self) -> String {
        format!("TypeTable(\"{}\")", self.inner.full_path())
    }
    fn __str__(&self) -> String {
        self.__repr__()
    }
}

/// Handle to a CCDB directory, mirroring the Rust API for navigation.
///
/// Attributes
/// ----------
/// full_path : str
///     Absolute directory path within CCDB.
#[pyclass(name = "DirectoryHandle", module = "gluex_ccdb", unsendable)]
pub struct PyDirectoryHandle {
    inner: DirectoryHandle,
}

#[pymethods]
impl PyDirectoryHandle {
    /// str: Full path of this directory.
    pub fn full_path(&self) -> String {
        self.inner.full_path()
    }
    /// parent(self)
    ///
    /// Returns
    /// -------
    /// DirectoryHandle | None
    ///     Parent directory or ``None`` when at the root.
    pub fn parent(&self) -> Option<Self> {
        self.inner.parent().map(|inner| Self { inner })
    }
    /// dirs(self)
    ///
    /// Returns
    /// -------
    /// list[DirectoryHandle]
    ///     Child directories directly under this directory.
    pub fn dirs(&self) -> Vec<Self> {
        self.inner
            .dirs()
            .into_iter()
            .map(|inner| Self { inner })
            .collect()
    }
    /// dir(self, name)
    ///
    /// Parameters
    /// ----------
    /// name : str
    ///     Relative directory name.
    ///
    /// Returns
    /// -------
    /// DirectoryHandle
    ///     Handle to the requested subdirectory.
    pub fn dir(&self, name: &str) -> PyResult<Self> {
        Ok(Self {
            inner: self.inner.dir(name).map_err(py_ccdb_error)?,
        })
    }
    /// tables(self)
    ///
    /// Returns
    /// -------
    /// list[TypeTableHandle]
    ///     Tables that live directly under this directory.
    pub fn tables(&self) -> Vec<PyTypeTableHandle> {
        self.inner
            .tables()
            .into_iter()
            .map(|inner| PyTypeTableHandle { inner })
            .collect()
    }
    /// table(self, name)
    ///
    /// Parameters
    /// ----------
    /// name : str
    ///     Table name relative to this directory.
    ///
    /// Returns
    /// -------
    /// TypeTableHandle
    ///     Handle to the requested table.
    pub fn table(&self, name: &str) -> PyResult<PyTypeTableHandle> {
        Ok(PyTypeTableHandle {
            inner: self.inner.table(name).map_err(py_ccdb_error)?,
        })
    }
    fn __repr__(&self) -> String {
        format!("Directory(\"{}\")", self.full_path())
    }
    fn __str__(&self) -> String {
        self.__repr__()
    }
}

/// Entry point for interacting with CCDB from Python.
///
/// Parameters
/// ----------
/// path : str
///     Filesystem path to an existing CCDB SQLite database file.
#[pyclass(name = "CCDB", module = "gluex_ccdb", unsendable)]
pub struct PyCCDB {
    inner: CCDB,
}

#[pymethods]
impl PyCCDB {
    /// __init__(self, path)
    ///
    /// Parameters
    /// ----------
    /// path : str
    ///     Filesystem path to an existing CCDB SQLite database file.
    #[new]
    pub fn new(path: &str) -> PyResult<Self> {
        Ok(Self {
            inner: CCDB::open(path).map_err(py_ccdb_error)?,
        })
    }

    /// dir(self, path)
    ///
    /// Parameters
    /// ----------
    /// path : str
    ///     Absolute or relative directory path.
    ///
    /// Returns
    /// -------
    /// DirectoryHandle
    ///     Handle to the requested directory.
    pub fn dir(&self, path: &str) -> PyResult<PyDirectoryHandle> {
        Ok(PyDirectoryHandle {
            inner: self.inner.dir(path).map_err(py_ccdb_error)?,
        })
    }
    /// table(self, path)
    ///
    /// Parameters
    /// ----------
    /// path : str
    ///     Absolute or relative table path.
    ///
    /// Returns
    /// -------
    /// TypeTableHandle
    ///     Handle to the requested table.
    pub fn table(&self, path: &str) -> PyResult<PyTypeTableHandle> {
        Ok(PyTypeTableHandle {
            inner: self.inner.table(path).map_err(py_ccdb_error)?,
        })
    }
    /// fetch(self, path, *, runs=None, variation=None, timestamp=None)
    ///
    /// Parameters
    /// ----------
    /// path : str
    ///     Absolute or relative table path.
    /// runs : list[int] | None, optional
    ///     Run numbers to query; defaults to run 0 when omitted.
    /// variation : str | None, optional
    ///     Variation branch to resolve (default "default").
    /// timestamp : datetime | str | None, optional
    ///     Timestamp used to select historical assignments.
    ///
    /// Returns
    /// -------
    /// dict[int, Data]
    ///     Mapping of run number to fetched dataset.
    #[pyo3(signature = (path, *, runs=None, variation=None, timestamp=None))]
    pub fn fetch(
        &self,
        path: &str,
        runs: Option<Vec<RunNumber>>,
        variation: Option<String>,
        timestamp: Option<Bound<'_, PyAny>>,
    ) -> PyResult<BTreeMap<RunNumber, PyData>> {
        let ctx = build_context(runs, variation, timestamp)?;
        Ok(self
            .inner
            .fetch(path, &ctx)
            .map_err(py_ccdb_error)?
            .into_iter()
            .map(|(run, data)| {
                (
                    run,
                    PyData {
                        inner: Arc::new(data),
                    },
                )
            })
            .collect())
    }

    /// fetch_run_period(self, path, *, run_period, rest_version=None, variation=None, timestamp=None)
    ///
    /// Parameters
    /// ----------
    /// path : str
    ///     Absolute or relative table path.
    /// run_period : str
    ///     The short string of the corresponding GlueX run period (e.g. "S17", "F18")
    /// rest_version : int | None, optional
    ///     The REST version to use when resolving a time stamp.
    /// variation : str | None, optional
    ///     Variation branch to resolve (default "default").
    /// timestamp : datetime | str | None, optional
    ///     Timestamp used to select historical assignments. This will override timestamp from the REST version if provided
    ///
    /// Returns
    /// -------
    /// dict[int, Data]
    ///     Mapping of run number to fetched dataset.
    #[pyo3(signature = (path, *, run_period, rest_version=None, variation=None, timestamp=None))]
    pub fn fetch_run_period(
        &self,
        path: &str,
        run_period: &str,
        rest_version: Option<usize>,
        variation: Option<String>,
        timestamp: Option<Bound<'_, PyAny>>,
    ) -> PyResult<BTreeMap<RunNumber, PyData>> {
        let mut ctx = Context::default()
            .with_run_period(
                run_period
                    .parse()
                    .map_err(|e: RunPeriodError| py_ccdb_error(CCDBError::RunPeriodError(e)))?,
                rest_version,
            )
            .map_err(py_ccdb_error)?;
        if let Some(variation) = variation {
            ctx.variation = variation;
        }
        if let Some(ts) = parse_py_timestamp(timestamp)? {
            ctx.timestamp = ts;
        }
        Ok(self
            .inner
            .fetch(path, &ctx)
            .map_err(py_ccdb_error)?
            .into_iter()
            .map(|(run, data)| {
                (
                    run,
                    PyData {
                        inner: Arc::new(data),
                    },
                )
            })
            .collect())
    }

    /// root(self)
    ///
    /// Returns
    /// -------
    /// DirectoryHandle
    ///     Handle to the root directory.
    pub fn root(&self) -> PyResult<PyDirectoryHandle> {
        Ok(PyDirectoryHandle {
            inner: self.inner.root(),
        })
    }
    /// str: Filesystem path that was used to open the database.
    #[getter]
    pub fn connection_path(&self) -> &str {
        self.inner.connection_path()
    }

    fn __repr__(&self) -> String {
        format!("CCDB(\"{}\")", self.inner.connection_path())
    }
    fn __str__(&self) -> String {
        self.__repr__()
    }
}

fn value_to_py(py: Python<'_>, value: Value<'_>) -> PyResult<Py<PyAny>> {
    Ok(match value {
        Value::Int(v) => PyInt::new(py, *v).unbind().into(),
        Value::UInt(v) => PyInt::new(py, *v).unbind().into(),
        Value::Long(v) => PyInt::new(py, *v).unbind().into(),
        Value::ULong(v) => PyInt::new(py, *v).unbind().into(),
        Value::Double(v) => PyFloat::new(py, *v).unbind().into(),
        Value::Bool(v) => {
            let obj = (*v).into_pyobject(py)?;
            <pyo3::Bound<'_, _> as Clone>::clone(&obj)
                .into_any()
                .unbind()
        }
        Value::String(v) => PyString::new(py, v).unbind().into(),
    })
}

fn parse_py_timestamp(ts: Option<Bound<'_, PyAny>>) -> PyResult<Option<DateTime<Utc>>> {
    let Some(val) = ts else {
        return Ok(None);
    };
    if let Ok(dt) = val.extract::<DateTime<Utc>>() {
        return Ok(Some(dt));
    }
    if let Ok(s) = val.extract::<String>() {
        let parsed = parse_timestamp(&s).map_err(|e| PyRuntimeError::new_err(e.to_string()))?;
        return Ok(Some(parsed));
    }
    Err(PyRuntimeError::new_err("timestamp must be str or datetime"))
}

fn parse_column_index(data: &Data, column: Bound<'_, PyAny>) -> PyResult<usize> {
    if let Ok(idx) = column.extract::<usize>() {
        if idx < data.n_columns() {
            return Ok(idx);
        }
        return Err(PyRuntimeError::new_err("column index out of range"));
    }
    if let Ok(name) = column.extract::<String>() {
        if let Some(idx) = data.column_names().iter().position(|n| n == &name) {
            return Ok(idx);
        }
        return Err(PyRuntimeError::new_err("column name not found"));
    }
    Err(PyRuntimeError::new_err("column must be int or str"))
}

fn build_context(
    runs: Option<Vec<RunNumber>>,
    variation: Option<String>,
    timestamp: Option<Bound<'_, PyAny>>,
) -> PyResult<Context> {
    let mut ctx = Context::default();
    if let Some(runs) = runs {
        ctx.runs = runs;
    }
    if let Some(variation) = variation {
        ctx.variation = variation;
    }
    if let Some(ts) = parse_py_timestamp(timestamp)? {
        ctx.timestamp = ts;
    }
    Ok(ctx)
}

#[pymodule]
/// Python module initializer for `gluex_ccdb` bindings.
pub fn gluex_ccdb(_py: Python<'_>, m: &Bound<'_, PyModule>) -> PyResult<()> {
    m.add_class::<PyCCDB>()?;
    m.add_class::<PyTypeTableHandle>()?;
    m.add_class::<PyDirectoryHandle>()?;
    m.add_class::<PyData>()?;
    m.add_class::<PyRowView>()?;
    m.add_class::<PyColumn>()?;
    m.add_class::<PyColumnMeta>()?;
    m.add_class::<PyTypeTableMeta>()?;
    m.add_class::<PyColumnType>()?;
    Ok(())
}
