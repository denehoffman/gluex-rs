use super::tuple::{TypedIterator, TypedTuple};
use crate::{RawColumn, RawResults, RawRow, RawValue};
use pyo3::{
    exceptions::{PyIndexError, PyKeyError, PyTypeError, PyValueError},
    prelude::*,
    types::{PyAny, PyBytes, PyInt},
};
use std::sync::Arc;

#[derive(FromPyObject)]
pub enum Scalar {
    Integer(Py<PyInt>),
    Real(f64),
    Text(String),
    Blob(Py<PyBytes>),
}

pub(crate) fn parameters(py: Python<'_>, values: Vec<Option<Scalar>>) -> PyResult<Vec<RawValue>> {
    values
        .into_iter()
        .map(|value| {
            Ok(match value {
                None => RawValue::Null,
                Some(Scalar::Integer(v)) => RawValue::Integer(v.bind(py).extract()?),
                Some(Scalar::Real(v)) => RawValue::Real(v),
                Some(Scalar::Text(v)) => RawValue::Text(v),
                Some(Scalar::Blob(v)) => RawValue::Blob(v.bind(py).as_bytes().to_vec()),
            })
        })
        .collect()
}

#[derive(IntoPyObject)]
pub enum OutputScalar {
    Integer(i64),
    Real(f64),
    Text(String),
    Blob(Py<PyBytes>),
}

fn value_to_py(py: Python<'_>, value: &RawValue) -> Option<OutputScalar> {
    Some(match value {
        RawValue::Null => return None,
        RawValue::Integer(v) => OutputScalar::Integer(*v),
        RawValue::Real(v) => OutputScalar::Real(*v),
        RawValue::Text(v) => OutputScalar::Text(v.clone()),
        RawValue::Blob(v) => OutputScalar::Blob(PyBytes::new(py, v).unbind()),
    })
}

/// Immutable raw column name and optional SQLite declared type.
#[pyclass(name = "RawColumn", module = "gluex", frozen)]
pub struct PyRawColumn(RawColumn);
#[pymethods]
impl PyRawColumn {
    /// SQL result name or alias. Names may repeat.
    #[getter]
    fn name(&self) -> &str {
        self.0.name()
    }
    /// SQLite declared type, or None for expressions without a declared type.
    #[getter]
    fn declared_type(&self) -> Option<&str> {
        self.0.declared_type()
    }
    fn __repr__(&self) -> String {
        format!(
            "RawColumn(name={:?}, declared_type={:?})",
            self.0.name(),
            self.0.declared_type()
        )
    }
}

/// Immutable positional values aligned with the result's columns.
#[pyclass(name = "RawRow", module = "gluex", frozen)]
pub struct PyRawRow {
    row: RawRow,
    column_names: Arc<[String]>,
}
#[pymethods]
impl PyRawRow {
    /// SQL values as a tuple of None, int, float, str or bytes.
    #[getter]
    fn values(&self, py: Python<'_>) -> TypedTuple<Option<OutputScalar>> {
        TypedTuple(
            self.row
                .values()
                .iter()
                .map(|v| value_to_py(py, v))
                .collect(),
        )
    }
    fn __len__(&self) -> usize {
        self.row.values().len()
    }
    fn __iter__(&self, py: Python<'_>) -> TypedIterator<Option<OutputScalar>> {
        TypedIterator(
            self.row
                .values()
                .iter()
                .map(|v| value_to_py(py, v))
                .collect(),
        )
    }
    /// Look up a value by integer position or by a unique SQL column name.
    fn __getitem__(
        &self,
        py: Python<'_>,
        key: &Bound<'_, PyAny>,
    ) -> PyResult<Option<OutputScalar>> {
        let index = if let Ok(index) = key.extract::<isize>() {
            let len = self.row.values().len() as isize;
            let normalized = if index < 0 { len + index } else { index };
            if normalized < 0 || normalized >= len {
                return Err(PyIndexError::new_err(format!(
                    "raw row index {index} is out of range for {len} columns"
                )));
            }
            normalized as usize
        } else if let Ok(name) = key.extract::<String>() {
            let matches = self
                .column_names
                .iter()
                .enumerate()
                .filter_map(|(index, candidate)| (candidate == &name).then_some(index))
                .collect::<Vec<_>>();
            match matches.as_slice() {
                [index] => *index,
                [] => return Err(PyKeyError::new_err(name)),
                _ => {
                    return Err(PyValueError::new_err(format!(
                        "raw column name {name:?} is ambiguous ({} matches)",
                        matches.len()
                    )));
                }
            }
        } else {
            return Err(PyTypeError::new_err(
                "raw row indices must be integers or column names",
            ));
        };
        Ok(value_to_py(py, &self.row.values()[index]))
    }
    fn __repr__(&self) -> String {
        format!("RawRow({:?})", self.row.values())
    }
}

/// Immutable materialized raw rows and ordered column metadata.
#[pyclass(name = "RawResults", module = "gluex", frozen)]
pub struct PyRawResults(pub(crate) RawResults);
#[pymethods]
impl PyRawResults {
    /// Column metadata in SQL result order, even when there are no rows.
    #[getter]
    fn columns(&self) -> TypedTuple<PyRawColumn> {
        TypedTuple(self.0.columns().iter().cloned().map(PyRawColumn).collect())
    }
    /// Rows in SQL result order as an immutable tuple.
    #[getter]
    fn rows(&self) -> TypedTuple<PyRawRow> {
        let column_names: Arc<[String]> = self
            .0
            .columns()
            .iter()
            .map(|column| column.name().to_owned())
            .collect::<Vec<_>>()
            .into();
        TypedTuple(
            self.0
                .rows()
                .iter()
                .cloned()
                .map(|row| PyRawRow {
                    row,
                    column_names: column_names.clone(),
                })
                .collect(),
        )
    }
    fn __len__(&self) -> usize {
        self.0.rows().len()
    }
    fn __repr__(&self) -> String {
        format!(
            "RawResults(rows={}, columns={})",
            self.0.rows().len(),
            self.0.columns().len()
        )
    }
}
