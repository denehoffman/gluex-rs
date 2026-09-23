//! In-memory conversion from laddu's Python dataset API to its Rust dataset API.

use std::sync::Arc;

use laddu::data::{
    RealVec4,
    data::{Dataset, EventBatch},
    schema::Schema,
};
use numpy::PyReadonlyArray1;
use pyo3::{
    exceptions::PyValueError,
    prelude::*,
    types::{PyAny, PyDict},
};

fn numbers(array: &Bound<'_, PyAny>) -> PyResult<Vec<f64>> {
    let array = array.extract::<PyReadonlyArray1<'_, f64>>()?;
    Ok(array.as_array().iter().copied().collect())
}

fn evaluate(dataset: &Bound<'_, PyAny>, expression: &Bound<'_, PyAny>) -> PyResult<Vec<f64>> {
    let kwargs = PyDict::new(dataset.py());
    kwargs.set_item("real", true)?;
    numbers(&dataset.call_method("evaluate", (expression,), Some(&kwargs))?)
}

/// Materialize a Python laddu dataset as a Rust dataset without a file bridge.
pub(super) fn from_python(py: Python<'_>, dataset: &Bound<'_, PyAny>) -> PyResult<Dataset> {
    let laddu = py.import("laddu")?;
    let p4_names: Vec<String> = dataset.call_method0("p4_names")?.extract()?;
    let scalar_names: Vec<String> = dataset.call_method0("scalar_names")?.extract()?;
    let weights = numbers(&dataset.call_method0("weights")?)?;

    let vec4 = laddu.getattr("Vec4")?;
    let mut p4_columns = Vec::with_capacity(p4_names.len());
    for name in &p4_names {
        let expression = vec4.call_method1("event", (name,))?;
        let e = evaluate(dataset, &expression.call_method0("e")?)?;
        let px = evaluate(dataset, &expression.call_method0("px")?)?;
        let py = evaluate(dataset, &expression.call_method0("py")?)?;
        let pz = evaluate(dataset, &expression.call_method0("pz")?)?;
        if [e.len(), px.len(), py.len(), pz.len()]
            .iter()
            .any(|&length| length != weights.len())
        {
            return Err(PyValueError::new_err(format!(
                "four-vector column '{name}' changed length while reading the dataset"
            )));
        }
        let column: Arc<[RealVec4]> = e
            .into_iter()
            .zip(px)
            .zip(py)
            .zip(pz)
            .map(|(((e, px), py), pz)| RealVec4::new(e, px, py, pz))
            .collect();
        p4_columns.push(column);
    }

    let mut scalar_columns = Vec::with_capacity(scalar_names.len());
    for name in &scalar_names {
        let expression = laddu.call_method1("scalar", (name,))?;
        scalar_columns.push(Arc::<[f64]>::from(evaluate(dataset, &expression)?));
    }

    let schema = Arc::new(
        Schema::new(p4_names, scalar_names, true)
            .map_err(|error| pyo3::exceptions::PyValueError::new_err(error.to_string()))?,
    );
    let batch = EventBatch::new(schema, p4_columns, scalar_columns, Some(weights.into()))
        .map_err(|error| pyo3::exceptions::PyValueError::new_err(error.to_string()))?;
    Ok(Dataset::from_batch(batch))
}
