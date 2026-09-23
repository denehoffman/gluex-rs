#[pyo3::pymodule(submodule)]
pub(crate) mod io {
    use std::path::PathBuf;

    use crate::io::{
        AmpToolsSource, FsRootSink, FsRootSource, PawianOrder, PawianSink, PawianSource, PawianUnit,
    };
    use laddu::data::io::{EventSource, ReadPlan};
    use numpy::{PyArray1, PyArray2};
    use pyo3::{
        exceptions::{PyRuntimeError, PyValueError},
        prelude::*,
        types::{PyAnyMethods, PyDict, PyDictMethods},
    };

    fn io_error(error: impl std::fmt::Display) -> PyErr {
        PyRuntimeError::new_err(error.to_string())
    }
    fn labels(particles: usize, names: Option<Vec<String>>) -> PyResult<Vec<String>> {
        let names =
            names.unwrap_or_else(|| (1..=particles).map(|i| format!("final_{i}")).collect());
        if names.len() != particles
            || names
                .iter()
                .any(|name| name.is_empty() || name == "beam" || name == "target")
        {
            return Err(PyValueError::new_err(
                "final_state_names must contain exactly `particles` nonempty names, excluding beam and target",
            ));
        }
        Ok(names)
    }
    fn order(value: &str) -> PyResult<PawianOrder> {
        match value {
            "px_py_pz_e" => Ok(PawianOrder::PxPyPzE),
            "e_px_py_pz" => Ok(PawianOrder::EPxPyPz),
            _ => Err(PyValueError::new_err(
                "order must be 'px_py_pz_e' or 'e_px_py_pz'",
            )),
        }
    }
    fn unit(value: &str) -> PyResult<PawianUnit> {
        match value {
            "GeV" => Ok(PawianUnit::GeV),
            "MeV" => Ok(PawianUnit::MeV),
            _ => Err(PyValueError::new_err("unit must be 'GeV' or 'MeV'")),
        }
    }
    #[derive(Clone)]
    enum Source {
        AmpTools(AmpToolsSource),
        FsRoot(FsRootSource),
        Pawian(PawianSource),
    }
    impl Source {
        fn inner(&self) -> &dyn EventSource {
            match self {
                Self::AmpTools(x) => x,
                Self::FsRoot(x) => x,
                Self::Pawian(x) => x,
            }
        }
    }

    #[pyclass]
    struct BatchFactory(Source);

    #[pymethods]
    impl BatchFactory {
        #[pyo3(signature = (*, chunk_size=None, rank=0, nranks=1))]
        fn __call__(
            &self,
            py: Python<'_>,
            chunk_size: Option<usize>,
            rank: usize,
            nranks: usize,
        ) -> PyResult<Vec<Py<PyAny>>> {
            if rank != 0 || nranks != 1 {
                return Err(PyValueError::new_err(
                    "distributed reads are not supported by these GlueX sources",
                ));
            }
            let mut result = Vec::new();
            let mut plan = ReadPlan::default();
            plan.chunk_size = chunk_size;
            for batch in self.0.inner().batches(plan).map_err(io_error)? {
                let batch = batch.map_err(io_error)?;
                let schema = batch.schema();
                let p4s = PyDict::new(py);
                for (index, name) in schema.p4s().iter().enumerate() {
                    let values = batch
                        .iter()
                        .map(|event| event.p4(index).components().to_vec())
                        .collect::<Vec<_>>();
                    p4s.set_item(
                        name.as_ref(),
                        PyArray2::from_vec2(py, &values).map_err(io_error)?,
                    )?;
                }
                let scalars = PyDict::new(py);
                for (index, name) in schema.scalars().iter().enumerate() {
                    let values = batch
                        .iter()
                        .map(|event| event.scalar(index))
                        .collect::<Vec<_>>();
                    scalars.set_item(name.as_ref(), PyArray1::from_vec(py, values))?;
                }
                let item = PyDict::new(py);
                item.set_item("p4s", p4s)?;
                item.set_item("scalars", scalars)?;
                if schema.has_weight() {
                    item.set_item(
                        "weights",
                        PyArray1::from_vec(py, batch.iter().map(|event| event.weight()).collect()),
                    )?;
                }
                result.push(item.into_any().unbind());
            }
            Ok(result)
        }
    }

    fn dataset(py: Python<'_>, source: Source) -> PyResult<Py<PyAny>> {
        let schema = source.inner().schema().map_err(io_error)?;
        let spec = PyDict::new(py);
        spec.set_item(
            "p4s",
            schema
                .p4s()
                .iter()
                .map(ToString::to_string)
                .collect::<Vec<_>>(),
        )?;
        spec.set_item(
            "scalars",
            schema
                .scalars()
                .iter()
                .map(ToString::to_string)
                .collect::<Vec<_>>(),
        )?;
        spec.set_item("weights", schema.has_weight())?;
        let factory = Py::new(py, BatchFactory(source))?;
        let laddu = py.import("laddu")?;
        let kwargs = PyDict::new(py);
        kwargs.set_item("schema", spec)?;
        Ok(laddu
            .getattr("Dataset")?
            .call_method("from_batches", (factory,), Some(&kwargs))?
            .unbind())
    }

    /// Read an AmpTools `kin` tree into a laddu dataset.
    #[pyfunction]
    #[pyo3(signature = (path, *, particles, final_state_names=None, tree="kin", zlm_polarization=false) -> "laddu.Dataset")]
    fn read_amptools(
        py: Python<'_>,
        path: PathBuf,
        particles: usize,
        final_state_names: Option<Vec<String>>,
        tree: &str,
        zlm_polarization: bool,
    ) -> PyResult<Py<PyAny>> {
        dataset(
            py,
            Source::AmpTools(
                AmpToolsSource::new(path, labels(particles, final_state_names)?)
                    .with_tree(tree)
                    .with_zlm_polarization(zlm_polarization),
            ),
        )
    }

    /// Read an FSRoot tree into a laddu dataset.
    #[pyfunction]
    #[pyo3(signature = (path, *, particles, final_state_names=None, tree="nt", prefix="", weight_branch="weight") -> "laddu.Dataset")]
    fn read_fsroot(
        py: Python<'_>,
        path: PathBuf,
        particles: usize,
        final_state_names: Option<Vec<String>>,
        tree: &str,
        prefix: &str,
        weight_branch: &str,
    ) -> PyResult<Py<PyAny>> {
        let source = FsRootSource::new(path, labels(particles, final_state_names)?)
            .with_tree(tree)
            .with_prefix(prefix)
            .with_weight_branch(weight_branch);
        dataset(py, Source::FsRoot(source))
    }

    /// Read Pawian text events into a laddu dataset.
    #[pyfunction]
    #[pyo3(signature = (path, *, particles, final_state_names=None, weights=false, polarization=false, order="px_py_pz_e", unit="GeV") -> "laddu.Dataset")]
    fn read_pawian(
        py: Python<'_>,
        path: PathBuf,
        particles: usize,
        final_state_names: Option<Vec<String>>,
        weights: bool,
        polarization: bool,
        order: &str,
        unit: &str,
    ) -> PyResult<Py<PyAny>> {
        dataset(
            py,
            Source::Pawian(
                PawianSource::new(path, labels(particles, final_state_names)?)
                    .with_weights(weights)
                    .with_polarization(polarization)
                    .with_order(self::order(order)?)
                    .with_unit(self::unit(unit)?),
            ),
        )
    }

    fn write_dataset(
        py: Python<'_>,
        dataset: &Bound<'_, PyAny>,
        sink: impl laddu::data::io::EventSink + 'static,
    ) -> PyResult<()> {
        let data = crate::python::laddu_bridge::from_python(py, dataset)?;
        py.detach(move || {
            let mut sink = sink;
            data.write_to(&mut sink).map_err(io_error)
        })
    }

    /// Write a laddu dataset to an FSRoot tree.
    #[pyfunction]
    #[pyo3(signature = (dataset: "laddu.Dataset", path, *, particles, final_state_names=None, tree="nt", prefix="", weight_branch="weight"))]
    fn write_fsroot(
        py: Python<'_>,
        dataset: &Bound<'_, PyAny>,
        path: PathBuf,
        particles: usize,
        final_state_names: Option<Vec<String>>,
        tree: &str,
        prefix: &str,
        weight_branch: &str,
    ) -> PyResult<()> {
        let sink = FsRootSink::new(path, labels(particles, final_state_names)?)
            .with_tree(tree)
            .with_prefix(prefix)
            .with_weight_branch(weight_branch);
        write_dataset(py, dataset, sink)
    }

    /// Write a laddu dataset in Pawian's default text layout.
    #[pyfunction]
    #[pyo3(signature = (dataset: "laddu.Dataset", path, *, particles, final_state_names=None, weights=false, polarization=false, order="px_py_pz_e", unit="GeV"))]
    fn write_pawian(
        py: Python<'_>,
        dataset: &Bound<'_, PyAny>,
        path: PathBuf,
        particles: usize,
        final_state_names: Option<Vec<String>>,
        weights: bool,
        polarization: bool,
        order: &str,
        unit: &str,
    ) -> PyResult<()> {
        let sink = PawianSink::new(path, labels(particles, final_state_names)?)
            .with_weights(weights)
            .with_polarization(polarization)
            .with_order(self::order(order)?)
            .with_unit(self::unit(unit)?);
        write_dataset(py, dataset, sink)
    }
}
