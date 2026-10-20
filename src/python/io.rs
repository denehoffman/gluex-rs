#[pyo3::pymodule(submodule)]
pub(crate) mod io {
    use std::{
        path::PathBuf,
        sync::{Arc, Mutex},
    };

    use crate::{
        generation::{GlueXHddmConfig, HddmSink},
        io::{
            AmpToolsSource, FsRootSink, FsRootSource, PawianOrder, PawianSink, PawianSource,
            PawianUnit,
        },
    };
    use laddu::data::{
        RealVec4,
        data::EventBatch,
        io::{EventBatchIter, EventSink, EventSource, ReadPlan, WritePlan},
        schema::Schema,
    };
    use numpy::{IntoPyArray, PyArray1, PyReadonlyArray1, PyReadonlyArray2, ndarray::Array2};
    use pyo3::{
        exceptions::{PyRuntimeError, PyValueError},
        inspect::PyStaticExpr,
        prelude::*,
        types::PyDict,
    };

    struct DatasetResult(Py<PyAny>);
    pub(crate) struct WriteResult(Py<PyAny>);
    struct FormatSinkResult(Py<PyAny>);
    struct FormatSpecResult(Py<PyAny>);
    struct DatasetInput(Py<PyAny>);
    pub(crate) struct ExecutionInput(pub(crate) Py<PyAny>);

    impl<'a, 'py> FromPyObject<'a, 'py> for DatasetInput {
        type Error = PyErr;
        const INPUT_TYPE: PyStaticExpr = pyo3::type_hint_identifier!("laddu", "Dataset");

        fn extract(value: Borrowed<'a, 'py, PyAny>) -> PyResult<Self> {
            Ok(Self(value.to_owned().unbind()))
        }
    }

    impl<'a, 'py> FromPyObject<'a, 'py> for ExecutionInput {
        type Error = PyErr;
        const INPUT_TYPE: PyStaticExpr = pyo3::type_hint_identifier!("laddu", "Execution");

        fn extract(value: Borrowed<'a, 'py, PyAny>) -> PyResult<Self> {
            Ok(Self(value.to_owned().unbind()))
        }
    }

    impl<'py> IntoPyObject<'py> for DatasetResult {
        type Target = PyAny;
        type Output = Bound<'py, PyAny>;
        type Error = PyErr;
        const OUTPUT_TYPE: PyStaticExpr = pyo3::type_hint_identifier!("laddu", "Dataset");

        fn into_pyobject(self, py: Python<'py>) -> PyResult<Self::Output> {
            Ok(self.0.into_bound(py))
        }
    }

    impl<'py> IntoPyObject<'py> for WriteResult {
        type Target = PyAny;
        type Output = Bound<'py, PyAny>;
        type Error = PyErr;
        const OUTPUT_TYPE: PyStaticExpr = pyo3::type_hint_identifier!("laddu.io", "WriteResult");

        fn into_pyobject(self, py: Python<'py>) -> PyResult<Self::Output> {
            Ok(self.0.into_bound(py))
        }
    }

    impl<'py> IntoPyObject<'py> for FormatSinkResult {
        type Target = PyAny;
        type Output = Bound<'py, PyAny>;
        type Error = PyErr;
        const OUTPUT_TYPE: PyStaticExpr = pyo3::type_hint_identifier!("laddu.io", "FormatSink");

        fn into_pyobject(self, py: Python<'py>) -> PyResult<Self::Output> {
            Ok(self.0.into_bound(py))
        }
    }

    impl<'py> IntoPyObject<'py> for FormatSpecResult {
        type Target = PyAny;
        type Output = Bound<'py, PyAny>;
        type Error = PyErr;
        const OUTPUT_TYPE: PyStaticExpr = pyo3::type_hint_identifier!("laddu.io", "FormatSpec");

        fn into_pyobject(self, py: Python<'py>) -> PyResult<Self::Output> {
            Ok(self.0.into_bound(py))
        }
    }

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

    #[derive(Clone)]
    enum SourceConfig {
        AmpTools {
            names: Vec<String>,
            tree: String,
            pol_in_beam: bool,
        },
        FsRoot {
            names: Vec<String>,
            tree: String,
            prefix: String,
            weight_branch: String,
        },
        Pawian {
            names: Vec<String>,
            weights: bool,
            polarization: bool,
            order: PawianOrder,
            unit: PawianUnit,
        },
    }

    impl SourceConfig {
        fn open(&self, path: PathBuf) -> Source {
            match self {
                Self::AmpTools {
                    names,
                    tree,
                    pol_in_beam,
                } => Source::AmpTools(
                    AmpToolsSource::new(path, names.clone())
                        .with_tree(tree)
                        .with_pol_in_beam(*pol_in_beam),
                ),
                Self::FsRoot {
                    names,
                    tree,
                    prefix,
                    weight_branch,
                } => Source::FsRoot(
                    FsRootSource::new(path, names.clone())
                        .with_tree(tree)
                        .with_prefix(prefix)
                        .with_weight_branch(weight_branch),
                ),
                Self::Pawian {
                    names,
                    weights,
                    polarization,
                    order,
                    unit,
                } => Source::Pawian(
                    PawianSource::new(path, names.clone())
                        .with_weights(*weights)
                        .with_polarization(*polarization)
                        .with_order(*order)
                        .with_unit(*unit),
                ),
            }
        }
    }

    impl Source {
        fn inner(&self) -> &dyn EventSource {
            match self {
                Self::AmpTools(value) => value,
                Self::FsRoot(value) => value,
                Self::Pawian(value) => value,
            }
        }
    }

    #[derive(Clone)]
    enum Sink {
        FsRoot {
            names: Vec<String>,
            tree: String,
            prefix: String,
            weight_branch: String,
        },
        Pawian {
            names: Vec<String>,
            weights: bool,
            polarization: bool,
            order: PawianOrder,
            unit: PawianUnit,
        },
        Hddm(GlueXHddmConfig),
    }

    impl Sink {
        fn open(&self, path: PathBuf) -> PyResult<Box<dyn EventSink>> {
            match self {
                Self::FsRoot {
                    names,
                    tree,
                    prefix,
                    weight_branch,
                } => Ok(Box::new(
                    FsRootSink::new(path, names.clone())
                        .with_tree(tree)
                        .with_prefix(prefix)
                        .with_weight_branch(weight_branch),
                )),
                Self::Pawian {
                    names,
                    weights,
                    polarization,
                    order,
                    unit,
                } => Ok(Box::new(
                    PawianSink::new(path, names.clone())
                        .with_weights(*weights)
                        .with_polarization(*polarization)
                        .with_order(*order)
                        .with_unit(*unit),
                )),
                Self::Hddm(config) => Ok(Box::new(
                    HddmSink::new(path, config.clone()).map_err(io_error)?,
                )),
            }
        }
    }

    /// Callbacks passed to laddu.io.FormatSpec. laddu handles partitioning and output paths.
    #[pyclass]
    struct Adapter {
        source: Option<SourceConfig>,
        sink: Option<Sink>,
    }

    #[pymethods]
    impl Adapter {
        #[pyo3(signature = (source, *, options))]
        fn describe(
            &self,
            py: Python<'_>,
            source: &Bound<'_, PyAny>,
            options: &Bound<'_, PyAny>,
        ) -> PyResult<Py<PyAny>> {
            let _ = options;
            let source = self
                .source
                .as_ref()
                .expect("reader callback has a source")
                .open(source.extract()?);
            let schema = source.inner().schema().map_err(io_error)?;
            let length = source.inner().num_events().map_err(io_error)?;
            let laddu_io = py.import("laddu.io")?;
            let kwargs = PyDict::new(py);
            kwargs.set_item(
                "p4s",
                schema
                    .p4s()
                    .iter()
                    .map(ToString::to_string)
                    .collect::<Vec<_>>(),
            )?;
            kwargs.set_item(
                "scalars",
                schema
                    .scalars()
                    .iter()
                    .map(ToString::to_string)
                    .collect::<Vec<_>>(),
            )?;
            kwargs.set_item("weights", schema.has_weight())?;
            let schema = laddu_io.getattr("Schema")?.call((), Some(&kwargs))?;
            let kwargs = PyDict::new(py);
            kwargs.set_item("length", length)?;
            Ok(laddu_io
                .getattr("SourceInfo")?
                .call((schema,), Some(&kwargs))?
                .unbind())
        }

        #[pyo3(signature = (source, *, options, plan))]
        fn read_batches(
            &self,
            source: &Bound<'_, PyAny>,
            options: &Bound<'_, PyAny>,
            plan: &Bound<'_, PyAny>,
        ) -> PyResult<ReadBatches> {
            let _ = options;
            let mut read_plan = ReadPlan::serial();
            read_plan.chunk_size = plan.getattr("chunk_size")?.extract()?;
            let source = self
                .source
                .as_ref()
                .expect("reader callback has a source")
                .open(source.extract()?);
            Ok(ReadBatches {
                batches: Mutex::new(source.inner().batches(read_plan).map_err(io_error)?),
            })
        }

        #[pyo3(signature = (target, batches, *, options, schema, plan))]
        fn write_batches(
            &self,
            target: PathBuf,
            batches: &Bound<'_, PyAny>,
            options: &Bound<'_, PyAny>,
            schema: &Bound<'_, PyAny>,
            plan: &Bound<'_, PyAny>,
        ) -> PyResult<()> {
            let _ = (options, plan);
            let mut sink = self
                .sink
                .as_ref()
                .expect("writer callback has a sink")
                .open(target)?;
            let schema = Arc::new(
                Schema::new(
                    schema.getattr("p4s")?.extract::<Vec<String>>()?,
                    schema.getattr("scalars")?.extract::<Vec<String>>()?,
                    true,
                )
                .map_err(io_error)?,
            );
            let result = (|| {
                sink.begin(Arc::clone(&schema), WritePlan::default())
                    .map_err(io_error)?;
                for item in batches.try_iter()? {
                    let batch = rust_batch(&item?, Arc::clone(&schema))?;
                    sink.write_batch(&batch).map_err(io_error)?;
                }
                sink.finish().map_err(io_error)
            })();
            if result.is_err() {
                let _ = sink.abort();
            }
            result
        }
    }

    #[pyclass]
    struct ReadBatches {
        batches: Mutex<EventBatchIter>,
    }

    #[pymethods]
    impl ReadBatches {
        fn __iter__(slf: PyRef<'_, Self>) -> PyRef<'_, Self> {
            slf
        }

        fn __next__(&mut self, py: Python<'_>) -> PyResult<Option<Py<PyAny>>> {
            let next = self.batches.lock().map_err(io_error)?.next();
            next.map(|result| python_batch(py, &result.map_err(io_error)?))
                .transpose()
        }
    }

    fn python_batch(py: Python<'_>, batch: &EventBatch) -> PyResult<Py<PyAny>> {
        let schema = batch.schema();
        let p4s = PyDict::new(py);
        for (index, name) in schema.p4s().iter().enumerate() {
            let values = batch
                .vec4_column(index)
                .iter()
                .flat_map(|p4| p4.components())
                .collect();
            let array = Array2::from_shape_vec((batch.len(), 4), values).map_err(io_error)?;
            p4s.set_item(name.as_ref(), array.into_pyarray(py))?;
        }
        let scalars = PyDict::new(py);
        for (index, name) in schema.scalars().iter().enumerate() {
            scalars.set_item(
                name.as_ref(),
                PyArray1::from_vec(py, batch.scalar_column(index).to_vec()),
            )?;
        }
        let kwargs = PyDict::new(py);
        kwargs.set_item("p4s", p4s)?;
        kwargs.set_item("scalars", scalars)?;
        kwargs.set_item("length", batch.len())?;
        if let Some(weights) = batch.weights_column() {
            kwargs.set_item("weights", PyArray1::from_vec(py, weights.to_vec()))?;
        }
        Ok(py
            .import("laddu.io")?
            .getattr("EventBatch")?
            .call((), Some(&kwargs))?
            .unbind())
    }

    fn rust_batch(batch: &Bound<'_, PyAny>, schema: Arc<Schema>) -> PyResult<EventBatch> {
        let length = batch.len()?;
        let p4s = batch.getattr("p4s")?;
        let mut p4_columns = Vec::with_capacity(schema.n_p4s());
        for name in schema.p4s() {
            let array = p4s
                .get_item(name.as_ref())?
                .extract::<PyReadonlyArray2<'_, f64>>()?;
            let values = array.as_array();
            if values.nrows() != length || values.ncols() != 4 {
                return Err(PyValueError::new_err(format!(
                    "four-vector {name} must have shape ({length}, 4)"
                )));
            }
            p4_columns.push(
                values
                    .outer_iter()
                    .map(|row| RealVec4::new(row[0], row[1], row[2], row[3]))
                    .collect(),
            );
        }
        let scalars = batch.getattr("scalars")?;
        let mut scalar_columns = Vec::with_capacity(schema.n_scalars());
        for name in schema.scalars() {
            let array = scalars
                .get_item(name.as_ref())?
                .extract::<PyReadonlyArray1<'_, f64>>()?;
            scalar_columns.push(Arc::<[f64]>::from(
                array.as_array().iter().copied().collect::<Vec<_>>(),
            ));
        }
        let array = batch
            .getattr("weights")?
            .extract::<PyReadonlyArray1<'_, f64>>()?;
        let weights = Arc::<[f64]>::from(array.as_array().iter().copied().collect::<Vec<_>>());
        EventBatch::new_with_len(schema, p4_columns, scalar_columns, Some(weights), length)
            .map_err(io_error)
    }

    fn format_spec(
        py: Python<'_>,
        name: &str,
        extension: &str,
        adapter: &Bound<'_, Adapter>,
    ) -> PyResult<Py<PyAny>> {
        let kwargs = PyDict::new(py);
        kwargs.set_item("name", name)?;
        kwargs.set_item("extension", extension)?;
        if adapter.borrow().source.is_some() {
            kwargs.set_item("describe", adapter.getattr("describe")?)?;
            kwargs.set_item("read_batches", adapter.getattr("read_batches")?)?;
        }
        if adapter.borrow().sink.is_some() {
            kwargs.set_item("write_batches", adapter.getattr("write_batches")?)?;
        }
        Ok(py
            .import("laddu.io")?
            .getattr("FormatSpec")?
            .call((), Some(&kwargs))?
            .unbind())
    }

    fn build_format(
        py: Python<'_>,
        name: &str,
        extension: &str,
        source: Option<SourceConfig>,
        sink: Option<Sink>,
    ) -> PyResult<Py<PyAny>> {
        let adapter = Py::new(py, Adapter { source, sink })?;
        format_spec(py, name, extension, adapter.bind(py))
    }

    fn read_dataset(
        py: Python<'_>,
        path: PathBuf,
        source: SourceConfig,
        name: &str,
        extension: &str,
        cache: &str,
        execution: Option<&Bound<'_, PyAny>>,
    ) -> PyResult<Py<PyAny>> {
        let spec = build_format(py, name, extension, Some(source), None)?;
        let kwargs = PyDict::new(py);
        kwargs.set_item("cache", cache)?;
        if let Some(execution) = execution {
            kwargs.set_item("execution", execution)?;
        }
        Ok(spec
            .bind(py)
            .call_method("read", (path,), Some(&kwargs))?
            .unbind())
    }

    #[allow(clippy::too_many_arguments)]
    fn format_sink(
        py: Python<'_>,
        path: PathBuf,
        sink: Sink,
        name: &str,
        extension: &str,
        output: &str,
        chunk_size: Option<usize>,
        execution: Option<&Bound<'_, PyAny>>,
    ) -> PyResult<Py<PyAny>> {
        let spec = build_format(py, name, extension, None, Some(sink))?;
        let kwargs = PyDict::new(py);
        kwargs.set_item("output", output)?;
        kwargs.set_item("chunk_size", chunk_size)?;
        if let Some(execution) = execution {
            kwargs.set_item("execution", execution)?;
        }
        Ok(spec
            .bind(py)
            .call_method("sink", (path,), Some(&kwargs))?
            .unbind())
    }

    #[allow(clippy::too_many_arguments)]
    fn write_dataset(
        py: Python<'_>,
        dataset: &Bound<'_, PyAny>,
        path: PathBuf,
        sink: Sink,
        name: &str,
        extension: &str,
        output: &str,
        chunk_size: Option<usize>,
        execution: Option<&Bound<'_, PyAny>>,
    ) -> PyResult<Py<PyAny>> {
        let sink = format_sink(
            py, path, sink, name, extension, output, chunk_size, execution,
        )?;
        Ok(dataset.call_method1("write_to", (sink.bind(py),))?.unbind())
    }

    pub(crate) fn write_hddm(
        py: Python<'_>,
        dataset: &Bound<'_, PyAny>,
        path: PathBuf,
        config: GlueXHddmConfig,
        output: &str,
        chunk_size: Option<usize>,
        execution: Option<&Bound<'_, PyAny>>,
    ) -> PyResult<WriteResult> {
        write_dataset(
            py,
            dataset,
            path,
            Sink::Hddm(config),
            "gluex-hddm",
            "hddm",
            output,
            chunk_size,
            execution,
        )
        .map(WriteResult)
    }

    fn fsroot_config(
        particles: usize,
        final_state_names: Option<Vec<String>>,
        tree: &str,
        prefix: &str,
        weight_branch: &str,
    ) -> PyResult<Sink> {
        Ok(Sink::FsRoot {
            names: labels(particles, final_state_names)?,
            tree: tree.into(),
            prefix: prefix.into(),
            weight_branch: weight_branch.into(),
        })
    }

    fn fsroot_source_config(
        names: Vec<String>,
        tree: &str,
        prefix: &str,
        weight_branch: &str,
    ) -> SourceConfig {
        SourceConfig::FsRoot {
            names,
            tree: tree.into(),
            prefix: prefix.into(),
            weight_branch: weight_branch.into(),
        }
    }

    fn pawian_config(
        particles: usize,
        final_state_names: Option<Vec<String>>,
        weights: bool,
        polarization: bool,
        order: &str,
        unit: &str,
    ) -> PyResult<Sink> {
        Ok(Sink::Pawian {
            names: labels(particles, final_state_names)?,
            weights,
            polarization,
            order: self::order(order)?,
            unit: self::unit(unit)?,
        })
    }

    fn pawian_source_config(
        names: Vec<String>,
        weights: bool,
        polarization: bool,
        order: PawianOrder,
        unit: PawianUnit,
    ) -> SourceConfig {
        SourceConfig::Pawian {
            names,
            weights,
            polarization,
            order,
            unit,
        }
    }

    /// AmpTools reader specification for laddu's external format interface.
    #[pyfunction]
    #[pyo3(signature = (*, particles, final_state_names=None, tree="kin", pol_in_beam=false))]
    fn amptools_format(
        py: Python<'_>,
        particles: usize,
        final_state_names: Option<Vec<String>>,
        tree: &str,
        pol_in_beam: bool,
    ) -> PyResult<FormatSpecResult> {
        let source = SourceConfig::AmpTools {
            names: labels(particles, final_state_names)?,
            tree: tree.into(),
            pol_in_beam,
        };
        build_format(py, "amptools", "root", Some(source), None).map(FormatSpecResult)
    }

    /// FSRoot reader and writer specification for laddu datasets.
    #[pyfunction]
    #[pyo3(signature = (*, particles, final_state_names=None, tree="nt", prefix="", weight_branch="weight"))]
    fn fsroot_format(
        py: Python<'_>,
        particles: usize,
        final_state_names: Option<Vec<String>>,
        tree: &str,
        prefix: &str,
        weight_branch: &str,
    ) -> PyResult<FormatSpecResult> {
        let names = labels(particles, final_state_names)?;
        build_format(
            py,
            "fsroot",
            "root",
            Some(fsroot_source_config(
                names.clone(),
                tree,
                prefix,
                weight_branch,
            )),
            Some(fsroot_config(
                particles,
                Some(names),
                tree,
                prefix,
                weight_branch,
            )?),
        )
        .map(FormatSpecResult)
    }

    /// Pawian reader and writer specification for laddu datasets.
    #[pyfunction]
    #[pyo3(signature = (*, particles, final_state_names=None, weights=false, polarization=false, order="px_py_pz_e", unit="GeV"))]
    fn pawian_format(
        py: Python<'_>,
        particles: usize,
        final_state_names: Option<Vec<String>>,
        weights: bool,
        polarization: bool,
        order: &str,
        unit: &str,
    ) -> PyResult<FormatSpecResult> {
        let names = labels(particles, final_state_names)?;
        let order = self::order(order)?;
        let unit = self::unit(unit)?;
        build_format(
            py,
            "pawian",
            "dat",
            Some(pawian_source_config(
                names.clone(),
                weights,
                polarization,
                order,
                unit,
            )),
            Some(Sink::Pawian {
                names,
                weights,
                polarization,
                order,
                unit,
            }),
        )
        .map(FormatSpecResult)
    }

    /// Configure an FSRoot sink for laddu.Dataset.write_to.
    #[pyfunction]
    #[pyo3(signature = (path, *, particles, final_state_names=None, tree="nt", prefix="", weight_branch="weight", output="auto", chunk_size=None, execution=None))]
    fn fsroot_sink(
        py: Python<'_>,
        path: PathBuf,
        particles: usize,
        final_state_names: Option<Vec<String>>,
        tree: &str,
        prefix: &str,
        weight_branch: &str,
        output: &str,
        chunk_size: Option<usize>,
        execution: Option<ExecutionInput>,
    ) -> PyResult<FormatSinkResult> {
        format_sink(
            py,
            path,
            fsroot_config(particles, final_state_names, tree, prefix, weight_branch)?,
            "fsroot",
            "root",
            output,
            chunk_size,
            execution.as_ref().map(|value| value.0.bind(py)),
        )
        .map(FormatSinkResult)
    }

    /// Configure a Pawian sink for laddu.Dataset.write_to.
    #[pyfunction]
    #[pyo3(signature = (path, *, particles, final_state_names=None, weights=false, polarization=false, order="px_py_pz_e", unit="GeV", output="auto", chunk_size=None, execution=None))]
    fn pawian_sink(
        py: Python<'_>,
        path: PathBuf,
        particles: usize,
        final_state_names: Option<Vec<String>>,
        weights: bool,
        polarization: bool,
        order: &str,
        unit: &str,
        output: &str,
        chunk_size: Option<usize>,
        execution: Option<ExecutionInput>,
    ) -> PyResult<FormatSinkResult> {
        format_sink(
            py,
            path,
            pawian_config(
                particles,
                final_state_names,
                weights,
                polarization,
                order,
                unit,
            )?,
            "pawian",
            "dat",
            output,
            chunk_size,
            execution.as_ref().map(|value| value.0.bind(py)),
        )
        .map(FormatSinkResult)
    }

    /// Read an AmpTools kin tree into a laddu dataset.
    #[pyfunction]
    #[pyo3(signature = (path, *, particles, final_state_names=None, tree="kin", pol_in_beam=false, cache="fastest", execution=None))]
    fn read_amptools(
        py: Python<'_>,
        path: PathBuf,
        particles: usize,
        final_state_names: Option<Vec<String>>,
        tree: &str,
        pol_in_beam: bool,
        cache: &str,
        execution: Option<ExecutionInput>,
    ) -> PyResult<DatasetResult> {
        let source = SourceConfig::AmpTools {
            names: labels(particles, final_state_names)?,
            tree: tree.into(),
            pol_in_beam,
        };
        read_dataset(
            py,
            path,
            source,
            "amptools",
            "root",
            cache,
            execution.as_ref().map(|value| value.0.bind(py)),
        )
        .map(DatasetResult)
    }

    /// Read an FSRoot nt tree into a laddu dataset.
    #[pyfunction]
    #[pyo3(signature = (path, *, particles, final_state_names=None, tree="nt", prefix="", weight_branch="weight", cache="fastest", execution=None))]
    fn read_fsroot(
        py: Python<'_>,
        path: PathBuf,
        particles: usize,
        final_state_names: Option<Vec<String>>,
        tree: &str,
        prefix: &str,
        weight_branch: &str,
        cache: &str,
        execution: Option<ExecutionInput>,
    ) -> PyResult<DatasetResult> {
        let source = fsroot_source_config(
            labels(particles, final_state_names)?,
            tree,
            prefix,
            weight_branch,
        );
        read_dataset(
            py,
            path,
            source,
            "fsroot",
            "root",
            cache,
            execution.as_ref().map(|value| value.0.bind(py)),
        )
        .map(DatasetResult)
    }

    /// Read Pawian text events into a laddu dataset.
    #[pyfunction]
    #[pyo3(signature = (path, *, particles, final_state_names=None, weights=false, polarization=false, order="px_py_pz_e", unit="GeV", cache="fastest", execution=None))]
    fn read_pawian(
        py: Python<'_>,
        path: PathBuf,
        particles: usize,
        final_state_names: Option<Vec<String>>,
        weights: bool,
        polarization: bool,
        order: &str,
        unit: &str,
        cache: &str,
        execution: Option<ExecutionInput>,
    ) -> PyResult<DatasetResult> {
        let source = pawian_source_config(
            labels(particles, final_state_names)?,
            weights,
            polarization,
            self::order(order)?,
            self::unit(unit)?,
        );
        read_dataset(
            py,
            path,
            source,
            "pawian",
            "dat",
            cache,
            execution.as_ref().map(|value| value.0.bind(py)),
        )
        .map(DatasetResult)
    }

    /// Write a laddu dataset to an FSRoot tree.
    #[pyfunction]
    #[pyo3(signature = (dataset, path, *, particles, final_state_names=None, tree="nt", prefix="", weight_branch="weight", output="auto", chunk_size=None, execution=None))]
    fn write_fsroot(
        py: Python<'_>,
        dataset: DatasetInput,
        path: PathBuf,
        particles: usize,
        final_state_names: Option<Vec<String>>,
        tree: &str,
        prefix: &str,
        weight_branch: &str,
        output: &str,
        chunk_size: Option<usize>,
        execution: Option<ExecutionInput>,
    ) -> PyResult<WriteResult> {
        write_dataset(
            py,
            dataset.0.bind(py),
            path,
            fsroot_config(particles, final_state_names, tree, prefix, weight_branch)?,
            "fsroot",
            "root",
            output,
            chunk_size,
            execution.as_ref().map(|value| value.0.bind(py)),
        )
        .map(WriteResult)
    }

    /// Write a laddu dataset in Pawian's default text layout.
    #[pyfunction]
    #[pyo3(signature = (dataset, path, *, particles, final_state_names=None, weights=false, polarization=false, order="px_py_pz_e", unit="GeV", output="auto", chunk_size=None, execution=None))]
    fn write_pawian(
        py: Python<'_>,
        dataset: DatasetInput,
        path: PathBuf,
        particles: usize,
        final_state_names: Option<Vec<String>>,
        weights: bool,
        polarization: bool,
        order: &str,
        unit: &str,
        output: &str,
        chunk_size: Option<usize>,
        execution: Option<ExecutionInput>,
    ) -> PyResult<WriteResult> {
        write_dataset(
            py,
            dataset.0.bind(py),
            path,
            pawian_config(
                particles,
                final_state_names,
                weights,
                polarization,
                order,
                unit,
            )?,
            "pawian",
            "dat",
            output,
            chunk_size,
            execution.as_ref().map(|value| value.0.bind(py)),
        )
        .map(WriteResult)
    }
}
