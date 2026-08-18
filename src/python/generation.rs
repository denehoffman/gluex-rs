#[pyo3::pymodule(submodule)]
pub(crate) mod generation {
    use std::{collections::HashMap, path::PathBuf, sync::Arc};

    use crate::generation::{
        GlueXHddmConfig, HddmSink,
        config::{GenerationConfig, ScalarDistribution},
        species::gluex_particle_from_external_ids,
    };
    use laddu::prelude::{Channel, CompiledModel, Dataset, ParquetSource, RealVec3};
    use pyo3::{
        exceptions::{PyRuntimeError, PyValueError},
        prelude::*,
        types::{PyAnyMethods, PyDict, PyDictMethods},
    };
    use serde::Serialize;

    fn generation_error(error: impl std::fmt::Display) -> PyErr {
        PyRuntimeError::new_err(error.to_string())
    }

    fn channel_from_python(channel: &Bound<'_, PyAny>) -> PyResult<Channel> {
        let channel_json: String = channel.call_method0("to_json")?.extract()?;
        serde_json::from_str(&channel_json).map_err(generation_error)
    }

    fn model_from_python(model: &Bound<'_, PyAny>) -> PyResult<CompiledModel> {
        let model_json: String = model.call_method0("to_json")?.extract()?;
        serde_json::from_str(&model_json).map_err(generation_error)
    }

    fn model_parameters(
        model: &Bound<'_, PyAny>,
        parameters: Option<&Bound<'_, PyAny>>,
    ) -> PyResult<Vec<f64>> {
        let names: Vec<String> = model.getattr("parameter_names")?.extract()?;
        let mut values: Vec<f64> = model.getattr("default_parameters")?.extract()?;
        let Some(parameters) = parameters else {
            return Ok(values);
        };
        if let Ok(mapping) = parameters.cast::<PyDict>() {
            for (index, name) in names.iter().enumerate() {
                if let Some(value) = mapping.get_item(name)? {
                    values[index] = value.extract()?;
                }
            }
            return Ok(values);
        }
        parameters.extract()
    }

    fn serialize_config(config: &GenerationConfig, indent: usize) -> PyResult<String> {
        let mut output = Vec::new();
        let indent = vec![b' '; indent];
        let formatter = serde_json::ser::PrettyFormatter::with_indent(&indent);
        let mut serializer = serde_json::Serializer::with_formatter(&mut output, formatter);
        config
            .serialize(&mut serializer)
            .map_err(generation_error)?;
        String::from_utf8(output).map_err(generation_error)
    }

    /// Distribution for an additional generated scalar branch.
    #[pyclass(
        name = "Scalar",
        module = "gluex.generation",
        frozen,
        skip_from_py_object
    )]
    #[derive(Clone)]
    pub struct PyScalar(ScalarDistribution);

    #[pymethods]
    impl PyScalar {
        /// Construct a scalar with one fixed value.
        #[staticmethod]
        fn fixed(value: f64) -> Self {
            Self(ScalarDistribution::Fixed { value })
        }

        /// Construct a scalar uniformly distributed on ``[low, high)``.
        #[staticmethod]
        fn uniform(low: f64, high: f64) -> Self {
            Self(ScalarDistribution::Uniform {
                min: low,
                max: high,
            })
        }

        /// Construct a piecewise-constant histogram scalar.
        #[staticmethod]
        fn histogram(edges: Vec<f64>, weights: Vec<f64>) -> Self {
            Self(ScalarDistribution::Histogram { edges, weights })
        }
    }

    fn config_from_python(
        channel: &Bound<'_, PyAny>,
        model: Option<&Bound<'_, PyAny>>,
        parameters: Option<&Bound<'_, PyAny>>,
        scalars: Option<&Bound<'_, PyDict>>,
        max_weight: Option<f64>,
        pilot_proposals: usize,
        safety_scale: f64,
        schema: Option<String>,
    ) -> PyResult<GenerationConfig> {
        if model.is_none() && parameters.is_some() {
            return Err(PyValueError::new_err("parameters require a model"));
        }
        let native = channel_from_python(channel)?;
        let mut config = GenerationConfig::try_from_channel(&native).map_err(generation_error)?;
        config.schema = schema;
        config.model = model.map(model_from_python).transpose()?;
        config.parameters = model
            .map(|model| model_parameters(model, parameters))
            .transpose()?;
        config.generation.max_weight = max_weight;
        config.generation.pilot_proposals = pilot_proposals;
        config.generation.safety_scale = safety_scale;
        if let Some(scalars) = scalars {
            for (name, source) in scalars {
                let name: String = name.extract()?;
                let source = source.extract::<PyRef<'_, PyScalar>>()?;
                config.scalars.insert(name, source.0.clone());
            }
        }
        config.validate().map_err(generation_error)?;
        Ok(config)
    }

    /// Python-authored standalone generation configuration.
    #[pyclass(
        name = "GenerationConfig",
        module = "gluex.generation",
        skip_from_py_object
    )]
    #[derive(Clone)]
    pub struct PyGenerationConfig(GenerationConfig);

    #[pymethods]
    impl PyGenerationConfig {
        /// Build a manifest from a Laddu channel and optional model.
        #[new]
        #[pyo3(signature = (
            channel: "laddu.Channel",
            *,
            model: "laddu.Model | None" = None,
            parameters: "Sequence[float] | dict[str, float] | None" = None,
            scalars: "dict[str, Scalar] | None" = None,
            max_weight=None,
            pilot_proposals=10_000,
            safety_scale=2.0,
            schema=None
        ))]
        #[allow(clippy::too_many_arguments)]
        fn new(
            channel: &Bound<'_, PyAny>,
            model: Option<&Bound<'_, PyAny>>,
            parameters: Option<&Bound<'_, PyAny>>,
            scalars: Option<&Bound<'_, PyDict>>,
            max_weight: Option<f64>,
            pilot_proposals: usize,
            safety_scale: f64,
            schema: Option<String>,
        ) -> PyResult<Self> {
            config_from_python(
                channel,
                model,
                parameters,
                scalars,
                max_weight,
                pilot_proposals,
                safety_scale,
                schema,
            )
            .map(Self)
        }

        /// Add or replace a named scalar branch.
        fn add_scalar(&mut self, name: String, source: &PyScalar) -> PyResult<()> {
            let mut updated = self.0.clone();
            updated.scalars.insert(name, source.0.clone());
            updated.validate().map_err(generation_error)?;
            self.0 = updated;
            Ok(())
        }

        /// Validate the complete configuration.
        fn validate(&self) -> PyResult<()> {
            self.0.validate_execution().map_err(generation_error)
        }

        /// Serialize the execution manifest as JSON.
        #[pyo3(signature = (*, indent=2))]
        fn to_json(&self, indent: usize) -> PyResult<String> {
            serialize_config(&self.0, indent)
        }

        /// Write the execution manifest to a JSON file.
        #[pyo3(signature = (path, *, indent=2))]
        fn write(&self, path: PathBuf, indent: usize) -> PyResult<()> {
            let json = serialize_config(&self.0, indent)?;
            std::fs::write(path, format!("{json}\n")).map_err(generation_error)
        }

        /// Optional manually supplied maximum event weight.
        #[getter]
        fn max_weight(&self) -> Option<f64> {
            self.0.generation.max_weight
        }

        /// Set an optional manually supplied maximum event weight.
        #[setter]
        fn set_max_weight(&mut self, value: Option<f64>) -> PyResult<()> {
            let mut updated = self.0.clone();
            updated.generation.max_weight = value;
            updated.validate().map_err(generation_error)?;
            self.0 = updated;
            Ok(())
        }

        /// Number of proposals used for model-backed envelope estimation.
        #[getter]
        fn pilot_proposals(&self) -> usize {
            self.0.generation.pilot_proposals
        }

        /// Set the number of proposals used for model-backed envelope estimation.
        #[setter]
        fn set_pilot_proposals(&mut self, value: usize) -> PyResult<()> {
            let mut updated = self.0.clone();
            updated.generation.pilot_proposals = value;
            updated.validate().map_err(generation_error)?;
            self.0 = updated;
            Ok(())
        }

        /// Safety factor applied to pilot and grown envelopes.
        #[getter]
        fn safety_scale(&self) -> f64 {
            self.0.generation.safety_scale
        }

        /// Set the safety factor applied to pilot and grown envelopes.
        #[setter]
        fn set_safety_scale(&mut self, value: f64) -> PyResult<()> {
            let mut updated = self.0.clone();
            updated.generation.safety_scale = value;
            updated.validate().map_err(generation_error)?;
            self.0 = updated;
            Ok(())
        }
    }

    /// Convert a supported laddu channel to strict standalone-generation JSON.
    #[pyfunction]
    #[pyo3(signature = (
        channel: "laddu.Channel",
        *,
        model: "laddu.Model | None" = None,
        parameters: "Sequence[float] | dict[str, float] | None" = None,
        scalars: "dict[str, Scalar] | None" = None,
        max_weight=None,
        pilot_proposals=10_000,
        safety_scale=2.0,
        schema=None,
        indent=2
    ))]
    #[allow(clippy::too_many_arguments)]
    pub fn config_json(
        channel: &Bound<'_, PyAny>,
        model: Option<&Bound<'_, PyAny>>,
        parameters: Option<&Bound<'_, PyAny>>,
        scalars: Option<&Bound<'_, PyDict>>,
        max_weight: Option<f64>,
        pilot_proposals: usize,
        safety_scale: f64,
        schema: Option<String>,
        indent: usize,
    ) -> PyResult<String> {
        let config = config_from_python(
            channel,
            model,
            parameters,
            scalars,
            max_weight,
            pilot_proposals,
            safety_scale,
            schema,
        )?;
        serialize_config(&config, indent)
    }

    fn config_from_channel(
        channel: &Bound<'_, PyAny>,
        beam_label: &str,
        target_label: &str,
    ) -> PyResult<GlueXHddmConfig> {
        let channel = channel_from_python(channel)?;
        let particles = channel
            .edges()
            .filter_map(|edge| {
                edge.properties().map(|properties| {
                    gluex_particle_from_external_ids(properties.ids())
                        .map(|particle| (Arc::<str>::from(edge.name()), particle))
                        .map_err(|error| {
                            generation_error(format!("channel edge {:?}: {error}", edge.name()))
                        })
                })
            })
            .collect::<PyResult<HashMap<_, _>>>()?;
        GlueXHddmConfig::from_particles(
            Arc::<str>::from(beam_label),
            Arc::<str>::from(target_label),
            particles,
        )
        .map_err(generation_error)
    }

    /// Configuration for streaming a laddu dataset to GlueX HDDM.
    #[pyclass(
        name = "GlueXHddmConfig",
        module = "gluex.generation",
        skip_from_py_object
    )]
    #[derive(Clone)]
    pub struct PyGlueXHddmConfig(GlueXHddmConfig);

    #[pymethods]
    impl PyGlueXHddmConfig {
        #[new]
        #[pyo3(signature = (
            channel: "laddu.Channel",
            *,
            beam="beam",
            target="target",
            run_number=0,
            first_event_number=0,
            random_seed=0,
            vertex=(0.0, 0.0, 0.0)
        ))]
        fn new(
            channel: &Bound<'_, PyAny>,
            beam: &str,
            target: &str,
            run_number: i64,
            first_event_number: i32,
            random_seed: u64,
            vertex: (f64, f64, f64),
        ) -> PyResult<Self> {
            Ok(Self(
                config_from_channel(channel, beam, target)?
                    .with_run_number(run_number)
                    .with_event_number(first_event_number)
                    .with_random_seed(random_seed)
                    .with_vertex(RealVec3::new(vertex.0, vertex.1, vertex.2)),
            ))
        }

        #[getter]
        fn beam(&self) -> &str {
            self.0.beam_label()
        }

        #[getter]
        fn target(&self) -> &str {
            self.0.target_label()
        }
    }

    /// Writer for streaming a laddu dataset to a GlueX HDDM file.
    #[pyclass(name = "GlueXHddmWriter", module = "gluex.generation")]
    pub struct PyGlueXHddmWriter(GlueXHddmConfig);

    #[pymethods]
    impl PyGlueXHddmWriter {
        #[new]
        fn new(config: &PyGlueXHddmConfig) -> Self {
            Self(config.0.clone())
        }

        /// Write every event in a laddu dataset to `path`.
        #[pyo3(signature = (dataset: "laddu.Dataset", path))]
        fn write(&self, py: Python<'_>, dataset: &Bound<'_, PyAny>, path: PathBuf) -> PyResult<()> {
            // Bridge through Parquet because laddu's Python Dataset does not expose its
            // native Dataset to other extension modules.
            let temp_dir = tempfile::tempdir().map_err(generation_error)?;
            let parquet_path = temp_dir.path().join("events.parquet");
            let laddu = py.import("laddu")?;
            let parquet_sink = laddu
                .getattr("ParquetSink")?
                .call1((parquet_path.to_string_lossy().as_ref(),))?;
            dataset.call_method1("write_to", (parquet_sink,))?;

            let config = self.0.clone();
            py.detach(move || {
                let source = ParquetSource::open(parquet_path.to_string_lossy())
                    .map_err(generation_error)?;
                let dataset = Dataset::new(source);
                let mut sink = HddmSink::new(path, config).map_err(generation_error)?;
                dataset.write_to(&mut sink).map_err(generation_error)?;
                drop(temp_dir);
                Ok(())
            })
        }
    }
}
