#[pyo3::pymodule(submodule)]
pub(crate) mod generation {
    use std::{collections::HashMap, path::PathBuf, sync::Arc};

    use crate::{
        Particle,
        generation::{GlueXHddmConfig, HddmSink},
    };
    use laddu::prelude::{Dataset, ParquetSource, RealVec3};
    use pyo3::{
        exceptions::{PyRuntimeError, PyValueError},
        prelude::*,
        types::PyAnyMethods,
    };

    fn generation_error(error: impl std::fmt::Display) -> PyErr {
        PyRuntimeError::new_err(error.to_string())
    }

    fn particle_from_channel(channel: &Bound<'_, PyAny>, label: &str) -> PyResult<Particle> {
        let particle = channel.call_method1("particle", (label,))?;
        let ids: HashMap<String, i64> = particle.getattr("ids")?.extract()?;
        let pdg = ids.get("pdg").copied().ok_or_else(|| {
            PyValueError::new_err(format!(
                "laddu channel particle {label:?} has no numeric 'pdg' identifier"
            ))
        })?;
        let pdg = isize::try_from(pdg)
            .map_err(|_| PyValueError::new_err(format!("PDG identifier {pdg} is out of range")))?;
        let particle = Particle::from_pdg(pdg);
        if particle.is_unknown() && pdg != 0 {
            return Err(PyValueError::new_err(format!(
                "PDG identifier {pdg} for channel particle {label:?} is not known to GlueX"
            )));
        }
        Ok(particle)
    }

    fn config_from_channel(
        channel: &Bound<'_, PyAny>,
        beam_label: &str,
        target_label: &str,
    ) -> PyResult<GlueXHddmConfig> {
        let edge_names: Vec<String> = channel.getattr("edge_names")?.extract()?;
        let particles = edge_names
            .into_iter()
            .map(|label| {
                particle_from_channel(channel, &label)
                    .map(|particle| (Arc::<str>::from(label), particle))
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
