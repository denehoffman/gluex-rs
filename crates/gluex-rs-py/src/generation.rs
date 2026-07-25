#[pyo3::pymodule(submodule)]
pub(crate) mod generation {
    use std::path::PathBuf;

    use ::gluex_rs::generation::{
        GeneratedBatchColumns, GeneratedParticleColumn, GlueXGenerationError, GlueXHddmConfig,
        GlueXHddmWriter,
    };
    use laddu::{ParticleSpecies, Vec3, Vec4};
    use numpy::{PyReadonlyArray1, PyReadonlyArray2};
    use pyo3::{
        exceptions::{PyRuntimeError, PyTypeError, PyValueError},
        prelude::*,
        types::PyAnyMethods,
    };

    fn py_generation_error(error: GlueXGenerationError) -> PyErr {
        PyRuntimeError::new_err(error.to_string())
    }

    fn parse_species(particle: &Bound<'_, PyAny>) -> PyResult<Option<ParticleSpecies>> {
        let species = particle.getattr("species")?;
        if species.is_none() {
            return Ok(None);
        }
        let id: Option<i64> = species.getattr("id")?.extract()?;
        if let Some(id) = id {
            let namespace: Option<String> = species.getattr("namespace")?.extract()?;
            return Ok(Some(namespace.map_or_else(
                || ParticleSpecies::code(id),
                |namespace| ParticleSpecies::with_namespace(namespace, id),
            )));
        }
        let label: Option<String> = species.getattr("label_value")?.extract()?;
        label.map(ParticleSpecies::label).map(Some).ok_or_else(|| {
            PyValueError::new_err("generated particle species has neither a numeric ID nor label")
        })
    }

    fn parse_p4_column(dataset: &Bound<'_, PyAny>, label: &str) -> PyResult<Vec<Vec4>> {
        let array: PyReadonlyArray2<'_, f64> = dataset
            .call_method1("p4_column_global", (label,))?
            .extract()
            .map_err(|_| {
                PyTypeError::new_err(format!(
                    "laddu Dataset.p4_column_global('{label}') must return a float64 two-dimensional NumPy array"
                ))
            })?;
        let values = array.as_array();
        if values.ncols() != 4 {
            return Err(PyValueError::new_err(format!(
                "laddu p4 column '{label}' has {} components per row; expected (px, py, pz, e)",
                values.ncols()
            )));
        }
        Ok(values
            .outer_iter()
            .map(|row| Vec4::new(row[0], row[1], row[2], row[3]))
            .collect())
    }

    fn parse_batch_columns(batch: &Bound<'_, PyAny>) -> PyResult<GeneratedBatchColumns> {
        let dataset = batch.getattr("dataset").map_err(|_| {
            PyTypeError::new_err("expected a laddu.GeneratedBatch with a dataset attribute")
        })?;
        let layout = batch.getattr("layout").map_err(|_| {
            PyTypeError::new_err("expected a laddu.GeneratedBatch with a layout attribute")
        })?;
        let weights: PyReadonlyArray1<'_, f64> = dataset
            .getattr("weights_global")?
            .extract()
            .map_err(|_| {
                PyTypeError::new_err(
                    "laddu Dataset.weights_global must return a float64 one-dimensional NumPy array",
                )
            })?;
        let weights = weights.as_slice()?.to_vec();
        let mut particles = Vec::new();
        for particle in layout.getattr("particles")?.try_iter()? {
            let particle = particle?;
            let id: String = particle.getattr("id")?.extract()?;
            let p4_label: Option<String> = particle.getattr("p4_label")?.extract()?;
            let p4 = p4_label
                .as_deref()
                .map(|label| parse_p4_column(&dataset, label))
                .transpose()?;
            particles.push(GeneratedParticleColumn::new(
                id,
                parse_species(&particle)?,
                p4,
            ));
        }
        GeneratedBatchColumns::new(particles, weights).map_err(py_generation_error)
    }

    /// Configuration for writing generated `laddu` event batches as GlueX HDDM.
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
        #[pyo3(signature = (beam_id, target_id, *, run_number=0, first_event_number=0, random_seed=0, vertex=(0.0, 0.0, 0.0)))]
        fn new(
            beam_id: String,
            target_id: String,
            run_number: i32,
            first_event_number: i32,
            random_seed: u64,
            vertex: (f64, f64, f64),
        ) -> Self {
            Self(
                GlueXHddmConfig::new(beam_id, target_id)
                    .with_run_number(run_number)
                    .with_first_event_number(first_event_number)
                    .with_random_seed(random_seed)
                    .with_vertex(Vec3::new(vertex.0, vertex.1, vertex.2)),
            )
        }

        #[getter]
        fn beam_id(&self) -> &str {
            self.0.beam_id()
        }

        #[getter]
        fn target_id(&self) -> &str {
            self.0.target_id()
        }
    }

    /// Writer for converting generated `laddu` batches to GlueX HDDM files.
    #[pyclass(name = "GlueXHddmWriter", module = "gluex.generation")]
    pub struct PyGlueXHddmWriter(GlueXHddmWriter);

    #[pymethods]
    impl PyGlueXHddmWriter {
        #[new]
        fn new(config: &PyGlueXHddmConfig) -> Self {
            Self(GlueXHddmWriter::new(config.0.clone()))
        }

        /// Write one generated batch to a new HDDM file.
        fn write_batch(&self, batch: &Bound<'_, PyAny>, path: PathBuf) -> PyResult<usize> {
            self.0
                .write_batch_columns(&parse_batch_columns(batch)?, path)
                .map_err(py_generation_error)
        }

        /// Append one generated batch to an existing HDDM file.
        fn append_batch(
            &self,
            batch: &Bound<'_, PyAny>,
            path: PathBuf,
            start_event: usize,
        ) -> PyResult<usize> {
            self.0
                .append_batch_columns(&parse_batch_columns(batch)?, path, start_event)
                .map_err(py_generation_error)
        }

        /// Write generated batches in iterator order, holding one batch at a time.
        fn write_batches(&self, batches: &Bound<'_, PyAny>, path: PathBuf) -> PyResult<()> {
            let mut batches = batches.try_iter()?;
            let first = batches
                .next()
                .transpose()?
                .ok_or_else(|| PyValueError::new_err("at least one generated batch is required"))?;
            let mut event_number = self.write_batch(&first, path.clone())?;
            for batch in batches {
                event_number = self.append_batch(&batch?, path.clone(), event_number)?;
            }
            Ok(())
        }
    }
}
