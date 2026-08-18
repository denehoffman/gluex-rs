use std::{collections::HashMap, path::Path, sync::Arc};

use crate::core::{Particle as GlueXParticle, RunNumber};
use fastrand::Rng;
use hddm::HddmFileWriter;
use laddu::{
    LadduDataError,
    data::{Name, RealVec4, io::EventSink},
    physics::{
        channel::{Channel, EdgeHandle},
        vectors::RealVec3,
    },
    prelude::{EventBatch, LadduDataResult, Schema, WritePlan},
};
use thiserror::Error;

use crate::generation::hddm_s::{
    Beam, Hddm, Momentum, Origin, PhysicsEvent, Product, Properties, Random, Reaction, Target,
    Vertex,
};

/// Strict standalone-generation configuration and validation.
pub mod config;
pub(crate) mod hddm_s;
pub mod species;

/// A result type for `GlueX` Monte Carlo generators.
pub type GlueXGenerationResult<T> = Result<T, GlueXGenerationError>;

/// An error type for `GlueX` Monte Carlo generators.
#[derive(Debug, Error)]
pub enum GlueXGenerationError {
    /// A catch-all error variant.
    #[error("{0}")]
    Custom(String),
}

/// Configuration for a `GlueX` HDDM sink.
#[derive(Clone, Debug)]
pub struct GlueXHddmConfig {
    beam_label: Name,
    target_label: Name,
    particles: HashMap<Name, GlueXParticle>,
    run_number: RunNumber,
    first_event_number: i32,
    random_seed: u64,
    vertex: RealVec3,
}

/// Adds `GlueX` labeling information to channel [`EdgeHandle`] values.
pub trait GlueXIdExt {
    /// Set the "gluex" namespaced [`ExternalId`](`laddu::physics::quantum::ExternalId`) to "beam".
    ///
    /// # Errors
    ///
    /// Returns an error when the selected edge has no particle properties.
    fn set_beam_id(&mut self) -> GlueXGenerationResult<&mut Self>;
    /// Set the "gluex" namespaced [`ExternalId`](`laddu::physics::quantum::ExternalId`) to "target".
    ///
    /// # Errors
    ///
    /// Returns an error when the selected edge has no particle properties.
    fn set_target_id(&mut self) -> GlueXGenerationResult<&mut Self>;
}

impl GlueXIdExt for EdgeHandle<'_> {
    fn set_beam_id(&mut self) -> GlueXGenerationResult<&mut Self> {
        let properties = self
            .edge_mut()
            .properties()
            .ok_or_else(|| {
                GlueXGenerationError::Custom("Selected edge has no properties".to_string())
            })?
            .clone();
        Ok(self.properties(&properties.with_id("gluex", "beam")))
    }

    fn set_target_id(&mut self) -> GlueXGenerationResult<&mut Self> {
        let properties = self
            .edge_mut()
            .properties()
            .ok_or_else(|| {
                GlueXGenerationError::Custom("Selected edge has no properties".to_string())
            })?
            .clone();
        Ok(self.properties(&properties.with_id("gluex", "target")))
    }
}

impl GlueXHddmConfig {
    /// Construct a new configuration for a `GlueX` HDDM sink from a [`Channel`].
    ///
    /// # Errors
    ///
    /// Returns an error when the channel has no edges labeled as the `GlueX`
    /// beam and target.
    pub fn new(channel: &Channel) -> GlueXGenerationResult<Self> {
        let beam_label = Self::get_beam_name(channel)?;
        let target_label = Self::get_target_name(channel)?;
        let particles = channel
            .edges()
            .filter_map(|edge| {
                edge.properties()
                    .map(|properties| (Arc::from(edge.name()), properties.clone().into()))
            })
            .collect();
        Self::from_particles(beam_label, target_label, particles)
    }

    /// Construct a configuration from explicit particle-column metadata.
    ///
    /// This lower-level constructor is useful at foreign-function boundaries
    /// where a laddu [`Channel`] cannot be shared as a native Rust value.
    ///
    /// # Errors
    ///
    /// Returns an error when either initial-state label is absent from
    /// `particles`.
    pub fn from_particles(
        beam_label: impl Into<Name>,
        target_label: impl Into<Name>,
        particles: HashMap<Name, GlueXParticle>,
    ) -> GlueXGenerationResult<Self> {
        let beam_label = beam_label.into();
        let target_label = target_label.into();
        if !particles.contains_key(&beam_label) {
            return Err(GlueXGenerationError::Custom(format!(
                "beam particle column {beam_label:?} is not configured"
            )));
        }
        if !particles.contains_key(&target_label) {
            return Err(GlueXGenerationError::Custom(format!(
                "target particle column {target_label:?} is not configured"
            )));
        }
        Ok(Self {
            beam_label,
            target_label,
            particles,
            run_number: 0,
            first_event_number: 0,
            random_seed: 0,
            vertex: RealVec3::zero(),
        })
    }
    /// Set the configuration's run number.
    #[must_use]
    pub const fn with_run_number(mut self, run_number: RunNumber) -> Self {
        self.run_number = run_number;
        self
    }
    /// Set the initial event number.
    #[must_use]
    pub const fn with_event_number(mut self, first_event_number: i32) -> Self {
        self.first_event_number = first_event_number;
        self
    }
    /// Set the seed used for generating randoms.
    #[must_use]
    pub const fn with_random_seed(mut self, random_seed: u64) -> Self {
        self.random_seed = random_seed;
        self
    }
    /// Set the initial vertex position.
    ///
    /// # Note
    ///
    /// If you do not set this manually, it will default to `(0, 0, 0)`.
    /// This is interpreted by `GlueX`'s Monte Carlo pipeline as an indication
    /// that we want Geant to automatially determine the initial vertex.
    #[must_use]
    pub const fn with_vertex(mut self, vertex: RealVec3) -> Self {
        self.vertex = vertex;
        self
    }
    /// Name of the beam four-vector column.
    #[must_use]
    pub fn beam_label(&self) -> &str {
        &self.beam_label
    }
    /// Name of the target four-vector column.
    #[must_use]
    pub fn target_label(&self) -> &str {
        &self.target_label
    }
    fn get_beam_name(channel: &Channel) -> GlueXGenerationResult<Name> {
        for edge in channel.initial_edges() {
            if let Some(properties) = edge.properties()
                && let Some(id) = properties.id("gluex")
                && id.label_value() == Some("beam")
            {
                return Ok(Arc::from(edge.name()));
            }
        }
        Err(GlueXGenerationError::Custom(
            "Could not find any edge with id \"gluex\" -> \"beam\"".to_string(),
        ))
    }
    fn get_target_name(channel: &Channel) -> GlueXGenerationResult<Name> {
        for edge in channel.initial_edges() {
            if let Some(properties) = edge.properties()
                && let Some(id) = properties.id("gluex")
                && id.label_value() == Some("target")
            {
                return Ok(Arc::from(edge.name()));
            }
        }
        Err(GlueXGenerationError::Custom(
            "Could not find any edge with id \"gluex\" -> \"target\"".to_string(),
        ))
    }
}

/// A sink for writing `GlueX` HDDM files.
pub struct HddmSink {
    config: GlueXHddmConfig,
    beam: (usize, GlueXParticle),
    target: (usize, GlueXParticle),
    final_state: Vec<(usize, GlueXParticle)>,
    writer: HddmFileWriter,
    next_event_number: i32,
    rng: Rng,
}

impl HddmSink {
    /// Create a new HDDM sink from a path and [`GlueXHddmConfig`].
    ///
    /// # Errors
    ///
    /// Returns an error when the output file cannot be created.
    pub fn new(path: impl AsRef<Path>, config: GlueXHddmConfig) -> LadduDataResult<Self> {
        Ok(Self {
            beam: (0, GlueXParticle::default()),
            target: (1, GlueXParticle::default()),
            final_state: Vec::new(),
            writer: hddm_s::create(path).map_err(|e| LadduDataError::Sink(e.to_string()))?, // append mode and errors
            next_event_number: config.first_event_number,
            rng: Rng::with_seed(config.random_seed),
            config,
        })
    }
}

impl EventSink for HddmSink {
    fn begin(&mut self, schema: Arc<Schema>, _plan: WritePlan) -> LadduDataResult<()> {
        let p4_names = schema.p4s();
        if !p4_names.contains(&self.config.beam_label) {
            return Err(LadduDataError::Schema(format!(
                "No particle labeled \"{}\" was found in the generator schema!",
                self.config.beam_label
            )));
        }
        if !p4_names.contains(&self.config.target_label) {
            return Err(LadduDataError::Schema(format!(
                "No particle labeled \"{}\" was found in the generator schema!",
                self.config.target_label
            )));
        }
        for (i, p4_name) in p4_names.iter().enumerate() {
            let properties = self.config.particles.get(p4_name).copied().ok_or_else(|| {
                LadduDataError::Schema(format!(
                    "No GlueX particle mapping was configured for {p4_name:?}"
                ))
            })?;
            let index_particle = (i, properties);
            if *p4_name == self.config.beam_label {
                self.beam = index_particle;
            } else if *p4_name == self.config.target_label {
                self.target = index_particle;
            } else {
                self.final_state.push(index_particle);
            }
        }
        Ok(())
    }

    #[allow(clippy::cast_possible_truncation)]
    fn write_batch(&mut self, batch: &EventBatch) -> LadduDataResult<()> {
        let run_no = i32::try_from(self.config.run_number).map_err(|_| {
            LadduDataError::Sink(format!(
                "run number {} does not fit in the HDDM i32 field",
                self.config.run_number
            ))
        })?;
        for event in batch.iter() {
            let products = self
                .final_state
                .iter()
                .map(|(column, particle)| {
                    let id = i32::try_from(*column).map_err(|_| {
                        LadduDataError::Sink(format!(
                            "particle column index {column} does not fit in the HDDM i32 field"
                        ))
                    })?;
                    let pdgtype = i32::try_from(particle.to_pdg()).map_err(|_| {
                        LadduDataError::Sink(format!(
                            "PDG identifier {} does not fit in the HDDM i32 field",
                            particle.to_pdg()
                        ))
                    })?;
                    Ok(Product {
                        decay_vertex: 0,
                        id,
                        mech: 0,
                        parentid: 0,
                        pdgtype,
                        type_: particle.into(),
                        momentum: momentum(event.p4(*column)),
                        polarization: None,
                        properties: Some(properties(*particle)),
                    })
                })
                .collect::<LadduDataResult<Vec<_>>>()?;
            let record = Hddm {
                geometry: None,
                physics_event: vec![PhysicsEvent {
                    event_no: self.next_event_number,
                    run_no,
                    data_version_string: vec![],
                    ccdb_context: vec![],
                    reaction: vec![Reaction {
                        type_: 0,
                        weight: event.weight() as f32,
                        beam: Some(Beam {
                            type_: self.beam.1.into(),
                            momentum: momentum(event.p4(self.beam.0)),
                            polarization: None,
                            properties: properties(self.beam.1),
                        }),
                        target: Some(Target {
                            type_: self.target.1.into(),
                            momentum: momentum(event.p4(self.target.0)),
                            polarization: None,
                            properties: properties(self.target.1),
                        }),
                        vertex: vec![Vertex {
                            product: products,
                            origin: Origin {
                                t: 0.0,
                                vx: self.config.vertex.x as f32,
                                vy: self.config.vertex.y as f32,
                                vz: self.config.vertex.z as f32,
                            },
                        }],
                        random: Some(Random {
                            seed1: self.rng.i32(0..),
                            seed2: self.rng.i32(0..),
                            seed3: self.rng.i32(0..),
                            seed4: self.rng.i32(0..),
                        }),
                        user_data: vec![],
                    }],
                    hit_view: None,
                    recon_view: None,
                }],
            };
            self.writer
                .write_record(&record)
                .map_err(|e| LadduDataError::Sink(e.to_string()))?;
            self.next_event_number = self.next_event_number.checked_add(1).ok_or_else(|| {
                LadduDataError::Sink("HDDM event number overflowed i32".to_string())
            })?;
        }
        Ok(())
    }

    fn finish(&mut self) -> LadduDataResult<()> {
        self.writer
            .finish()
            .map_err(|e| LadduDataError::Sink(e.to_string()))
    }
}

#[allow(clippy::cast_possible_truncation)]
fn properties(particle: GlueXParticle) -> Properties {
    Properties {
        charge: i32::try_from(particle.charge_number())
            .expect("GlueX particle charges always fit in i32"),
        mass: particle.particle_mass() as f32,
    }
}

#[allow(clippy::cast_possible_truncation)]
fn momentum(p4: RealVec4) -> Momentum {
    Momentum {
        e: p4.e() as f32,
        px: p4.px() as f32,
        py: p4.py() as f32,
        pz: p4.pz() as f32,
        momentum_double: None,
    }
}
