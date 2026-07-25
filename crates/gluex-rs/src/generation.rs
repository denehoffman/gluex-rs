//! HDDM export utilities for laddu-generated Monte Carlo batches.
#![allow(clippy::cast_possible_truncation, clippy::cast_possible_wrap)]

/// Particle species mapping utilities.
pub mod species;

pub(crate) mod hddm_s;

use std::path::Path;

use fastrand::Rng;
use gluex_core::particles::Particle as GluexParticle;
use hddm::HddmFileWriter;
use laddu::{
    GeneratedBatchView, GeneratedLayout, GeneratedRecord, GeneratedSink, GenerationOutput,
    LadduError, LadduResult, Vec3, Vec4,
};
use thiserror::Error;

use crate::generation::{
    hddm_s::{
        Beam, Hddm, Momentum, Origin, PhysicsEvent, Product, Properties, Random, Reaction, Target,
        Vertex,
    },
    species::{SpeciesMappingError, gluex_particle_from_external_ids},
};

/// Error returned while converting a generated batch into `GlueX` HDDM records.
#[derive(Debug, Error)]
pub enum GlueXGenerationError {
    /// The requested particle does not have a stored p4 column in the generated dataset.
    #[error("generated particle '{id}' was not found")]
    MissingParticle {
        /// Generated particle identifier.
        id: String,
    },
    /// The requested particle did not have species metadata.
    #[error("generated particle '{id}' has no species metadata")]
    MissingSpecies {
        /// Generated particle identifier.
        id: String,
    },
    /// The requested particle did not have a stored p4 column.
    #[error("generated particle '{id}' has no stored p4 column")]
    MissingStoredP4 {
        /// Generated particle identifier.
        id: String,
    },
    /// The dataset event did not contain an expected p4 value.
    #[error("event is missing p4 column '{label}'")]
    MissingEventP4 {
        /// Dataset p4 column label.
        label: String,
    },
    /// A column-oriented generated batch contains inconsistent event counts.
    #[error(
        "generated p4 column for particle '{id}' contains {found} events, but the batch contains {expected} weights"
    )]
    ColumnLength {
        /// Generated particle identifier.
        id: String,
        /// Expected column length.
        expected: usize,
        /// Observed column length.
        found: usize,
    },
    /// Species metadata could not be mapped to GlueX/HDDM particle IDs.
    #[error(transparent)]
    Species(#[from] SpeciesMappingError),
    /// HDDM writing failed.
    #[error(transparent)]
    Hddm(#[from] hddm::HddmError),
}

/// Configuration for exporting generated events to `GlueX` simulation HDDM.
#[derive(Clone, Debug)]
pub struct GlueXHddmConfig {
    beam_id: String,
    target_id: String,
    run_number: i32,
    first_event_number: i32,
    random_seed: u64,
    vertex: Vec3,
}

impl GlueXHddmConfig {
    /// Construct a `GlueX` HDDM export configuration.
    ///
    /// The `beam_id` and `target_id` identify generated initial-state particles. Transport
    /// products are inferred from the stored generated p4 columns in each [`GeneratedBatch`],
    /// excluding the beam and target. Composite particles that should be decayed by Geant4, such as
    /// `KShort`, should be selected with `GeneratedStorage`, while their generated daughters should
    /// be omitted from generated p4 storage.
    pub fn new(beam_id: impl Into<String>, target_id: impl Into<String>) -> Self {
        Self {
            beam_id: beam_id.into(),
            target_id: target_id.into(),
            run_number: 0,
            first_event_number: 0,
            random_seed: 0,
            vertex: Vec3::zero(),
        }
    }

    /// Return the generated beam particle identifier.
    #[must_use]
    pub fn beam_id(&self) -> &str {
        &self.beam_id
    }

    /// Return the generated target particle identifier.
    #[must_use]
    pub fn target_id(&self) -> &str {
        &self.target_id
    }

    /// Return a copy of this config with a different run number.
    #[must_use]
    pub const fn with_run_number(mut self, run_number: i32) -> Self {
        self.run_number = run_number;
        self
    }

    /// Return a copy of this config with a different first event number.
    #[must_use]
    pub const fn with_first_event_number(mut self, first_event_number: i32) -> Self {
        self.first_event_number = first_event_number;
        self
    }

    /// Return a copy of this config with a deterministic random seed.
    #[must_use]
    pub const fn with_random_seed(mut self, random_seed: u64) -> Self {
        self.random_seed = random_seed;
        self
    }

    /// Return a copy of this config with a fixed production vertex in centimeters.
    #[must_use]
    pub const fn with_vertex(mut self, vertex: Vec3) -> Self {
        self.vertex = vertex;
        self
    }
}

#[derive(Clone)]
struct HddmMetadata {
    run_number: i32,
    target_vertex: Vec3,
    beam_particle: GluexParticle,
    beam_index: usize,
    target_particle: GluexParticle,
    target_index: usize,
    products: Vec<(usize, GluexParticle)>,
}

impl HddmMetadata {
    fn new(
        run_number: i32,
        beam_name: &str,
        target_name: &str,
        output: &GenerationOutput,
        layout: &GeneratedLayout,
    ) -> Self {
        let beam_particle_info = layout.particle(beam_name).expect("todo"); // TODO: handle errors
        // and validate role
        let beam_particle =
            gluex_particle_from_external_ids(beam_particle_info.properties.ids()).expect("todo"); // TODO: errors
        let target_particle_info = layout.particle(target_name).expect("todo"); // TODO: handle errors
        // and validate role
        let target_particle =
            gluex_particle_from_external_ids(target_particle_info.properties.ids()).expect("todo"); // TODO: errors
        let mut products = Vec::new();
        for particle in layout.particles() {
            if particle.label == beam_name || particle.label == target_name {
                continue;
            }
            if output.includes(particle) {
                products.push((
                    particle.output_index.expect("todo"), // TODO: errors
                    gluex_particle_from_external_ids(particle.properties.ids()).expect("todo"), // TODO:
                                                                                                // errors
                ));
            }
        }
        Self {
            run_number,
            target_vertex: Vec3::new(0.0, 0.0, 0.0), // TODO: custom
            beam_particle: beam_particle,
            beam_index: beam_particle_info.output_index.expect("todo"), // TODO: errors
            target_particle: target_particle,
            target_index: target_particle_info.output_index.expect("todo"), // TODO: errors
            products,
        }
    }
}

/// A sink which can write events to an HDDM file.
pub struct HddmSink {
    writer: HddmFileWriter,
    beam: String,
    target: String,
    output: GenerationOutput,
    run_number: i32,
    metadata: Option<HddmMetadata>,
    count: usize,
}

impl HddmSink {
    /// Create a new HDDM sink.
    pub fn new(
        path: impl AsRef<Path>,
        run_number: i32,
        beam: &str,
        target: &str,
        output: GenerationOutput,
    ) -> Self {
        Self {
            writer: hddm_s::create(path).expect("todo"), // TODO: Append mode and error handling
            beam: beam.to_string(),
            target: target.to_string(),
            output,
            run_number,
            metadata: None,
            count: 0,
        }
    }

    /// Set which generated particles should appear in the output file.
    pub fn output(mut self, output: GenerationOutput) -> Self {
        self.output = output;
        self
    }
}

impl GeneratedSink for HddmSink {
    type Output = usize;

    fn begin(&mut self, layout: &GeneratedLayout) -> LadduResult<()> {
        self.metadata = Some(HddmMetadata::new(
            self.run_number,
            &self.beam,
            &self.target,
            &self.output,
            layout,
        ));
        Ok(())
    }

    fn push_batch(&mut self, batch: GeneratedBatchView<'_>, rng: &mut Rng) -> LadduResult<()> {
        let metadata = self.metadata.clone().unwrap_or_else(|| {
            HddmMetadata::new(
                self.run_number,
                &self.beam,
                &self.target,
                &self.output,
                batch.layout,
            )
        });
        for generated_record in batch.records {
            self.writer
                .write_record(&generated_record_to_hddm(generated_record, &metadata, rng))
                .map_err(|_| LadduError::Custom("todo".to_string()))? // TODO: error handling
        }
        self.count += batch.records.len();
        Ok(())
    }

    fn finish(mut self) -> LadduResult<Self::Output> {
        self.writer.finish().expect("todo"); // TODO: errors
        Ok(self.count)
    }
}

fn generated_record_to_hddm(
    generated_record: &GeneratedRecord,
    metadata: &HddmMetadata,
    rng: &mut Rng,
) -> Hddm {
    record(
        metadata.run_number,
        generated_record.global_index as i32,
        metadata.target_vertex,
        metadata.beam_particle,
        generated_record.p4s[metadata.beam_index],
        metadata.target_particle,
        generated_record.p4s[metadata.target_index],
        metadata
            .products
            .iter()
            .enumerate()
            .map(|(id, (p4_index, particle))| {
                product_from_particle(id, *particle, generated_record.p4s[*p4_index])
            })
            .collect(),
        rng,
        generated_record.weight,
    )
}

#[allow(clippy::too_many_arguments)]
fn record(
    run_number: i32,
    event_number: i32,
    target_vertex: Vec3,
    beam_particle: GluexParticle,
    beam_p4: Vec4,
    target_particle: GluexParticle,
    target_p4: Vec4,
    products: Vec<Product>,
    rng: &mut Rng,
    weight: f64,
) -> Hddm {
    Hddm {
        geometry: None,
        physics_event: vec![PhysicsEvent {
            event_no: event_number,
            run_no: run_number,
            data_version_string: vec![],
            ccdb_context: vec![],
            reaction: vec![Reaction {
                type_: 0,
                weight: weight as f32,
                beam: Some(Beam {
                    type_: beam_particle.into(),
                    momentum: momentum(beam_p4),
                    polarization: None,
                    properties: properties(beam_particle),
                }),
                target: Some(Target {
                    type_: target_particle.into(),
                    momentum: momentum(target_p4),
                    polarization: None,
                    properties: properties(target_particle),
                }),
                vertex: vec![Vertex {
                    product: products,
                    origin: Origin {
                        t: 0.0,
                        vx: target_vertex.x as f32,
                        vy: target_vertex.y as f32,
                        vz: target_vertex.z as f32,
                    },
                }],
                random: Some(Random {
                    seed1: rng.i32(0..),
                    seed2: rng.i32(0..),
                    seed3: rng.i32(0..),
                    seed4: rng.i32(0..),
                }),
                user_data: vec![],
            }],
            hit_view: None,
            recon_view: None,
        }],
    }
}

fn product_from_particle(id: usize, particle: GluexParticle, p4: Vec4) -> Product {
    Product {
        decay_vertex: 0,
        id: id as i32,
        mech: 0,
        parentid: 0,
        pdgtype: particle.to_pdg() as i32,
        type_: particle.into(),
        momentum: momentum(p4),
        polarization: None,
        properties: Some(properties(particle)),
    }
}

fn properties(particle: GluexParticle) -> Properties {
    Properties {
        charge: particle.charge_number() as i32,
        mass: particle.particle_mass() as f32,
    }
}

fn momentum(p4: Vec4) -> Momentum {
    Momentum {
        e: p4.e() as f32,
        px: p4.px() as f32,
        py: p4.py() as f32,
        pz: p4.pz() as f32,
        momentum_double: None,
    }
}
