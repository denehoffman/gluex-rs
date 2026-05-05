//! HDDM export utilities for laddu-generated Monte Carlo batches.
#![allow(clippy::cast_possible_truncation, clippy::cast_possible_wrap)]

/// Particle species mapping utilities.
pub mod species;

pub(crate) mod hddm_s;

use std::path::Path;

use fastrand::Rng;
use gluex_core::particles::Particle as GluexParticle;
use hddm::Compression;
use laddu::{GeneratedBatch, GeneratedParticleLayout, OwnedEvent, Vec3, Vec4};
use thiserror::Error;

use crate::generation::{
    hddm_s::{
        Beam, Hddm, Momentum, Origin, PhysicsEvent, Product, Properties, Random, Reaction, Target,
        Vertex,
    },
    species::{SpeciesMappingError, gluex_particle_from_species},
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
    /// Species metadata could not be mapped to GlueX/HDDM particle IDs.
    #[error(transparent)]
    Species(#[from] SpeciesMappingError),
    /// HDDM writing failed.
    #[error(transparent)]
    Hddm(#[from] hddm::HddmError),
}

/// Configuration for exporting generated events to `GlueX` simulation HDDM.
#[derive(Clone, Debug)]
pub struct GluexHddmConfig {
    beam_id: String,
    target_id: String,
    run_number: i32,
    first_event_number: i32,
    random_seed: u64,
    vertex: Vec3,
}

impl GluexHddmConfig {
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

/// Writer for converting generated laddu batches into `GlueX` simulation HDDM files.
#[derive(Clone, Debug)]
pub struct GluexHddmWriter {
    config: GluexHddmConfig,
}

impl GluexHddmWriter {
    /// Construct a writer from an export configuration.
    #[must_use]
    pub const fn new(config: GluexHddmConfig) -> Self {
        Self { config }
    }

    /// Return this writer's export configuration.
    #[must_use]
    pub const fn config(&self) -> &GluexHddmConfig {
        &self.config
    }

    /// Write a generated batch to a new HDDM file.
    ///
    /// This creates one `GlueX` primary vertex per event. Beam and target are stored in their
    /// dedicated HDDM fields, and selected products are stored with `parentid = 0`, matching the
    /// convention used by `GlueX`'s `HDDMDataWriter` for internally generated transport products.
    ///
    /// # Errors
    ///
    /// Returns an error if a requested particle is absent, lacks species metadata, lacks required
    /// p4 data, cannot be mapped to `GlueX` particle IDs, or if the HDDM writer fails.
    pub fn write_batch(
        &self,
        batch: &GeneratedBatch,
        path: impl AsRef<Path>,
    ) -> Result<usize, GlueXGenerationError> {
        let mut writer = hddm_s::create(path.as_ref())?;
        let mut event_number = 0;
        self.write_batch_to_writer(batch, &mut writer, &mut event_number)?;
        Ok(event_number)
    }

    /// Append a generated batch to an existing HDDM file.
    ///
    /// The existing file must have been created with the same HDDM schema.
    ///
    /// # Errors
    ///
    /// Returns an error if conversion or file writing fails.
    pub fn append_batch(
        &self,
        batch: &GeneratedBatch,
        path: impl AsRef<Path>,
        start_event: usize,
    ) -> Result<usize, GlueXGenerationError> {
        let mut writer = hddm_s::append(path.as_ref())?;
        let mut event_number = start_event;
        self.write_batch_to_writer(batch, &mut writer, &mut event_number)?;
        Ok(event_number)
    }

    /// Write generated batches to a new HDDM file.
    ///
    /// This keeps the file open across batches, so records after the first batch are appended
    /// without rewriting the schema header.
    ///
    /// # Errors
    ///
    /// Returns an error if conversion or file writing fails.
    pub fn write_batches<'a>(
        &self,
        batches: impl IntoIterator<Item = &'a GeneratedBatch>,
        path: impl AsRef<Path>,
    ) -> Result<(), GlueXGenerationError> {
        let mut writer = hddm_s::create(path.as_ref())?;
        writer.set_compression(Compression::None)?; // NOTE: hdgeant4 can't handle compression
        let mut event_number = 0;
        for batch in batches {
            for record in self.records(batch, event_number)? {
                writer.write_record(&record)?;
                event_number += 1;
            }
        }
        writer.finish()?;
        Ok(())
    }

    /// Build HDDM records for a generated batch without writing them.
    ///
    /// # Errors
    ///
    /// Returns the same conversion errors as [`GluexHddmWriter::write_batch`].
    fn records(
        &self,
        batch: &GeneratedBatch,
        start_event: usize,
    ) -> Result<Vec<Hddm>, GlueXGenerationError> {
        let beam_layout = require_particle(batch, &self.config.beam_id)?;
        let target_layout = require_particle(batch, &self.config.target_id)?;
        let product_layouts =
            stored_product_layouts(batch, &self.config.beam_id, &self.config.target_id);

        let beam_particle = require_gluex_particle(beam_layout)?;
        let target_particle = require_gluex_particle(target_layout)?;
        let product_particles = product_layouts
            .iter()
            .map(|layout| require_gluex_particle(layout))
            .collect::<Result<Vec<_>, _>>()?;

        let mut rng = Rng::with_seed(self.config.random_seed);
        let mut records = Vec::with_capacity(batch.dataset().n_events_local());
        for (event_index, event) in batch.dataset().events_global().iter().enumerate() {
            let beam_p4 = stored_p4(batch, beam_layout, event)?;
            let target_p4 = optional_stored_p4(batch, target_layout, event)?
                .unwrap_or_else(|| Vec4::new(0.0, 0.0, 0.0, target_particle.particle_mass()));
            let products = product_layouts
                .iter()
                .zip(product_particles.iter().copied())
                .enumerate()
                .map(|(index, (layout, particle))| {
                    let p4 = stored_p4(batch, layout, event)?;
                    Ok(product_from_particle(index + 1, particle, p4))
                })
                .collect::<Result<Vec<_>, GlueXGenerationError>>()?;

            records.push(self.record(
                event_index + start_event,
                beam_particle,
                beam_p4,
                target_particle,
                target_p4,
                products,
                &mut rng,
                event.weight(),
            ));
        }
        Ok(records)
    }

    fn write_batch_to_writer(
        &self,
        batch: &GeneratedBatch,
        writer: &mut hddm::HddmFileWriter,
        start_event: &mut usize,
    ) -> Result<(), GlueXGenerationError> {
        for record in self.records(batch, *start_event)? {
            writer.write_record(&record)?;
            *start_event += 1;
        }
        writer.finish()?;
        Ok(())
    }

    #[allow(clippy::too_many_arguments)]
    fn record(
        &self,
        event_index: usize,
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
                event_no: self.config.first_event_number + event_index as i32,
                run_no: self.config.run_number,
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
                            vx: self.config.vertex.x as f32,
                            vy: self.config.vertex.y as f32,
                            vz: self.config.vertex.z as f32,
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
}

fn require_particle<'a>(
    batch: &'a GeneratedBatch,
    id: &str,
) -> Result<&'a GeneratedParticleLayout, GlueXGenerationError> {
    batch
        .layout()
        .particle(id)
        .ok_or_else(|| GlueXGenerationError::MissingParticle { id: id.to_string() })
}

fn stored_product_layouts<'a>(
    batch: &'a GeneratedBatch,
    beam_id: &str,
    target_id: &str,
) -> Vec<&'a GeneratedParticleLayout> {
    batch
        .layout()
        .particles()
        .iter()
        .filter(|layout| {
            layout.p4_label().is_some() && layout.id() != beam_id && layout.id() != target_id
        })
        .collect()
}

fn require_gluex_particle(
    layout: &GeneratedParticleLayout,
) -> Result<GluexParticle, GlueXGenerationError> {
    let species = layout
        .species()
        .ok_or_else(|| GlueXGenerationError::MissingSpecies {
            id: layout.id().to_string(),
        })?;
    Ok(gluex_particle_from_species(species)?)
}

fn stored_p4(
    batch: &GeneratedBatch,
    layout: &GeneratedParticleLayout,
    event: &OwnedEvent,
) -> Result<Vec4, GlueXGenerationError> {
    let label = layout
        .p4_label()
        .ok_or_else(|| GlueXGenerationError::MissingStoredP4 {
            id: layout.id().to_string(),
        })?;
    if !batch
        .layout()
        .p4_labels()
        .iter()
        .any(|stored| stored == label)
    {
        return Err(GlueXGenerationError::MissingStoredP4 {
            id: layout.id().to_string(),
        });
    }
    event
        .p4(label)
        .ok_or_else(|| GlueXGenerationError::MissingEventP4 {
            label: label.to_string(),
        })
}

fn optional_stored_p4(
    batch: &GeneratedBatch,
    layout: &GeneratedParticleLayout,
    event: &OwnedEvent,
) -> Result<Option<Vec4>, GlueXGenerationError> {
    if layout.p4_label().is_some() {
        stored_p4(batch, layout, event).map(Some)
    } else {
        Ok(None)
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
        charge: particle.particle_charge() as i32,
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

#[cfg(test)]
mod tests {
    use std::{collections::HashMap, fs, path::PathBuf, process};

    use laddu::{
        Vec3,
        generation::{
            CompositeGenerator, EventGenerator, GeneratedParticle, GeneratedReaction,
            GeneratedStorage, InitialGenerator, MandelstamTDistribution, ParticleSpecies,
            Reconstruction, StableGenerator,
        },
    };

    use super::{GluexHddmConfig, GluexHddmWriter};

    fn ksks_batch() -> laddu::LadduResult<laddu::GeneratedBatch> {
        let beam = GeneratedParticle::initial(
            "beam",
            InitialGenerator::beam_with_fixed_energy(0.0, 9.0),
            Reconstruction::Stored,
        )
        .with_species(ParticleSpecies::code(22));
        let target = GeneratedParticle::initial(
            "target",
            InitialGenerator::target(0.938_272_046),
            Reconstruction::Missing,
        )
        .with_species(ParticleSpecies::code(2212));
        let pip_1 = GeneratedParticle::stable(
            "pip1",
            StableGenerator::new(0.139_570_18),
            Reconstruction::Stored,
        )
        .with_species(ParticleSpecies::code(211));
        let pim_1 = GeneratedParticle::stable(
            "pim1",
            StableGenerator::new(0.139_570_18),
            Reconstruction::Stored,
        )
        .with_species(ParticleSpecies::code(-211));
        let pip_2 = GeneratedParticle::stable(
            "pip2",
            StableGenerator::new(0.139_570_18),
            Reconstruction::Stored,
        )
        .with_species(ParticleSpecies::code(211));
        let pim_2 = GeneratedParticle::stable(
            "pim2",
            StableGenerator::new(0.139_570_18),
            Reconstruction::Stored,
        )
        .with_species(ParticleSpecies::code(-211));
        let ks_1 = GeneratedParticle::composite(
            "ks1",
            CompositeGenerator::new(0.497_614, 0.497_615),
            (&pip_1, &pim_1),
            Reconstruction::Stored,
        )
        .with_species(ParticleSpecies::code(310));
        let ks_2 = GeneratedParticle::composite(
            "ks2",
            CompositeGenerator::new(0.497_614, 0.497_615),
            (&pip_2, &pim_2),
            Reconstruction::Stored,
        )
        .with_species(ParticleSpecies::code(310));
        let kk = GeneratedParticle::composite(
            "kk",
            CompositeGenerator::new(1.1, 1.3),
            (&ks_1, &ks_2),
            Reconstruction::Composite,
        );
        let recoil = GeneratedParticle::stable(
            "recoil",
            StableGenerator::new(0.938_272_046),
            Reconstruction::Stored,
        )
        .with_species(ParticleSpecies::code(2212));
        let reaction = GeneratedReaction::two_to_two(
            beam,
            target,
            kk,
            recoil,
            MandelstamTDistribution::Exponential { slope: 1.0 },
        )?;
        EventGenerator::new(reaction, HashMap::new(), Some(0))
            .with_storage(GeneratedStorage::only(["beam", "ks1", "ks2", "recoil"]))?
            .generate_batch(2)
    }

    #[test]
    fn exports_selected_transport_products_only() {
        let batch = ksks_batch().unwrap();
        let writer = GluexHddmWriter::new(
            GluexHddmConfig::new("beam", "target")
                .with_run_number(90_000)
                .with_first_event_number(7)
                .with_vertex(Vec3::new(0.1, 0.2, 50.0)),
        );
        let records = writer.records(&batch, 0).unwrap();
        assert_eq!(records.len(), 2);
        let event = &records[0].physics_event[0];
        assert_eq!(event.run_no, 90_000);
        assert_eq!(event.event_no, 7);
        let reaction = &event.reaction[0];
        assert_eq!(reaction.beam.as_ref().unwrap().type_, hddm::Particle::Gamma);
        assert_eq!(
            reaction.target.as_ref().unwrap().type_,
            hddm::Particle::Proton
        );
        let products = &reaction.vertex[0].product;
        assert_eq!(products.len(), 3);
        assert_eq!(products[0].type_, hddm::Particle::KShort);
        assert_eq!(products[1].type_, hddm::Particle::KShort);
        assert_eq!(products[2].type_, hddm::Particle::Proton);
        assert!(products.iter().all(|product| product.parentid == 0));
        assert_eq!(
            products
                .iter()
                .map(|product| product.id)
                .collect::<Vec<_>>(),
            [1, 2, 3]
        );
    }

    #[test]
    fn writes_selected_transport_products_to_hddm_file() {
        let batch = ksks_batch().unwrap();
        let writer = GluexHddmWriter::new(GluexHddmConfig::new("beam", "target"));
        let path = PathBuf::from(format!(
            "/tmp/gluex-rs-ksks-demo-{}-{}.hddm",
            process::id(),
            fastrand::u64(..)
        ));
        writer.write_batch(&batch, &path).unwrap();
        assert!(path.metadata().unwrap().len() > 0);
        fs::remove_file(path).unwrap();
    }

    #[test]
    fn appends_batches_to_existing_hddm_file() {
        let first = ksks_batch().unwrap();
        let second = ksks_batch().unwrap();
        let writer = GluexHddmWriter::new(GluexHddmConfig::new("beam", "target"));
        let path = PathBuf::from(format!(
            "/tmp/gluex-rs-ksks-append-{}-{}.hddm",
            process::id(),
            fastrand::u64(..)
        ));
        let event_number = writer.write_batch(&first, &path).unwrap();
        let first_size = path.metadata().unwrap().len();
        let _ = writer.append_batch(&second, &path, event_number).unwrap();
        assert!(path.metadata().unwrap().len() > first_size);
        fs::remove_file(path).unwrap();
    }
}
