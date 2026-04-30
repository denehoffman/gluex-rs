use std::{path::Path, sync::Arc};

use fastrand::Rng;
use gluex_core::RunPeriod;
use hddm::Particle;
use laddu::{Dataset, GeneratedBatch, Vec3};

use crate::hddm_s::{
    Beam, Hddm, Momentum, Origin, PhysicsEvent, Product, Properties, Random, Reaction, Target,
    Vertex,
};

struct HddmBatchWriter {
    dataset: GeneratedBatch,
    reaction: laddu::Reaction,
    initial_vertex: Vec3,
}

impl HddmBatchWriter {
    fn append_to(&self, path: impl AsRef<Path>) {
        let mut rng = Rng::with_seed(0);
        let mut writer = crate::hddm_s::append(path.as_ref()).unwrap();
        for (i, event) in self.dataset.dataset().iter().enumerate() {
            let event_beam = event.p4("beam").unwrap();
            let beam = Beam {
                type_: Particle::Gamma,
                momentum: Momentum {
                    e: event_beam.e() as f32,
                    px: event_beam.px() as f32,
                    py: event_beam.py() as f32,
                    pz: event_beam.pz() as f32,
                    momentum_double: None,
                },
                polarization: None,
                properties: Properties {
                    charge: 0,
                    mass: 0.0,
                },
            };
            let target = Target {
                type_: Particle::Proton,
                momentum: Momentum {
                    e: 0.938272,
                    px: 0.0,
                    py: 0.0,
                    pz: 0.0,
                    momentum_double: None,
                },
                polarization: None,
                properties: Properties {
                    charge: 1,
                    mass: 0.938272,
                },
            };
            let mut product_list = todo!();
            // let mut product_list = self
            //     .reaction
            //     .iter_final_state()
            //     .map(|p| Product {
            //         decay_vertex: 0, // TODO:
            //         id: p.index as i32,
            //         mech: 0,
            //         parentid: p.parent_index as i32,
            //         pdgtype: gluex_core::Particle::from(p.particle).to_pdg() as i32,
            //         type_: p.particle,
            //         momentum: Momentum {
            //             e: p.p4.e() as f32,
            //             px: p.p4.px() as f32,
            //             py: p.p4.py() as f32,
            //             pz: p.p4.pz() as f32,
            //             momentum_double: None,
            //         },
            //         polarization: None,
            //         properties: Some(Properties {
            //             charge: gluex_core::Particle::from(p.particle).particle_charge() as i32,
            //             mass: gluex_core::Particle::from(p.particle).particle_mass() as f32,
            //         }),
            //     })
            //     .collect::<Vec<Product>>();
            let vertex_list = vec![Vertex {
                product: product_list,
                origin: Origin {
                    t: 0.0,
                    vx: self.initial_vertex.x as f32,
                    vy: self.initial_vertex.y as f32,
                    vz: self.initial_vertex.z as f32,
                },
            }];
            let mut randoms = Random {
                seed1: rng.i32(0..),
                seed2: rng.i32(0..),
                seed3: rng.i32(0..),
                seed4: rng.i32(0..),
            };
            let reaction = Reaction {
                type_: 0,
                weight: 1.0,
                beam: Some(beam),
                target: Some(target),
                vertex: vertex_list,
                random: Some(randoms),
                user_data: vec![],
            };
            let record = Hddm {
                geometry: None,
                physics_event: vec![PhysicsEvent {
                    event_no: i as i32,
                    run_no: RunPeriod::RP2019_11.min_run() as i32,
                    data_version_string: vec![],
                    ccdb_context: vec![],
                    reaction: vec![reaction],
                    hit_view: None,
                    recon_view: None,
                }],
            };
            writer.write_record(&record).unwrap();
        }
        writer.finish().unwrap();
    }
}
