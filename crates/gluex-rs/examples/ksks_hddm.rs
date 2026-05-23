//! Generate `gamma p -> KShort KShort p` and write `GlueX` simulation HDDM.

use std::{collections::HashMap, env, error::Error, path::PathBuf};

use gluex_rs::generation::{GlueXHddmConfig, GlueXHddmWriter};
use laddu::{
    Vec3,
    generation::{
        CompositeGenerator, EventGenerator, GeneratedParticle, GeneratedReaction, GeneratedStorage,
        InitialGenerator, MandelstamTDistribution, ParticleSpecies, Reconstruction,
        StableGenerator,
    },
};

fn main() -> Result<(), Box<dyn Error>> {
    let output_path = env::args_os()
        .nth(1)
        .map_or_else(|| PathBuf::from("laddu_ksks_demo.hddm"), PathBuf::from);

    let writer = GlueXHddmWriter::new(
        GlueXHddmConfig::new("beam", "target")
            .with_run_number(40_000)
            .with_first_event_number(1)
            .with_random_seed(0)
            .with_vertex(Vec3::new(0.0, 0.0, 50.0)),
    );
    let batch = generate_ksks_batch(1000, 0)?;
    let mut event_number = writer.write_batch(&batch, &output_path)?;
    for i in 1..1000 {
        let batch = generate_ksks_batch(1000, i)?;
        event_number = writer.append_batch(&batch, &output_path, event_number)?;
    }
    println!("wrote {}", output_path.display());
    Ok(())
}

fn generate_ksks_batch(n_events: usize, seed: u64) -> laddu::LadduResult<laddu::GeneratedBatch> {
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

    let pip_1 = pion("pip1", 211);
    let pim_1 = pion("pim1", -211);
    let pip_2 = pion("pip2", 211);
    let pim_2 = pion("pim2", -211);

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

    EventGenerator::new(reaction, HashMap::new(), Some(seed))
        .with_storage(GeneratedStorage::only(["beam", "ks1", "ks2", "recoil"]))?
        .generate_batch(n_events)
}

fn pion(id: &str, pdg: i64) -> GeneratedParticle {
    GeneratedParticle::stable(
        id,
        StableGenerator::new(0.139_570_18),
        Reconstruction::Stored,
    )
    .with_species(ParticleSpecies::code(pdg))
}
