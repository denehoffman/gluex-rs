//! Generate `gamma p -> KShort KShort p` and write `GlueX` simulation HDDM.

use std::{env, error::Error, path::PathBuf};

use fastrand::Rng;
use gluex_rs::generation::HddmSink;
use laddu::{
    Angles, Axes, Axis, Channel, Charge, EventGenerator, Expression, Frame, GenerationMode,
    GenerationOptions, GenerationPlan, Isospin, J, LadduResult, M, PI, Parity, ParticleGeneration,
    ParticleProperties, PhotonHelicity, PhotonSDME, PolarComplexScalar, RuleSet,
    ScalarDistribution, WignerD, j, l, m, parameter,
    reaction::TwoBodyCoupling,
    samplers::{rest, t_exponential, uniform_energy, uniform_mass},
};

fn f0_1500() -> laddu::LadduResult<laddu::Expression> {
    let channel = build_channel()?;
    laddu::BreitWigner::new(
        ["f0", "S"],
        parameter!("f_0(1500) mass", 1.522),
        parameter!("f_0(1500) width", 0.108),
        0,
        &channel.mass("kshort1")?,
        &channel.mass("kshort2")?,
        &channel.mass("X")?,
    )
}
fn f2_1525() -> laddu::LadduResult<laddu::Expression> {
    let channel = build_channel()?;
    laddu::BreitWigner::new(
        ["f2", "D"],
        parameter!("f_2(1525) mass", 1.517),
        parameter!("f_2(1525) width", 0.072),
        2,
        &channel.mass("kshort1")?,
        &channel.mass("kshort2")?,
        &channel.mass("X")?,
    )
}

fn build_channel() -> LadduResult<Channel> {
    let photon = ParticleProperties::default()
        .with_name("gamma")
        .with_id("pdg", 22)
        .with_mass(0.0)
        .with_spin(j!(1))
        .with_charge(Charge::int(0))
        .with_parity(Parity::Negative)
        .with_c_parity(Parity::Negative);
    let proton = ParticleProperties::default()
        .with_name("proton")
        .with_id("pdg", 2212)
        .with_mass(0.938_272_046)
        .with_spin(j!(1 / 2))
        .with_charge(Charge::int(-1))
        .with_isospin(Isospin::new(j!(1 / 2), None)?)
        .with_parity(Parity::Positive);
    let kshort = ParticleProperties::default()
        .with_name("K_S")
        .with_id("pdg", 310)
        .with_mass(0.497_611)
        .with_spin(j!(0))
        .with_charge(Charge::int(0))
        .with_isospin(Isospin::new(j!(1 / 2), None)?)
        .with_parity(Parity::Negative);
    let pi_plus = ParticleProperties::default()
        .with_name("pi+")
        .with_id("pdg", 211)
        .with_mass(0.139_570_18)
        .with_charge(Charge::int(1))
        .with_isospin(Isospin::new(j!(1), None)?)
        .with_g_parity(Parity::Negative)
        .with_spin(j!(0))
        .with_parity(Parity::Negative);
    let pi_minus = ParticleProperties::default()
        .with_name("pi-")
        .with_id("pdg", -211)
        .with_mass(0.139_570_18)
        .with_charge(Charge::int(-1))
        .with_isospin(Isospin::new(j!(1), None)?)
        .with_g_parity(Parity::Negative)
        .with_spin(j!(0))
        .with_parity(Parity::Negative);

    let mut channel = Channel::new();
    channel.create_production("production", ["beam", "target"], ["X", "recoil"])?;
    channel.create_decay("X decay", "X", ["kshort1", "kshort2"])?;
    channel.create_decay("kshort1 decay", "kshort1", ["piplus1", "piminus1"])?;
    channel.create_decay("kshort2 decay", "kshort2", ["piplus2", "piminus2"])?;
    channel
        .edit_vertex("production")?
        .generate(t_exponential(2.80));
    channel.edit_vertex("X decay")?.rules(RuleSet::strong());
    channel.edit_vertex("kshort1 decay")?.rules(RuleSet::weak());
    channel.edit_vertex("kshort2 decay")?.rules(RuleSet::weak());

    channel
        .edit_particle("beam")?
        .properties(photon)
        .generation(ParticleGeneration::default().with_momentum(uniform_energy(8.0, 9.0)));
    channel
        .edit_particle("target")?
        .properties(proton.clone())
        .generation(ParticleGeneration::default().with_momentum(rest()));
    channel.edit_particle("recoil")?.properties(proton);
    channel.edit_particle("kshort1")?.properties(kshort.clone());
    channel.edit_particle("kshort2")?.properties(kshort);
    channel
        .edit_particle("piplus1")?
        .properties(pi_plus.clone());
    channel
        .edit_particle("piminus1")?
        .properties(pi_minus.clone());
    channel.edit_particle("piplus2")?.properties(pi_plus);
    channel.edit_particle("piminus2")?.properties(pi_minus);

    channel
        .edit_particle("X")?
        .mass_sampler(uniform_mass(1.0, 2.0));
    Ok(channel)
}

fn build_model(channel: &Channel) -> LadduResult<Expression> {
    let decay_frame = Frame::new(
        "X decay",
        Axes::from_y_z(
            Axis::normal("beam", "recoil").at("production"),
            Axis::opposite("recoil").at("production"),
        ),
    )?;
    let decay_angles = channel.angles("kshort1", decay_frame)?;
    let polarization = channel.polarization("production", "pol_magnitude", "pol_angle")?;

    let proton_sectors = [(m!(1 / 2), m!(1 / 2)), (m!(1 / 2), m!(-1 / 2))];
    let photon_helicities = [m!(1), m!(-1)];

    let mut intensity = Expression::zero();
    let couplings = channel.two_body_couplings("X decay", j!(2), l!(2))?;
    for (_target_helicity, _recoil_helicity) in proton_sectors {
        for photon_helicity in photon_helicities {
            let amp = helicity_amplitude(&couplings, photon_helicity, &decay_angles)?;
            for photon_helicity_prime in photon_helicities {
                let amp_prime =
                    helicity_amplitude(&couplings, photon_helicity_prime, &decay_angles)?;
                let rho = PhotonSDME::new(
                    format!("rho_{}{}", photon_helicity, photon_helicity_prime),
                    laddu::PhotonPolarization::Linear(Box::new(polarization.clone())),
                    PhotonHelicity::new(photon_helicity.doubled() / 2)?,
                    PhotonHelicity::new(photon_helicity_prime.doubled() / 2)?,
                )?;
                intensity += (rho * amp.clone() * amp_prime.conj()).real();
            }
        }
    }
    Ok(intensity)
}

fn helicity_amplitude(
    couplings: &[TwoBodyCoupling],
    photon_helicity: M,
    decay_angles: &Angles,
) -> LadduResult<Expression> {
    let mut amp = Expression::zero();
    for coupling in couplings {
        for x_m in coupling.j().projections() {
            let decay = WignerD::new((), coupling.j(), x_m, m!(0), decay_angles)?;
            let label = label(coupling.j(), photon_helicity, x_m);
            if coupling.j() == j!(0) {
                amp += f0_1500()?
                    * &decay
                    * PolarComplexScalar::new(
                        &label,
                        parameter!(format!("{} mag", &label), initial: (0.0, 100.0), bounds: (0.0, None)),
                        parameter!(format!("{} phase", &label), 0.0),
                    )?
            } else if coupling.j() == j!(2) {
                amp += f2_1525()?
                    * &decay
                    * PolarComplexScalar::new(
                        &label,
                        parameter!(format!("{} mag", &label), initial: (0.0, 100.0), bounds: (0.0, None)),
                        parameter!(format!("{} phase", &label), initial: (0.0, PI)),
                    )?
            }
        }
    }
    Ok(amp)
}

fn label(j: J, photon_helicity: M, x_projection: M) -> String {
    format!("T^{j}_{{{photon_helicity}{x_projection}}}")
}

fn main() -> Result<(), Box<dyn Error>> {
    let output_path = env::args_os()
        .nth(1)
        .map_or_else(|| PathBuf::from("laddu_ksks_demo.hddm"), PathBuf::from);

    let channel = build_channel()?;
    let model = build_model(&channel)?;

    let generator = EventGenerator::new(
        GenerationPlan::from_channel(&channel)?
            .with_aux(
                "pol_magnitude",
                ScalarDistribution::Uniform { min: 0.2, max: 0.3 },
            )
            .with_aux("pol_angle", ScalarDistribution::Fixed(PI / 2.0)),
    )
    .with_seed(0);
    let sink = HddmSink::new(
        output_path,
        30_000,
        "beam",
        "target",
        laddu::GenerationOutput::Only(vec![
            "beam".to_string(),
            "target".to_string(),
            "recoil".to_string(),
            "kshort1".to_string(),
            "kshort2".to_string(),
        ]),
    );
    let mut rng = Rng::with_seed(0);
    let parameters = model.parameters().free().initial_values(&mut rng)?;
    let res = generator.generate(
        10_000,
        sink,
        GenerationMode::Accepted {
            expression: Box::new(model),
            parameters,
            envelope: laddu::Envelope::Adaptive {
                initial: 1.0,
                growth_factor: 1.25,
            },
        },
        GenerationOptions::default(),
    )?;
    println!("{}", res.stats.audit());
    println!("{}", res.output);

    Ok(())
}
