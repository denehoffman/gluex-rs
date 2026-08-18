//! Generate `gamma p -> KShort KShort p` and write `GlueX` simulation HDDM.

use std::{env, error::Error, f64::consts::PI, path::PathBuf};

use gluex_rs::generation::{
    GlueXGenerationError, GlueXGenerationResult, GlueXHddmConfig, GlueXIdExt, HddmSink,
};
use laddu::{
    physics::quantum::builtin::{K_SHORT, PHOTON, PROTON},
    prelude::*,
};

/// f2 magnitude truth
pub const F2_MAGNITUDE_TRUTH: f64 = 0.65;
/// f2 relative phase truth
pub const F2_PHASE_TRUTH: f64 = 0.7;

const F0_MASS: f64 = 1.522;
const F0_WIDTH: f64 = 0.108;
const F2_MASS: f64 = 1.275_412_049_919_005;
const F2_WIDTH: f64 = 0.186_554_356_637_326_4;

fn truth_parameters(model: &CompiledModel) -> Result<ParamValues, ParamError> {
    let free = model
        .params()
        .free_values_with(|parameter| match parameter.name() {
            "f2_magnitude" => F2_MAGNITUDE_TRUTH,
            "f2_phase" => F2_PHASE_TRUTH,
            name => panic!("unexpected free parameter `{name}` in K_S K_S model"),
        });
    model.params().values(&free)
}

fn build_channel() -> GlueXGenerationResult<Channel> {
    let mut channel = Channel::new("λ + p -> X + p, X -> Kₛ+ Kₛ");
    channel
        .edge("gamma")
        .p4(Vec4::event("gamma"))
        .properties(&PHOTON)
        .initial_energy_source_direction(ScalarSource::uniform(8.0, 9.0), RealVec3::z())
        .output()
        .set_beam_id()?;
    channel
        .edge("target")
        .p4(Vec4::event("target"))
        .properties(&PROTON)
        .initial_momentum(RealVec3::zero())
        .output()
        .set_target_id()?;
    channel
        .edge("X")
        .mass_proposal(MassProposal::uniform(1.0, 2.0));
    channel
        .edge("ks1")
        .p4(Vec4::event("ks1"))
        .properties(&K_SHORT)
        .output();
    channel
        .edge("ks2")
        .p4(Vec4::event("ks2"))
        .properties(&K_SHORT)
        .output();
    channel
        .edge("recoil")
        .p4(Vec4::event("recoil"))
        .properties(&PROTON)
        .output();
    channel
        .vertex("production")
        .incoming(["gamma", "target"])
        .outgoing(["X", "recoil"])
        .generation(VertexProposal::t_exchange(
            ("gamma", "X"),
            TDistribution::exponential(2.80)
                .with_limits(Some(-2.0), Some(0.0))
                .map_err(|e| GlueXGenerationError::Custom(e.to_string()))?,
        ));
    channel
        .vertex("decay")
        .incoming(["X"])
        .outgoing(["ks1", "ks2"]);

    Ok(channel)
}

#[allow(clippy::too_many_lines)]
fn ksks_intensity(channel: &Channel) -> LadduResult<Expr> {
    let production = channel.get_vertex("production")?;
    let beam_axis = production.vec3("gamma")?;
    let helicity_axis = production.vec3("X")?;
    let production_normal = beam_axis.cross(&helicity_axis);
    let pol_axis = Vec3::new(
        event_scalar("pol_angle").cos(),
        event_scalar("pol_angle").sin(),
        0.0,
    );
    let pol_angle = atan2(
        production_normal.dot(&pol_axis),
        beam_axis.unit().dot(&pol_axis.cross(&production_normal)),
    );
    let pol_magnitude = event_scalar("pol_magnitude");

    let rho = matrix([
        [0.5.into(), -0.5 * &pol_magnitude * cis(-2.0 * &pol_angle)],
        [-0.5 * pol_magnitude * cis(2.0 * pol_angle), 0.5.into()],
    ]);
    let s = channel.s("X")?;
    let k_short_mass = channel.particle("ks1")?.mass()?;
    let f0 = ParticleProperties::unknown()
        .with_name("f0(1500)")
        .with_spin(j!(0))
        .with_parity(Parity::Positive)
        .with_mass(F0_MASS);
    let f2 = ParticleProperties::unknown()
        .with_name("f2(1270)")
        .with_spin(j!(2))
        .with_parity(Parity::Positive)
        .with_mass(F2_MASS);
    let f0_bw =
        relativistic_breit_wigner(&s, f0.mass()?, F0_WIDTH, k_short_mass, k_short_mass, l!(0))?;
    let f2_bw =
        relativistic_breit_wigner(&s, f2.mass()?, F2_WIDTH, k_short_mass, k_short_mass, l!(2))?;
    let photon_helicities = channel
        .particle("gamma")?
        .spin()?
        .projections()
        .into_iter()
        .filter(|projection| *projection != M::int(0))
        .collect::<Vec<_>>();
    let target_helicities = channel.particle("target")?.spin()?.projections();
    let recoil_helicities = channel.particle("recoil")?.spin()?.projections();
    let first_kaon_helicities = channel.particle("ks1")?.spin()?.projections();
    let second_kaon_helicities = channel.particle("ks2")?.spin()?.projections();
    let f0_decay_wave = unique_decay_partial_wave(channel, &f0)?;
    let f2_decay_wave = unique_decay_partial_wave(channel, &f2)?;
    let f2_coupling = polar_complex(
        parameter!(
            "f2_magnitude",
            initial: 0.35,
            bounds: (0.0, 2.0),
            scale: 0.5
        ),
        parameter!(
            "f2_phase",
            initial: 0.0,
            bounds: (-PI, PI),
            periodic: true,
            scale: 1.0
        ),
    );
    let mut coherent = Expr::from(0.0);
    for (i, photon) in photon_helicities.iter().enumerate() {
        for (j, photon_prime) in photon_helicities.iter().enumerate() {
            for &target in &target_helicities {
                for &recoil in &recoil_helicities {
                    for &first_kaon in &first_kaon_helicities {
                        for &second_kaon in &second_kaon_helicities {
                            let f0_amp = sequential_wave(
                                channel,
                                &f0,
                                f0_decay_wave,
                                *photon,
                                target,
                                recoil,
                                first_kaon,
                                second_kaon,
                                &f0_bw,
                            )?
                            .tagged("f0");
                            let f2_amp = (f2_coupling.clone()
                                * sequential_wave(
                                    channel,
                                    &f2,
                                    f2_decay_wave,
                                    *photon,
                                    target,
                                    recoil,
                                    first_kaon,
                                    second_kaon,
                                    &f2_bw,
                                )?)
                            .tagged("f2");
                            // The f0 coupling is the fixed scale and phase reference.
                            let amplitude = f0_amp + f2_amp;
                            let f0_amp_prime = sequential_wave(
                                channel,
                                &f0,
                                f0_decay_wave,
                                *photon_prime,
                                target,
                                recoil,
                                first_kaon,
                                second_kaon,
                                &f0_bw,
                            )?
                            .tagged("f0");
                            let f2_amp_prime = (f2_coupling.clone()
                                * sequential_wave(
                                    channel,
                                    &f2,
                                    f2_decay_wave,
                                    *photon_prime,
                                    target,
                                    recoil,
                                    first_kaon,
                                    second_kaon,
                                    &f2_bw,
                                )?)
                            .tagged("f2");
                            let amplitude_prime = f0_amp_prime + f2_amp_prime;
                            // The f0 coupling is the fixed scale and phase reference.
                            let val = amplitude * rho.matrix_element(i, j) * amplitude_prime.conj();
                            coherent += val;
                        }
                    }
                }
            }
        }
    }
    Ok(coherent * 0.25)
}

#[allow(clippy::too_many_arguments)]
fn sequential_wave(
    channel: &Channel,
    resonance: &ParticleProperties,
    decay_wave: PartialWave,
    m_photon: M,
    m_target: M,
    m_recoil: M,
    m_ks1: M,
    m_ks2: M,
    line_shape: &Expr,
) -> LadduResult<Expr> {
    let production = channel.get_vertex("production")?;
    let decay = channel.get_vertex("decay")?;
    let beam_axis = production.vec3("gamma")?;
    let helicity_axis = production.vec3("X")?;
    let production_normal = beam_axis.cross(&helicity_axis);
    let production_theta = production.theta("X", beam_axis.clone(), Vec3::y())?;
    let production_phi = production.phi("X", beam_axis, Vec3::y())?;
    let decay_theta = decay.theta("ks1", helicity_axis.clone(), production_normal.clone())?;
    let decay_phi = decay.phi("ks1", helicity_axis, production_normal)?;
    let resonance_spin = resonance.spin()?;
    let photon_spin = channel.particle("gamma")?.spin()?;
    let target_spin = channel.particle("target")?.spin()?;
    let recoil_spin = channel.particle("recoil")?.spin()?;
    let first_kaon_spin = channel.particle("ks1")?.spin()?;
    let second_kaon_spin = channel.particle("ks2")?.spin()?;
    let production_total_j = production_total_j(channel, resonance)?;
    let decay_helicity = m_ks1 - m_ks2;
    // The target and recoil travel opposite the corresponding +z helicity axes.
    let initial_projection = m_photon - m_target;
    let initial_coupling = clebsch_gordan(
        photon_spin,
        m_photon,
        target_spin,
        -m_target,
        production_total_j,
        initial_projection,
    );
    let mut angular = Expr::from(0.0);
    for resonance_helicity in resonance_spin.projections() {
        let final_projection = resonance_helicity - m_recoil;
        let production_coupling = clebsch_gordan(
            resonance_spin,
            resonance_helicity,
            recoil_spin,
            -m_recoil,
            production_total_j,
            final_projection,
        );
        let daughter_spin_coupling = clebsch_gordan(
            first_kaon_spin,
            m_ks1,
            second_kaon_spin,
            -m_ks2,
            decay_wave.s,
            decay_helicity,
        );
        let orbital_coupling = clebsch_gordan(
            J::from(decay_wave.l),
            m!(0),
            decay_wave.s,
            decay_helicity,
            decay_wave.j,
            decay_helicity,
        );
        if initial_coupling == 0.0
            || production_coupling == 0.0
            || daughter_spin_coupling == 0.0
            || orbital_coupling == 0.0
        {
            continue;
        }
        let production_d =
            WignerDMatrix::new(production_total_j, initial_projection, final_projection)?
                .D(&production_phi, &production_theta, 0.0)
                .conj();
        let decay_d = WignerDMatrix::new(resonance_spin, resonance_helicity, decay_helicity)?
            .D(&decay_phi, &decay_theta, 0.0)
            .conj();
        angular += initial_coupling
            * production_coupling
            * daughter_spin_coupling
            * orbital_coupling
            * production_d
            * decay_d;
    }
    let normalization = (f64::from(production_total_j.multiplicity())
        * f64::from(resonance_spin.multiplicity()))
    .sqrt()
        / (4.0 * PI);
    let ls_normalization =
        (f64::from(decay_wave.l.multiplicity()) / f64::from(decay_wave.j.multiplicity())).sqrt();
    Ok(normalization * ls_normalization * line_shape * angular)
}

fn unique_decay_partial_wave(
    channel: &Channel,
    resonance: &ParticleProperties,
) -> LadduPhysicsResult<PartialWave> {
    let resonance_spin = resonance.spin()?;
    let first_daughter = channel.particle("ks1")?;
    let second_daughter = channel.particle("ks2")?;
    let max_l = L::try_from(resonance_spin + first_daughter.spin()? + second_daughter.spin()?)?;
    Ok(SelectionRules::angular(max_l)
        .allowed_partial_waves(resonance, (first_daughter, second_daughter))
        .into_iter()
        .next()
        .map(|allowed| allowed.wave)
        .expect("there should be at least one valid wave"))
}

fn production_total_j(channel: &Channel, resonance: &ParticleProperties) -> LadduPhysicsResult<J> {
    let initial = SelectionRules::coupled_spins(
        channel.particle("gamma")?.spin()?,
        channel.particle("target")?.spin()?,
    );
    let final_state =
        SelectionRules::coupled_spins(resonance.spin()?, channel.particle("recoil")?.spin()?);
    Ok(initial
        .into_iter()
        .find(|candidate| final_state.contains(candidate))
        .expect("there should be at least one valid coupling"))
}

fn main() -> Result<(), Box<dyn Error>> {
    let output_path = env::args_os()
        .nth(1)
        .map_or_else(|| PathBuf::from("laddu_ksks_demo.hddm"), PathBuf::from);

    let channel = build_channel()?;
    let compiled = CompiledModel::from_expr(&ksks_intensity(&channel)?)?;
    let evaluator = ModelEvaluator::prepare(
        &compiled,
        truth_parameters(&compiled)?,
        &Execution::default(),
    )?;
    let generator = ChannelGenerator::new(channel.clone())?
        .with_scalar("pol_magnitude", ScalarSource::uniform(0.2, 0.3))?
        .with_scalar("pol_angle", ScalarSource::constant(PI / 2.0))?;

    let mut sink = HddmSink::new(
        output_path,
        GlueXHddmConfig::new(&channel)?.with_run_number(30_000),
    )?;

    let res = generator.generate_unweighted_to(
        UnweightedConfig {
            events: 10_000,
            max_proposals: None,
            seed: 0,
            diagnostics: false,
            envelope: EnvelopeMode::Strict { max_weight: 6.5e-5 },
            envelope_overflow: EnvelopeOverflow::default(),
            memory: MemoryBudget::Auto,
        },
        Some(&evaluator),
        &mut sink,
    )?;

    println!("{res:?}");
    Ok(())
}
