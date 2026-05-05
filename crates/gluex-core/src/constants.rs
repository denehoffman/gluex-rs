use crate::{Polarization, RunNumber, RunPeriod};
use lazy_static::lazy_static;
use std::collections::HashMap;

pub const MIN_RUN_NUMBER: RunNumber = 0;
pub const MAX_RUN_NUMBER: RunNumber = 2_147_483_647;

pub const RF_TIME_NS: f64 = 4.008016032;

lazy_static! {
    /// True polarization angles for each run period (in degrees)
    pub static ref POLARIZATION_ANGLES_DEG: HashMap<RunPeriod, HashMap<Polarization, f64>> = {
        let mut m = HashMap::new();
        let mut m_s17 = HashMap::new();
        m_s17.insert(Polarization::Para0, 1.8);
        m_s17.insert(Polarization::Perp45, 47.9);
        m_s17.insert(Polarization::Para90, 94.5);
        m_s17.insert(Polarization::Perp135, -41.6);
        m.insert(RunPeriod::RP2017_01, m_s17);
        let mut m_s18 = HashMap::new();
        m_s18.insert(Polarization::Para0, 4.1);
        m_s18.insert(Polarization::Perp45, 48.5);
        m_s18.insert(Polarization::Para90, 94.2);
        m_s18.insert(Polarization::Perp135, -42.4);
        m.insert(RunPeriod::RP2018_01, m_s18);
        let mut m_f18 = HashMap::new();
        m_f18.insert(Polarization::Para0, 3.3);
        m_f18.insert(Polarization::Perp45, 48.3);
        m_f18.insert(Polarization::Para90, 92.9);
        m_f18.insert(Polarization::Perp135, -42.1);
        m.insert(RunPeriod::RP2018_08, m_f18);
        let mut m_s20 = HashMap::new();
        m_s20.insert(Polarization::Para0, 1.4);
        m_s20.insert(Polarization::Perp45, 47.1);
        m_s20.insert(Polarization::Para90, 93.4);
        m_s20.insert(Polarization::Perp135, -42.2);
        m.insert(RunPeriod::RP2019_11, m_s20);
        m
    };
}
