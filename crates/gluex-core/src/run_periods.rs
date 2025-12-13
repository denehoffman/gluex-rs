use chrono::{DateTime, TimeZone, Utc};
use lazy_static::lazy_static;
use std::{collections::HashMap, str::FromStr};

use strum::{EnumIter, IntoEnumIterator};
use thiserror::Error;

use crate::{RestVersion, RunNumber};

#[derive(Copy, Clone, Debug, EnumIter, PartialEq, Eq, Hash, PartialOrd, Ord)]
pub enum RunPeriod {
    /// Commisioning, 12 GeV
    RP2016_02,
    /// GlueX Phase I, 12 GeV
    RP2017_01,
    /// GlueX Phase I, 12 GeV
    RP2018_01,
    /// GlueX Phase I, 12 GeV / PrimEx Commissioning (Low Energy runs 51384-51457)
    RP2018_08,
    /// DIRC Commissioning/PrimEx
    RP2019_01,
    /// DIRC Commissioning/GlueX Phase II
    RP2019_11,
    /// PrimEx
    RP2021_08,
    /// SRC
    RP2021_11,
    /// CPP/NPP
    RP2022_05,
    /// PrimEx
    RP2022_08,
    /// GlueX Phase II
    RP2023_01,
    /// ECAL Commissioning/GlueX Phase II
    RP2025_01,
}

impl RunPeriod {
    pub fn min_run(&self) -> RunNumber {
        match self {
            Self::RP2016_02 => 10000,
            Self::RP2017_01 => 30000,
            Self::RP2018_01 => 40000,
            Self::RP2018_08 => 50000,
            Self::RP2019_01 => 60000,
            Self::RP2019_11 => 70000,
            Self::RP2021_08 => 80000,
            Self::RP2021_11 => 90000,
            Self::RP2022_05 => 100000,
            Self::RP2022_08 => 110000,
            Self::RP2023_01 => 120000,
            Self::RP2025_01 => 130000,
        }
    }

    pub fn max_run(&self) -> RunNumber {
        match self {
            Self::RP2016_02 => 19999,
            Self::RP2017_01 => 39999,
            Self::RP2018_01 => 49999,
            Self::RP2018_08 => 59999,
            Self::RP2019_01 => 69999,
            Self::RP2019_11 => 79999,
            Self::RP2021_08 => 89999,
            Self::RP2021_11 => 99999,
            Self::RP2022_05 => 109999,
            Self::RP2022_08 => 119999,
            Self::RP2023_01 => 129999,
            Self::RP2025_01 => 139999,
        }
    }

    pub fn short_name(&self) -> &str {
        match self {
            Self::RP2016_02 => "S16",
            Self::RP2017_01 => "S17",
            Self::RP2018_01 => "S18",
            Self::RP2018_08 => "F18",
            Self::RP2019_01 => "S19",
            Self::RP2019_11 => "S20",
            Self::RP2021_08 => "SRC",
            Self::RP2021_11 => "CPP/NPP",
            Self::RP2022_05 => "S22",
            Self::RP2022_08 => "F22",
            Self::RP2023_01 => "S23",
            Self::RP2025_01 => "S25",
        }
    }

    pub fn iter_runs(&self) -> impl Iterator<Item = RunNumber> {
        self.min_run()..=self.max_run()
    }
}

pub const GLUEX_PHASE_I: [RunPeriod; 3] = [
    RunPeriod::RP2017_01,
    RunPeriod::RP2018_01,
    RunPeriod::RP2018_08,
];

pub const GLUEX_PHASE_II: [RunPeriod; 3] = [
    RunPeriod::RP2019_11,
    RunPeriod::RP2023_01,
    RunPeriod::RP2025_01,
];

pub fn coherent_peak(run: RunNumber) -> (f64, f64) {
    if run < 2760 {
        (8.4, 9.0)
    } else if run < 4001 {
        (2.5, 3.0)
    } else if run < 30000 {
        (8.4, 9.0)
    } else if run < 70000 {
        (8.2, 8.8)
    } else if run < 100000 {
        (8.0, 8.6)
    } else if run < 110000 {
        (5.2, 5.7)
    } else {
        // NOTE: will need to update with later runs
        (8.0, 8.6)
    }
}

#[derive(Error, Debug)]
pub enum RunPeriodError {
    #[error("Run number {0} not in range of any known run period")]
    UnknownRunPeriodError(RunNumber),
    #[error("Could not parse run period from string {0}")]
    RunPeriodParseError(String),
}

impl FromStr for RunPeriod {
    type Err = RunPeriodError;

    fn from_str(s: &str) -> Result<Self, Self::Err> {
        match s.to_lowercase().as_str() {
            "s16" => Ok(Self::RP2016_02),
            "s17" => Ok(Self::RP2017_01),
            "s18" => Ok(Self::RP2018_01),
            "f18" => Ok(Self::RP2018_08),
            "s19" => Ok(Self::RP2019_01),
            "s20" => Ok(Self::RP2019_11),
            "src" => Ok(Self::RP2021_08),
            "cpp" | "npp" | "cpp/npp" => Ok(Self::RP2021_11),
            "s22" => Ok(Self::RP2022_05),
            "f22" => Ok(Self::RP2022_08),
            "s23" => Ok(Self::RP2023_01),
            "s25" => Ok(Self::RP2025_01),
            _ => Err(RunPeriodError::RunPeriodParseError(s.to_string())),
        }
    }
}

impl TryFrom<RunNumber> for RunPeriod {
    type Error = RunPeriodError;

    fn try_from(value: RunNumber) -> Result<Self, Self::Error> {
        RunPeriod::iter()
            .find(|rp: &RunPeriod| value >= rp.min_run() && value <= rp.max_run())
            .ok_or(RunPeriodError::UnknownRunPeriodError(value))
    }
}

fn _latest_utc(year: i32, month: u32, day: u32) -> DateTime<Utc> {
    Utc.with_ymd_and_hms(year, month, day, 23, 59, 59).unwrap()
}

lazy_static! {
    pub static ref REST_VERSION_TIMESTAMPS: HashMap<RunPeriod, HashMap<RestVersion, DateTime<Utc>>> = {
        // TODO: these are just some eyeballed values, we need the full table
        let mut m = HashMap::new();
        let mut m_s17 = HashMap::new();
        m_s17.insert(52, _latest_utc(2018, 12, 1));
        let mut m_s18 = HashMap::new();
        m_s18.insert(19, _latest_utc(2019, 8, 1));
        let mut m_f18 = HashMap::new();
        m_f18.insert(19, _latest_utc(2019, 11, 1));
        let mut m_s20 = HashMap::new();
        m_s20.insert(4, _latest_utc(2022, 6, 1));
        m.insert(RunPeriod::RP2017_01, m_s17);
        m.insert(RunPeriod::RP2018_01, m_s18);
        m.insert(RunPeriod::RP2018_08, m_f18);
        m.insert(RunPeriod::RP2019_11, m_s20);
        m
    };
}
