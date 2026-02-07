use chrono::{DateTime, TimeZone, Utc};
use lazy_static::lazy_static;
use std::{collections::HashMap, str::FromStr};

use strum::{EnumIter, IntoEnumIterator};

use crate::{GlueXCoreError, RESTVersion, RunNumber};

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

/// REST version selection for run-period queries.
#[derive(Debug, Copy, Clone, PartialEq, Eq, Hash, Default)]
pub enum RESTVersionSelection {
    /// Use the current timestamp for the run period.
    #[default]
    Current,
    /// Use a specific REST version.
    Version(RESTVersion),
    /// Use a specific timestamp directly.
    Timestamp(DateTime<Utc>),
}

impl RESTVersionSelection {
    /// Returns a selection for a specific REST version after validating against known metadata.
    ///
    /// # Errors
    ///
    /// Returns an error if the requested REST version is not known for the given run period.
    pub fn try_new(
        run_period: RunPeriod,
        rest_version: RESTVersion,
    ) -> Result<Self, GlueXCoreError> {
        let Some(versions) = REST_VERSION_TIMESTAMPS.get(&run_period) else {
            return Err(GlueXCoreError::MissingRESTVersions(run_period));
        };
        if versions.contains_key(&rest_version) {
            Ok(Self::Version(rest_version))
        } else {
            Err(GlueXCoreError::UnknownRESTVersion {
                run_period,
                requested: rest_version,
            })
        }
    }

    /// Returns a selection for a specific timestamp.
    #[must_use]
    pub fn from_timestamp(timestamp: DateTime<Utc>) -> Self {
        Self::Timestamp(timestamp)
    }

    /// Resolve the timestamp for this selection within the given run period.
    ///
    /// # Errors
    ///
    /// Returns an error if the requested REST version is not defined for the run period or
    /// if the run period has no REST metadata.
    pub fn resolve_timestamp(self, run_period: RunPeriod) -> Result<DateTime<Utc>, GlueXCoreError> {
        match self {
            Self::Current => Ok(Utc::now()),
            Self::Timestamp(timestamp) => Ok(timestamp),
            Self::Version(rest_version) => {
                let rest_versions = REST_VERSION_TIMESTAMPS
                    .get(&run_period)
                    .ok_or(GlueXCoreError::MissingRESTVersions(run_period))?;
                rest_versions.get(&rest_version).copied().ok_or(
                    GlueXCoreError::UnknownRESTVersion {
                        run_period,
                        requested: rest_version,
                    },
                )
            }
        }
    }
}

impl TryFrom<(RunPeriod, RESTVersion)> for RESTVersionSelection {
    type Error = GlueXCoreError;

    fn try_from(value: (RunPeriod, RESTVersion)) -> Result<Self, Self::Error> {
        RESTVersionSelection::try_new(value.0, value.1)
    }
}

impl TryFrom<(RunPeriod, &RESTVersion)> for RESTVersionSelection {
    type Error = GlueXCoreError;

    fn try_from(value: (RunPeriod, &RESTVersion)) -> Result<Self, Self::Error> {
        RESTVersionSelection::try_new(value.0, *value.1)
    }
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

    pub fn run_range(&self) -> std::ops::RangeInclusive<RunNumber> {
        self.min_run()..=self.max_run()
    }

    pub fn contains(&self, run_number: RunNumber) -> bool {
        self.run_range().contains(&run_number)
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

impl FromStr for RunPeriod {
    type Err = GlueXCoreError;

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
            _ => Err(GlueXCoreError::RunPeriodParse(s.to_string())),
        }
    }
}

impl TryFrom<RunNumber> for RunPeriod {
    type Error = GlueXCoreError;

    fn try_from(value: RunNumber) -> Result<Self, Self::Error> {
        RunPeriod::iter()
            .find(|rp: &RunPeriod| value >= rp.min_run() && value <= rp.max_run())
            .ok_or(GlueXCoreError::UnknownRunPeriod(value))
    }
}

lazy_static! {
    /// REST version timestamps sourced from hallddb
pub static ref REST_VERSION_TIMESTAMPS: HashMap<RunPeriod, HashMap<RESTVersion, DateTime<Utc>>> = {
        let mut m = HashMap::new();
        let mut m_s16 = HashMap::new();
        m_s16.insert(1, Utc.with_ymd_and_hms(2016, 7, 5, 14, 20, 0).unwrap());
        m_s16.insert(2, Utc.with_ymd_and_hms(2016, 9, 2, 14, 42, 0).unwrap());
        m_s16.insert(3, Utc.with_ymd_and_hms(2016, 11, 4, 14, 57, 0).unwrap());
        m_s16.insert(4, Utc.with_ymd_and_hms(2017, 5, 19, 11, 58, 0).unwrap());
        m_s16.insert(5, Utc.with_ymd_and_hms(2018, 1, 24, 17, 10, 0).unwrap());
        m_s16.insert(6, Utc.with_ymd_and_hms(2018, 7, 27, 17, 14, 0).unwrap());
        m.insert(RunPeriod::RP2016_02, m_s16);
        let mut m_s17 = HashMap::new();
        m_s17.insert(1, Utc.with_ymd_and_hms(2017, 6, 12, 18, 2, 0).unwrap());
        m_s17.insert(2, Utc.with_ymd_and_hms(2017, 11, 27, 19, 5, 0).unwrap());
        m_s17.insert(3, Utc.with_ymd_and_hms(2018, 7, 27, 17, 14, 0).unwrap());
        m_s17.insert(4, Utc.with_ymd_and_hms(2020, 7, 24, 0, 0, 1).unwrap());
        m.insert(RunPeriod::RP2017_01, m_s17);
        let mut m_s18 = HashMap::new();
        m_s18.insert(0, Utc.with_ymd_and_hms(2018, 12, 29, 17, 52, 0).unwrap());
        m_s18.insert(1, Utc.with_ymd_and_hms(2018, 12, 29, 17, 52, 0).unwrap());
        m_s18.insert(2, Utc.with_ymd_and_hms(2019, 2, 14, 12, 0, 0).unwrap());
        m.insert(RunPeriod::RP2018_01, m_s18);
        let mut m_f18 = HashMap::new();
        m_f18.insert(0, Utc.with_ymd_and_hms(2019, 4, 24, 17, 18, 0).unwrap());
        m_f18.insert(1, Utc.with_ymd_and_hms(2019, 5, 16, 11, 4, 0).unwrap());
        m_f18.insert(2, Utc.with_ymd_and_hms(2019, 7, 21, 12, 0, 0).unwrap());
        m.insert(RunPeriod::RP2018_08, m_f18);
        let mut m_s19 = HashMap::new();
        m_s19.insert(1, Utc.with_ymd_and_hms(2019, 9, 13, 14, 41, 0).unwrap());
        m_s19.insert(2, Utc.with_ymd_and_hms(2019, 10, 16, 10, 55, 0).unwrap());
        m_s19.insert(7, Utc.with_ymd_and_hms(2022, 8, 10, 12, 0, 1).unwrap());
        m.insert(RunPeriod::RP2019_01, m_s19);
        let mut m_s20 = HashMap::new();
        m_s20.insert(1, Utc.with_ymd_and_hms(2020, 7, 24, 0, 0, 1).unwrap());
        m.insert(RunPeriod::RP2019_11, m_s20);
        let mut m_src = HashMap::new();
        m_src.insert(2, Utc.with_ymd_and_hms(2022, 12, 14, 0, 0, 1).unwrap());
        m.insert(RunPeriod::RP2021_08, m_src);
        let mut m_cpp_npp = HashMap::new();
        m_cpp_npp.insert(1, Utc.with_ymd_and_hms(2022, 8, 10, 0, 0, 1).unwrap());
        m_cpp_npp.insert(2, Utc.with_ymd_and_hms(2024, 2, 23, 0, 0, 1).unwrap());
        m_cpp_npp.insert(3, Utc.with_ymd_and_hms(2025, 7, 18, 0, 0, 1).unwrap());
        m_cpp_npp.insert(4, Utc.with_ymd_and_hms(2025, 7, 18, 0, 0, 1).unwrap());
        m.insert(RunPeriod::RP2021_11, m_cpp_npp);
        let mut m_s22 = HashMap::new();
        m_s22.insert(1, Utc.with_ymd_and_hms(2024, 6, 24, 0, 0, 1).unwrap());
        m.insert(RunPeriod::RP2022_05, m_s22);
        let mut m_f22 = HashMap::new();
        m_f22.insert(1, Utc.with_ymd_and_hms(2024, 8, 31, 16, 13, 8).unwrap());
        m.insert(RunPeriod::RP2022_08, m_f22);
        let mut m_s23 = HashMap::new();
        m_s23.insert(1, Utc.with_ymd_and_hms(2023, 12, 7, 0, 0, 1).unwrap());
        m_s23.insert(2, Utc.with_ymd_and_hms(2023, 12, 7, 0, 0, 1).unwrap());
        m_s23.insert(3, Utc.with_ymd_and_hms(2024, 1, 21, 16, 0, 1).unwrap());
        m_s23.insert(4, Utc.with_ymd_and_hms(2025, 5, 10, 0, 0, 1).unwrap());
        m.insert(RunPeriod::RP2023_01, m_s23);
        let mut m_s25 = HashMap::new();
        m_s25.insert(1, Utc.with_ymd_and_hms(2025, 8, 27, 12, 0, 1).unwrap());
        m_s25.insert(2, Utc.with_ymd_and_hms(2025, 10, 19, 2, 0, 1).unwrap());
        m.insert(RunPeriod::RP2025_01, m_s25);
        m
    };
}

/// Return the available REST versions and timestamps for `run_period` ordered by version.
pub fn rest_versions_for(run_period: RunPeriod) -> Option<Vec<(RESTVersion, DateTime<Utc>)>> {
    let mut versions: Vec<(RESTVersion, DateTime<Utc>)> = REST_VERSION_TIMESTAMPS
        .get(&run_period)?
        .iter()
        .map(|(&version, &timestamp)| (version, timestamp))
        .collect();
    versions.sort_unstable_by_key(|(version, _)| *version);
    Some(versions)
}

/// Parse an optional REST version for the given run period into a selection.
pub fn parse_rest_version_selection(
    run_period: RunPeriod,
    rest_version: Option<RESTVersion>,
) -> Result<RESTVersionSelection, GlueXCoreError> {
    match rest_version {
        Some(version) => RESTVersionSelection::try_new(run_period, version),
        None => Ok(RESTVersionSelection::Current),
    }
}
