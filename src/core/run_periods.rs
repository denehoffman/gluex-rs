use chrono::{DateTime, Utc};
use lazy_static::lazy_static;
use std::str::FromStr;

use strum::{EnumIter, IntoEnumIterator};

use crate::core::parsers::parse_timestamp;
use crate::core::{GlueXCoreError, RESTVersion, RunNumber};

const REST_VERSION_DATA: &str = include_str!("../../data/rest_versions.tsv");

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
    /// GlueX low-energy running
    RP2026_03,
    /// GlueX Phase II/JEF
    RP2026_06,
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

/// CCDB metadata associated with a reconstruction REST version.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct RESTVersionInfo {
    /// Run period reconstructed by this version.
    pub run_period: RunPeriod,
    /// Reconstruction revision number.
    pub version: RESTVersion,
    /// CCDB variation used during reconstruction.
    pub variation: String,
    /// CCDB calibration timestamp used during reconstruction.
    pub timestamp: DateTime<Utc>,
}

/// Resolved CCDB selection for a REST version request.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct RESTVersionContext {
    /// CCDB variation to query.
    pub variation: String,
    /// CCDB calibration timestamp to query.
    pub timestamp: DateTime<Utc>,
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
        if !REST_VERSION_CATALOG
            .iter()
            .any(|info| info.run_period == run_period)
        {
            return Err(GlueXCoreError::MissingRESTVersions(run_period));
        }
        if rest_version_info(run_period, rest_version).is_some() {
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
        Ok(self.resolve_context(run_period)?.timestamp)
    }

    /// Resolve the CCDB variation and timestamp for this selection.
    ///
    /// # Errors
    ///
    /// Returns an error if the requested REST version is not defined for the run period or
    /// if the run period has no REST metadata.
    pub fn resolve_context(
        self,
        run_period: RunPeriod,
    ) -> Result<RESTVersionContext, GlueXCoreError> {
        match self {
            Self::Current => Ok(RESTVersionContext {
                variation: "default".to_string(),
                timestamp: Utc::now(),
            }),
            Self::Timestamp(timestamp) => Ok(RESTVersionContext {
                variation: "default".to_string(),
                timestamp,
            }),
            Self::Version(rest_version) => rest_version_info(run_period, rest_version)
                .map(|info| RESTVersionContext {
                    variation: info.variation.clone(),
                    timestamp: info.timestamp,
                })
                .ok_or(GlueXCoreError::UnknownRESTVersion {
                    run_period,
                    requested: rest_version,
                }),
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
    /// Canonical run-period name used by Hall-D production metadata.
    #[must_use]
    pub const fn data_name(&self) -> &'static str {
        match self {
            Self::RP2016_02 => "RunPeriod-2016-02",
            Self::RP2017_01 => "RunPeriod-2017-01",
            Self::RP2018_01 => "RunPeriod-2018-01",
            Self::RP2018_08 => "RunPeriod-2018-08",
            Self::RP2019_01 => "RunPeriod-2019-01",
            Self::RP2019_11 => "RunPeriod-2019-11",
            Self::RP2021_08 => "RunPeriod-2021-08",
            Self::RP2021_11 => "RunPeriod-2021-11",
            Self::RP2022_05 => "RunPeriod-2022-05",
            Self::RP2022_08 => "RunPeriod-2022-08",
            Self::RP2023_01 => "RunPeriod-2023-01",
            Self::RP2025_01 => "RunPeriod-2025-01",
            Self::RP2026_03 => "RunPeriod-2026-03",
            Self::RP2026_06 => "RunPeriod-2026-06",
        }
    }

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
            Self::RP2026_03 => 140000,
            Self::RP2026_06 => 150000,
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
            Self::RP2026_03 => 149999,
            Self::RP2026_06 => 159999,
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
            Self::RP2026_03 => "2026-03",
            Self::RP2026_06 => "2026-06",
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

pub const GLUEX_PHASE_II: [RunPeriod; 4] = [
    RunPeriod::RP2019_11,
    RunPeriod::RP2023_01,
    RunPeriod::RP2025_01,
    RunPeriod::RP2026_06,
];

impl FromStr for RunPeriod {
    type Err = GlueXCoreError;

    fn from_str(s: &str) -> Result<Self, Self::Err> {
        let normalized = s.to_ascii_lowercase().replace('_', "-");
        match normalized.as_str() {
            "s16" | "2016-02" | "runperiod-2016-02" => Ok(Self::RP2016_02),
            "s17" | "2017-01" | "runperiod-2017-01" => Ok(Self::RP2017_01),
            "s18" | "2018-01" | "runperiod-2018-01" => Ok(Self::RP2018_01),
            "f18" | "2018-08" | "runperiod-2018-08" => Ok(Self::RP2018_08),
            "s19" | "2019-01" | "runperiod-2019-01" => Ok(Self::RP2019_01),
            "s20" | "2019-11" | "runperiod-2019-11" => Ok(Self::RP2019_11),
            "src" | "2021-08" | "runperiod-2021-08" => Ok(Self::RP2021_08),
            "cpp" | "npp" | "cpp/npp" | "2021-11" | "runperiod-2021-11" => Ok(Self::RP2021_11),
            "s22" | "2022-05" | "runperiod-2022-05" => Ok(Self::RP2022_05),
            "f22" | "2022-08" | "runperiod-2022-08" => Ok(Self::RP2022_08),
            "s23" | "2023-01" | "runperiod-2023-01" => Ok(Self::RP2023_01),
            "s25" | "2025-01" | "runperiod-2025-01" => Ok(Self::RP2025_01),
            "2026-03" | "runperiod-2026-03" => Ok(Self::RP2026_03),
            "2026-06" | "runperiod-2026-06" => Ok(Self::RP2026_06),
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
    static ref REST_VERSION_CATALOG: Vec<RESTVersionInfo> = {
        let mut catalog = REST_VERSION_DATA
            .lines()
            .skip(1)
            .filter(|line| !line.trim().is_empty())
            .filter_map(|line| {
                let mut fields = line.split('\t');
                let run_period = fields.next()?.parse::<RunPeriod>().ok()?;
                let version = fields
                    .next()
                    .expect("REST catalog row is missing its revision")
                    .parse::<RESTVersion>()
                    .expect("REST catalog revision is invalid");
                let variation = fields
                    .next()
                    .expect("REST catalog row is missing its variation")
                    .to_string();
                let timestamp = parse_timestamp(
                    fields
                        .next()
                        .expect("REST catalog row is missing its timestamp"),
                )
                .expect("REST catalog timestamp is invalid");
                Some(RESTVersionInfo {
                    run_period,
                    version,
                    variation,
                    timestamp,
                })
            })
            .collect::<Vec<_>>();
        catalog.sort_unstable_by_key(|info| (info.run_period, info.version));
        catalog
    };
}

fn rest_version_info(
    run_period: RunPeriod,
    version: RESTVersion,
) -> Option<&'static RESTVersionInfo> {
    REST_VERSION_CATALOG
        .binary_search_by_key(&(run_period, version), |info| {
            (info.run_period, info.version)
        })
        .ok()
        .map(|index| &REST_VERSION_CATALOG[index])
}

/// Return the available REST metadata for `run_period`, ordered by version.
#[must_use]
pub fn rest_version_info_for(run_period: RunPeriod) -> Vec<RESTVersionInfo> {
    REST_VERSION_CATALOG
        .iter()
        .filter(|info| info.run_period == run_period)
        .cloned()
        .collect()
}

/// Return the available REST versions and timestamps for `run_period` ordered by version.
pub fn rest_versions_for(run_period: RunPeriod) -> Option<Vec<(RESTVersion, DateTime<Utc>)>> {
    let versions = rest_version_info_for(run_period)
        .into_iter()
        .map(|info| (info.version, info.timestamp))
        .collect::<Vec<_>>();
    (!versions.is_empty()).then_some(versions)
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

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn canonical_run_period_names_round_trip() {
        for run_period in RunPeriod::iter() {
            assert_eq!(
                run_period.data_name().parse::<RunPeriod>().unwrap(),
                run_period
            );
        }
    }

    #[test]
    fn run_period_parser_accepts_dates_and_underscore_variants() {
        let cases = [
            ("2018-08", RunPeriod::RP2018_08),
            ("2018_08", RunPeriod::RP2018_08),
            ("RunPeriod_2018_08", RunPeriod::RP2018_08),
            ("runperiod_2025_01", RunPeriod::RP2025_01),
            ("2026-03", RunPeriod::RP2026_03),
            ("RunPeriod_2026_06", RunPeriod::RP2026_06),
        ];
        for (input, expected) in cases {
            assert_eq!(input.parse::<RunPeriod>().unwrap(), expected, "{input}");
        }
    }

    #[test]
    fn catalog_keys_are_unique() {
        assert!(REST_VERSION_CATALOG.windows(2).all(|pair| {
            (pair[0].run_period, pair[0].version) != (pair[1].run_period, pair[1].version)
        }));
    }

    #[test]
    fn resolves_legacy_default_context() {
        let resolved = RESTVersionSelection::try_new(RunPeriod::RP2018_08, 2)
            .unwrap()
            .resolve_context(RunPeriod::RP2018_08)
            .unwrap();
        assert_eq!(resolved.variation, "default");
        assert_eq!(
            resolved.timestamp,
            parse_timestamp("2019-07-21T12:00:00Z").unwrap()
        );
    }

    #[test]
    fn resolves_non_default_variation() {
        let resolved = RESTVersionSelection::try_new(RunPeriod::RP2017_01, 5)
            .unwrap()
            .resolve_context(RunPeriod::RP2017_01)
            .unwrap();
        assert_eq!(resolved.variation, "recon_2017_01_ver05");
        assert_eq!(
            resolved.timestamp,
            parse_timestamp("2025-11-26T13:41:24Z").unwrap()
        );
    }
}
