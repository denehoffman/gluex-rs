//! `GlueX` photon-flux and tagged-luminosity utilities.
//!
//! This crate builds run-dependent flux and luminosity histograms from RCDB and
//! CCDB calibration sources.

use chrono::{DateTime, TimeZone, Utc};
use gluex_ccdb::{CCDB, CCDBContext, CCDBError};
use gluex_rcdb::{RCDB, RCDBContext, RCDBError};
use serde::{Deserialize, Serialize};
use std::{
    collections::{HashMap, HashSet},
    env,
    path::{Path, PathBuf},
    str::FromStr,
};
use thiserror::Error;

/// Command-line entry points for the `gluex-lumi` executable.
pub mod cli;

/// Radiation length of beryllium in meters.
pub const BERILLIUM_RADIATION_LENGTH_METERS: f64 = 35.28e-2;

pub use gluex_core::{
    GlueXCoreError, Histogram, RESTVersion, RESTVersionSelection, RunNumber, run_periods::RunPeriod,
};

#[derive(Error, Debug)]
/// Errors returned by luminosity context construction and histogram generation.
pub enum LuminosityError {
    /// Wrapper around [`RCDBError`].
    #[error(transparent)]
    RCDBError(#[from] RCDBError),
    /// Wrapper around [`CCDBError`].
    #[error(transparent)]
    CCDBError(#[from] CCDBError),
    /// Failed to parse or map a converter description from RCDB.
    #[error("unknown radiator: {0}")]
    UnknownRadiator(String),
    /// Endpoint calibration was required but unavailable for this run.
    #[error("Missing endpoint calibration for run {0}")]
    MissingEndpointCalibration(RunNumber),
    /// Wrapper around [`GlueXCoreError`].
    #[error(transparent)]
    GlueXCoreError(#[from] GlueXCoreError),
    /// No runs remained after selection and exclusions.
    #[error("at least one run number is required")]
    EmptyRunSelection,
}

#[derive(Debug, Copy, Clone)]
/// Polarimeter converter configuration used to compute radiation-length scaling.
pub enum Converter {
    /// No converter in beam.
    Retracted,
    /// Unknown converter state.
    Unknown,
    /// 750 um beryllium converter.
    Be750um,
    /// 75 um beryllium converter.
    Be75um,
    /// 50 um beryllium converter.
    Be50um,
}
impl FromStr for Converter {
    type Err = LuminosityError;

    fn from_str(s: &str) -> Result<Self, Self::Err> {
        match s {
            "Retracted" => Ok(Self::Retracted),
            "Unknown" => Ok(Self::Unknown),
            "Be 750um" => Ok(Self::Be750um),
            "Be 75um" => Ok(Self::Be75um),
            "Be 50um" => Ok(Self::Be50um),
            _ => Err(LuminosityError::UnknownRadiator(s.to_string())),
        }
    }
}
impl Converter {
    /// Converter thickness in meters.
    #[must_use]
    pub const fn thickness(&self) -> Option<f64> {
        match self {
            Self::Retracted | Self::Unknown => None,
            Self::Be750um => Some(750e-6),
            Self::Be75um => Some(75e-6),
            Self::Be50um => Some(50e-6),
        }
    }
    /// Converter thickness in units of radiation length.
    #[must_use]
    pub fn radiation_lengths(&self) -> Option<f64> {
        self.thickness()
            .map(|t| t / BERILLIUM_RADIATION_LENGTH_METERS)
    }
}

/// Nominal liquid-hydrogen target length in centimeters.
pub const TARGET_LENGTH_CM: f64 = 29.5;
/// Avogadro constant in mol^-1.
pub const AVOGADRO_CONSTANT: f64 = 6.022_140_76e23;
const RP2019_11_OVERRIDE_START: RunNumber = 72436;
fn rp2019_11_override_timestamp() -> DateTime<Utc> {
    Utc.with_ymd_and_hms(2021, 4, 23, 0, 0, 1).unwrap()
}

#[derive(Debug, Clone)]
/// Selection options used when computing flux and luminosity histograms.
pub struct LuminosityContext {
    runs: Vec<RunNumber>,
    rest_version: HashMap<RunPeriod, RESTVersionSelection>,
    coherent_peak: bool,
    polarized: bool,
    exclude_runs: Vec<RunNumber>,
}

impl LuminosityContext {
    /// Create a context from explicit runs and per-period REST version selection.
    ///
    /// # Errors
    /// Returns [`LuminosityError::EmptyRunSelection`] if `runs` is empty.
    pub fn new(
        runs: Vec<RunNumber>,
        rest_version: HashMap<RunPeriod, RESTVersionSelection>,
    ) -> Result<Self, LuminosityError> {
        let mut runs = runs;
        runs.sort_unstable();
        runs.dedup();
        if runs.is_empty() {
            return Err(LuminosityError::EmptyRunSelection);
        }
        Ok(Self {
            runs,
            rest_version,
            coherent_peak: false,
            polarized: false,
            exclude_runs: Vec::new(),
        })
    }

    /// Sorted unique runs to include in the calculation.
    #[must_use]
    pub fn runs(&self) -> &[RunNumber] {
        &self.runs
    }

    /// Per-run-period REST version selection map.
    #[must_use]
    pub const fn rest_version(&self) -> &HashMap<RunPeriod, RESTVersionSelection> {
        &self.rest_version
    }

    /// Whether coherent-peak-only flux should be used.
    #[must_use]
    pub const fn coherent_peak(&self) -> bool {
        self.coherent_peak
    }

    /// Whether polarized beam constraints and constants should be used.
    #[must_use]
    pub const fn polarized(&self) -> bool {
        self.polarized
    }

    /// Runs excluded after the primary run selection.
    #[must_use]
    pub fn exclude_runs(&self) -> &[RunNumber] {
        &self.exclude_runs
    }

    /// Replace the run list with a new sorted unique run set.
    ///
    /// # Errors
    /// Returns [`LuminosityError::EmptyRunSelection`] if `runs` is empty.
    pub fn with_runs(
        mut self,
        runs: impl IntoIterator<Item = RunNumber>,
    ) -> Result<Self, LuminosityError> {
        let mut run_list: Vec<RunNumber> = runs.into_iter().collect();
        run_list.sort_unstable();
        run_list.dedup();
        if run_list.is_empty() {
            return Err(LuminosityError::EmptyRunSelection);
        }
        self.runs = run_list;
        Ok(self)
    }

    /// Add runs to the existing run set.
    #[must_use]
    pub fn add_runs(mut self, runs: impl IntoIterator<Item = RunNumber>) -> Self {
        self.runs.extend(runs);
        self.runs.sort_unstable();
        self.runs.dedup();
        self
    }

    /// Add all runs from a run period to the existing run set.
    #[must_use]
    pub fn with_run_period(mut self, run_period: RunPeriod) -> Self {
        self.runs.extend(run_period.iter_runs());
        self.runs.sort_unstable();
        self.runs.dedup();
        self
    }

    /// Override REST version selection for a run period.
    #[must_use]
    pub fn with_rest_version(
        mut self,
        run_period: RunPeriod,
        selection: RESTVersionSelection,
    ) -> Self {
        self.rest_version.insert(run_period, selection);
        self
    }

    /// Enable or disable coherent-peak-only flux selection.
    #[must_use]
    pub const fn with_coherent_peak(mut self, enabled: bool) -> Self {
        self.coherent_peak = enabled;
        self
    }

    /// Enable or disable polarized beam selection.
    #[must_use]
    pub const fn with_polarized(mut self, enabled: bool) -> Self {
        self.polarized = enabled;
        self
    }

    /// Add runs that should be excluded from processing.
    #[must_use]
    pub fn with_exclude_runs(mut self, runs: impl IntoIterator<Item = RunNumber>) -> Self {
        self.exclude_runs.extend(runs);
        self.exclude_runs.sort_unstable();
        self.exclude_runs.dedup();
        self
    }
}

#[derive(Debug, Clone)]
/// Entry point for tagged flux and luminosity calculations.
pub struct Luminosity {
    rcdb: PathBuf,
    ccdb: PathBuf,
}

impl Default for Luminosity {
    fn default() -> Self {
        let rcdb = env::var("RCDB_CONNECTION")
            .expect("RCDB_CONNECTION is not set for Luminosity::default()");
        let ccdb = env::var("CCDB_CONNECTION")
            .expect("CCDB_CONNECTION is not set for Luminosity::default()");
        Self {
            rcdb: PathBuf::from(rcdb),
            ccdb: PathBuf::from(ccdb),
        }
    }
}

impl Luminosity {
    /// Create a calculator from RCDB and CCDB `SQLite` paths.
    pub fn new(rcdb: impl AsRef<Path>, ccdb: impl AsRef<Path>) -> Self {
        Self {
            rcdb: rcdb.as_ref().to_path_buf(),
            ccdb: ccdb.as_ref().to_path_buf(),
        }
    }
}

#[derive(Debug, Clone)]
/// Cached per-run CCDB/RCDB calibration data used to build histograms.
pub struct FluxCache {
    /// Combined livetime and converter scaling factor.
    pub livetime_scaling: f64,
    /// Pair-spectrometer acceptance parameters `(p0, p1, p2)`.
    pub pair_spectrometer_parameters: (f64, f64, f64),
    /// Photon endpoint energy in `GeV`.
    pub photon_endpoint_energy: f64,
    /// TAGM tagged flux rows `(column, flux, error)`.
    pub tagm_tagged_flux: Vec<(f64, f64, f64)>,
    /// TAGM scaled-energy ranges `(emin, emax)`.
    pub tagm_scaled_energy_range: Vec<(f64, f64)>,
    /// TAGH tagged flux rows `(counter, flux, error)`.
    pub tagh_tagged_flux: Vec<(f64, f64, f64)>,
    /// TAGH scaled-energy ranges `(emin, emax)`.
    pub tagh_scaled_energy_range: Vec<(f64, f64)>,
    /// Optional endpoint calibration correction in `GeV`.
    pub photon_endpoint_calibration: Option<f64>,
    /// Number of target scattering centers and uncertainty `(value, error)`.
    pub target_scattering_centers: (f64, f64),
}

#[allow(clippy::too_many_lines)]
fn get_flux_cache(
    run_period: RunPeriod,
    runs: &[RunNumber],
    polarized: bool,
    timestamp: DateTime<Utc>,
    rcdb_path: &Path,
    ccdb_path: &Path,
) -> Result<HashMap<RunNumber, FluxCache>, LuminosityError> {
    if runs.is_empty() {
        return Ok(HashMap::new());
    }
    let rcdb = RCDB::open(rcdb_path)?;
    let mut rcdb_filters = gluex_rcdb::conditions::aliases::approved_production(run_period);
    if polarized {
        rcdb_filters = gluex_rcdb::conditions::all([
            rcdb_filters,
            gluex_rcdb::conditions::aliases::is_coherent_beam(),
        ]);
    }
    let polarimeter_converter: HashMap<RunNumber, Converter> = rcdb
        .fetch(
            ["polarimeter_converter"],
            &RCDBContext::default()
                .with_runs(runs.iter().copied())
                .filter(rcdb_filters),
        )?
        .into_iter()
        .map(|(r, pc_map)| {
            let mut converter = pc_map["polarimeter_converter"]
                .as_string()
                .unwrap()
                .parse()?;
            if !matches!(
                converter,
                Converter::Be75um | Converter::Be750um | Converter::Be50um,
            ) && r > 10633
                && r < 10694
            {
                converter = Converter::Be75um; // no converter in RCDB but 75um found in logbook
            }
            Ok((r, converter))
        })
        .collect::<Result<HashMap<RunNumber, Converter>, LuminosityError>>()?;
    let ccdb = CCDB::open(ccdb_path)?;
    let ccdb_context = CCDBContext::default().with_runs(runs.iter().copied());
    let ccdb_context_restver = ccdb_context.clone().with_timestamp(timestamp);
    let livetime_ratio: HashMap<RunNumber, f64> = ccdb
        .fetch(
            "/PHOTON_BEAM/pair_spectrometer/lumi/trig_live",
            &ccdb_context,
        )?
        .into_iter()
        .filter_map(|(r, d)| {
            let livetime = d.column(1)?;
            let live = livetime.row(0).as_double()?;
            let total = livetime.row(3).as_double()?;
            Some((r, if total > 0.0 { live / total } else { 1.0 }))
        })
        .collect::<HashMap<_, _>>();
    let livetime_scaling: HashMap<RunNumber, f64> = polarimeter_converter
        .into_iter()
        .filter_map(|(r, c)| {
            // See https://doi.org/10.1103/RevModPhys.46.815 Section IV parts B, C, and D
            Some((
                r,
                livetime_ratio.get(&r).unwrap_or(&1.0) * 9.0 / (7.0 * c.radiation_lengths()?),
            ))
        })
        .collect();
    let pair_spectrometer_parameters = fetch_pair_spectrometer_parameters(&ccdb, &ccdb_context)?;
    let mut photon_endpoint_energy = fetch_photon_endpoint_energy(&ccdb, &ccdb_context_restver)?;
    let microscope_tagged_flux = fetch_tagm_tagged_flux(&ccdb, &ccdb_context)?;
    let mut microscope_scaled_energy_range =
        fetch_tagm_scaled_energy_range(&ccdb, &ccdb_context_restver)?;
    let hodoscope_tagged_flux = fetch_tagh_tagged_flux(&ccdb, &ccdb_context)?;
    let mut hodoscope_scaled_energy_range =
        fetch_tagh_scaled_energy_range(&ccdb, &ccdb_context_restver)?;
    let mut photon_endpoint_calibration =
        fetch_photon_endpoint_calibration(&ccdb, &ccdb_context_restver)?;
    // Density is in mg/cm^3, so to get the number of scattering centers, we multiply density by
    // the target length to get mg/cm^2, then we multiply by 1e-3 to get g/cm^2. We then multiply
    // by 1e-24 cm^2/barn to get g/barn, and finally by Avogadro's constant to get g/(mol * barn).
    // Finally, we divide by 1 g/mol (proton molar mass) to get protons/barn
    let factor = 1e-24 * AVOGADRO_CONSTANT * 1e-3 * TARGET_LENGTH_CM;
    let target_scattering_centers: HashMap<RunNumber, (f64, f64)> = ccdb
        .fetch("/TARGET/density", &ccdb_context)?
        .into_iter()
        .filter_map(|(r, d)| Some((r, (d.double(0, 0)? * factor, d.double(1, 0)? * factor))))
        .collect();

    if run_period == RunPeriod::RP2019_11 {
        let override_context = ccdb_context.with_timestamp(rp2019_11_override_timestamp());
        apply_run_override(
            &mut photon_endpoint_energy,
            fetch_photon_endpoint_energy(&ccdb, &override_context)?,
            RP2019_11_OVERRIDE_START,
            run_period.max_run(),
        );
        apply_run_override(
            &mut microscope_scaled_energy_range,
            fetch_tagm_scaled_energy_range(&ccdb, &override_context)?,
            RP2019_11_OVERRIDE_START,
            run_period.max_run(),
        );
        apply_run_override(
            &mut hodoscope_scaled_energy_range,
            fetch_tagh_scaled_energy_range(&ccdb, &override_context)?,
            RP2019_11_OVERRIDE_START,
            run_period.max_run(),
        );
        apply_run_override(
            &mut photon_endpoint_calibration,
            fetch_photon_endpoint_calibration(&ccdb, &override_context)?,
            RP2019_11_OVERRIDE_START,
            run_period.max_run(),
        );
    }
    Ok(livetime_scaling
        .into_iter()
        .filter_map(|(r, livetime_scaling)| {
            let pair_spectrometer_parameters = *pair_spectrometer_parameters.get(&r)?;
            let photon_endpoint_energy = *photon_endpoint_energy.get(&r)?;
            let photon_endpoint_calibration = photon_endpoint_calibration.get(&r).copied();
            let target_scattering_centers = *target_scattering_centers.get(&r)?;
            Some((
                r,
                FluxCache {
                    livetime_scaling,
                    pair_spectrometer_parameters,
                    photon_endpoint_energy,
                    tagm_tagged_flux: microscope_tagged_flux.get(&r)?.clone(),
                    tagm_scaled_energy_range: microscope_scaled_energy_range.get(&r)?.clone(),
                    tagh_tagged_flux: hodoscope_tagged_flux.get(&r)?.clone(),
                    tagh_scaled_energy_range: hodoscope_scaled_energy_range.get(&r)?.clone(),
                    photon_endpoint_calibration,
                    target_scattering_centers,
                },
            ))
        })
        .collect())
}

/// Photon flux and luminosity histograms aggregated across TAGM and TAGH detectors.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct FluxHistograms {
    /// Total photon flux summed over TAGM and TAGH detectors as a [`Histogram`].
    pub tagged_flux: Histogram,
    /// Photon flux measured by the microscope (TAGM) detector only as a [`Histogram`].
    pub tagm_flux: Histogram,
    /// Photon flux measured by the hodoscope (TAGH) detector only as a [`Histogram`].
    pub tagh_flux: Histogram,
    /// Tagged luminosity derived from the flux and scattering-center constants as a [`Histogram`].
    pub tagged_luminosity: Histogram,
}

fn pair_spectrometer_acceptance(x: f64, args: (f64, f64, f64)) -> f64 {
    let (p0, p1, p2) = args;
    if x > 2.0 * p1 && x < p1 + p2 {
        return p0 * (1.0 - 2.0 * p1 / x);
    }
    if x >= p1 + p2 {
        return p0 * (2.0 * p2 / x - 1.0);
    }
    0.0
}

fn fetch_pair_spectrometer_parameters(
    ccdb: &CCDB,
    context: &CCDBContext,
) -> Result<HashMap<RunNumber, (f64, f64, f64)>, CCDBError> {
    Ok(ccdb
        .fetch("/PHOTON_BEAM/pair_spectrometer/lumi/PS_accept", context)?
        .into_iter()
        .filter_map(|(r, d)| {
            let row = d.row(0).ok()?;
            Some((r, (row.double(0)?, row.double(1)?, row.double(2)?)))
        })
        .collect())
}

fn fetch_photon_endpoint_energy(
    ccdb: &CCDB,
    context: &CCDBContext,
) -> Result<HashMap<RunNumber, f64>, CCDBError> {
    Ok(ccdb
        .fetch("/PHOTON_BEAM/endpoint_energy", context)?
        .into_iter()
        .filter_map(|(r, d)| Some((r, d.value(0, 0)?.as_double()?)))
        .collect())
}

#[allow(clippy::type_complexity)]
fn fetch_tagm_tagged_flux(
    ccdb: &CCDB,
    context: &CCDBContext,
) -> Result<HashMap<RunNumber, Vec<(f64, f64, f64)>>, CCDBError> {
    Ok(ccdb
        .fetch("/PHOTON_BEAM/pair_spectrometer/lumi/tagm/tagged", context)?
        .into_iter()
        .map(|(r, d)| {
            (
                r,
                d.iter_rows()
                    .filter_map(|row| Some((row.double(0)?, row.double(1)?, row.double(2)?)))
                    .collect::<Vec<_>>(),
            )
        })
        .collect())
}

fn fetch_tagm_scaled_energy_range(
    ccdb: &CCDB,
    context: &CCDBContext,
) -> Result<HashMap<RunNumber, Vec<(f64, f64)>>, CCDBError> {
    Ok(ccdb
        .fetch("/PHOTON_BEAM/microscope/scaled_energy_range", context)?
        .into_iter()
        .map(|(r, d)| {
            (
                r,
                d.iter_rows()
                    .filter_map(|row| Some((row.double(1)?, row.double(2)?)))
                    .collect::<Vec<_>>(),
            )
        })
        .collect())
}

#[allow(clippy::type_complexity)]
fn fetch_tagh_tagged_flux(
    ccdb: &CCDB,
    context: &CCDBContext,
) -> Result<HashMap<RunNumber, Vec<(f64, f64, f64)>>, CCDBError> {
    Ok(ccdb
        .fetch("/PHOTON_BEAM/pair_spectrometer/lumi/tagh/tagged", context)?
        .into_iter()
        .map(|(r, d)| {
            (
                r,
                d.iter_rows()
                    .filter_map(|row| Some((row.double(0)?, row.double(1)?, row.double(2)?)))
                    .collect::<Vec<_>>(),
            )
        })
        .collect())
}

fn fetch_tagh_scaled_energy_range(
    ccdb: &CCDB,
    context: &CCDBContext,
) -> Result<HashMap<RunNumber, Vec<(f64, f64)>>, CCDBError> {
    Ok(ccdb
        .fetch("/PHOTON_BEAM/hodoscope/scaled_energy_range", context)?
        .into_iter()
        .map(|(r, d)| {
            (
                r,
                d.iter_rows()
                    .filter_map(|row| Some((row.double(1)?, row.double(2)?)))
                    .collect::<Vec<_>>(),
            )
        })
        .collect())
}

fn fetch_photon_endpoint_calibration(
    ccdb: &CCDB,
    context: &CCDBContext,
) -> Result<HashMap<RunNumber, f64>, CCDBError> {
    Ok(ccdb
        .fetch("/PHOTON_BEAM/hodoscope/endpoint_calib", context)?
        .into_iter()
        .filter_map(|(r, d)| Some((r, d.double(0, 0)?)))
        .collect())
}

fn apply_run_override<T>(
    target: &mut HashMap<RunNumber, T>,
    overrides: HashMap<RunNumber, T>,
    run_min: RunNumber,
    run_max: RunNumber,
) {
    for (run, value) in overrides {
        if run >= run_min && run <= run_max {
            target.insert(run, value);
        }
    }
}

impl Luminosity {
    /// Construct tagged photon-flux and luminosity histograms for a run context.
    ///
    /// # Arguments
    /// * `edges` - Photon-energy bin edges used to construct output [`Histogram`]s.
    /// * `ctx` - [`LuminosityContext`] defining runs, REST versions, and selection flags.
    ///
    /// # Returns
    /// [`FluxHistograms`] for flux and tagged luminosity that satisfy the requested selections.
    ///
    /// # Errors
    /// Returns a [`LuminosityError`] if RCDB/CCDB data cannot be fetched or the run
    /// selection is invalid after filtering.
    #[allow(clippy::too_many_lines)]
    pub fn fetch(
        &self,
        edges: &[f64],
        ctx: &LuminosityContext,
    ) -> Result<FluxHistograms, LuminosityError> {
        let mut cache: HashMap<RunNumber, FluxCache> = HashMap::new();
        let coherent_peak = ctx.coherent_peak();
        let mut tagged_flux_hist = Histogram::empty(edges)?;
        let mut microscope_flux_hist = Histogram::empty(edges)?;
        let mut hodoscope_flux_hist = Histogram::empty(edges)?;
        let mut tagged_luminosity_hist = Histogram::empty(edges)?;
        let mut run_numbers: Vec<RunNumber> = ctx.runs().to_vec();
        if !ctx.exclude_runs().is_empty() {
            let exclude_set: HashSet<RunNumber> = ctx.exclude_runs().iter().copied().collect();
            run_numbers.retain(|run| !exclude_set.contains(run));
        }
        if run_numbers.is_empty() {
            return Err(LuminosityError::EmptyRunSelection);
        }
        let mut runs_by_period: HashMap<RunPeriod, Vec<RunNumber>> = HashMap::new();
        for run in &run_numbers {
            let period = RunPeriod::try_from(*run)?;
            runs_by_period.entry(period).or_default().push(*run);
        }
        let mut run_periods: Vec<RunPeriod> = runs_by_period.keys().copied().collect();
        run_periods.sort_unstable();
        for rp in &run_periods {
            let selection = ctx
                .rest_version()
                .get(rp)
                .copied()
                .unwrap_or(RESTVersionSelection::Current);
            let timestamp = selection.resolve_timestamp(*rp)?;
            cache.extend(get_flux_cache(
                *rp,
                runs_by_period
                    .get(rp)
                    .map_or(&[][..], |runs| runs.as_slice()),
                ctx.polarized(),
                timestamp,
                &self.rcdb,
                &self.ccdb,
            )?);
        }
        for run in run_numbers {
            if let Some(data) = cache.get(&run) {
                let delta_e = match data.photon_endpoint_calibration {
                    Some(calibration) => data.photon_endpoint_energy - calibration,
                    None if run > 60000 => {
                        return Err(LuminosityError::MissingEndpointCalibration(run));
                    }
                    None => 0.0,
                };
                // Fill microscope
                for (tagged_flux, e_range) in data
                    .tagm_tagged_flux
                    .iter()
                    .zip(data.tagm_scaled_energy_range.iter())
                {
                    let energy = (data.photon_endpoint_energy * (e_range.0 + e_range.1))
                        .mul_add(0.5, delta_e);

                    if coherent_peak {
                        let (coherent_peak_low, coherent_peak_high) =
                            gluex_core::run_periods::coherent_peak(run);
                        if energy < coherent_peak_low || energy > coherent_peak_high {
                            continue;
                        }
                    }
                    let acceptance =
                        pair_spectrometer_acceptance(energy, data.pair_spectrometer_parameters);
                    if acceptance <= 0.0 {
                        continue;
                    }
                    if let Some(ibin) = tagged_flux_hist.get_index(energy) {
                        let count = tagged_flux.1 * data.livetime_scaling / acceptance;
                        let error = tagged_flux.2 * data.livetime_scaling / acceptance;
                        tagged_flux_hist.counts[ibin] += count;
                        tagged_flux_hist.errors[ibin] = tagged_flux_hist.errors[ibin].hypot(error);
                        microscope_flux_hist.counts[ibin] += count;
                        microscope_flux_hist.errors[ibin] =
                            microscope_flux_hist.errors[ibin].hypot(error);
                    }
                }
                // Fill hodoscope
                for (tagged_flux, e_range) in data
                    .tagh_tagged_flux
                    .iter()
                    .zip(data.tagh_scaled_energy_range.iter())
                {
                    let energy = (data.photon_endpoint_energy * (e_range.0 + e_range.1))
                        .mul_add(0.5, delta_e);

                    if coherent_peak {
                        let (coherent_peak_low, coherent_peak_high) =
                            gluex_core::run_periods::coherent_peak(run);
                        if energy < coherent_peak_low || energy > coherent_peak_high {
                            continue;
                        }
                    }
                    let acceptance =
                        pair_spectrometer_acceptance(energy, data.pair_spectrometer_parameters);
                    if acceptance <= 0.0 {
                        continue;
                    }
                    if let Some(ibin) = tagged_flux_hist.get_index(energy) {
                        let count = tagged_flux.1 * data.livetime_scaling / acceptance;
                        let error = tagged_flux.2 * data.livetime_scaling / acceptance;
                        tagged_flux_hist.counts[ibin] += count;
                        tagged_flux_hist.errors[ibin] = tagged_flux_hist.errors[ibin].hypot(error);
                        hodoscope_flux_hist.counts[ibin] += count;
                        hodoscope_flux_hist.errors[ibin] =
                            hodoscope_flux_hist.errors[ibin].hypot(error);
                    }
                }
                let (n_scattering_centers, n_scattering_centers_error) =
                    data.target_scattering_centers;
                for ibin in 0..tagged_flux_hist.bins() {
                    let count = tagged_flux_hist.counts[ibin];
                    if count <= 0.0 {
                        continue;
                    }
                    let luminosity = count * n_scattering_centers / 1e12; // pb^-1
                    let flux_error = tagged_flux_hist.errors[ibin] / count;
                    let target_error = n_scattering_centers_error / n_scattering_centers;
                    tagged_luminosity_hist.counts[ibin] = luminosity;
                    tagged_luminosity_hist.errors[ibin] =
                        luminosity * target_error.hypot(flux_error);
                }
            }
        }
        Ok(FluxHistograms {
            tagged_flux: tagged_flux_hist,
            tagm_flux: microscope_flux_hist,
            tagh_flux: hodoscope_flux_hist,
            tagged_luminosity: tagged_luminosity_hist,
        })
    }
}
