//! `GlueX` photon-flux and tagged-luminosity utilities.
//!
//! This crate builds run-dependent flux and luminosity histograms from RCDB and
//! CCDB calibration sources.

use crate::ccdb::{CCDB, CCDBContext, CCDBError};
use crate::core::Histogram;
use crate::rcdb::{RCDB, RCDBContext, RCDBError};
use chrono::{DateTime, TimeZone, Utc};
use laddu::LadduPhysicsError;
use serde::{Deserialize, Serialize};
use std::{collections::HashMap, str::FromStr};
use thiserror::Error;

/// Radiation length of beryllium in meters.
pub const BERILLIUM_RADIATION_LENGTH_METERS: f64 = 35.28e-2;

pub use crate::core::{
    GlueXCoreError, RESTVersion, RESTVersionContext, RESTVersionSelection, RunNumber,
    run_periods::RunPeriod,
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
    /// Wrapper around [`LadduPhysicsError`].
    #[error(transparent)]
    LadduPhysics(#[from] LadduPhysicsError),
    /// No runs remained after selection and exclusions.
    #[error("at least one run number is required")]
    EmptyRunSelection,
    /// A selected production run lacks an input needed to compute luminosity.
    #[error("missing required luminosity input {input} for selected run {run}")]
    MissingRunInput {
        /// Run with incomplete data.
        run: RunNumber,
        /// Missing or unusable input.
        input: &'static str,
    },
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
pub(crate) struct LuminosityContext {
    runs: Vec<RunNumber>,
    rest_context: HashMap<RunPeriod, RESTVersionContext>,
    coherent_peak: bool,
    polarized: bool,
}

impl LuminosityContext {
    /// Create a context from explicit runs and resolved per-period REST contexts.
    ///
    /// # Errors
    /// Returns [`LuminosityError::EmptyRunSelection`] if `runs` is empty.
    pub fn new(
        runs: Vec<RunNumber>,
        rest_context: HashMap<RunPeriod, RESTVersionContext>,
    ) -> Result<Self, LuminosityError> {
        let mut runs = runs;
        runs.sort_unstable();
        runs.dedup();
        if runs.is_empty() {
            return Err(LuminosityError::EmptyRunSelection);
        }
        Ok(Self {
            runs,
            rest_context,
            coherent_peak: false,
            polarized: false,
        })
    }

    /// Sorted unique runs to include in the calculation.
    #[must_use]
    pub fn runs(&self) -> &[RunNumber] {
        &self.runs
    }

    /// Resolved per-run-period REST contexts.
    #[must_use]
    pub const fn rest_context(&self) -> &HashMap<RunPeriod, RESTVersionContext> {
        &self.rest_context
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
}

#[derive(Debug, Clone)]
/// Internal tagged flux and luminosity calculation engine used by `GlueX` Workflows.
pub(crate) struct Luminosity {
    readers: LuminosityReaders,
}

/// The existing backend readers form one reusable luminosity read session.
#[derive(Debug, Clone)]
struct LuminosityReaders {
    rcdb: RCDB,
    ccdb: CCDB,
}

impl Luminosity {
    pub(crate) const fn from_readers(rcdb: RCDB, ccdb: CCDB) -> Self {
        Self {
            readers: LuminosityReaders { rcdb, ccdb },
        }
    }
    fn readers(&self) -> LuminosityReaders {
        self.readers.clone()
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
    rest_context: &crate::core::RESTVersionContext,
    rcdb: &RCDB,
    ccdb: &CCDB,
    options: &crate::ExecutionOptions,
) -> Result<HashMap<RunNumber, FluxCache>, LuminosityError> {
    if runs.is_empty() {
        return Ok(HashMap::new());
    }
    let run_context = RCDBContext::default().with_runs(runs.iter().copied());
    let run_context = if polarized {
        run_context.filter(crate::rcdb::conditions::aliases::is_coherent_beam())
    } else {
        run_context
    };
    let polarimeter_converter: HashMap<RunNumber, Converter> = rcdb
        .fetch_with_options(["polarimeter_converter"], &run_context, options)?
        .into_iter()
        .map(|(r, pc_map)| {
            let mut converter = pc_map
                .get("polarimeter_converter")
                .ok_or(LuminosityError::MissingRunInput {
                    run: r,
                    input: "polarimeter converter",
                })?
                .as_string()
                .ok_or(LuminosityError::MissingRunInput {
                    run: r,
                    input: "text polarimeter converter",
                })?
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
    let ccdb_context = CCDBContext::default().with_runs(runs.iter().copied());
    let ccdb_context_restver = ccdb_context
        .clone()
        .with_variation(&rest_context.variation)
        .with_timestamp(rest_context.timestamp);
    let livetime_ratio: HashMap<RunNumber, f64> = ccdb
        .fetch_with_options(
            "/PHOTON_BEAM/pair_spectrometer/lumi/trig_live",
            &ccdb_context,
            options,
        )?
        .into_iter()
        .map(|(run, data)| {
            let missing = || LuminosityError::MissingRunInput {
                run,
                input: "pair-spectrometer livetime",
            };
            let livetime = data.column(1).ok_or_else(missing)?;
            let live = livetime.row(0).as_double().ok_or_else(missing)?;
            let total = livetime.row(3).as_double().ok_or_else(missing)?;
            if total <= 0.0 {
                return Err(missing());
            }
            Ok((run, live / total))
        })
        .collect::<Result<_, LuminosityError>>()?;
    let mut livetime_scaling: HashMap<RunNumber, f64> = HashMap::new();
    for (run, converter) in polarimeter_converter {
        let radiation_lengths =
            converter
                .radiation_lengths()
                .ok_or(LuminosityError::MissingRunInput {
                    run,
                    input: "usable polarimeter converter",
                })?;
        // See https://doi.org/10.1103/RevModPhys.46.815 Section IV parts B, C, and D
        livetime_scaling.insert(
            run,
            livetime_ratio
                .get(&run)
                .copied()
                .ok_or(LuminosityError::MissingRunInput {
                    run,
                    input: "pair-spectrometer livetime",
                })?
                * 9.0
                / (7.0 * radiation_lengths),
        );
    }
    let pair_spectrometer_parameters =
        fetch_pair_spectrometer_parameters(ccdb, &ccdb_context, options)?;
    let mut photon_endpoint_energy =
        fetch_photon_endpoint_energy(ccdb, &ccdb_context_restver, options)?;
    let microscope_tagged_flux = fetch_tagm_tagged_flux(ccdb, &ccdb_context, options)?;
    let mut microscope_scaled_energy_range =
        fetch_tagm_scaled_energy_range(ccdb, &ccdb_context_restver, options)?;
    let hodoscope_tagged_flux = fetch_tagh_tagged_flux(ccdb, &ccdb_context, options)?;
    let mut hodoscope_scaled_energy_range =
        fetch_tagh_scaled_energy_range(ccdb, &ccdb_context_restver, options)?;
    let mut photon_endpoint_calibration =
        fetch_photon_endpoint_calibration(ccdb, &ccdb_context_restver, options)?;
    // Density is in mg/cm^3, so to get the number of scattering centers, we multiply density by
    // the target length to get mg/cm^2, then we multiply by 1e-3 to get g/cm^2. We then multiply
    // by 1e-24 cm^2/barn to get g/barn, and finally by Avogadro's constant to get g/(mol * barn).
    // Finally, we divide by 1 g/mol (proton molar mass) to get protons/barn
    let factor = 1e-24 * AVOGADRO_CONSTANT * 1e-3 * TARGET_LENGTH_CM;
    let target_scattering_centers: HashMap<RunNumber, (f64, f64)> = ccdb
        .fetch_with_options("/TARGET/density", &ccdb_context, options)?
        .into_iter()
        .filter_map(|(r, d)| Some((r, (d.double(0, 0)? * factor, d.double(1, 0)? * factor))))
        .collect();

    if run_period == RunPeriod::RP2019_11 {
        let override_context = ccdb_context.with_timestamp(rp2019_11_override_timestamp());
        apply_run_override(
            &mut photon_endpoint_energy,
            fetch_photon_endpoint_energy(ccdb, &override_context, options)?,
            RP2019_11_OVERRIDE_START,
            run_period.max_run(),
        );
        apply_run_override(
            &mut microscope_scaled_energy_range,
            fetch_tagm_scaled_energy_range(ccdb, &override_context, options)?,
            RP2019_11_OVERRIDE_START,
            run_period.max_run(),
        );
        apply_run_override(
            &mut hodoscope_scaled_energy_range,
            fetch_tagh_scaled_energy_range(ccdb, &override_context, options)?,
            RP2019_11_OVERRIDE_START,
            run_period.max_run(),
        );
        apply_run_override(
            &mut photon_endpoint_calibration,
            fetch_photon_endpoint_calibration(ccdb, &override_context, options)?,
            RP2019_11_OVERRIDE_START,
            run_period.max_run(),
        );
    }
    let required = |run, input| LuminosityError::MissingRunInput { run, input };
    let mut cache = HashMap::new();
    for (run, livetime_scaling) in livetime_scaling {
        let pair_spectrometer_parameters = *pair_spectrometer_parameters
            .get(&run)
            .ok_or_else(|| required(run, "pair-spectrometer acceptance"))?;
        let photon_endpoint_energy = *photon_endpoint_energy
            .get(&run)
            .ok_or_else(|| required(run, "photon endpoint energy"))?;
        let photon_endpoint_calibration = photon_endpoint_calibration.get(&run).copied();
        let target_scattering_centers = *target_scattering_centers
            .get(&run)
            .ok_or_else(|| required(run, "target density"))?;
        cache.insert(
            run,
            FluxCache {
                livetime_scaling,
                pair_spectrometer_parameters,
                photon_endpoint_energy,
                tagm_tagged_flux: microscope_tagged_flux
                    .get(&run)
                    .ok_or_else(|| required(run, "TAGM tagged flux"))?
                    .clone(),
                tagm_scaled_energy_range: microscope_scaled_energy_range
                    .get(&run)
                    .ok_or_else(|| required(run, "TAGM scaled energy range"))?
                    .clone(),
                tagh_tagged_flux: hodoscope_tagged_flux
                    .get(&run)
                    .ok_or_else(|| required(run, "TAGH tagged flux"))?
                    .clone(),
                tagh_scaled_energy_range: hodoscope_scaled_energy_range
                    .get(&run)
                    .ok_or_else(|| required(run, "TAGH scaled energy range"))?
                    .clone(),
                photon_endpoint_calibration,
                target_scattering_centers,
            },
        );
    }
    Ok(cache)
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
    options: &crate::ExecutionOptions,
) -> Result<HashMap<RunNumber, (f64, f64, f64)>, CCDBError> {
    Ok(ccdb
        .fetch_with_options(
            "/PHOTON_BEAM/pair_spectrometer/lumi/PS_accept",
            context,
            options,
        )?
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
    options: &crate::ExecutionOptions,
) -> Result<HashMap<RunNumber, f64>, CCDBError> {
    Ok(ccdb
        .fetch_with_options("/PHOTON_BEAM/endpoint_energy", context, options)?
        .into_iter()
        .filter_map(|(r, d)| Some((r, d.value(0, 0)?.as_double()?)))
        .collect())
}

#[allow(clippy::type_complexity)]
fn fetch_tagm_tagged_flux(
    ccdb: &CCDB,
    context: &CCDBContext,
    options: &crate::ExecutionOptions,
) -> Result<HashMap<RunNumber, Vec<(f64, f64, f64)>>, CCDBError> {
    Ok(ccdb
        .fetch_with_options(
            "/PHOTON_BEAM/pair_spectrometer/lumi/tagm/tagged",
            context,
            options,
        )?
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
    options: &crate::ExecutionOptions,
) -> Result<HashMap<RunNumber, Vec<(f64, f64)>>, CCDBError> {
    Ok(ccdb
        .fetch_with_options(
            "/PHOTON_BEAM/microscope/scaled_energy_range",
            context,
            options,
        )?
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
    options: &crate::ExecutionOptions,
) -> Result<HashMap<RunNumber, Vec<(f64, f64, f64)>>, CCDBError> {
    Ok(ccdb
        .fetch_with_options(
            "/PHOTON_BEAM/pair_spectrometer/lumi/tagh/tagged",
            context,
            options,
        )?
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
    options: &crate::ExecutionOptions,
) -> Result<HashMap<RunNumber, Vec<(f64, f64)>>, CCDBError> {
    Ok(ccdb
        .fetch_with_options(
            "/PHOTON_BEAM/hodoscope/scaled_energy_range",
            context,
            options,
        )?
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
    options: &crate::ExecutionOptions,
) -> Result<HashMap<RunNumber, f64>, CCDBError> {
    Ok(ccdb
        .fetch_with_options("/PHOTON_BEAM/hodoscope/endpoint_calib", context, options)?
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
        options: &crate::ExecutionOptions,
    ) -> Result<FluxHistograms, LuminosityError> {
        let mut cache: HashMap<RunNumber, FluxCache> = HashMap::new();
        let coherent_peak = ctx.coherent_peak();
        let mut tagged_flux_hist = Histogram::empty_with_edges(edges.to_vec())?;
        let mut microscope_flux_hist = Histogram::empty_with_edges(edges.to_vec())?;
        let mut hodoscope_flux_hist = Histogram::empty_with_edges(edges.to_vec())?;
        let mut tagged_luminosity_hist = Histogram::empty_with_edges(edges.to_vec())?;
        let mut target_scattering_centers = None;
        let run_numbers: Vec<RunNumber> = ctx.runs().to_vec();
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
            let rest_context =
                ctx.rest_context()
                    .get(rp)
                    .ok_or(LuminosityError::MissingRunInput {
                        run: runs_by_period[rp][0],
                        input: "resolved reconstruction context",
                    })?;
            let readers = self.readers();
            cache.extend(get_flux_cache(
                *rp,
                runs_by_period
                    .get(rp)
                    .map_or(&[][..], |runs| runs.as_slice()),
                ctx.polarized(),
                rest_context,
                &readers.rcdb,
                &readers.ccdb,
                options,
            )?);
        }
        for run in run_numbers {
            if options.interrupted() {
                return Err(RCDBError::from(crate::execution::interrupted_error()).into());
            }
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
                            crate::core::run_periods::coherent_peak(run);
                        if energy < coherent_peak_low || energy > coherent_peak_high {
                            continue;
                        }
                    }
                    let acceptance =
                        pair_spectrometer_acceptance(energy, data.pair_spectrometer_parameters);
                    if acceptance <= 0.0 {
                        continue;
                    }
                    let count = tagged_flux.1 * data.livetime_scaling / acceptance;
                    let error = tagged_flux.2 * data.livetime_scaling / acceptance;
                    tagged_flux_hist.fill_weighted_with_error(energy, count, error)?;
                    microscope_flux_hist.fill_weighted_with_error(energy, count, error)?;
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
                            crate::core::run_periods::coherent_peak(run);
                        if energy < coherent_peak_low || energy > coherent_peak_high {
                            continue;
                        }
                    }
                    let acceptance =
                        pair_spectrometer_acceptance(energy, data.pair_spectrometer_parameters);
                    if acceptance <= 0.0 {
                        continue;
                    }
                    let count = tagged_flux.1 * data.livetime_scaling / acceptance;
                    let error = tagged_flux.2 * data.livetime_scaling / acceptance;
                    tagged_flux_hist.fill_weighted_with_error(energy, count, error)?;
                    hodoscope_flux_hist.fill_weighted_with_error(energy, count, error)?;
                }
                target_scattering_centers = Some(data.target_scattering_centers);
            } else {
                return Err(LuminosityError::MissingRunInput {
                    run,
                    input: "luminosity inputs after run constraints",
                });
            }
        }
        if let Some((n_scattering_centers, n_scattering_centers_error)) = target_scattering_centers
        {
            for ibin in 0..tagged_flux_hist.bins() {
                let flux = tagged_flux_hist.counts()[ibin];
                if flux <= 0.0 {
                    continue;
                }
                let luminosity = flux * n_scattering_centers / 1e12;
                let flux_error = tagged_flux_hist.errors()[ibin] / flux;
                let target_error = n_scattering_centers_error / n_scattering_centers;
                tagged_luminosity_hist.set_count(ibin, luminosity)?;
                tagged_luminosity_hist
                    .set_error(ibin, luminosity * target_error.hypot(flux_error))?;
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
