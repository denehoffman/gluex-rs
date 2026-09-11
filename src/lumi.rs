//! `GlueX` photon-flux and tagged-luminosity utilities.
//!
//! This crate builds run-dependent flux and luminosity histograms from RCDB and
//! CCDB calibration sources.

use crate::calibrations::{CalibrationCatalog, CalibrationPayload, CalibrationSeries};
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
    /// Coherent-photon energy window `(minimum, maximum)` in `GeV`, when requested.
    pub coherent_energy: Option<(f64, f64)>,
}

pub(crate) struct LuminosityBatch {
    pub histograms: HashMap<RunNumber, FluxHistograms>,
    pub missing: HashMap<RunNumber, &'static str>,
}

struct FluxCacheBatch {
    entries: HashMap<RunNumber, FluxCache>,
    missing: HashMap<RunNumber, &'static str>,
}

#[derive(Clone, Copy)]
struct FluxSelection {
    polarized: bool,
    coherent_peak: bool,
}

fn invalid_schema(path: &str, detail: &str) -> CCDBError {
    CCDBError::InvalidMetadata(format!("{path}: {detail}"))
}

fn collect_calibration(
    catalog: &CalibrationCatalog,
    path: &str,
    runs: &[RunNumber],
    context: &CCDBContext,
    options: &crate::ExecutionOptions,
) -> Result<CalibrationSeries, CCDBError> {
    let table = catalog
        .get(path)
        .ok_or_else(|| CCDBError::TableNotFoundError(path.to_owned()))?;
    Ok(table
        .for_runs(crate::RunSelection::runs(runs.iter().copied()))?
        .with_variation(context.variation.clone())
        .as_of(context.timestamp)
        .with_execution(options.clone())
        .collect()?)
}

fn required_double(
    data: &CalibrationPayload,
    column: usize,
    row: usize,
    path: &str,
) -> Result<f64, CCDBError> {
    data.double(column, row).ok_or_else(|| {
        invalid_schema(
            path,
            &format!("expected a double at row {row}, column {column}"),
        )
    })
}

#[allow(clippy::type_complexity)]
fn fetch_three_double_rows(
    catalog: &CalibrationCatalog,
    path: &str,
    runs: &[RunNumber],
    context: &CCDBContext,
    options: &crate::ExecutionOptions,
) -> Result<HashMap<RunNumber, Vec<(f64, f64, f64)>>, CCDBError> {
    let series = collect_calibration(catalog, path, runs, context, options)?;
    series
        .items()
        .map(|(&run, entry)| {
            let data = entry.payload();
            let rows = (0..data.n_rows())
                .map(|row| {
                    Ok((
                        required_double(data, 0, row, path)?,
                        required_double(data, 1, row, path)?,
                        required_double(data, 2, row, path)?,
                    ))
                })
                .collect::<Result<Vec<_>, CCDBError>>()?;
            Ok((run, rows))
        })
        .collect()
}

fn fetch_two_double_rows(
    catalog: &CalibrationCatalog,
    path: &str,
    runs: &[RunNumber],
    context: &CCDBContext,
    options: &crate::ExecutionOptions,
    first_column: usize,
    second_column: usize,
) -> Result<HashMap<RunNumber, Vec<(f64, f64)>>, CCDBError> {
    let series = collect_calibration(catalog, path, runs, context, options)?;
    series
        .items()
        .map(|(&run, entry)| {
            let data = entry.payload();
            let rows = (0..data.n_rows())
                .map(|row| {
                    Ok((
                        required_double(data, first_column, row, path)?,
                        required_double(data, second_column, row, path)?,
                    ))
                })
                .collect::<Result<Vec<_>, CCDBError>>()?;
            Ok((run, rows))
        })
        .collect()
}

#[allow(clippy::too_many_lines)]
fn get_flux_cache(
    run_period: RunPeriod,
    runs: &[RunNumber],
    selection: FluxSelection,
    rest_context: &crate::core::RESTVersionContext,
    rcdb: &RCDB,
    ccdb: &CCDB,
    options: &crate::ExecutionOptions,
) -> Result<FluxCacheBatch, LuminosityError> {
    if runs.is_empty() {
        return Ok(FluxCacheBatch {
            entries: HashMap::new(),
            missing: HashMap::new(),
        });
    }
    let run_context = RCDBContext::default().with_runs(runs.iter().copied());
    let run_context = if selection.polarized {
        run_context.filter(crate::rcdb::conditions::aliases::is_coherent_beam())
    } else {
        run_context
    };
    let converter_rows =
        rcdb.fetch_with_options(["polarimeter_converter"], &run_context, options)?;
    let mut polarimeter_converter = HashMap::new();
    for (r, pc_map) in converter_rows {
        let Some(value) = pc_map.get("polarimeter_converter") else {
            continue;
        };
        let text = value.as_string().ok_or_else(|| {
            CCDBError::InvalidMetadata(format!(
                "RCDB polarimeter_converter for run {r} is not text"
            ))
        })?;
        let mut converter: Converter = text.parse()?;
        if !matches!(
            converter,
            Converter::Be75um | Converter::Be750um | Converter::Be50um,
        ) && r > 10633
            && r < 10694
        {
            converter = Converter::Be75um; // no converter in RCDB but 75um found in logbook
        }
        polarimeter_converter.insert(r, converter);
    }
    let ccdb_context = ccdb.default_context(runs.iter().copied());
    let ccdb_context_restver = ccdb_context
        .clone()
        .with_variation(&rest_context.variation)
        .with_timestamp(rest_context.timestamp);
    let catalog = CalibrationCatalog::new(ccdb);
    let coherent_energies = if selection.coherent_peak {
        ccdb.coherent_peaks_with_options(runs, &ccdb_context, options)?
    } else {
        std::collections::BTreeMap::new()
    };
    let livetime_series = collect_calibration(
        &catalog,
        "/PHOTON_BEAM/pair_spectrometer/lumi/trig_live",
        runs,
        &ccdb_context,
        options,
    )?;
    let mut livetime_ratio = HashMap::new();
    for (&run, entry) in livetime_series.items() {
        let data = entry.payload();
        let live = required_double(data, 1, 0, livetime_series.provenance().table())?;
        let total = required_double(data, 1, 3, livetime_series.provenance().table())?;
        if total > 0.0 {
            livetime_ratio.insert(run, live / total);
        }
    }
    let mut livetime_scaling: HashMap<RunNumber, f64> = HashMap::new();
    for (run, converter) in polarimeter_converter {
        let Some(radiation_lengths) = converter.radiation_lengths() else {
            continue;
        };
        // See https://doi.org/10.1103/RevModPhys.46.815 Section IV parts B, C, and D
        let Some(ratio) = livetime_ratio.get(&run).copied() else {
            continue;
        };
        livetime_scaling.insert(run, ratio * 9.0 / (7.0 * radiation_lengths));
    }
    let pair_spectrometer_parameters =
        fetch_pair_spectrometer_parameters(&catalog, runs, &ccdb_context, options)?;
    let mut photon_endpoint_energy =
        fetch_photon_endpoint_energy(&catalog, runs, &ccdb_context_restver, options)?;
    let microscope_tagged_flux = fetch_tagm_tagged_flux(&catalog, runs, &ccdb_context, options)?;
    let mut microscope_scaled_energy_range =
        fetch_tagm_scaled_energy_range(&catalog, runs, &ccdb_context_restver, options)?;
    let hodoscope_tagged_flux = fetch_tagh_tagged_flux(&catalog, runs, &ccdb_context, options)?;
    let mut hodoscope_scaled_energy_range =
        fetch_tagh_scaled_energy_range(&catalog, runs, &ccdb_context_restver, options)?;
    let mut photon_endpoint_calibration =
        fetch_photon_endpoint_calibration(&catalog, runs, &ccdb_context_restver, options)?;
    // Density is in mg/cm^3, so to get the number of scattering centers, we multiply density by
    // the target length to get mg/cm^2, then we multiply by 1e-3 to get g/cm^2. We then multiply
    // by 1e-24 cm^2/barn to get g/barn, and finally by Avogadro's constant to get g/(mol * barn).
    // Finally, we divide by 1 g/mol (proton molar mass) to get protons/barn
    let factor = 1e-24 * AVOGADRO_CONSTANT * 1e-3 * TARGET_LENGTH_CM;
    let density_series =
        collect_calibration(&catalog, "/TARGET/density", runs, &ccdb_context, options)?;
    let mut target_scattering_centers = HashMap::new();
    for (&run, entry) in density_series.items() {
        let data = entry.payload();
        let density = required_double(data, 0, 0, density_series.provenance().table())?;
        let error = required_double(data, 1, 0, density_series.provenance().table())?;
        target_scattering_centers.insert(run, (density * factor, error * factor));
    }

    if run_period == RunPeriod::RP2019_11 {
        let override_context = ccdb_context.with_timestamp(rp2019_11_override_timestamp());
        apply_run_override(
            &mut photon_endpoint_energy,
            fetch_photon_endpoint_energy(&catalog, runs, &override_context, options)?,
            RP2019_11_OVERRIDE_START,
            run_period.max_run(),
        );
        apply_run_override(
            &mut microscope_scaled_energy_range,
            fetch_tagm_scaled_energy_range(&catalog, runs, &override_context, options)?,
            RP2019_11_OVERRIDE_START,
            run_period.max_run(),
        );
        apply_run_override(
            &mut hodoscope_scaled_energy_range,
            fetch_tagh_scaled_energy_range(&catalog, runs, &override_context, options)?,
            RP2019_11_OVERRIDE_START,
            run_period.max_run(),
        );
        apply_run_override(
            &mut photon_endpoint_calibration,
            fetch_photon_endpoint_calibration(&catalog, runs, &override_context, options)?,
            RP2019_11_OVERRIDE_START,
            run_period.max_run(),
        );
    }
    let mut cache = HashMap::new();
    let mut missing = HashMap::new();
    for &run in runs {
        let Some(&livetime_scaling) = livetime_scaling.get(&run) else {
            missing.insert(run, "polarimeter converter or pair-spectrometer livetime");
            continue;
        };
        let Some(&pair_spectrometer_parameters) = pair_spectrometer_parameters.get(&run) else {
            missing.insert(run, "pair-spectrometer acceptance");
            continue;
        };
        let Some(&photon_endpoint_energy) = photon_endpoint_energy.get(&run) else {
            missing.insert(run, "photon endpoint energy");
            continue;
        };
        let photon_endpoint_calibration = photon_endpoint_calibration.get(&run).copied();
        let Some(&target_scattering_centers) = target_scattering_centers.get(&run) else {
            missing.insert(run, "target density");
            continue;
        };
        let Some(microscope_flux_rows) = microscope_tagged_flux.get(&run).cloned() else {
            missing.insert(run, "TAGM tagged flux");
            continue;
        };
        let Some(microscope_energy_rows) = microscope_scaled_energy_range.get(&run).cloned() else {
            missing.insert(run, "TAGM scaled energy range");
            continue;
        };
        let Some(hodoscope_flux_rows) = hodoscope_tagged_flux.get(&run).cloned() else {
            missing.insert(run, "TAGH tagged flux");
            continue;
        };
        let Some(hodoscope_energy_rows) = hodoscope_scaled_energy_range.get(&run).cloned() else {
            missing.insert(run, "TAGH scaled energy range");
            continue;
        };
        let coherent_energy = if selection.coherent_peak {
            let Some(&window) = coherent_energies.get(&run) else {
                missing.insert(run, "coherent-energy window");
                continue;
            };
            Some(window)
        } else {
            None
        };
        cache.insert(
            run,
            FluxCache {
                livetime_scaling,
                pair_spectrometer_parameters,
                photon_endpoint_energy,
                tagm_tagged_flux: microscope_flux_rows,
                tagm_scaled_energy_range: microscope_energy_rows,
                tagh_tagged_flux: hodoscope_flux_rows,
                tagh_scaled_energy_range: hodoscope_energy_rows,
                photon_endpoint_calibration,
                target_scattering_centers,
                coherent_energy,
            },
        );
    }
    Ok(FluxCacheBatch {
        entries: cache,
        missing,
    })
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
    catalog: &CalibrationCatalog,
    runs: &[RunNumber],
    context: &CCDBContext,
    options: &crate::ExecutionOptions,
) -> Result<HashMap<RunNumber, (f64, f64, f64)>, CCDBError> {
    let path = "/PHOTON_BEAM/pair_spectrometer/lumi/PS_accept";
    let series = collect_calibration(catalog, path, runs, context, options)?;
    series
        .items()
        .map(|(&run, entry)| {
            let data = entry.payload();
            Ok((
                run,
                (
                    required_double(data, 0, 0, path)?,
                    required_double(data, 1, 0, path)?,
                    required_double(data, 2, 0, path)?,
                ),
            ))
        })
        .collect()
}

fn fetch_photon_endpoint_energy(
    catalog: &CalibrationCatalog,
    runs: &[RunNumber],
    context: &CCDBContext,
    options: &crate::ExecutionOptions,
) -> Result<HashMap<RunNumber, f64>, CCDBError> {
    let path = "/PHOTON_BEAM/endpoint_energy";
    let series = collect_calibration(catalog, path, runs, context, options)?;
    series
        .items()
        .map(|(&run, entry)| Ok((run, required_double(entry.payload(), 0, 0, path)?)))
        .collect()
}

#[allow(clippy::type_complexity)]
fn fetch_tagm_tagged_flux(
    catalog: &CalibrationCatalog,
    runs: &[RunNumber],
    context: &CCDBContext,
    options: &crate::ExecutionOptions,
) -> Result<HashMap<RunNumber, Vec<(f64, f64, f64)>>, CCDBError> {
    let path = "/PHOTON_BEAM/pair_spectrometer/lumi/tagm/tagged";
    fetch_three_double_rows(catalog, path, runs, context, options)
}

fn fetch_tagm_scaled_energy_range(
    catalog: &CalibrationCatalog,
    runs: &[RunNumber],
    context: &CCDBContext,
    options: &crate::ExecutionOptions,
) -> Result<HashMap<RunNumber, Vec<(f64, f64)>>, CCDBError> {
    let path = "/PHOTON_BEAM/microscope/scaled_energy_range";
    fetch_two_double_rows(catalog, path, runs, context, options, 1, 2)
}

#[allow(clippy::type_complexity)]
fn fetch_tagh_tagged_flux(
    catalog: &CalibrationCatalog,
    runs: &[RunNumber],
    context: &CCDBContext,
    options: &crate::ExecutionOptions,
) -> Result<HashMap<RunNumber, Vec<(f64, f64, f64)>>, CCDBError> {
    let path = "/PHOTON_BEAM/pair_spectrometer/lumi/tagh/tagged";
    fetch_three_double_rows(catalog, path, runs, context, options)
}

fn fetch_tagh_scaled_energy_range(
    catalog: &CalibrationCatalog,
    runs: &[RunNumber],
    context: &CCDBContext,
    options: &crate::ExecutionOptions,
) -> Result<HashMap<RunNumber, Vec<(f64, f64)>>, CCDBError> {
    let path = "/PHOTON_BEAM/hodoscope/scaled_energy_range";
    fetch_two_double_rows(catalog, path, runs, context, options, 1, 2)
}

fn fetch_photon_endpoint_calibration(
    catalog: &CalibrationCatalog,
    runs: &[RunNumber],
    context: &CCDBContext,
    options: &crate::ExecutionOptions,
) -> Result<HashMap<RunNumber, f64>, CCDBError> {
    let path = "/PHOTON_BEAM/hodoscope/endpoint_calib";
    let series = collect_calibration(catalog, path, runs, context, options)?;
    series
        .items()
        .map(|(&run, entry)| Ok((run, required_double(entry.payload(), 0, 0, path)?)))
        .collect()
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

fn empty_flux_histograms(edges: &[f64]) -> Result<FluxHistograms, LuminosityError> {
    Ok(FluxHistograms {
        tagged_flux: Histogram::empty_with_edges(edges.to_vec())?,
        tagm_flux: Histogram::empty_with_edges(edges.to_vec())?,
        tagh_flux: Histogram::empty_with_edges(edges.to_vec())?,
        tagged_luminosity: Histogram::empty_with_edges(edges.to_vec())?,
    })
}

#[allow(clippy::too_many_lines)]
fn flux_histograms_for_run(
    edges: &[f64],
    run: RunNumber,
    data: &FluxCache,
    coherent_peak: bool,
) -> Result<FluxHistograms, LuminosityError> {
    if data.tagm_tagged_flux.len() != data.tagm_scaled_energy_range.len() {
        return Err(invalid_schema(
            "TAGM luminosity inputs",
            "tagged-flux and scaled-energy rows are not aligned",
        )
        .into());
    }
    if data.tagh_tagged_flux.len() != data.tagh_scaled_energy_range.len() {
        return Err(invalid_schema(
            "TAGH luminosity inputs",
            "tagged-flux and scaled-energy rows are not aligned",
        )
        .into());
    }
    let mut histograms = empty_flux_histograms(edges)?;
    let delta_e = data
        .photon_endpoint_calibration
        .map_or(0.0, |calibration| data.photon_endpoint_energy - calibration);
    for (tagged_flux, e_range) in data
        .tagm_tagged_flux
        .iter()
        .zip(&data.tagm_scaled_energy_range)
    {
        let energy = (data.photon_endpoint_energy * (e_range.0 + e_range.1)).mul_add(0.5, delta_e);
        if coherent_peak {
            let (low, high) = data.coherent_energy.ok_or_else(|| {
                invalid_schema(
                    "/PHOTON_BEAM/coherent_energy",
                    &format!("missing resolved window for run {run}"),
                )
            })?;
            if energy < low || energy > high {
                continue;
            }
        }
        let acceptance = pair_spectrometer_acceptance(energy, data.pair_spectrometer_parameters);
        if acceptance <= 0.0 {
            continue;
        }
        let count = tagged_flux.1 * data.livetime_scaling / acceptance;
        let error = tagged_flux.2 * data.livetime_scaling / acceptance;
        histograms
            .tagged_flux
            .fill_weighted_with_error(energy, count, error)?;
        histograms
            .tagm_flux
            .fill_weighted_with_error(energy, count, error)?;
    }
    for (tagged_flux, e_range) in data
        .tagh_tagged_flux
        .iter()
        .zip(&data.tagh_scaled_energy_range)
    {
        let energy = (data.photon_endpoint_energy * (e_range.0 + e_range.1)).mul_add(0.5, delta_e);
        if coherent_peak {
            let (low, high) = data.coherent_energy.ok_or_else(|| {
                invalid_schema(
                    "/PHOTON_BEAM/coherent_energy",
                    &format!("missing resolved window for run {run}"),
                )
            })?;
            if energy < low || energy > high {
                continue;
            }
        }
        let acceptance = pair_spectrometer_acceptance(energy, data.pair_spectrometer_parameters);
        if acceptance <= 0.0 {
            continue;
        }
        let count = tagged_flux.1 * data.livetime_scaling / acceptance;
        let error = tagged_flux.2 * data.livetime_scaling / acceptance;
        histograms
            .tagged_flux
            .fill_weighted_with_error(energy, count, error)?;
        histograms
            .tagh_flux
            .fill_weighted_with_error(energy, count, error)?;
    }
    let (scattering_centers, scattering_centers_error) = data.target_scattering_centers;
    for index in 0..histograms.tagged_flux.bins() {
        let flux = histograms.tagged_flux.counts()[index];
        if flux <= 0.0 {
            continue;
        }
        let luminosity = flux * scattering_centers / 1e12;
        let flux_error = histograms.tagged_flux.errors()[index] / flux;
        let target_error = scattering_centers_error / scattering_centers;
        histograms.tagged_luminosity.set_count(index, luminosity)?;
        histograms
            .tagged_luminosity
            .set_error(index, luminosity * target_error.hypot(flux_error))?;
    }
    Ok(histograms)
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
    pub(crate) fn fetch_each(
        &self,
        edges: &[f64],
        ctx: &LuminosityContext,
        options: &crate::ExecutionOptions,
    ) -> Result<LuminosityBatch, LuminosityError> {
        let run_numbers = ctx.runs();
        if run_numbers.is_empty() {
            return Err(LuminosityError::EmptyRunSelection);
        }
        let mut runs_by_period: HashMap<RunPeriod, Vec<RunNumber>> = HashMap::new();
        for &run in run_numbers {
            runs_by_period
                .entry(RunPeriod::try_from(run)?)
                .or_default()
                .push(run);
        }
        let readers = self.readers();
        let mut cache = HashMap::new();
        let mut missing = HashMap::new();
        for (period, runs) in runs_by_period {
            let rest_context =
                ctx.rest_context()
                    .get(&period)
                    .ok_or(LuminosityError::MissingRunInput {
                        run: runs[0],
                        input: "resolved reconstruction context",
                    })?;
            let batch = get_flux_cache(
                period,
                &runs,
                FluxSelection {
                    polarized: ctx.polarized(),
                    coherent_peak: ctx.coherent_peak(),
                },
                rest_context,
                &readers.rcdb,
                &readers.ccdb,
                options,
            )?;
            cache.extend(batch.entries);
            missing.extend(batch.missing);
        }
        let mut histograms = HashMap::new();
        for &run in run_numbers {
            if options.interrupted() {
                return Err(RCDBError::from(crate::execution::interrupted_error()).into());
            }
            let Some(data) = cache.get(&run) else {
                missing
                    .entry(run)
                    .or_insert("luminosity inputs after run constraints");
                continue;
            };
            if data.photon_endpoint_calibration.is_none() && run > 60_000 {
                missing.insert(run, "photon endpoint calibration");
                continue;
            }
            histograms.insert(
                run,
                flux_histograms_for_run(edges, run, data, ctx.coherent_peak())?,
            );
        }
        Ok(LuminosityBatch {
            histograms,
            missing,
        })
    }
}
