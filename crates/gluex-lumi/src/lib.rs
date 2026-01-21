use chrono::{DateTime, TimeZone, Utc};
use gluex_ccdb::{
    context::Context as CCDBContext,
    prelude::{CCDBError, CCDB},
};
use gluex_core::{
    histograms::Histogram,
    run_periods::{resolve_rest_version, RestVersionError, RunPeriod},
    RestVersion, RunNumber,
};
use gluex_rcdb::prelude::{RCDBError, RCDB};
use serde::{Deserialize, Serialize};
use std::{collections::HashMap, path::Path, str::FromStr};
use thiserror::Error;

pub mod cli;

pub const BERILLIUM_RADIATION_LENGTH_METERS: f64 = 35.28e-2;

#[derive(Error, Debug)]
#[error("Unknown radiator: {0}")]
pub struct ConverterParseError(String);

#[derive(Debug, Copy, Clone)]
pub enum Converter {
    Retracted,
    Unknown,
    Be750um,
    Be75um,
    Be50um,
}
impl FromStr for Converter {
    type Err = ConverterParseError;

    fn from_str(s: &str) -> Result<Self, Self::Err> {
        match s {
            "Retracted" => Ok(Self::Retracted),
            "Unknown" => Ok(Self::Unknown),
            "Be 750um" => Ok(Self::Be750um),
            "Be 75um" => Ok(Self::Be75um),
            "Be 50um" => Ok(Self::Be50um),
            _ => Err(ConverterParseError(s.to_string())),
        }
    }
}
impl Converter {
    pub fn thickness(&self) -> Option<f64> {
        match self {
            Converter::Retracted => None,
            Converter::Unknown => None,
            Converter::Be750um => Some(750e-6),
            Converter::Be75um => Some(75e-6),
            Converter::Be50um => Some(50e-6),
        }
    }
    pub fn radiation_lengths(&self) -> Option<f64> {
        self.thickness()
            .map(|t| t / BERILLIUM_RADIATION_LENGTH_METERS)
    }
}

pub const TARGET_LENGTH_CM: f64 = 29.5;
pub const AVOGADRO_CONSTANT: f64 = 6.02214076e23;
const RP2019_11_OVERRIDE_START: RunNumber = 72436;
fn rp2019_11_override_timestamp() -> DateTime<Utc> {
    Utc.with_ymd_and_hms(2021, 4, 23, 0, 0, 1).unwrap()
}

#[derive(Debug, Copy, Clone, PartialEq, Eq, Hash)]
pub enum RestSelection {
    Current,
    Version(RestVersion),
}

#[derive(Debug)]
pub struct FluxCache {
    pub livetime_scaling: f64,
    pub pair_spectrometer_parameters: (f64, f64, f64),
    pub photon_endpoint_energy: f64,
    pub tagm_tagged_flux: Vec<(f64, f64, f64)>,
    pub tagm_scaled_energy_range: Vec<(f64, f64)>,
    pub tagh_tagged_flux: Vec<(f64, f64, f64)>,
    pub tagh_scaled_energy_range: Vec<(f64, f64)>,
    pub photon_endpoint_calibration: Option<f64>,
    pub target_scattering_centers: (f64, f64),
}

#[derive(Error, Debug)]
pub enum GlueXLumiError {
    #[error("{0}")]
    RCDBError(#[from] RCDBError),
    #[error("{0}")]
    CCDBError(#[from] CCDBError),
    #[error("{0}")]
    ConverterParseError(#[from] ConverterParseError),
    #[error("Missing endpoint calibration for run {0}")]
    MissingEndpointCalibration(RunNumber),
    #[error("{0}")]
    RestVersionError(#[from] RestVersionError),
}

fn get_flux_cache(
    run_period: RunPeriod,
    polarized: bool,
    timestamp: DateTime<Utc>,
    rcdb_path: impl AsRef<Path>,
    ccdb_path: impl AsRef<Path>,
) -> Result<HashMap<RunNumber, FluxCache>, GlueXLumiError> {
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
            &gluex_rcdb::context::Context::default()
                .with_run_range(run_period.min_run()..=run_period.max_run())
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
        .collect::<Result<HashMap<RunNumber, Converter>, ConverterParseError>>()?;
    let ccdb = CCDB::open(ccdb_path)?;
    let ccdb_context = gluex_ccdb::context::Context::default()
        .with_run_range(run_period.min_run()..run_period.max_run());
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
    let tagm_tagged_flux = fetch_tagm_tagged_flux(&ccdb, &ccdb_context)?;
    let mut tagm_scaled_energy_range =
        fetch_tagm_scaled_energy_range(&ccdb, &ccdb_context_restver)?;
    let tagh_tagged_flux = fetch_tagh_tagged_flux(&ccdb, &ccdb_context)?;
    let mut tagh_scaled_energy_range =
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
        let override_context = ccdb_context
            .clone()
            .with_timestamp(rp2019_11_override_timestamp());
        apply_run_override(
            &mut photon_endpoint_energy,
            fetch_photon_endpoint_energy(&ccdb, &override_context)?,
            RP2019_11_OVERRIDE_START,
            run_period.max_run(),
        );
        apply_run_override(
            &mut tagm_scaled_energy_range,
            fetch_tagm_scaled_energy_range(&ccdb, &override_context)?,
            RP2019_11_OVERRIDE_START,
            run_period.max_run(),
        );
        apply_run_override(
            &mut tagh_scaled_energy_range,
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
            let tagm_tagged_flux = tagm_tagged_flux.get(&r)?.to_vec();
            let tagm_scaled_energy_range = tagm_scaled_energy_range.get(&r)?.to_vec();
            let tagh_tagged_flux = tagh_tagged_flux.get(&r)?.to_vec();
            let tagh_scaled_energy_range = tagh_scaled_energy_range.get(&r)?.to_vec();
            let photon_endpoint_calibration = photon_endpoint_calibration.get(&r).copied();
            let target_scattering_centers = *target_scattering_centers.get(&r)?;
            Some((
                r,
                FluxCache {
                    livetime_scaling,
                    pair_spectrometer_parameters,
                    photon_endpoint_energy,
                    tagm_tagged_flux,
                    tagm_scaled_energy_range,
                    tagh_tagged_flux,
                    tagh_scaled_energy_range,
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

/// Construct tagged photon-flux and luminosity histograms for a set of run periods.
///
/// # Arguments
/// * `run_period_selection` - [`HashMap`] mapping [`RunPeriod`] values to [`RestSelection`] entries
///   that define the timestamp to use.
/// * `edges` - Photon-energy bin edges used to construct output [`Histogram`]s.
/// * `coherent_peak` - When true, only photons inside the per-run coherent peak contribute.
/// * `polarized` - Selects the polarized-flux calibration set when true.
/// * `rcdb_path` - Filesystem path to the RCDB SQLite database (any type implementing
///   `AsRef<Path>`).
/// * `ccdb_path` - Filesystem path to the CCDB SQLite database (any type implementing
///   `AsRef<Path>`).
/// * `exclude_runs` - Optional list of run numbers to exclude from the calculation.
///
/// # Returns
/// [`FluxHistograms`] for flux and tagged luminosity that satisfy the requested selections.
pub fn get_flux_histograms(
    run_period_selection: HashMap<RunPeriod, RestSelection>,
    edges: &[f64],
    coherent_peak: bool,
    polarized: bool,
    rcdb_path: impl AsRef<Path>,
    ccdb_path: impl AsRef<Path>,
    exclude_runs: Option<Vec<RunNumber>>,
) -> Result<FluxHistograms, GlueXLumiError> {
    let mut cache: HashMap<RunNumber, FluxCache> = HashMap::new();
    let mut tagged_flux_hist = Histogram::empty(edges);
    let mut tagm_flux_hist = Histogram::empty(edges);
    let mut tagh_flux_hist = Histogram::empty(edges);
    let mut tagged_luminosity_hist = Histogram::empty(edges);
    let mut run_periods: Vec<(RunPeriod, RestSelection)> = run_period_selection
        .iter()
        .map(|(rp, rest)| (*rp, *rest))
        .collect();
    run_periods.sort_unstable_by_key(|(rp, _)| *rp);
    let run_numbers: Vec<RunNumber> = run_periods
        .iter()
        .flat_map(|(rp, _)| rp.min_run()..=rp.max_run())
        .collect();
    let run_numbers = if let Some(exclude_runs) = exclude_runs {
        run_numbers
            .into_iter()
            .filter(|run| !exclude_runs.contains(run))
            .collect()
    } else {
        run_numbers
    };
    for (rp, selection) in run_periods.iter() {
        let timestamp = match selection {
            RestSelection::Current => Utc::now(),
            RestSelection::Version(rest_version) => {
                let resolved = resolve_rest_version(*rp, *rest_version)?;
                if resolved.requested != resolved.used {
                    eprintln!(
                        "Warning: REST ver{req:02} was not found for run period {} so ver{used:02} was used instead.",
                        rp.short_name(),
                        req = resolved.requested,
                        used = resolved.used
                    );
                }
                resolved.timestamp
            }
        };
        cache.extend(get_flux_cache(
            *rp, polarized, timestamp, &rcdb_path, &ccdb_path,
        )?);
    }
    for run in run_numbers {
        if let Some(data) = cache.get(&run) {
            let delta_e = match data.photon_endpoint_calibration {
                Some(calibration) => data.photon_endpoint_energy - calibration,
                None if run > 60000 => {
                    return Err(GlueXLumiError::MissingEndpointCalibration(run));
                }
                None => 0.0,
            };
            // Fill microscope
            for (tagged_flux, e_range) in data
                .tagm_tagged_flux
                .iter()
                .zip(data.tagm_scaled_energy_range.iter())
            {
                let energy = data.photon_endpoint_energy * (e_range.0 + e_range.1) * 0.5 + delta_e;

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
                    tagm_flux_hist.counts[ibin] += count;
                    tagm_flux_hist.errors[ibin] = tagm_flux_hist.errors[ibin].hypot(error);
                }
            }
            // Fill hodoscope
            for (tagged_flux, e_range) in data
                .tagh_tagged_flux
                .iter()
                .zip(data.tagh_scaled_energy_range.iter())
            {
                let energy = data.photon_endpoint_energy * (e_range.0 + e_range.1) * 0.5 + delta_e;

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
                    tagh_flux_hist.counts[ibin] += count;
                    tagh_flux_hist.errors[ibin] = tagh_flux_hist.errors[ibin].hypot(error);
                }
            }
            let (n_scattering_centers, n_scattering_centers_error) = data.target_scattering_centers;
            for ibin in 0..tagged_flux_hist.bins() {
                let count = tagged_flux_hist.counts[ibin];
                if count <= 0.0 {
                    continue;
                }
                let luminosity = count * n_scattering_centers / 1e12; // pb^-1
                let flux_error = tagged_flux_hist.errors[ibin] / count;
                let target_error = n_scattering_centers_error / n_scattering_centers;
                tagged_luminosity_hist.counts[ibin] = luminosity;
                tagged_luminosity_hist.errors[ibin] = luminosity * target_error.hypot(flux_error);
            }
        }
    }
    Ok(FluxHistograms {
        tagged_flux: tagged_flux_hist,
        tagm_flux: tagm_flux_hist,
        tagh_flux: tagh_flux_hist,
        tagged_luminosity: tagged_luminosity_hist,
    })
}
