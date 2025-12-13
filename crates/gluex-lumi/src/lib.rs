use chrono::{DateTime, Utc};
use gluex_ccdb::prelude::{CCDBError, CCDB};
use gluex_core::{
    histograms::Histogram,
    run_periods::{RunPeriod, REST_VERSION_TIMESTAMPS},
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

#[derive(Debug)]
pub struct FluxCache {
    pub livetime_scaling: f64,
    pub pair_spectrometer_parameters: (f64, f64, f64),
    pub photon_endpoint_energy: f64,
    pub tagm_tagged_flux: Vec<(f64, f64, f64)>,
    pub tagm_scaled_energy_range: Vec<(f64, f64)>,
    pub tagh_tagged_flux: Vec<(f64, f64, f64)>,
    pub tagh_scaled_energy_range: Vec<(f64, f64)>,
    pub photon_endpoint_calibration: f64,
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
}

fn get_flux_cache(
    run_period: RunPeriod,
    polarized: bool,
    timestamp: DateTime<Utc>,
    rcdb_path: impl AsRef<Path>,
    ccdb_path: impl AsRef<Path>,
) -> Result<HashMap<RunNumber, FluxCache>, GlueXLumiError> {
    dbg!("open");
    let rcdb = RCDB::open(rcdb_path)?;
    dbg!("here");
    let mut rcdb_filters = gluex_rcdb::conditions::aliases::approved_production(run_period);
    if polarized {
        rcdb_filters = gluex_rcdb::conditions::all([
            rcdb_filters,
            gluex_rcdb::conditions::aliases::is_coherent_beam(),
        ]);
    }
    dbg!("polarimeter_converter start");
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
    dbg!("polarimeter_converter");
    let ccdb = CCDB::open(ccdb_path)?;
    let ccdb_context = gluex_ccdb::context::Context::default()
        .with_run_range(run_period.min_run()..run_period.max_run())
        .with_timestamp(timestamp);
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
    dbg!("livetime_ratio");
    let pair_spectrometer_parameters: HashMap<RunNumber, (f64, f64, f64)> = ccdb
        .fetch(
            "/PHOTON_BEAM/pair_spectrometer/lumi/PS_accept",
            &ccdb_context,
        )?
        .into_iter()
        .filter_map(|(r, d)| {
            let row = d.row(0).ok()?;
            let pars = (row.double(0)?, row.double(1)?, row.double(2)?);
            Some((r, pars))
        })
        .collect();
    dbg!("pair_spectrometer_parameters");
    let photon_endpoint_energy: HashMap<RunNumber, f64> = ccdb
        .fetch("/PHOTON_BEAM/endpoint_energy", &ccdb_context)?
        .into_iter()
        .filter_map(|(r, d)| Some((r, d.value(0, 0)?.as_double()?)))
        .collect();
    dbg!("tagm_tagged_flux");
    let tagm_tagged_flux: HashMap<RunNumber, Vec<(f64, f64, f64)>> = ccdb
        .fetch(
            "/PHOTON_BEAM/pair_spectrometer/lumi/tagm/tagged",
            &ccdb_context,
        )?
        .into_iter()
        .map(|(r, d)| {
            (
                r,
                d.iter_rows()
                    .filter_map(|r| Some((r.double(0)?, r.double(1)?, r.double(2)?)))
                    .collect::<Vec<_>>(),
            )
        })
        .collect();
    dbg!("tagm_scaled_energy_range");
    let tagm_scaled_energy_range: HashMap<RunNumber, Vec<(f64, f64)>> = ccdb
        .fetch("/PHOTON_BEAM/microscope/scaled_energy_range", &ccdb_context)?
        .into_iter()
        .map(|(r, d)| {
            (
                r,
                d.iter_rows()
                    .filter_map(|r| Some((r.double(1)?, r.double(2)?)))
                    .collect::<Vec<_>>(),
            )
        })
        .collect();
    dbg!("tagh_tagged_flux");
    let tagh_tagged_flux: HashMap<RunNumber, Vec<(f64, f64, f64)>> = ccdb
        .fetch(
            "/PHOTON_BEAM/pair_spectrometer/lumi/tagh/tagged",
            &ccdb_context,
        )?
        .into_iter()
        .map(|(r, d)| {
            (
                r,
                d.iter_rows()
                    .filter_map(|r| Some((r.double(0)?, r.double(1)?, r.double(2)?)))
                    .collect::<Vec<_>>(),
            )
        })
        .collect();
    dbg!("tagh_scaled_energy_range");
    let tagh_scaled_energy_range: HashMap<RunNumber, Vec<(f64, f64)>> = ccdb
        .fetch("/PHOTON_BEAM/hodoscope/scaled_energy_range", &ccdb_context)?
        .into_iter()
        .map(|(r, d)| {
            (
                r,
                d.iter_rows()
                    .filter_map(|r| Some((r.double(1)?, r.double(2)?)))
                    .collect::<Vec<_>>(),
            )
        })
        .collect();
    dbg!("photon_endpoint_calibration");
    let photon_endpoint_calibration: HashMap<RunNumber, f64> = ccdb
        .fetch("/PHOTON_BEAM/hodoscope/endpoint_calib", &ccdb_context)?
        .into_iter()
        .filter_map(|(r, d)| Some((r, d.double(0, 0)?)))
        .collect();
    // Density is in mg/cm^3, so to get the number of scattering centers, we multiply density by
    // the target length to get mg/cm^2, then we multiply by 1e-3 to get g/cm^2. We then multiply
    // by 1e-24 cm^2/barn to get g/barn, and finally by Avogadro's constant to get g/(mol * barn).
    // Finally, we divide by 1 g/mol (proton molar mass) to get protons/barn
    let factor = 1e-24 * AVOGADRO_CONSTANT * 1e-3 * TARGET_LENGTH_CM;
    dbg!("target_scattering_centers");
    let target_scattering_centers: HashMap<RunNumber, (f64, f64)> = ccdb
        .fetch("/TARGET/density", &ccdb_context)?
        .into_iter()
        .filter_map(|(r, d)| Some((r, (d.double(0, 0)? * factor, d.double(1, 0)? * factor))))
        .collect();
    Ok(livetime_scaling
        .into_iter()
        .filter_map(|(r, livetime_scaling)| {
            Some((
                r,
                FluxCache {
                    livetime_scaling,
                    pair_spectrometer_parameters: *pair_spectrometer_parameters.get(&r)?,
                    photon_endpoint_energy: *photon_endpoint_energy.get(&r)?,
                    tagm_tagged_flux: tagm_tagged_flux.get(&r)?.to_vec(),
                    tagm_scaled_energy_range: tagm_scaled_energy_range.get(&r)?.to_vec(),
                    tagh_tagged_flux: tagh_tagged_flux.get(&r)?.to_vec(),
                    tagh_scaled_energy_range: tagh_scaled_energy_range.get(&r)?.to_vec(),
                    photon_endpoint_calibration: *photon_endpoint_calibration.get(&r)?,
                    target_scattering_centers: *target_scattering_centers.get(&r)?,
                },
            ))
        })
        .collect())
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct FluxHistograms {
    pub tagged_flux: Histogram,
    pub tagm_flux: Histogram,
    pub tagh_flux: Histogram,
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

fn get_timestamp(run_period: RunPeriod, rest_version: RestVersion) -> DateTime<Utc> {
    REST_VERSION_TIMESTAMPS
        .get(&run_period)
        .and_then(|m: &HashMap<RestVersion, DateTime<Utc>>| m.get(&rest_version))
        .copied()
        .unwrap_or(Utc::now())
}

pub fn get_flux_histograms(
    run_period_selection: HashMap<RunPeriod, RestVersion>,
    edges: &[f64],
    coherent_peak: bool,
    polarized: bool,
    rcdb_path: impl AsRef<Path>,
    ccdb_path: impl AsRef<Path>,
) -> Result<FluxHistograms, GlueXLumiError> {
    let mut cache: HashMap<RunNumber, FluxCache> = HashMap::new();
    let mut tagged_flux_hist = Histogram::empty(edges);
    let mut tagm_flux_hist = Histogram::empty(edges);
    let mut tagh_flux_hist = Histogram::empty(edges);
    let mut tagged_luminosity_hist = Histogram::empty(edges);
    let mut run_periods: Vec<RunPeriod> = run_period_selection.keys().copied().collect();
    run_periods.sort_unstable();
    let run_numbers: Vec<RunNumber> = run_periods
        .iter()
        .flat_map(|rp| rp.min_run()..=rp.max_run())
        .collect();
    for rp in run_periods.iter() {
        let rest_version = run_period_selection.get(rp).copied().unwrap_or_default();
        cache.extend(get_flux_cache(
            *rp,
            polarized,
            get_timestamp(*rp, rest_version),
            &rcdb_path,
            &ccdb_path,
        )?);
    }
    for run in run_numbers {
        if let Some(data) = cache.get(&run) {
            let delta_e = if run > 60000 {
                data.photon_endpoint_energy - data.photon_endpoint_calibration
            } else {
                0.0
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
