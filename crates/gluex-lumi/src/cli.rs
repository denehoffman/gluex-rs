use std::{collections::HashMap, env, ffi::OsString, path::PathBuf, str::FromStr};

use clap::Parser;
use gluex_core::run_periods::RunPeriod;
use serde_json::to_writer_pretty;

use crate::get_flux_histograms;

#[derive(Parser)]
#[command(name = "gluex-lumi", version)]
struct Cli {
    /// Run period selection: <run>[=<rest>]
    /// Example: f18, s19=2
    #[arg(long = "run", value_parser = parse_run_pair)]
    runs: Vec<(RunPeriod, usize)>,

    /// Number of bins
    #[arg(long)]
    bins: usize,

    /// Minimum bin edge
    #[arg(long)]
    min: f64,

    /// Maximum bin edge
    #[arg(long)]
    max: f64,

    /// Enable coherent peak
    #[arg(long)]
    coherent_peak: bool,

    /// Use polarized flux
    #[arg(long)]
    polarized: bool,

    /// RCDB path (or env RCDB_CONNECTION)
    #[arg(long, env = "RCDB_CONNECTION")]
    rcdb: PathBuf,

    /// CCDB path (or env CCDB_CONNECTION)
    #[arg(long, env = "CCDB_CONNECTION")]
    ccdb: PathBuf,
}

fn parse_run_pair(s: &str) -> Result<(RunPeriod, usize), String> {
    let (run_str, rest) = match s.split_once('=') {
        Some((r, v)) => (r, Some(v)),
        None => (s, None),
    };

    let run = RunPeriod::from_str(run_str).map_err(|e| format!("{e:?}"))?;

    let rest = match rest {
        Some(v) => v
            .parse::<usize>()
            .map_err(|_| format!("REST must be an unsigned integer, got '{v}'"))?,
        None => 0,
    };

    Ok((run, rest))
}

fn uniform_edges(bins: usize, min: f64, max: f64) -> Vec<f64> {
    let width = (max - min) / bins as f64;
    (0..=bins).map(|i| min + i as f64 * width).collect()
}

/// Execute the command-line interface with a custom argv iterator.
pub fn run_with_args<I, T>(args: I) -> Result<(), Box<dyn std::error::Error>>
where
    I: IntoIterator<Item = T>,
    T: Into<OsString> + Clone,
{
    let cli = Cli::parse_from(args);
    let run_period_selection: HashMap<RunPeriod, usize> = cli.runs.into_iter().collect();

    let edges = uniform_edges(cli.bins, cli.min, cli.max);

    let histos = get_flux_histograms(
        run_period_selection,
        &edges,
        cli.coherent_peak,
        cli.polarized,
        cli.rcdb,
        cli.ccdb,
    )?;

    to_writer_pretty(std::io::stdout(), &histos)?;
    Ok(())
}

pub fn cli() -> Result<(), Box<dyn std::error::Error>> {
    run_with_args(env::args_os())
}
