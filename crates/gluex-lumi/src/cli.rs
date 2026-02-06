use std::{collections::HashMap, env, ffi::OsString, io, path::PathBuf, str::FromStr};

use clap::{Args, CommandFactory, Parser, Subcommand};
use gluex_core::{
    run_periods::{parse_rest_version_selection, rest_versions_for, RESTVersionError, RunPeriod},
    RunNumber,
};
use serde_json::to_writer_pretty;
use strum::IntoEnumIterator;

use crate::{Luminosity, LuminosityContext, RESTVersionSelection};

#[derive(Parser)]
#[command(name = "gluex-lumi", version)]
struct Cli {
    #[command(flatten)]
    flux: FluxArgs,

    #[command(subcommand)]
    command: Option<Command>,
}

#[derive(Subcommand)]
enum Command {
    /// List known REST versions for one or all run periods.
    List { run_period: Option<RunPeriod> },
}

#[derive(Args, Debug, Clone)]
struct FluxArgs {
    /// Run period selection: <run>[=<rest_version>]
    /// Example: f18=0, s19=2, s23
    /// Unknown REST versions fall back to the current timestamp with a warning.
    #[arg(long = "run", value_parser = parse_run_pair)]
    runs: Vec<(RunPeriod, RESTVersionSelection)>,

    /// Number of bins
    #[arg(long)]
    bins: Option<usize>,

    /// Minimum bin edge
    #[arg(long)]
    min: Option<f64>,

    /// Maximum bin edge
    #[arg(long)]
    max: Option<f64>,

    /// Enable coherent peak
    #[arg(long)]
    coherent_peak: bool,

    /// Use polarized flux
    #[arg(long)]
    polarized: bool,

    /// RCDB path
    #[arg(long, env = "RCDB_CONNECTION")]
    rcdb: Option<PathBuf>,

    /// CCDB path
    #[arg(long, env = "CCDB_CONNECTION")]
    ccdb: Option<PathBuf>,
}

struct FluxConfig {
    run_selection: HashMap<RunPeriod, RESTVersionSelection>,
    bins: usize,
    min_edge: f64,
    max_edge: f64,
    coherent_peak: bool,
    polarized: bool,
    rcdb: PathBuf,
    ccdb: PathBuf,
}

fn parse_run_pair(s: &str) -> Result<(RunPeriod, RESTVersionSelection), String> {
    let (run_str, rest_version) = match s.split_once('=') {
        Some((r, v)) => (r, Some(v)),
        None => (s, None),
    };

    let run = RunPeriod::from_str(run_str).map_err(|e| format!("{e:?}"))?;

    let parsed = rest_version
        .map(|v| {
            v.parse::<usize>()
                .map_err(|_| format!("REST must be an unsigned integer, got '{v}'"))
        })
        .transpose()?;
    let selection = match parse_rest_version_selection(run, parsed) {
        Ok(selection) => selection,
        Err(RESTVersionError::UnknownRESTVersion { requested, .. }) => {
            eprintln!(
                "Warning: REST ver{requested:02} is not defined for run period {}. Using current timestamp instead.",
                run.short_name()
            );
            RESTVersionSelection::Current
        }
        Err(err) => return Err(err.to_string()),
    };

    Ok((run, selection))
}

fn print_rest_versions(run_period: RunPeriod) {
    println!(
        "REST versions for {} ({}-{}):",
        run_period.short_name(),
        run_period.min_run(),
        run_period.max_run()
    );
    match rest_versions_for(run_period) {
        Some(versions) if !versions.is_empty() => {
            for (version, timestamp) in versions {
                println!("  ver{version:02}: {}", timestamp.to_rfc3339());
            }
        }
        _ => println!("  (no REST versions available)"),
    }
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
    let args_vec: Vec<OsString> = args.into_iter().map(Into::into).collect();
    if args_vec.len() <= 1 {
        Cli::command().print_help()?;
        println!();
        return Ok(());
    }
    let cli = Cli::parse_from(args_vec);

    match cli.command {
        Some(Command::List { run_period }) => {
            if let Some(period) = run_period {
                print_rest_versions(period);
            } else {
                for (idx, period) in RunPeriod::iter().enumerate() {
                    if idx > 0 {
                        println!();
                    }
                    print_rest_versions(period);
                }
            }
            Ok(())
        }
        None => run_flux(cli.flux),
    }
}

pub fn cli() -> Result<(), Box<dyn std::error::Error>> {
    run_with_args(env::args_os())
}

impl FluxArgs {
    fn into_config(self) -> Result<FluxConfig, Box<dyn std::error::Error>> {
        let run_selection: HashMap<RunPeriod, RESTVersionSelection> =
            self.runs.into_iter().collect();
        if run_selection.is_empty() {
            return Err(io::Error::new(
                io::ErrorKind::InvalidInput,
                "at least one --run=<period>=<rest_version> argument is required",
            )
            .into());
        }
        let bins = self
            .bins
            .ok_or_else(|| io::Error::new(io::ErrorKind::InvalidInput, "--bins is required"))?;
        if bins == 0 {
            return Err(io::Error::new(
                io::ErrorKind::InvalidInput,
                "--bins must be greater than zero",
            )
            .into());
        }
        let min_edge = self
            .min
            .ok_or_else(|| io::Error::new(io::ErrorKind::InvalidInput, "--min is required"))?;
        let max_edge = self
            .max
            .ok_or_else(|| io::Error::new(io::ErrorKind::InvalidInput, "--max is required"))?;
        if max_edge <= min_edge {
            return Err(io::Error::new(
                io::ErrorKind::InvalidInput,
                "--max must be greater than --min",
            )
            .into());
        }
        let rcdb = self.rcdb.ok_or_else(|| {
            io::Error::new(
                io::ErrorKind::InvalidInput,
                "--rcdb is required (or set RCDB_CONNECTION)",
            )
        })?;
        let ccdb = self.ccdb.ok_or_else(|| {
            io::Error::new(
                io::ErrorKind::InvalidInput,
                "--ccdb is required (or set CCDB_CONNECTION)",
            )
        })?;

        Ok(FluxConfig {
            run_selection,
            bins,
            min_edge,
            max_edge,
            coherent_peak: self.coherent_peak,
            polarized: self.polarized,
            rcdb,
            ccdb,
        })
    }
}

fn run_flux(args: FluxArgs) -> Result<(), Box<dyn std::error::Error>> {
    let config = args.into_config()?;
    let FluxConfig {
        run_selection,
        bins,
        min_edge,
        max_edge,
        coherent_peak,
        polarized,
        rcdb,
        ccdb,
    } = config;

    let edges = uniform_edges(bins, min_edge, max_edge);
    let runs: Vec<RunNumber> = run_selection
        .keys()
        .flat_map(|period| period.iter_runs())
        .collect();
    let ctx = LuminosityContext::new(runs, run_selection)?
        .with_coherent_peak(coherent_peak)
        .with_polarized(polarized);
    let lumi = Luminosity::new(rcdb, ccdb);
    let histos = lumi.fetch(&edges, &ctx)?;

    to_writer_pretty(std::io::stdout(), &histos)?;
    Ok(())
}
