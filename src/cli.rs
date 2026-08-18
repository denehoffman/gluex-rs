//! Command-line interface for unified GlueX tools.

use std::{
    collections::HashMap, env, ffi::OsString, io, path::PathBuf, process::ExitCode, str::FromStr,
};

use crate::core::{
    GlueXCoreError, RunNumber,
    run_periods::{RunPeriod, coherent_peak, parse_rest_version_selection, rest_versions_for},
};
use crate::generation::{
    GenerationRunOptions,
    config::{GenerationConfig, validate_hddm_species},
    generate,
};
use crate::lumi::{Luminosity, LuminosityContext, RESTVersionSelection};
use clap::{
    Args, CommandFactory, Parser, Subcommand,
    builder::{Styles, styling::AnsiColor},
    error::ErrorKind,
};
use serde_json::to_writer_pretty;
use strum::IntoEnumIterator;

const STYLES: Styles = Styles::styled()
    .header(AnsiColor::Yellow.on_default().bold())
    .usage(AnsiColor::Yellow.on_default().bold())
    .literal(AnsiColor::Green.on_default().bold())
    .placeholder(AnsiColor::Green.on_default())
    .error(AnsiColor::Red.on_default());

#[derive(Parser)]
#[command(styles = STYLES)]
#[command(name = "gluex", version)]
struct Cli {
    #[command(subcommand)]
    command: Option<Command>,
}

#[derive(Subcommand)]
enum Command {
    /// Validate and run standalone Monte Carlo generation.
    Gen(GenArgs),
    /// Calculate photon flux and tagged luminosity histograms.
    Lumi(FluxArgs),
    /// Print reference metadata for `GlueX` analysis inputs.
    Info(InfoArgs),
}

#[derive(Args)]
struct GenArgs {
    #[command(subcommand)]
    command: GenCommand,
}

#[derive(Subcommand)]
enum GenCommand {
    /// Print the generation JSON Schema.
    Schema {
        /// Write the schema to a file instead of standard output.
        #[arg(long)]
        output: Option<PathBuf>,
    },
    /// Parse, semantically validate, and compile a generation file.
    Check {
        /// Generation manifest.
        config: PathBuf,
        /// Emit a machine-readable success result.
        #[arg(long)]
        json: bool,
    },
    /// Generate unweighted events from a validated manifest.
    Run {
        /// Generation manifest.
        config: PathBuf,
        /// Number of accepted events.
        #[arg(long)]
        events: usize,
        /// `GlueX` run number stored in HDDM.
        #[arg(long)]
        run_number: RunNumber,
        /// Deterministic generation and HDDM seed.
        #[arg(long, default_value_t = 0)]
        seed: u64,
        /// Destination HDDM file. Defaults to the manifest path with `.hddm`.
        #[arg(long)]
        output: Option<PathBuf>,
        /// First HDDM event number.
        #[arg(long, default_value_t = 0)]
        first_event: i32,
        /// Optional generation memory cap in bytes.
        #[arg(long)]
        memory: Option<u64>,
        /// Optional production proposal limit.
        #[arg(long)]
        max_proposals: Option<usize>,
        /// Manually supplied maximum target weight, overriding the manifest.
        ///
        /// Without this option, model-less generation uses a certified bound;
        /// model-backed generation estimates one from pilot proposals.
        #[arg(long)]
        max_weight: Option<f64>,
        /// Pilot proposals used to estimate a model-weighted envelope.
        #[arg(long)]
        pilot_proposals: Option<usize>,
        /// Scale applied to pilot estimates and grown envelopes.
        #[arg(long)]
        safety_scale: Option<f64>,
        /// Optional JSON generation report.
        #[arg(long)]
        report: Option<PathBuf>,
        /// Replace an existing HDDM or report file.
        #[arg(long)]
        force: bool,
    },
}

#[derive(Args)]
struct InfoArgs {
    #[command(subcommand)]
    command: InfoCommand,
}

#[derive(Subcommand)]
enum InfoCommand {
    /// List known REST versions, optionally limited to one run period.
    Rest { run_period: Option<RunPeriod> },
    /// List run periods, ranges, and coherent-peak bounds.
    Runs { run_period: Option<RunPeriod> },
}

#[derive(Args, Debug, Clone)]
struct FluxArgs {
    /// Run period selection: <run>[=<`rest_version`>]
    /// Example: f18=0, s19=2, s23
    /// Unknown REST versions fall back to the current timestamp with a warning.
    #[arg(long = "run", value_parser = parse_run_pair)]
    runs: Vec<(RunPeriod, RESTVersionSelection)>,

    /// Number of bins.
    #[arg(long, default_value_t = 60)]
    bins: usize,

    /// Minimum bin edge.
    #[arg(long, default_value_t = 6.0)]
    min: f64,

    /// Maximum bin edge.
    #[arg(long, default_value_t = 12.0)]
    max: f64,

    /// Select only data in the coherent peak.
    #[arg(long)]
    coherent_peak: bool,

    /// Include polarized runs only.
    #[arg(long)]
    polarized: bool,

    /// RCDB path.
    #[arg(long, env = "RCDB_CONNECTION")]
    rcdb: Option<PathBuf>,

    /// CCDB path.
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

    let run = RunPeriod::from_str(run_str).map_err(|err| format!("{err:?}"))?;
    let parsed = rest_version
        .map(|version| {
            version
                .parse::<usize>()
                .map_err(|_| format!("REST must be an unsigned integer, got '{version}'"))
        })
        .transpose()?;
    let selection = match parse_rest_version_selection(run, parsed) {
        Ok(selection) => selection,
        Err(GlueXCoreError::UnknownRESTVersion { requested, .. }) => {
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

fn print_run_period(run_period: RunPeriod) {
    let (coherent_min, coherent_max) = coherent_peak(run_period.min_run());
    println!(
        "{run_period:?} ({})\n  runs: {}-{}\n  coherent peak: {coherent_min:.1}-{coherent_max:.1} GeV",
        run_period.short_name(),
        run_period.min_run(),
        run_period.max_run()
    );
}

fn uniform_edges(bins: usize, min: f64, max: f64) -> Vec<f64> {
    let bins_u32 = u32::try_from(bins).expect("bins validated by FluxArgs::into_config");
    let width = (max - min) / f64::from(bins_u32);
    (0..=bins_u32)
        .map(|i| f64::from(i).mul_add(width, min))
        .collect()
}

fn lumi_error(error: impl std::fmt::Display) -> Box<dyn std::error::Error> {
    let mut command = Cli::command();
    let lumi_command = command
        .find_subcommand_mut("lumi")
        .expect("lumi subcommand exists");
    lumi_command.set_bin_name("gluex lumi");
    Box::new(lumi_command.error(ErrorKind::ValueValidation, error))
}

/// Execute the command-line interface with a custom argument iterator.
///
/// # Errors
/// Returns an error if argument parsing fails or command execution fails.
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
    let cli = Cli::try_parse_from(args_vec)?;

    match cli.command {
        Some(Command::Gen(args)) => run_gen(args),
        Some(Command::Lumi(args)) => run_flux(args).map_err(lumi_error),
        Some(Command::Info(args)) => match args.command {
            InfoCommand::Rest {
                run_period: Some(period),
            } => {
                print_rest_versions(period);
                Ok(())
            }
            InfoCommand::Rest { run_period: None } => {
                for (index, period) in RunPeriod::iter().enumerate() {
                    if index > 0 {
                        println!();
                    }
                    print_rest_versions(period);
                }
                Ok(())
            }
            InfoCommand::Runs {
                run_period: Some(period),
            } => {
                print_run_period(period);
                Ok(())
            }
            InfoCommand::Runs { run_period: None } => {
                for (index, period) in RunPeriod::iter().enumerate() {
                    if index > 0 {
                        println!();
                    }
                    print_run_period(period);
                }
                Ok(())
            }
        },
        None => {
            Cli::command().print_help()?;
            println!();
            Ok(())
        }
    }
}

#[allow(clippy::too_many_lines)]
fn run_gen(args: GenArgs) -> Result<(), Box<dyn std::error::Error>> {
    match args.command {
        GenCommand::Schema { output } => {
            let schema = GenerationConfig::json_schema();
            if let Some(path) = output {
                let file = std::fs::File::create(path)?;
                serde_json::to_writer_pretty(file, &schema)?;
            } else {
                serde_json::to_writer_pretty(io::stdout().lock(), &schema)?;
                println!();
            }
            Ok(())
        }
        GenCommand::Check { config, json } => {
            let source = std::fs::read_to_string(&config)?;
            let parsed = GenerationConfig::from_json(&source)?;
            let channel = parsed.to_channel()?;
            validate_hddm_species(&channel)?;
            parsed.validate_execution()?;
            if json {
                serde_json::to_writer_pretty(
                    io::stdout().lock(),
                    &serde_json::json!({
                        "valid": true,
                        "config_sha256": parsed.semantic_sha256()?,
                    }),
                )?;
                println!();
            } else {
                println!(
                    "{} is valid (sha256 {})",
                    config.display(),
                    parsed.semantic_sha256()?
                );
            }
            Ok(())
        }
        GenCommand::Run {
            config,
            events,
            run_number,
            seed,
            output,
            first_event,
            memory,
            max_proposals,
            max_weight,
            pilot_proposals,
            safety_scale,
            report,
            force,
        } => {
            let output = output.unwrap_or_else(|| config.with_extension("hddm"));
            require_new_output(&output, force)?;
            if let Some(report) = &report {
                require_new_output(report, force)?;
            }
            let source = std::fs::read_to_string(&config)?;
            let parsed = GenerationConfig::from_json(&source)?;
            let generation_report = generate(
                &parsed,
                &output,
                &GenerationRunOptions {
                    events,
                    run_number,
                    seed,
                    first_event_number: first_event,
                    memory,
                    max_proposals,
                    max_weight,
                    pilot_proposals,
                    safety_scale,
                    force,
                },
            )
            .map_err(|error| io::Error::other(error.to_string()))?;
            if let Some(report_path) = report {
                write_json_transactional(&report_path, &generation_report, force)?;
            }
            if generation_report.envelope_updates > 0 {
                eprintln!(
                    "warning: rejection envelope was exceeded {} time(s); grew the envelope to {:.6e} and retrospectively thinned accepted events",
                    generation_report.envelope_updates,
                    generation_report.envelope.unwrap_or_default(),
                );
            }
            println!(
                "wrote {} events to {} ({:.3}% acceptance)",
                generation_report.produced,
                output.display(),
                100.0 * generation_report.acceptance_rate()
            );
            Ok(())
        }
    }
}

fn require_new_output(path: &std::path::Path, force: bool) -> io::Result<()> {
    if path.exists() && !force {
        return Err(io::Error::new(
            io::ErrorKind::AlreadyExists,
            format!(
                "{} already exists; pass --force to replace it",
                path.display()
            ),
        ));
    }
    Ok(())
}

fn persist_temp(
    temporary: tempfile::TempPath,
    output: &std::path::Path,
    force: bool,
) -> io::Result<()> {
    if force {
        temporary.persist(output).map_err(|error| error.error)
    } else {
        temporary
            .persist_noclobber(output)
            .map_err(|error| error.error)
    }
}

fn write_json_transactional(
    output: &std::path::Path,
    value: &impl serde::Serialize,
    force: bool,
) -> Result<(), Box<dyn std::error::Error>> {
    require_new_output(output, force)?;
    let parent = output.parent().unwrap_or_else(|| std::path::Path::new("."));
    let mut temporary = tempfile::Builder::new()
        .prefix(".gluex-gen-")
        .suffix(".json.tmp")
        .tempfile_in(parent)?;
    serde_json::to_writer_pretty(&mut temporary, value)?;
    temporary.as_file_mut().sync_all()?;
    persist_temp(temporary.into_temp_path(), output, force)?;
    Ok(())
}

/// Render command output or errors and return the process exit status.
///
/// This is the executable boundary shared by the native binary and the Python
/// console-script adapter. `clap` help and version requests are successful
/// output; command execution failures are rendered as a single diagnostic.
pub fn exit_code_with_args<I, T>(args: I) -> u8
where
    I: IntoIterator<Item = T>,
    T: Into<OsString> + Clone,
{
    match run_with_args(args) {
        Ok(()) => 0,
        Err(error) => error.downcast_ref::<clap::Error>().map_or_else(
            || {
                eprintln!("error: {error}");
                1
            },
            |clap_error| {
                let _ = clap_error.print();
                u8::try_from(clap_error.exit_code()).unwrap_or(1)
            },
        ),
    }
}

/// Execute the `gluex` command-line interface using process arguments.
#[must_use]
pub fn cli() -> ExitCode {
    ExitCode::from(exit_code_with_args(env::args_os()))
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
        let bins = self.bins;
        if bins == 0 {
            return Err(io::Error::new(
                io::ErrorKind::InvalidInput,
                "--bins must be greater than zero",
            )
            .into());
        }
        if bins > u32::MAX as usize {
            return Err(io::Error::new(
                io::ErrorKind::InvalidInput,
                "--bins must be <= 4294967295",
            )
            .into());
        }
        let min_edge = self.min;
        let max_edge = self.max;
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
        .flat_map(RunPeriod::iter_runs)
        .collect();
    let context = LuminosityContext::new(runs, run_selection)?
        .with_coherent_peak(coherent_peak)
        .with_polarized(polarized);
    let histograms = Luminosity::new(rcdb, ccdb).fetch(&edges, &context)?;

    to_writer_pretty(std::io::stdout(), &histograms)?;
    Ok(())
}
