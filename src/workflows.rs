//! Canonical GlueX Workflows built on configured database sources.

use std::{
    collections::{BTreeMap, HashMap},
    time::Duration,
};

use crate::{
    CancellationToken, ExecutionOptions, GlueXError, Histogram, MissingDataPolicy,
    RESTVersionContext, RESTVersionSelection, ReconstructionSelection, RunNumber, RunPeriod,
    RunProvenance, RunSet, Sources,
    lumi::{FluxHistograms, Luminosity, LuminosityContext, LuminosityError},
};
use chrono::{DateTime, Utc};
use thiserror::Error;

/// Stable identifier for the implemented luminosity procedure.
pub const LUMINOSITY_PROCEDURE_VERSION: &str = "gluex-luminosity-v1";

/// Failure to construct or evaluate a `GlueX` Workflow.
#[derive(Debug, Error)]
pub enum WorkflowError {
    /// A required configured source is unavailable.
    #[error(transparent)]
    Session(#[from] GlueXError),
    /// Invalid run-period or reconstruction reference information.
    #[error(transparent)]
    Core(#[from] crate::GlueXCoreError),
    /// The requested reconstruction mapping omitted a represented run period.
    #[error("reconstruction selection is missing {0:?}")]
    MissingReconstruction(RunPeriod),
    /// Luminosity input retrieval or calculation failed.
    #[error(transparent)]
    Luminosity(#[from] LuminosityError),
    /// Evaluation was cancelled or exceeded its deadline.
    #[error("workflow execution interrupted by cancellation or timeout")]
    Interrupted,
}

/// Workflows bound to the source generation captured by a [`crate::GlueX`] session.
#[derive(Debug, Clone)]
pub struct Workflows {
    sources: Sources,
}

impl Workflows {
    pub(crate) const fn new(sources: Sources) -> Self {
        Self { sources }
    }

    /// Build a lazy canonical luminosity request from an already resolved Run Set.
    #[must_use]
    pub fn luminosity(
        &self,
        runs: &RunSet,
        reconstruction: ReconstructionSelection,
        edges: impl IntoIterator<Item = f64>,
    ) -> LuminosityQuery {
        LuminosityQuery {
            sources: self.sources.clone(),
            runs: runs.clone(),
            reconstruction,
            edges: edges.into_iter().collect(),
            coherent_peak: false,
            polarized: false,
            policy: MissingDataPolicy::Strict,
            fallback_run: None,
            execution: ExecutionOptions::default(),
        }
    }
}

/// Lazy canonical luminosity request. The default missing-data policy is strict.
#[derive(Debug, Clone)]
pub struct LuminosityQuery {
    sources: Sources,
    runs: RunSet,
    reconstruction: ReconstructionSelection,
    edges: Vec<f64>,
    coherent_peak: bool,
    polarized: bool,
    policy: MissingDataPolicy,
    fallback_run: Option<RunNumber>,
    execution: ExecutionOptions,
}

impl LuminosityQuery {
    /// Restrict detector contributions to each run's coherent peak.
    #[must_use]
    pub fn with_coherent_peak(&self, enabled: bool) -> Self {
        let mut query = self.clone();
        query.coherent_peak = enabled;
        query
    }

    /// Require coherent-beam inputs for the selected runs.
    #[must_use]
    pub fn with_polarized(&self, enabled: bool) -> Self {
        let mut query = self.clone();
        query.polarized = enabled;
        query
    }

    /// Report and exclude runs with genuinely absent required scientific inputs.
    #[must_use]
    pub fn report_missing(&self) -> Self {
        let mut query = self.clone();
        query.policy = MissingDataPolicy::Report;
        query.fallback_run = None;
        query
    }

    /// Use the complete luminosity inputs for `run` when a selected run has missing inputs.
    ///
    /// The substitution is explicit and recorded in the result report. The fallback run must
    /// have a reconstruction selection and a complete set of valid workflow inputs.
    #[must_use]
    pub fn fallback_to(&self, run: RunNumber) -> Self {
        let mut query = self.clone();
        query.policy = MissingDataPolicy::Fallback;
        query.fallback_run = Some(run);
        query
    }

    /// Stop evaluation after `duration`.
    #[must_use]
    pub fn with_timeout(&self, duration: Duration) -> Self {
        let mut query = self.clone();
        query.execution = query.execution.with_timeout(duration);
        query
    }

    /// Stop evaluation when `token` is cancelled.
    #[must_use]
    pub fn with_cancellation(&self, token: CancellationToken) -> Self {
        let mut query = self.clone();
        query.execution = query.execution.with_cancellation(token);
        query
    }

    #[cfg(feature = "python")]
    pub(crate) fn with_interrupt_check(
        &self,
        check: impl Fn() -> bool + Send + Sync + 'static,
    ) -> Self {
        let mut query = self.clone();
        query.execution = query.execution.with_interrupt_check(check);
        query
    }

    /// Evaluate the request, returning histograms, a run report and reproducibility provenance.
    ///
    /// # Errors
    /// Fails for missing capabilities, incomplete reconstruction mappings, cancellation,
    /// malformed inputs, or missing scientific inputs under the default strict policy.
    pub fn collect(&self) -> Result<LuminosityResult, WorkflowError> {
        if self.execution.interrupted() {
            return Err(WorkflowError::Interrupted);
        }
        let rcdb = self.sources.rcdb()?.clone();
        let ccdb = self.sources.ccdb()?.clone();
        let rcdb_source = rcdb.connection_path().to_owned();
        let ccdb_source = ccdb.connection_path().to_owned();
        let source_opened_at = ccdb.opened_at();
        let calculator = Luminosity::from_readers(rcdb, ccdb);
        let mut resolution_runs = self.runs.numbers().to_vec();
        if let Some(run) = self.fallback_run {
            resolution_runs.push(run);
        }
        let resolved =
            resolve_reconstruction(&resolution_runs, &self.reconstruction, source_opened_at)?;
        let contexts: HashMap<RunPeriod, RESTVersionContext> =
            resolved.clone().into_iter().collect();
        let mut histograms = empty_histograms(&self.edges)?;
        let mut used_runs = Vec::new();
        let mut excluded_runs = Vec::new();
        let mut substitutions = Vec::new();
        let context = LuminosityContext::new(resolution_runs, contexts)?
            .with_coherent_peak(self.coherent_peak)
            .with_polarized(self.polarized);
        let batch = calculator.fetch_each(&self.edges, &context, &self.execution)?;
        for &run in self.runs.numbers() {
            if self.execution.interrupted() {
                return Err(WorkflowError::Interrupted);
            }
            if let Some(run_histograms) = batch.histograms.get(&run) {
                add_histograms(&mut histograms, run_histograms)?;
                used_runs.push(run);
                continue;
            }
            let input = batch
                .missing
                .get(&run)
                .copied()
                .unwrap_or("luminosity inputs after run constraints");
            if self.policy == MissingDataPolicy::Report {
                excluded_runs.push((
                    run,
                    LuminosityError::MissingRunInput { run, input }.to_string(),
                ));
                continue;
            }
            if self.policy == MissingDataPolicy::Fallback {
                let fallback_run = self.fallback_run.ok_or(LuminosityError::MissingRunInput {
                    run,
                    input: "explicit fallback run",
                })?;
                let fallback_histograms = batch.histograms.get(&fallback_run).ok_or_else(|| {
                    LuminosityError::MissingRunInput {
                        run: fallback_run,
                        input: batch
                            .missing
                            .get(&fallback_run)
                            .copied()
                            .unwrap_or("fallback luminosity inputs"),
                    }
                })?;
                add_histograms(&mut histograms, fallback_histograms)?;
                used_runs.push(run);
                substitutions.push((run, fallback_run));
                continue;
            }
            return Err(LuminosityError::MissingRunInput { run, input }.into());
        }
        let report = LuminosityReport {
            selected_runs: self.runs.numbers().to_vec(),
            used_runs,
            excluded_runs,
            substitutions,
            complete: true,
        };
        let provenance = LuminosityProvenance {
            runs: self.runs.provenance().clone(),
            rcdb_source,
            ccdb_source,
            requested_reconstruction: self.reconstruction.clone(),
            resolved_reconstruction: resolved,
            calibration_default_as_of: source_opened_at,
            procedure_version: LUMINOSITY_PROCEDURE_VERSION,
            procedure_status: "provisional",
            references: vec!["https://doi.org/10.1103/RevModPhys.46.815"],
            exceptions: vec![
                "RP2019_11 endpoint constants after run 72435 use the documented 2021-04-23 override",
            ],
            missing_policy: self.policy,
            coherent_peak: self.coherent_peak,
            polarized: self.polarized,
        };
        Ok(LuminosityResult {
            histograms,
            provenance,
            report,
        })
    }
}

fn resolve_reconstruction(
    runs: &[RunNumber],
    selection: &ReconstructionSelection,
    source_opened_at: DateTime<Utc>,
) -> Result<BTreeMap<RunPeriod, RESTVersionContext>, WorkflowError> {
    let mut periods = runs
        .iter()
        .copied()
        .map(RunPeriod::try_from)
        .collect::<Result<Vec<_>, _>>()?;
    periods.sort_unstable();
    periods.dedup();
    periods
        .into_iter()
        .map(|period| {
            let requested = match selection {
                ReconstructionSelection::Latest => {
                    RESTVersionSelection::from_timestamp(source_opened_at)
                }
                ReconstructionSelection::Periods(selections) => selections
                    .get(&period)
                    .copied()
                    .ok_or(WorkflowError::MissingReconstruction(period))?,
            };
            Ok((period, requested.resolve_context(period)?))
        })
        .collect()
}

fn empty_histograms(edges: &[f64]) -> Result<FluxHistograms, LuminosityError> {
    Ok(FluxHistograms {
        tagged_flux: Histogram::empty_with_edges(edges.to_vec())?,
        tagm_flux: Histogram::empty_with_edges(edges.to_vec())?,
        tagh_flux: Histogram::empty_with_edges(edges.to_vec())?,
        tagged_luminosity: Histogram::empty_with_edges(edges.to_vec())?,
    })
}

fn add_histogram(target: &mut Histogram, value: &Histogram) -> Result<(), LuminosityError> {
    for index in 0..target.bins() {
        target.set_count(index, target.counts()[index] + value.counts()[index])?;
        target.set_error(index, target.errors()[index].hypot(value.errors()[index]))?;
    }
    Ok(())
}

pub(crate) fn add_histograms(
    target: &mut FluxHistograms,
    value: &FluxHistograms,
) -> Result<(), LuminosityError> {
    add_histogram(&mut target.tagged_flux, &value.tagged_flux)?;
    add_histogram(&mut target.tagm_flux, &value.tagm_flux)?;
    add_histogram(&mut target.tagh_flux, &value.tagh_flux)?;
    add_histogram(&mut target.tagged_luminosity, &value.tagged_luminosity)
}

/// Reproducibility information retained by a luminosity result.
#[derive(Debug, Clone)]
pub struct LuminosityProvenance {
    runs: RunProvenance,
    rcdb_source: String,
    ccdb_source: String,
    requested_reconstruction: ReconstructionSelection,
    resolved_reconstruction: BTreeMap<RunPeriod, RESTVersionContext>,
    calibration_default_as_of: DateTime<Utc>,
    procedure_version: &'static str,
    procedure_status: &'static str,
    references: Vec<&'static str>,
    exceptions: Vec<&'static str>,
    missing_policy: MissingDataPolicy,
    coherent_peak: bool,
    polarized: bool,
}

impl LuminosityProvenance {
    /// Resolved run membership and predicates supplied by the caller.
    #[must_use]
    pub const fn runs(&self) -> &RunProvenance {
        &self.runs
    }
    /// RCDB source identity captured by the workflow.
    #[must_use]
    pub fn rcdb_source(&self) -> &str {
        &self.rcdb_source
    }
    /// CCDB source identity captured by the workflow.
    #[must_use]
    pub fn ccdb_source(&self) -> &str {
        &self.ccdb_source
    }
    /// Reconstruction selector requested by the caller before resolution.
    #[must_use]
    pub const fn requested_reconstruction(&self) -> &ReconstructionSelection {
        &self.requested_reconstruction
    }
    /// Effective reconstruction selection for each represented period.
    #[must_use]
    pub const fn resolved_reconstruction(&self) -> &BTreeMap<RunPeriod, RESTVersionContext> {
        &self.resolved_reconstruction
    }
    /// Default calibration cutoff captured when the bound CCDB source opened.
    #[must_use]
    pub const fn calibration_default_as_of(&self) -> DateTime<Utc> {
        self.calibration_default_as_of
    }
    /// Stable implementation identifier.
    #[must_use]
    pub const fn procedure_version(&self) -> &'static str {
        self.procedure_version
    }
    /// Scientific-review status; this implementation is intentionally not claimed canonical yet.
    #[must_use]
    pub const fn procedure_status(&self) -> &'static str {
        self.procedure_status
    }
    /// Scientific references used by the procedure.
    #[must_use]
    pub fn references(&self) -> &[&'static str] {
        &self.references
    }
    /// Explicit scientific exceptions applied by the implementation.
    #[must_use]
    pub fn exceptions(&self) -> &[&'static str] {
        &self.exceptions
    }
    /// Missing-input policy used by this result.
    #[must_use]
    pub const fn missing_policy(&self) -> MissingDataPolicy {
        self.missing_policy
    }
    /// Whether coherent-peak filtering affected this result.
    #[must_use]
    pub const fn coherent_peak(&self) -> bool {
        self.coherent_peak
    }
    /// Whether coherent-beam inputs were required for this result.
    #[must_use]
    pub const fn polarized(&self) -> bool {
        self.polarized
    }
}

/// Completed run accounting for luminosity evaluation.
#[derive(Debug, Clone)]
pub struct LuminosityReport {
    selected_runs: Vec<RunNumber>,
    used_runs: Vec<RunNumber>,
    excluded_runs: Vec<(RunNumber, String)>,
    substitutions: Vec<(RunNumber, RunNumber)>,
    complete: bool,
}

impl LuminosityReport {
    /// Runs supplied by the caller.
    #[must_use]
    pub fn selected_runs(&self) -> &[RunNumber] {
        &self.selected_runs
    }
    /// Runs whose inputs contributed to the result.
    #[must_use]
    pub fn used_runs(&self) -> &[RunNumber] {
        &self.used_runs
    }
    /// Runs omitted in report mode with the missing-input reason.
    #[must_use]
    pub fn excluded_runs(&self) -> &[(RunNumber, String)] {
        &self.excluded_runs
    }
    /// Explicit selected-run to fallback-run substitutions.
    #[must_use]
    pub fn substitutions(&self) -> &[(RunNumber, RunNumber)] {
        &self.substitutions
    }
    /// Whether evaluation finished normally.
    #[must_use]
    pub const fn complete(&self) -> bool {
        self.complete
    }
}

/// Histograms and retained evidence from a completed luminosity workflow.
#[derive(Debug, Clone)]
pub struct LuminosityResult {
    histograms: FluxHistograms,
    provenance: LuminosityProvenance,
    report: LuminosityReport,
}

impl LuminosityResult {
    /// Flux and luminosity histograms. Energies are `GeV` and luminosity is inverse picobarns.
    #[must_use]
    pub const fn histograms(&self) -> &FluxHistograms {
        &self.histograms
    }
    /// Inputs, reconstruction selections, procedure identity, references and exceptions.
    #[must_use]
    pub const fn provenance(&self) -> &LuminosityProvenance {
        &self.provenance
    }
    /// Selected, used, excluded and substituted runs.
    #[must_use]
    pub const fn report(&self) -> &LuminosityReport {
        &self.report
    }
}
