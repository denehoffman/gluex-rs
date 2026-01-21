use std::{collections::HashMap, env, error::Error, ffi::CString, io, str::FromStr};

use ::gluex_lumi as lumi_crate;
use gluex_core::{histograms::Histogram, run_periods::RunPeriod, RestVersion, RunNumber};
use lumi_crate::{
    get_flux_histograms as compute_flux_histograms, FluxHistograms as RustFluxHistograms,
    GlueXLumiError, RestSelection,
};
use pyo3::{
    exceptions::PyRuntimeError,
    prelude::*,
    types::{PyDict, PyModule},
};
use serde_json::to_writer_pretty;

const PLOT_HELPER: &str = r#"
import matplotlib
matplotlib.use("Agg")
import matplotlib.pyplot as plt

_HIST_ORDER = [
    ("tagged_flux", "Tagged Flux"),
    ("tagm_flux", "TAGM Flux"),
    ("tagh_flux", "TAGH Flux"),
    ("tagged_luminosity", "Tagged Luminosity"),
]


def plot_histograms(data, output_path):
    fig, axes = plt.subplots(2, 2, figsize=(12, 8), sharex=False)
    axes = axes.flatten()

    for ax, (key, title) in zip(axes, _HIST_ORDER):
        hist = data[key]
        edges = [float(x) for x in hist["edges"]]
        counts = [float(x) for x in hist["counts"]]
        errors = [float(x) for x in hist["errors"]]
        centers = [0.5 * (edges[i] + edges[i + 1]) for i in range(len(edges) - 1)]
        if counts:
            step_counts = counts + [counts[-1]]
        else:
            step_counts = [0.0 for _ in edges]
        ylabel = r"Luminosity [pb$^{-1}$]" if key == "tagged_luminosity" else "Counts"
        ax.step(edges, step_counts, where="post", color="C0")
        if counts:
            ax.errorbar(centers, counts, yerr=errors, fmt="none", ecolor="black", capsize=2)
        ax.set_title(title)
        ax.set_xlabel("Energy [GeV]")
        ax.set_ylabel(ylabel)
        ax.set_ylim(bottom=0.0)
        ax.grid(True, alpha=0.3)

    fig.tight_layout()
    fig.savefig(output_path, dpi=150, bbox_inches="tight")
    plt.close(fig)
"#;

#[pyclass(module = "gluex_lumi", name = "Histogram")]
pub struct PyHistogram {
    #[pyo3(get)]
    counts: Vec<f64>,
    #[pyo3(get)]
    edges: Vec<f64>,
    #[pyo3(get)]
    errors: Vec<f64>,
}

impl PyHistogram {
    fn to_dict(&self, py: Python<'_>) -> PyResult<Py<PyDict>> {
        let dict = PyDict::new(py);
        dict.set_item("counts", self.counts.clone())?;
        dict.set_item("edges", self.edges.clone())?;
        dict.set_item("errors", self.errors.clone())?;
        Ok(dict.unbind())
    }
}

#[pymethods]
impl PyHistogram {
    #[new]
    fn new(counts: Vec<f64>, edges: Vec<f64>, errors: Vec<f64>) -> Self {
        Self {
            counts,
            edges,
            errors,
        }
    }

    pub fn as_dict(&self, py: Python<'_>) -> PyResult<Py<PyDict>> {
        self.to_dict(py)
    }
}

#[pyclass(module = "gluex_lumi", name = "FluxHistograms")]
pub struct PyFluxHistograms {
    #[pyo3(get)]
    tagged_flux: Py<PyHistogram>,
    #[pyo3(get)]
    tagm_flux: Py<PyHistogram>,
    #[pyo3(get)]
    tagh_flux: Py<PyHistogram>,
    #[pyo3(get)]
    tagged_luminosity: Py<PyHistogram>,
}

impl PyFluxHistograms {
    fn to_dict(&self, py: Python<'_>) -> PyResult<Py<PyDict>> {
        let dict = PyDict::new(py);
        let tagged_flux = self.tagged_flux.bind(py);
        let tagm_flux = self.tagm_flux.bind(py);
        let tagh_flux = self.tagh_flux.bind(py);
        let tagged_luminosity = self.tagged_luminosity.bind(py);
        dict.set_item("tagged_flux", tagged_flux.borrow().to_dict(py)?)?;
        dict.set_item("tagm_flux", tagm_flux.borrow().to_dict(py)?)?;
        dict.set_item("tagh_flux", tagh_flux.borrow().to_dict(py)?)?;
        dict.set_item("tagged_luminosity", tagged_luminosity.borrow().to_dict(py)?)?;
        Ok(dict.unbind())
    }
}

#[pymethods]
impl PyFluxHistograms {
    #[new]
    fn new(
        tagged_flux: Py<PyHistogram>,
        tagm_flux: Py<PyHistogram>,
        tagh_flux: Py<PyHistogram>,
        tagged_luminosity: Py<PyHistogram>,
    ) -> Self {
        Self {
            tagged_flux,
            tagm_flux,
            tagh_flux,
            tagged_luminosity,
        }
    }

    pub fn as_dict(&self, py: Python<'_>) -> PyResult<Py<PyDict>> {
        self.to_dict(py)
    }
}

fn py_lumi_error(err: GlueXLumiError) -> PyErr {
    PyRuntimeError::new_err(err.to_string())
}

fn parse_run_periods(obj: &Bound<'_, PyAny>) -> PyResult<HashMap<RunPeriod, RestSelection>> {
    let mapping: HashMap<String, Option<RestVersion>> = obj.extract().map_err(|_| {
        PyRuntimeError::new_err("run_periods must map run-period names to REST versions or None")
    })?;
    let mut selection = HashMap::with_capacity(mapping.len());
    for (name, rest) in mapping {
        let period =
            RunPeriod::from_str(&name).map_err(|e| PyRuntimeError::new_err(e.to_string()))?;
        let request = match rest {
            Some(value) => RestSelection::Version(value),
            None => RestSelection::Current,
        };
        selection.insert(period, request);
    }
    Ok(selection)
}

fn resolve_connection_path(value: Option<String>, env_var: &str) -> PyResult<String> {
    match value {
        Some(path) if !path.is_empty() => Ok(path),
        _ => env::var(env_var).map_err(|_| {
            PyRuntimeError::new_err(format!("{env_var} is not set and no path was provided"))
        }),
    }
}

fn histogram_to_py(py: Python<'_>, hist: &Histogram) -> PyResult<Py<PyHistogram>> {
    Py::new(
        py,
        PyHistogram {
            counts: hist.counts.clone(),
            edges: hist.edges.clone(),
            errors: hist.errors.clone(),
        },
    )
}

fn flux_histograms_to_py(
    py: Python<'_>,
    flux: &RustFluxHistograms,
) -> PyResult<Py<PyFluxHistograms>> {
    let tagged_flux = histogram_to_py(py, &flux.tagged_flux)?;
    let tagm_flux = histogram_to_py(py, &flux.tagm_flux)?;
    let tagh_flux = histogram_to_py(py, &flux.tagh_flux)?;
    let tagged_luminosity = histogram_to_py(py, &flux.tagged_luminosity)?;
    Py::new(
        py,
        PyFluxHistograms {
            tagged_flux,
            tagm_flux,
            tagh_flux,
            tagged_luminosity,
        },
    )
}

fn plot_histograms(py: Python<'_>, data: &Bound<'_, PyDict>, output_path: &str) -> PyResult<()> {
    if py.import("matplotlib").is_err() {
        return Err(PyRuntimeError::new_err(
            "matplotlib is required for --plot. Install with `pip install gluex_lumi[plot]`.",
        ));
    }
    let code = CString::new(PLOT_HELPER).expect("CString conversion");
    let filename = CString::new("_plot_helper.py").expect("CString conversion");
    let modulename = CString::new("_gluex_lumi_plot").expect("CString conversion");
    let module = PyModule::from_code(
        py,
        code.as_c_str(),
        filename.as_c_str(),
        modulename.as_c_str(),
    )?;
    let plot_fn = module.getattr("plot_histograms")?;
    plot_fn.call1((data, output_path))?;
    Ok(())
}

struct ParsedCliArgs {
    run_selection: HashMap<RunPeriod, RestSelection>,
    bins: usize,
    min_edge: f64,
    max_edge: f64,
    coherent_peak: bool,
    polarized: bool,
    rcdb: String,
    ccdb: String,
    exclude_runs: Option<Vec<RunNumber>>,
    plot_path: String,
}

fn parse_run_pair_arg(raw: &str) -> PyResult<(RunPeriod, RestSelection)> {
    let (run_str, rest) = match raw.split_once('=') {
        Some((run, rest)) => (run, Some(rest)),
        None => (raw, None),
    };
    let period =
        RunPeriod::from_str(run_str).map_err(|e| PyRuntimeError::new_err(format!("{e:?}")))?;
    let rest_version = match rest {
        Some(value) => value.parse::<usize>().map_err(|_| {
            PyRuntimeError::new_err(format!("REST must be an unsigned integer, got '{value}'"))
        })?,
        None => return Ok((period, RestSelection::Current)),
    };
    Ok((period, RestSelection::Version(rest_version)))
}

fn parse_exclude_runs_arg(raw: &str) -> PyResult<Vec<RunNumber>> {
    if raw.trim().is_empty() {
        return Err(PyRuntimeError::new_err("--exclude-runs cannot be empty"));
    }
    raw.split(',')
        .map(|entry| {
            let value = entry.trim();
            if value.is_empty() {
                return Err(PyRuntimeError::new_err(
                    "--exclude-runs cannot contain empty entries",
                ));
            }
            value
                .parse::<RunNumber>()
                .map_err(|_| PyRuntimeError::new_err(format!("invalid run number '{value}'")))
        })
        .collect()
}

fn parse_plot_cli_args(argv: &[String], plot_path: String) -> PyResult<ParsedCliArgs> {
    if argv.is_empty() {
        return Err(PyRuntimeError::new_err("argv is empty"));
    }
    let mut runs: HashMap<RunPeriod, RestSelection> = HashMap::new();
    let mut bins: Option<usize> = None;
    let mut min_edge: Option<f64> = None;
    let mut max_edge: Option<f64> = None;
    let mut rcdb_path: Option<String> = None;
    let mut ccdb_path: Option<String> = None;
    let mut exclude_runs: Option<Vec<RunNumber>> = None;
    let mut coherent_peak = false;
    let mut polarized = false;
    let mut i = 1; // skip program name
    while i < argv.len() {
        let arg = argv[i].as_str();
        let take_value = |name: &str, i: &mut usize, argv: &[String]| -> PyResult<Option<String>> {
            if arg == name {
                *i += 1;
                if *i >= argv.len() {
                    return Err(PyRuntimeError::new_err(format!(
                        "{name} requires an argument"
                    )));
                }
                Ok(Some(argv[*i].clone()))
            } else if let Some(value) = arg.strip_prefix(&format!("{name}=")) {
                Ok(Some(value.to_string()))
            } else {
                Ok(None)
            }
        };

        if let Some(value) = take_value("--run", &mut i, argv)? {
            let (period, rest) = parse_run_pair_arg(&value)?;
            runs.insert(period, rest);
        } else if let Some(value) = take_value("--bins", &mut i, argv)? {
            bins = Some(
                value
                    .parse::<usize>()
                    .map_err(|_| PyRuntimeError::new_err("--bins expects an unsigned integer"))?,
            );
        } else if let Some(value) = take_value("--min", &mut i, argv)? {
            min_edge =
                Some(value.parse::<f64>().map_err(|_| {
                    PyRuntimeError::new_err("--min expects a floating point value")
                })?);
        } else if let Some(value) = take_value("--max", &mut i, argv)? {
            max_edge =
                Some(value.parse::<f64>().map_err(|_| {
                    PyRuntimeError::new_err("--max expects a floating point value")
                })?);
        } else if let Some(value) = take_value("--rcdb", &mut i, argv)? {
            rcdb_path = Some(value);
        } else if let Some(value) = take_value("--ccdb", &mut i, argv)? {
            ccdb_path = Some(value);
        } else if let Some(value) = take_value("--exclude-runs", &mut i, argv)? {
            let parsed = parse_exclude_runs_arg(&value)?;
            if let Some(existing) = exclude_runs.as_mut() {
                existing.extend(parsed);
            } else {
                exclude_runs = Some(parsed);
            }
        } else if arg == "--coherent-peak" {
            coherent_peak = true;
        } else if arg == "--polarized" {
            polarized = true;
        } else {
            return Err(PyRuntimeError::new_err(format!(
                "unexpected argument '{arg}'"
            )));
        }
        i += 1;
    }

    if runs.is_empty() {
        return Err(PyRuntimeError::new_err(
            "at least one --run argument is required",
        ));
    }
    let bins = bins.ok_or_else(|| PyRuntimeError::new_err("--bins is required"))?;
    if bins == 0 {
        return Err(PyRuntimeError::new_err("--bins must be greater than zero"));
    }
    let min_edge = min_edge.ok_or_else(|| PyRuntimeError::new_err("--min is required"))?;
    let max_edge = max_edge.ok_or_else(|| PyRuntimeError::new_err("--max is required"))?;
    if max_edge <= min_edge {
        return Err(PyRuntimeError::new_err("--max must be greater than --min"));
    }
    let rcdb = resolve_connection_path(rcdb_path, "RCDB_CONNECTION")?;
    let ccdb = resolve_connection_path(ccdb_path, "CCDB_CONNECTION")?;

    Ok(ParsedCliArgs {
        run_selection: runs,
        bins,
        min_edge,
        max_edge,
        coherent_peak,
        polarized,
        rcdb,
        ccdb,
        exclude_runs,
        plot_path,
    })
}

fn uniform_edges(bins: usize, min_edge: f64, max_edge: f64) -> Vec<f64> {
    let width = (max_edge - min_edge) / bins as f64;
    (0..=bins).map(|i| min_edge + i as f64 * width).collect()
}

/// get_flux_histograms(run_periods, edges, *, coherent_peak=False, polarized=False, rcdb=None, ccdb=None, exclude_runs=None)
///
/// Parameters
/// ----------
/// run_periods : Mapping[str, int]
///     Mapping from run-period short names (e.g. ``"f18"``) to REST versions.
/// edges : Sequence[float]
///     Monotonically increasing photon-energy bin edges.
/// coherent_peak : bool, optional
///     If true, only retain photons in the coherent peak for each run.
/// polarized : bool, optional
///     Use the polarized flux calibration constants when true.
/// rcdb : str, optional
///     Path to the RCDB SQLite database. Defaults to the ``RCDB_CONNECTION`` env var.
/// ccdb : str, optional
///     Path to the CCDB SQLite database. Defaults to the ``CCDB_CONNECTION`` env var.
/// exclude_runs : Sequence[int], optional
///     Run numbers to skip when computing the histograms.
///
/// Returns
/// -------
/// FluxHistograms
///     Object exposing ``tagged_flux``, ``tagm_flux``, ``tagh_flux``, and
///     ``tagged_luminosity`` histograms.
#[pyfunction(name = "get_flux_histograms")]
#[pyo3(signature = (run_periods, edges, *, coherent_peak=false, polarized=false, rcdb=None, ccdb=None, exclude_runs=None))]
pub fn py_get_flux_histograms(
    py: Python<'_>,
    run_periods: Bound<'_, PyAny>,
    edges: Vec<f64>,
    coherent_peak: bool,
    polarized: bool,
    rcdb: Option<String>,
    ccdb: Option<String>,
    exclude_runs: Option<Vec<RunNumber>>,
) -> PyResult<Py<PyFluxHistograms>> {
    if edges.len() < 2 {
        return Err(PyRuntimeError::new_err(
            "edges must contain at least two values",
        ));
    }
    let run_selection = parse_run_periods(&run_periods)?;
    let rcdb_path = resolve_connection_path(rcdb, "RCDB_CONNECTION")?;
    let ccdb_path = resolve_connection_path(ccdb, "CCDB_CONNECTION")?;
    let histograms = compute_flux_histograms(
        run_selection,
        &edges,
        coherent_peak,
        polarized,
        rcdb_path,
        ccdb_path,
        exclude_runs,
    )
    .map_err(py_lumi_error)?;
    flux_histograms_to_py(py, &histograms)
}

/// cli()
///
/// Notes
/// -----
/// Mirrors the Rust ``gluex-lumi`` executable so that ``python -m pip install gluex-lumi``
/// also exposes the command-line interface. Use ``--plot=path`` to write a PNG plot
/// of the histograms using matplotlib (Python-only convenience).
#[pyfunction(name = "cli")]
pub fn py_cli(py: Python<'_>) -> PyResult<()> {
    let sys = py.import("sys")?;
    let argv: Vec<String> = sys.getattr("argv")?.extract()?;
    let mut filtered_args = Vec::with_capacity(argv.len());
    let mut plot_path: Option<String> = None;
    let mut i = 0;
    while i < argv.len() {
        let arg = argv[i].as_str();
        if arg == "--plot" {
            i += 1;
            if i >= argv.len() {
                return Err(PyRuntimeError::new_err("--plot requires a path"));
            }
            plot_path = Some(argv[i].clone());
        } else if let Some(value) = arg.strip_prefix("--plot=") {
            plot_path = Some(value.to_string());
        } else {
            filtered_args.push(argv[i].clone());
        }
        i += 1;
    }

    if let Some(plot_path) = plot_path {
        let parsed = parse_plot_cli_args(&filtered_args, plot_path)?;
        let edges = uniform_edges(parsed.bins, parsed.min_edge, parsed.max_edge);
        let hist = compute_flux_histograms(
            parsed.run_selection,
            &edges,
            parsed.coherent_peak,
            parsed.polarized,
            parsed.rcdb,
            parsed.ccdb,
            parsed.exclude_runs,
        )
        .map_err(py_lumi_error)?;
        to_writer_pretty(io::stdout(), &hist)
            .map_err(|err| PyRuntimeError::new_err(err.to_string()))?;
        println!();
        let py_flux = flux_histograms_to_py(py, &hist)?;
        let flux_bound = py_flux.bind(py);
        let dict = flux_bound.borrow().to_dict(py)?;
        let bound = dict.bind(py);
        plot_histograms(py, &bound, &parsed.plot_path)?;
        Ok(())
    } else {
        lumi_crate::cli::run_with_args(filtered_args)
            .map_err(|err: Box<dyn Error>| PyRuntimeError::new_err(err.to_string()))
    }
}

#[pymodule]
/// gluex_lumi
///
/// Python bindings for the GlueX luminosity utilities.
pub fn gluex_lumi(_py: Python<'_>, m: &Bound<'_, PyModule>) -> PyResult<()> {
    m.add_function(wrap_pyfunction!(py_get_flux_histograms, m)?)?;
    m.add_function(wrap_pyfunction!(py_cli, m)?)?;
    m.add_class::<PyHistogram>()?;
    m.add_class::<PyFluxHistograms>()?;
    let version = env!("CARGO_PKG_VERSION");
    m.add("__version__", version)?;
    Ok(())
}
