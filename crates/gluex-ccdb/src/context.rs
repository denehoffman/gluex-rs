use chrono::{DateTime, Utc};
use gluex_core::{
    constants::{MAX_RUN_NUMBER, MIN_RUN_NUMBER},
    errors::ParseTimestampError,
    parsers::parse_timestamp,
    run_periods::{resolve_rest_version, RunPeriod},
    RunNumber,
};
use std::{ops::Bound, str::FromStr};
use thiserror::Error;

use crate::CCDBResult;

/// Absolute CCDB path wrapper that enforces formatting rules.
#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub struct NamePath(pub String);
impl FromStr for NamePath {
    type Err = NamePathError;

    fn from_str(s: &str) -> Result<Self, Self::Err> {
        if !s.starts_with('/') {
            return Err(NamePathError::NotAbsolutePath(s.to_string()));
        }
        if !s
            .chars()
            .all(|c| c.is_ascii_alphanumeric() || c == '/' || c == '_' || c == '-')
        {
            return Err(NamePathError::IllegalCharacter(s.to_string()));
        }
        Ok(Self(s.to_string()))
    }
}
impl NamePath {
    /// Returns the absolute path string (always begins with `/`).
    #[must_use]
    pub fn full_path(&self) -> &str {
        &self.0
    }
    /// Returns the final component of the path (table or directory name).
    #[must_use]
    pub fn name(&self) -> &str {
        self.0.rsplit('/').next().unwrap_or("")
    }
    /// Returns the parent path, or [`None`] when this path is root.
    #[must_use]
    pub fn parent(&self) -> Option<NamePath> {
        if self.is_root() {
            return None;
        }
        let mut parts: Vec<&str> = self.0.split('/').collect();
        parts.pop();
        Some(NamePath(format!("/{}", parts.join("/"))))
    }
    /// True when the path corresponds to the root directory.
    #[must_use]
    pub fn is_root(&self) -> bool {
        self.0 == "/"
    }
}
/// Errors that can occur while parsing or validating [`NamePath`] values.
#[derive(Error, Debug)]
pub enum NamePathError {
    /// Path did not begin with a forward slash.
    #[error("path \"{0}\" is not absolute (must start with '/')")]
    NotAbsolutePath(String),
    /// Path contained a character outside the allowed set.
    #[error("illegal character encountered in path \"{0}\"")]
    IllegalCharacter(String),
}

const DEFAULT_VARIATION: &str = "default";
const DEFAULT_RUN_NUMBER: RunNumber = 0;

/// Query context describing run selection, variation, and timestamp.
#[derive(Debug, Clone)]
pub struct Context {
    /// [`RunNumber`] values to consider when resolving assignments.
    pub runs: Vec<RunNumber>,
    /// Variation (branch) to resolve within CCDB.
    pub variation: String,
    /// [`DateTime`] in the [`Utc`] timezone used to select the newest constants not newer than this time.
    pub timestamp: DateTime<Utc>,
}
impl Default for Context {
    fn default() -> Self {
        Self {
            runs: vec![DEFAULT_RUN_NUMBER],
            variation: DEFAULT_VARIATION.to_string(),
            timestamp: Utc::now(),
        }
    }
}
impl Context {
    /// Builds a new context with optional run, variation, and timestamp overrides.
    #[must_use]
    pub fn new(
        runs: Option<Vec<RunNumber>>,
        variation: Option<String>,
        timestamp: Option<DateTime<Utc>>,
    ) -> Self {
        let mut context = Self::default();
        if let Some(runs) = runs {
            context.runs = runs;
        }
        if let Some(variation) = variation {
            context.variation = variation;
        }
        if let Some(timestamp) = timestamp {
            context.timestamp = timestamp;
        }
        context
    }
    /// Returns a context scoped to all runs associated with the given [`RunPeriod`]. Additionally,
    /// if a REST version is provided, the timestamp will be resolved for that version. If the
    /// given [`RunPeriod`] does not have the requested REST version, the closest REST version less
    /// than the requested one will be used.
    ///
    /// # Errors
    ///
    /// This method will return an error if the run period is not found in the [`REST_VERSION_TIMESTAMPS`] map or if no lower REST version exists when the requested one is not found.
    pub fn with_run_period(
        mut self,
        run_period: RunPeriod,
        rest_version: Option<usize>,
    ) -> CCDBResult<Self> {
        self.runs = run_period.run_range().collect();
        if let Some(rest_version) = rest_version {
            let version = resolve_rest_version(run_period, rest_version)?;
            self.timestamp = version.timestamp;
        }
        Ok(self)
    }
    /// Returns a context scoped to a single run number.
    #[must_use]
    pub fn with_run(mut self, run: RunNumber) -> Self {
        self.runs = vec![run.clamp(MIN_RUN_NUMBER, MAX_RUN_NUMBER)];
        self
    }
    /// Replaces the run list with the provided runs.
    #[must_use]
    pub fn with_runs(mut self, iter: impl IntoIterator<Item = RunNumber>) -> Self {
        self.runs = iter
            .into_iter()
            .map(|r| r.clamp(MIN_RUN_NUMBER, MAX_RUN_NUMBER))
            .collect();
        self
    }
    /// Replaces the run list with all runs inside the supplied range.
    #[must_use]
    pub fn with_run_range(mut self, run_range: impl std::ops::RangeBounds<RunNumber>) -> Self {
        let start = match run_range.start_bound() {
            Bound::Included(&s) => s,
            Bound::Excluded(&s) => s.saturating_add(1),
            Bound::Unbounded => MIN_RUN_NUMBER,
        }
        .max(MIN_RUN_NUMBER);
        let end = match run_range.end_bound() {
            Bound::Included(&e) => e,
            Bound::Excluded(&e) => e.saturating_sub(1),
            Bound::Unbounded => MAX_RUN_NUMBER,
        }
        .min(MAX_RUN_NUMBER);
        self.runs = if start > end {
            Vec::new()
        } else {
            (start..=end).collect()
        };
        self
    }
    /// Sets the variation branch for subsequent queries.
    #[must_use]
    pub fn with_variation(mut self, variation: &str) -> Self {
        self.variation = variation.to_string();
        self
    }
    /// Sets the timestamp for selecting assignments (query will give the most recent assignment not newer than this).
    #[must_use]
    pub fn with_timestamp(mut self, timestamp: DateTime<Utc>) -> Self {
        self.timestamp = timestamp;
        self
    }
    /// Sets the timestamp for selecting assignments from a formatted timestamp string (query will give the most recent assignment not newer than this).
    ///
    /// # Errors
    ///
    /// This method returns a [`ParseTimestampError`] if the timestamp is not in the format allowed by CCDB.
    pub fn with_timestamp_string(mut self, timestamp: &str) -> Result<Self, ParseTimestampError> {
        self.timestamp = parse_timestamp(timestamp)?;
        Ok(self)
    }
}

/// Errors that can occur when parsing a [`Request`] string.
#[derive(Error, Debug)]
pub enum ParseRequestError {
    /// Failed to parse the path portion of the request.
    #[error("{0}")]
    NamePathError(#[from] NamePathError),
    /// Failed to parse the timestamp portion of the request.
    #[error("{0}")]
    TimestampParseError(#[from] ParseTimestampError),
    /// Run number was not a valid integer.
    #[error("invalid run number: {0}")]
    InvalidRunNumberError(String),
}

/// Parsed representation of a CCDB request string, containing both the [`NamePath`] and [`Context`].
#[derive(Debug, Clone)]
pub struct Request {
    /// Absolute path to the requested table.
    pub path: NamePath,
    /// Context describing run/variation/timestamp selection.
    pub context: Context,
}
impl FromStr for Request {
    type Err = ParseRequestError;

    fn from_str(s: &str) -> Result<Self, Self::Err> {
        let (path_str, rest) = s.split_once(':').map_or((s, None), |(p, r)| (p, Some(r)));
        let path = NamePath::from_str(path_str)?;
        let mut run: Option<RunNumber> = None;
        let mut variation: Option<String> = None;
        let mut timestamp: Option<DateTime<Utc>> = None;
        if let Some(rest) = rest {
            let mut parts: Vec<&str> = rest.splitn(3, ':').collect();
            while parts.len() < 3 {
                parts.push("");
            }
            let (run_s, var_s, time_s) = (parts[0], parts[1], parts[2]);
            if !run_s.is_empty() {
                run =
                    Some(run_s.parse::<RunNumber>().map_err(|_| {
                        ParseRequestError::InvalidRunNumberError(run_s.to_string())
                    })?);
            }
            if !var_s.is_empty() {
                variation = Some(var_s.to_string());
            }
            if !time_s.is_empty() {
                timestamp = Some(parse_timestamp(time_s)?);
            }
        }
        Ok(Request {
            path,
            context: Context::new(run.map(|r| vec![r]), variation, timestamp),
        })
    }
}
