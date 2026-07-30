use crate::core::{
    RunNumber,
    constants::{MAX_RUN_NUMBER, MIN_RUN_NUMBER},
    parsers::parse_timestamp,
    run_periods::{RESTVersionSelection, RunPeriod},
};
use chrono::{DateTime, Utc};
use std::{ops::Bound, str::FromStr};

use crate::ccdb::{CCDBError, CCDBResult};

/// Absolute CCDB path wrapper that enforces formatting rules.
#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub struct NamePath(pub String);
impl FromStr for NamePath {
    type Err = CCDBError;

    fn from_str(s: &str) -> Result<Self, Self::Err> {
        if !s.starts_with('/') {
            return Err(CCDBError::NotAbsolutePath(s.to_string()));
        }
        if !s
            .chars()
            .all(|c| c.is_ascii_alphanumeric() || c == '/' || c == '_' || c == '-')
        {
            return Err(CCDBError::IllegalCharacter(s.to_string()));
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
    pub fn parent(&self) -> Option<Self> {
        if self.is_root() {
            return None;
        }
        let mut parts: Vec<&str> = self.0.split('/').collect();
        parts.pop();
        Some(Self(format!("/{}", parts.join("/"))))
    }
    /// True when the path corresponds to the root directory.
    #[must_use]
    pub fn is_root(&self) -> bool {
        self.0 == "/"
    }
}
const DEFAULT_VARIATION: &str = "default";
const DEFAULT_RUN_NUMBER: RunNumber = 0;

/// Query context describing run selection, variation, and timestamp.
#[derive(Debug, Clone)]
pub struct CCDBContext {
    /// [`RunNumber`] values to consider when resolving assignments.
    pub runs: Vec<RunNumber>,
    /// Variation (branch) to resolve within CCDB.
    pub variation: String,
    /// [`DateTime`] in the [`Utc`] timezone used to select the newest constants not newer than this time.
    pub timestamp: DateTime<Utc>,
}
impl Default for CCDBContext {
    fn default() -> Self {
        Self {
            runs: vec![DEFAULT_RUN_NUMBER],
            variation: DEFAULT_VARIATION.to_string(),
            timestamp: Utc::now(),
        }
    }
}
impl CCDBContext {
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
    /// if a REST version is provided, the timestamp will be resolved for that version.
    ///
    /// # Errors
    ///
    /// This method will return an error if the run period is not found in the [`REST_VERSION_TIMESTAMPS`] map
    /// or if the requested REST version is not defined for the run period.
    pub fn with_run_period(
        mut self,
        run_period: RunPeriod,
        rest_version: RESTVersionSelection,
    ) -> CCDBResult<Self> {
        self.runs = run_period.run_range().collect();
        self.timestamp = rest_version.resolve_timestamp(run_period)?;
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
    pub const fn with_timestamp(mut self, timestamp: DateTime<Utc>) -> Self {
        self.timestamp = timestamp;
        self
    }
    /// Sets the timestamp for selecting assignments from a formatted timestamp string (query will give the most recent assignment not newer than this).
    ///
    /// # Errors
    ///
    /// This method returns a [`CCDBError`] if the timestamp is not in the format allowed by CCDB.
    pub fn with_timestamp_string(mut self, timestamp: &str) -> CCDBResult<Self> {
        self.timestamp = parse_timestamp(timestamp)?;
        Ok(self)
    }
}

/// Parsed representation of a CCDB request string, containing both the [`NamePath`] and [`CCDBContext`].
#[derive(Debug, Clone)]
pub struct Request {
    /// Absolute path to the requested table.
    pub path: NamePath,
    /// Context describing run/variation/timestamp selection.
    pub context: CCDBContext,
}
impl FromStr for Request {
    type Err = CCDBError;

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
                run = Some(
                    run_s
                        .parse::<RunNumber>()
                        .map_err(|_| CCDBError::InvalidRunNumberError(run_s.to_string()))?,
                );
            }
            if !var_s.is_empty() {
                variation = Some(var_s.to_string());
            }
            if !time_s.is_empty() {
                timestamp = Some(parse_timestamp(time_s)?);
            }
        }
        Ok(Self {
            path,
            context: CCDBContext::new(run.map(|r| vec![r]), variation, timestamp),
        })
    }
}
