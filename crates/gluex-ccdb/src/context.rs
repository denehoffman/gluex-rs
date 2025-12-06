use std::{ops::Bound, str::FromStr};

use chrono::{DateTime, Datelike, NaiveDate, NaiveDateTime, NaiveTime, Utc};
use thiserror::Error;

use crate::RunNumber;

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
    pub fn full_path(&self) -> &str {
        &self.0
    }
    pub fn name(&self) -> &str {
        self.0.rsplit('/').next().unwrap_or("")
    }
    pub fn parent(&self) -> Option<NamePath> {
        if self.is_root() {
            return None;
        }
        let mut parts: Vec<&str> = self.0.split('/').collect();
        parts.pop();
        Some(NamePath(format!("/{}", parts.join("/"))))
    }
    pub fn is_root(&self) -> bool {
        self.0 == "/"
    }
}

#[derive(Error, Debug)]
pub enum NamePathError {
    #[error("path \"{0}\" is not absolute (must start with '/')")]
    NotAbsolutePath(String),
    #[error("illegal character encountered in path \"{0}\"")]
    IllegalCharacter(String),
}

const DEFAULT_VARIATION: &str = "default";
const DEFAULT_RUN_NUMBER: RunNumber = 0;
const MAX_RUN_NUMBER: RunNumber = 2_147_483_647;

#[derive(Debug, Clone)]
pub struct Context {
    pub runs: Vec<RunNumber>,
    pub variation: String,
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
            context.timestamp = timestamp
        }
        context
    }
    pub fn with_run(mut self, run: RunNumber) -> Self {
        self.runs = vec![run.clamp(0, MAX_RUN_NUMBER)];
        self
    }
    pub fn with_runs(mut self, iter: impl IntoIterator<Item = RunNumber>) -> Self {
        self.runs = iter
            .into_iter()
            .map(|r| r.clamp(0, MAX_RUN_NUMBER))
            .collect();
        self
    }
    pub fn with_run_range(mut self, run_range: impl std::ops::RangeBounds<RunNumber>) -> Self {
        let start = match run_range.start_bound() {
            Bound::Included(&s) => s,
            Bound::Excluded(&s) => s + 1,
            Bound::Unbounded => 0,
        }
        .max(0);
        let end = match run_range.end_bound() {
            Bound::Included(&e) => e,
            Bound::Excluded(&e) => e - 1,
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
    pub fn with_variation(mut self, variation: &str) -> Self {
        self.variation = variation.to_string();
        self
    }
    pub fn with_timestamp(mut self, timestamp: DateTime<Utc>) -> Self {
        self.timestamp = timestamp;
        self
    }
}

#[derive(Error, Debug)]
pub enum ParseTimestampError {
    #[error("timestamp \"{0}\" has no digits")]
    NoDigits(String),
    #[error("invalid timestamp: {0}")]
    ChronoError(String),
}

pub fn parse_timestamp(input: &str) -> Result<DateTime<Utc>, ParseTimestampError> {
    let digits: Vec<i32> = input
        .split(|c: char| !c.is_ascii_digit())
        .filter(|s| !s.is_empty())
        .filter_map(|s| s.parse::<i32>().ok())
        .collect();
    if digits.is_empty() {
        return Err(ParseTimestampError::NoDigits(input.to_string()));
    }
    let year = digits[0];
    let month = digits.get(1).copied().unwrap_or(12) as u32;
    let day = digits.get(2).copied().unwrap_or_else(|| {
        let start = NaiveDate::from_ymd_opt(year, month, 1)
            .unwrap_or_else(|| NaiveDate::from_ymd_opt(1970, 1, 1).unwrap());
        let next_month = if month == 12 {
            NaiveDate::from_ymd_opt(year + 1, 1, 1)
        } else {
            NaiveDate::from_ymd_opt(year, month + 1, 1)
        }
        .unwrap_or(start);
        next_month.pred_opt().unwrap_or(start).day() as i32
    }) as u32;
    let hour = digits.get(3).copied().unwrap_or(23) as u32;
    let minute = digits.get(4).copied().unwrap_or(59) as u32;
    let second = digits.get(5).copied().unwrap_or(59) as u32;

    let date = NaiveDate::from_ymd_opt(year, month, day).ok_or_else(|| {
        ParseTimestampError::ChronoError(format!("invalid date: {year}-{month}-{day}"))
    })?;
    let time = NaiveTime::from_hms_opt(hour, minute, second).ok_or_else(|| {
        ParseTimestampError::ChronoError(format!("invalid time: {hour}:{minute}:{second}"))
    })?;
    let naive = NaiveDateTime::new(date, time);
    Ok(DateTime::<Utc>::from_naive_utc_and_offset(naive, Utc))
}

#[derive(Error, Debug)]
pub enum ParseRequestError {
    #[error("{0}")]
    NamePathError(#[from] NamePathError),
    #[error("{0}")]
    TimestampParseError(#[from] ParseTimestampError),
    #[error("invalid run number: {0}")]
    InvalidRunNumberError(String),
}

#[derive(Debug, Clone)]
pub struct Request {
    pub path: NamePath,
    pub context: Context,
}
impl FromStr for Request {
    type Err = ParseRequestError;

    fn from_str(s: &str) -> Result<Self, Self::Err> {
        let (path_str, rest) = s
            .split_once(':')
            .map(|(p, r)| (p, Some(r)))
            .unwrap_or((s, None));
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
