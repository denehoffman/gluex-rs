use std::ops::{Bound, RangeBounds};

use gluex_core::{
    constants::{MAX_RUN_NUMBER, MIN_RUN_NUMBER},
    run_periods::RunPeriod,
    RunNumber,
};

use crate::conditions::{Expr, IntoExprList};

/// Describes how runs should be selected when fetching condition values.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum RunSelection {
    /// Return conditions for every run stored in RCDB.
    All,
    /// Return conditions only for the exact run numbers in the list.
    Runs(Vec<RunNumber>),
    /// Return conditions for every run within the inclusive range.
    Range {
        /// Inclusive start run number.
        start: RunNumber,
        /// Inclusive end run number.
        end: RunNumber,
    },
}

impl RunSelection {
    /// True when no runs will be returned.
    #[must_use]
    pub fn is_empty(&self) -> bool {
        matches!(self, RunSelection::Runs(r) if r.is_empty())
    }
}

/// Lightweight request context describing run selection.
#[derive(Debug, Clone)]
pub struct Context {
    selection: RunSelection,
    filters: Vec<Expr>,
}

impl Default for Context {
    fn default() -> Self {
        Self {
            selection: RunSelection::All,
            filters: Vec::new(),
        }
    }
}

impl Context {
    /// Builds a context that selects every run.
    #[must_use]
    pub fn new() -> Self {
        Self::default()
    }

    /// Restricts the context to a single run period.
    #[must_use]
    pub fn with_run_period(mut self, run_period: RunPeriod) -> Self {
        self.selection = RunSelection::Range {
            start: run_period.min_run(),
            end: run_period.max_run(),
        };
        self
    }

    /// Restricts the context to a single run number.
    #[must_use]
    pub fn with_run(mut self, run: RunNumber) -> Self {
        self.selection = RunSelection::Runs(vec![run]);
        self
    }

    /// Restricts the context to the provided run numbers.
    #[must_use]
    pub fn with_runs(mut self, runs: impl IntoIterator<Item = RunNumber>) -> Self {
        let mut run_list: Vec<RunNumber> = runs.into_iter().collect();
        run_list.sort_unstable();
        run_list.dedup();
        self.selection = RunSelection::Runs(run_list);
        self
    }

    /// Restricts the context to the inclusive range described by the [`RangeBounds`] passed as `run_range`.
    #[must_use]
    pub fn with_run_range(mut self, run_range: impl RangeBounds<RunNumber>) -> Self {
        let start = match run_range.start_bound() {
            Bound::Included(&s) => s,
            Bound::Excluded(&s) => s.saturating_add(1),
            Bound::Unbounded => MIN_RUN_NUMBER,
        };
        let end = match run_range.end_bound() {
            Bound::Included(&e) => e,
            Bound::Excluded(&e) => e.saturating_sub(1),
            Bound::Unbounded => MAX_RUN_NUMBER,
        };
        if start > end {
            self.selection = RunSelection::Runs(Vec::new());
        } else {
            self.selection = RunSelection::Range { start, end };
        }
        self
    }

    /// Adds one or more predicate expressions that must all evaluate to true.
    #[must_use]
    pub fn filter(mut self, filters: impl IntoExprList) -> Self {
        self.filters.extend(filters.into_list());
        self
    }

    /// Returns the run selection strategy for this context.
    #[must_use]
    pub fn selection(&self) -> &RunSelection {
        &self.selection
    }

    /// Returns the [`RunNumber`] values when the context is scoped to explicit runs.
    #[must_use]
    pub fn runs(&self) -> Option<&[RunNumber]> {
        if let RunSelection::Runs(runs) = &self.selection {
            Some(runs)
        } else {
            None
        }
    }

    /// Returns the current [`Expr`] filters specified by this context.
    #[must_use]
    pub fn filters(&self) -> &[Expr] {
        &self.filters
    }
}
