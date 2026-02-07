use thiserror::Error;

/// Errors that can occur while parsing a timestamp string.
#[derive(Error, Debug, Clone)]
pub enum ParseTimestampError {
    /// Input contained no digits from which to form a timestamp.
    #[error("timestamp \"{0}\" has no digits")]
    NoDigits(String),
    /// Parsed timestamp was invalid according to the [`chrono`] crate.
    #[error("invalid timestamp: {0}")]
    ChronoError(String),
}

/// Errors that can occur while constructing or mutating histograms.
#[derive(Error, Debug, Clone, PartialEq)]
pub enum HistogramError {
    /// Histogram requires at least two edge values.
    #[error("histogram edges must contain at least two values (found {len})")]
    TooFewEdges {
        /// Number of edges provided.
        len: usize,
    },
    /// Edge value was NaN or infinite.
    #[error("histogram edge at index {index} is not finite: {value}")]
    NonFiniteEdge {
        /// Index of the edge value.
        index: usize,
        /// Edge value that failed validation.
        value: f64,
    },
    /// Consecutive edge values were not strictly increasing.
    #[error(
        "histogram edges must be strictly increasing (edges[{index}]={left}, edges[{next_index}]={right})"
    )]
    NotStrictlyIncreasing {
        /// Left edge index in the non-increasing pair.
        index: usize,
        /// Right edge index in the non-increasing pair.
        next_index: usize,
        /// Left edge value.
        left: f64,
        /// Right edge value.
        right: f64,
    },
    /// Number of counts does not match number of bins.
    #[error("counts length mismatch: expected {expected}, found {found}")]
    CountLengthMismatch {
        /// Expected number of counts.
        expected: usize,
        /// Actual number of counts.
        found: usize,
    },
    /// Number of errors does not match number of bins.
    #[error("errors length mismatch: expected {expected}, found {found}")]
    ErrorLengthMismatch {
        /// Expected number of errors.
        expected: usize,
        /// Actual number of errors.
        found: usize,
    },
    /// Number of weights does not match number of values.
    #[error("weights length mismatch: expected {expected}, found {found}")]
    WeightLengthMismatch {
        /// Expected number of weights.
        expected: usize,
        /// Actual number of weights.
        found: usize,
    },
    /// Uniform histogram requested with zero bins.
    #[error("uniform histogram requires at least one bin")]
    EmptyBinCount,
    /// Uniform histogram limits were not finite and strictly increasing.
    #[error(
        "uniform histogram limits must be finite and strictly increasing (min={min}, max={max})"
    )]
    InvalidUniformLimits {
        /// Lower edge of the histogram range.
        min: f64,
        /// Upper edge of the histogram range.
        max: f64,
    },
}
