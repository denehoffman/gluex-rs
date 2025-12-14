use thiserror::Error;

/// Errors that can occur while parsing a timestamp string.
#[derive(Error, Debug)]
pub enum ParseTimestampError {
    /// Input contained no digits from which to form a timestamp.
    #[error("timestamp \"{0}\" has no digits")]
    NoDigits(String),
    /// Parsed timestamp was invalid according to the [`chrono`] crate.
    #[error("invalid timestamp: {0}")]
    ChronoError(String),
}
