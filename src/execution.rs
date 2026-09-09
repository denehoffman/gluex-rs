//! Cooperative cancellation and deadlines for synchronous database work.

use std::{
    fmt,
    sync::{
        Arc,
        atomic::{AtomicBool, Ordering},
    },
    time::{Duration, Instant},
};

/// Shareable cancellation handle for an in-flight synchronous request.
#[derive(Debug, Clone, Default)]
pub struct CancellationToken(Arc<AtomicBool>);

impl CancellationToken {
    /// Create a token in the active state.
    #[must_use]
    pub fn new() -> Self {
        Self::default()
    }

    /// Request cancellation. Calling this more than once is harmless.
    pub fn cancel(&self) {
        self.0.store(true, Ordering::Release);
    }

    /// Whether cancellation has been requested.
    #[must_use]
    pub fn is_cancelled(&self) -> bool {
        self.0.load(Ordering::Acquire)
    }
}

type InterruptCheck = Arc<dyn Fn() -> bool + Send + Sync>;

/// Optional deadline and cooperative cancellation attached to query evaluation.
#[derive(Clone, Default)]
pub struct ExecutionOptions {
    deadline: Option<Instant>,
    cancellation: Option<CancellationToken>,
    interrupt_check: Option<InterruptCheck>,
}

impl fmt::Debug for ExecutionOptions {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("ExecutionOptions")
            .field("deadline", &self.deadline)
            .field(
                "cancelled",
                &self
                    .cancellation
                    .as_ref()
                    .is_some_and(CancellationToken::is_cancelled),
            )
            .finish_non_exhaustive()
    }
}

impl ExecutionOptions {
    /// Stop work once `duration` has elapsed, measured from this call.
    #[must_use]
    pub fn timeout(duration: Duration) -> Self {
        Self {
            deadline: Some(Instant::now() + duration),
            ..Self::default()
        }
    }

    /// Stop work when `token` is cancelled.
    #[must_use]
    pub fn cancellable(token: CancellationToken) -> Self {
        Self {
            cancellation: Some(token),
            ..Self::default()
        }
    }

    /// Add or replace the deadline while retaining cancellation state.
    #[must_use]
    pub fn with_timeout(mut self, duration: Duration) -> Self {
        self.deadline = Some(Instant::now() + duration);
        self
    }

    /// Add or replace the cancellation token while retaining the deadline.
    #[must_use]
    pub fn with_cancellation(mut self, token: CancellationToken) -> Self {
        self.cancellation = Some(token);
        self
    }

    #[cfg(feature = "python")]
    pub(crate) fn with_interrupt_check(
        mut self,
        check: impl Fn() -> bool + Send + Sync + 'static,
    ) -> Self {
        self.interrupt_check = Some(Arc::new(check));
        self
    }

    pub(crate) fn interrupted(&self) -> bool {
        self.deadline
            .is_some_and(|deadline| Instant::now() >= deadline)
            || self
                .cancellation
                .as_ref()
                .is_some_and(CancellationToken::is_cancelled)
            || self.interrupt_check.as_ref().is_some_and(|check| check())
    }
}

pub(crate) fn interrupted_error() -> rusqlite::Error {
    rusqlite::Error::SqliteFailure(
        rusqlite::ffi::Error::new(rusqlite::ffi::SQLITE_INTERRUPT),
        Some("execution interrupted by cancellation or timeout".into()),
    )
}

pub(crate) fn with_sqlite_progress<T, E>(
    connection: &rusqlite::Connection,
    options: &ExecutionOptions,
    execute: impl FnOnce() -> Result<T, E>,
) -> Result<T, E>
where
    E: From<rusqlite::Error>,
{
    if options.interrupted() {
        return Err(interrupted_error().into());
    }
    let progress_options = options.clone();
    connection
        .progress_handler(1_000, Some(move || progress_options.interrupted()))
        .map_err(E::from)?;
    let result = execute();
    let reset = connection.progress_handler(0, None::<fn() -> bool>);
    match (result, reset) {
        (Ok(value), Ok(())) => Ok(value),
        (Err(error), _) => Err(error),
        (Ok(_), Err(error)) => Err(error.into()),
    }
}
