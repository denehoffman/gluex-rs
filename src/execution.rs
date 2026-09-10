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

/// Typed reason that active database execution stopped.
#[derive(Debug, Clone, Copy, PartialEq, Eq, thiserror::Error)]
pub enum ExecutionError {
    /// The configured active-work budget was exhausted.
    #[error("database execution timed out")]
    Timeout,
    /// A caller-owned cancellation token was cancelled.
    #[error("database execution cancelled")]
    Cancelled,
    /// The language binding or host interrupted execution.
    #[error("database execution interrupted")]
    Interrupted,
}

/// Optional deadline and cooperative cancellation attached to query evaluation.
#[derive(Clone, Default)]
pub struct ExecutionOptions {
    timeout: Option<Duration>,
    deadline: Option<Instant>,
    cancellation: Option<CancellationToken>,
    interrupt_check: Option<InterruptCheck>,
}

impl fmt::Debug for ExecutionOptions {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("ExecutionOptions")
            .field("deadline", &self.deadline)
            .field("timeout", &self.timeout)
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
    /// Configure an execution budget. Timing begins when a terminal starts.
    #[must_use]
    pub fn timeout(duration: Duration) -> Self {
        Self {
            timeout: Some(duration),
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

    /// Add or replace the execution budget while retaining cancellation state.
    #[must_use]
    pub const fn with_timeout(mut self, duration: Duration) -> Self {
        self.timeout = Some(duration);
        self.deadline = None;
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

    pub(crate) fn failure(&self) -> Option<ExecutionError> {
        if self.interrupt_check.as_ref().is_some_and(|check| check()) {
            Some(ExecutionError::Interrupted)
        } else if self
            .cancellation
            .as_ref()
            .is_some_and(CancellationToken::is_cancelled)
        {
            Some(ExecutionError::Cancelled)
        } else if self
            .deadline
            .is_some_and(|deadline| Instant::now() >= deadline)
        {
            Some(ExecutionError::Timeout)
        } else {
            None
        }
    }

    pub(crate) fn interrupted(&self) -> bool {
        self.failure().is_some()
    }

    fn started(&self) -> Self {
        let mut active = self.clone();
        if active.deadline.is_none() {
            active.deadline = active.timeout.map(|duration| Instant::now() + duration);
        }
        active
    }

    pub(crate) const fn configured_timeout(&self) -> Option<Duration> {
        self.timeout
    }

    pub(crate) const fn deadline_is_inactive(&self) -> bool {
        self.deadline.is_none()
    }

    pub(crate) fn for_stream_step(&self, remaining: Option<Duration>) -> Self {
        if self.deadline.is_some() {
            return self.clone();
        }
        let mut active = self.clone();
        active.timeout = remaining;
        active.started()
    }
}

pub(crate) fn interrupted_error() -> rusqlite::Error {
    rusqlite::Error::SqliteFailure(
        rusqlite::ffi::Error::new(rusqlite::ffi::SQLITE_INTERRUPT),
        Some("execution interrupted by cancellation or timeout".into()),
    )
}

/// Private contract implemented by lazy domain queries that participate in the
/// shared terminal-execution path.
///
/// Keeping this crate-private lets each domain retain its own public query and
/// result types while the mechanics around cancellation and binding adapters
/// remain uniform.
pub(crate) trait TerminalQuery: Clone {
    type Error;

    fn execution_options(&self) -> &ExecutionOptions;

    fn with_execution_options(&self, options: ExecutionOptions) -> Self;

    fn interruption_error(&self, failure: ExecutionError) -> Self::Error;
}

/// Remaining active-work budget for a lazy stream.
#[derive(Default)]
pub(crate) struct StreamBudget {
    remaining: Option<Duration>,
}

impl StreamBudget {
    pub(crate) const fn new(options: &ExecutionOptions) -> Self {
        Self {
            remaining: options.configured_timeout(),
        }
    }

    pub(crate) fn execute<Q, T>(
        &mut self,
        query: &Q,
        execute: impl FnOnce(&Q) -> Result<T, Q::Error>,
    ) -> Result<T, Q::Error>
    where
        Q: TerminalQuery,
    {
        let started = Instant::now();
        let active =
            query.with_execution_options(query.execution_options().for_stream_step(self.remaining));
        let result = execute_terminal(&active, execute);
        if query.execution_options().deadline_is_inactive() {
            self.remaining = self
                .remaining
                .map(|remaining| remaining.saturating_sub(started.elapsed()));
        }
        result
    }
}

/// Execute one active terminal step through the shared interruption boundary.
pub(crate) fn execute_terminal<Q, T>(
    query: &Q,
    execute: impl FnOnce(&Q) -> Result<T, Q::Error>,
) -> Result<T, Q::Error>
where
    Q: TerminalQuery,
{
    let active = query.with_execution_options(query.execution_options().started());
    if let Some(failure) = active.execution_options().failure() {
        return Err(active.interruption_error(failure));
    }
    let result = execute(&active);
    active
        .execution_options()
        .failure()
        .map_or(result, |failure| Err(active.interruption_error(failure)))
}

pub(crate) fn with_sqlite_progress<T, E>(
    connection: &rusqlite::Connection,
    options: &ExecutionOptions,
    execute: impl FnOnce() -> Result<T, E>,
) -> Result<T, E>
where
    E: From<rusqlite::Error> + From<ExecutionError>,
{
    let active = options.started();
    if let Some(failure) = active.failure() {
        return Err(failure.into());
    }
    let progress_options = active.clone();
    connection
        .progress_handler(1_000, Some(move || progress_options.interrupted()))
        .map_err(E::from)?;
    let result = execute();
    let reset = connection.progress_handler(0, None::<fn() -> bool>);
    match (result, reset, active.failure()) {
        (_, _, Some(failure)) => Err(failure.into()),
        (Ok(value), Ok(()), None) => Ok(value),
        (Err(error), _, None) => Err(error),
        (Ok(_), Err(error), None) => Err(error.into()),
    }
}

#[cfg(test)]
mod tests;
