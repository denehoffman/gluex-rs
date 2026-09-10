//! Independently configured database readers for a `GlueX` session.

use std::{env, fmt, path::PathBuf};

use crate::{ccdb::CCDB, rcdb::RCDB};
use thiserror::Error;

/// Configuration for one optional database source.
#[derive(Debug, Clone, Default)]
pub enum SourceConfig {
    /// Read the corresponding connection environment variable; absence disables it.
    #[default]
    FromEnv,
    /// Disable the source even if its environment variable is set.
    Disabled,
    /// Open a local SQLite file, overriding environment configuration.
    Sqlite(PathBuf),
}

impl SourceConfig {
    /// Configure an explicit SQLite file. Tilde expansion is supported.
    #[must_use]
    pub fn sqlite(path: impl Into<PathBuf>) -> Self {
        Self::Sqlite(path.into())
    }
}

/// A database capability available to a session.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum DatabaseKind {
    /// Recorded runs and conditions.
    Rcdb,
    /// Calibration tables and assignments.
    Ccdb,
}

impl DatabaseKind {
    /// Environment variable used to configure this source.
    #[must_use]
    pub const fn environment_variable(self) -> &'static str {
        match self {
            Self::Rcdb => "RCDB_CONNECTION",
            Self::Ccdb => "CCDB_CONNECTION",
        }
    }
}

impl fmt::Display for DatabaseKind {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(match self {
            Self::Rcdb => "RCDB",
            Self::Ccdb => "CCDB",
        })
    }
}

/// Configuration and missing-capability errors from a session.
#[derive(Debug, Error)]
pub enum GlueXError {
    /// A requested database was not configured.
    #[error("{0} is unavailable; pass an explicit SQLite path or set {environment}", environment = .0.environment_variable())]
    MissingCapability(DatabaseKind),
    /// A configured source could not be opened; it is not treated as absent.
    #[error(
        "cannot configure {database}: {reason}; supply a local SQLite file or disable the source"
    )]
    Configuration {
        /// Database whose configuration failed.
        database: DatabaseKind,
        /// Context describing the invalid configuration or opening failure.
        reason: String,
    },
}

/// Immutable description of configured database capabilities.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct Capabilities {
    rcdb: bool,
    ccdb: bool,
}

/// Inspectable bounds and current occupancy for session-owned metadata caches.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct CacheInfo {
    calibration_payload_capacity: usize,
    ccdb_metadata_entries: usize,
}

impl CacheInfo {
    /// Maximum decoded calibration payloads retained by one active stream.
    #[must_use]
    pub const fn calibration_payload_capacity(self) -> usize {
        self.calibration_payload_capacity
    }
    /// Current shared variation, inheritance and column-layout cache entries.
    #[must_use]
    pub const fn ccdb_metadata_entries(self) -> usize {
        self.ccdb_metadata_entries
    }
}

impl Capabilities {
    /// Whether recorded runs and conditions are available.
    #[must_use]
    pub const fn rcdb(self) -> bool {
        self.rcdb
    }

    /// Whether calibration tables and assignments are available.
    #[must_use]
    pub const fn ccdb(self) -> bool {
        self.ccdb
    }
}

/// Database-native access through shared readers.
///
/// Clones share connections and metadata caches. Keep files unchanged while any
/// reader or handle is in use. These are the existing backend-specific APIs.
#[derive(Debug, Clone)]
pub struct Sources {
    rcdb: Option<RCDB>,
    ccdb: Option<CCDB>,
}

impl Sources {
    /// Access recorded runs and conditions.
    ///
    /// # Errors
    /// Returns [`GlueXError::MissingCapability`] if RCDB is unconfigured.
    pub fn rcdb(&self) -> Result<&RCDB, GlueXError> {
        self.rcdb
            .as_ref()
            .ok_or(GlueXError::MissingCapability(DatabaseKind::Rcdb))
    }

    /// Access calibration metadata and payloads.
    ///
    /// # Errors
    /// Returns [`GlueXError::MissingCapability`] if CCDB is unconfigured.
    pub fn ccdb(&self) -> Result<&CCDB, GlueXError> {
        self.ccdb
            .as_ref()
            .ok_or(GlueXError::MissingCapability(DatabaseKind::Ccdb))
    }
}

/// A session with independently optional RCDB and CCDB capabilities.
///
/// Database-independent reference information remains available at the crate
/// root. Opening validates configured sources immediately. Clones share readers;
/// source files must remain unchanged while the session or its handles are used.
///
/// ```
/// use gluex_rs::{GlueX, SourceConfig, RunPeriod};
/// let gx = GlueX::open(SourceConfig::Disabled, SourceConfig::Disabled)?;
/// assert!(!gx.capabilities().rcdb());
/// assert!(RunPeriod::RP2018_08.min_run() > 0);
/// # Ok::<(), gluex_rs::GlueXError>(())
/// ```
#[derive(Clone)]
pub struct GlueX {
    sources: Sources,
}

impl GlueX {
    /// Inspect cache bounds and current shared metadata occupancy without querying rows.
    #[must_use]
    pub fn cache_info(&self) -> CacheInfo {
        let ccdb = self.sources.ccdb.as_ref();
        CacheInfo {
            calibration_payload_capacity: ccdb.map_or(0, crate::ccdb::CCDB::payload_cache_capacity),
            ccdb_metadata_entries: ccdb.map_or(0, crate::ccdb::CCDB::runtime_cache_entries),
        }
    }

    /// Set the per-stream decoded calibration payload budget. Values below one become one.
    pub fn set_calibration_payload_cache_capacity(&self, capacity: usize) {
        if let Some(ccdb) = &self.sources.ccdb {
            ccdb.set_payload_cache_capacity(capacity);
        }
    }

    /// Clear disposable CCDB metadata caches. Existing results remain immutable.
    pub fn clear_caches(&self) {
        if let Some(ccdb) = &self.sources.ccdb {
            ccdb.clear_runtime_caches();
        }
    }
    /// Access canonical `GlueX` workflows bound to this session's captured sources.
    #[must_use]
    pub fn workflows(&self) -> crate::Workflows {
        crate::Workflows::new(self.sources.clone())
    }
    /// Reopen the captured source paths and establish new opening defaults and caches.
    /// Existing queries, catalogs, readers and cloned sessions keep their original bindings.
    /// Environment variables are not consulted again. Keep files unchanged while in use;
    /// finish old-file work before replacing files. This does not preserve historical files.
    ///
    /// # Errors
    /// Returns a configuration error if either source cannot reopen. On failure this
    /// session is unchanged; a successfully opened sibling is not partially installed.
    pub fn refresh(&mut self) -> Result<(), GlueXError> {
        let rcdb = self
            .sources
            .rcdb
            .as_ref()
            .map_or(SourceConfig::Disabled, |r| {
                SourceConfig::sqlite(r.connection_path())
            });
        let ccdb = self
            .sources
            .ccdb
            .as_ref()
            .map_or(SourceConfig::Disabled, |r| {
                SourceConfig::sqlite(r.connection_path())
            });
        *self = Self::open(rcdb, ccdb)?;
        Ok(())
    }

    /// Inspect full-path calibration and directory catalogs without fetching constants.
    ///
    /// # Errors
    /// Returns a missing-capability error when CCDB is unavailable.
    pub fn calibrations(&self) -> Result<crate::CalibrationCatalog, GlueXError> {
        Ok(crate::CalibrationCatalog::new(self.sources.ccdb()?))
    }

    /// Inspect the immutable Condition Definition catalog, including dynamic database names.
    ///
    /// # Errors
    /// Returns a missing-capability error when RCDB is unavailable.
    pub fn conditions(&self) -> Result<crate::ConditionCatalog, GlueXError> {
        Ok(self.sources.rcdb()?.conditions())
    }

    /// Build a lazy recorded Run Query with explicit numeric scope and no scientific cuts.
    ///
    /// # Errors
    /// Returns a missing-capability error when RCDB is unavailable.
    pub fn runs(&self, selection: crate::RunSelection) -> Result<crate::RunQuery, GlueXError> {
        Ok(crate::RunQuery::new(
            self.sources.rcdb()?.clone(),
            selection,
        ))
    }

    /// Open a session with independent source configurations.
    ///
    /// [`SourceConfig::FromEnv`] consults only the corresponding environment
    /// variable. An unset variable means unavailable; an empty or invalid value
    /// is an error. Explicit paths override the environment and
    /// [`SourceConfig::Disabled`] suppresses it. Only local SQLite files are supported.
    ///
    /// # Errors
    /// Returns [`GlueXError::Configuration`] if a configured source cannot open.
    pub fn open(rcdb: SourceConfig, ccdb: SourceConfig) -> Result<Self, GlueXError> {
        Ok(Self {
            sources: Sources {
                rcdb: open_source(rcdb, DatabaseKind::Rcdb, |path| {
                    let reader = RCDB::open(path)?;
                    reader.validate_read_schema()?;
                    Ok::<_, crate::rcdb::RCDBError>(reader)
                })?,
                ccdb: open_source(ccdb, DatabaseKind::Ccdb, |path| {
                    let reader = CCDB::open(path)?;
                    reader.validate_read_schema()?;
                    Ok::<_, crate::ccdb::CCDBError>(reader)
                })?,
            },
        })
    }

    /// Open using `RCDB_CONNECTION` and `CCDB_CONNECTION` independently.
    ///
    /// # Errors
    /// Returns [`GlueXError::Configuration`] if a configured source cannot open.
    pub fn from_env() -> Result<Self, GlueXError> {
        Self::open(SourceConfig::FromEnv, SourceConfig::FromEnv)
    }

    /// Inspect capabilities without querying the databases.
    #[must_use]
    pub const fn capabilities(&self) -> Capabilities {
        Capabilities {
            rcdb: self.sources.rcdb.is_some(),
            ccdb: self.sources.ccdb.is_some(),
        }
    }

    /// Access the existing database-native reading APIs.
    #[must_use]
    pub const fn sources(&self) -> &Sources {
        &self.sources
    }
}

impl fmt::Debug for GlueX {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        fmt::Display::fmt(self, f)
    }
}

impl fmt::Display for GlueX {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(
            f,
            "GlueX(rcdb={}, ccdb={})",
            self.sources
                .rcdb
                .as_ref()
                .map_or("unavailable", RCDB::connection_path),
            self.sources
                .ccdb
                .as_ref()
                .map_or("unavailable", CCDB::connection_path)
        )
    }
}

fn open_source<T, E: fmt::Display>(
    config: SourceConfig,
    database: DatabaseKind,
    open: impl FnOnce(PathBuf) -> Result<T, E>,
) -> Result<Option<T>, GlueXError> {
    let path = match config {
        SourceConfig::Disabled => return Ok(None),
        SourceConfig::FromEnv => match env::var_os(database.environment_variable()) {
            Some(path) => PathBuf::from(path),
            None => return Ok(None),
        },
        SourceConfig::Sqlite(path) => path,
    };
    let invalid = |reason| GlueXError::Configuration { database, reason };
    if path.as_os_str().is_empty() {
        return Err(invalid("empty connection value".into()));
    }
    // A connection URL must not accidentally become a local filename or fall
    // back to another source. Do not echo credentials from unsupported URLs.
    if let Some((scheme, _)) = path.to_string_lossy().split_once("://") {
        return Err(invalid(format!(
            "unsupported connection scheme {scheme:?}; use a filesystem path"
        )));
    }
    open(path.clone())
        .map(Some)
        .map_err(|error| invalid(format!("{}: {error}", path.display())))
}
