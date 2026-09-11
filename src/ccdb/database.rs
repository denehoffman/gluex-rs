use crate::ccdb::{
    CCDBError, CCDBResult,
    assignment::{AssignmentCandidate, ResolvedAssignment, resolve_candidates},
    context::{CCDBContext, Request},
    data::{ColumnLayout, Data},
    models::{
        ColumnMeta, ColumnType, ConstantSetMeta, DirectoryMeta, TypeTableMeta, VariationMeta,
    },
};
use crate::core::{Id, RunNumber, utils::resolve_path};
use chrono::{DateTime, Utc};
use dashmap::DashMap;
use parking_lot::{Mutex, MutexGuard};
use rusqlite::{Connection, OpenFlags};
use std::{
    collections::{BTreeMap, HashSet},
    env,
    path::Path,
    sync::{
        Arc,
        atomic::{AtomicUsize, Ordering},
    },
};

fn normalize_path(base: &str, path: &str) -> String {
    let mut segments: Vec<String> = Vec::new();
    let mut push_parts = |value: &str| {
        for part in value.split('/') {
            if part.is_empty() || part == "." {
                continue;
            }
            if part == ".." {
                segments.pop();
            } else {
                segments.push(part.to_string());
            }
        }
    };
    if !path.starts_with('/') {
        push_parts(base);
    }
    push_parts(path);
    if segments.is_empty() {
        "/".to_string()
    } else {
        format!("/{}", segments.join("/"))
    }
}

/// Read-only client for the Jefferson Lab Calibration and Conditions Database.
#[derive(Debug, Clone)]
pub struct CCDB {
    connection: Arc<Mutex<Connection>>,
    connection_path: String,
    opened_at: DateTime<Utc>,
    variation_cache: Arc<DashMap<String, VariationMeta>>,
    variation_chain_cache: Arc<DashMap<Id, Vec<VariationMeta>>>,
    directory_meta: Arc<DashMap<Id, DirectoryMeta>>,
    directory_by_path: Arc<DashMap<String, Id>>,
    table_meta: Arc<DashMap<Id, TypeTableMeta>>,
    table_by_dir_name: Arc<DashMap<(Id, String), Id>>,
    column_layouts: Arc<DashMap<Id, Arc<ColumnLayout>>>,
    payload_cache_capacity: Arc<AtomicUsize>,
}

impl CCDB {
    pub(crate) fn catalog_tables(&self) -> Vec<TypeTableHandle> {
        self.table_meta
            .iter()
            .map(|meta| TypeTableHandle {
                db: self.clone(),
                meta: meta.value().clone(),
            })
            .collect()
    }
    pub(crate) fn catalog_directories(&self) -> Vec<DirectoryHandle> {
        self.directory_meta
            .iter()
            .map(|meta| DirectoryHandle {
                db: self.clone(),
                meta: meta.value().clone(),
            })
            .collect()
    }

    /// Opens a read-only handle using the `CCDB_CONNECTION` environment variable.
    ///
    /// # Errors
    ///
    /// This method returns an error if the environment variable is not set or the database cannot be opened.
    pub fn new() -> CCDBResult<Self> {
        let path = env::var("CCDB_CONNECTION")
            .map_err(|_| CCDBError::MissingConnectionEnv("CCDB_CONNECTION".to_string()))?;
        Self::open(path)
    }

    /// Opens a read-only connection to an existing CCDB `SQLite` database file.
    ///
    /// # Errors
    ///
    /// This method returns an error if the database cannot be opened.
    pub fn open(path: impl AsRef<Path>) -> CCDBResult<Self> {
        let resolved_path = resolve_path(path)?;
        let path_str = resolved_path.to_string_lossy().to_string();
        let conn = Connection::open_with_flags(&resolved_path, OpenFlags::SQLITE_OPEN_READ_ONLY)?;
        conn.pragma_update(None, "foreign_keys", "ON")?; // TODO: check
        crate::raw::restrict(&conn)?;
        let db = Self {
            connection: Arc::new(Mutex::new(conn)),
            variation_cache: Arc::new(DashMap::new()),
            variation_chain_cache: Arc::new(DashMap::new()),
            directory_meta: Arc::new(DashMap::new()),
            directory_by_path: Arc::new(DashMap::new()),
            table_meta: Arc::new(DashMap::new()),
            table_by_dir_name: Arc::new(DashMap::new()),
            column_layouts: Arc::new(DashMap::new()),
            payload_cache_capacity: Arc::new(AtomicUsize::new(128)),
            connection_path: path_str,
            opened_at: Utc::now(),
        };
        db.load_directories()?;
        db.load_tables()?;
        Ok(db)
    }

    pub(crate) fn runtime_cache_entries(&self) -> usize {
        self.variation_cache.len() + self.variation_chain_cache.len() + self.column_layouts.len()
    }

    pub(crate) fn clear_runtime_caches(&self) {
        self.variation_cache.clear();
        self.variation_chain_cache.clear();
        self.column_layouts.clear();
    }

    pub(crate) fn payload_cache_capacity(&self) -> usize {
        self.payload_cache_capacity.load(Ordering::Acquire)
    }

    pub(crate) fn set_payload_cache_capacity(&self, capacity: usize) {
        self.payload_cache_capacity
            .store(capacity.max(1), Ordering::Release);
    }
    /// Execute one parameterized read-only SQLite statement, returning immutable rows.
    ///
    /// Positional parameters support NULL, integers, reals, text and blobs.
    /// Result column names can repeat; use positional values for unambiguous access.
    ///
    /// # Errors
    /// Returns an error for unauthorized SQL, multiple statements, invalid parameters,
    /// database failures or malformed UTF-8 text.
    pub fn raw(
        &self,
        sql: &str,
        parameters: &[crate::RawValue],
    ) -> Result<crate::RawResults, crate::RawError> {
        crate::raw::query(&self.connection(), sql, parameters)
    }

    /// Execute a raw read with cooperative cancellation and an optional deadline.
    ///
    /// # Errors
    /// Returns the same errors as [`Self::raw`], including an interrupted SQLite
    /// error when cancellation or the deadline stops evaluation.
    pub fn raw_with_options(
        &self,
        sql: &str,
        parameters: &[crate::RawValue],
        options: &crate::ExecutionOptions,
    ) -> Result<crate::RawResults, crate::RawError> {
        crate::raw::query_with_options(&self.connection(), sql, parameters, options)
    }

    /// Returns the underlying [`rusqlite::Connection`].
    pub(crate) fn connection(&self) -> MutexGuard<'_, Connection> {
        self.connection.lock()
    }
    /// Returns the filesystem path used to open the database.
    #[must_use]
    pub fn connection_path(&self) -> &str {
        &self.connection_path
    }

    /// Time captured once when this reader opened, shared by all clones.
    ///
    /// This is an exploratory calibration default, not a historical snapshot.
    #[must_use]
    pub const fn opened_at(&self) -> DateTime<Utc> {
        self.opened_at
    }

    /// Build an independent context using `default` variation and opening time.
    ///
    /// Supply explicit numeric runs. Subsequent context overrides do not change
    /// this reader's defaults. [`CCDBContext::default`] remains a standalone
    /// context using its construction time.
    #[must_use]
    pub fn default_context(&self, runs: impl IntoIterator<Item = RunNumber>) -> CCDBContext {
        CCDBContext {
            runs: runs.into_iter().collect(),
            variation: "default".into(),
            timestamp: self.opened_at,
        }
    }

    pub(crate) fn validate_read_schema(&self) -> CCDBResult<()> {
        let connection = self.connection();
        // Opening already loaded directories and typeTables. Prepare the other
        // read projections to validate their tables/columns without reading rows
        // or decoding calibration payloads. Empty but valid tables are allowed.
        for query in [
            "SELECT id, created, modified, name, typeId, columnType, `order`, comment FROM columns LIMIT 0",
            "SELECT id, created, modified, name, description, authorId, comment,
                    parentId, isLocked, lockTime, lockedByUserId, goBackBehavior,
                    goBackTime, isDeprecated, deprecatedByUserId FROM variations LIMIT 0",
            "SELECT id, created, constantSetId, runRangeId, variationId FROM assignments LIMIT 0",
            "SELECT id, created, modified, vault, constantTypeId FROM constantSets LIMIT 0",
            "SELECT id, runMin, runMax FROM runRanges LIMIT 0",
        ] {
            drop(connection.prepare(query)?);
        }
        Ok(())
    }
    fn load_directories(&self) -> CCDBResult<()> {
        let connection = self.connection();
        let mut stmt = connection.prepare(
            "SELECT id, created, modified, name, parentId, authorId, comment,
                    isDeprecated, deprecatedByUserId, isLocked, lockedByUserId
             FROM directories",
        )?;
        let rows = stmt.query_map([], |row| {
            Ok(DirectoryMeta {
                id: row.get(0)?,
                created: row.get(1)?,
                modified: row.get(2)?,
                name: row.get(3)?,
                parent_id: row.get(4)?,
                author_id: row.get(5)?,
                comment: row.get(6).unwrap_or_default(),
                is_deprecated: row.get(7).unwrap_or_default(),
                deprecated_by_user_id: row.get(8).unwrap_or_default(),
                is_locked: row.get(9).unwrap_or_default(),
                locked_by_user_id: row.get(10).unwrap_or_default(),
            })
        })?;
        self.directory_meta.clear();
        self.directory_by_path.clear();
        for dir in rows {
            let dir = dir?;
            let id = dir.id;
            let path = self.build_dir_path_from_meta(&dir);
            self.directory_by_path.insert(path, id);
            self.directory_meta.insert(id, dir);
        }
        Ok(())
    }
    fn build_dir_path_from_meta(&self, dir: &DirectoryMeta) -> String {
        if dir.parent_id == 0 {
            format!("/{}", dir.name)
        } else if let Some(parent) = self.directory_meta.get(&dir.parent_id) {
            let mut p = self.build_dir_path_from_meta(&parent);
            if !p.ends_with('/') {
                p.push('/');
            }
            p.push_str(&dir.name);
            p
        } else {
            format!("/{}", dir.name)
        }
    }
    fn load_tables(&self) -> CCDBResult<()> {
        let connection = self.connection();
        let mut stmt = connection.prepare(
            "SELECT id, created, modified, directoryId, name,
                    nRows, nColumns, nAssignments, authorId, comment,
                    isDeprecated, deprecatedByUserId, isLocked, lockedByUserId, lockTime
             FROM typeTables",
        )?;
        let rows = stmt.query_map([], |row| {
            Ok(TypeTableMeta {
                id: row.get(0)?,
                created: row.get(1)?,
                modified: row.get(2)?,
                directory_id: row.get(3)?,
                name: row.get(4)?,
                n_rows: row.get(5)?,
                n_columns: row.get(6)?,
                n_assignments: row.get(7)?,
                author_id: row.get(8)?,
                comment: row.get(9).unwrap_or_default(),
                is_deprecated: row.get(10).unwrap_or_default(),
                deprecated_by_user_id: row.get(11).unwrap_or_default(),
                is_locked: row.get(12).unwrap_or_default(),
                locked_by_user_id: row.get(13).unwrap_or_default(),
                lock_time: row.get(14).unwrap_or_default(),
            })
        })?;
        self.table_meta.clear();
        self.table_by_dir_name.clear();
        for table in rows {
            let table = table?;
            let id = table.id;
            let key = (table.directory_id, table.name.clone());
            self.table_by_dir_name.insert(key, id);
            self.table_meta.insert(id, table);
        }
        Ok(())
    }

    /// Returns a handle to the virtual root directory.
    #[must_use]
    pub fn root(&self) -> DirectoryHandle {
        DirectoryHandle {
            db: self.clone(),
            meta: DirectoryMeta {
                id: 0,
                name: String::new(),
                ..Default::default()
            },
        }
    }

    /// Resolves an absolute or relative directory path into a handle.
    ///
    /// # Errors
    ///
    /// This method returns an error if the directory cannot be found.
    pub fn dir(&self, path: &str) -> CCDBResult<DirectoryHandle> {
        if path == "/" || path.is_empty() {
            return Ok(self.root());
        }
        let norm = normalize_path("/", path);
        let id = self
            .directory_by_path
            .get(&norm)
            .ok_or_else(|| CCDBError::DirectoryNotFoundError(norm.clone()))?;
        let meta = self
            .directory_meta
            .get(&id)
            .ok_or_else(|| CCDBError::DirectoryNotFoundError(norm.clone()))?;
        Ok(DirectoryHandle {
            db: self.clone(),
            meta: meta.clone(),
        })
    }

    /// Resolves a table path ("/dir/name") into a handle.
    ///
    /// # Errors
    ///
    /// This method returns an error if the table cannot be found.
    pub fn table(&self, path: &str) -> CCDBResult<TypeTableHandle> {
        let norm = normalize_path("/", path);
        let (dir_path, table_name) = match norm.rsplit_once('/') {
            Some((parent, name)) if !name.is_empty() => (parent, name),
            _ => return Err(CCDBError::InvalidPathError(norm)),
        };
        let dir = self.dir(dir_path)?;
        dir.table(table_name)
    }
    /// Loads variation metadata, caching repeated lookups.
    ///
    /// # Errors
    ///
    /// This method returns an error if the variation cannot be found.
    pub fn variation(&self, name: &str) -> CCDBResult<VariationMeta> {
        if let Some(v) = self.variation_cache.get(name) {
            return Ok(v.clone());
        }
        let connection = self.connection();
        let mut stmt = connection.prepare_cached(
            "SELECT id, created, modified, name, description, authorId, comment,
                    parentId, isLocked, lockTime, lockedByUserId,
                    goBackBehavior, goBackTime, isDeprecated, deprecatedByUserId
             FROM variations
             WHERE name = ?",
        )?;
        let mut rows = stmt.query([name])?;
        if let Some(r) = rows.next()? {
            let var = VariationMeta {
                id: r.get(0)?,
                created: r.get(1)?,
                modified: r.get(2)?,
                name: r.get(3)?,
                description: r.get(4).unwrap_or_default(),
                author_id: r.get(5)?,
                comment: r.get(6).unwrap_or_default(),
                parent_id: r.get(7)?,
                is_locked: r.get(8).unwrap_or_default(),
                lock_time: r.get(9).unwrap_or_default(),
                locked_by_user_id: r.get(10).unwrap_or_default(),
                go_back_behavior: r.get(11).unwrap_or_default(),
                go_back_time: r.get(12).unwrap_or_default(),
                is_deprecated: r.get(13).unwrap_or_default(),
                deprecated_by_user_id: r.get(14).unwrap_or_default(),
            };
            self.variation_cache.insert(name.to_string(), var.clone());
            Ok(var)
        } else {
            Err(CCDBError::VariationNotFoundError(name.to_string()))
        }
    }
    /// Resolves a variation chain from the given starting variation up to the root.
    ///
    /// # Errors
    ///
    /// This method returns an error if any of the variations cannot be found.
    pub fn variation_chain(&self, start: &VariationMeta) -> CCDBResult<Vec<VariationMeta>> {
        if let Some(cached) = self.variation_chain_cache.get(&start.id) {
            return Ok(cached.clone());
        }
        let mut chain = Vec::new();
        let mut current = start.clone();

        chain.push(current.clone());
        let connection = self.connection();
        let mut stmt = connection.prepare_cached(
            "SELECT id, created, modified, name, description, authorId, comment,
                    parentId, isLocked, lockTime, lockedByUserId,
                    goBackBehavior, goBackTime, isDeprecated, deprecatedByUserId
             FROM variations
             WHERE id = ?",
        )?;

        let mut visited = HashSet::from([current.id]);
        while current.parent_id > 0 {
            if !visited.insert(current.parent_id) {
                return Err(CCDBError::InvalidMetadata(format!(
                    "variation {} has a parent cycle",
                    start.name
                )));
            }
            let mut rows = stmt.query([current.parent_id])?;
            if let Some(r) = rows.next()? {
                current = VariationMeta {
                    id: r.get(0)?,
                    created: r.get(1)?,
                    modified: r.get(2)?,
                    name: r.get(3)?,
                    description: r.get(4).unwrap_or_default(),
                    author_id: r.get(5)?,
                    comment: r.get(6).unwrap_or_default(),
                    parent_id: r.get(7)?,
                    is_locked: r.get(8).unwrap_or_default(),
                    lock_time: r.get(9).unwrap_or_default(),
                    locked_by_user_id: r.get(10).unwrap_or_default(),
                    go_back_behavior: r.get(11).unwrap_or(0),
                    go_back_time: r.get(12).unwrap_or_default(),
                    is_deprecated: r.get(13).unwrap_or_default(),
                    deprecated_by_user_id: r.get(14).unwrap_or_default(),
                };
                chain.push(current.clone());
            } else {
                return Err(CCDBError::InvalidMetadata(format!(
                    "variation {} has missing parent {}",
                    current.name, current.parent_id
                )));
            }
        }

        self.variation_chain_cache.insert(start.id, chain.clone());
        Ok(chain)
    }
    /// Parses a request string of the form "/path:run:variation:timestamp" (see [`Request`]) and fetches data.
    ///
    /// # Errors
    ///
    /// This method returns an error if the request string cannot be parsed, the parsed table path
    /// does not exist, or an error occurs while fetching data.
    pub fn request(&self, request_string: &str) -> CCDBResult<BTreeMap<RunNumber, Data>> {
        let request: Request = request_string.parse()?;
        let table = self.table(request.path.full_path())?;
        table.fetch(&request.context)
    }

    /// Fetches data for a table path using the supplied [`CCDBContext`].
    /// # Errors
    ///
    /// This method returns an error if the parsed table path
    /// does not exist or an error occurs while fetching data.
    pub fn fetch(&self, path: &str, ctx: &CCDBContext) -> CCDBResult<BTreeMap<RunNumber, Data>> {
        self.fetch_with_options(path, ctx, &crate::ExecutionOptions::default())
    }

    pub(crate) fn fetch_with_options(
        &self,
        path: &str,
        ctx: &CCDBContext,
        options: &crate::ExecutionOptions,
    ) -> CCDBResult<BTreeMap<RunNumber, Data>> {
        let table = self.table(path)?;
        table.fetch_with_options(ctx, options)
    }
}

/// Handle to a CCDB directory, allowing navigation and table discovery.
#[derive(Debug, Clone)]
pub struct DirectoryHandle {
    db: CCDB,
    pub(crate) meta: DirectoryMeta,
}

impl DirectoryHandle {
    /// Returns the directory metadata as loaded from CCDB.
    #[must_use]
    pub const fn meta(&self) -> &DirectoryMeta {
        &self.meta
    }
    /// Returns the absolute path for this directory.
    #[must_use]
    pub fn full_path(&self) -> String {
        if self.meta.id == 0 {
            "/".to_string()
        } else {
            let mut names = Vec::new();
            let mut current = self.meta.clone();
            loop {
                if current.parent_id == 0 {
                    names.push(current.name);
                    break;
                }
                names.push(current.name.clone());
                if let Some(parent) = self.db.directory_meta.get(&current.parent_id) {
                    current = parent.clone();
                } else {
                    break;
                }
            }
            names.reverse();
            format!("/{}", names.join("/"))
        }
    }
    /// Returns the parent directory, if one exists.
    #[must_use]
    pub fn parent(&self) -> Option<Self> {
        if self.meta.parent_id == 0 {
            None
        } else {
            Some(Self {
                db: self.db.clone(),
                meta: self.db.directory_meta.get(&self.meta.parent_id)?.clone(),
            })
        }
    }
    /// Lists subdirectories directly under this directory.
    #[must_use]
    pub fn dirs(&self) -> Vec<Self> {
        self.db
            .directory_meta
            .iter()
            .filter(|meta| meta.parent_id == self.meta.id)
            .map(|meta| Self {
                db: self.db.clone(),
                meta: meta.value().clone(),
            })
            .collect()
    }
    /// Resolves a child directory given a relative path.
    ///
    /// # Errors
    ///
    /// This method returns an error if the directory cannot be found.
    pub fn dir(&self, path: &str) -> CCDBResult<Self> {
        let target = normalize_path(&self.full_path(), path);
        self.db.dir(&target)
    }
    /// Lists tables that live directly under this directory.
    #[must_use]
    pub fn tables(&self) -> Vec<TypeTableHandle> {
        self.db
            .table_meta
            .iter()
            .filter(|meta| meta.directory_id == self.meta.id)
            .map(|meta| TypeTableHandle {
                db: self.db.clone(),
                meta: meta.value().clone(),
            })
            .collect()
    }
    /// Resolves a table within this directory by name.
    ///
    /// # Errors
    ///
    /// This method returns an error if the table cannot be found.
    pub fn table(&self, name: &str) -> CCDBResult<TypeTableHandle> {
        let id = self
            .db
            .table_by_dir_name
            .get(&(self.meta.id, name.to_string()))
            .ok_or_else(|| {
                CCDBError::TableNotFoundError(format!("{}/{}", self.full_path(), name))
            })?;
        let meta = self.db.table_meta.get(&id).ok_or_else(|| {
            CCDBError::TableNotFoundError(format!("{}/{}", self.full_path(), name))
        })?;
        Ok(TypeTableHandle {
            db: self.db.clone(),
            meta: meta.clone(),
        })
    }
}

/// Handle to a CCDB table, enabling metadata inspection and data fetches.
#[derive(Debug, Clone)]
pub struct TypeTableHandle {
    db: CCDB,
    pub(crate) meta: TypeTableMeta,
}
impl TypeTableHandle {
    pub(crate) fn payload_cache_capacity(&self) -> usize {
        self.db.payload_cache_capacity()
    }
    /// Build a context with this table's source opening time and `default` variation.
    ///
    /// Context overrides do not change the source defaults.
    #[must_use]
    pub fn default_context(&self, runs: impl IntoIterator<Item = RunNumber>) -> CCDBContext {
        self.db.default_context(runs)
    }

    /// Returns the table metadata as loaded from CCDB.
    #[must_use]
    pub const fn meta(&self) -> &TypeTableMeta {
        &self.meta
    }
    /// Returns the table name (without parent path components).
    #[must_use]
    pub fn name(&self) -> &str {
        &self.meta.name
    }
    /// Returns the unique numeric identifier for this table.
    #[must_use]
    pub const fn id(&self) -> Id {
        self.meta.id
    }
    /// Returns the absolute path of this table, including directory prefix.
    #[must_use]
    pub fn full_path(&self) -> String {
        let dir_meta = self.db.directory_meta.get(&self.meta.directory_id);
        dir_meta.map_or_else(
            || format!("/{}", self.meta.name),
            |dir_meta| {
                let dir = DirectoryHandle {
                    db: self.db.clone(),
                    meta: dir_meta.clone(),
                };
                let mut p = dir.full_path();
                if !p.ends_with('/') {
                    p.push('/');
                }
                p.push_str(&self.meta.name);
                p
            },
        )
    }
    /// Loads column metadata for this table.
    ///
    /// # Errors
    ///
    /// This method will fail if the underlying SQL query fails or any part of the `columns` table
    /// fails to parse.
    pub fn columns(&self) -> CCDBResult<Vec<ColumnMeta>> {
        Ok(self.column_layout()?.columns().to_vec())
    }
    fn load_column_metadata(&self) -> CCDBResult<Vec<ColumnMeta>> {
        let connection = self.db.connection();
        let mut stmt = connection.prepare_cached(
            "SELECT id, created, modified, name, typeId, columnType, `order`, comment
             FROM columns
             WHERE typeId = ?
             ORDER BY `order`",
        )?;
        let columns = stmt
            .query_map([self.meta.id], |row| {
                Ok(ColumnMeta {
                    id: row.get(0)?,
                    created: row.get(1)?,
                    modified: row.get(2)?,
                    name: row.get(3).unwrap_or_default(),
                    type_id: row.get(4)?,
                    column_type: {
                        let raw: String = row.get(5)?;
                        ColumnType::type_from_str(&raw).ok_or_else(|| {
                            rusqlite::Error::FromSqlConversionFailure(
                                5,
                                rusqlite::types::Type::Text,
                                Box::new(CCDBError::InvalidMetadata(format!(
                                    "unknown column type {raw:?}"
                                ))),
                            )
                        })?
                    },
                    order: row.get(6)?,
                    comment: row.get(7).unwrap_or_default(),
                })
            })?
            .collect::<Result<Vec<ColumnMeta>, _>>()?;
        Ok(columns)
    }

    pub(crate) fn column_layout(&self) -> CCDBResult<Arc<ColumnLayout>> {
        if let Some(existing) = self.db.column_layouts.get(&self.meta.id) {
            return Ok(existing.clone());
        }
        let columns = self.load_column_metadata()?;
        if usize::try_from(self.meta.n_columns).ok() != Some(columns.len()) || columns.is_empty() {
            return Err(CCDBError::InvalidMetadata(format!(
                "{}: declared column count does not match column definitions",
                self.full_path()
            )));
        }
        let mut names = HashSet::new();
        for (index, column) in columns.iter().enumerate() {
            if usize::try_from(column.order).ok() != Some(index)
                || !names.insert(column.name.clone())
            {
                return Err(CCDBError::InvalidMetadata(format!(
                    "{}: duplicate column names or invalid column order",
                    self.full_path()
                )));
            }
        }
        let layout = Arc::new(ColumnLayout::new(columns));
        self.db.column_layouts.insert(self.meta.id, layout.clone());
        Ok(layout)
    }
    /// Fetches data for this table using the provided query context.
    ///
    /// # Errors
    ///
    /// Returns an error if resolving assignments fails, if any SQL queries fail, or if vault data
    /// cannot be decoded for the requested runs.
    pub fn fetch(&self, ctx: &CCDBContext) -> CCDBResult<BTreeMap<RunNumber, Data>> {
        self.fetch_with_options(ctx, &crate::ExecutionOptions::default())
    }

    pub(crate) fn fetch_with_options(
        &self,
        ctx: &CCDBContext,
        options: &crate::ExecutionOptions,
    ) -> CCDBResult<BTreeMap<RunNumber, Data>> {
        let runs = ctx.runs.clone();
        let assignments =
            self.resolve_assignments_with_options(&runs, &ctx.variation, ctx.timestamp, options)?;
        if assignments.is_empty() {
            return Ok(BTreeMap::new());
        }
        self.load_vaults_with_options(&assignments, options)
    }
    pub(crate) fn resolve_assignments_with_options(
        &self,
        runs: &[RunNumber],
        variation: &str,
        timestamp: DateTime<Utc>,
        options: &crate::ExecutionOptions,
    ) -> CCDBResult<BTreeMap<RunNumber, ResolvedAssignment>> {
        if options.interrupted() {
            return Err(crate::execution::interrupted_error().into());
        }
        let start_var_meta = self.db.variation(variation)?;
        let var_chain = self.db.variation_chain(&start_var_meta)?;
        if runs.is_empty() {
            return Ok(BTreeMap::new());
        }
        let min_run = *runs.iter().min().expect("this is a bug, please report it!");
        let max_run = *runs.iter().max().expect("this is a bug, please report it!");
        let mut final_assignments: BTreeMap<RunNumber, ResolvedAssignment> = BTreeMap::new();
        let mut unresolved: HashSet<RunNumber> = runs.iter().copied().collect();
        for var_meta in var_chain {
            if options.interrupted() {
                return Err(crate::execution::interrupted_error().into());
            }
            if unresolved.is_empty() {
                break;
            }
            let partial = self.resolve_assignments_for_variation(
                &unresolved,
                &var_meta,
                timestamp,
                min_run,
                max_run,
                options,
            )?;
            for (run, meta) in partial {
                final_assignments.insert(run, meta);
                unresolved.remove(&run);
            }
        }
        Ok(final_assignments)
    }
    fn resolve_assignments_for_variation(
        &self,
        runs: &HashSet<RunNumber>,
        var_meta: &VariationMeta,
        timestamp: DateTime<Utc>,
        min_run: RunNumber,
        max_run: RunNumber,
        options: &crate::ExecutionOptions,
    ) -> CCDBResult<BTreeMap<RunNumber, ResolvedAssignment>> {
        let connection = self.db.connection();
        crate::execution::with_sqlite_progress(&connection, options, || -> CCDBResult<_> {
            let mut stmt = connection.prepare_cached(
                "SELECT
                 a.id, a.created, a.constantSetId,
                 cs.id, cs.created, cs.modified, cs.vault, cs.constantTypeId,
                 rr.runMin, rr.runMax
             FROM assignments a
             JOIN constantSets cs ON cs.id = a.constantSetId
             LEFT JOIN runRanges rr ON rr.id = a.runRangeId
             WHERE cs.constantTypeId = ?
               AND a.variationId = ?
               AND (rr.id IS NULL
                    OR (rr.runMin <= rr.runMax AND rr.runMax >= ? AND rr.runMin <= ?))",
            )?;
            let raw_candidates = stmt
                .query_map((self.meta.id, var_meta.id, min_run, max_run), |row| {
                    let id: Id = row.get(0)?;
                    let created: String = row.get(1)?;
                    let constant_set_id: Id = row.get(2)?;
                    let constant_set = ConstantSetMeta {
                        id: row.get(3)?,
                        created: row.get(4)?,
                        modified: row.get(5)?,
                        vault: row.get(6)?,
                        constant_type_id: row.get(7)?,
                    };
                    let run_min: RunNumber = row.get(8)?;
                    let run_max: RunNumber = row.get(9)?;
                    Ok((id, created, constant_set_id, constant_set, run_min, run_max))
                })?
                .collect::<Result<Vec<_>, _>>()?;
            let candidates = raw_candidates
                .into_iter()
                .map(
                    |(id, created, constant_set_id, constant_set, run_min, run_max)| {
                        if constant_set.id != constant_set_id {
                            return Err(CCDBError::InvalidMetadata(format!(
                                "assignment {id} references inconsistent constant set metadata"
                            )));
                        }
                        AssignmentCandidate::try_new(id, &created, constant_set, run_min, run_max)
                    },
                )
                .collect::<CCDBResult<Vec<_>>>()?;
            resolve_candidates(runs, &candidates, &var_meta.name, timestamp, options)
        })
    }
    fn load_vaults_with_options(
        &self,
        assignments: &BTreeMap<RunNumber, ResolvedAssignment>,
        options: &crate::ExecutionOptions,
    ) -> CCDBResult<BTreeMap<RunNumber, Data>> {
        if assignments.is_empty() {
            return Ok(BTreeMap::new());
        }
        let layout = self.column_layout()?;
        #[allow(clippy::cast_possible_truncation, clippy::cast_sign_loss)]
        let n_rows = self.meta.n_rows as usize;
        assignments
            .iter()
            .map(|(run, constant_set)| {
                if options.interrupted() {
                    return Err(crate::execution::interrupted_error().into());
                }
                Ok((
                    *run,
                    Data::from_vault(&constant_set.constant_set.vault, layout.clone(), n_rows)?,
                ))
            })
            .collect::<CCDBResult<BTreeMap<RunNumber, Data>>>()
    }
}
