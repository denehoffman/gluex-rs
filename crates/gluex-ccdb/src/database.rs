use crate::{
    context::{Context, Request},
    data::{ColumnLayout, Data},
    models::{
        AssignmentMetaLite, ColumnMeta, ColumnType, ConstantSetMeta, DirectoryMeta, TypeTableMeta,
        VariationMeta,
    },
    CCDBError, CCDBResult,
};
use chrono::{DateTime, Utc};
use dashmap::DashMap;
use gluex_core::{Id, RunNumber};
use parking_lot::{Mutex, MutexGuard};
use rusqlite::{Connection, OpenFlags};
use std::{
    collections::{BTreeMap, HashMap, HashSet},
    path::Path,
    sync::Arc,
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
    if path.starts_with('/') {
        push_parts(path);
    } else {
        push_parts(base);
        push_parts(path);
    }
    if segments.is_empty() {
        "/".to_string()
    } else {
        format!("/{}", segments.join("/"))
    }
}

/// Read-only client for the Jefferson Lab Calibration and Conditions Database.
#[derive(Clone)]
pub struct CCDB {
    connection: Arc<Mutex<Connection>>,
    connection_path: String,
    variation_cache: Arc<DashMap<String, VariationMeta>>,
    variation_chain_cache: Arc<DashMap<Id, Vec<VariationMeta>>>,
    directory_meta: Arc<DashMap<Id, DirectoryMeta>>,
    directory_by_path: Arc<DashMap<String, Id>>,
    table_meta: Arc<DashMap<Id, TypeTableMeta>>,
    table_by_dir_name: Arc<DashMap<(Id, String), Id>>,
    column_layouts: Arc<DashMap<Id, Arc<ColumnLayout>>>,
}

impl CCDB {
    /// Opens a read-only connection to an existing CCDB `SQLite` database file.
    ///
    /// # Errors
    ///
    /// This method returns an error if the database cannot be opened.
    pub fn open(path: impl AsRef<Path>) -> CCDBResult<Self> {
        let path_str = path.as_ref().to_string_lossy().to_string();
        let conn = Connection::open_with_flags(&path, OpenFlags::SQLITE_OPEN_READ_ONLY)?;
        conn.pragma_update(None, "foreign_keys", "ON")?; // TODO: check
        let db = CCDB {
            connection: Arc::new(Mutex::new(conn)),
            variation_cache: Arc::new(DashMap::new()),
            variation_chain_cache: Arc::new(DashMap::new()),
            directory_meta: Arc::new(DashMap::new()),
            directory_by_path: Arc::new(DashMap::new()),
            table_meta: Arc::new(DashMap::new()),
            table_by_dir_name: Arc::new(DashMap::new()),
            column_layouts: Arc::new(DashMap::new()),
            connection_path: path_str,
        };
        db.load_directories()?;
        db.load_tables()?;
        Ok(db)
    }
    /// Returns the underlying [`rusqlite::Connection`].
    pub fn connection(&self) -> MutexGuard<'_, Connection> {
        self.connection.lock()
    }
    /// Returns the filesystem path used to open the database.
    #[must_use]
    pub fn connection_path(&self) -> &str {
        &self.connection_path
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

        while current.parent_id > 0 {
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
                break;
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

    /// Fetches data for a table path using the supplied [`Context`].
    /// # Errors
    ///
    /// This method returns an error if the parsed table path
    /// does not exist or an error occurs while fetching data.
    pub fn fetch(&self, path: &str, ctx: &Context) -> CCDBResult<BTreeMap<RunNumber, Data>> {
        let table = self.table(path)?;
        table.fetch(ctx)
    }
}

/// Handle to a CCDB directory, allowing navigation and table discovery.
#[derive(Clone)]
pub struct DirectoryHandle {
    db: CCDB,
    pub(crate) meta: DirectoryMeta,
}

impl DirectoryHandle {
    /// Returns the directory metadata as loaded from CCDB.
    #[must_use]
    pub fn meta(&self) -> &DirectoryMeta {
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
                    names.push(current.name.clone());
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
            Some(DirectoryHandle {
                db: self.db.clone(),
                meta: self.db.directory_meta.get(&self.meta.parent_id)?.clone(),
            })
        }
    }
    /// Lists subdirectories directly under this directory.
    #[must_use]
    pub fn dirs(&self) -> Vec<DirectoryHandle> {
        self.db
            .directory_meta
            .iter()
            .filter(|meta| meta.parent_id == self.meta.id)
            .map(|meta| DirectoryHandle {
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
    pub fn dir(&self, path: &str) -> CCDBResult<DirectoryHandle> {
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
#[derive(Clone)]
pub struct TypeTableHandle {
    db: CCDB,
    pub(crate) meta: TypeTableMeta,
}
impl TypeTableHandle {
    /// Returns the table metadata as loaded from CCDB.
    #[must_use]
    pub fn meta(&self) -> &TypeTableMeta {
        &self.meta
    }
    /// Returns the table name (without parent path components).
    #[must_use]
    pub fn name(&self) -> &str {
        &self.meta.name
    }
    /// Returns the unique numeric identifier for this table.
    #[must_use]
    pub fn id(&self) -> Id {
        self.meta.id
    }
    /// Returns the absolute path of this table, including directory prefix.
    #[must_use]
    pub fn full_path(&self) -> String {
        let dir_meta = self.db.directory_meta.get(&self.meta.directory_id);
        if let Some(dir_meta) = dir_meta {
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
        } else {
            format!("/{}", self.meta.name)
        }
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
                    column_type: ColumnType::type_from_str(&row.get::<_, String>(5)?)
                        .unwrap_or_default(),
                    order: row.get(6)?,
                    comment: row.get(7).unwrap_or_default(),
                })
            })?
            .collect::<Result<Vec<ColumnMeta>, _>>()?;
        Ok(columns)
    }

    fn column_layout(&self) -> CCDBResult<Arc<ColumnLayout>> {
        if let Some(existing) = self.db.column_layouts.get(&self.meta.id) {
            return Ok(existing.clone());
        }
        let columns = self.load_column_metadata()?;
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
    pub fn fetch(&self, ctx: &Context) -> CCDBResult<BTreeMap<RunNumber, Data>> {
        let runs: Vec<RunNumber> = if ctx.runs.is_empty() {
            vec![0]
        } else {
            ctx.runs.clone() // PERF: is this ever expensive?
        };
        let assignments = self.resolve_assignments(&runs, &ctx.variation, ctx.timestamp)?;
        if assignments.is_empty() {
            return Ok(BTreeMap::new());
        }
        self.load_vaults(&assignments)
    }
    fn resolve_assignments(
        &self,
        runs: &[RunNumber],
        variation: &str,
        timestamp: DateTime<Utc>,
    ) -> CCDBResult<BTreeMap<RunNumber, Arc<ConstantSetMeta>>> {
        if runs.is_empty() {
            return Ok(BTreeMap::new());
        }
        let min_run = *runs.iter().min().expect("this is a bug, please report it!");
        let max_run = *runs.iter().max().expect("this is a bug, please report it!");
        let start_var_meta = self.db.variation(variation)?;
        let var_chain = self.db.variation_chain(&start_var_meta)?;
        let mut final_assignments: BTreeMap<RunNumber, Arc<ConstantSetMeta>> = BTreeMap::new();
        let mut unresolved: HashSet<RunNumber> = runs.iter().copied().collect();
        for var_meta in var_chain {
            if unresolved.is_empty() {
                break;
            }
            let partial = self.resolve_assignments_for_variation(
                &unresolved,
                &var_meta,
                timestamp,
                min_run,
                max_run,
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
    ) -> CCDBResult<BTreeMap<RunNumber, Arc<ConstantSetMeta>>> {
        let connection = self.db.connection();
        let mut stmt = connection.prepare_cached(
            "SELECT
                 a.id, a.created, a.constantSetId,
                 cs.id, cs.created, cs.modified, cs.vault, cs.constantTypeId,
                 rr.runMin, rr.runMax
             FROM assignments a
             JOIN constantSets cs ON cs.id = a.constantSetId
             JOIN runRanges rr ON rr.id = a.runRangeId
             WHERE cs.constantTypeId = ?
               AND a.created <= datetime(?, 'unixepoch', 'localtime')
               AND a.variationId = ?
               AND rr.runMax >= ?
               AND rr.runMin <= ?",
        )?;
        let valid_assignments = stmt
            .query_map(
                (
                    self.meta.id,
                    timestamp.timestamp(),
                    var_meta.id,
                    min_run,
                    max_run,
                ),
                |row| {
                    let meta = AssignmentMetaLite {
                        id: row.get(0)?,
                        created: row.get(1)?,
                        constant_set_id: row.get(2)?,
                    };
                    let constant_set = ConstantSetMeta {
                        id: row.get(3)?,
                        created: row.get(4)?,
                        modified: row.get(5)?,
                        vault: row.get(6)?,
                        constant_type_id: row.get(7)?,
                    };
                    let run_min: RunNumber = row.get(8)?;
                    let run_max: RunNumber = row.get(9)?;
                    Ok((meta, constant_set, run_min, run_max))
                },
            )?
            .collect::<Result<Vec<(AssignmentMetaLite, ConstantSetMeta, RunNumber, RunNumber)>, _>>(
            )?;
        let mut best: BTreeMap<RunNumber, Arc<ConstantSetMeta>> = BTreeMap::new();
        let mut best_created: HashMap<RunNumber, DateTime<Utc>> = HashMap::new(); // timestamp map
        let mut constant_set_cache: HashMap<Id, Arc<ConstantSetMeta>> = HashMap::new();
        for &run in runs {
            for (meta, constant_set, rmin, rmax) in &valid_assignments {
                if run >= *rmin && run <= *rmax {
                    let cur_best = best_created.get(&run);
                    let created = meta.created()?;
                    if cur_best.is_none_or(|t| created > *t) {
                        let cs_entry = constant_set_cache
                            .entry(constant_set.id)
                            .or_insert_with(|| Arc::new(constant_set.clone()))
                            .clone();
                        best.insert(run, cs_entry);
                        best_created.insert(run, created);
                    }
                }
            }
        }
        Ok(best)
    }
    fn load_vaults(
        &self,
        assignments: &BTreeMap<RunNumber, Arc<ConstantSetMeta>>,
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
                Ok((
                    *run,
                    Data::from_vault(&constant_set.vault, layout.clone(), n_rows)?,
                ))
            })
            .collect::<CCDBResult<BTreeMap<RunNumber, Data>>>()
    }
}
