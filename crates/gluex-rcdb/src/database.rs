use std::{
    collections::{BTreeMap, HashMap, HashSet},
    path::Path,
    sync::Arc,
};

use gluex_core::{parsers::parse_timestamp, Id, RunNumber};
use parking_lot::{Mutex, MutexGuard, RwLock};
use rusqlite::types::Value as SqlValue;
use rusqlite::{params_from_iter, Connection, OpenFlags, ToSql};

use crate::{
    context::{Context, RunSelection},
    data::Value,
    models::{ConditionTypeMeta, ValueType},
    RCDBError, RCDBResult,
};

/// Primary entry point for interacting with an RCDB `SQLite` file.
#[derive(Clone)]
pub struct RCDB {
    connection: Arc<Mutex<Connection>>,
    connection_path: String,
    condition_types: Arc<RwLock<HashMap<String, ConditionTypeMeta>>>,
    conditions_run_number_index: Option<String>,
}

impl RCDB {
    /// Opens a read-only handle to the supplied RCDB `SQLite` database file.
    ///
    /// # Errors
    ///
    /// This method returns an error if the database cannot be opened.
    pub fn open(path: impl AsRef<Path>) -> RCDBResult<Self> {
        let path_str = path.as_ref().to_string_lossy().to_string();
        let connection = Connection::open_with_flags(
            path,
            OpenFlags::SQLITE_OPEN_READ_ONLY | OpenFlags::SQLITE_OPEN_NO_MUTEX,
        )?;
        connection.pragma_update(None, "foreign_keys", "ON")?;
        ensure_schema_version(&connection)?;
        let run_number_index = lookup_conditions_run_number_index(&connection)?;
        let db = Self {
            connection: Arc::new(Mutex::new(connection)),
            connection_path: path_str,
            condition_types: Arc::new(RwLock::new(HashMap::new())),
            conditions_run_number_index: run_number_index,
        };
        db.load_condition_types()?;
        Ok(db)
    }

    /// Returns the filesystem path used to open this connection.
    #[must_use]
    pub fn connection_path(&self) -> &str {
        &self.connection_path
    }

    /// Returns the underlying [`rusqlite::Connection`].
    pub fn connection(&self) -> MutexGuard<'_, Connection> {
        self.connection.lock()
    }

    /// Reloads the `condition_types` table into memory.
    ///
    /// # Errors
    ///
    /// This method will fail if there are any problems parsing the `condition_types` table.
    pub fn load_condition_types(&self) -> RCDBResult<()> {
        let connection = self.connection();
        let mut stmt = connection
            .prepare("SELECT id, name, value_type, created, description FROM condition_types")?;
        let mut rows = stmt.query([])?;
        let mut loaded: HashMap<String, ConditionTypeMeta> = HashMap::new();
        while let Some(row) = rows.next()? {
            let id: Id = row.get(0)?;
            let name: String = row.get(1)?;
            let value_type_name: String = row.get(2)?;
            let value_type = ValueType::from_identifier(&value_type_name)
                .ok_or_else(|| RCDBError::UnknownValueType(value_type_name.clone()))?;
            let created: Option<String> = row.get(3)?;
            let description: Option<String> = row.get(4)?;
            loaded.insert(
                name.clone(),
                ConditionTypeMeta {
                    id,
                    name,
                    value_type,
                    created: created.unwrap_or_default(),
                    description: description.unwrap_or_default(),
                },
            );
        }
        *self.condition_types.write() = loaded;
        Ok(())
    }

    fn condition_type(&self, name: &str) -> Option<ConditionTypeMeta> {
        self.condition_types.read().get(name).cloned()
    }

    /// Fetches multiple condition values for the supplied names and context.
    ///
    /// # Errors
    ///
    /// This method will return an error if any of the requested conditions cannot be found, if the
    /// conditions list is empty (use [`RCDB::fetch_runs`] instead), or if the SQL query fails.
    #[allow(clippy::too_many_lines)]
    pub fn fetch<S>(
        &self,
        condition_names: S,
        context: &Context,
    ) -> RCDBResult<BTreeMap<RunNumber, HashMap<String, Value>>>
    where
        S: IntoIterator,
        S::Item: AsRef<str>,
    {
        let mut requested: Vec<String> = Vec::new();
        let mut seen: HashSet<String> = HashSet::new();
        for name in condition_names {
            let name_ref = name.as_ref();
            if seen.insert(name_ref.to_string()) {
                requested.push(name_ref.to_string());
            }
        }
        if requested.is_empty() {
            return Err(RCDBError::EmptyConditionList);
        }
        if matches!(context.selection(), RunSelection::Runs(runs) if runs.is_empty()) {
            return Ok(BTreeMap::new());
        }
        let (matched_runs_sql, mut params) = self.build_matched_runs_query(context)?;
        let mut requested_conditions: Vec<RequestedCondition> = Vec::new();
        let mut requested_index_by_id: HashMap<Id, usize> = HashMap::new();
        for name in &requested {
            let meta = self
                .condition_type(name)
                .ok_or_else(|| RCDBError::ConditionTypeNotFound(name.clone()))?;
            let idx = requested_conditions.len();
            requested_index_by_id.insert(meta.id(), idx);
            requested_conditions.push(RequestedCondition {
                name: name.clone(),
                id: meta.id(),
                value_type: meta.value_type(),
            });
        }
        let mut sql = String::from("WITH matched_runs AS (");
        sql.push_str(&matched_runs_sql);
        let index_hint = self
            .conditions_run_number_index
            .as_deref()
            .map(|name| format!("INDEXED BY {name} "))
            .unwrap_or_default();
        sql.push_str(
            ") SELECT matched_runs.number, c.condition_type_id, c.text_value, c.int_value, c.float_value, c.bool_value, c.time_value FROM matched_runs LEFT JOIN conditions AS c ",
        );
        sql.push_str(&index_hint);
        sql.push_str("ON c.run_number = matched_runs.number");
        let cond_placeholders = vec!["?"; requested_conditions.len()].join(", ");
        #[allow(clippy::format_push_string)]
        sql.push_str(&format!(
            " AND c.condition_type_id IN ({cond_placeholders})"
        ));
        for cond in &requested_conditions {
            params.push(SqlValue::Integer(cond.id));
        }
        sql.push_str(" ORDER BY matched_runs.number");
        let connection = self.connection();
        let mut stmt = connection.prepare(&sql)?;
        let mut rows = if params.is_empty() {
            stmt.query([])?
        } else {
            let param_refs: Vec<&dyn ToSql> = params.iter().map(|v| v as &dyn ToSql).collect();
            stmt.query(params_from_iter(param_refs))?
        };

        let run_filter = match context.selection() {
            RunSelection::Runs(runs) => Some(runs.iter().copied().collect::<HashSet<_>>()),
            _ => None,
        };

        let mut results: BTreeMap<RunNumber, HashMap<String, Value>> = BTreeMap::new();
        while let Some(row) = rows.next()? {
            let run_number: RunNumber = row.get(0)?;
            if let Some(filter) = &run_filter {
                if !filter.contains(&run_number) {
                    continue;
                }
            }

            let entry = results.entry(run_number).or_default();
            let cond_type_id: Option<Id> = row.get(1)?;
            let Some(cond_type_id) = cond_type_id else {
                continue;
            };
            let Some(&index) = requested_index_by_id.get(&cond_type_id) else {
                continue;
            };
            let requested = &requested_conditions[index];
            match requested.value_type {
                ValueType::String | ValueType::Json | ValueType::Blob => {
                    let value: Option<String> = row.get(2)?;
                    if let Some(text) = value {
                        entry.insert(
                            requested.name.clone(),
                            Value::text(requested.value_type, Some(text)),
                        );
                    }
                }
                ValueType::Int => {
                    let value: Option<i64> = row.get(3)?;
                    if let Some(v) = value {
                        entry.insert(requested.name.clone(), Value::int(v));
                    }
                }
                ValueType::Float => {
                    let value: Option<f64> = row.get(4)?;
                    if let Some(v) = value {
                        entry.insert(requested.name.clone(), Value::float(v));
                    }
                }
                ValueType::Bool => {
                    let value: Option<i64> = row.get(5)?;
                    if let Some(v) = value {
                        entry.insert(requested.name.clone(), Value::bool(v != 0));
                    }
                }
                ValueType::Time => {
                    let value: Option<String> = row.get(6)?;
                    if let Some(raw) = value {
                        let parsed = parse_timestamp(&raw)?;
                        entry.insert(requested.name.clone(), Value::time(parsed));
                    }
                }
            }
        }
        Ok(results)
    }

    /// Returns the runs that satisfy the context filters (without loading condition values).
    ///
    /// # Errors
    ///
    /// This method will return an error if the SQL query fails.
    pub fn fetch_runs(&self, context: &Context) -> RCDBResult<Vec<RunNumber>> {
        if matches!(context.selection(), RunSelection::Runs(runs) if runs.is_empty()) {
            return Ok(Vec::new());
        }

        let (sql, params) = self.build_matched_runs_query(context)?;

        let connection = self.connection();
        let mut stmt = connection.prepare(&sql)?;
        let mut rows = if params.is_empty() {
            stmt.query([])?
        } else {
            let param_refs: Vec<&dyn ToSql> = params.iter().map(|v| v as &dyn ToSql).collect();
            stmt.query(params_from_iter(param_refs))?
        };

        let run_filter = match context.selection() {
            RunSelection::Runs(runs) => Some(runs.iter().copied().collect::<HashSet<_>>()),
            _ => None,
        };

        let mut runs = Vec::new();
        while let Some(row) = rows.next()? {
            let run_number: RunNumber = row.get(0)?;
            if let Some(filter) = &run_filter {
                if !filter.contains(&run_number) {
                    continue;
                }
            }
            runs.push(run_number);
        }
        Ok(runs)
    }

    fn ensure_query_entry(
        &self,
        name: &str,
        entries: &mut Vec<ConditionQueryEntry>,
        index_by_name: &mut HashMap<String, usize>,
    ) -> RCDBResult<()> {
        if index_by_name.contains_key(name) {
            return Ok(());
        }
        let meta = self
            .condition_type(name)
            .ok_or_else(|| RCDBError::ConditionTypeNotFound(name.to_string()))?;
        let alias = format!("cond_{}", entries.len());
        entries.push(ConditionQueryEntry {
            name: name.to_string(),
            meta,
            alias,
        });
        index_by_name.insert(name.to_string(), entries.len() - 1);
        Ok(())
    }

    fn build_matched_runs_query(&self, context: &Context) -> RCDBResult<(String, Vec<SqlValue>)> {
        let mut entries: Vec<ConditionQueryEntry> = Vec::new();
        let mut index_by_name: HashMap<String, usize> = HashMap::new();
        let mut predicate_refs: HashSet<String> = HashSet::new();
        for expr in context.filters() {
            let mut refs = Vec::new();
            expr.referenced_conditions(&mut refs);
            for name in refs {
                predicate_refs.insert(name);
            }
        }
        for name in predicate_refs {
            self.ensure_query_entry(&name, &mut entries, &mut index_by_name)?;
        }

        let mut sql = String::from("SELECT runs.number FROM runs ");
        let join_hint = self
            .conditions_run_number_index
            .as_deref()
            .map(|name| format!("INDEXED BY {name} "))
            .unwrap_or_default();
        for entry in &entries {
            #[allow(clippy::format_push_string)]
            sql.push_str(&format!(
                "LEFT JOIN conditions AS {alias} {hint}ON {alias}.run_number = runs.number AND {alias}.condition_type_id = {type_id} ",
                alias = entry.alias,
                type_id = entry.meta.id(),
                hint = join_hint,
            ));
        }

        let mut params: Vec<SqlValue> = Vec::new();
        let mut where_clauses: Vec<String> = Vec::new();
        append_run_selection_clause(context.selection(), &mut where_clauses, &mut params);

        let alias_map: HashMap<String, AliasInfo> = entries
            .iter()
            .map(|entry| {
                (
                    entry.name.clone(),
                    AliasInfo {
                        alias: entry.alias.clone(),
                        value_type: entry.meta.value_type(),
                    },
                )
            })
            .collect();
        let alias_lookup = |name: &str| -> Option<(String, ValueType)> {
            alias_map
                .get(name)
                .map(|info| (info.alias.clone(), info.value_type))
        };

        for expr in context.filters() {
            let clause = expr.to_sql(&alias_lookup, &mut params)?;
            if clause != "1 = 1" {
                where_clauses.push(clause);
            }
        }

        if !where_clauses.is_empty() {
            sql.push_str(" WHERE ");
            sql.push_str(&where_clauses.join(" AND "));
        }

        sql.push_str(" ORDER BY runs.number");
        Ok((sql, params))
    }
}

fn ensure_schema_version(connection: &Connection) -> RCDBResult<()> {
    let mut stmt = connection.prepare("SELECT 1 FROM schema_versions WHERE version = 2 LIMIT 1")?;
    let exists = stmt.exists([])?;
    if exists {
        Ok(())
    } else {
        Err(RCDBError::MissingSchemaVersion)
    }
}

fn lookup_conditions_run_number_index(connection: &Connection) -> RCDBResult<Option<String>> {
    let mut stmt = connection.prepare("PRAGMA index_list('conditions')")?;
    let mut rows = stmt.query([])?;
    while let Some(row) = rows.next()? {
        let index_name: String = row.get(1)?;
        if index_has_column(connection, &index_name, "run_number")? {
            return Ok(Some(index_name));
        }
    }
    Ok(None)
}

fn index_has_column(
    connection: &Connection,
    index_name: &str,
    column_name: &str,
) -> RCDBResult<bool> {
    let pragma = format!("PRAGMA index_info('{index_name}')");
    let mut stmt = connection.prepare(&pragma)?;
    let mut rows = stmt.query([])?;
    while let Some(row) = rows.next()? {
        let col_name: String = row.get(2)?;
        if col_name == column_name {
            return Ok(true);
        }
    }
    Ok(false)
}

struct ConditionQueryEntry {
    name: String,
    meta: ConditionTypeMeta,
    alias: String,
}

struct AliasInfo {
    alias: String,
    value_type: ValueType,
}

struct RequestedCondition {
    name: String,
    id: Id,
    value_type: ValueType,
}

const MAX_RUN_RANGE_CLAUSES: usize = 400;

fn append_run_selection_clause(
    selection: &RunSelection,
    where_clauses: &mut Vec<String>,
    params: &mut Vec<SqlValue>,
) {
    match selection {
        RunSelection::All => {}
        RunSelection::Range { start, end } => {
            where_clauses.push("runs.number BETWEEN ? AND ?".to_string());
            params.push(SqlValue::Integer(*start));
            params.push(SqlValue::Integer(*end));
        }
        RunSelection::Runs(runs) => {
            if runs.is_empty() {
                where_clauses.push("1 = 0".to_string());
                return;
            }
            let ranges = limit_run_ranges(runs);
            let mut clauses = Vec::with_capacity(ranges.len());
            for (start, end) in ranges {
                clauses.push("runs.number BETWEEN ? AND ?".to_string());
                params.push(SqlValue::Integer(start));
                params.push(SqlValue::Integer(end));
            }
            where_clauses.push(format!("({})", clauses.join(" OR ")));
        }
    }
}

fn limit_run_ranges(runs: &[RunNumber]) -> Vec<(RunNumber, RunNumber)> {
    if runs.is_empty() {
        return Vec::new();
    }
    let mut ranges: Vec<(RunNumber, RunNumber)> = Vec::new();
    let mut start = runs[0];
    let mut end = runs[0];
    for &run in runs.iter().skip(1) {
        if run == end + 1 {
            end = run;
        } else {
            ranges.push((start, end));
            start = run;
            end = run;
        }
    }
    ranges.push((start, end));

    if ranges.len() <= MAX_RUN_RANGE_CLAUSES {
        return ranges;
    }

    let mut reduced = ranges;
    while reduced.len() > MAX_RUN_RANGE_CLAUSES {
        let mut merged: Vec<(RunNumber, RunNumber)> = Vec::with_capacity(reduced.len().div_ceil(2));
        for chunk in reduced.chunks(2) {
            let start = chunk[0].0;
            let end = chunk.last().unwrap().1;
            merged.push((start, end));
        }
        reduced = merged;
    }
    reduced
}
