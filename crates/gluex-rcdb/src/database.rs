use std::{
    collections::{BTreeMap, HashMap, HashSet},
    path::Path,
    sync::Arc,
};

use gluex_core::{parsers::parse_timestamp, Id, RunNumber};
use parking_lot::RwLock;
use rusqlite::types::Value as SqlValue;
use rusqlite::{params_from_iter, Connection, OpenFlags, ToSql};

use crate::{
    context::{Context, RunSelection},
    data::Value,
    models::{ConditionTypeMeta, ValueType},
    RCDBError, RCDBResult,
};

/// Primary entry point for interacting with an RCDB SQLite file.
#[derive(Clone)]
pub struct RCDB {
    connection: Arc<Connection>,
    connection_path: String,
    condition_types: Arc<RwLock<HashMap<String, ConditionTypeMeta>>>,
}

impl RCDB {
    /// Opens a read-only handle to the supplied RCDB SQLite database file.
    pub fn open(path: impl AsRef<Path>) -> RCDBResult<Self> {
        let path_str = path.as_ref().to_string_lossy().to_string();
        let connection = Connection::open_with_flags(
            path,
            OpenFlags::SQLITE_OPEN_READ_ONLY | OpenFlags::SQLITE_OPEN_NO_MUTEX,
        )?;
        connection.pragma_update(None, "foreign_keys", "ON")?;
        ensure_schema_version(&connection)?;
        let db = Self {
            connection: Arc::new(connection),
            connection_path: path_str,
            condition_types: Arc::new(RwLock::new(HashMap::new())),
        };
        db.load_condition_types()?;
        Ok(db)
    }

    /// Returns the filesystem path used to open this connection.
    pub fn connection_path(&self) -> &str {
        &self.connection_path
    }

    /// Returns the underlying SQLite connection.
    pub fn connection(&self) -> &Connection {
        &self.connection
    }

    /// Reloads the `condition_types` table into memory.
    pub fn load_condition_types(&self) -> RCDBResult<()> {
        let mut stmt = self
            .connection
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

        let mut entries: Vec<ConditionQueryEntry> = Vec::new();
        let mut index_by_name: HashMap<String, usize> = HashMap::new();
        for name in &requested {
            self.ensure_query_entry(name, true, &mut entries, &mut index_by_name)?;
        }
        let mut predicate_refs: HashSet<String> = HashSet::new();
        for expr in context.filters() {
            let mut refs = Vec::new();
            expr.referenced_conditions(&mut refs);
            for name in refs {
                predicate_refs.insert(name);
            }
        }
        for name in predicate_refs {
            self.ensure_query_entry(&name, false, &mut entries, &mut index_by_name)?;
        }
        let mut sql = String::from("SELECT runs.number");
        let mut selected_columns: Vec<SelectedColumn> = Vec::new();
        for entry in &entries {
            if entry.select {
                let column = entry.meta.value_type().column_name();
                sql.push_str(&format!(", {}.{}", entry.alias, column));
                selected_columns.push(SelectedColumn {
                    name: entry.name.clone(),
                    value_type: entry.meta.value_type(),
                });
            }
        }
        sql.push_str(" FROM runs ");
        for entry in &entries {
            sql.push_str(&format!(
                "LEFT JOIN conditions AS {alias} ON {alias}.run_number = runs.number AND {alias}.condition_type_id = {type_id} ",
                alias = entry.alias,
                type_id = entry.meta.id(),
            ));
        }
        let mut params: Vec<SqlValue> = Vec::new();
        let mut where_clauses: Vec<String> = Vec::new();
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

        match context.selection() {
            RunSelection::All => {}
            RunSelection::Range { start, end } => {
                where_clauses.push("runs.number BETWEEN ? AND ?".to_string());
                params.push(SqlValue::Integer(*start));
                params.push(SqlValue::Integer(*end));
            }
            RunSelection::Runs(runs) => {
                if runs.is_empty() {
                    return Ok(BTreeMap::new());
                }
                let mut placeholders = Vec::new();
                for run in runs {
                    placeholders.push("?");
                    params.push(SqlValue::Integer(*run));
                }
                where_clauses.push(format!("runs.number IN ({})", placeholders.join(", ")));
            }
        }
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

        let mut stmt = self.connection.prepare(&sql)?;
        let mut rows = if params.is_empty() {
            stmt.query([])?
        } else {
            let param_refs: Vec<&dyn ToSql> = params.iter().map(|v| v as &dyn ToSql).collect();
            stmt.query(params_from_iter(param_refs))?
        };

        let mut results: BTreeMap<RunNumber, HashMap<String, Value>> = BTreeMap::new();
        while let Some(row) = rows.next()? {
            let run_number: RunNumber = row.get(0)?;
            let entry = results.entry(run_number).or_default();
            for (offset, column) in selected_columns.iter().enumerate() {
                let column_index = offset + 1;
                match column.value_type {
                    ValueType::String | ValueType::Json | ValueType::Blob => {
                        let value: Option<String> = row.get(column_index)?;
                        if let Some(text) = value {
                            entry.insert(
                                column.name.clone(),
                                Value::text(column.value_type, Some(text)),
                            );
                        }
                    }
                    ValueType::Int => {
                        let value: Option<i64> = row.get(column_index)?;
                        if let Some(v) = value {
                            entry.insert(column.name.clone(), Value::int(v));
                        }
                    }
                    ValueType::Float => {
                        let value: Option<f64> = row.get(column_index)?;
                        if let Some(v) = value {
                            entry.insert(column.name.clone(), Value::float(v));
                        }
                    }
                    ValueType::Bool => {
                        let value: Option<i64> = row.get(column_index)?;
                        if let Some(v) = value {
                            entry.insert(column.name.clone(), Value::bool(v != 0));
                        }
                    }
                    ValueType::Time => {
                        let value: Option<String> = row.get(column_index)?;
                        if let Some(raw) = value {
                            let parsed = parse_timestamp(&raw)?;
                            entry.insert(column.name.clone(), Value::time(parsed));
                        }
                    }
                }
            }
        }
        Ok(results)
    }

    /// Returns the runs that satisfy the context filters (without loading condition values).
    pub fn fetch_runs(&self, context: &Context) -> RCDBResult<Vec<RunNumber>> {
        if matches!(context.selection(), RunSelection::Runs(runs) if runs.is_empty()) {
            return Ok(Vec::new());
        }

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
            self.ensure_query_entry(&name, false, &mut entries, &mut index_by_name)?;
        }

        let mut sql = String::from("SELECT DISTINCT runs.number FROM runs ");
        for entry in &entries {
            sql.push_str(&format!(
                "LEFT JOIN conditions AS {alias} ON {alias}.run_number = runs.number AND {alias}.condition_type_id = {type_id} ",
                alias = entry.alias,
                type_id = entry.meta.id(),
            ));
        }

        let mut params: Vec<SqlValue> = Vec::new();
        let mut where_clauses: Vec<String> = Vec::new();
        match context.selection() {
            RunSelection::All => {}
            RunSelection::Range { start, end } => {
                where_clauses.push("runs.number BETWEEN ? AND ?".to_string());
                params.push(SqlValue::Integer(*start));
                params.push(SqlValue::Integer(*end));
            }
            RunSelection::Runs(runs) => {
                if runs.is_empty() {
                    return Ok(Vec::new());
                }
                let mut placeholders = Vec::new();
                for run in runs {
                    placeholders.push("?");
                    params.push(SqlValue::Integer(*run));
                }
                where_clauses.push(format!("runs.number IN ({})", placeholders.join(", ")));
            }
        }

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

        let mut stmt = self.connection.prepare(&sql)?;
        let mut rows = if params.is_empty() {
            stmt.query([])?
        } else {
            let param_refs: Vec<&dyn ToSql> = params.iter().map(|v| v as &dyn ToSql).collect();
            stmt.query(params_from_iter(param_refs))?
        };

        let mut runs = Vec::new();
        while let Some(row) = rows.next()? {
            runs.push(row.get(0)?);
        }
        Ok(runs)
    }

    fn ensure_query_entry(
        &self,
        name: &str,
        select: bool,
        entries: &mut Vec<ConditionQueryEntry>,
        index_by_name: &mut HashMap<String, usize>,
    ) -> RCDBResult<()> {
        if let Some(&idx) = index_by_name.get(name) {
            if select {
                entries[idx].select = true;
            }
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
            select,
        });
        index_by_name.insert(name.to_string(), entries.len() - 1);
        Ok(())
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

struct ConditionQueryEntry {
    name: String,
    meta: ConditionTypeMeta,
    alias: String,
    select: bool,
}

struct SelectedColumn {
    name: String,
    value_type: ValueType,
}

struct AliasInfo {
    alias: String,
    value_type: ValueType,
}
