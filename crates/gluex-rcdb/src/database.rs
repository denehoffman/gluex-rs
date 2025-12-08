use std::{
    collections::{BTreeMap, HashMap},
    path::Path,
    sync::Arc,
};

use gluex_core::{parsers::parse_timestamp, Id, RunNumber};
use parking_lot::RwLock;
use rusqlite::{params, Connection, OpenFlags, Row};

use crate::{
    context::{Context, RunSelection},
    data::Value,
    models::{ConditionTypeMeta, ValueType},
    RCDBError, RCDBResult,
};

const BASE_SELECT: &str =
    "SELECT run_number, text_value, int_value, float_value, bool_value, time_value FROM conditions";

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
    fn load_condition_types(&self) -> RCDBResult<()> {
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

    /// Fetches condition values for the supplied condition name and context.
    pub fn fetch(
        &self,
        condition_name: &str,
        context: &Context,
    ) -> RCDBResult<BTreeMap<RunNumber, Value>> {
        let Some(meta) = self.condition_type(condition_name) else {
            return Err(RCDBError::ConditionTypeNotFound(condition_name.to_string()));
        };
        match context.selection() {
            RunSelection::All => {
                let sql = format!(
                    "{} WHERE condition_type_id = ?1 ORDER BY run_number",
                    BASE_SELECT
                );
                self.query_conditions(&sql, params![meta.id()], &meta)
            }
            RunSelection::Range { start, end } => {
                let sql = format!(
                    "{} WHERE condition_type_id = ?1 AND run_number BETWEEN ?2 AND ?3 ORDER BY run_number",
                    BASE_SELECT
                );
                self.query_conditions(&sql, params![meta.id(), *start, *end], &meta)
            }
            RunSelection::Runs(runs) => {
                if runs.is_empty() {
                    return Ok(BTreeMap::new());
                }
                let run_list = runs
                    .iter()
                    .map(|run| run.to_string())
                    .collect::<Vec<_>>()
                    .join(", ");
                let sql = format!(
                    "{} WHERE condition_type_id = ?1 AND run_number IN ({}) ORDER BY run_number",
                    BASE_SELECT, run_list
                );
                self.query_conditions(&sql, params![meta.id()], &meta)
            }
        }
    }

    fn query_conditions<P>(
        &self,
        sql: &str,
        params: P,
        cond_type: &ConditionTypeMeta,
    ) -> RCDBResult<BTreeMap<RunNumber, Value>>
    where
        P: rusqlite::Params,
    {
        let mut stmt = self.connection.prepare(sql)?;
        let mut rows = stmt.query(params)?;
        let mut map = BTreeMap::new();
        while let Some(row) = rows.next()? {
            let (run, value) = read_condition_row(&row, cond_type)?;
            map.insert(run, value);
        }
        Ok(map)
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

fn read_condition_row(
    row: &Row<'_>,
    cond_type: &ConditionTypeMeta,
) -> RCDBResult<(RunNumber, Value)> {
    let run_number: RunNumber = row.get(0)?;
    let value = decode_value(row, cond_type, run_number)?;
    Ok((run_number, value))
}

fn decode_value(
    row: &Row<'_>,
    cond_type: &ConditionTypeMeta,
    run_number: RunNumber,
) -> RCDBResult<Value> {
    match cond_type.value_type() {
        ValueType::String | ValueType::Json | ValueType::Blob => {
            let text: Option<String> = row.get(1)?;
            Ok(Value::text(cond_type.value_type(), text))
        }
        ValueType::Int => {
            let value: i64 = row.get(2)?;
            Ok(Value::int(value))
        }
        ValueType::Float => {
            let value: f64 = row.get(3)?;
            Ok(Value::float(value))
        }
        ValueType::Bool => {
            let value: i64 = row.get(4)?;
            Ok(Value::bool(value != 0))
        }
        ValueType::Time => {
            let raw: Option<String> = row.get(5)?;
            let Some(raw) = raw else {
                return Err(RCDBError::MissingTimeValue {
                    condition_name: cond_type.name().to_string(),
                    run_number,
                });
            };
            let parsed = parse_timestamp(&raw)?;
            Ok(Value::time(parsed))
        }
    }
}
