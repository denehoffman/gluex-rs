//! Parameterized, backend-enforced read-only row retrieval.

use rusqlite::{
    Connection, params_from_iter,
    types::{Value, ValueRef},
};

/// A database-native scalar used for parameters and immutable raw results.
#[derive(Debug, Clone, PartialEq)]
pub enum RawValue {
    /// SQL NULL.
    Null,
    /// Signed SQLite integer.
    Integer(i64),
    /// SQLite floating-point value.
    Real(f64),
    /// UTF-8 text.
    Text(String),
    /// Arbitrary bytes.
    Blob(Vec<u8>),
}

impl From<&RawValue> for Value {
    fn from(value: &RawValue) -> Self {
        match value {
            RawValue::Null => Self::Null,
            RawValue::Integer(v) => Self::Integer(*v),
            RawValue::Real(v) => Self::Real(*v),
            RawValue::Text(v) => Self::Text(v.clone()),
            RawValue::Blob(v) => Self::Blob(v.clone()),
        }
    }
}

/// Column metadata in result order. Names may repeat for arbitrary SQL.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct RawColumn {
    name: String,
    declared_type: Option<String>,
}
impl RawColumn {
    /// Result column name, including any SQL alias.
    #[must_use]
    pub fn name(&self) -> &str {
        &self.name
    }
    /// SQLite declared type, if available; expressions may have no declared type.
    #[must_use]
    pub fn declared_type(&self) -> Option<&str> {
        self.declared_type.as_deref()
    }
}

/// Immutable raw row with values aligned to result columns.
#[derive(Debug, Clone, PartialEq)]
pub struct RawRow(Vec<RawValue>);
impl RawRow {
    /// Values in result column order.
    #[must_use]
    pub fn values(&self) -> &[RawValue] {
        &self.0
    }
}

/// Materialized immutable raw rows and their column metadata.
#[derive(Debug, Clone)]
pub struct RawResults {
    columns: Vec<RawColumn>,
    rows: Vec<RawRow>,
}
impl RawResults {
    /// Metadata in result column order, including for empty results.
    #[must_use]
    pub fn columns(&self) -> &[RawColumn] {
        &self.columns
    }
    /// Rows in SQL result order.
    #[must_use]
    pub fn rows(&self) -> &[RawRow] {
        &self.rows
    }
}

/// Failure preparing, authorizing, binding or decoding a raw query.
#[derive(Debug, thiserror::Error)]
pub enum RawError {
    /// SQLite preparation, authorization, binding, or decoding failed.
    #[error("read-only query failed: {0}")]
    Sqlite(#[from] rusqlite::Error),
    /// Execution was cancelled, interrupted, or timed out.
    #[error(transparent)]
    Execution(#[from] crate::ExecutionError),
}

pub(crate) fn query(
    connection: &Connection,
    sql: &str,
    parameters: &[RawValue],
) -> Result<RawResults, RawError> {
    query_with_options(
        connection,
        sql,
        parameters,
        &crate::ExecutionOptions::default(),
    )
}

pub(crate) fn query_with_options(
    connection: &Connection,
    sql: &str,
    parameters: &[RawValue],
    options: &crate::ExecutionOptions,
) -> Result<RawResults, RawError> {
    crate::execution::with_sqlite_progress(
        connection,
        options,
        || -> Result<RawResults, RawError> {
            let mut statement = connection.prepare(sql)?;
            if !statement.readonly() || statement.column_count() == 0 {
                return Err(rusqlite::Error::InvalidQuery.into());
            }
            let columns = statement
                .columns()
                .iter()
                .map(|column| RawColumn {
                    name: column.name().to_owned(),
                    declared_type: column.decl_type().map(str::to_owned),
                })
                .collect::<Vec<_>>();
            let mut cursor =
                statement.query(params_from_iter(parameters.iter().map(Value::from)))?;
            let mut rows = Vec::new();
            while let Some(row) = cursor.next()? {
                let values = (0..columns.len())
                    .map(|index| {
                        Ok(match row.get_ref(index)? {
                            ValueRef::Null => RawValue::Null,
                            ValueRef::Integer(value) => RawValue::Integer(value),
                            ValueRef::Real(value) => RawValue::Real(value),
                            ValueRef::Text(value) => RawValue::Text(
                                std::str::from_utf8(value)
                                    .map_err(|error| {
                                        rusqlite::Error::FromSqlConversionFailure(
                                            index,
                                            rusqlite::types::Type::Text,
                                            Box::new(error),
                                        )
                                    })?
                                    .to_owned(),
                            ),
                            ValueRef::Blob(value) => RawValue::Blob(value.to_vec()),
                        })
                    })
                    .collect::<Result<Vec<_>, rusqlite::Error>>()?;
                rows.push(RawRow(values));
            }
            Ok(RawResults { columns, rows })
        },
    )
}

// Installed once for the lifetime of each reader, including statement preparation
// and execution. No public connection handle can remove or replace this guard.
pub(crate) fn restrict(connection: &Connection) -> rusqlite::Result<()> {
    use rusqlite::hooks::{AuthAction, AuthContext, Authorization};
    connection.pragma_update(None, "query_only", true)?;
    connection.authorizer(Some(|context: AuthContext<'_>| {
        let allowed = match context.action {
            AuthAction::Select | AuthAction::Read { .. } | AuthAction::Recursive => true,
            AuthAction::Function { function_name } => !matches!(
                function_name.to_ascii_lowercase().as_str(),
                "load_extension" | "writefile" | "readfile"
            ),
            AuthAction::Pragma {
                pragma_name,
                pragma_value,
            } => match pragma_name.to_ascii_lowercase().as_str() {
                "table_info" | "table_xinfo" | "index_list" | "index_info" | "index_xinfo"
                | "foreign_key_list" => true,
                "query_only" | "schema_version" | "user_version" | "database_list"
                | "table_list" | "compile_options" => pragma_value.is_none(),
                _ => false,
            },
            _ => false,
        };
        if allowed {
            Authorization::Allow
        } else {
            Authorization::Deny
        }
    }))?;
    Ok(())
}
