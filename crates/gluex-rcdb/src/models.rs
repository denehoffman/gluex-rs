use chrono::{DateTime, Utc};
use gluex_core::{errors::ParseTimestampError, parsers::parse_timestamp, Id, RunNumber};

/// Typed representation of a condition value column.
#[derive(Debug, Copy, Clone, Default, PartialEq, Eq)]
pub enum ValueType {
    /// Human readable UTF-8 string payload.
    #[default]
    String,
    /// Signed integer payload stored in `int_value` (i32).
    Int,
    /// Boolean payload stored in `bool_value`.
    Bool,
    /// Floating point payload stored in `float_value`.
    Float,
    /// JSON encoded blob stored in `text_value` (f64).
    Json,
    /// Arbitrary blob (stored as text) stored in `text_value`.
    Blob,
    /// Timestamp payload stored in `time_value`.
    Time,
}
impl ValueType {
    /// Returns the identifier string stored in the database.
    #[must_use]
    pub fn as_str(&self) -> &'static str {
        match self {
            ValueType::String => "string",
            ValueType::Int => "int",
            ValueType::Bool => "bool",
            ValueType::Float => "float",
            ValueType::Json => "json",
            ValueType::Blob => "blob",
            ValueType::Time => "time",
        }
    }

    /// Builds a [`ValueType`] from the identifier stored in `SQLite`.
    #[must_use]
    pub fn from_identifier(value: &str) -> Option<Self> {
        match value {
            "string" => Some(ValueType::String),
            "int" => Some(ValueType::Int),
            "bool" => Some(ValueType::Bool),
            "float" => Some(ValueType::Float),
            "json" => Some(ValueType::Json),
            "blob" => Some(ValueType::Blob),
            "time" => Some(ValueType::Time),
            _ => None,
        }
    }

    /// True when the value is backed by the `text_value` column.
    #[must_use]
    pub fn is_textual(&self) -> bool {
        matches!(self, ValueType::String | ValueType::Json | ValueType::Blob)
    }

    /// Returns the storage column name used in the `conditions` table.
    #[must_use]
    pub fn column_name(&self) -> &'static str {
        match self {
            ValueType::String | ValueType::Json | ValueType::Blob => "text_value",
            ValueType::Int => "int_value",
            ValueType::Bool => "bool_value",
            ValueType::Float => "float_value",
            ValueType::Time => "time_value",
        }
    }
}
/// Metadata record for a condition type entry.
#[derive(Debug, Clone)]
pub struct ConditionTypeMeta {
    pub(crate) id: Id,
    pub(crate) name: String,
    pub(crate) value_type: ValueType,
    pub(crate) created: String,
    pub(crate) description: String,
}
impl ConditionTypeMeta {
    /// Database identifier for the condition type.
    #[must_use]
    pub fn id(&self) -> Id {
        self.id
    }
    /// Name of the condition type.
    #[must_use]
    pub fn name(&self) -> &str {
        &self.name
    }
    /// [`ValueType`] used to store the condition.
    #[must_use]
    pub fn value_type(&self) -> ValueType {
        self.value_type
    }
    /// Timestamp describing when the condition was created.
    #[must_use]
    pub fn created(&self) -> String {
        self.created.clone()
    }
    /// Free-form description associated with the condition.
    #[must_use]
    pub fn description(&self) -> &str {
        &self.description
    }
}

/// Raw metadata row for an individual condition value.
pub struct ConditionMeta {
    pub(crate) id: Id,
    pub(crate) text_value: String,
    pub(crate) int_value: i64,
    pub(crate) float_value: f64,
    pub(crate) bool_value: i64,
    pub(crate) run_number: RunNumber,
    pub(crate) condition_type_id: Id,
    pub(crate) created: String,
    pub(crate) time_value: String,
}
impl ConditionMeta {
    /// Identifier of the condition row.
    #[must_use]
    pub fn id(&self) -> Id {
        self.id
    }
    /// Raw text value stored alongside the condition.
    #[must_use]
    pub fn text_value(&self) -> &str {
        &self.text_value
    }
    /// Raw integer value stored alongside the condition.
    #[must_use]
    pub fn int_value(&self) -> i64 {
        self.int_value
    }
    /// Raw floating-point value stored alongside the condition.
    #[must_use]
    pub fn float_value(&self) -> f64 {
        self.float_value
    }
    /// Raw boolean stored as an integer (0 or 1).
    #[must_use]
    pub fn bool_value(&self) -> i64 {
        self.bool_value
    }
    /// Run number associated with the condition.
    #[must_use]
    pub fn run_number(&self) -> i64 {
        self.run_number
    }
    /// Identifier referencing the condition type entry.
    #[must_use]
    pub fn condition_type_id(&self) -> Id {
        self.condition_type_id
    }
    /// Timestamp describing when the condition was created.
    ///
    /// # Errors
    ///
    /// Returns an error if the stored creation timestamp cannot be parsed as a UTC datetime.
    pub fn created(&self) -> Result<DateTime<Utc>, ParseTimestampError> {
        parse_timestamp(&self.created)
    }
    /// Optional timestamp value associated with the condition.
    ///
    /// # Errors
    ///
    /// Returns an error if the stored timestamp cannot be parsed as a UTC datetime.
    pub fn time_value(&self) -> Result<DateTime<Utc>, ParseTimestampError> {
        parse_timestamp(&self.time_value)
    }
}

/// Metadata describing a named RCDB run period.
pub struct RunPeriodMeta {
    pub(crate) id: Id,
    pub(crate) name: String,
    pub(crate) description: String,
    pub(crate) run_min: RunNumber,
    pub(crate) run_max: RunNumber,
    pub(crate) start_date: String,
    pub(crate) end_date: String,
}
impl RunPeriodMeta {
    /// Identifier of the run period.
    #[must_use]
    pub fn id(&self) -> Id {
        self.id
    }
    /// Human-readable period name.
    #[must_use]
    pub fn name(&self) -> &str {
        &self.name
    }
    /// Optional descriptive text for the run period.
    #[must_use]
    pub fn description(&self) -> &str {
        &self.description
    }
    /// Minimum run number included in the period.
    #[must_use]
    pub fn run_min(&self) -> RunNumber {
        self.run_min
    }
    /// Maximum run number included in the period.
    #[must_use]
    pub fn run_max(&self) -> RunNumber {
        self.run_max
    }
    /// Timestamp describing when the period started.
    ///
    /// # Errors
    ///
    /// Returns an error if the stored start timestamp cannot be parsed as a UTC datetime.
    pub fn start_date(&self) -> Result<DateTime<Utc>, ParseTimestampError> {
        parse_timestamp(&self.start_date)
    }
    /// Timestamp describing when the period ended.
    ///
    /// # Errors
    ///
    /// Returns an error if the stored end timestamp cannot be parsed as a UTC datetime.
    pub fn end_date(&self) -> Result<DateTime<Utc>, ParseTimestampError> {
        parse_timestamp(&self.end_date)
    }
}

/// Metadata describing a single run record.
pub struct RunMeta {
    pub(crate) number: RunNumber,
    pub(crate) started: String,
    pub(crate) finished: String,
}
impl RunMeta {
    /// Run number for this record.
    #[must_use]
    pub fn number(&self) -> RunNumber {
        self.number
    }
    /// Timestamp indicating when the run began.
    ///
    /// # Errors
    ///
    /// Returns an error if the stored run start timestamp cannot be parsed as a UTC datetime.
    pub fn started(&self) -> Result<DateTime<Utc>, ParseTimestampError> {
        parse_timestamp(&self.started)
    }
    /// Timestamp indicating when the run finished.
    ///
    /// # Errors
    ///
    /// Returns an error if the stored run end timestamp cannot be parsed as a UTC datetime.
    pub fn finished(&self) -> Result<DateTime<Utc>, ParseTimestampError> {
        parse_timestamp(&self.finished)
    }
}
