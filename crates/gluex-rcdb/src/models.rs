use chrono::{DateTime, Utc};
use gluex_core::{errors::ParseTimestampError, parsers::parse_timestamp, Id, RunNumber};

/// Typed representation of a condition value column.
#[derive(Debug, Copy, Clone, Default, PartialEq, Eq)]
pub enum ValueType {
    /// Human readable UTF-8 string payload.
    #[default]
    String,
    /// Signed integer payload stored in `int_value`.
    Int,
    /// Boolean payload stored in `bool_value`.
    Bool,
    /// Floating point payload stored in `float_value`.
    Float,
    /// JSON encoded blob stored in `text_value`.
    Json,
    /// Arbitrary blob (stored as text) stored in `text_value`.
    Blob,
    /// Timestamp payload stored in `time_value`.
    Time,
}
impl ValueType {
    /// Returns the identifier string stored in the database.
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

    /// Builds a `ValueType` from the identifier stored in SQLite.
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
    pub fn is_textual(&self) -> bool {
        matches!(self, ValueType::String | ValueType::Json | ValueType::Blob)
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
    pub fn id(&self) -> Id {
        self.id
    }
    pub fn name(&self) -> &str {
        &self.name
    }
    pub fn value_type(&self) -> ValueType {
        self.value_type
    }
    pub fn created(&self) -> String {
        self.created.clone()
    }
    pub fn description(&self) -> &str {
        &self.description
    }
}

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
    pub fn id(&self) -> Id {
        self.id
    }
    pub fn text_value(&self) -> &str {
        &self.text_value
    }
    pub fn int_value(&self) -> i64 {
        self.int_value
    }
    pub fn float_value(&self) -> f64 {
        self.float_value
    }
    pub fn bool_value(&self) -> i64 {
        self.bool_value
    }
    pub fn run_number(&self) -> i64 {
        self.run_number
    }
    pub fn condition_type_id(&self) -> Id {
        self.condition_type_id
    }
    pub fn created(&self) -> Result<DateTime<Utc>, ParseTimestampError> {
        parse_timestamp(&self.created)
    }
    pub fn time_value(&self) -> Result<DateTime<Utc>, ParseTimestampError> {
        parse_timestamp(&self.time_value)
    }
}

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
    pub fn id(&self) -> Id {
        self.id
    }
    pub fn name(&self) -> &str {
        &self.name
    }
    pub fn description(&self) -> &str {
        &self.description
    }
    pub fn run_min(&self) -> RunNumber {
        self.run_min
    }
    pub fn run_max(&self) -> RunNumber {
        self.run_max
    }
    pub fn start_date(&self) -> Result<DateTime<Utc>, ParseTimestampError> {
        parse_timestamp(&self.start_date)
    }
    pub fn end_date(&self) -> Result<DateTime<Utc>, ParseTimestampError> {
        parse_timestamp(&self.end_date)
    }
}

pub struct RunMeta {
    pub(crate) number: RunNumber,
    pub(crate) started: String,
    pub(crate) finished: String,
}
impl RunMeta {
    pub fn number(&self) -> RunNumber {
        self.number
    }
    pub fn started(&self) -> Result<DateTime<Utc>, ParseTimestampError> {
        parse_timestamp(&self.started)
    }
    pub fn finished(&self) -> Result<DateTime<Utc>, ParseTimestampError> {
        parse_timestamp(&self.finished)
    }
}
