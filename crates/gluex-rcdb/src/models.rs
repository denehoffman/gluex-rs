use chrono::{DateTime, Utc};
use gluex_core::{errors::ParseTimestampError, parsers::parse_timestamp, Id, RunNumber};

#[derive(Debug, Copy, Clone, Default)]
pub enum ValueType {
    #[default]
    Text,
    Int,
    Float,
    Bool,
    Time,
}

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
