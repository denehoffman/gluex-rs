use chrono::{DateTime, Utc};

use crate::models::ValueType;

#[derive(Debug, Clone)]
enum Repr {
    Text(String),
    Int(i64),
    Float(f64),
    Bool(bool),
    Time(DateTime<Utc>),
}

/// Typed wrapper for an individual RCDB condition value.
#[derive(Debug, Clone)]
pub struct Value {
    value_type: ValueType,
    repr: Repr,
}

impl Value {
    fn new(value_type: ValueType, repr: Repr) -> Self {
        Self { value_type, repr }
    }

    pub(crate) fn text(value_type: ValueType, value: Option<String>) -> Self {
        Value::new(value_type, Repr::Text(value.unwrap_or_default()))
    }

    pub(crate) fn int(value: i64) -> Self {
        Value::new(ValueType::Int, Repr::Int(value))
    }

    pub(crate) fn float(value: f64) -> Self {
        Value::new(ValueType::Float, Repr::Float(value))
    }

    pub(crate) fn bool(value: bool) -> Self {
        Value::new(ValueType::Bool, Repr::Bool(value))
    }

    pub(crate) fn time(value: DateTime<Utc>) -> Self {
        Value::new(ValueType::Time, Repr::Time(value))
    }

    /// Returns the declared RCDB type of the value.
    #[must_use]
    pub fn value_type(&self) -> ValueType {
        self.value_type
    }

    /// Returns the inner string (string, json, or blob) value if available.
    #[must_use]
    pub fn as_string(&self) -> Option<&str> {
        if self.value_type.is_textual() {
            if let Repr::Text(text) = &self.repr {
                return Some(text);
            }
        }
        None
    }

    /// Returns the integer payload when the value type is `int`.
    #[must_use]
    pub fn as_int(&self) -> Option<i64> {
        match &self.repr {
            Repr::Int(value) if self.value_type == ValueType::Int => Some(*value),
            _ => None,
        }
    }

    /// Returns the floating point payload when the value type is `float`.
    #[must_use]
    pub fn as_float(&self) -> Option<f64> {
        match &self.repr {
            Repr::Float(value) if self.value_type == ValueType::Float => Some(*value),
            _ => None,
        }
    }

    /// Returns the boolean payload when the value type is `bool`.
    #[must_use]
    pub fn as_bool(&self) -> Option<bool> {
        match &self.repr {
            Repr::Bool(value) if self.value_type == ValueType::Bool => Some(*value),
            _ => None,
        }
    }

    /// Returns the timestamp payload when the value type is `time`.
    #[must_use]
    pub fn as_time(&self) -> Option<DateTime<Utc>> {
        match &self.repr {
            Repr::Time(value) if self.value_type == ValueType::Time => Some(*value),
            _ => None,
        }
    }
}
