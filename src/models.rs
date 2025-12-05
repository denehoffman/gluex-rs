use std::fmt::Display;

use jiff::Timestamp;

use crate::{
    context::{parse_timestamp, ParseTimestampError},
    Id,
};

#[derive(Debug, Copy, Clone, Default)]
pub enum ColumnType {
    Int,
    UInt,
    Long,
    ULong,
    #[default]
    Double,
    String,
    Bool,
}
impl ColumnType {
    pub fn from_str(s: &str) -> Option<Self> {
        match s {
            "int" => Some(Self::Int),
            "uint" => Some(Self::UInt),
            "long" => Some(Self::Long),
            "ulong" => Some(Self::ULong),
            "double" => Some(Self::Double),
            "bool" => Some(Self::Bool),
            "string" => Some(Self::String),
            _ => None,
        }
    }

    pub fn as_str(&self) -> &'static str {
        match self {
            Self::Int => "int",
            Self::UInt => "uint",
            Self::Long => "long",
            Self::ULong => "ulong",
            Self::Double => "double",
            Self::Bool => "bool",
            Self::String => "string",
        }
    }
}
impl Display for ColumnType {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{}", self.as_str())
    }
}

#[derive(Debug, Clone, Default)]
pub struct ColumnMeta {
    pub id: Id,
    pub created: String,
    pub modified: String,
    pub name: String,
    pub type_id: Id,
    pub column_type: ColumnType,
    pub order: i64,
    pub comment: String,
}
impl ColumnMeta {
    pub fn created(&self) -> Result<Timestamp, ParseTimestampError> {
        parse_timestamp(&self.created)
    }
    pub fn modified(&self) -> Result<Timestamp, ParseTimestampError> {
        parse_timestamp(&self.modified)
    }
}

#[derive(Debug, Clone, Default)]
pub struct DirectoryMeta {
    pub id: Id,
    pub created: String,
    pub modified: String,
    pub name: String,
    pub parent_id: Id,
    pub author_id: Id,
    pub comment: String,
    pub is_deprecated: bool,
    pub deprecated_by_user_id: Id,
    pub is_locked: bool,
    pub locked_by_user_id: Id,
}
impl DirectoryMeta {
    pub fn created(&self) -> Result<Timestamp, ParseTimestampError> {
        parse_timestamp(&self.created)
    }
    pub fn modified(&self) -> Result<Timestamp, ParseTimestampError> {
        parse_timestamp(&self.modified)
    }
}

#[derive(Debug, Clone, Default)]
pub struct TypeTableMeta {
    pub id: Id,
    pub created: String,
    pub modified: String,
    pub directory_id: Id,
    pub name: String,
    pub n_rows: i64,
    pub n_columns: i64,
    pub n_assignments: i64,
    pub author_id: Id,
    pub comment: String,
    pub is_deprecated: bool,
    pub deprecated_by_user_id: Id,
    pub is_locked: bool,
    pub locked_by_user_id: Id,
    pub lock_time: String,
}

impl TypeTableMeta {
    pub fn created(&self) -> Result<Timestamp, ParseTimestampError> {
        parse_timestamp(&self.created)
    }
    pub fn modified(&self) -> Result<Timestamp, ParseTimestampError> {
        parse_timestamp(&self.modified)
    }
    pub fn lock_time(&self) -> Result<Timestamp, ParseTimestampError> {
        parse_timestamp(&self.lock_time)
    }
}

#[derive(Debug, Clone, Default)]
pub struct ConstantSetMeta {
    pub id: Id,
    pub created: String,
    pub modified: String,
    pub vault: String,
    pub constant_type_id: Id,
}

impl ConstantSetMeta {
    pub fn created(&self) -> Result<Timestamp, ParseTimestampError> {
        parse_timestamp(&self.created)
    }
    pub fn modified(&self) -> Result<Timestamp, ParseTimestampError> {
        parse_timestamp(&self.modified)
    }
}

#[derive(Debug, Clone, Default)]
pub struct AssignmentMeta {
    pub id: Id,
    pub created: String,
    pub modified: String,
    pub variation_id: Id,
    pub run_range_id: Id,
    pub event_range_id: Id,
    pub author_id: Id,
    pub comment: String,
    pub constant_set_id: Id,
}
impl AssignmentMeta {
    pub fn created(&self) -> Result<Timestamp, ParseTimestampError> {
        parse_timestamp(&self.created)
    }
    pub fn modified(&self) -> Result<Timestamp, ParseTimestampError> {
        parse_timestamp(&self.modified)
    }
}

#[derive(Debug, Clone, Default)]
pub struct VariationMeta {
    pub id: Id,
    pub created: String,
    pub modified: String,
    pub name: String,
    pub description: String,
    pub author_id: Id,
    pub comment: String,
    pub parent_id: Id,
    pub is_locked: bool,
    pub lock_time: String,
    pub locked_by_user_id: Id,
    pub go_back_behavior: i64,
    pub go_back_time: String,
    pub is_deprecated: bool,
    pub deprecated_by_user_id: Id,
}
impl VariationMeta {
    pub fn created(&self) -> Result<Timestamp, ParseTimestampError> {
        parse_timestamp(&self.created)
    }
    pub fn modified(&self) -> Result<Timestamp, ParseTimestampError> {
        parse_timestamp(&self.modified)
    }
    pub fn lock_time(&self) -> Result<Timestamp, ParseTimestampError> {
        parse_timestamp(&self.lock_time)
    }
    pub fn go_back_time(&self) -> Result<Timestamp, ParseTimestampError> {
        parse_timestamp(&self.go_back_time)
    }
}

#[derive(Debug, Clone, Default)]
pub struct RunRangeMeta {
    pub id: Id,
    pub created: String,
    pub modified: String,
    pub name: String,
    pub run_min: i64,
    pub run_max: i64,
    pub comment: String,
}

impl RunRangeMeta {
    pub fn created(&self) -> Result<Timestamp, ParseTimestampError> {
        parse_timestamp(&self.created)
    }
    pub fn modified(&self) -> Result<Timestamp, ParseTimestampError> {
        parse_timestamp(&self.modified)
    }
}

#[derive(Debug, Clone, Default)]
pub struct EventRangeMeta {
    pub id: Id,
    pub created: String,
    pub modified: String,
    pub run_number: i64,
    pub event_min: i64,
    pub event_max: i64,
    pub comment: String,
}

impl EventRangeMeta {
    pub fn created(&self) -> Result<Timestamp, ParseTimestampError> {
        parse_timestamp(&self.created)
    }
    pub fn modified(&self) -> Result<Timestamp, ParseTimestampError> {
        parse_timestamp(&self.modified)
    }
}
