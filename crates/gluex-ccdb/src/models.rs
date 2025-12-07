use crate::CCDBResult;
use chrono::{DateTime, Utc};
use gluex_core::{parsers::parse_timestamp, Id, RunNumber};
use std::fmt::Display;

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
    pub(crate) id: Id,
    pub(crate) created: String,
    pub(crate) modified: String,
    pub(crate) name: String,
    pub(crate) type_id: Id,
    pub(crate) column_type: ColumnType,
    pub(crate) order: i64,
    pub(crate) comment: String,
}
impl ColumnMeta {
    pub fn id(&self) -> Id {
        self.id
    }
    pub fn name(&self) -> &str {
        &self.name
    }
    pub fn type_id(&self) -> Id {
        self.type_id
    }
    pub fn column_type(&self) -> ColumnType {
        self.column_type
    }
    pub fn order(&self) -> i64 {
        self.order
    }
    pub fn comment(&self) -> &str {
        &self.comment
    }
    pub fn created(&self) -> CCDBResult<DateTime<Utc>> {
        Ok(parse_timestamp(&self.created)?)
    }
    pub fn modified(&self) -> CCDBResult<DateTime<Utc>> {
        Ok(parse_timestamp(&self.modified)?)
    }
}

#[derive(Debug, Clone, Default)]
pub struct DirectoryMeta {
    pub(crate) id: Id,
    pub(crate) created: String,
    pub(crate) modified: String,
    pub(crate) name: String,
    pub(crate) parent_id: Id,
    pub(crate) author_id: Id,
    pub(crate) comment: String,
    pub(crate) is_deprecated: bool,
    pub(crate) deprecated_by_user_id: Id,
    pub(crate) is_locked: bool,
    pub(crate) locked_by_user_id: Id,
}
impl DirectoryMeta {
    pub fn id(&self) -> Id {
        self.id
    }
    pub fn name(&self) -> &str {
        &self.name
    }
    pub fn parent_id(&self) -> Id {
        self.parent_id
    }
    pub fn author_id(&self) -> Id {
        self.author_id
    }
    pub fn comment(&self) -> &str {
        &self.comment
    }
    pub fn is_deprecated(&self) -> bool {
        self.is_deprecated
    }
    pub fn deprecated_by_user_id(&self) -> Id {
        self.deprecated_by_user_id
    }
    pub fn is_locked(&self) -> bool {
        self.is_locked
    }
    pub fn locked_by_user_id(&self) -> Id {
        self.locked_by_user_id
    }
    pub fn created(&self) -> CCDBResult<DateTime<Utc>> {
        Ok(parse_timestamp(&self.created)?)
    }
    pub fn modified(&self) -> CCDBResult<DateTime<Utc>> {
        Ok(parse_timestamp(&self.modified)?)
    }
}

#[derive(Debug, Clone, Default)]
pub struct TypeTableMeta {
    pub(crate) id: Id,
    pub(crate) created: String,
    pub(crate) modified: String,
    pub(crate) directory_id: Id,
    pub(crate) name: String,
    pub(crate) n_rows: i64,
    pub(crate) n_columns: i64,
    pub(crate) n_assignments: i64,
    pub(crate) author_id: Id,
    pub(crate) comment: String,
    pub(crate) is_deprecated: bool,
    pub(crate) deprecated_by_user_id: Id,
    pub(crate) is_locked: bool,
    pub(crate) locked_by_user_id: Id,
    pub(crate) lock_time: String,
}

impl TypeTableMeta {
    pub fn id(&self) -> Id {
        self.id
    }
    pub fn directory_id(&self) -> Id {
        self.directory_id
    }
    pub fn name(&self) -> &str {
        &self.name
    }
    pub fn n_rows(&self) -> i64 {
        self.n_rows
    }
    pub fn n_columns(&self) -> i64 {
        self.n_columns
    }
    pub fn n_assignments(&self) -> i64 {
        self.n_assignments
    }
    pub fn author_id(&self) -> Id {
        self.author_id
    }
    pub fn comment(&self) -> &str {
        &self.comment
    }
    pub fn is_deprecated(&self) -> bool {
        self.is_deprecated
    }
    pub fn deprecated_by_user_id(&self) -> Id {
        self.deprecated_by_user_id
    }
    pub fn is_locked(&self) -> bool {
        self.is_locked
    }
    pub fn locked_by_user_id(&self) -> Id {
        self.locked_by_user_id
    }
    pub fn created(&self) -> CCDBResult<DateTime<Utc>> {
        Ok(parse_timestamp(&self.created)?)
    }
    pub fn modified(&self) -> CCDBResult<DateTime<Utc>> {
        Ok(parse_timestamp(&self.modified)?)
    }
    pub fn lock_time(&self) -> CCDBResult<DateTime<Utc>> {
        Ok(parse_timestamp(&self.lock_time)?)
    }
}

#[derive(Debug, Clone, Default)]
pub struct ConstantSetMeta {
    pub(crate) id: Id,
    pub(crate) created: String,
    pub(crate) modified: String,
    pub(crate) vault: String,
    pub(crate) constant_type_id: Id,
}

impl ConstantSetMeta {
    pub fn id(&self) -> Id {
        self.id
    }
    pub fn vault(&self) -> &str {
        &self.vault
    }
    pub fn constant_type_id(&self) -> Id {
        self.constant_type_id
    }
    pub fn created(&self) -> CCDBResult<DateTime<Utc>> {
        Ok(parse_timestamp(&self.created)?)
    }
    pub fn modified(&self) -> CCDBResult<DateTime<Utc>> {
        Ok(parse_timestamp(&self.modified)?)
    }
}

#[derive(Debug, Clone, Default)]
pub struct AssignmentMeta {
    pub(crate) id: Id,
    pub(crate) created: String,
    pub(crate) modified: String,
    pub(crate) variation_id: Id,
    pub(crate) run_range_id: Id,
    pub(crate) event_range_id: Id,
    pub(crate) author_id: Id,
    pub(crate) comment: String,
    pub(crate) constant_set_id: Id,
}
impl AssignmentMeta {
    pub fn id(&self) -> Id {
        self.id
    }
    pub fn variation_id(&self) -> Id {
        self.variation_id
    }
    pub fn run_range_id(&self) -> Id {
        self.run_range_id
    }
    pub fn event_range_id(&self) -> Id {
        self.event_range_id
    }
    pub fn author_id(&self) -> Id {
        self.author_id
    }
    pub fn comment(&self) -> &str {
        &self.comment
    }
    pub fn constant_set_id(&self) -> Id {
        self.constant_set_id
    }
    pub fn created(&self) -> CCDBResult<DateTime<Utc>> {
        Ok(parse_timestamp(&self.created)?)
    }
    pub fn modified(&self) -> CCDBResult<DateTime<Utc>> {
        Ok(parse_timestamp(&self.modified)?)
    }
}

#[derive(Debug, Clone, Default)]
pub struct AssignmentMetaLite {
    pub(crate) id: Id,
    pub(crate) created: String,
    pub(crate) constant_set_id: Id,
}
impl AssignmentMetaLite {
    pub fn id(&self) -> Id {
        self.id
    }
    pub fn constant_set_id(&self) -> Id {
        self.constant_set_id
    }
    pub fn created(&self) -> CCDBResult<DateTime<Utc>> {
        Ok(parse_timestamp(&self.created)?)
    }
}

#[derive(Debug, Clone, Default)]
pub struct VariationMeta {
    pub(crate) id: Id,
    pub(crate) created: String,
    pub(crate) modified: String,
    pub(crate) name: String,
    pub(crate) description: String,
    pub(crate) author_id: Id,
    pub(crate) comment: String,
    pub(crate) parent_id: Id,
    pub(crate) is_locked: bool,
    pub(crate) lock_time: String,
    pub(crate) locked_by_user_id: Id,
    pub(crate) go_back_behavior: i64,
    pub(crate) go_back_time: String,
    pub(crate) is_deprecated: bool,
    pub(crate) deprecated_by_user_id: Id,
}
impl VariationMeta {
    pub fn id(&self) -> Id {
        self.id
    }
    pub fn name(&self) -> &str {
        &self.name
    }
    pub fn description(&self) -> &str {
        &self.description
    }
    pub fn author_id(&self) -> Id {
        self.author_id
    }
    pub fn comment(&self) -> &str {
        &self.comment
    }
    pub fn parent_id(&self) -> Id {
        self.parent_id
    }
    pub fn is_locked(&self) -> bool {
        self.is_locked
    }
    pub fn locked_by_user_id(&self) -> Id {
        self.locked_by_user_id
    }
    pub fn go_back_behavior(&self) -> i64 {
        self.go_back_behavior
    }
    pub fn is_deprecated(&self) -> bool {
        self.is_deprecated
    }
    pub fn deprecated_by_user_id(&self) -> Id {
        self.deprecated_by_user_id
    }
    pub fn created(&self) -> CCDBResult<DateTime<Utc>> {
        Ok(parse_timestamp(&self.created)?)
    }
    pub fn modified(&self) -> CCDBResult<DateTime<Utc>> {
        Ok(parse_timestamp(&self.modified)?)
    }
    pub fn lock_time(&self) -> CCDBResult<DateTime<Utc>> {
        Ok(parse_timestamp(&self.lock_time)?)
    }
    pub fn go_back_time(&self) -> CCDBResult<DateTime<Utc>> {
        Ok(parse_timestamp(&self.go_back_time)?)
    }
}

#[derive(Debug, Clone, Default)]
pub struct RunRangeMeta {
    pub(crate) id: Id,
    pub(crate) created: String,
    pub(crate) modified: String,
    pub(crate) name: String,
    pub(crate) run_min: RunNumber,
    pub(crate) run_max: RunNumber,
    pub(crate) comment: String,
}

impl RunRangeMeta {
    pub fn id(&self) -> Id {
        self.id
    }
    pub fn name(&self) -> &str {
        &self.name
    }
    pub fn run_min(&self) -> RunNumber {
        self.run_min
    }
    pub fn run_max(&self) -> RunNumber {
        self.run_max
    }
    pub fn comment(&self) -> &str {
        &self.comment
    }
    pub fn created(&self) -> CCDBResult<DateTime<Utc>> {
        Ok(parse_timestamp(&self.created)?)
    }
    pub fn modified(&self) -> CCDBResult<DateTime<Utc>> {
        Ok(parse_timestamp(&self.modified)?)
    }
}

#[derive(Debug, Clone, Default)]
pub struct EventRangeMeta {
    pub(crate) id: Id,
    pub(crate) created: String,
    pub(crate) modified: String,
    pub(crate) run_number: RunNumber,
    pub(crate) event_min: i64,
    pub(crate) event_max: i64,
    pub(crate) comment: String,
}

impl EventRangeMeta {
    pub fn id(&self) -> Id {
        self.id
    }
    pub fn run_number(&self) -> RunNumber {
        self.run_number
    }
    pub fn event_min(&self) -> i64 {
        self.event_min
    }
    pub fn event_max(&self) -> i64 {
        self.event_max
    }
    pub fn comment(&self) -> &str {
        &self.comment
    }
    pub fn created(&self) -> CCDBResult<DateTime<Utc>> {
        Ok(parse_timestamp(&self.created)?)
    }
    pub fn modified(&self) -> CCDBResult<DateTime<Utc>> {
        Ok(parse_timestamp(&self.modified)?)
    }
}
