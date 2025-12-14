use crate::CCDBResult;
use chrono::{DateTime, Utc};
use gluex_core::{parsers::parse_timestamp, Id, RunNumber};
use std::fmt::Display;

/// Typed representation of a column type.
#[derive(Debug, Copy, Clone, Default)]
pub enum ColumnType {
    /// A column of signed integers (i32).
    Int,
    /// A column of unsigned integers (u32).
    UInt,
    /// A column of signed integers (i64).
    Long,
    /// A column of unsigned integers (u64).
    ULong,
    /// A column of floating-point values (f64).
    #[default]
    Double,
    /// A column of UTF-8 encoded strings.
    String,
    /// A column of boolean values.
    Bool,
}
impl ColumnType {
    /// Attempts to build a [`ColumnType`] from the identifier stored in CCDB.
    #[must_use]
    pub fn type_from_str(s: &str) -> Option<Self> {
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

    /// Returns the identifier string stored in CCDB for this type.
    #[must_use]
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

/// Metadata row describing a column belonging to a CCDB constant type.
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
    /// Identifier of the column definition.
    #[must_use]
    pub fn id(&self) -> Id {
        self.id
    }
    /// Human readable column name.
    #[must_use]
    pub fn name(&self) -> &str {
        &self.name
    }
    /// Identifier of the type table the column belongs to.
    #[must_use]
    pub fn type_id(&self) -> Id {
        self.type_id
    }
    /// Typed representation of the stored column data.
    #[must_use]
    pub fn column_type(&self) -> ColumnType {
        self.column_type
    }
    /// Ordering index for the column within the table schema.
    #[must_use]
    pub fn order(&self) -> i64 {
        self.order
    }
    /// Free-form comment associated with the column.
    #[must_use]
    pub fn comment(&self) -> &str {
        &self.comment
    }
    /// Timestamp describing when the column definition was created.
    ///
    /// # Errors
    ///
    /// Returns an error if the stored creation timestamp cannot be parsed as a UTC datetime.
    pub fn created(&self) -> CCDBResult<DateTime<Utc>> {
        Ok(parse_timestamp(&self.created)?)
    }
    /// Timestamp describing when the column definition was last updated.
    ///
    /// # Errors
    ///
    /// Returns an error if the stored modification timestamp cannot be parsed as a UTC datetime.
    pub fn modified(&self) -> CCDBResult<DateTime<Utc>> {
        Ok(parse_timestamp(&self.modified)?)
    }
}

/// Metadata describing a directory entry that groups constant types.
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
    /// Identifier of the directory row.
    #[must_use]
    pub fn id(&self) -> Id {
        self.id
    }
    /// Human readable directory name.
    #[must_use]
    pub fn name(&self) -> &str {
        &self.name
    }
    /// Identifier of the parent directory.
    #[must_use]
    pub fn parent_id(&self) -> Id {
        self.parent_id
    }
    /// Identifier of the user who created the directory.
    #[must_use]
    pub fn author_id(&self) -> Id {
        self.author_id
    }
    /// Free-form comment describing the directory.
    #[must_use]
    pub fn comment(&self) -> &str {
        &self.comment
    }
    /// True when the directory has been marked as deprecated.
    #[must_use]
    pub fn is_deprecated(&self) -> bool {
        self.is_deprecated
    }
    /// Identifier of the user who deprecated the directory.
    #[must_use]
    pub fn deprecated_by_user_id(&self) -> Id {
        self.deprecated_by_user_id
    }
    /// True when the directory is locked against modification.
    #[must_use]
    pub fn is_locked(&self) -> bool {
        self.is_locked
    }
    /// Identifier of the user who locked the directory.
    #[must_use]
    pub fn locked_by_user_id(&self) -> Id {
        self.locked_by_user_id
    }
    /// Timestamp describing when the directory was created.
    ///
    /// # Errors
    ///
    /// Returns an error if the stored creation timestamp cannot be parsed as a UTC datetime.
    pub fn created(&self) -> CCDBResult<DateTime<Utc>> {
        Ok(parse_timestamp(&self.created)?)
    }
    /// Timestamp describing when the directory was last updated.
    ///
    /// # Errors
    ///
    /// Returns an error if the stored modification timestamp cannot be parsed as a UTC datetime.
    pub fn modified(&self) -> CCDBResult<DateTime<Utc>> {
        Ok(parse_timestamp(&self.modified)?)
    }
}

/// Metadata describing a CCDB type table containing constants.
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
    /// Identifier of the type table.
    #[must_use]
    pub fn id(&self) -> Id {
        self.id
    }
    /// Identifier of the directory containing the type.
    #[must_use]
    pub fn directory_id(&self) -> Id {
        self.directory_id
    }
    /// Name of the type table.
    #[must_use]
    pub fn name(&self) -> &str {
        &self.name
    }
    /// Number of rows stored in the table.
    #[must_use]
    pub fn n_rows(&self) -> i64 {
        self.n_rows
    }
    /// Number of columns defined for the table.
    #[must_use]
    pub fn n_columns(&self) -> i64 {
        self.n_columns
    }
    /// Number of assignments referencing this table.
    #[must_use]
    pub fn n_assignments(&self) -> i64 {
        self.n_assignments
    }
    /// Identifier of the user who created the type.
    #[must_use]
    pub fn author_id(&self) -> Id {
        self.author_id
    }
    /// Free-form comment explaining the type table.
    #[must_use]
    pub fn comment(&self) -> &str {
        &self.comment
    }
    /// True when the type has been deprecated.
    #[must_use]
    pub fn is_deprecated(&self) -> bool {
        self.is_deprecated
    }
    /// Identifier of the user who deprecated the type.
    #[must_use]
    pub fn deprecated_by_user_id(&self) -> Id {
        self.deprecated_by_user_id
    }
    /// True when the type is locked.
    #[must_use]
    pub fn is_locked(&self) -> bool {
        self.is_locked
    }
    /// Identifier of the user who locked the type.
    #[must_use]
    pub fn locked_by_user_id(&self) -> Id {
        self.locked_by_user_id
    }
    /// Timestamp describing when the type was created.
    ///
    /// # Errors
    ///
    /// Returns an error if the stored creation timestamp cannot be parsed as a UTC datetime.
    pub fn created(&self) -> CCDBResult<DateTime<Utc>> {
        Ok(parse_timestamp(&self.created)?)
    }
    /// Timestamp describing when the type metadata was updated.
    ///
    /// # Errors
    ///
    /// Returns an error if the stored modification timestamp cannot be parsed as a UTC datetime.
    pub fn modified(&self) -> CCDBResult<DateTime<Utc>> {
        Ok(parse_timestamp(&self.modified)?)
    }
    /// Timestamp describing when the type was locked.
    ///
    /// # Errors
    ///
    /// Returns an error if the stored lock timestamp cannot be parsed as a UTC datetime.
    pub fn lock_time(&self) -> CCDBResult<DateTime<Utc>> {
        Ok(parse_timestamp(&self.lock_time)?)
    }
}

/// Metadata describing a stored set of constants for a type table.
#[derive(Debug, Clone, Default)]
pub struct ConstantSetMeta {
    pub(crate) id: Id,
    pub(crate) created: String,
    pub(crate) modified: String,
    pub(crate) vault: String,
    pub(crate) constant_type_id: Id,
}

impl ConstantSetMeta {
    /// Identifier of the constant set.
    #[must_use]
    pub fn id(&self) -> Id {
        self.id
    }
    /// Vault path or identifier for the backing data blob.
    #[must_use]
    pub fn vault(&self) -> &str {
        &self.vault
    }
    /// Identifier of the type table the set belongs to.
    #[must_use]
    pub fn constant_type_id(&self) -> Id {
        self.constant_type_id
    }
    /// Timestamp describing when the set was created.
    ///
    /// # Errors
    ///
    /// Returns an error if the stored creation timestamp cannot be parsed as a UTC datetime.
    pub fn created(&self) -> CCDBResult<DateTime<Utc>> {
        Ok(parse_timestamp(&self.created)?)
    }
    /// Timestamp describing when the set was last modified.
    ///
    /// # Errors
    ///
    /// Returns an error if the stored modification timestamp cannot be parsed as a UTC datetime.
    pub fn modified(&self) -> CCDBResult<DateTime<Utc>> {
        Ok(parse_timestamp(&self.modified)?)
    }
}

/// Metadata describing an assignment of a constant set to a run/event range.
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
    /// Identifier of the assignment.
    #[must_use]
    pub fn id(&self) -> Id {
        self.id
    }
    /// Identifier of the variation referenced by the assignment.
    #[must_use]
    pub fn variation_id(&self) -> Id {
        self.variation_id
    }
    /// Identifier of the associated run range.
    #[must_use]
    pub fn run_range_id(&self) -> Id {
        self.run_range_id
    }
    /// Identifier of the associated event range.
    #[must_use]
    pub fn event_range_id(&self) -> Id {
        self.event_range_id
    }
    /// Identifier of the user who created the assignment.
    #[must_use]
    pub fn author_id(&self) -> Id {
        self.author_id
    }
    /// Free-form comment associated with the assignment.
    #[must_use]
    pub fn comment(&self) -> &str {
        &self.comment
    }
    /// Identifier of the constant set referenced by the assignment.
    #[must_use]
    pub fn constant_set_id(&self) -> Id {
        self.constant_set_id
    }
    /// Timestamp describing when the assignment was created.
    ///
    /// # Errors
    ///
    /// Returns an error if the stored creation timestamp cannot be parsed as a UTC datetime.
    pub fn created(&self) -> CCDBResult<DateTime<Utc>> {
        Ok(parse_timestamp(&self.created)?)
    }
    /// Timestamp describing when the assignment was last updated.
    ///
    /// # Errors
    ///
    /// Returns an error if the stored modification timestamp cannot be parsed as a UTC datetime.
    pub fn modified(&self) -> CCDBResult<DateTime<Utc>> {
        Ok(parse_timestamp(&self.modified)?)
    }
}

/// Lightweight assignment row containing only identity and creation info.
#[derive(Debug, Clone, Default)]
pub struct AssignmentMetaLite {
    pub(crate) id: Id,
    pub(crate) created: String,
    pub(crate) constant_set_id: Id,
}
impl AssignmentMetaLite {
    /// Identifier of the assignment.
    #[must_use]
    pub fn id(&self) -> Id {
        self.id
    }
    /// Identifier of the constant set referenced by the assignment.
    #[must_use]
    pub fn constant_set_id(&self) -> Id {
        self.constant_set_id
    }
    /// Timestamp describing when the assignment was created.
    ///
    /// # Errors
    ///
    /// Returns an error if the stored creation timestamp cannot be parsed as a UTC datetime.
    pub fn created(&self) -> CCDBResult<DateTime<Utc>> {
        Ok(parse_timestamp(&self.created)?)
    }
}

/// Metadata describing a variation that partitions assignments.
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
    /// Identifier of the variation row.
    #[must_use]
    pub fn id(&self) -> Id {
        self.id
    }
    /// Human readable variation name.
    #[must_use]
    pub fn name(&self) -> &str {
        &self.name
    }
    /// Optional descriptive text for the variation.
    #[must_use]
    pub fn description(&self) -> &str {
        &self.description
    }
    /// Identifier of the user who created the variation.
    #[must_use]
    pub fn author_id(&self) -> Id {
        self.author_id
    }
    /// Free-form comment associated with the variation.
    #[must_use]
    pub fn comment(&self) -> &str {
        &self.comment
    }
    /// Identifier of the parent variation.
    #[must_use]
    pub fn parent_id(&self) -> Id {
        self.parent_id
    }
    /// True when the variation is locked.
    #[must_use]
    pub fn is_locked(&self) -> bool {
        self.is_locked
    }
    /// Identifier of the user who locked the variation.
    #[must_use]
    pub fn locked_by_user_id(&self) -> Id {
        self.locked_by_user_id
    }
    /// Behavior flag defining how lookups walk parent variations.
    #[must_use]
    pub fn go_back_behavior(&self) -> i64 {
        self.go_back_behavior
    }
    /// True when the variation is deprecated.
    #[must_use]
    pub fn is_deprecated(&self) -> bool {
        self.is_deprecated
    }
    /// Identifier of the user who deprecated the variation.
    #[must_use]
    pub fn deprecated_by_user_id(&self) -> Id {
        self.deprecated_by_user_id
    }
    /// Timestamp describing when the variation was created.
    ///
    /// # Errors
    ///
    /// Returns an error if the stored creation timestamp cannot be parsed as a UTC datetime.
    pub fn created(&self) -> CCDBResult<DateTime<Utc>> {
        Ok(parse_timestamp(&self.created)?)
    }
    /// Timestamp describing when the variation metadata was updated.
    ///
    /// # Errors
    ///
    /// Returns an error if the stored modification timestamp cannot be parsed as a UTC datetime.
    pub fn modified(&self) -> CCDBResult<DateTime<Utc>> {
        Ok(parse_timestamp(&self.modified)?)
    }
    /// Timestamp describing when the variation was locked.
    ///
    /// # Errors
    ///
    /// Returns an error if the stored lock timestamp cannot be parsed as a UTC datetime.
    pub fn lock_time(&self) -> CCDBResult<DateTime<Utc>> {
        Ok(parse_timestamp(&self.lock_time)?)
    }
    /// Timestamp describing when the go-back window expires.
    ///
    /// # Errors
    ///
    /// Returns an error if the stored go-back timestamp cannot be parsed as a UTC datetime.
    pub fn go_back_time(&self) -> CCDBResult<DateTime<Utc>> {
        Ok(parse_timestamp(&self.go_back_time)?)
    }
}

/// Metadata describing an inclusive range of run numbers.
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
    /// Identifier of the run range.
    #[must_use]
    pub fn id(&self) -> Id {
        self.id
    }
    /// Human readable name of the run range.
    #[must_use]
    pub fn name(&self) -> &str {
        &self.name
    }
    /// Minimum run number included in the range.
    #[must_use]
    pub fn run_min(&self) -> RunNumber {
        self.run_min
    }
    /// Maximum run number included in the range.
    #[must_use]
    pub fn run_max(&self) -> RunNumber {
        self.run_max
    }
    /// Free-form comment describing the run range.
    #[must_use]
    pub fn comment(&self) -> &str {
        &self.comment
    }
    /// Timestamp describing when the run range was created.
    ///
    /// # Errors
    ///
    /// Returns an error if the stored creation timestamp cannot be parsed as a UTC datetime.
    pub fn created(&self) -> CCDBResult<DateTime<Utc>> {
        Ok(parse_timestamp(&self.created)?)
    }
    /// Timestamp describing when the run range metadata was updated.
    ///
    /// # Errors
    ///
    /// Returns an error if the stored modification timestamp cannot be parsed as a UTC datetime.
    pub fn modified(&self) -> CCDBResult<DateTime<Utc>> {
        Ok(parse_timestamp(&self.modified)?)
    }
}

/// Metadata describing an inclusive event range bound to a run.
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
    /// Identifier of the event range.
    #[must_use]
    pub fn id(&self) -> Id {
        self.id
    }
    /// Run number this event range belongs to.
    #[must_use]
    pub fn run_number(&self) -> RunNumber {
        self.run_number
    }
    /// Minimum event number included in the range.
    #[must_use]
    pub fn event_min(&self) -> i64 {
        self.event_min
    }
    /// Maximum event number included in the range.
    #[must_use]
    pub fn event_max(&self) -> i64 {
        self.event_max
    }
    /// Free-form comment describing the event range.
    #[must_use]
    pub fn comment(&self) -> &str {
        &self.comment
    }
    /// Timestamp describing when the event range was created.
    ///
    /// # Errors
    ///
    /// Returns an error if the stored creation timestamp cannot be parsed as a UTC datetime.
    pub fn created(&self) -> CCDBResult<DateTime<Utc>> {
        Ok(parse_timestamp(&self.created)?)
    }
    /// Timestamp describing when the event range metadata was updated.
    ///
    /// # Errors
    ///
    /// Returns an error if the stored modification timestamp cannot be parsed as a UTC datetime.
    pub fn modified(&self) -> CCDBResult<DateTime<Utc>> {
        Ok(parse_timestamp(&self.modified)?)
    }
}
