use crate::models::{ColumnMeta, ColumnType};
use itertools::izip;
use memchr::memchr;
use std::{collections::HashMap, sync::Arc};
use thiserror::Error;

/// Column-oriented storage for a single CCDB field.
#[derive(Debug, Clone)]
pub enum Column {
    /// Signed 32-bit integer values.
    Int(Vec<i32>),
    /// Unsigned 32-bit integer values.
    UInt(Vec<u32>),
    /// Signed 64-bit integer values.
    Long(Vec<i64>),
    /// Unsigned 64-bit integer values.
    ULong(Vec<u64>),
    /// Floating-point values.
    Double(Vec<f64>),
    /// Boolean values.
    Bool(Vec<bool>),
    /// UTF-8 string values.
    String(Vec<String>),
}

impl Column {
    /// Number of rows in this column.
    #[must_use]
    pub fn len(&self) -> usize {
        match self {
            Self::Int(v) => v.len(),
            Self::UInt(v) => v.len(),
            Self::Long(v) => v.len(),
            Self::ULong(v) => v.len(),
            Self::Double(v) => v.len(),
            Self::Bool(v) => v.len(),
            Self::String(v) => v.len(),
        }
    }

    /// Check if the column is empty.
    #[must_use]
    pub fn is_empty(&self) -> bool {
        match self {
            Self::Int(v) => v.is_empty(),
            Self::UInt(v) => v.is_empty(),
            Self::Long(v) => v.is_empty(),
            Self::ULong(v) => v.is_empty(),
            Self::Double(v) => v.is_empty(),
            Self::Bool(v) => v.is_empty(),
            Self::String(v) => v.is_empty(),
        }
    }

    /// Returns the value at the requested row.
    #[must_use]
    pub fn row(&self, row: usize) -> Value<'_> {
        match self {
            Self::Int(v) => Value::Int(&v[row]),
            Self::UInt(v) => Value::UInt(&v[row]),
            Self::Long(v) => Value::Long(&v[row]),
            Self::ULong(v) => Value::ULong(&v[row]),
            Self::Double(v) => Value::Double(&v[row]),
            Self::Bool(v) => Value::Bool(&v[row]),
            Self::String(v) => Value::String(&v[row]),
        }
    }

    /// Returns a clone of the underlying [`i32`] data, if the type matches.
    #[must_use]
    pub fn int(&self) -> Option<Vec<i32>> {
        match self {
            Self::Int(v) => Some(v.clone()),
            _ => None,
        }
    }
    /// Returns a clone of the underlying [`u32`] data, if the type matches.
    #[must_use]
    pub fn uint(&self) -> Option<Vec<u32>> {
        match self {
            Self::UInt(v) => Some(v.clone()),
            _ => None,
        }
    }
    /// Returns a clone of the underlying [`i64`] data, if the type matches.
    #[must_use]
    pub fn long(&self) -> Option<Vec<i64>> {
        match self {
            Self::Long(v) => Some(v.clone()),
            _ => None,
        }
    }
    /// Returns a clone of the underlying [`u64`] data, if the type matches.
    #[must_use]
    pub fn ulong(&self) -> Option<Vec<u64>> {
        match self {
            Self::ULong(v) => Some(v.clone()),
            _ => None,
        }
    }
    /// Returns a clone of the underlying [`f64`] data, if the type matches.
    #[must_use]
    pub fn double(&self) -> Option<Vec<f64>> {
        match self {
            Self::Double(v) => Some(v.clone()),
            _ => None,
        }
    }
    /// Returns a clone of the underlying [`bool`] data, if the type matches.
    #[must_use]
    pub fn bool(&self) -> Option<Vec<bool>> {
        match self {
            Self::Bool(v) => Some(v.clone()),
            _ => None,
        }
    }
    /// Returns a clone of the underlying [`String`] data, if the type matches.
    #[must_use]
    pub fn string(&self) -> Option<Vec<String>> {
        match self {
            Self::String(v) => Some(v.clone()),
            _ => None,
        }
    }
}

/// Borrowed view into a single cell of CCDB data.
#[derive(Debug, Copy, Clone)]
pub enum Value<'a> {
    /// Signed 32-bit integer cell.
    Int(&'a i32),
    /// Unsigned 32-bit integer cell.
    UInt(&'a u32),
    /// Signed 64-bit integer cell.
    Long(&'a i64),
    /// Unsigned 64-bit integer cell.
    ULong(&'a u64),
    /// Floating-point cell.
    Double(&'a f64),
    /// Boolean cell.
    Bool(&'a bool),
    /// UTF-8 string cell.
    String(&'a str),
}
impl<'a> Value<'a> {
    /// Converts to [`i32`] if this is an integer cell.
    #[must_use]
    pub fn as_int(self) -> Option<i32> {
        if let Value::Int(v) = self {
            Some(*v)
        } else {
            None
        }
    }

    /// Converts to [`u32`] if this is an unsigned integer cell.
    #[must_use]
    pub fn as_uint(self) -> Option<u32> {
        if let Value::UInt(v) = self {
            Some(*v)
        } else {
            None
        }
    }

    /// Converts to [`i64`] if this is a 64-bit integer cell.
    #[must_use]
    pub fn as_long(self) -> Option<i64> {
        if let Value::Long(v) = self {
            Some(*v)
        } else {
            None
        }
    }

    /// Converts to [`u64`] if this is an unsigned 64-bit integer cell.
    #[must_use]
    pub fn as_ulong(self) -> Option<u64> {
        if let Value::ULong(v) = self {
            Some(*v)
        } else {
            None
        }
    }

    /// Converts to [`f64`] if this is a floating-point cell.
    #[must_use]
    pub fn as_double(self) -> Option<f64> {
        if let Value::Double(v) = self {
            Some(*v)
        } else {
            None
        }
    }

    /// Converts to [`bool`] if this is a boolean cell.
    #[must_use]
    pub fn as_bool(self) -> Option<bool> {
        if let Value::Bool(v) = self {
            Some(*v)
        } else {
            None
        }
    }

    /// Converts to [`&str`] if this is a string cell.
    #[must_use]
    pub fn as_str(self) -> Option<&'a str> {
        if let Value::String(v) = self {
            Some(v)
        } else {
            None
        }
    }
}

/// Borrowed view over a single row of a [`Data`] table.
pub struct RowView<'a> {
    row: usize,
    columns: &'a [Column],
    column_names: &'a [String],
    column_indices: &'a HashMap<String, usize>,
    column_types: &'a [ColumnType],
}
impl<'a> RowView<'a> {
    /// Returns a typed cell by positional column index.
    #[must_use]
    pub fn value(&self, column: usize) -> Option<Value<'a>> {
        self.columns.get(column).map(|col| col.row(self.row))
    }

    /// Returns a typed cell by column name.
    #[must_use]
    pub fn named_value(&self, name: &str) -> Option<Value<'a>> {
        self.column_indices
            .get(name)
            .and_then(|&idx| self.value(idx))
    }

    /// Returns a positional column as [`i32`] if present and typed accordingly.
    #[must_use]
    pub fn int(&self, column: usize) -> Option<i32> {
        self.value(column)?.as_int()
    }
    /// Returns a positional column as [`u32`] if present and typed accordingly.
    #[must_use]
    pub fn uint(&self, column: usize) -> Option<u32> {
        self.value(column)?.as_uint()
    }
    /// Returns a positional column as [`i64`] if present and typed accordingly.
    #[must_use]
    pub fn long(&self, column: usize) -> Option<i64> {
        self.value(column)?.as_long()
    }
    /// Returns a positional column as [`u64`] if present and typed accordingly.
    #[must_use]
    pub fn ulong(&self, column: usize) -> Option<u64> {
        self.value(column)?.as_ulong()
    }
    /// Returns a positional column as [`f64`] if present and typed accordingly.
    #[must_use]
    pub fn double(&self, column: usize) -> Option<f64> {
        self.value(column)?.as_double()
    }
    /// Returns a positional column as [`&str`] if present and typed accordingly.
    #[must_use]
    pub fn string(&self, column: usize) -> Option<&'a str> {
        self.value(column)?.as_str()
    }
    /// Returns a positional column as [`bool`] if present and typed accordingly.
    #[must_use]
    pub fn bool(&self, column: usize) -> Option<bool> {
        self.value(column)?.as_bool()
    }

    /// Returns a named column as [`i32`] if present and typed accordingly.
    #[must_use]
    pub fn named_int(&self, name: &str) -> Option<i32> {
        self.named_value(name)?.as_int()
    }
    /// Returns a named column as [`u32`] if present and typed accordingly.
    #[must_use]
    pub fn named_uint(&self, name: &str) -> Option<u32> {
        self.named_value(name)?.as_uint()
    }
    /// Returns a named column as [`i64`] if present and typed accordingly.
    #[must_use]
    pub fn named_long(&self, name: &str) -> Option<i64> {
        self.named_value(name)?.as_long()
    }
    /// Returns a named column as [`u64`] if present and typed accordingly.
    #[must_use]
    pub fn named_ulong(&self, name: &str) -> Option<u64> {
        self.named_value(name)?.as_ulong()
    }
    /// Returns a named column as [`f64`] if present and typed accordingly.
    #[must_use]
    pub fn named_double(&self, name: &str) -> Option<f64> {
        self.named_value(name)?.as_double()
    }
    /// Returns a named column as [`&str`] if present and typed accordingly.
    #[must_use]
    pub fn named_string(&self, name: &str) -> Option<&'a str> {
        self.named_value(name)?.as_str()
    }
    /// Returns a named column as [`bool`] if present and typed accordingly.
    #[must_use]
    pub fn named_bool(&self, name: &str) -> Option<bool> {
        self.named_value(name)?.as_bool()
    }

    /// Iterates over `(name, type, value)` tuples for the current row.
    pub fn iter_columns(&self) -> impl Iterator<Item = (&'a str, ColumnType, Value<'a>)> + '_ {
        izip!(
            self.column_names.iter(),
            self.column_types.iter(),
            self.columns.iter()
        )
        .map(move |(name, column_type, col)| (name.as_str(), *column_type, col.row(self.row)))
    }

    /// Checks whether the row contains a column with the given name.
    #[must_use]
    pub fn contains(&self, name: &str) -> bool {
        self.column_indices.contains_key(name)
    }

    /// Number of columns in this row.
    #[must_use]
    pub fn n_columns(&self) -> usize {
        self.columns.len()
    }

    /// Column types in this row in positional order.
    #[must_use]
    pub fn column_types(&self) -> &'a [ColumnType] {
        self.column_types
    }
}

/// Description of a column in a CCDB table.
#[derive(Debug, Clone)]
pub struct ColumnDef {
    /// Zero-based column position.
    pub index: usize,
    /// Column name as stored in CCDB metadata.
    pub name: String,
    /// Logical column type for this field.
    pub column_type: ColumnType,
}

/// Immutable layout information for a table's columns.
#[derive(Debug, Clone)]
pub struct ColumnLayout {
    columns: Vec<ColumnMeta>,
    column_names: Vec<String>,
    column_indices: HashMap<String, usize>,
    column_types: Vec<ColumnType>,
}

impl ColumnLayout {
    /// Builds a layout from ordered column metadata.
    #[must_use]
    pub fn new(mut columns: Vec<ColumnMeta>) -> Self {
        columns.sort_unstable_by_key(|c| c.order);
        let column_names: Vec<String> = columns
            .iter()
            .enumerate()
            .map(|(i, c)| {
                if c.name.is_empty() {
                    i.to_string()
                } else {
                    c.name.clone()
                }
            })
            .collect();
        let column_types: Vec<ColumnType> = columns.iter().map(|c| c.column_type).collect();
        let column_indices: HashMap<String, usize> = column_names
            .iter()
            .enumerate()
            .map(|(idx, name)| (name.clone(), idx))
            .collect();
        Self {
            columns,
            column_names,
            column_indices,
            column_types,
        }
    }

    /// Sorted column metadata.
    #[must_use]
    pub fn columns(&self) -> &[ColumnMeta] {
        &self.columns
    }

    /// Column names in positional order.
    #[must_use]
    pub fn column_names(&self) -> &[String] {
        &self.column_names
    }

    /// Column name to positional index lookup table.
    #[must_use]
    pub fn column_indices(&self) -> &HashMap<String, usize> {
        &self.column_indices
    }

    /// Column types in positional order.
    #[must_use]
    pub fn column_types(&self) -> &[ColumnType] {
        &self.column_types
    }

    /// Number of columns described by this layout.
    #[must_use]
    pub fn column_count(&self) -> usize {
        self.columns.len()
    }
}

/// Column-major table returned from CCDB fetch operations.
pub struct Data {
    n_rows: usize,
    layout: Arc<ColumnLayout>,
    columns: Vec<Column>,
}

impl Data {
    /// Builds a [`Data`] table from a raw vault string and column metadata.
    ///
    /// # Errors
    ///
    /// This method will return an error if the parsed number of columns does not equal the
    /// expected number from the database or if any of the column contents cannot be parsed into
    /// their respective data types.
    pub fn from_vault(
        vault: &str,
        layout: Arc<ColumnLayout>,
        n_rows: usize,
    ) -> Result<Self, CCDBDataError> {
        let n_columns = layout.column_count();
        let expected_cells = n_rows * n_columns;
        let column_types = layout.column_types();
        let mut column_vecs: Vec<Column> = column_types
            .iter()
            .map(|t| match t {
                ColumnType::Int => Column::Int(Vec::with_capacity(n_rows)),
                ColumnType::UInt => Column::UInt(Vec::with_capacity(n_rows)),
                ColumnType::Long => Column::Long(Vec::with_capacity(n_rows)),
                ColumnType::ULong => Column::ULong(Vec::with_capacity(n_rows)),
                ColumnType::Double => Column::Double(Vec::with_capacity(n_rows)),
                ColumnType::String => Column::String(Vec::with_capacity(n_rows)),
                ColumnType::Bool => Column::Bool(Vec::with_capacity(n_rows)),
            })
            .collect();
        let mut raw_iter = VaultFieldIter::new(vault);
        for idx in 0..expected_cells {
            let Some(raw) = raw_iter.next() else {
                return Err(CCDBDataError::ColumnCountMismatch {
                    expected: expected_cells,
                    found: idx,
                });
            };
            let row = idx / n_columns;
            let col = idx % n_columns;
            let column_type = column_types[col];

            match (&mut column_vecs[col], column_type) {
                (Column::Int(vec), ColumnType::Int) => {
                    vec.push(raw.parse().map_err(|_| CCDBDataError::ParseError {
                        column: col,
                        row,
                        column_type,
                        text: raw.to_string(),
                    })?);
                }
                (Column::UInt(vec), ColumnType::UInt) => {
                    vec.push(raw.parse().map_err(|_| CCDBDataError::ParseError {
                        column: col,
                        row,
                        column_type,
                        text: raw.to_string(),
                    })?);
                }
                (Column::Long(vec), ColumnType::Long) => {
                    vec.push(raw.parse().map_err(|_| CCDBDataError::ParseError {
                        column: col,
                        row,
                        column_type,
                        text: raw.to_string(),
                    })?);
                }
                (Column::ULong(vec), ColumnType::ULong) => {
                    vec.push(raw.parse().map_err(|_| CCDBDataError::ParseError {
                        column: col,
                        row,
                        column_type,
                        text: raw.to_string(),
                    })?);
                }
                (Column::Double(vec), ColumnType::Double) => {
                    vec.push(raw.parse().map_err(|_| CCDBDataError::ParseError {
                        column: col,
                        row,
                        column_type,
                        text: raw.to_string(),
                    })?);
                }
                (Column::String(vec), ColumnType::String) => {
                    let decoded = raw.replace("&delimeter", "|");
                    vec.push(decoded);
                }
                (Column::Bool(vec), ColumnType::Bool) => {
                    vec.push(parse_bool(raw));
                }
                _ => unreachable!("column type mismatch"),
            }
        }
        if raw_iter.next().is_some() {
            let found = expected_cells + 1 + raw_iter.count();
            return Err(CCDBDataError::ColumnCountMismatch {
                expected: expected_cells,
                found,
            });
        }
        Ok(Data {
            n_rows,
            layout,
            columns: column_vecs,
        })
    }

    /// Number of rows in the dataset.
    #[must_use]
    pub fn n_rows(&self) -> usize {
        self.n_rows
    }
    /// Number of columns in the dataset.
    #[must_use]
    pub fn n_columns(&self) -> usize {
        self.layout.column_count()
    }
    /// Column names in positional order.
    #[must_use]
    pub fn column_names(&self) -> &[String] {
        self.layout.column_names()
    }

    /// Column types in positional order.
    #[must_use]
    pub fn column_types(&self) -> &[ColumnType] {
        self.layout.column_types()
    }

    /// Returns a borrowed column by positional index.
    #[must_use]
    pub fn column(&self, idx: usize) -> Option<&Column> {
        self.columns.get(idx)
    }

    /// Returns a borrowed column by name.
    #[must_use]
    pub fn named_column(&self, name: &str) -> Option<&Column> {
        self.layout
            .column_indices()
            .get(name)
            .and_then(|idx| self.columns.get(*idx))
    }

    /// Returns a cloned column by positional index.
    #[must_use]
    pub fn column_clone(&self, idx: usize) -> Option<Column> {
        self.columns.get(idx).cloned()
    }

    /// Returns a cloned column by name.
    #[must_use]
    pub fn named_column_clone(&self, name: &str) -> Option<Column> {
        self.layout
            .column_indices()
            .get(name)
            .and_then(|idx| self.columns.get(*idx))
            .cloned()
    }

    /// Returns a single cell value by column and row index.
    #[must_use]
    pub fn value(&self, column: usize, row: usize) -> Option<Value<'_>> {
        if row >= self.n_rows || column >= self.layout.column_count() {
            return None;
        }
        match self.columns.get(column)? {
            Column::Int(v) => Some(Value::Int(&v[row])),
            Column::UInt(v) => Some(Value::UInt(&v[row])),
            Column::Long(v) => Some(Value::Long(&v[row])),
            Column::ULong(v) => Some(Value::ULong(&v[row])),
            Column::Double(v) => Some(Value::Double(&v[row])),
            Column::Bool(v) => Some(Value::Bool(&v[row])),
            Column::String(v) => Some(Value::String(&v[row])),
        }
    }
    /// Returns a named cell as [`i32`] if present and typed accordingly.
    #[must_use]
    pub fn named_int(&self, name: &str, row: usize) -> Option<i32> {
        self.named_column(name)?.row(row).as_int()
    }
    /// Returns a named cell as [`u32`] if present and typed accordingly.
    #[must_use]
    pub fn named_uint(&self, name: &str, row: usize) -> Option<u32> {
        self.named_column(name)?.row(row).as_uint()
    }
    /// Returns a named cell as [`i64`] if present and typed accordingly.
    #[must_use]
    pub fn named_long(&self, name: &str, row: usize) -> Option<i64> {
        self.named_column(name)?.row(row).as_long()
    }
    /// Returns a named cell as [`u64`] if present and typed accordingly.
    #[must_use]
    pub fn named_ulong(&self, name: &str, row: usize) -> Option<u64> {
        self.named_column(name)?.row(row).as_ulong()
    }
    /// Returns a named cell as [`f64`] if present and typed accordingly.
    #[must_use]
    pub fn named_double(&self, name: &str, row: usize) -> Option<f64> {
        self.named_column(name)?.row(row).as_double()
    }
    /// Returns a named cell as [`&str`] if present and typed accordingly.
    #[must_use]
    pub fn named_string(&self, name: &str, row: usize) -> Option<&str> {
        self.named_column(name)?.row(row).as_str()
    }
    /// Returns a named cell as [`bool`] if present and typed accordingly.
    #[must_use]
    pub fn named_bool(&self, name: &str, row: usize) -> Option<bool> {
        self.named_column(name)?.row(row).as_bool()
    }

    /// Returns a positional cell as [`i32`] if present and typed accordingly.
    #[must_use]
    pub fn int(&self, column: usize, row: usize) -> Option<i32> {
        self.value(column, row)?.as_int()
    }
    /// Returns a positional cell as [`u32`] if present and typed accordingly.
    #[must_use]
    pub fn uint(&self, column: usize, row: usize) -> Option<u32> {
        self.value(column, row)?.as_uint()
    }
    /// Returns a positional cell as [`i64`] if present and typed accordingly.
    #[must_use]
    pub fn long(&self, column: usize, row: usize) -> Option<i64> {
        self.value(column, row)?.as_long()
    }
    /// Returns a positional cell as [`u64`] if present and typed accordingly.
    #[must_use]
    pub fn ulong(&self, column: usize, row: usize) -> Option<u64> {
        self.value(column, row)?.as_ulong()
    }
    /// Returns a positional cell as [`f64`] if present and typed accordingly.
    #[must_use]
    pub fn double(&self, column: usize, row: usize) -> Option<f64> {
        self.value(column, row)?.as_double()
    }
    /// Returns a positional cell as [`&str`] if present and typed accordingly.
    #[must_use]
    pub fn string(&self, column: usize, row: usize) -> Option<&str> {
        self.value(column, row)?.as_str()
    }
    /// Returns a positional cell as [`bool`] if present and typed accordingly.
    #[must_use]
    pub fn bool(&self, column: usize, row: usize) -> Option<bool> {
        self.value(column, row)?.as_bool()
    }

    /// Returns a borrowed view of a single row, or an error if out of bounds.
    ///
    /// # Errors
    ///
    /// This method will return an error if `row` is out of bounds.
    pub fn row(&self, row: usize) -> Result<RowView<'_>, CCDBDataError> {
        if row >= self.n_rows {
            return Err(CCDBDataError::RowOutOfBounds {
                requested: row,
                n_rows: self.n_rows,
            });
        }
        let layout = self.layout.as_ref();
        Ok(RowView {
            row,
            columns: &self.columns,
            column_names: layout.column_names(),
            column_indices: layout.column_indices(),
            column_types: layout.column_types(),
        })
    }

    /// Iterates over all rows in the dataset.
    pub fn iter_rows(&self) -> impl Iterator<Item = RowView<'_>> {
        let layout = self.layout.as_ref();
        let columns = &self.columns;
        let column_names = layout.column_names();
        let column_indices = layout.column_indices();
        let column_types = layout.column_types();
        (0..self.n_rows).map(move |row| RowView {
            row,
            columns,
            column_names,
            column_indices,
            column_types,
        })
    }

    /// Iterates over `(name, type, column)` tuples for each column.
    pub fn iter_columns(&self) -> impl Iterator<Item = (&String, &ColumnType, &Column)> {
        izip!(
            self.layout.column_names().iter(),
            self.layout.column_types().iter(),
            self.columns.iter()
        )
    }

    /// True if a column with the given name exists.
    #[must_use]
    pub fn contains(&self, name: &str) -> bool {
        self.layout.column_indices().contains_key(name)
    }
}

struct VaultFieldIter<'a> {
    input: &'a str,
    cursor: usize,
    finished: bool,
}

impl<'a> VaultFieldIter<'a> {
    fn new(input: &'a str) -> Self {
        Self {
            input,
            cursor: 0,
            finished: false,
        }
    }
}

impl<'a> Iterator for VaultFieldIter<'a> {
    type Item = &'a str;

    fn next(&mut self) -> Option<Self::Item> {
        if self.finished {
            return None;
        }
        if self.cursor > self.input.len() {
            self.finished = true;
            return Some("");
        }
        if self.cursor == self.input.len() {
            self.finished = true;
            return Some("");
        }
        let bytes = self.input.as_bytes();
        if let Some(pos) = memchr(b'|', &bytes[self.cursor..]) {
            let start = self.cursor;
            let end = start + pos;
            self.cursor = end + 1;
            Some(&self.input[start..end])
        } else {
            self.finished = true;
            let start = self.cursor;
            Some(&self.input[start..])
        }
    }
}

fn parse_bool(s: &str) -> bool {
    if s == "true" {
        return true;
    }
    if s == "false" {
        return false;
    }
    s.parse::<i32>().unwrap_or(0) != 0
}

/// Errors that can occur when decoding CCDB vault payloads.
#[derive(Error, Debug)]
pub enum CCDBDataError {
    /// Failed to parse data because the number of cells was not divisible by the number of columns.
    #[error("column count mismatch (expected {expected}, found {found})")]
    ColumnCountMismatch {
        /// The total expected number of cells.
        expected: usize,
        /// The number of cells found while parsing.
        found: usize,
    },
    /// Failed to parse a cell to the given type.
    #[error("parse error at row {row}, column {column} ({column_type}): {text:?}")]
    ParseError {
        /// The column index of the cell.
        column: usize,
        /// The row index of the cell.
        row: usize,
        /// The expected column type for the cell.
        column_type: ColumnType,
        /// The unparsed contents of the cell.
        text: String,
    },
    /// Failed to retrieve a row due to an out-of-bounds index.
    #[error("row index {requested} out of bounds (n_rows={n_rows})")]
    RowOutOfBounds {
        /// The requested index.
        requested: usize,
        /// The available number of rows.
        n_rows: usize,
    },
}
