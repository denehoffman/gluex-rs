use crate::models::{ColumnMeta, ColumnType};
use std::collections::HashMap;
use thiserror::Error;

#[derive(Debug, Clone)]
pub enum Column {
    Int(Vec<i32>),
    UInt(Vec<u32>),
    Long(Vec<i64>),
    ULong(Vec<u64>),
    Double(Vec<f64>),
    Bool(Vec<bool>),
    String(Vec<String>),
}

impl Column {
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
}

#[derive(Debug, Copy, Clone)]
pub enum Value<'a> {
    Int(&'a i32),
    UInt(&'a u32),
    Long(&'a i64),
    ULong(&'a u64),
    Double(&'a f64),
    Bool(&'a bool),
    String(&'a str),
}
impl<'a> Value<'a> {
    pub fn as_int(self) -> Option<i32> {
        if let Value::Int(v) = self {
            Some(*v)
        } else {
            None
        }
    }

    pub fn as_uint(self) -> Option<u32> {
        if let Value::UInt(v) = self {
            Some(*v)
        } else {
            None
        }
    }

    pub fn as_long(self) -> Option<i64> {
        if let Value::Long(v) = self {
            Some(*v)
        } else {
            None
        }
    }

    pub fn as_ulong(self) -> Option<u64> {
        if let Value::ULong(v) = self {
            Some(*v)
        } else {
            None
        }
    }

    pub fn as_double(self) -> Option<f64> {
        if let Value::Double(v) = self {
            Some(*v)
        } else {
            None
        }
    }

    pub fn as_bool(self) -> Option<bool> {
        if let Value::Bool(v) = self {
            Some(*v)
        } else {
            None
        }
    }

    pub fn as_str(self) -> Option<&'a str> {
        if let Value::String(v) = self {
            Some(v)
        } else {
            None
        }
    }
}

pub struct RowView<'a> {
    pub values: HashMap<&'a str, Value<'a>>,
}
impl<'a> RowView<'a> {
    pub fn get(&self, name: &str) -> Option<Value<'a>> {
        self.values.get(name).copied()
    }
    pub fn get_int(&self, name: &str) -> Option<i32> {
        self.get(name)?.as_int()
    }
    pub fn get_uint(&self, name: &str) -> Option<u32> {
        self.get(name)?.as_uint()
    }
    pub fn get_long(&self, name: &str) -> Option<i64> {
        self.get(name)?.as_long()
    }
    pub fn get_ulong(&self, name: &str) -> Option<u64> {
        self.get(name)?.as_ulong()
    }
    pub fn get_double(&self, name: &str) -> Option<f64> {
        self.get(name)?.as_double()
    }
    pub fn get_string(&self, name: &str) -> Option<&'a str> {
        self.get(name)?.as_str()
    }
    pub fn get_bool(&self, name: &str) -> Option<bool> {
        self.get(name)?.as_bool()
    }
}

#[derive(Debug, Clone)]
pub struct ColumnDef {
    pub index: usize,
    pub name: String,
    pub column_type: ColumnType,
}

pub struct Data {
    nrows: usize,
    ncolumns: usize,
    column_names: Vec<String>,
    column_indices: HashMap<String, usize>,
    column_types: Vec<ColumnType>,
    columns: Vec<Column>,
}

impl Data {
    pub fn from_vault(
        vault: &str,
        columns: &[ColumnMeta],
        nrows: usize,
    ) -> Result<Self, CCDBDataError> {
        let ncols = columns.len();
        let expected_cells = nrows * ncols;

        let mut cols_sorted = columns.to_vec();
        cols_sorted.sort_unstable_by_key(|c| c.order);

        let column_names: Vec<String> = cols_sorted
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

        let column_types: Vec<ColumnType> = cols_sorted.iter().map(|c| c.column_type).collect();
        let column_indices: HashMap<String, usize> = column_names
            .iter()
            .enumerate()
            .map(|(idx, name)| (name.clone(), idx))
            .collect();

        let mut column_vecs: Vec<Column> = column_types
            .iter()
            .map(|t| match t {
                ColumnType::Int => Column::Int(Vec::with_capacity(nrows)),
                ColumnType::UInt => Column::UInt(Vec::with_capacity(nrows)),
                ColumnType::Long => Column::Long(Vec::with_capacity(nrows)),
                ColumnType::ULong => Column::ULong(Vec::with_capacity(nrows)),
                ColumnType::Double => Column::Double(Vec::with_capacity(nrows)),
                ColumnType::String => Column::String(Vec::with_capacity(nrows)),
                ColumnType::Bool => Column::Bool(Vec::with_capacity(nrows)),
            })
            .collect();
        let mut raw_iter = vault.split('|');
        for idx in 0..expected_cells {
            let raw = match raw_iter.next() {
                Some(raw) => raw,
                None => {
                    return Err(CCDBDataError::ColumnCountMismatch {
                        expected: expected_cells,
                        found: idx,
                    })
                }
            };
            let row = idx / ncols;
            let col = idx % ncols;
            let column_type = column_types[col];

            match (&mut column_vecs[col], column_type) {
                (Column::Int(vec), ColumnType::Int) => {
                    vec.push(raw.parse().map_err(|_| CCDBDataError::ParseError {
                        column: col,
                        row,
                        column_type,
                        text: raw.to_string(),
                    })?)
                }
                (Column::UInt(vec), ColumnType::UInt) => {
                    vec.push(raw.parse().map_err(|_| CCDBDataError::ParseError {
                        column: col,
                        row,
                        column_type,
                        text: raw.to_string(),
                    })?)
                }
                (Column::Long(vec), ColumnType::Long) => {
                    vec.push(raw.parse().map_err(|_| CCDBDataError::ParseError {
                        column: col,
                        row,
                        column_type,
                        text: raw.to_string(),
                    })?)
                }
                (Column::ULong(vec), ColumnType::ULong) => {
                    vec.push(raw.parse().map_err(|_| CCDBDataError::ParseError {
                        column: col,
                        row,
                        column_type,
                        text: raw.to_string(),
                    })?)
                }
                (Column::Double(vec), ColumnType::Double) => {
                    vec.push(raw.parse().map_err(|_| CCDBDataError::ParseError {
                        column: col,
                        row,
                        column_type,
                        text: raw.to_string(),
                    })?)
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
        if let Some(_) = raw_iter.next() {
            let found = expected_cells + 1 + raw_iter.count();
            return Err(CCDBDataError::ColumnCountMismatch {
                expected: expected_cells,
                found,
            });
        }
        Ok(Data {
            nrows,
            ncolumns: ncols,
            column_names,
            column_indices,
            column_types,
            columns: column_vecs,
        })
    }

    pub fn nrows(&self) -> usize {
        self.nrows
    }
    pub fn ncolumns(&self) -> usize {
        self.ncolumns
    }
    pub fn column_names(&self) -> &[String] {
        &self.column_names
    }

    pub fn column_types(&self) -> &[ColumnType] {
        &self.column_types
    }

    pub fn column_by_index(&self, idx: usize) -> Option<&Column> {
        self.columns.get(idx)
    }

    pub fn column_by_name(&self, name: &str) -> Option<&Column> {
        self.column_indices
            .get(name)
            .and_then(|idx| self.columns.get(*idx))
    }

    pub fn value(&self, row: usize, column: usize) -> Option<Value<'_>> {
        if row >= self.nrows || column >= self.ncolumns {
            return None;
        }
        Some(self.columns[column].row(row))
    }
    pub fn get_named_int(&self, name: &str, row: usize) -> Option<i32> {
        self.column_by_name(name)?.row(row).as_int()
    }
    pub fn get_named_uint(&self, name: &str, row: usize) -> Option<u32> {
        self.column_by_name(name)?.row(row).as_uint()
    }
    pub fn get_named_long(&self, name: &str, row: usize) -> Option<i64> {
        self.column_by_name(name)?.row(row).as_long()
    }
    pub fn get_named_ulong(&self, name: &str, row: usize) -> Option<u64> {
        self.column_by_name(name)?.row(row).as_ulong()
    }
    pub fn get_named_double(&self, name: &str, row: usize) -> Option<f64> {
        self.column_by_name(name)?.row(row).as_double()
    }
    pub fn get_named_string(&self, name: &str, row: usize) -> Option<&str> {
        self.column_by_name(name)?.row(row).as_str()
    }
    pub fn get_named_bool(&self, name: &str, row: usize) -> Option<bool> {
        self.column_by_name(name)?.row(row).as_bool()
    }

    pub fn get_int(&self, column: usize, row: usize) -> Option<i32> {
        self.value(row, column)?.as_int()
    }
    pub fn get_uint(&self, column: usize, row: usize) -> Option<u32> {
        self.value(row, column)?.as_uint()
    }
    pub fn get_long(&self, column: usize, row: usize) -> Option<i64> {
        self.value(row, column)?.as_long()
    }
    pub fn get_ulong(&self, column: usize, row: usize) -> Option<u64> {
        self.value(row, column)?.as_ulong()
    }
    pub fn get_double(&self, column: usize, row: usize) -> Option<f64> {
        self.value(row, column)?.as_double()
    }
    pub fn get_string(&self, column: usize, row: usize) -> Option<&str> {
        self.value(row, column)?.as_str()
    }
    pub fn get_bool(&self, column: usize, row: usize) -> Option<bool> {
        self.value(row, column)?.as_bool()
    }

    pub fn row(&self, row: usize) -> Result<RowView<'_>, CCDBDataError> {
        if row >= self.nrows {
            return Err(CCDBDataError::RowOutOfBounds {
                requested: row,
                nrows: self.nrows,
            });
        }
        Ok(RowView {
            values: self
                .column_names
                .iter()
                .zip(self.columns.iter())
                .map(|(name, col)| (name.as_str(), col.row(row)))
                .collect(),
        })
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

#[derive(Error, Debug)]
pub enum CCDBDataError {
    #[error("column count mismatch (expected {expected}, found {found})")]
    ColumnCountMismatch { expected: usize, found: usize },
    #[error("parse error at row {row}, column {column} ({column_type}): {text:?}")]
    ParseError {
        column: usize,
        row: usize,
        column_type: ColumnType,
        text: String,
    },
    #[error("row index {requested} out of bounds (nrows={nrows})")]
    RowOutOfBounds { requested: usize, nrows: usize },
}
