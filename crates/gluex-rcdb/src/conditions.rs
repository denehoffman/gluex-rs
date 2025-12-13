use std::{fmt, sync::Arc};

use chrono::{DateTime, Utc};
use rusqlite::types::Value;

use crate::{models::ValueType, RCDBError};

/// Condition expression used to filter RCDB queries.
#[derive(Debug, Clone)]
pub struct Expr(Arc<ExprInner>);

#[derive(Debug, Clone)]
enum ExprInner {
    True,
    Comparison(Comparison),
    Group { kind: GroupKind, clauses: Vec<Expr> },
    Not(Expr),
}

#[derive(Debug, Clone)]
pub(crate) struct Comparison {
    field: String,
    value_type: ValueType,
    operator: Operator,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum GroupKind {
    And,
    Or,
}

#[derive(Debug, Clone)]
enum Operator {
    Bool(bool),
    IntEquals(i64),
    IntNotEquals(i64),
    IntGt(i64),
    IntGe(i64),
    IntLt(i64),
    IntLe(i64),
    FloatEquals(f64),
    FloatGt(f64),
    FloatGe(f64),
    FloatLt(f64),
    FloatLe(f64),
    StringEquals(String),
    StringNotEquals(String),
    StringIn(Vec<String>),
    StringContains(String),
    TimeEquals(DateTime<Utc>),
    TimeGt(DateTime<Utc>),
    TimeGe(DateTime<Utc>),
    TimeLt(DateTime<Utc>),
    TimeLe(DateTime<Utc>),
    Exists,
}

impl Expr {
    fn new(inner: ExprInner) -> Self {
        Self(Arc::new(inner))
    }

    pub(crate) fn referenced_conditions(&self, out: &mut Vec<String>) {
        match self.0.as_ref() {
            ExprInner::True => {}
            ExprInner::Comparison(cmp) => out.push(cmp.field.clone()),
            ExprInner::Group { clauses, .. } => {
                for clause in clauses {
                    clause.referenced_conditions(out);
                }
            }
            ExprInner::Not(inner) => inner.referenced_conditions(out),
        }
    }

    pub(crate) fn to_sql(
        &self,
        alias_lookup: &dyn Fn(&str) -> Option<(String, ValueType)>,
        params: &mut Vec<Value>,
    ) -> Result<String, RCDBError> {
        match self.0.as_ref() {
            ExprInner::True => Ok("1 = 1".to_string()),
            ExprInner::Comparison(cmp) => cmp.to_sql(alias_lookup, params),
            ExprInner::Group { kind, clauses } => {
                let mut rendered: Vec<String> = Vec::new();
                for clause in clauses {
                    rendered.push(clause.to_sql(alias_lookup, params)?);
                }
                if rendered.is_empty() {
                    return Ok("1 = 1".to_string());
                }
                let joiner = match kind {
                    GroupKind::And => " AND ",
                    GroupKind::Or => " OR ",
                };
                Ok(format!("({})", rendered.join(joiner)))
            }
            ExprInner::Not(inner) => Ok(format!("NOT ({})", inner.to_sql(alias_lookup, params)?)),
        }
    }

    /// Negates the expression.
    #[must_use]
    pub fn negate(self) -> Expr {
        Expr::new(ExprInner::Not(self))
    }

    fn fmt_with(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self.0.as_ref() {
            ExprInner::True => write!(f, "TRUE"),
            ExprInner::Comparison(cmp) => write!(f, "{cmp}"),
            ExprInner::Group { kind, clauses } => {
                let joiner = match kind {
                    GroupKind::And => " AND ",
                    GroupKind::Or => " OR ",
                };
                let mut parts = Vec::new();
                for clause in clauses {
                    parts.push(clause.to_string());
                }
                write!(f, "({})", parts.join(joiner))
            }
            ExprInner::Not(inner) => {
                write!(f, "NOT ({inner})")
            }
        }
    }
}

impl fmt::Display for Expr {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        self.fmt_with(f)
    }
}

impl Comparison {
    fn to_sql(
        &self,
        alias_lookup: &dyn Fn(&str) -> Option<(String, ValueType)>,
        params: &mut Vec<Value>,
    ) -> Result<String, RCDBError> {
        let (alias, actual_type) = alias_lookup(&self.field)
            .ok_or_else(|| RCDBError::ConditionTypeNotFound(self.field.clone()))?;
        if actual_type != self.value_type {
            return Err(RCDBError::ConditionTypeMismatch {
                condition_name: self.field.clone(),
                expected: self.value_type,
                actual: actual_type,
            });
        }
        Ok(match &self.operator {
            Operator::Bool(true) => format!("{alias}.bool_value = 1"),
            Operator::Bool(false) => format!("{alias}.bool_value = 0"),
            Operator::IntEquals(v) => {
                push_param(params, &alias, "int_value", "=", Value::Integer(*v))
            }
            Operator::IntNotEquals(v) => {
                push_param(params, &alias, "int_value", "!=", Value::Integer(*v))
            }
            Operator::IntGt(v) => push_param(params, &alias, "int_value", ">", Value::Integer(*v)),
            Operator::IntGe(v) => push_param(params, &alias, "int_value", ">=", Value::Integer(*v)),
            Operator::IntLt(v) => push_param(params, &alias, "int_value", "<", Value::Integer(*v)),
            Operator::IntLe(v) => push_param(params, &alias, "int_value", "<=", Value::Integer(*v)),
            Operator::FloatEquals(v) => {
                push_param(params, &alias, "float_value", "=", Value::Real(*v))
            }
            Operator::FloatGt(v) => push_param(params, &alias, "float_value", ">", Value::Real(*v)),
            Operator::FloatGe(v) => {
                push_param(params, &alias, "float_value", ">=", Value::Real(*v))
            }
            Operator::FloatLt(v) => push_param(params, &alias, "float_value", "<", Value::Real(*v)),
            Operator::FloatLe(v) => {
                push_param(params, &alias, "float_value", "<=", Value::Real(*v))
            }
            Operator::StringEquals(v) => {
                push_param(params, &alias, "text_value", "=", Value::Text(v.clone()))
            }
            Operator::StringNotEquals(v) => {
                push_param(params, &alias, "text_value", "!=", Value::Text(v.clone()))
            }
            Operator::StringIn(values) => {
                if values.is_empty() {
                    return Ok("1 = 0".to_string());
                }
                let mut placeholders = Vec::with_capacity(values.len());
                for value in values {
                    params.push(Value::Text(value.clone()));
                    placeholders.push("?");
                }
                format!("{}.text_value IN ({})", alias, placeholders.join(", "))
            }
            Operator::StringContains(substr) => {
                params.push(Value::Text(substr.clone()));
                format!("INSTR({alias}.text_value, ?) > 0")
            }
            Operator::TimeEquals(v) => push_time(params, &alias, "=", v),
            Operator::TimeGt(v) => push_time(params, &alias, ">", v),
            Operator::TimeGe(v) => push_time(params, &alias, ">=", v),
            Operator::TimeLt(v) => push_time(params, &alias, "<", v),
            Operator::TimeLe(v) => push_time(params, &alias, "<=", v),
            Operator::Exists => format!("{}.{} IS NOT NULL", alias, self.value_type.column_name()),
        })
    }

    fn fmt_operator(&self) -> String {
        match &self.operator {
            Operator::Bool(v) => format!("{v}"),
            Operator::IntEquals(v)
            | Operator::IntNotEquals(v)
            | Operator::IntGt(v)
            | Operator::IntGe(v)
            | Operator::IntLt(v)
            | Operator::IntLe(v) => v.to_string(),
            Operator::FloatEquals(v)
            | Operator::FloatGt(v)
            | Operator::FloatGe(v)
            | Operator::FloatLt(v)
            | Operator::FloatLe(v) => format!("{v}"),
            Operator::StringEquals(v)
            | Operator::StringNotEquals(v)
            | Operator::StringContains(v) => format!("{v:?}"),
            Operator::TimeEquals(v)
            | Operator::TimeGt(v)
            | Operator::TimeGe(v)
            | Operator::TimeLt(v)
            | Operator::TimeLe(v) => format!("{v:?}"),
            Operator::StringIn(values) => {
                let rendered: Vec<String> = values.iter().map(|v| format!("{v:?}")).collect();
                format!("[{}]", rendered.join(", "))
            }
            Operator::Exists => String::new(),
        }
    }
}

impl fmt::Display for Comparison {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        let field = &self.field;
        match &self.operator {
            Operator::Bool(true) => write!(f, "{field} IS TRUE"),
            Operator::Bool(false) => write!(f, "{field} IS FALSE"),
            Operator::IntEquals(_)
            | Operator::FloatEquals(_)
            | Operator::StringEquals(_)
            | Operator::TimeEquals(_) => {
                write!(f, "{} == {}", field, self.fmt_operator())
            }
            Operator::IntNotEquals(_) | Operator::StringNotEquals(_) => {
                write!(f, "{} != {}", field, self.fmt_operator())
            }
            Operator::IntGt(_) | Operator::FloatGt(_) | Operator::TimeGt(_) => {
                write!(f, "{} > {}", field, self.fmt_operator())
            }
            Operator::IntGe(_) | Operator::FloatGe(_) | Operator::TimeGe(_) => {
                write!(f, "{} >= {}", field, self.fmt_operator())
            }
            Operator::IntLt(_) | Operator::FloatLt(_) | Operator::TimeLt(_) => {
                write!(f, "{} < {}", field, self.fmt_operator())
            }
            Operator::IntLe(_) | Operator::FloatLe(_) | Operator::TimeLe(_) => {
                write!(f, "{} <= {}", field, self.fmt_operator())
            }
            Operator::StringIn(values) => {
                let rendered: Vec<String> = values.iter().map(|v| format!("{v:?}")).collect();
                write!(f, "{} IN [{}]", field, rendered.join(", "))
            }
            Operator::StringContains(_) => {
                write!(f, "{} CONTAINS {}", field, self.fmt_operator())
            }
            Operator::Exists => write!(f, "{field} EXISTS"),
        }
    }
}

fn push_param(
    params: &mut Vec<Value>,
    alias: &str,
    column: &str,
    op: &str,
    value: Value,
) -> String {
    params.push(value);
    format!("{alias}.{column} {op} ?")
}

fn push_time(params: &mut Vec<Value>, alias: &str, op: &str, value: &DateTime<Utc>) -> String {
    params.push(Value::Text(format_time(value)));
    format!("{alias}.time_value {op} ?")
}

fn format_time(value: &DateTime<Utc>) -> String {
    value.format("%Y-%m-%d %H:%M:%S").to_string()
}

/// Begins constructing an integer comparison against the named condition.
pub fn int_cond(name: impl Into<String>) -> IntField {
    IntField { field: name.into() }
}

/// Begins constructing a floating-point comparison against the named condition.
pub fn float_cond(name: impl Into<String>) -> FloatField {
    FloatField { field: name.into() }
}

/// Begins constructing a string comparison against the named condition.
pub fn string_cond(name: impl Into<String>) -> StringField {
    StringField { field: name.into() }
}

/// Begins constructing a boolean comparison against the named condition.
pub fn bool_cond(name: impl Into<String>) -> BoolField {
    BoolField { field: name.into() }
}

/// Begins constructing a timestamp comparison against the named condition.
pub fn time_cond(name: impl Into<String>) -> TimeField {
    TimeField { field: name.into() }
}

/// Combines the supplied expressions with logical AND semantics.
pub fn all<I>(iter: I) -> Expr
where
    I: IntoIterator<Item = Expr>,
{
    let clauses: Vec<Expr> = iter.into_iter().collect();
    if clauses.len() <= 1 {
        clauses
            .into_iter()
            .next()
            .unwrap_or_else(|| Expr::new(ExprInner::True))
    } else {
        Expr::new(ExprInner::Group {
            kind: GroupKind::And,
            clauses,
        })
    }
}

/// Combines the supplied expressions with logical OR semantics.
pub fn any<I>(iter: I) -> Expr
where
    I: IntoIterator<Item = Expr>,
{
    let clauses: Vec<Expr> = iter.into_iter().collect();
    if clauses.len() <= 1 {
        clauses
            .into_iter()
            .next()
            .unwrap_or_else(|| Expr::new(ExprInner::True))
    } else {
        Expr::new(ExprInner::Group {
            kind: GroupKind::Or,
            clauses,
        })
    }
}

/// Builder used to create integer comparison expressions.
#[derive(Clone)]
pub struct IntField {
    field: String,
}
impl IntField {
    /// Matches when the condition is exactly equal to `value`.
    #[must_use]
    pub fn eq(self, value: i64) -> Expr {
        Expr::new(ExprInner::Comparison(Comparison {
            field: self.field,
            value_type: ValueType::Int,
            operator: Operator::IntEquals(value),
        }))
    }
    /// Matches when the condition is not equal to `value`.
    #[must_use]
    pub fn ne(self, value: i64) -> Expr {
        Expr::new(ExprInner::Comparison(Comparison {
            field: self.field,
            value_type: ValueType::Int,
            operator: Operator::IntNotEquals(value),
        }))
    }
    /// Matches when the condition is strictly greater than `value`.
    #[must_use]
    pub fn gt(self, value: i64) -> Expr {
        Expr::new(ExprInner::Comparison(Comparison {
            field: self.field,
            value_type: ValueType::Int,
            operator: Operator::IntGt(value),
        }))
    }
    /// Matches when the condition is greater than or equal to `value`.
    #[must_use]
    pub fn ge(self, value: i64) -> Expr {
        Expr::new(ExprInner::Comparison(Comparison {
            field: self.field,
            value_type: ValueType::Int,
            operator: Operator::IntGe(value),
        }))
    }
    /// Matches when the condition is strictly less than `value`.
    #[must_use]
    pub fn lt(self, value: i64) -> Expr {
        Expr::new(ExprInner::Comparison(Comparison {
            field: self.field,
            value_type: ValueType::Int,
            operator: Operator::IntLt(value),
        }))
    }
    /// Matches when the condition is less than or equal to `value`.
    #[must_use]
    pub fn le(self, value: i64) -> Expr {
        Expr::new(ExprInner::Comparison(Comparison {
            field: self.field,
            value_type: ValueType::Int,
            operator: Operator::IntLe(value),
        }))
    }
}

/// Builder used to create floating-point comparison expressions.
#[derive(Clone)]
pub struct FloatField {
    field: String,
}
impl FloatField {
    /// Matches when the condition is exactly equal to `value`.
    #[must_use]
    pub fn eq(self, value: f64) -> Expr {
        Expr::new(ExprInner::Comparison(Comparison {
            field: self.field,
            value_type: ValueType::Float,
            operator: Operator::FloatEquals(value),
        }))
    }
    /// Matches when the condition is strictly greater than `value`.
    #[must_use]
    pub fn gt(self, value: f64) -> Expr {
        Expr::new(ExprInner::Comparison(Comparison {
            field: self.field,
            value_type: ValueType::Float,
            operator: Operator::FloatGt(value),
        }))
    }
    /// Matches when the condition is greater than or equal to `value`.
    #[must_use]
    pub fn ge(self, value: f64) -> Expr {
        Expr::new(ExprInner::Comparison(Comparison {
            field: self.field,
            value_type: ValueType::Float,
            operator: Operator::FloatGe(value),
        }))
    }
    /// Matches when the condition is strictly less than `value`.
    #[must_use]
    pub fn lt(self, value: f64) -> Expr {
        Expr::new(ExprInner::Comparison(Comparison {
            field: self.field,
            value_type: ValueType::Float,
            operator: Operator::FloatLt(value),
        }))
    }
    /// Matches when the condition is less than or equal to `value`.
    #[must_use]
    pub fn le(self, value: f64) -> Expr {
        Expr::new(ExprInner::Comparison(Comparison {
            field: self.field,
            value_type: ValueType::Float,
            operator: Operator::FloatLe(value),
        }))
    }
}

/// Builder used to create string comparison expressions.
#[derive(Clone)]
pub struct StringField {
    field: String,
}
impl StringField {
    /// Matches when the condition is exactly equal to `value`.
    pub fn eq(self, value: impl Into<String>) -> Expr {
        Expr::new(ExprInner::Comparison(Comparison {
            field: self.field,
            value_type: ValueType::String,
            operator: Operator::StringEquals(value.into()),
        }))
    }
    /// Matches when the condition is not equal to `value`.
    pub fn ne(self, value: impl Into<String>) -> Expr {
        Expr::new(ExprInner::Comparison(Comparison {
            field: self.field,
            value_type: ValueType::String,
            operator: Operator::StringNotEquals(value.into()),
        }))
    }
    /// Matches when the condition string is one of `values`.
    pub fn isin<I, S>(self, values: I) -> Expr
    where
        I: IntoIterator<Item = S>,
        S: Into<String>,
    {
        let list: Vec<String> = values.into_iter().map(std::convert::Into::into).collect();
        Expr::new(ExprInner::Comparison(Comparison {
            field: self.field,
            value_type: ValueType::String,
            operator: Operator::StringIn(list),
        }))
    }
    /// Matches when the condition string contains `value` as a substring.
    pub fn contains(self, value: impl Into<String>) -> Expr {
        Expr::new(ExprInner::Comparison(Comparison {
            field: self.field,
            value_type: ValueType::String,
            operator: Operator::StringContains(value.into()),
        }))
    }
}

/// Builder used to create boolean comparison expressions.
#[derive(Clone)]
pub struct BoolField {
    field: String,
}
impl BoolField {
    /// Matches when the condition is explicitly true.
    #[must_use]
    pub fn is_true(self) -> Expr {
        Expr::new(ExprInner::Comparison(Comparison {
            field: self.field,
            value_type: ValueType::Bool,
            operator: Operator::Bool(true),
        }))
    }
    /// Matches when the condition is explicitly false.
    #[must_use]
    pub fn is_false(self) -> Expr {
        Expr::new(ExprInner::Comparison(Comparison {
            field: self.field,
            value_type: ValueType::Bool,
            operator: Operator::Bool(false),
        }))
    }
    /// Matches when the condition exists for the run regardless of value.
    #[must_use]
    pub fn exists(self) -> Expr {
        Expr::new(ExprInner::Comparison(Comparison {
            field: self.field,
            value_type: ValueType::Bool,
            operator: Operator::Exists,
        }))
    }
}

/// Builder used to create timestamp comparison expressions.
#[derive(Clone)]
pub struct TimeField {
    field: String,
}
impl TimeField {
    /// Matches when the condition timestamp equals `value`.
    #[must_use]
    pub fn eq(self, value: DateTime<Utc>) -> Expr {
        Expr::new(ExprInner::Comparison(Comparison {
            field: self.field,
            value_type: ValueType::Time,
            operator: Operator::TimeEquals(value),
        }))
    }
    /// Matches when the condition timestamp is strictly greater than `value`.
    #[must_use]
    pub fn gt(self, value: DateTime<Utc>) -> Expr {
        Expr::new(ExprInner::Comparison(Comparison {
            field: self.field,
            value_type: ValueType::Time,
            operator: Operator::TimeGt(value),
        }))
    }
    /// Matches when the condition timestamp is greater than or equal to `value`.
    #[must_use]
    pub fn ge(self, value: DateTime<Utc>) -> Expr {
        Expr::new(ExprInner::Comparison(Comparison {
            field: self.field,
            value_type: ValueType::Time,
            operator: Operator::TimeGe(value),
        }))
    }
    /// Matches when the condition timestamp is strictly less than `value`.
    #[must_use]
    pub fn lt(self, value: DateTime<Utc>) -> Expr {
        Expr::new(ExprInner::Comparison(Comparison {
            field: self.field,
            value_type: ValueType::Time,
            operator: Operator::TimeLt(value),
        }))
    }
    /// Matches when the condition timestamp is less than or equal to `value`.
    #[must_use]
    pub fn le(self, value: DateTime<Utc>) -> Expr {
        Expr::new(ExprInner::Comparison(Comparison {
            field: self.field,
            value_type: ValueType::Time,
            operator: Operator::TimeLe(value),
        }))
    }
}

/// Trait describing types that can be converted into a list of expressions.
pub trait IntoExprList {
    /// Convert the input into a vector of expressions.
    fn into_list(self) -> Vec<Expr>;
}

impl IntoExprList for Expr {
    fn into_list(self) -> Vec<Expr> {
        vec![self]
    }
}

impl IntoExprList for Vec<Expr> {
    fn into_list(self) -> Vec<Expr> {
        self
    }
}

impl IntoExprList for &[Expr] {
    fn into_list(self) -> Vec<Expr> {
        self.to_vec()
    }
}

impl IntoExprList for &Vec<Expr> {
    fn into_list(self) -> Vec<Expr> {
        self.clone()
    }
}

/// Convenience functions for referencing built-in alias expressions directly.
pub mod aliases {
    use gluex_core::run_periods::RunPeriod;

    use super::{all, float_cond, int_cond, string_cond, Expr};

    /// Returns the reusable expression for the `is_production` alias.
    #[must_use]
    pub fn is_production() -> Expr {
        all([
            string_cond("run_type").isin([
                "hd_all.tsg",
                "hd_all.tsg_ps",
                "hd_all.bcal_fcal_st.tsg",
            ]),
            float_cond("beam_current").gt(2.0),
            int_cond("event_count").gt(500_000),
            float_cond("solenoid_current").gt(100.0),
            string_cond("collimator_diameter").ne("Blocking"),
        ])
    }

    /// Returns the reusable expression for the `is_2018production` alias.
    #[must_use]
    pub fn is_2018production() -> Expr {
        all([
            string_cond("daq_run").eq("PHYSICS"),
            float_cond("beam_current").gt(2.0),
            int_cond("event_count").gt(10_000_000),
            float_cond("solenoid_current").gt(100.0),
            string_cond("collimator_diameter").ne("Blocking"),
        ])
    }

    /// Returns the reusable expression for the `is_primex_production` alias.
    #[must_use]
    pub fn is_primex_production() -> Expr {
        all([
            string_cond("daq_run").eq("PHYSICS_PRIMEX"),
            int_cond("event_count").gt(1_000_000),
            string_cond("collimator_diameter").ne("Blocking"),
        ])
    }

    /// Returns the reusable expression for the `is_dirc_production` alias.
    #[must_use]
    pub fn is_dirc_production() -> Expr {
        all([
            string_cond("daq_run").eq("PHYSICS_DIRC"),
            float_cond("beam_current").gt(2.0),
            int_cond("event_count").gt(5_000_000),
            float_cond("solenoid_current").gt(100.0),
            string_cond("collimator_diameter").ne("Blocking"),
        ])
    }

    /// Returns the reusable expression for the `is_src_production` alias.
    #[must_use]
    pub fn is_src_production() -> Expr {
        all([
            string_cond("daq_run").eq("PHYSICS_SRC"),
            float_cond("beam_current").gt(2.0),
            int_cond("event_count").gt(5_000_000),
            float_cond("solenoid_current").gt(100.0),
            string_cond("collimator_diameter").ne("Blocking"),
        ])
    }

    /// Returns the reusable expression for the `is_cpp_production` alias.
    #[must_use]
    pub fn is_cpp_production() -> Expr {
        all([
            string_cond("daq_run").eq("PHYSICS_CPP"),
            float_cond("beam_current").gt(2.0),
            int_cond("event_count").gt(5_000_000),
            float_cond("solenoid_current").gt(100.0),
            string_cond("collimator_diameter").ne("Blocking"),
        ])
    }

    /// Returns the reusable expression for the `is_production_long` alias.
    #[must_use]
    pub fn is_production_long() -> Expr {
        all([
            string_cond("daq_run").eq("PHYSICS_raw"),
            float_cond("beam_current").gt(2.0),
            int_cond("event_count").gt(5_000_000),
            float_cond("solenoid_current").gt(100.0),
            string_cond("collimator_diameter").ne("Blocking"),
        ])
    }

    /// Returns the reusable expression for the `is_cosmic` alias.
    #[must_use]
    pub fn is_cosmic() -> Expr {
        all([
            string_cond("run_config").contains("cosmic"),
            float_cond("beam_current").lt(1.0),
            int_cond("event_count").gt(5_000),
        ])
    }

    /// Returns the reusable expression for the `is_empty_target` alias.
    #[must_use]
    pub fn is_empty_target() -> Expr {
        string_cond("target_type").eq("EMPTY & Ready")
    }

    /// Returns the reusable expression for the `is_amorph_radiator` alias.
    #[must_use]
    pub fn is_amorph_radiator() -> Expr {
        float_cond("polarization_angle").lt(0.0)
    }

    /// Returns the reusable expression for the `is_coherent_beam` alias.
    #[must_use]
    pub fn is_coherent_beam() -> Expr {
        float_cond("polarization_angle").ge(0.0)
    }

    /// Returns the reusable expression for the `is_field_off` alias.
    #[must_use]
    pub fn is_field_off() -> Expr {
        float_cond("solenoid_current").lt(100.0)
    }

    /// Returns the reusable expression for the `is_field_on` alias.
    #[must_use]
    pub fn is_field_on() -> Expr {
        float_cond("solenoid_current").ge(100.0)
    }

    /// Returns the reusable expression for the `status_calibration` alias.
    #[must_use]
    pub fn status_calibration() -> Expr {
        int_cond("status").eq(3)
    }

    /// Returns the reusable expression for the `status_approved_long` alias.
    #[must_use]
    pub fn status_approved_long() -> Expr {
        int_cond("status").eq(2)
    }

    /// Returns the reusable expression for the `status_approved` alias.
    #[must_use]
    pub fn status_approved() -> Expr {
        int_cond("status").eq(1)
    }

    /// Returns the reusable expression for the `status_unchecked` alias.
    #[must_use]
    pub fn status_unchecked() -> Expr {
        int_cond("status").eq(-1)
    }

    /// Returns the reusable expression for the `status_reject` alias.
    #[must_use]
    pub fn status_reject() -> Expr {
        int_cond("status").eq(0)
    }

    /// Returns an expression which matches approved production runs for the given [`RunPeriod`].
    #[must_use]
    pub fn approved_production(run_period: RunPeriod) -> Expr {
        match run_period {
            RunPeriod::RP2016_02 | RunPeriod::RP2017_01 => {
                all([is_production(), status_approved()])
            }
            RunPeriod::RP2018_01 | RunPeriod::RP2018_08 => {
                all([is_2018production(), status_approved()])
            }
            RunPeriod::RP2019_11 => all([is_dirc_production(), status_approved()]),
            RunPeriod::RP2023_01 | RunPeriod::RP2025_01 => {
                all([is_dirc_production(), status_approved()])
            }
            _ => unimplemented!(),
        }
    }
}
