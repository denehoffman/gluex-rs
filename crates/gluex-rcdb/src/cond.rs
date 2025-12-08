use std::sync::Arc;

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
    pub fn not(self) -> Expr {
        Expr::new(ExprInner::Not(self))
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
            Operator::Bool(true) => format!("{}.bool_value = 1", alias),
            Operator::Bool(false) => format!("{}.bool_value = 0", alias),
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
                format!("INSTR({}.text_value, ?) > 0", alias)
            }
            Operator::TimeEquals(v) => push_time(params, &alias, "=", v),
            Operator::TimeGt(v) => push_time(params, &alias, ">", v),
            Operator::TimeGe(v) => push_time(params, &alias, ">=", v),
            Operator::TimeLt(v) => push_time(params, &alias, "<", v),
            Operator::TimeLe(v) => push_time(params, &alias, "<=", v),
            Operator::Exists => format!("{}.{} IS NOT NULL", alias, self.value_type.column_name()),
        })
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
    format!("{}.{} {} ?", alias, column, op)
}

fn push_time(params: &mut Vec<Value>, alias: &str, op: &str, value: &DateTime<Utc>) -> String {
    params.push(Value::Text(format_time(value)));
    format!("{}.time_value {} ?", alias, op)
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
    if clauses.is_empty() {
        Expr::new(ExprInner::True)
    } else if clauses.len() == 1 {
        clauses.into_iter().next().unwrap()
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
    if clauses.is_empty() {
        Expr::new(ExprInner::True)
    } else if clauses.len() == 1 {
        clauses.into_iter().next().unwrap()
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
    pub fn eq(self, value: i64) -> Expr {
        Expr::new(ExprInner::Comparison(Comparison {
            field: self.field,
            value_type: ValueType::Int,
            operator: Operator::IntEquals(value),
        }))
    }
    /// Matches when the condition is not equal to `value`.
    pub fn neq(self, value: i64) -> Expr {
        Expr::new(ExprInner::Comparison(Comparison {
            field: self.field,
            value_type: ValueType::Int,
            operator: Operator::IntNotEquals(value),
        }))
    }
    /// Matches when the condition is strictly greater than `value`.
    pub fn gt(self, value: i64) -> Expr {
        Expr::new(ExprInner::Comparison(Comparison {
            field: self.field,
            value_type: ValueType::Int,
            operator: Operator::IntGt(value),
        }))
    }
    /// Matches when the condition is greater than or equal to `value`.
    pub fn geq(self, value: i64) -> Expr {
        Expr::new(ExprInner::Comparison(Comparison {
            field: self.field,
            value_type: ValueType::Int,
            operator: Operator::IntGe(value),
        }))
    }
    /// Matches when the condition is strictly less than `value`.
    pub fn lt(self, value: i64) -> Expr {
        Expr::new(ExprInner::Comparison(Comparison {
            field: self.field,
            value_type: ValueType::Int,
            operator: Operator::IntLt(value),
        }))
    }
    /// Matches when the condition is less than or equal to `value`.
    pub fn leq(self, value: i64) -> Expr {
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
    pub fn eq(self, value: f64) -> Expr {
        Expr::new(ExprInner::Comparison(Comparison {
            field: self.field,
            value_type: ValueType::Float,
            operator: Operator::FloatEquals(value),
        }))
    }
    /// Matches when the condition is strictly greater than `value`.
    pub fn gt(self, value: f64) -> Expr {
        Expr::new(ExprInner::Comparison(Comparison {
            field: self.field,
            value_type: ValueType::Float,
            operator: Operator::FloatGt(value),
        }))
    }
    /// Matches when the condition is greater than or equal to `value`.
    pub fn geq(self, value: f64) -> Expr {
        Expr::new(ExprInner::Comparison(Comparison {
            field: self.field,
            value_type: ValueType::Float,
            operator: Operator::FloatGe(value),
        }))
    }
    /// Matches when the condition is strictly less than `value`.
    pub fn lt(self, value: f64) -> Expr {
        Expr::new(ExprInner::Comparison(Comparison {
            field: self.field,
            value_type: ValueType::Float,
            operator: Operator::FloatLt(value),
        }))
    }
    /// Matches when the condition is less than or equal to `value`.
    pub fn leq(self, value: f64) -> Expr {
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
    pub fn neq(self, value: impl Into<String>) -> Expr {
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
        let list: Vec<String> = values.into_iter().map(|v| v.into()).collect();
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
    pub fn is_true(self) -> Expr {
        Expr::new(ExprInner::Comparison(Comparison {
            field: self.field,
            value_type: ValueType::Bool,
            operator: Operator::Bool(true),
        }))
    }
    /// Matches when the condition is explicitly false.
    pub fn is_false(self) -> Expr {
        Expr::new(ExprInner::Comparison(Comparison {
            field: self.field,
            value_type: ValueType::Bool,
            operator: Operator::Bool(false),
        }))
    }
    /// Matches when the condition exists for the run regardless of value.
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
    pub fn eq(self, value: DateTime<Utc>) -> Expr {
        Expr::new(ExprInner::Comparison(Comparison {
            field: self.field,
            value_type: ValueType::Time,
            operator: Operator::TimeEquals(value),
        }))
    }
    /// Matches when the condition timestamp is strictly greater than `value`.
    pub fn gt(self, value: DateTime<Utc>) -> Expr {
        Expr::new(ExprInner::Comparison(Comparison {
            field: self.field,
            value_type: ValueType::Time,
            operator: Operator::TimeGt(value),
        }))
    }
    /// Matches when the condition timestamp is greater than or equal to `value`.
    pub fn geq(self, value: DateTime<Utc>) -> Expr {
        Expr::new(ExprInner::Comparison(Comparison {
            field: self.field,
            value_type: ValueType::Time,
            operator: Operator::TimeGe(value),
        }))
    }
    /// Matches when the condition timestamp is strictly less than `value`.
    pub fn lt(self, value: DateTime<Utc>) -> Expr {
        Expr::new(ExprInner::Comparison(Comparison {
            field: self.field,
            value_type: ValueType::Time,
            operator: Operator::TimeLt(value),
        }))
    }
    /// Matches when the condition timestamp is less than or equal to `value`.
    pub fn lte(self, value: DateTime<Utc>) -> Expr {
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

impl<'a> IntoExprList for &'a [Expr] {
    fn into_list(self) -> Vec<Expr> {
        self.to_vec()
    }
}

impl<'a> IntoExprList for &'a Vec<Expr> {
    fn into_list(self) -> Vec<Expr> {
        self.clone()
    }
}

/// Named expression shortcuts that capture common selection logic.
#[derive(Copy, Clone)]
/// Named expression shortcut used for reusable filters.
pub struct ConditionAlias {
    /// Alias name used when referencing this expression.
    pub name: &'static str,
    /// Human-readable comment describing the alias.
    pub comment: &'static str,
    builder: fn() -> Expr,
}

impl ConditionAlias {
    /// Returns a fresh expression constructed from the alias definition.
    pub fn expression(&self) -> Expr {
        (self.builder)()
    }
}

const fn make_alias(
    name: &'static str,
    comment: &'static str,
    builder: fn() -> Expr,
) -> ConditionAlias {
    ConditionAlias {
        name,
        comment,
        builder,
    }
}

/// Built-in list of condition aliases.
pub const DEFAULT_ALIASES: &[ConditionAlias] = &[
    make_alias("is_production", "Is production run", alias_is_production),
    make_alias(
        "is_2018production",
        "Is production run",
        alias_is_2018_production,
    ),
    make_alias(
        "is_primex_production",
        "Is PrimEx production run",
        alias_is_primex_production,
    ),
    make_alias(
        "is_dirc_production",
        "Is DIRC production run",
        alias_is_dirc_production,
    ),
    make_alias(
        "is_src_production",
        "Is SRC production run",
        alias_is_src_production,
    ),
    make_alias(
        "is_cpp_production",
        "Is CPP production run",
        alias_is_cpp_production,
    ),
    make_alias(
        "is_production_long",
        "Is production run with long mode data",
        alias_is_production_long,
    ),
    make_alias("is_cosmic", "Is cosmic run", alias_is_cosmic),
    make_alias("is_empty_target", "Target is empty", alias_is_empty_target),
    make_alias(
        "is_amorph_radiator",
        "Amorphous Radiator",
        alias_is_amorph_radiator,
    ),
    make_alias("is_coherent_beam", "Coherent Beam", alias_is_coherent_beam),
    make_alias("is_field_off", " Field Off", alias_is_field_off),
    make_alias("is_field_on", " Field On", alias_is_field_on),
    make_alias(
        "status_calibration",
        "Run status = calibration",
        alias_status_calibration,
    ),
    make_alias(
        "status_approved_long",
        "Run status = approved (long)",
        alias_status_approved_long,
    ),
    make_alias(
        "status_approved",
        "Run status = approved",
        alias_status_approved,
    ),
    make_alias(
        "status_unchecked",
        "Run status = unchecked",
        alias_status_unchecked,
    ),
    make_alias("status_reject", "Run status = reject", alias_status_reject),
];

/// Returns the expression associated with the supplied alias (if it exists).
pub fn alias(name: &str) -> Option<Expr> {
    DEFAULT_ALIASES
        .iter()
        .find(|alias| alias.name == name)
        .map(|alias| alias.expression())
}

fn alias_is_production() -> Expr {
    all([
        string_cond("run_type").isin(["hd_all.tsg", "hd_all.tsg_ps", "hd_all.bcal_fcal_st.tsg"]),
        float_cond("beam_current").gt(2.0),
        int_cond("event_count").gt(500_000),
        float_cond("solenoid_current").gt(100.0),
        string_cond("collimator_diameter").neq("Blocking"),
    ])
}

fn alias_is_2018_production() -> Expr {
    all([
        string_cond("daq_run").eq("PHYSICS"),
        float_cond("beam_current").gt(2.0),
        int_cond("event_count").gt(10_000_000),
        float_cond("solenoid_current").gt(100.0),
        string_cond("collimator_diameter").neq("Blocking"),
    ])
}

fn alias_is_primex_production() -> Expr {
    all([
        string_cond("daq_run").eq("PHYSICS_PRIMEX"),
        int_cond("event_count").gt(1_000_000),
        string_cond("collimator_diameter").neq("Blocking"),
    ])
}

fn alias_is_dirc_production() -> Expr {
    all([
        string_cond("daq_run").eq("PHYSICS_DIRC"),
        float_cond("beam_current").gt(2.0),
        int_cond("event_count").gt(5_000_000),
        float_cond("solenoid_current").gt(100.0),
        string_cond("collimator_diameter").neq("Blocking"),
    ])
}

fn alias_is_src_production() -> Expr {
    all([
        string_cond("daq_run").eq("PHYSICS_SRC"),
        float_cond("beam_current").gt(2.0),
        int_cond("event_count").gt(5_000_000),
        float_cond("solenoid_current").gt(100.0),
        string_cond("collimator_diameter").neq("Blocking"),
    ])
}

fn alias_is_cpp_production() -> Expr {
    all([
        string_cond("daq_run").eq("PHYSICS_CPP"),
        float_cond("beam_current").gt(2.0),
        int_cond("event_count").gt(5_000_000),
        float_cond("solenoid_current").gt(100.0),
        string_cond("collimator_diameter").neq("Blocking"),
    ])
}

fn alias_is_production_long() -> Expr {
    all([
        string_cond("daq_run").eq("PHYSICS_raw"),
        float_cond("beam_current").gt(2.0),
        int_cond("event_count").gt(5_000_000),
        float_cond("solenoid_current").gt(100.0),
        string_cond("collimator_diameter").neq("Blocking"),
    ])
}

fn alias_is_cosmic() -> Expr {
    all([
        string_cond("run_config").contains("cosmic"),
        float_cond("beam_current").lt(1.0),
        int_cond("event_count").gt(5_000),
    ])
}

fn alias_is_empty_target() -> Expr {
    string_cond("target_type").eq("EMPTY & Ready")
}

fn alias_is_amorph_radiator() -> Expr {
    float_cond("polarization_angle").lt(0.0)
}

fn alias_is_coherent_beam() -> Expr {
    float_cond("polarization_angle").geq(0.0)
}

fn alias_is_field_off() -> Expr {
    float_cond("solenoid_current").lt(100.0)
}

fn alias_is_field_on() -> Expr {
    float_cond("solenoid_current").geq(100.0)
}

fn alias_status_calibration() -> Expr {
    int_cond("status").eq(3)
}

fn alias_status_approved_long() -> Expr {
    int_cond("status").eq(2)
}

fn alias_status_approved() -> Expr {
    int_cond("status").eq(1)
}

fn alias_status_unchecked() -> Expr {
    int_cond("status").eq(-1)
}

fn alias_status_reject() -> Expr {
    int_cond("status").eq(0)
}
