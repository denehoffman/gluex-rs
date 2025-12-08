# ruff: noqa: A001
from typing import Any

RCDB: Any
Context: Any
Expr: Any
Value: Any
ConditionAlias: Any


def int_cond(name: str) -> Any: ...
def float_cond(name: str) -> Any: ...
def string_cond(name: str) -> Any: ...
def bool_cond(name: str) -> Any: ...
def time_cond(name: str) -> Any: ...

def all(*exprs: Expr) -> Expr: ...
def any(*exprs: Expr) -> Expr: ...
def alias(name: str) -> Expr | None: ...
def aliases() -> list[ConditionAlias]: ...

__all__ = [
    "RCDB",
    "ConditionAlias",
    "Context",
    "Expr",
    "Value",
    "alias",
    "aliases",
    "all",
    "any",
    "bool_cond",
    "float_cond",
    "int_cond",
    "string_cond",
    "time_cond",
]
