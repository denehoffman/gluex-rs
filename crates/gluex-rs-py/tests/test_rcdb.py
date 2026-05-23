"""Tests for the unified ``gluex.rcdb`` binding surface."""

from __future__ import annotations

import os
from pathlib import Path
from typing import cast
import datetime as dt

from gluex import rcdb
import pytest


def _db_path() -> Path:
    raw = os.environ.get("RCDB_CONNECTION")
    if not raw:
        pytest.skip("RCDB_CONNECTION is not set for RCDB integration tests")
    return Path(raw)


@pytest.fixture(scope="module")
def db() -> rcdb.RCDB:
    return rcdb.RCDB(str(_db_path()))


def test_fetch_single_run_int_condition(db: rcdb.RCDB) -> None:
    data = db.fetch(["event_count"], runs=[2])
    assert data[2]["event_count"] == 2


def test_fetch_with_filters(db: rcdb.RCDB) -> None:
    data = db.fetch(
        ["beam_current", "event_count"],
        run_min=1000,
        run_max=1100,
        filters=rcdb.all(
            rcdb.string_cond("run_type").isin(
                ["hd_all.tsg", "hd_all.tsg-m8", "hd_all.tsg-m7"]
            ),
            rcdb.float_cond("beam_current").gt(0.1),
            rcdb.int_cond("event_count").gt(50),
        ),
    )
    assert data
    for run, values in data.items():
        assert 1000 <= run <= 1100
        assert values["event_count"] > 50


def test_fetch_runs_with_alias(db: rcdb.RCDB) -> None:
    runs = db.fetch_runs(
        run_min=10000, run_max=10300, filters=rcdb.aliases.is_production
    )
    assert runs
    assert all(10000 <= run <= 10300 for run in runs)


def test_invalid_selectors_and_filters_raise(db: rcdb.RCDB) -> None:
    with pytest.raises(RuntimeError, match="mutually exclusive"):
        db.fetch_runs(runs=[2], run_min=1)
    with pytest.raises(RuntimeError, match="filters must be"):
        db.fetch_runs(runs=[2], filters=cast(rcdb.Expr, "not an expression"))


def test_complete_expression_builder_surface() -> None:
    timestamp = dt.datetime(2017, 1, 1, tzinfo=dt.timezone.utc)
    expressions = [
        rcdb.bool_cond("is_valid").exists(),
        rcdb.time_cond("start_time").ge(timestamp),
        rcdb.any(rcdb.aliases.status_approved, rcdb.aliases.status_calibration),
        rcdb.aliases.is_2018production,
        rcdb.aliases.is_dirc_production,
        rcdb.aliases.is_field_on,
    ]
    assert all(isinstance(expr, rcdb.Expr) for expr in expressions)
