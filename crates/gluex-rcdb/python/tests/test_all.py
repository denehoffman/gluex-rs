"""Integration tests for the Python RCDB bindings."""
from __future__ import annotations

import os
from pathlib import Path

import pytest

import gluex_rcdb as rcdb


def _rcdb_path() -> Path:
    raw = os.environ.get("RCDB_TEST_SQLITE_CONNECTION", "rcdb.sqlite")
    for candidate in _candidate_paths(raw):
        if candidate.exists():
            return candidate
    pytest.skip(
        "RCDB test database not found. Set RCDB_TEST_SQLITE_CONNECTION or place rcdb.sqlite at the repo root."
    )


def _candidate_paths(raw: str) -> list[Path]:
    raw_path = Path(raw)
    if raw_path.is_absolute():
        return [raw_path]
    bases = [
        Path(__file__).resolve().parents[2],  # crate root
        Path(__file__).resolve().parents[4],  # workspace root
    ]
    return [raw_path, *(base / raw_path for base in bases)]

def _open_db() -> rcdb.RCDB:
    return rcdb.RCDB(str(_rcdb_path()))


def test_fetch_single_run_int_condition() -> None:
    db = _open_db()
    ctx = rcdb.Context().with_run(2)
    data = db.fetch(["event_count"], ctx)
    assert 2 in data
    value = data[2]["event_count"]
    assert value.as_int() == 2


def test_fetch_with_filters() -> None:
    db = _open_db()
    ctx = (
        rcdb.Context()
        .with_run_range(1000, 1100)
        .filter(
            rcdb.all(
                rcdb.string_cond("run_type").isin(
                    ["hd_all.tsg", "hd_all.tsg-m8", "hd_all.tsg-m7"]
                ),
                rcdb.float_cond("beam_current").gt(0.1),
                rcdb.int_cond("event_count").gt(50),
            )
        )
    )
    data = db.fetch(["beam_current", "event_count"], ctx)
    assert data
    for run, values in data.items():
        assert 1000 <= run <= 1100
        assert values["event_count"].as_int() and values["event_count"].as_int() > 50


def test_fetch_runs_with_alias() -> None:
    db = _open_db()
    ctx = rcdb.Context().with_run_range(10000, 10300).filter(rcdb.alias("is_production"))
    runs = db.fetch_runs(ctx)
    assert runs
    assert all(10000 <= run <= 10300 for run in runs)


def test_alias_metadata_roundtrip() -> None:
    aliases = rcdb.aliases()
    assert any(alias.name == "is_production" for alias in aliases)
    expr = aliases[0].expression()
    ctx = rcdb.Context().filter(expr)
    assert isinstance(ctx, rcdb.Context)
