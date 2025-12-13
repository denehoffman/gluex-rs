"""Integration tests for the Python CCDB bindings."""

from __future__ import annotations

import datetime as dt
import os
from pathlib import Path

import gluex_ccdb
import pytest

TABLE_PATH = "/test/demo/mytable"
FIRST_AVAILABLE = dt.datetime(2013, 2, 22, 19, 40, 35, tzinfo=dt.timezone.utc)


def resolve_db_path() -> Path:
    raw = os.environ.get("CCDB_TEST_SQLITE_CONNECTION", "ccdb.sqlite")
    for candidate in _candidate_paths(raw):
        if candidate.exists():
            return candidate
    pytest.skip(
        "CCDB test database not found. Set CCDB_TEST_SQLITE_CONNECTION or place ccdb.sqlite at the repo root."
    )
    raise FileNotFoundError("CCDB test database not found")


def _candidate_paths(raw: str) -> list[Path]:
    raw_path = Path(raw)
    if raw_path.is_absolute():
        return [raw_path]
    bases = [
        Path(__file__).resolve().parents[2],
        Path(__file__).resolve().parents[4],
    ]
    return [raw_path, *(base / raw_path for base in bases)]


@pytest.fixture(scope="module")
def db() -> gluex_ccdb.CCDB:
    return gluex_ccdb.CCDB(str(resolve_db_path()))


def test_directory_and_table_metadata(db: gluex_ccdb.CCDB):
    root = db.root()
    assert root.full_path() == "/"

    test_dir = db.dir("/test")
    assert test_dir.full_path() == "/test"

    demo_dir = test_dir.dir("demo")
    assert demo_dir.full_path() == "/test/demo"

    table = demo_dir.table("mytable")
    assert table.full_path() == TABLE_PATH

    meta = table.meta
    assert meta.n_rows == 2
    assert meta.n_columns == 3

    columns = table.columns()
    assert [c.name for c in columns] == ["x", "y", "z"]
    assert [c.column_type.name for c in columns] == ["double", "double", "double"]


def test_fetch_across_runs_timestamps_and_variations(db: gluex_ccdb.CCDB):
    table = db.table(TABLE_PATH)

    before_first = db.fetch(
        TABLE_PATH, runs=[0, 1, 2, 3], timestamp="2013-02-22 19:40:34"
    )
    assert before_first == {}

    first = db.fetch(TABLE_PATH, runs=[0, 1, 2, 3], timestamp=FIRST_AVAILABLE)
    assert set(first) == {0, 1, 2, 3}
    for data in first.values():
        assert data.n_rows == 2
        assert data.column_names == ["x", "y", "z"]
        assert data.value("x", 0) == 0.0
        assert data.value("y", 0) == 1.0
        assert data.value("z", 0) == 2.0
        assert data.value("x", 1) == 3.0
        assert data.value("y", 1) == 4.0
        assert data.value("z", 1) == 5.0

    mc = table.fetch(runs=[2], variation="mc", timestamp=FIRST_AVAILABLE)
    assert set(mc) == {2}
    mc_row = mc[2].row(1)
    assert mc_row.value("z") == 5.0

    updated = db.fetch(TABLE_PATH, runs=[0, 1, 2, 3], timestamp="2020-02-01 00:00:00")
    assert set(updated) == {0, 1, 2, 3}
    for data in updated.values():
        assert data.value(0, 0) == 1.0
        assert data.value(1, 0) == 2.0
        assert data.value(2, 0) == 3.0
        row_columns = data.row(1).columns()
        assert [name for name, _, _ in row_columns] == ["x", "y", "z"]
        assert [ctype.name for _, ctype, _ in row_columns] == [
            "double",
            "double",
            "double",
        ]
        assert [value for _, _, value in row_columns] == [4.0, 5.0, 6.0]
