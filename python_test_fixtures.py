"""Shared pytest fixtures for temporary GlueX database snapshots."""

from __future__ import annotations

import os
import sqlite3
from pathlib import Path

import pytest

_SEEDS = Path(__file__).parent / "tests" / "fixtures"


def _create_database(path: Path, seed_name: str) -> Path:
    with sqlite3.connect(path) as connection:
        connection.executescript((_SEEDS / seed_name).read_text())
    return path


@pytest.fixture(scope="session")
def ccdb_path(tmp_path_factory: pytest.TempPathFactory) -> Path:
    return _create_database(tmp_path_factory.mktemp("databases") / "ccdb.sqlite", "ccdb.sql")


@pytest.fixture(scope="session")
def rcdb_path(tmp_path_factory: pytest.TempPathFactory) -> Path:
    return _create_database(tmp_path_factory.mktemp("databases") / "rcdb.sqlite", "rcdb.sql")


@pytest.fixture(scope="session", autouse=True)
def database_environment(ccdb_path: Path, rcdb_path: Path):
    previous_ccdb = os.environ.get("CCDB_CONNECTION")
    previous_rcdb = os.environ.get("RCDB_CONNECTION")
    os.environ["CCDB_CONNECTION"] = str(ccdb_path)
    os.environ["RCDB_CONNECTION"] = str(rcdb_path)
    yield
    if previous_ccdb is None:
        os.environ.pop("CCDB_CONNECTION", None)
    else:
        os.environ["CCDB_CONNECTION"] = previous_ccdb
    if previous_rcdb is None:
        os.environ.pop("RCDB_CONNECTION", None)
    else:
        os.environ["RCDB_CONNECTION"] = previous_rcdb
