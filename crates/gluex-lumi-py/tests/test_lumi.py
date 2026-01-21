"""Integration tests for the gluex_lumi Python bindings."""

from __future__ import annotations

import os
from pathlib import Path
import pytest

import gluex_lumi


REQUIRED_KEYS = {
    "tagged_flux",
    "tagm_flux",
    "tagh_flux",
    "tagged_luminosity",
}


def _candidate_paths(raw: str) -> list[Path]:
    raw_path = Path(raw)
    if raw_path.is_absolute():
        return [raw_path]
    parents = Path(__file__).resolve().parents
    # Always check the crate root and monorepo root, falling back if the
    # expected parent index is missing (e.g. when vendored elsewhere).
    base_indices = (1, 3)
    bases = [parents[idx] for idx in base_indices if len(parents) > idx]
    return [raw_path, *(base / raw_path for base in bases)]


def _resolve_path(env_var: str, default: str, friendly: str) -> Path:
    raw = os.environ.get(env_var, default)
    for candidate in _candidate_paths(raw):
        if candidate.exists():
            return candidate
    pytest.skip(
        f"{friendly} database not found. Set {env_var} or place {default} at the repo root."
    )
    raise FileNotFoundError(f"{friendly} database not found")


def _rcdb_path() -> Path:
    return _resolve_path("RCDB_TEST_SQLITE_CONNECTION", "rcdb.sqlite", "RCDB")


def _ccdb_path() -> Path:
    return _resolve_path("CCDB_TEST_SQLITE_CONNECTION", "ccdb.sqlite", "CCDB")


def test_get_flux_histograms_smoke() -> None:
    histograms = gluex_lumi.get_flux_histograms(
        {"f18": 2},
        [8.0, 8.5, 9.0],
        rcdb=str(_rcdb_path()),
        ccdb=str(_ccdb_path()),
        exclude_runs=[50000],
    )
    for key in REQUIRED_KEYS:
        assert hasattr(histograms, key)
        hist = getattr(histograms, key)
        assert isinstance(hist, gluex_lumi.Histogram)
        assert len(hist.edges) == 3
        assert len(hist.counts) == 2
        assert len(hist.errors) == 2
