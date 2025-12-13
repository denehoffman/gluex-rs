"""Integration tests for the gluex_lumi Python bindings."""

from __future__ import annotations

import os
from pathlib import Path
from typing import cast

import pytest

import gluex_lumi


REQUIRED_KEYS = {
    'tagged_flux',
    'tagm_flux',
    'tagh_flux',
    'tagged_luminosity',
}


def _candidate_paths(raw: str) -> list[Path]:
    raw_path = Path(raw)
    if raw_path.is_absolute():
        return [raw_path]
    bases = [
        Path(__file__).resolve().parents[2],
        Path(__file__).resolve().parents[4],
    ]
    return [raw_path, *(base / raw_path for base in bases)]


def _resolve_path(env_var: str, default: str, friendly: str) -> Path:
    raw = os.environ.get(env_var, default)
    for candidate in _candidate_paths(raw):
        if candidate.exists():
            return candidate
    pytest.skip(f'{friendly} database not found. Set {env_var} or place {default} at the repo root.')
    raise FileNotFoundError(f'{friendly} database not found')


def _rcdb_path() -> Path:
    return _resolve_path('RCDB_TEST_SQLITE_CONNECTION', 'rcdb.sqlite', 'RCDB')


def _ccdb_path() -> Path:
    return _resolve_path('CCDB_TEST_SQLITE_CONNECTION', 'ccdb.sqlite', 'CCDB')


def test_get_flux_histograms_smoke() -> None:
    histograms = gluex_lumi.get_flux_histograms(
        {'f18': 19},
        [8.0, 8.5, 9.0],
        rcdb=str(_rcdb_path()),
        ccdb=str(_ccdb_path()),
    )
    assert REQUIRED_KEYS.issubset(histograms.keys())
    typed = cast(dict[str, gluex_lumi.HistogramDict], histograms)
    for key in REQUIRED_KEYS:
        hist = typed[key]
        assert len(hist['edges']) == 3
        assert len(hist['counts']) == 2
        assert len(hist['errors']) == 2
