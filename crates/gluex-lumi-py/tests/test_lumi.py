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


def _rcdb_path() -> Path:
    raw = os.environ.get("RCDB_CONNECTION")
    if not raw:
        raise RuntimeError("RCDB_CONNECTION must be set for lumi tests")
    return Path(raw)


def _ccdb_path() -> Path:
    raw = os.environ.get("CCDB_CONNECTION")
    if not raw:
        raise RuntimeError("CCDB_CONNECTION must be set for lumi tests")
    return Path(raw)


def test_luminosity_fetch_smoke() -> None:
    ctx = gluex_lumi.Context(
        [50000, 50001],
        rest={"f18": 2},
        exclude_runs=[50000],
    )
    lumi = gluex_lumi.Luminosity(rcdb=str(_rcdb_path()), ccdb=str(_ccdb_path()))
    histograms = lumi.fetch([8.0, 8.5, 9.0], ctx)
    for key in REQUIRED_KEYS:
        assert hasattr(histograms, key)
        hist = getattr(histograms, key)
        assert isinstance(hist, gluex_lumi.Histogram)
        assert len(hist.edges) == 3
        assert len(hist.counts) == 2
        assert len(hist.errors) == 2
