"""Integration tests for the gluex_lumi Python bindings."""

from __future__ import annotations

import datetime as dt
import os
from pathlib import Path

import gluex_lumi
import pytest


REQUIRED_KEYS = {
    "tagged_flux",
    "tagm_flux",
    "tagh_flux",
    "tagged_luminosity",
}
TAGM_FLUX = 48_116_930.84601025
TAGH_FLUX = 642_059_090.0805457
TAGGED_FLUX = 690_176_020.9265559
TAGGED_LUMINOSITY = 0.0008695639199135528


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
        [50685, 50697],
        rest_version={"f18": 2},
        exclude_runs=[50697],
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
    assert histograms.tagged_flux.counts[0] == 0.0
    assert histograms.tagm_flux.counts[1] == pytest.approx(TAGM_FLUX)
    assert histograms.tagh_flux.counts[1] == pytest.approx(TAGH_FLUX)
    assert histograms.tagged_flux.counts[1] == pytest.approx(TAGGED_FLUX)
    assert histograms.tagged_luminosity.counts[1] == pytest.approx(TAGGED_LUMINOSITY)


def test_luminosity_context_accepts_datetime_rest_version() -> None:
    ctx = gluex_lumi.Context(
        [50685, 50697],
        rest_version={
            "f18": dt.datetime(2019, 7, 21, 12, 0, 0, tzinfo=dt.timezone.utc),
        },
        exclude_runs=[50697],
    )
    lumi = gluex_lumi.Luminosity(rcdb=str(_rcdb_path()), ccdb=str(_ccdb_path()))
    histograms = lumi.fetch([8.0, 8.5, 9.0], ctx)
    for key in REQUIRED_KEYS:
        assert hasattr(histograms, key)
    assert histograms.tagged_flux.counts[1] == pytest.approx(TAGGED_FLUX)
