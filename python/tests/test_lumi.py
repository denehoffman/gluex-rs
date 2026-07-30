"""Tests for the initial unified ``gluex.lumi`` binding surface."""

from __future__ import annotations

import os
from pathlib import Path
from typing import Any, cast

import pytest
from gluex import RESTVersionSelection, RunPeriod, lumi

TAGM_FLUX = 48_116_930.84601025
TAGH_FLUX = 642_059_090.0805457
TAGGED_FLUX = 690_176_020.9265559
TAGGED_LUMINOSITY = 0.0008695639199135528


def _db_path(variable: str) -> Path:
    raw = os.environ.get(variable)
    if not raw:
        pytest.skip(f'{variable} is not set for luminosity integration tests')
    return Path(raw)


def test_luminosity_fetch_uses_keyword_selection_api() -> None:
    calculator = lumi.Luminosity(
        rcdb=str(_db_path('RCDB_CONNECTION')),
        ccdb=str(_db_path('CCDB_CONNECTION')),
    )
    with pytest.raises(RuntimeError, match='at least one run number is required'):
        calculator.fetch(
            [8.0, 8.5, 9.0],
            runs=[],
            rest_version={RunPeriod.RP2018_08: RESTVersionSelection.version(RunPeriod.RP2018_08, 2)},
        )


def test_luminosity_fetch_matches_seeded_detector_aggregation() -> None:
    calculator = lumi.Luminosity(
        rcdb=str(_db_path('RCDB_CONNECTION')),
        ccdb=str(_db_path('CCDB_CONNECTION')),
    )
    histograms = calculator.fetch(
        [8.0, 8.5, 9.0],
        runs=[50685, 50697],
        rest_version={RunPeriod.RP2018_08: RESTVersionSelection.version(RunPeriod.RP2018_08, 2)},
        exclude_runs=[50697],
    )

    assert histograms.tagged_flux.counts[0] == 0.0
    assert histograms.tagm_flux.counts[1] == pytest.approx(TAGM_FLUX)
    assert histograms.tagh_flux.counts[1] == pytest.approx(TAGH_FLUX)
    assert histograms.tagged_flux.counts[1] == pytest.approx(TAGGED_FLUX)
    assert histograms.tagged_luminosity.counts[1] == pytest.approx(TAGGED_LUMINOSITY)


def test_luminosity_rejects_invalid_rest_selection_values() -> None:
    calculator = lumi.Luminosity(
        rcdb=str(_db_path('RCDB_CONNECTION')),
        ccdb=str(_db_path('CCDB_CONNECTION')),
    )
    with pytest.raises(RuntimeError, match='rest_version'):
        calculator.fetch(
            [8.0, 8.5, 9.0],
            runs=[50685],
            rest_version=cast('Any', {RunPeriod.RP2018_08: object()}),
        )
