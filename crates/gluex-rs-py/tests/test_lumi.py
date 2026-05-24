"""Tests for the initial unified ``gluex.lumi`` binding surface."""

from __future__ import annotations

import os
from pathlib import Path

from gluex import RESTVersionSelection, RunPeriod, lumi
import pytest


def _db_path(variable: str) -> Path:
    raw = os.environ.get(variable)
    if not raw:
        pytest.skip(f"{variable} is not set for luminosity integration tests")
    return Path(raw)


def test_luminosity_fetch_uses_keyword_selection_api() -> None:
    calculator = lumi.Luminosity(
        rcdb=str(_db_path("RCDB_CONNECTION")),
        ccdb=str(_db_path("CCDB_CONNECTION")),
    )
    with pytest.raises(RuntimeError, match="at least one run number is required"):
        calculator.fetch(
            [8.0, 8.5, 9.0],
            runs=[],
            rest_version={
                RunPeriod.RP2018_08: RESTVersionSelection.version(RunPeriod.RP2018_08, 2)
            },
        )
