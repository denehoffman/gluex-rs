"""Typed interface for the gluex_lumi Python bindings."""

from __future__ import annotations

from collections.abc import Mapping, Sequence
from typing import TypedDict


class HistogramDict(TypedDict):
    counts: list[float]
    edges: list[float]
    errors: list[float]


class FluxHistograms(TypedDict):
    tagged_flux: HistogramDict
    tagm_flux: HistogramDict
    tagh_flux: HistogramDict
    tagged_luminosity: HistogramDict


def get_flux_histograms(
    run_periods: Mapping[str, int],
    edges: Sequence[float],
    *,
    coherent_peak: bool = False,
    polarized: bool = False,
    rcdb: str | None = None,
    ccdb: str | None = None,
) -> FluxHistograms: ...


def cli() -> None: ...
