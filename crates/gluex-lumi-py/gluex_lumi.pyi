"""Typed interface for the gluex_lumi Python bindings."""

from __future__ import annotations

from collections.abc import Mapping, Sequence

class Histogram:
    counts: list[float]
    edges: list[float]
    errors: list[float]

    def __init__(
        self, counts: list[float], edges: list[float], errors: list[float]
    ) -> None: ...
    def as_dict(self) -> dict[str, list[float]]: ...

class FluxHistograms:
    tagged_flux: Histogram
    tagm_flux: Histogram
    tagh_flux: Histogram
    tagged_luminosity: Histogram

    def __init__(
        self,
        tagged_flux: Histogram,
        tagm_flux: Histogram,
        tagh_flux: Histogram,
        tagged_luminosity: Histogram,
    ) -> None: ...
    def as_dict(self) -> dict[str, dict[str, list[float]]]: ...

def get_flux_histograms(
    run_periods: Mapping[str, int | None],
    edges: Sequence[float],
    *,
    coherent_peak: bool = False,
    polarized: bool = False,
    rcdb: str | None = None,
    ccdb: str | None = None,
    exclude_runs: Sequence[int] | None = None,
) -> FluxHistograms: ...
def cli() -> None: ...
