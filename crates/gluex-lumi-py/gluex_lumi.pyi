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

class Context:
    def __init__(
        self,
        runs: Sequence[int],
        rest: Mapping[str, int | None] | None = None,
        *,
        coherent_peak: bool = False,
        polarized: bool = False,
        exclude_runs: Sequence[int] | None = None,
    ) -> None: ...

class Luminosity:
    def __init__(self, rcdb: str | None = None, ccdb: str | None = None) -> None: ...
    def fetch(self, edges: Sequence[float], ctx: Context) -> FluxHistograms: ...

def cli() -> None: ...
