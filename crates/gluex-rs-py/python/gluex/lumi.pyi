from collections.abc import Mapping, Sequence
from datetime import datetime

from . import Histogram

class FluxHistograms:
    tagged_flux: Histogram
    tagm_flux: Histogram
    tagh_flux: Histogram
    tagged_luminosity: Histogram

    def as_dict(self) -> dict[str, dict[str, list[float]]]: ...

class Luminosity:
    def __init__(self, rcdb: str | None = None, ccdb: str | None = None) -> None: ...
    def fetch(
        self,
        edges: Sequence[float],
        *,
        runs: Sequence[int],
        rest_version: Mapping[str, int | datetime | None] | None = None,
        coherent_peak: bool = False,
        polarized: bool = False,
        exclude_runs: Sequence[int] | None = None,
    ) -> FluxHistograms: ...
