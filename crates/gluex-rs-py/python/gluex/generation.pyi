from collections.abc import Iterable
from os import PathLike

from laddu.generation import GeneratedBatch

class GlueXHddmConfig:
    beam_id: str
    target_id: str

    def __init__(
        self,
        beam_id: str,
        target_id: str,
        *,
        run_number: int = 0,
        first_event_number: int = 0,
        random_seed: int = 0,
        vertex: tuple[float, float, float] = (0.0, 0.0, 0.0),
    ) -> None: ...

class GlueXHddmWriter:
    def __init__(self, config: GlueXHddmConfig) -> None: ...
    def write_batch(self, batch: GeneratedBatch, path: str | PathLike[str]) -> int: ...
    def append_batch(
        self, batch: GeneratedBatch, path: str | PathLike[str], start_event: int
    ) -> int: ...
    def write_batches(
        self, batches: Iterable[GeneratedBatch], path: str | PathLike[str]
    ) -> None: ...
