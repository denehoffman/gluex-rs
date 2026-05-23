from collections.abc import Sequence
from types import ModuleType

class Histogram:
    counts: list[float]
    edges: list[float]
    errors: list[float]

    def __init__(
        self,
        counts: Sequence[float],
        edges: Sequence[float],
        errors: Sequence[float] | None = None,
    ) -> None: ...
    def as_dict(self) -> dict[str, list[float]]: ...

class _CCDBModule(ModuleType): ...
class _LumiModule(ModuleType): ...
class _RCDBModule(ModuleType): ...

ccdb: _CCDBModule
lumi: _LumiModule
rcdb: _RCDBModule
__version__: str
