from __future__ import annotations

import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).parents[3]))

from python_test_fixtures import (  # noqa: E402
    ccdb_path as ccdb_path,
    database_environment as database_environment,
    rcdb_path as rcdb_path,
)
