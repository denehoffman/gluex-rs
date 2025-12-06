from __future__ import annotations

import datetime as dt
import os
from pathlib import Path

import pytest
import gluex_ccdb


TABLE_PATH = '/test/demo/mytable'
FIRST_AVAILABLE = dt.datetime(2013, 2, 22, 19, 40, 35, tzinfo=dt.timezone.utc)


def resolve_db_path() -> Path:
    raw = os.environ.get('CCDB_TEST_SQLITE_CONNECTION')
    if raw is None:
        pytest.skip('CCDB_TEST_SQLITE_CONNECTION is not set')

    candidates: list[Path] = []
    raw_path = Path(raw)
    if raw_path.is_absolute():
        candidates.append(raw_path)
    else:
        candidates.extend(
            [
                raw_path,
                Path(__file__).resolve().parents[2] / raw,
                Path(__file__).resolve().parents[4] / raw,
            ]
        )

    for candidate in candidates:
        if candidate.exists():
            return candidate

    pytest.fail(f'CCDB_TEST_SQLITE_CONNECTION does not point to a file: {raw}')


@pytest.fixture(scope='module')
def db() -> gluex_ccdb.CCDB:
    return gluex_ccdb.CCDB(str(resolve_db_path()))


def test_directory_and_table_metadata(db: gluex_ccdb.CCDB):
    root = db.root()
    assert root.full_path() == '/'  # noqa: S101

    test_dir = db.dir('/test')
    assert test_dir.full_path() == '/test'  # noqa: S101

    demo_dir = test_dir.dir('demo')
    assert demo_dir.full_path() == '/test/demo'  # noqa: S101

    table = demo_dir.table('mytable')
    assert table.full_path() == TABLE_PATH  # noqa: S101

    meta = table.meta
    assert meta.n_rows == 2  # noqa: S101
    assert meta.n_columns == 3  # noqa: S101

    columns = table.columns()
    assert [c.name for c in columns] == ['x', 'y', 'z']  # noqa: S101
    assert [c.column_type.name for c in columns] == ['double', 'double', 'double']  # noqa: S101


def test_fetch_across_runs_timestamps_and_variations(db: gluex_ccdb.CCDB):
    table = db.table(TABLE_PATH)

    before_first = db.fetch(TABLE_PATH, runs=[0, 1, 2, 3], timestamp='2013-02-22 19:40:34')
    assert before_first == {}  # noqa: S101

    first = db.fetch(TABLE_PATH, runs=[0, 1, 2, 3], timestamp=FIRST_AVAILABLE)
    assert set(first) == {0, 1, 2, 3}  # noqa: S101
    for data in first.values():
        assert data.n_rows == 2  # noqa: S101
        assert data.column_names == ['x', 'y', 'z']  # noqa: S101
        assert data.value('x', 0) == 0.0  # noqa: S101
        assert data.value('y', 0) == 1.0  # noqa: S101
        assert data.value('z', 0) == 2.0  # noqa: S101
        assert data.value('x', 1) == 3.0  # noqa: S101
        assert data.value('y', 1) == 4.0  # noqa: S101
        assert data.value('z', 1) == 5.0  # noqa: S101

    mc = table.fetch(runs=[2], variation='mc', timestamp=FIRST_AVAILABLE)
    assert set(mc) == {2}  # noqa: S101
    mc_row = mc[2].row(1)
    assert mc_row.value('z') == 5.0  # noqa: S101

    updated = db.fetch(TABLE_PATH, runs=[0, 1, 2, 3], timestamp='2020-02-01 00:00:00')
    assert set(updated) == {0, 1, 2, 3}  # noqa: S101
    for data in updated.values():
        assert data.value(0, 0) == 1.0  # noqa: S101
        assert data.value(1, 0) == 2.0  # noqa: S101
        assert data.value(2, 0) == 3.0  # noqa: S101
        row_columns = data.row(1).columns()
        assert [name for name, _, _ in row_columns] == ['x', 'y', 'z']  # noqa: S101
        assert [ctype.name for _, ctype, _ in row_columns] == ['double', 'double', 'double']  # noqa: S101
        assert [value for _, _, value in row_columns] == [4.0, 5.0, 6.0]  # noqa: S101
