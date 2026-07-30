"""Tests for the unified ``gluex.ccdb`` binding surface."""

from __future__ import annotations

import datetime as dt
import os
from pathlib import Path
from typing import cast

import pytest
from gluex import RESTVersionSelection, RunPeriod, ccdb

TABLE_PATH = '/test/demo/mytable'
FIRST_AVAILABLE = dt.datetime(2013, 2, 22, 13, 40, 35, tzinfo=dt.timezone.utc)


def _db_path() -> Path:
    raw = os.environ.get('CCDB_CONNECTION')
    if not raw:
        pytest.skip('CCDB_CONNECTION is not set for CCDB integration tests')
    return Path(raw)


@pytest.fixture(scope='module')
def db() -> ccdb.CCDB:
    return ccdb.CCDB(str(_db_path()))


def test_directory_and_table_metadata(db: ccdb.CCDB) -> None:
    assert db.root().full_path() == '/'
    demo_dir = db.dir('/test').dir('demo')
    assert demo_dir.full_path() == '/test/demo'

    table = demo_dir.table('mytable')
    assert table.full_path() == TABLE_PATH
    assert table.meta.n_rows == 2
    assert table.meta.n_columns == 3

    columns = table.columns()
    assert [column.name for column in columns] == ['x', 'y', 'z']
    assert [column.column_type.name for column in columns] == ['double'] * 3


def test_fetch_data_across_timestamps_and_variations(db: ccdb.CCDB) -> None:
    before_first = db.fetch(TABLE_PATH, runs=[0, 1, 2, 3], timestamp='2013-02-22 13:40:34')
    assert before_first == {}

    first = db.fetch(TABLE_PATH, runs=[0, 1, 2, 3], timestamp=FIRST_AVAILABLE)
    assert set(first) == {0, 1, 2, 3}
    for data in first.values():
        assert data.n_rows == 2
        assert data.column_names == ['x', 'y', 'z']
        assert data.value('x', 0) == 0.0
        assert data.value('z', 1) == 5.0
        assert data.value('z', 99) is None
        assert data.value('missing', 0) is None
        assert data.as_dict() == {
            'x': [0.0, 3.0],
            'y': [1.0, 4.0],
            'z': [2.0, 5.0],
        }
        assert data['z'].values() == [2.0, 5.0]
        assert data['z'][1] == 5.0
        assert data[1].as_dict() == {'x': 3.0, 'y': 4.0, 'z': 5.0}
        assert data[1]['z'] == 5.0
        assert [row.as_dict() for row in data.rows()] == [
            {'x': 0.0, 'y': 1.0, 'z': 2.0},
            {'x': 3.0, 'y': 4.0, 'z': 5.0},
        ]

    mc = db.table(TABLE_PATH).fetch(runs=[2], variation='mc', timestamp=FIRST_AVAILABLE)
    assert set(mc) == {2}
    assert mc[2].row(1).value('z') == 5.0

    updated = db.fetch(TABLE_PATH, runs=[0, 1, 2, 3], timestamp='2020-02-01 00:00:00')
    assert set(updated) == {0, 1, 2, 3}
    for data in updated.values():
        row_columns = data.row(1).columns()
        assert [name for name, _, _ in row_columns] == ['x', 'y', 'z']
        assert [kind.name for _, kind, _ in row_columns] == ['double'] * 3
        assert [value for _, _, value in row_columns] == [4.0, 5.0, 6.0]


def test_fetch_run_period_accepts_datetime_rest_version(db: ccdb.CCDB) -> None:
    timestamp = dt.datetime(2017, 6, 12, 18, 2, 0, tzinfo=dt.timezone.utc)

    by_rest_version = db.fetch_run_period(TABLE_PATH, run_period='s17', rest_version=timestamp)
    by_timestamp = db.fetch_run_period(TABLE_PATH, run_period='s17', timestamp=timestamp)
    assert set(by_rest_version) == set(by_timestamp)

    table = db.table(TABLE_PATH)
    table_by_rest = table.fetch_run_period(run_period='s17', rest_version=timestamp)
    table_by_timestamp = table.fetch_run_period(run_period='s17', timestamp=timestamp)
    assert set(table_by_rest) == set(table_by_timestamp)

    typed = db.fetch_run_period(
        TABLE_PATH,
        run_period=RunPeriod.RP2017_01,
        rest_version=RESTVersionSelection.timestamp(timestamp),
    )
    assert set(typed) == set(by_timestamp)


def test_data_indexing_is_strict_while_value_can_probe(db: ccdb.CCDB) -> None:
    data = db.fetch(TABLE_PATH, runs=[0], timestamp=FIRST_AVAILABLE)[0]
    assert data[0].value('missing') is None
    with pytest.raises(KeyError):
        _ = data['missing']
    with pytest.raises(IndexError):
        _ = data[10]
    with pytest.raises(KeyError):
        _ = data[0]['missing']


def test_invalid_timestamp_and_rest_version_inputs_raise(db: ccdb.CCDB) -> None:
    with pytest.raises(RuntimeError, match='timestamp'):
        db.fetch(TABLE_PATH, runs=[0], timestamp=cast('str', object()))
    with pytest.raises(RuntimeError, match='rest_version'):
        db.fetch_run_period(
            TABLE_PATH,
            run_period=RunPeriod.RP2017_01,
            rest_version=cast('int', object()),
        )
