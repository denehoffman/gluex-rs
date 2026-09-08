"""Recorded membership and immutable Condition Definition discovery."""

import shutil
import sqlite3
from datetime import datetime, timezone
from pathlib import Path

import gluex
import pytest


def test_recorded_membership_and_catalog(rcdb_path: Path) -> None:
    gx = gluex.open(rcdb=rcdb_path, ccdb=gluex.DISABLED)
    selection = gluex.RunSelection.runs([5, 2, 1, 5, 3])
    query = gx.runs(selection)
    assert 'RunQuery' in repr(query)
    result = query.collect()
    assert tuple(result) == (2, 3, 5)
    assert result.numbers == (2, 3, 5)
    assert result.provenance.source == str(rcdb_path.resolve())
    assert repr(result.provenance.selection) == repr(selection)
    assert 3 in result
    assert 1 not in result
    assert result[0] == 2
    assert result[-1] == 5
    catalog = gx.conditions
    assert tuple(catalog) == catalog.keys()
    assert dict(catalog.items())['event_count'].name == 'event_count'
    assert catalog['event_count'].value_type == 'int'
    assert len(catalog) == 11
    with pytest.raises(KeyError):
        _ = catalog['absent']
    with pytest.raises(AttributeError):
        catalog['event_count'].name = 'changed'
    with pytest.raises(AttributeError):
        result.numbers = ()
    del gx
    assert tuple(query.collect()) == (2, 3, 5)


@pytest.mark.parametrize(
    ('selection', 'expected'),
    [
        (gluex.RunSelection.range(2, 4), (2, 3, 4)),
        (gluex.RunSelection.range(4, 2), ()),
        (gluex.RunSelection.runs([]), ()),
        (gluex.RunSelection.period(gluex.RunPeriod.RP2018_08), (50685, 50697)),
        (gluex.RunSelection.range(-(2**63), 2**63 - 1), (2, 3, 4, 5, 1100, 10204, 50685, 50697)),
    ],
)
def test_numeric_scope(selection: gluex.RunSelection, expected: tuple[int, ...]) -> None:
    assert gluex.open().runs(selection).collect().numbers == expected


def test_missing_rcdb_remains_discoverable() -> None:
    gx = gluex.open(rcdb=gluex.DISABLED, ccdb=gluex.DISABLED)
    assert 'runs' in dir(gx)
    assert 'conditions' in dir(gx)
    with pytest.raises(RuntimeError, match='RCDB'):
        gx.runs(gluex.RunSelection.runs([2]))
    with pytest.raises(RuntimeError, match='RCDB'):
        _ = gx.conditions


def test_condition_projection(rcdb_path: Path) -> None:
    gx = gluex.open(rcdb=rcdb_path, ccdb=gluex.DISABLED)
    query = gx.runs(gluex.RunSelection.range(2, 4))
    projected = query.select(['event_count', 'is_valid_run_end'])
    assert isinstance(projected, gluex.ConditionQuery)
    assert 'event_count' in repr(projected)
    result = projected.collect()
    assert isinstance(result, gluex.ConditionResults)
    assert result.runs.numbers == (2, 3, 4)
    assert result[3, 'event_count'] == 1686
    assert result[3, 'is_valid_run_end'] is None
    assert result.column('is_valid_run_end') == (False, None, True)
    assert result.report.missing_values == ((3, 'is_valid_run_end'),)
    assert result.provenance.fields == ('event_count', 'is_valid_run_end')
    assert result.provenance.runs.source == str(rcdb_path.resolve())
    assert isinstance(query.collect(), gluex.RunSet)
    with pytest.raises(KeyError):
        _ = result[1, 'event_count']
    with pytest.raises(KeyError):
        result.column('absent')
    with pytest.raises(ValueError):
        query.select(['absent'])
    with pytest.raises(TypeError):
        query.select([2])
    with pytest.raises(AttributeError):
        result.runs = ()


@pytest.mark.parametrize('text', ['broken', 'garbage 2014 garbage', '2014'])
def test_projected_timestamp_rejects_corruption(rcdb_path, tmp_path, text):
    fixture = tmp_path / 'malformed.sqlite'
    shutil.copyfile(rcdb_path, fixture)
    with sqlite3.connect(fixture) as connection:
        connection.execute('UPDATE conditions SET time_value = ? WHERE id = 7', (text,))
    gx = gluex.open(rcdb=fixture, ccdb=gluex.DISABLED)
    query = gx.runs(gluex.RunSelection.runs([2])).select(['run_start_time'])
    with pytest.raises(RuntimeError, match='run_start_time at run 2'):
        query.collect()


def test_condition_columns_preserve_types_nulls_and_filter_reports(rcdb_path, tmp_path):
    fixture = tmp_path / 'types.sqlite'
    shutil.copyfile(rcdb_path, fixture)
    with sqlite3.connect(fixture) as connection:
        connection.execute("UPDATE conditions SET time_value = '2015-12-08 15:47:20.125' WHERE id = 7")
    gx = gluex.open(rcdb=fixture, ccdb=gluex.DISABLED)
    result = gx.runs(gluex.RunSelection.runs([2, 3, 4])).select(['run_start_time', 'run_type']).collect()
    assert result.column('run_start_time') == (
        datetime(2015, 12, 8, 15, 47, 20, 125000, tzinfo=timezone.utc),
        None,
        None,
    )
    assert result.column('run_type') == (None, None, None)
    valid_end = True
    filtered = (
        gx.runs(gluex.RunSelection.range(2, 4))
        .where(gx.conditions['is_valid_run_end'].eq(valid_end))
        .select(['event_count'])
        .collect()
    )
    assert filtered.runs.numbers == (4,)
    assert filtered.column('event_count') == (5000,)
    assert filtered.runs.report.unknown_runs == (3,)
    assert len(filtered.provenance.runs.predicates) == 1
