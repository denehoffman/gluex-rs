"""Recorded membership and immutable Condition Definition discovery."""

import shutil
import signal
import sqlite3
import subprocess
import sys
import time
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
    assert result.provenance.source_identity.value == result.provenance.source
    assert str(result.provenance.source_identity) == result.provenance.source
    assert result.report.accounting.complete
    assert result.report.accounting.evaluated_runs == (2, 3, 5)
    assert result.report.accounting.omissions == ()
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
        catalog['event_count'].name = 'changed'  # ty: ignore[invalid-assignment]
    with pytest.raises(AttributeError):
        result.numbers = ()  # ty: ignore[invalid-assignment]
    del gx
    assert tuple(query.collect()) == (2, 3, 5)


def test_run_query_timeout_is_immutable() -> None:
    query = gluex.open().runs(gluex.RunSelection.range(2, 5))
    with pytest.raises(RuntimeError, match='interrupted'):
        query.timeout(0.0).collect()
    assert query.collect().numbers == (2, 3, 4, 5)


@pytest.mark.skipif(sys.platform == 'win32', reason='uses POSIX signal delivery')
def test_keyboard_interrupt_stops_domain_evaluation(rcdb_path: Path, tmp_path: Path) -> None:
    fixture = tmp_path / 'large.sqlite'
    shutil.copyfile(rcdb_path, fixture)
    with sqlite3.connect(fixture) as connection:
        connection.execute("""
            WITH RECURSIVE numbers(n) AS (
                SELECT 200000 UNION ALL SELECT n + 1 FROM numbers WHERE n < 500000
            ) INSERT INTO runs(number) SELECT n FROM numbers
        """)
    script = """
import gluex
import sys
try:
    gx = gluex.open(rcdb=sys.argv[1], ccdb=gluex.DISABLED)
    gx.runs(gluex.RunSelection.range(200000, 500000)).count()
except KeyboardInterrupt:
    print("interrupted")
else:
    raise SystemExit("query unexpectedly completed")
"""
    process = subprocess.Popen(  # noqa: S603 - executable, script, and path are test-controlled
        [sys.executable, '-c', script, str(fixture)],
        stdout=subprocess.PIPE,
        stderr=subprocess.PIPE,
        text=True,
    )
    time.sleep(0.05)
    process.send_signal(signal.SIGINT)
    stdout, stderr = process.communicate(timeout=10)
    assert process.returncode == 0, stderr
    assert stdout.strip() == 'interrupted'


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
        query.select([2])  # ty: ignore[invalid-argument-type]
    with pytest.raises(AttributeError):
        result.runs = ()  # ty: ignore[invalid-assignment]


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
    with pytest.raises(RuntimeError, match='run_start_time at run 2'):
        next(query.strict().stream(chunk_size=1))
    with pytest.raises(RuntimeError, match='run_start_time at run 2'):
        next(query.fill('run_start_time', value=datetime.now(timezone.utc)).stream(chunk_size=1))


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


def test_streaming_terminals_and_missing_policies(rcdb_path):
    gx = gluex.open(rcdb=rcdb_path, ccdb=gluex.DISABLED)
    query = gx.runs(gluex.RunSelection.range(2, 5))
    chunks = [chunk for chunk in query.stream(chunk_size=2) if chunk is not None]
    assert tuple(run for chunk in chunks for run in chunk) == query.collect().numbers
    assert chunks[0].report.complete is False
    assert chunks[-1].report.complete is True
    assert query.first() == 2
    assert query.count() == 4
    with pytest.raises(RuntimeError, match='exactly one'):
        query.one()
    abandoned = query.stream(chunk_size=1)
    next(abandoned)
    del abandoned
    assert query.count() == 4

    projected = query.select(['event_count', 'is_valid_run_end'])
    projected_chunks = [chunk for chunk in projected.stream(chunk_size=2) if chunk is not None]
    assert projected_chunks[0].runs.numbers == (2, 3)
    first = projected.first()
    assert first is not None
    assert first.runs.numbers == (2,)
    assert projected.count() == 4
    with pytest.raises(RuntimeError, match='missing'):
        projected.strict().collect()
    filled = projected.fill('is_valid_run_end', value=False).collect()
    assert filled[3, 'is_valid_run_end'] is False
    assert filled.report.substitutions == ((3, 'is_valid_run_end'), (5, 'is_valid_run_end'))
    assert filled.provenance.missing_policy == 'fallback'
    assert filled.provenance.fallback_fields == ('is_valid_run_end',)
    with pytest.raises((TypeError, ValueError)):
        projected.fill('is_valid_run_end', value='wrong')
