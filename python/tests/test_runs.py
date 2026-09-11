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
import polars as pl
import pytest


def test_recorded_membership_and_catalog(rcdb_path: Path) -> None:
    gx = gluex.open(rcdb=rcdb_path, ccdb=gluex.DISABLED)
    selection = gluex.RunSelection.runs([5, 2, 1, 5, 3])
    query = gx.runs.select(selection)
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
    catalog = gx.runs.conditions
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


@pytest.mark.parametrize(
    ('scope', 'expected'),
    [
        (2, (2,)),
        ([5, 2, 3, 2], (2, 3, 5)),
        (range(2, 5), (2, 3, 4)),
        (range(5, 2), ()),
        (gluex.RunPeriod.RP2018_08, (50685, 50697)),
        ('F18', (50685, 50697)),
        (gluex.RunSelection.runs([2, 4]), (2, 4)),
    ],
)
def test_runs_facade_coerces_common_scopes(scope, expected) -> None:
    gx = gluex.open()
    assert gx.runs.select(scope).collect().numbers == expected
    assert gx.runs.conditions['event_count'].name == 'event_count'


def test_runs_facade_between_and_variadic_columns_are_unambiguous() -> None:
    gx = gluex.open()
    query = gx.runs.between(2, 4)
    assert query.collect().numbers == (2, 3, 4)
    result = query.columns('event_count', 'is_valid_run_end').collect()
    assert result.provenance.fields == ('event_count', 'is_valid_run_end')
    assert gx.runs.select(query).collect().numbers == (2, 3, 4)
    assert gx.runs.select(2, 4).collect().numbers == (2, 4)
    with pytest.raises(TypeError):
        gx.runs(2)  # ty: ignore[call-non-callable]
    assert not hasattr(gluex.RunSelection, 'range')
    assert not hasattr(query, 'select')


def test_run_period_is_a_value_object_with_a_rest_selector() -> None:
    period = gluex.RunPeriod('s17')
    configured = period.rest(5)
    reconstruction = gluex.ReconstructionSelection.periods(configured)

    assert str(period) == 'S17'
    assert not hasattr(period, 'rest_version')
    assert not hasattr(period, 'variation')
    assert not hasattr(period, 'calibration_time')
    assert isinstance(configured, gluex.CalibratedRunPeriod)
    assert configured.period == period
    assert configured.rest_version == 5
    assert configured.variation == 'recon_2017_01_ver05'
    assert configured.calibration_time.isoformat() == '2025-11-26T13:41:24+00:00'
    assert reconstruction.resolve(period) == ('recon_2017_01_ver05', '2025-11-26T13:41:24+00:00')
    overridden = gluex.ReconstructionSelection.periods(period.rest(5, variation='custom'))
    assert overridden.resolve(period)[0] == 'custom'


def test_calibrated_run_period_supports_immutable_variation_and_timestamp_selection() -> None:
    period = gluex.RunPeriod('s17')
    timestamp = datetime(2024, 6, 1, 12, 30, tzinfo=timezone.utc)

    original = period.rest(5)
    overridden = period.rest(5, variation='custom')
    historical = period.at(timestamp, variation='historical')

    assert original.variation == 'recon_2017_01_ver05'
    assert overridden.variation == 'custom'
    assert overridden.calibration_time == original.calibration_time
    assert historical.rest_version is None
    assert historical.variation == 'historical'
    assert historical.calibration_time == timestamp
    assert historical.period == period
    with pytest.raises(TypeError):
        period.rest(5, 'custom')  # ty: ignore[too-many-positional-arguments]
    assert not hasattr(overridden, 'with_variation')


def test_run_aliases_have_one_typed_discoverable_home() -> None:
    gx = gluex.open()
    aliases = gx.runs.aliases
    query = gx.runs.select(['S16', 'F18']).where(aliases.approved_production)
    assert isinstance(aliases.is_coherent_beam, gluex.RunPredicate)
    assert isinstance(query, gluex.RunQuery)
    assert query.collect().numbers == (50685, 50697)
    assert 'aliases=available' in repr(gx.runs)
    assert not hasattr(gluex, 'approved_production')


def test_select_accepts_sequences_of_run_periods(rcdb_path: Path) -> None:
    gx = gluex.open(rcdb=rcdb_path, ccdb=gluex.DISABLED)

    periods = [gluex.RunPeriod('2016-02'), gluex.RunPeriod('2018_08')]
    assert gx.runs.select(periods).collect().numbers == (10204, 50685, 50697)
    assert gx.runs.select(['2016_02', '2018-08']).collect().numbers == (10204, 50685, 50697)


def test_session_aliases_compose_with_dynamic_conditions(rcdb_path: Path) -> None:
    gx = gluex.open(rcdb=rcdb_path, ccdb=gluex.DISABLED)
    current = gx.runs.conditions['beam_current']

    runs = gx.runs.select('2018-08').where(gx.runs.aliases.is_field_on & (current > 10.0)).collect()

    assert isinstance(runs, gluex.RunSet)


def test_run_query_timeout_is_immutable() -> None:
    query = gluex.open().runs.select(gluex.RunSelection.between(2, 5))
    with pytest.raises(TimeoutError, match='timed out'):
        query.timeout(0.0).collect()
    assert query.collect().numbers == (2, 3, 4, 5)


def test_run_timeout_starts_at_each_terminal_and_excludes_stream_idle() -> None:
    query = gluex.open().runs.select(gluex.RunSelection.between(2, 5)).timeout(0.1)
    time.sleep(0.15)
    assert query.count() == 4
    time.sleep(0.15)
    assert query.count() == 4

    stream = query.stream(chunk_size=1)
    time.sleep(0.15)
    first = next(stream)
    assert first is not None
    assert first.numbers == (2,)
    time.sleep(0.15)
    second = next(stream)
    assert second is not None
    assert second.numbers == (3,)


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
    gx.runs.between(200000, 500000).count()
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
        (gluex.RunSelection.between(2, 4), (2, 3, 4)),
        (gluex.RunSelection.between(4, 2), ()),
        (gluex.RunSelection.runs([]), ()),
        (gluex.RunSelection.period(gluex.RunPeriod.RP2018_08), (50685, 50697)),
        (gluex.RunSelection.between(-(2**63), 2**63 - 1), (2, 3, 4, 5, 1100, 10204, 50685, 50697)),
    ],
)
def test_numeric_scope(selection: gluex.RunSelection, expected: tuple[int, ...]) -> None:
    assert gluex.open().runs.select(selection).collect().numbers == expected


def test_multiple_run_scopes_are_unioned(rcdb_path) -> None:
    gx = gluex.connect(rcdb=rcdb_path, ccdb=gluex.DISABLED)
    result = gx.runs.select(2, 'f18', [2, 3]).collect()
    assert result.numbers == (2, 3, 50685, 50697)


def test_missing_rcdb_remains_discoverable() -> None:
    gx = gluex.open(rcdb=gluex.DISABLED, ccdb=gluex.DISABLED)
    assert 'runs' in dir(gx)
    assert 'conditions' in dir(gx.runs)
    assert 'conditions' not in dir(gx)
    with pytest.raises(gluex.MissingCapabilityError, match='RCDB'):
        gx.runs.select(2)
    with pytest.raises(gluex.MissingCapabilityError, match='RCDB'):
        _ = gx.runs.conditions


def test_public_exception_hierarchy_is_catchable() -> None:
    assert issubclass(gluex.MissingCapabilityError, RuntimeError)
    assert issubclass(gluex.ConfigurationError, ValueError)
    assert issubclass(gluex.QueryError, RuntimeError)
    assert issubclass(gluex.DecodeError, gluex.QueryError)
    assert issubclass(gluex.MissingDataError, gluex.QueryError)
    assert issubclass(gluex.CancellationError, gluex.QueryError)
    assert issubclass(gluex.DatabaseTimeoutError, TimeoutError)


def test_condition_projection(rcdb_path: Path) -> None:
    gx = gluex.open(rcdb=rcdb_path, ccdb=gluex.DISABLED)
    query = gx.runs.between(2, 4)
    projected = query.columns('event_count', 'is_valid_run_end')
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
        query.columns('absent')
    with pytest.raises(TypeError):
        query.columns(2)
    with pytest.raises(AttributeError):
        result.runs = ()  # ty: ignore[invalid-assignment]


def test_condition_results_convert_directly_to_polars(rcdb_path: Path) -> None:
    gx = gluex.connect(rcdb=rcdb_path, ccdb=gluex.DISABLED)
    frame = (
        gx.runs.select([2, 3, 4])
        .columns('event_count', 'is_valid_run_end', 'run_start_time', 'run_type')
        .collect()
        .to_polars()
    )

    assert frame.schema == {
        'run_number': pl.UInt32,
        'event_count': pl.Int64,
        'is_valid_run_end': pl.Boolean,
        'run_start_time': pl.Datetime('us', 'UTC'),
        'run_type': pl.String,
    }
    assert frame['run_number'].to_list() == [2, 3, 4]
    assert frame['event_count'].to_list() == [2, 1686, 5000]
    assert frame['is_valid_run_end'].to_list() == [False, None, True]
    assert frame['run_type'].to_list() == [None, None, None]


@pytest.mark.parametrize('text', ['broken', 'garbage 2014 garbage', '2014'])
def test_projected_timestamp_rejects_corruption(rcdb_path, tmp_path, text):
    fixture = tmp_path / 'malformed.sqlite'
    shutil.copyfile(rcdb_path, fixture)
    with sqlite3.connect(fixture) as connection:
        connection.execute('UPDATE conditions SET time_value = ? WHERE id = 7', (text,))
    gx = gluex.open(rcdb=fixture, ccdb=gluex.DISABLED)
    query = gx.runs.select(2).columns('run_start_time')
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
    result = gx.runs.select([2, 3, 4]).columns('run_start_time', 'run_type').collect()
    assert result.column('run_start_time') == (
        datetime(2015, 12, 8, 15, 47, 20, 125000, tzinfo=timezone.utc),
        None,
        None,
    )
    assert result.column('run_type') == (None, None, None)
    valid_end = True
    filtered = (
        gx.runs.between(2, 4)
        .where(gx.runs.conditions['is_valid_run_end'].eq(valid_end))
        .columns('event_count')
        .collect()
    )
    assert filtered.runs.numbers == (4,)
    assert filtered.column('event_count') == (5000,)
    assert filtered.runs.report.unknown_runs == (3,)
    assert len(filtered.provenance.runs.predicates) == 1


def test_streaming_terminals_and_missing_policies(rcdb_path):
    gx = gluex.open(rcdb=rcdb_path, ccdb=gluex.DISABLED)
    query = gx.runs.between(2, 5)
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

    projected = query.columns('event_count', 'is_valid_run_end')
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
