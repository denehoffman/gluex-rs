import shutil
import sqlite3
from datetime import datetime, timezone

import gluex
import polars as pl
import pytest


def test_ccdb_only_catalog_and_series(ccdb_path):
    gx = gluex.open(rcdb=gluex.DISABLED, ccdb=ccdb_path)
    catalog = gx.calibrations
    assert '/TARGET/density' in catalog
    assert catalog.directories['/TARGET'].tables['density'].path == '/TARGET/density'
    table = catalog['/test/demo/mytable']
    assert [column.name for column in table.columns] == ['x', 'y', 'z']
    query = table.for_runs(gluex.RunSelection.between(2, 3))
    assert query.provenance.variation == 'default'
    assert query.provenance.as_of == gx.sources.ccdb.opened_at
    del gx
    series = query.collect()
    assert series.runs == (2, 3)
    assert series[2].assignment_id == 230266
    assert series[2].constant_set_id == 230302
    assert series[2].payload.column('x') == (1.0, 4.0)
    with pytest.raises(AttributeError):
        series[2].assignment_id = 1  # ty: ignore[invalid-assignment]
    missing = catalog['/TARGET/density'].for_runs(gluex.RunSelection.runs([2, 50685])).collect()
    assert missing.report.missing_runs == (2,)
    assert missing[50685].payload.column('density') == (70.92,)
    with pytest.raises(KeyError):
        missing[2]


def test_metadata_is_lazy_and_payload_errors_are_not_omissions(ccdb_path, tmp_path):
    path = tmp_path / 'bad.sqlite'
    shutil.copyfile(ccdb_path, path)
    with sqlite3.connect(path) as connection:
        connection.execute("UPDATE constantSets SET vault = 'broken' WHERE id = 230302")
    gx = gluex.open(rcdb=gluex.DISABLED, ccdb=path)
    table = gx.calibrations['/test/demo/mytable']
    assert len(table.columns) == 3
    query = table.for_runs(gluex.RunSelection.between(-(2**63), 2**63 - 1))
    assert query.provenance.table == table.path
    assert 'CalibrationQuery' in repr(query)
    with pytest.raises(gluex.DecodeError):
        table.for_runs(gluex.RunSelection.runs([2])).collect()
    assert len(table.for_runs(gluex.RunSelection.between(4, 2)).collect()) == 0
    with pytest.raises(RuntimeError, match='CCDB'):
        _ = gluex.open(rcdb=gluex.DISABLED, ccdb=gluex.DISABLED).calibrations


def test_latest_preceding_calibration_carries_across_run_gaps(ccdb_path, tmp_path):
    path = tmp_path / 'sparse.sqlite'
    shutil.copyfile(ccdb_path, path)
    with sqlite3.connect(path) as connection:
        connection.execute('UPDATE runRanges SET runMax = 50685 WHERE id = 2')

    gx = gluex.open(rcdb=gluex.DISABLED, ccdb=path)
    result = gx.calibrations['/TARGET/density'].for_runs([50685, 50697]).collect()

    assert result.runs == (50685, 50697)
    assert result.report.missing_runs == ()
    assert result[50697].assignment_id == result[50685].assignment_id
    assert result[50697].payload.column('density') == (70.92,)


def test_collected_runs_preserve_per_run_calibration_contexts(rcdb_path, ccdb_path):
    gx = gluex.open(rcdb=rcdb_path, ccdb=ccdb_path)
    old = datetime(2019, 1, 1, tzinfo=timezone.utc)
    current = datetime(2021, 1, 1, tzinfo=timezone.utc)
    old_run = gx.runs.select(50685).at(old, variation='default')
    current_run = gx.runs.select(50697).at(current, variation='default')

    runs = gx.runs.select([old_run, current_run]).collect()
    result = gx.calibrations.select(runs).tables('/test/demo/mytable').collect()

    assert result['/test/demo/mytable'][50685].assignment_id == 76
    assert result['/test/demo/mytable'][50697].assignment_id == 230266

    conflicting = gx.runs.select(
        [
            gx.runs.select(50685).at(old),
            gx.runs.select(50685).at(current),
        ]
    ).collect()
    with pytest.raises(ValueError, match='conflicting calibration contexts'):
        gx.calibrations.select(conflicting)


def test_historical_query_selectors(ccdb_path):

    gx = gluex.open(rcdb=gluex.DISABLED, ccdb=ccdb_path)
    query = gx.calibrations['/test/demo/mytable'].for_runs(gluex.RunSelection.between(2, 3))
    old = query.with_variation('mc').as_of(datetime(2013, 2, 22, 13, 40, 35, tzinfo=timezone.utc))
    result = old.collect()
    assert result[2].assignment_id == 76
    assert result[2].variation == 'default'
    assert result[2].payload.column('x') == (0.0, 3.0)
    assert old.provenance.variation == 'mc'
    assert query.provenance.variation == 'default'
    assert query.collect()[2].assignment_id == 230266
    with pytest.raises((TypeError, ValueError)):
        query.as_of(datetime(2020, 1, 1))  # noqa: DTZ001 — naive dates must be rejected


def test_concise_calibration_query_forms(rcdb_path, ccdb_path):
    gx = gluex.open(rcdb=rcdb_path, ccdb=ccdb_path)
    table = gx.calibrations['/test/demo/mytable']
    cutoff = datetime(2013, 2, 22, 13, 40, 35, tzinfo=timezone.utc)

    concise = table.for_runs([2, 3], variation='mc', as_of=cutoff)
    fluent = table.for_runs(gluex.RunSelection.runs([2, 3])).with_variation('mc').as_of(cutoff)
    assert concise.collect().runs == fluent.collect().runs
    assert concise.collect()[2].assignment_id == fluent.collect()[2].assignment_id
    assert table.for_runs(2).collect().runs == (2,)
    assert table.for_runs(range(2, 4)).collect().runs == (2, 3)
    assert table.for_runs(gx.runs.select([2, 3])).collect().runs == (2, 3)
    assert table.for_runs(gx.runs.select([2, 3]).collect()).collect().runs == (2, 3)

    strict = gx.calibrations['/TARGET/density'].for_runs([2, 50685], missing_policy='strict')
    with pytest.raises(gluex.MissingDataError):
        strict.collect()
    filled = (
        gx.calibrations['/TARGET/density'].for_runs([2, 50685], missing_policy='fallback', fallback_run=50685).collect()
    )
    assert filled.report.substitutions == ((2, 50685),)

    with pytest.raises(ValueError, match='fallback_run'):
        table.for_runs([2], missing_policy='fallback')
    with pytest.raises(ValueError, match='conflict'):
        table.for_runs([2], variation='default', reconstruction=gluex.ReconstructionSelection.latest())


def test_concise_reconstruction_mappings_and_payload_indexing(rcdb_path, ccdb_path):
    gx = gluex.open(rcdb=rcdb_path, ccdb=ccdb_path)
    explicit = gluex.ReconstructionSelection.periods(
        {gluex.RunPeriod.RP2018_08: gluex.RESTVersionSelection.version(gluex.RunPeriod.RP2018_08, 2)}
    )
    concise = gluex.ReconstructionSelection.periods({'F18': 2})
    table = gx.calibrations['/TARGET/density']
    assert table.for_runs(50685, reconstruction={'F18': 2}).collect()[50685].assignment_id == (
        table.for_runs(50685, reconstruction=explicit).collect()[50685].assignment_id
    )
    assert table.for_runs(50685, reconstruction=concise).collect()[50685].assignment_id == (
        table.for_runs(50685, reconstruction=explicit).collect()[50685].assignment_id
    )
    configured_period = gluex.RunPeriod('f18').rest(2)
    configured = table.for_runs(configured_period).collect()
    assert (
        configured[50685].assignment_id == table.for_runs(50685, reconstruction=explicit).collect()[50685].assignment_id
    )
    assert next(iter(configured.provenance.resolved_reconstruction.values()))[0] == 'default'
    with pytest.raises(ValueError, match='duplicate reconstruction'):
        gluex.ReconstructionSelection.periods({'F18': 2, gluex.RunPeriod.RP2018_08: 2})
    with pytest.raises(ValueError, match='conflicts with mapping key'):
        gluex.ReconstructionSelection.periods({'F18': gluex.RESTVersionSelection.version(gluex.RunPeriod.RP2019_01, 1)})
    with pytest.raises(ValueError, match='REST'):
        gluex.ReconstructionSelection.periods({'F18': 999})
    assert 'periods' in repr(concise)
    assert 'run_scope' in repr(table)

    payload = gx.calibrations['/test/demo/mytable'].for_runs(2).collect()[2].payload
    assert payload['x'] == payload.column('x') == (1.0, 4.0)
    with pytest.raises(KeyError):
        _ = payload['unknown']


@pytest.mark.parametrize(
    'change',
    [
        "UPDATE assignments SET created = 'not-a-date' WHERE id = 230266",
        "UPDATE assignments SET created = 'garbage 2014 garbage' WHERE id = 230266",
        'UPDATE assignments SET runRangeId = 999 WHERE id = 230266',
        "UPDATE variations SET parentId = 999 WHERE name = 'mc'",
        "UPDATE variations SET parentId = 2 WHERE name = 'mc'",
        "UPDATE columns SET columnType = 'invalid' WHERE id = 641",
    ],
)
def test_historical_errors_are_contextual(ccdb_path, tmp_path, change):

    fixture = tmp_path / 'malformed.sqlite'
    shutil.copyfile(ccdb_path, fixture)
    with sqlite3.connect(fixture) as connection:
        connection.executescript(change)
    gx = gluex.open(rcdb=gluex.DISABLED, ccdb=fixture)
    query = gx.calibrations['/test/demo/mytable'].for_runs(gluex.RunSelection.runs([2])).with_variation('mc')
    with pytest.raises(RuntimeError, match='/test/demo/mytable'):
        query.collect()
    with pytest.raises(RuntimeError, match='/test/demo/mytable'):
        next(query.strict().stream(chunk_size=1))
    with pytest.raises(RuntimeError, match='/test/demo/mytable'):
        next(query.fallback_to(2).stream(chunk_size=1))


def test_nested_variations_use_assignment_order(ccdb_path, tmp_path):

    fixture = tmp_path / 'history.sqlite'
    shutil.copyfile(ccdb_path, fixture)
    with sqlite3.connect(fixture) as connection:
        connection.executescript("""
            INSERT INTO variations (id, name, parentId) VALUES (3, 'nested', 2);
            INSERT INTO runRanges (id, runMin, runMax) VALUES (3, 2, 3);
            INSERT INTO assignments (id, created, variationId, runRangeId, constantSetId) VALUES
                (300000, '2014-01-01 00:00:00', 1, 1, 76),
                (300001, '2015-01-01 00:00:00', 2, 3, 230302);
        """)
    gx = gluex.open(rcdb=gluex.DISABLED, ccdb=fixture)
    series = (
        gx.calibrations['/test/demo/mytable']
        .for_runs(gluex.RunSelection.between(1, 4))
        .with_variation('nested')
        .collect()
    )
    assert [series[run].assignment_id for run in series] == [300000, 300001, 300001, 300000]
    assert series[2].variation == 'mc'
    assert series[1].variation == 'default'


def test_reversed_run_ranges_are_ignored_as_empty_intervals(ccdb_path, tmp_path):
    fixture = tmp_path / 'reversed.sqlite'
    shutil.copyfile(ccdb_path, fixture)
    with sqlite3.connect(fixture) as connection:
        connection.executescript("""
            INSERT INTO runRanges (id, runMin, runMax) VALUES (10, 100900, 100899);
            INSERT INTO assignments (id, created, variationId, runRangeId, constantSetId)
            VALUES (300000, '2024-10-18 12:27:33', 1, 10, 230302);
        """)
    gx = gluex.open(rcdb=gluex.DISABLED, ccdb=fixture)
    series = gx.calibrations['/test/demo/mytable'].for_runs(2).collect()
    assert series[2].assignment_id == 230266


def test_calibration_series_converts_directly_to_polars(ccdb_path):
    gx = gluex.connect(rcdb=gluex.DISABLED, ccdb=ccdb_path)
    frame = gx.calibrations['/test/demo/mytable'].for_runs([2, 3]).collect().to_polars()
    assert frame.schema == {
        'run_number': pl.UInt32,
        'x': pl.Float64,
        'y': pl.Float64,
        'z': pl.Float64,
    }
    assert frame.to_dict(as_series=False) == {
        'run_number': [2, 2, 3, 3],
        'x': [1.0, 4.0, 1.0, 4.0],
        'y': [2.0, 5.0, 2.0, 5.0],
        'z': [3.0, 6.0, 3.0, 6.0],
    }


def test_selector_first_multi_table_results_convert_to_nested_polars(ccdb_path):
    gx = gluex.connect(rcdb=gluex.DISABLED, ccdb=ccdb_path)
    result = gx.calibrations.select([50685, 50697]).tables('/test/demo/mytable', '/TARGET/density').collect()

    assert result.tables == ('/test/demo/mytable', '/TARGET/density')
    assert result.runs == (50685, 50697)
    assert result['/test/demo/mytable'].runs == (50685, 50697)
    frame = result.to_polars()
    assert frame.schema['run_number'] == pl.UInt32
    assert isinstance(frame.schema['/test/demo/mytable'], pl.Struct)
    assert frame['/test/demo/mytable'].struct.field('x').to_list() == [[1.0, 4.0], [1.0, 4.0]]
    assert frame.schema['/TARGET/density'] == pl.Struct(
        {'density': pl.Float64, 'densityErr': pl.Float64}
    )
    assert frame['/TARGET/density'].struct.field('density').to_list() == [70.92, 70.92]


def test_selector_first_context_has_one_configuration_site(ccdb_path):
    gx = gluex.connect(rcdb=gluex.DISABLED, ccdb=ccdb_path)
    query = gx.calibrations.select([2, 3], variation='default').tables('/TARGET/density')
    assert query.collect().runs == (2, 3)
    with pytest.raises(ValueError, match='would also overwrite'):
        gx.calibrations.select(gluex.RunPeriod('s17').rest(5), variation='custom')


def test_calibrated_scope_overlap_is_actionable_and_exclusions_enable_override(ccdb_path):
    gx = gluex.connect(rcdb=gluex.DISABLED, ccdb=ccdb_path)
    timestamp = datetime(2024, 6, 1, tzinfo=timezone.utc)
    s17 = gluex.RunPeriod('s17').rest(5)
    custom = gluex.RunSelection.runs([30274, 30275]).at(timestamp, variation='mc')

    with pytest.raises(ValueError, match=r'runs \[30274, 30275\].*excluding\(30274, 30275\)'):
        gx.calibrations.select(s17, custom)

    query = gx.calibrations.select(
        s17.excluding(30274, 30275),
        custom,
    ).tables('/TARGET/density')
    assert isinstance(query, gluex.CalibrationTablesQuery)

    historical = datetime(2013, 2, 22, 13, 40, 35, tzinfo=timezone.utc)
    result = (
        gx.calibrations.select(
            gluex.RunSelection.runs([2]).at(historical, variation='mc'),
            [3],
        )
        .tables('/test/demo/mytable')
        .collect()
    )
    table = result['/test/demo/mytable']
    assert len(table.contexts) == 2
    assert {context.variation for context in table.contexts} == {'default', 'mc'}
    assert table.to_polars()['run_number'].to_list() == [2, 2, 3, 3]


def test_explicit_run_selection_can_use_rest_context() -> None:
    selection = gluex.RunSelection.runs([30274, 30275]).rest(5)
    assert isinstance(selection, gluex.CalibratedRunSelection)
    assert 'S17' in repr(selection)


def test_multiple_periods_keep_independent_calibration_contexts(ccdb_path, tmp_path):
    fixture = tmp_path / 'periods.sqlite'
    shutil.copyfile(ccdb_path, fixture)
    with sqlite3.connect(fixture) as connection:
        connection.executescript("""
            INSERT INTO runRanges (id, runMin, runMax) VALUES
                (10, 30000, 30000),
                (11, 40000, 40000);
            INSERT INTO assignments (id, created, variationId, runRangeId, constantSetId) VALUES
                (300000, '2017-01-01 00:00:00', 1, 10, 230302),
                (300001, '2018-01-01 00:00:00', 1, 11, 230302);
        """)
    gx = gluex.connect(rcdb=gluex.DISABLED, ccdb=fixture)
    s17 = gluex.RunPeriod('s17')
    s18 = gluex.RunPeriod('s18')
    f18 = gluex.RunPeriod('f18')
    series = (
        gx.calibrations['/test/demo/mytable']
        .for_runs(
            s17.rest(4),
            s18,
            f18.rest(2),
        )
        .collect()
    )
    assert len(series) == 30_000
    assert series.runs[0] == 30000
    assert series.runs[-1] == 59999
    assert series[30000].assignment_id == 300000
    assert series[40000].assignment_id == 300001
    contexts = series.provenance.resolved_reconstruction
    assert contexts['RunPeriod-2017-01'][1] == s17.rest(4).calibration_time
    assert contexts['RunPeriod-2018-01'][1] == gx.sources.ccdb.opened_at
    assert contexts['RunPeriod-2018-08'][1] == f18.rest(2).calibration_time

    with pytest.raises(ValueError, match='conflicting calibration contexts'):
        gx.calibrations['/test/demo/mytable'].for_runs(s17, s17.rest(4))


def test_fractional_cutoff_is_inclusive(ccdb_path, tmp_path):
    fixture = tmp_path / 'fractional.sqlite'
    shutil.copyfile(ccdb_path, fixture)
    with sqlite3.connect(fixture) as connection:
        connection.execute("UPDATE assignments SET created = '2020-01-15 13:08:18.500' WHERE id = 230266")
    gx = gluex.open(rcdb=gluex.DISABLED, ccdb=fixture)
    query = gx.calibrations['/test/demo/mytable'].for_runs(gluex.RunSelection.runs([2]))
    before = datetime(2020, 1, 15, 13, 8, 18, 499000, tzinfo=timezone.utc)
    exact = datetime(2020, 1, 15, 13, 8, 18, 500000, tzinfo=timezone.utc)
    assert query.as_of(before).collect()[2].assignment_id == 76
    assert query.as_of(exact).collect()[2].assignment_id == 230266
    assert query.as_of(exact).collect()[2].created == exact


def test_composed_streaming_reconstruction_and_missing_policies(rcdb_path, ccdb_path):
    gx = gluex.open(rcdb=rcdb_path, ccdb=ccdb_path)
    runs = gx.runs.between(50685, 50697)
    reconstruction = gluex.ReconstructionSelection.periods(
        {gluex.RunPeriod.RP2018_08: gluex.RESTVersionSelection.version(gluex.RunPeriod.RP2018_08, 2)}
    )
    query = gx.calibrations['/TARGET/density'].for_runs(runs).with_reconstruction(reconstruction)
    result = query.collect()
    assert result.runs == (50685, 50697)
    assert result.provenance.runs is not None
    assert len(result.provenance.resolved_reconstruction) == 1
    assert result.provenance.run_report is not None
    assert result.provenance.run_report.complete is True
    with pytest.raises(RuntimeError, match='conflict'):
        query.with_variation('default').collect()

    shared = gx.calibrations['/test/demo/mytable'].for_runs(gluex.RunSelection.between(1, 4))
    chunks = [chunk for chunk in shared.stream(chunk_size=2) if chunk is not None]
    assert tuple(run for chunk in chunks for run in chunk) == shared.collect().runs
    assert chunks[-1].report.complete is True
    first = shared.first()
    assert first is not None
    assert first.runs == (1,)
    assert shared.count() == 4
    abandoned = shared.stream(chunk_size=1)
    next(abandoned)
    del abandoned
    assert shared.count() == 4

    missing = gx.calibrations['/TARGET/density'].for_runs(gluex.RunSelection.runs([2, 50685]))
    with pytest.raises(gluex.MissingDataError, match='missing'):
        missing.strict().collect()
    filled = missing.fallback_to(50685).collect()
    assert filled[2].constant_set_id == filled[50685].constant_set_id
    assert filled.report.substitutions == ((2, 50685),)
    assert filled.provenance.missing_policy == 'fallback'
    assert filled.provenance.fallback_run == 50685
