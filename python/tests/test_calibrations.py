import shutil
import sqlite3
from datetime import datetime, timezone

import gluex
import pytest


def test_ccdb_only_catalog_and_series(ccdb_path):
    gx = gluex.open(rcdb=gluex.DISABLED, ccdb=ccdb_path)
    catalog = gx.calibrations
    assert '/TARGET/density' in catalog
    assert catalog.directories['/TARGET'].tables['density'].path == '/TARGET/density'
    table = catalog['/test/demo/mytable']
    assert [column.name for column in table.columns] == ['x', 'y', 'z']
    query = table.for_runs(gluex.RunSelection.range(2, 3))
    assert query.provenance.variation == 'default'
    assert query.provenance.as_of == gx.sources.ccdb.opened_at
    del gx
    series = query.collect()
    assert series.runs == (2, 3)
    assert series[2].assignment_id == 230266
    assert series[2].constant_set_id == 230302
    assert series[2].payload.column('x') == (1.0, 4.0)
    with pytest.raises(AttributeError):
        series[2].assignment_id = 1
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
    query = table.for_runs(gluex.RunSelection.range(-(2**63), 2**63 - 1))
    assert query.provenance.table == table.path
    assert 'CalibrationQuery' in repr(query)
    with pytest.raises(RuntimeError):
        table.for_runs(gluex.RunSelection.runs([2])).collect()
    assert len(table.for_runs(gluex.RunSelection.range(4, 2)).collect()) == 0
    with pytest.raises(RuntimeError, match='CCDB'):
        _ = gluex.open(rcdb=gluex.DISABLED, ccdb=gluex.DISABLED).calibrations


def test_historical_query_selectors(ccdb_path):

    gx = gluex.open(rcdb=gluex.DISABLED, ccdb=ccdb_path)
    query = gx.calibrations['/test/demo/mytable'].for_runs(gluex.RunSelection.range(2, 3))
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


@pytest.mark.parametrize(
    'change',
    [
        "UPDATE assignments SET created = 'not-a-date' WHERE id = 230266",
        "UPDATE assignments SET created = 'garbage 2014 garbage' WHERE id = 230266",
        'UPDATE assignments SET runRangeId = 999 WHERE id = 230266',
        'UPDATE runRanges SET runMin = 9, runMax = 1 WHERE id = 1',
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
        .for_runs(gluex.RunSelection.range(1, 4))
        .with_variation('nested')
        .collect()
    )
    assert [series[run].assignment_id for run in series] == [300000, 300001, 300001, 300000]
    assert series[2].variation == 'mc'
    assert series[1].variation == 'default'


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
