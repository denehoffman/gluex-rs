import shutil
import sqlite3

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
