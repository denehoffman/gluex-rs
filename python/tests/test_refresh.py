import shutil

import gluex
import pytest


def test_refresh_keeps_old_queries_and_results(ccdb_path):
    gx = gluex.open(rcdb=gluex.DISABLED, ccdb=ccdb_path)
    old_table = gx.calibrations['/test/demo/mytable']
    query = old_table.for_runs(gluex.RunSelection.runs([2]))
    result = query.collect()
    captured = query.provenance.as_of
    gx.refresh()
    new = gx.calibrations['/test/demo/mytable'].for_runs(gluex.RunSelection.runs([2]))
    assert new.provenance.as_of > captured
    assert query.provenance.as_of == captured
    assert result.provenance.as_of == captured
    assert old_table.for_runs(gluex.RunSelection.runs([2])).provenance.as_of == captured
    assert query.collect()[2].assignment_id == 230266


def test_refresh_uses_captured_sources_and_is_atomic(rcdb_path, ccdb_path, monkeypatch, tmp_path):

    copied = tmp_path / 'ccdb.sqlite'
    shutil.copyfile(ccdb_path, copied)
    gx = gluex.open(rcdb=rcdb_path, ccdb=copied)
    old = gx.sources
    query = gx.runs(gluex.RunSelection.runs([2])).select(['event_count'])
    monkeypatch.setenv('RCDB_CONNECTION', '/missing/rcdb.sqlite')
    monkeypatch.setenv('CCDB_CONNECTION', '/missing/ccdb.sqlite')
    gx.refresh()
    assert query.collect()[2, 'event_count'] == 2
    hidden = tmp_path / 'hidden.sqlite'
    copied.rename(hidden)
    try:
        with pytest.raises(ValueError, match='CCDB'):
            gx.refresh()
    finally:
        hidden.rename(copied)
    assert gx.capabilities.rcdb
    assert gx.capabilities.ccdb
    assert gx.runs(gluex.RunSelection.runs([2])).collect().numbers == (2,)
    assert old.rcdb.connection_path == str(rcdb_path.resolve())
