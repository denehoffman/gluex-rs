"""Public configuration and capability behavior for GlueX sessions."""

import os
import shutil
import sqlite3
import subprocess
import sys
from pathlib import Path

import gluex
import pytest


def test_connect_is_the_canonical_session_constructor() -> None:
    gx = gluex.connect(rcdb=gluex.DISABLED, ccdb=gluex.DISABLED)
    assert not gx.capabilities.rcdb
    assert not gx.capabilities.ccdb


def test_disabled_sources_leave_reference_information_available() -> None:
    gx = gluex.open(rcdb=gluex.DISABLED, ccdb=gluex.DISABLED)
    assert not gx.capabilities.rcdb
    assert not gx.capabilities.ccdb
    assert gluex.RunPeriod.RP2018_08 is not None
    assert 'unavailable' in repr(gx)
    with pytest.raises(RuntimeError, match='RCDB_CONNECTION'):
        _ = gx.sources.rcdb
    with pytest.raises(RuntimeError, match='CCDB_CONNECTION'):
        _ = gx.sources.ccdb


@pytest.mark.parametrize('has_rcdb', [False, True])
@pytest.mark.parametrize('has_ccdb', [False, True])
def test_independent_sources_and_surviving_handles(
    rcdb_path: Path,
    ccdb_path: Path,
    *,
    has_rcdb: bool,
    has_ccdb: bool,
) -> None:
    gx = gluex.open(
        rcdb=rcdb_path if has_rcdb else gluex.DISABLED,
        ccdb=ccdb_path if has_ccdb else gluex.DISABLED,
    )
    assert gx.capabilities.rcdb == has_rcdb
    assert gx.capabilities.ccdb == has_ccdb
    assert {'rcdb', 'ccdb'} <= set(dir(gx.sources))
    sources = gx.sources
    del gx
    if has_rcdb:
        assert sources.rcdb.fetch(['event_count'], runs=[2])[2]['event_count'] == 2
    else:
        with pytest.raises(RuntimeError, match='RCDB_CONNECTION'):
            _ = sources.rcdb
    if has_ccdb:
        assert sources.ccdb.fetch('/test/demo/mytable', runs=[2])[2].value('x', 0) == 1.0
    else:
        with pytest.raises(RuntimeError, match='CCDB_CONNECTION'):
            _ = sources.ccdb


def test_environment_precedence_and_capture_in_subprocess(rcdb_path: Path, ccdb_path: Path) -> None:
    environment = os.environ.copy()
    environment['RCDB_CONNECTION'] = str(rcdb_path)
    environment['CCDB_CONNECTION'] = str(ccdb_path)
    code = """
import os
import gluex
import pytest

rcdb, ccdb = os.environ['RCDB_CONNECTION'], os.environ['CCDB_CONNECTION']
for gx in [gluex.open(), gluex.open(rcdb=None, ccdb=None), gluex.GlueX()]:
    assert gx.capabilities.rcdb and gx.capabilities.ccdb
os.environ.pop('RCDB_CONNECTION')
os.environ.pop('CCDB_CONNECTION')
assert not gluex.open().capabilities.rcdb
assert not gluex.open(rcdb=None, ccdb=None).capabilities.ccdb
assert gx.sources.rcdb.fetch(['event_count'], runs=[2])[2]['event_count'] == 2

os.environ['RCDB_CONNECTION'] = '/missing-rcdb.sqlite'
os.environ['CCDB_CONNECTION'] = '/missing-ccdb.sqlite'
gx = gluex.open(rcdb=rcdb, ccdb=ccdb)
assert gx.capabilities.rcdb and gx.capabilities.ccdb
gx = gluex.open(rcdb=gluex.DISABLED, ccdb=gluex.DISABLED)
assert not gx.capabilities.rcdb and not gx.capabilities.ccdb
with pytest.raises(ValueError, match='RCDB'):
    gluex.open(ccdb=gluex.DISABLED)
with pytest.raises(ValueError, match='CCDB'):
    gluex.open(rcdb=gluex.DISABLED)
os.environ['RCDB_CONNECTION'] = ''
with pytest.raises(ValueError, match='empty connection'):
    gluex.open(ccdb=gluex.DISABLED)
"""
    result = subprocess.run(  # noqa: S603 - fixed test code in an isolated interpreter
        [sys.executable, '-c', code],
        env=environment,
        capture_output=True,
        text=True,
        check=False,
    )
    assert result.returncode == 0, result.stdout + result.stderr


@pytest.mark.parametrize('database', ['rcdb', 'ccdb'])
def test_invalid_explicit_source_does_not_fall_back(
    tmp_path: Path,
    database: str,
) -> None:
    invalid = tmp_path / 'invalid.sqlite'
    invalid.write_bytes(b'not a SQLite database')
    for path in [tmp_path / 'missing.sqlite', invalid, tmp_path, 'mysql://localhost/database']:
        kwargs = {'rcdb': gluex.DISABLED, 'ccdb': gluex.DISABLED, database: path}
        with pytest.raises(gluex.ConfigurationError, match=database.upper()):
            gluex.open(**kwargs)


def test_capabilities_are_immutable_and_arguments_are_keyword_only() -> None:
    gx = gluex.open(rcdb=gluex.DISABLED, ccdb=gluex.DISABLED)
    with pytest.raises(AttributeError):
        setattr(gx.capabilities, 'rcdb', True)  # noqa: B010 - exercise read-only properties
    with pytest.raises(AttributeError):
        setattr(gx, 'sources', None)  # noqa: B010
    with pytest.raises(TypeError):
        gluex.open(gluex.DISABLED)  # ty: ignore[too-many-positional-arguments]
    with pytest.raises(TypeError):
        gluex.open(rcdb=False)  # ty: ignore[invalid-argument-type]
    assert repr(gluex.DISABLED) == 'gluex.DISABLED'


def test_ccdb_source_defaults_survive_context_overrides(ccdb_path: Path) -> None:
    gx = gluex.open(rcdb=gluex.DISABLED, ccdb=ccdb_path)
    reader = gx.sources.ccdb
    captured = reader.opened_at
    historical = reader.fetch('/test/demo/mytable', runs=[2], timestamp='2013-02-22 13:40:35')
    assert historical[2].value('x', 0) == 0.0
    assert gx.sources.ccdb.opened_at == captured
    assert captured.tzinfo is not None
    table = reader.root().dir('test').dir('demo').table('mytable')
    del gx, reader
    assert table.fetch(runs=[2])[2].value('x', 0) == 1.0


@pytest.mark.parametrize(
    ('database', 'change'),
    [
        ('ccdb', 'DROP TABLE assignments'),
        ('ccdb', 'ALTER TABLE constantSets DROP COLUMN vault'),
        ('rcdb', 'DROP TABLE runs'),
        ('rcdb', 'ALTER TABLE conditions DROP COLUMN time_value'),
    ],
)
def test_incomplete_schema_fails_at_open(
    rcdb_path: Path,
    ccdb_path: Path,
    tmp_path: Path,
    database: str,
    change: str,
) -> None:
    path = tmp_path / 'incomplete.sqlite'
    shutil.copyfile(rcdb_path if database == 'rcdb' else ccdb_path, path)
    with sqlite3.connect(path) as connection:
        connection.execute(change)
    kwargs = {'rcdb': gluex.DISABLED, 'ccdb': gluex.DISABLED, database: path}
    with pytest.raises(ValueError, match=database.upper()):
        gluex.open(**kwargs)
