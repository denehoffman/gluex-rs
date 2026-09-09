"""Read-only SQL enforcement through both configured source APIs."""

import signal
import subprocess
import sys
import threading
import time

import gluex
import pytest


@pytest.mark.parametrize('source', ['rcdb', 'ccdb'])
def test_raw_reads_and_enforcement(source: str) -> None:
    reader = getattr(gluex.open().sources, source)
    result = reader.raw(
        'SELECT ? AS n, ? AS text, ? AS bytes, ? AS missing, ? AS real',
        parameters=[2, "x'; DROP TABLE runs; --", b'\x00\xff', None, 1.5],
    )
    assert tuple(column.name for column in result.columns) == ('n', 'text', 'bytes', 'missing', 'real')
    assert result.rows[0].values == (2, "x'; DROP TABLE runs; --", b'\x00\xff', None, 1.5)
    with pytest.raises(AttributeError):
        result.rows[0].values = ()
    before = tuple(row.values for row in reader.raw('SELECT name, sql FROM sqlite_schema ORDER BY name').rows)
    for sql in [
        'PRAGMA query_only=OFF',
        "ATTACH ':memory:' AS extra",
        'CREATE TEMP TABLE x (a)',
        'SELECT 1; SELECT 2',
        'DELETE FROM schema_versions',
        'PRAGMA writable_schema=ON',
    ]:
        with pytest.raises(RuntimeError, match='read-only'):
            reader.raw(sql)
    assert reader.raw('PRAGMA query_only').rows[0].values == (1,)
    assert tuple(row.values for row in reader.raw('SELECT name, sql FROM sqlite_schema ORDER BY name').rows) == before
    assert reader.raw('WITH t(x) AS (SELECT 2) SELECT x FROM t').rows[0].values == (2,)


def test_raw_timeout_releases_reader() -> None:
    reader = gluex.open().sources.rcdb
    expensive = """
        WITH RECURSIVE values_(n) AS (
            SELECT 0 UNION ALL SELECT n + 1 FROM values_ WHERE n < 100000000
        )
        SELECT sum(n) FROM values_
    """
    with pytest.raises(RuntimeError, match='interrupted'):
        reader.raw(expensive, timeout=0.0)
    assert reader.raw('SELECT 42').rows[0].values == (42,)


def test_raw_database_work_releases_the_gil() -> None:
    reader = gluex.open().sources.rcdb
    progressed = threading.Event()

    def background() -> None:
        time.sleep(0.02)
        progressed.set()

    thread = threading.Thread(target=background)
    thread.start()
    expensive = """
        WITH RECURSIVE values_(n) AS (
            SELECT 0 UNION ALL SELECT n + 1 FROM values_ WHERE n < 100000000
        )
        SELECT sum(n) FROM values_
    """
    with pytest.raises(RuntimeError, match='interrupted'):
        reader.raw(expensive, timeout=0.2)
    assert progressed.is_set()
    thread.join()


@pytest.mark.skipif(sys.platform == 'win32', reason='uses POSIX signal delivery')
def test_keyboard_interrupt_stops_database_work() -> None:
    script = '''
import gluex
try:
    gluex.open().sources.rcdb.raw("""
        WITH RECURSIVE values_(n) AS (
            SELECT 0 UNION ALL SELECT n + 1 FROM values_ WHERE n < 100000000
        ) SELECT sum(n) FROM values_
    """)
except KeyboardInterrupt:
    print("interrupted")
else:
    raise SystemExit("query unexpectedly completed")
'''
    process = subprocess.Popen(  # noqa: S603 - executable and script are test-controlled
        [sys.executable, '-c', script],
        stdout=subprocess.PIPE,
        stderr=subprocess.PIPE,
        text=True,
    )
    time.sleep(0.1)
    process.send_signal(signal.SIGINT)
    stdout, stderr = process.communicate(timeout=10)
    assert process.returncode == 0, stderr
    assert stdout.strip() == 'interrupted'
