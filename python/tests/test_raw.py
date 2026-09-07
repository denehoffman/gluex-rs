"""Read-only SQL enforcement through both configured source APIs."""

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
