"""Static positive and negative probes for the installed database reading APIs."""

from collections.abc import Iterator
from typing import assert_type

import gluex


def typed_reads(gx: gluex.GlueX) -> None:
    catalog = gx.conditions
    assert_type(catalog, gluex.ConditionCatalog)
    assert_type(catalog.keys(), tuple[str, ...])
    assert_type(catalog.items(), tuple[tuple[str, gluex.ConditionDefinition], ...])
    assert_type(iter(catalog), Iterator[str])
    assert_type(catalog['event_count'].value_type, str)
    query = gx.runs(gluex.RunSelection.range(2, 5))
    assert_type(query, gluex.RunQuery)
    assert_type(query.selection, gluex.RunSelection)
    assert_type(query.provenance, gluex.RunProvenance)
    result = query.collect()
    assert_type(result, gluex.RunSet)
    assert_type(result.numbers, tuple[int, ...])
    assert_type(iter(result), Iterator[int])
    assert_type(result[0], int)
    assert_type(result.provenance.source, str)
    for reader in (gx.sources.rcdb, gx.sources.ccdb):
        raw = reader.raw('SELECT ?, ?, ?, ?, ?', parameters=[2, 1.5, 'text', b'bytes', None])
        assert_type(raw, gluex.RawResults)
        assert_type(raw.columns, tuple[gluex.RawColumn, ...])
        assert_type(raw.columns[0].declared_type, str | None)
        assert_type(raw.rows, tuple[gluex.RawRow, ...])
        assert_type(raw.rows[0].values, tuple[int | float | str | bytes | None, ...])

    gx.runs([2])  # ty: ignore[invalid-argument-type]
    gluex.RunSelection.range('2', 5)  # ty: ignore[invalid-argument-type]
    gx.sources.rcdb.raw('SELECT ?', [2])  # ty: ignore[too-many-positional-arguments]
    gx.sources.ccdb.raw('SELECT ?', parameters=[object()])  # ty: ignore[invalid-argument-type]
    result.numbers = ()  # ty: ignore[invalid-assignment]
