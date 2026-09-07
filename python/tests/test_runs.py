"""Recorded membership and immutable Condition Definition discovery."""

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
        catalog['event_count'].name = 'changed'
    with pytest.raises(AttributeError):
        result.numbers = ()
    del gx
    assert tuple(query.collect()) == (2, 3, 5)


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
