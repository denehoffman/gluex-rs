import datetime as dt

import gluex
import pytest


def test_predicates_preserve_unknown_and_original_query(rcdb_path):
    gx = gluex.open(rcdb=rcdb_path, ccdb=gluex.DISABLED)
    base = gx.runs(gluex.RunSelection.runs([2, 3, 4]))
    valid = gx.conditions['is_valid_run_end']
    result = base.where(~(valid.eq(value=True))).collect()
    assert result.numbers == (2,)
    assert result.report.unknown_runs == (3,)
    omission = result.report.accounting.omissions[0]
    assert omission.run == 3
    assert omission.reason == gluex.RunOmissionReason.UnknownPredicate
    assert 'RunOmission' in repr(omission)
    assert base.collect().numbers == (2, 3, 4)
    assert base.where(valid.is_missing()).collect().numbers == (3,)
    assert base.where((valid.eq(value=True)) | valid.is_missing()).collect().numbers == (3, 4)
    assert result.provenance.predicates
    with pytest.raises(TypeError):
        bool(valid.eq(value=True))
    with pytest.raises(ValueError, match='event_count'):
        _ = gx.conditions['event_count'] > 'wrong'


@pytest.mark.parametrize(
    ('a', 'b', 'expected_and', 'expected_or'),
    [
        (0, 0, True, True),
        (0, 1, False, True),
        (0, 2, None, True),
        (1, 0, False, True),
        (1, 1, False, False),
        (1, 2, False, None),
        (2, 0, None, True),
        (2, 1, False, None),
        (2, 2, None, None),
    ],
)
def test_three_valued_composition(rcdb_path, a, b, expected_and, expected_or):
    gx = gluex.open(rcdb=rcdb_path, ccdb=gluex.DISABLED)
    d = gx.conditions
    states = [d['event_count'] > 0, d['event_count'] < 0, d['is_valid_run_end'].eq(value=True)]
    base = gx.runs(gluex.RunSelection.runs([3]))
    for predicate, expected in [(states[a] & states[b], expected_and), (states[a] | states[b], expected_or)]:
        result = base.where(predicate).collect()
        assert result.numbers == ((3,) if expected is True else ())
        assert result.report.unknown_runs == ((3,) if expected is None else ())


def test_operand_types_and_explicit_approval(rcdb_path):
    gx = gluex.open(rcdb=rcdb_path, ccdb=gluex.DISABLED)
    d = gx.conditions
    base = gx.runs(gluex.RunSelection.runs([2, 3, 4]))
    assert base.where((d['event_count'] >= 1686) & (d['event_count'] <= 5000)).collect().numbers == (3, 4)
    assert base.where(d['event_count'].ne(2)).collect().numbers == (3, 4)
    assert base.where(
        d['run_start_time'].eq(dt.datetime(2015, 12, 8, 15, 47, 20, tzinfo=dt.timezone.utc))
    ).collect().numbers == (2,)
    with pytest.raises(OverflowError):
        d['event_count'].eq(2**100)
    with pytest.raises(ValueError):
        d['event_count'].eq(value=True)
    with pytest.raises(ValueError):
        _ = d['beam_current'] > float('nan')  # noqa: PLW0177
    assert gx.runs(gluex.RunSelection.range(50000, 59999)).where(
        gluex.approved_production(gluex.RunPeriod.RP2018_01)
    ).collect().numbers == (50685, 50697)


def test_equality_operators_direct_users_to_typed_methods(rcdb_path):
    definition = gluex.open(rcdb=rcdb_path, ccdb=gluex.DISABLED).conditions['event_count']
    with pytest.raises(TypeError, match='eq'):
        _ = definition == 2
    with pytest.raises(TypeError, match='ne'):
        _ = definition != 2
