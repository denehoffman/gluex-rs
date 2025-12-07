import pytest
import gluex_rcdb


def test_sum_as_string():
    assert gluex_rcdb.sum_as_string(1, 1) == "2"
