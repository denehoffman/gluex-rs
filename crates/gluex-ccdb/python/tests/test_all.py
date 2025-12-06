import pytest
import ccdb_rs


def test_sum_as_string():
    assert ccdb_rs.sum_as_string(1, 1) == "2"
