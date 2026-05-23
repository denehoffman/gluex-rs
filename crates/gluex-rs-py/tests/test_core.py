"""Tests for shared ``gluex`` root types."""

from __future__ import annotations

import gluex
import pytest


def test_histogram_validates_data() -> None:
    histogram = gluex.Histogram([1.0, 4.0], [0.0, 1.0, 2.0])
    assert histogram.errors == [1.0, 2.0]
    assert histogram.as_dict()["counts"] == [1.0, 4.0]

    with pytest.raises(ValueError):
        gluex.Histogram([1.0], [0.0, 1.0, 2.0])
