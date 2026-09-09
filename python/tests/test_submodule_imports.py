"""Tests for conventional imports from the public GlueX submodules."""

from gluex.ccdb import CCDB
from gluex.lumi import FluxHistograms
from gluex.rcdb import RCDB


def test_public_submodule_classes_are_directly_importable() -> None:
    assert CCDB.__module__ == 'gluex.ccdb'
    assert FluxHistograms.__module__ == 'gluex.lumi'
    assert RCDB.__module__ == 'gluex.rcdb'
