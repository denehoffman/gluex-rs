import gluex_ccdb


def test_imports():
    # Basic smoke test to ensure module is loadable
    assert hasattr(gluex_ccdb, "CCDB")  # noqa: S101
