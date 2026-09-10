"""Checks for the generated-stub external import finalizer."""

from scripts.finalize_generated_stubs import finalize, finalize_root


def test_external_imports_are_added_once_after_generated_imports() -> None:
    generated = 'from typing import Any\n\nclass Config:\n    channel: "laddu.Channel"\n'

    finalized = finalize(generated)

    assert finalized.startswith(
        'from typing import Any\nfrom collections.abc import Sequence\nimport laddu\n\n'
    )
    assert finalize(finalized) == finalized


def test_exception_hierarchy_is_added_once_to_the_root_stub() -> None:
    finalized = finalize_root('from typing import Any\n')

    assert 'class MissingCapabilityError(RuntimeError)' in finalized
    assert 'class MissingDataError(QueryError)' in finalized
    assert 'class DatabaseTimeoutError(TimeoutError)' in finalized
    assert finalize_root(finalized) == finalized
