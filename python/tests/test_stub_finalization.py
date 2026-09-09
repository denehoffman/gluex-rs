"""Checks for the generated-stub external import finalizer."""

from scripts.finalize_generated_stubs import finalize


def test_external_imports_are_added_once_after_generated_imports() -> None:
    generated = 'from typing import Any\n\nclass Config:\n    channel: "laddu.Channel"\n'

    finalized = finalize(generated)

    assert finalized.startswith(
        'from typing import Any\nfrom collections.abc import Sequence\nimport laddu\n\n'
    )
    assert finalize(finalized) == finalized
