from __future__ import annotations

import pytest

from semantiva.registry.processor_registry import ProcessorRegistry


@pytest.fixture()
def isolated_processor_registry():
    """Ensure ProcessorRegistry mutations do not leak across tests."""

    snap = ProcessorRegistry._snapshot_state_for_tests()
    try:
        yield
    finally:
        ProcessorRegistry._restore_state_for_tests(snap)
