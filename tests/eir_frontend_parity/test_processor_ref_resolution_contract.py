# Copyright 2025 Semantiva authors
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#     http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

"""Processor reference resolution contract tests (TDR v2 parity)."""

from __future__ import annotations

import pytest

from semantiva.registry.processor_registry import ProcessorRegistry
from semantiva.registry.resolve import (
    AmbiguousProcessorError,
    resolve_symbol,
)
import semantiva.registry.resolve as resolve_mod
from semantiva.examples.test_utils import FloatAddOperation


# Module-level dummy classes for stable qualnames (avoid <locals> artifacts)
class ModuleLevelClashA:
    """Test fixture: distinct class A."""

    pass


class ModuleLevelClashB:
    """Test fixture: distinct class B."""

    pass


def test_resolve_symbol_accepts_dotted_fqn() -> None:
    """Dotted FQN resolution for explicit processor_ref."""
    cls = resolve_symbol("semantiva.examples.test_utils.FloatAddOperation")
    assert cls is FloatAddOperation


def test_ambiguous_short_name_errors_deterministically(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Ambiguous short names must error deterministically with fix instructions."""
    ProcessorRegistry.clear()
    monkeypatch.setattr(resolve_mod, "_ensure_defaults_loaded", lambda: None)

    # Register two different classes under the same short name
    ProcessorRegistry.register_processor("Clash", ModuleLevelClashA)
    ProcessorRegistry.register_processor("Clash", ModuleLevelClashB)

    with pytest.raises(AmbiguousProcessorError) as exc_info:
        resolve_symbol("Clash")

    msg = str(exc_info.value)
    # Message must be actionable and include both candidates
    assert "Ambiguous processor symbol" in msg
    assert "Clash" in msg
    assert f"{ModuleLevelClashA.__module__}.{ModuleLevelClashA.__qualname__}" in msg
    assert f"{ModuleLevelClashB.__module__}.{ModuleLevelClashB.__qualname__}" in msg
    assert "processor_ref" in msg


def test_get_candidates_returns_sorted_fqcns(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Candidates must be returned as sorted FQCNs."""
    ProcessorRegistry.clear()
    monkeypatch.setattr(resolve_mod, "_ensure_defaults_loaded", lambda: None)

    ProcessorRegistry.register_processor("Test", ModuleLevelClashA)
    ProcessorRegistry.register_processor("Test", ModuleLevelClashB)

    candidates = ProcessorRegistry.get_candidates("Test")
    assert isinstance(candidates, list)
    assert len(candidates) == 2
    assert candidates == sorted(candidates)


def test_short_name_reregistration_idempotent_does_not_create_ambiguity(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Re-registering the same class under the same short name is idempotent."""
    ProcessorRegistry.clear()
    monkeypatch.setattr(resolve_mod, "_ensure_defaults_loaded", lambda: None)

    # Register the same class twice
    ProcessorRegistry.register_processor("FloatAdd", FloatAddOperation)
    ProcessorRegistry.register_processor("FloatAdd", FloatAddOperation)

    # Should resolve without ambiguity error
    resolved = resolve_symbol("FloatAdd")
    assert resolved is FloatAddOperation


def test_dotted_fqn_resolution_registers_safe_alias_key() -> None:
    """Resolving dotted-FQN registers a safe alias keyed by the dotted string."""
    dotted_ref = "semantiva.examples.test_utils.FloatAddOperation"
    cls = resolve_symbol(dotted_ref)
    assert cls is FloatAddOperation

    # Alias key is the dotted string itself, not a short name
    registered_cls = ProcessorRegistry.get_processor(dotted_ref)
    assert registered_cls is FloatAddOperation
