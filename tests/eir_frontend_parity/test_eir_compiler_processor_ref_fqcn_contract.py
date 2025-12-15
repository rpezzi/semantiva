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

from __future__ import annotations

from pathlib import Path

import pytest

from semantiva.eir import compile_eir_v1
from semantiva.registry.processor_registry import ProcessorRegistry

REPO_ROOT = Path(__file__).parents[2]
SUITE = REPO_ROOT / "tests" / "eir_reference_suite"


def _order(eir: dict) -> list[str]:
    return list(eir["plan"]["segments"][0]["node_order"])


def _node_processor_refs(eir: dict) -> list[str]:
    order = _order(eir)
    node_io = eir["semantics"]["payload_forms"]["node_io"]
    return [str(node_io[n]["processor_ref"]) for n in order]


@pytest.mark.parametrize("ref_id", ["float_ref_01", "float_ref_02"])
def test_compiled_eir_emits_fqcn_processor_refs(ref_id: str) -> None:
    eir = compile_eir_v1(str(SUITE / f"{ref_id}.yaml"))
    refs = _node_processor_refs(eir)

    assert refs, "Expected at least one node processor_ref"
    assert all(
        "." in r for r in refs
    ), f"All processor_ref must be dotted FQCNs: {refs}"


def test_ambiguous_short_name_errors_deterministically(
    isolated_processor_registry,
) -> None:
    # Create two distinct classes with different FQCNs and register both under same short name.
    A = type("AmbigProcEIR", (), {"__module__": "m1"})
    B = type("AmbigProcEIR", (), {"__module__": "m2"})
    ProcessorRegistry.register_processor("AmbigProcEIR", A)
    ProcessorRegistry.register_processor("AmbigProcEIR", B)

    # Minimal YAML spec referencing the short name (not dotted-FQCN).
    bad = """
pipeline:
  nodes:
    - processor: AmbigProcEIR
      parameters: {}
"""

    with pytest.raises(ValueError) as exc_info:
        compile_eir_v1(bad)

    msg = str(exc_info.value)
    assert "AmbigProcEIR" in msg
    assert "Ambiguous" in msg
    assert "FQCN" in msg or "fully-qualified" in msg or "dotted" in msg
