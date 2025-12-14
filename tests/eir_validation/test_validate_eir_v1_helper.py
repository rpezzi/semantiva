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

import pytest

import jsonschema

from semantiva.eir import compile_eir_v1, validate_eir_v1


def test_validate_eir_v1_accepts_compiled_reference() -> None:
    """Integration: compiler output validates via the canonical helper."""
    eir = compile_eir_v1("tests/eir_reference_suite/float_ref_01.yaml")
    validate_eir_v1(eir)  # should not raise


def test_validate_eir_v1_rejects_missing_required_fields() -> None:
    """Unit: helper surfaces schema errors."""
    eir = compile_eir_v1("tests/eir_reference_suite/float_ref_01.yaml")
    bad = dict(eir)
    bad.pop("plan")  # required by schema

    with pytest.raises(jsonschema.ValidationError):
        validate_eir_v1(bad)


def test_validate_eir_v1_minimal_golden_fixture() -> None:
    """Golden: validate a stable minimal EIR fixture (no timestamp drift)."""
    minimal = {
        "eir_version": 1,
        "identity": {
            "pipeline_id": "plid-test",
            "pipeline_variant_id": "pvid-test",
            "eir_id": "eirid-test",
        },
        "graph": {"graph_version": 1, "nodes": [], "edges": []},
        "plan": {
            "plan_version": 1,
            "segments": [{"kind": "classic_linear", "node_order": ["n0"]}],
        },
    }
    validate_eir_v1(minimal)  # should not raise
