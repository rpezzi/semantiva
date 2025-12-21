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

from semantiva.eir import compile_eir_v1

REPO_ROOT = Path(__file__).parents[2]
REF = REPO_ROOT / "tests" / "eir_reference_suite" / "float_ref_01.yaml"


def test_pipeline_id_ignores_semantics_and_derived_sections() -> None:
    eir = compile_eir_v1(str(REF))
    pipeline_id = eir["identity"]["pipeline_id"]

    mutated = dict(eir)
    mutated["semantics"] = {
        "payload_forms": {
            "version": 1,
            "root_form": "scalar",
            "terminal_form": "scalar",
            "node_io": {},
        }
    }
    mutated["derived"] = {
        "edges": [{"source_node_uuid": "a", "target_node_uuid": "b"}],
        "plan": [],
        "diagnostics": [],
    }

    assert mutated["identity"]["pipeline_id"] == pipeline_id
