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
from semantiva.pipeline.graph_builder import build_graph, compute_pipeline_id

REPO_ROOT = Path(__file__).parents[2]
REF = REPO_ROOT / "tests" / "eir_reference_suite" / "float_ref_01.yaml"


def test_compile_eir_v1_is_deterministic_and_matches_pipeline_id() -> None:
    e1 = compile_eir_v1(str(REF))
    e2 = compile_eir_v1(str(REF))

    # pipeline_id must match GraphV1
    canonical = build_graph(str(REF))
    assert e1["identity"]["pipeline_id"] == compute_pipeline_id(canonical)

    # eir_id must be stable across compiles (created_at changes must not matter)
    assert e1["identity"]["eir_id"] == e2["identity"]["eir_id"]
    assert (
        e1["identity"]["pipeline_variant_id"] == e2["identity"]["pipeline_variant_id"]
    )
