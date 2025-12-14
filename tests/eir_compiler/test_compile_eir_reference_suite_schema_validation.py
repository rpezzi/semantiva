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

import json
from importlib import resources
from pathlib import Path

import jsonschema
import pytest

from semantiva.eir import compile_eir_v1

REPO_ROOT = Path(__file__).parents[2]
SUITE = REPO_ROOT / "tests" / "eir_reference_suite"
REFS = [
    SUITE / "float_ref_01.yaml",
    SUITE / "float_ref_02.yaml",
    SUITE / "float_ref_channel_01.yaml",
    SUITE / "float_ref_slots_01.yaml",
    SUITE / "float_ref_lane_01.yaml",
]


@pytest.mark.parametrize("ref", REFS, ids=[p.stem for p in REFS])
def test_compile_reference_suite_validates_against_schema(ref: Path) -> None:
    eir = compile_eir_v1(str(ref))
    schema_path = resources.files("semantiva.eir.schema") / "eir_v1.schema.json"
    schema = json.loads(schema_path.read_text(encoding="utf-8"))
    jsonschema.Draft202012Validator(schema).validate(eir)
