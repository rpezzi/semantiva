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

import jsonschema
import pytest

from semantiva.pipeline.cpsv1.validation import validate_canonical_pipeline_spec_v1


def _minimal_cpsv1() -> dict:
    return {
        "version": 1,
        "nodes": [
            {
                "node_uuid": "11111111-1111-1111-1111-111111111111",
                "role": "processor",
                "processor_ref": "semantiva.examples.processors.FloatValueDataSource",
                "parameters": {},
                "ports": {},
                "derive": None,
                "bind": {"data": "channel:primary"},
                "publish": {"channels": {"out": "primary"}, "context_key": None},
                "declaration_index": 0,
                "declaration_subindex": 0,
            }
        ],
    }


def test_validate_cpsv1_accepts_minimal_golden_fixture() -> None:
    validate_canonical_pipeline_spec_v1(_minimal_cpsv1())  # should not raise


def test_validate_cpsv1_rejects_missing_bind_data() -> None:
    bad = _minimal_cpsv1()
    bad["nodes"][0]["bind"] = {}  # canonical CPSV1 must be explicit

    with pytest.raises(jsonschema.ValidationError):
        validate_canonical_pipeline_spec_v1(bad)


def test_validate_cpsv1_rejects_missing_publish_out() -> None:
    bad = _minimal_cpsv1()
    bad["nodes"][0]["publish"]["channels"] = {}  # canonical CPSV1 must be explicit

    with pytest.raises(jsonschema.ValidationError):
        validate_canonical_pipeline_spec_v1(bad)
