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
from typing import Any, Dict

import jsonschema


def load_canonical_pipeline_spec_v1_schema() -> Dict[str, Any]:
    """Load the packaged CanonicalPipelineSpecV1 JSON schema."""
    schema_path = (
        resources.files("semantiva.pipeline.schema")
        / "canonical_pipeline_spec_v1.schema.json"
    )
    return json.loads(schema_path.read_text(encoding="utf-8"))


def validate_canonical_pipeline_spec_v1(spec: Dict[str, Any]) -> None:
    """Validate a CPSV1 document against the packaged CPSV1 schema."""
    schema = load_canonical_pipeline_spec_v1_schema()
    jsonschema.Draft202012Validator(schema).validate(spec)
