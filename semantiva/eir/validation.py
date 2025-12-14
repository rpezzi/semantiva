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


def load_eir_v1_schema() -> Dict[str, Any]:
    """
    Load the packaged EIR v1 JSON schema.

    Returns:
        The parsed JSON schema as a dict.

    Raises:
        FileNotFoundError: if the packaged schema resource cannot be found.
        json.JSONDecodeError: if the schema file is not valid JSON.
    """
    schema_path = resources.files("semantiva.eir.schema") / "eir_v1.schema.json"
    return json.loads(schema_path.read_text(encoding="utf-8"))


def validate_eir_v1(eir: Dict[str, Any]) -> None:
    """
    Validate an EIR v1 document against the packaged EIR v1 JSON schema.

    This is intended as a canonical preflight helper for Phase 3 runtime epics.

    Args:
        eir: EIR document (dict) to validate.

    Raises:
        jsonschema.ValidationError: if the document is not schema-conformant.
        jsonschema.SchemaError: if the packaged schema is invalid.
    """
    schema = load_eir_v1_schema()
    jsonschema.Draft202012Validator(schema).validate(eir)
