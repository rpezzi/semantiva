from __future__ import annotations

import json
from importlib import resources
from pathlib import Path

import jsonschema

from semantiva.eir import compile_eir_v1

REPO_ROOT = Path(__file__).parents[2]
REF = REPO_ROOT / "tests" / "eir_reference_suite" / "float_ref_01.yaml"


def test_compiled_eir_validates_against_schema() -> None:
    eir = compile_eir_v1(str(REF))

    schema_path = resources.files("semantiva.eir.schema") / "eir_v1.schema.json"
    schema = json.loads(schema_path.read_text(encoding="utf-8"))

    validator = jsonschema.Draft202012Validator(schema)
    validator.validate(eir)
