from __future__ import annotations

import json
from importlib import resources
from pathlib import Path

import jsonschema
import pytest

from semantiva.eir import compile_eir_v1

REPO_ROOT = Path(__file__).parents[2]
SUITE = REPO_ROOT / "tests" / "payload_algebra_reference_suite"
REFS = sorted(SUITE.glob("*.yaml"))


@pytest.mark.parametrize("ref", REFS, ids=[p.stem for p in REFS])
def test_compile_payload_algebra_refs_validates_against_eir_schema(ref: Path) -> None:
    eir = compile_eir_v1(str(ref))
    schema_path = resources.files("semantiva.eir.schema") / "eir_v1.schema.json"
    schema = json.loads(schema_path.read_text(encoding="utf-8"))
    jsonschema.Draft202012Validator(schema).validate(eir)
