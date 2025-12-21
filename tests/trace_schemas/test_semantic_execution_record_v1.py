from __future__ import annotations

import json
from pathlib import Path


def _forbidden_identity_tokens() -> tuple[str, str]:
    """Return identity tokens that must not appear in trace schemas."""
    variant_token = "pipeline" + "_variant" + "_id"
    artifact_token = "eir" + "_id"
    return variant_token, artifact_token


def test_ser_schema_surface_excludes_prohibited_identity_fields() -> None:
    schema_path = Path(
        "semantiva/trace/schema/semantic_execution_record_v1.schema.json"
    )
    schema = json.loads(schema_path.read_text(encoding="utf-8"))
    schema_str = json.dumps(schema, sort_keys=True)

    variant_token, artifact_token = _forbidden_identity_tokens()
    assert variant_token not in schema_str
    assert artifact_token not in schema_str
