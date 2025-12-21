from __future__ import annotations

import copy

import pytest
import jsonschema

from semantiva.eir import compile_eir_v1, validate_eir_v1


_REF = "tests/eir_reference_suite/float_ref_01.yaml"


def _identity_forbidden_keys() -> tuple[str, str]:
    """Return identity keys that must be rejected by the EIRv1 schema."""
    variant_key = "pipeline" + "_variant" + "_id"
    artifact_key = "eir" + "_id"
    return variant_key, artifact_key


def test_validate_eir_v1_accepts_compiled_reference() -> None:
    eir = compile_eir_v1(_REF)
    validate_eir_v1(eir)


def test_validate_eir_v1_accepts_pipeline_id_only_identity_when_other_fields_unchanged() -> (
    None
):
    eir = compile_eir_v1(_REF)
    eir_min = copy.deepcopy(eir)
    eir_min["identity"] = {"pipeline_id": eir["identity"]["pipeline_id"]}
    validate_eir_v1(eir_min)


def test_validate_eir_v1_rejects_forbidden_identity_variant_axis() -> None:
    variant_key, _ = _identity_forbidden_keys()
    eir = compile_eir_v1(_REF)
    bad = copy.deepcopy(eir)
    bad["identity"][variant_key] = "pvid-test"
    with pytest.raises(jsonschema.ValidationError):
        validate_eir_v1(bad)


def test_validate_eir_v1_rejects_forbidden_identity_artifact_axis() -> None:
    _, artifact_key = _identity_forbidden_keys()
    eir = compile_eir_v1(_REF)
    bad = copy.deepcopy(eir)
    bad["identity"][artifact_key] = "artifact-test"
    with pytest.raises(jsonschema.ValidationError):
        validate_eir_v1(bad)
