"""Semantic validation and equivalence locks for CPSV1 bind/channel wiring."""

import copy
from pathlib import Path

import pytest
import yaml

from semantiva.pipeline.cpsv1.canonicalize import canonicalize_yaml_to_cpsv1
from semantiva.pipeline.cpsv1.identity import compute_pipeline_id_cpsv1
from semantiva.pipeline.cpsv1.validation import (
    validate_cpsv1,
    validate_cpsv1_semantics,
)


def _load(path: str) -> dict:
    return yaml.safe_load(Path(path).read_text(encoding="utf-8"))


def test_equivalence_43_minimal_equals_explicit():
    minimal = _load(
        "tests/payload_algebra_reference_suite/float_ref_slots_43_add_second_source.yaml"
    )
    explicit = _load(
        "tests/payload_algebra_reference_suite/float_ref_slots_43_explicit_equivalent.yaml"
    )

    cps_min = canonicalize_yaml_to_cpsv1(minimal)
    cps_exp = canonicalize_yaml_to_cpsv1(copy.deepcopy(explicit))

    validate_cpsv1(cps_min)
    validate_cpsv1(cps_exp)
    validate_cpsv1_semantics(cps_min)
    validate_cpsv1_semantics(cps_exp)

    assert cps_min == cps_exp
    assert compute_pipeline_id_cpsv1(cps_min) == compute_pipeline_id_cpsv1(cps_exp)


def test_semantics_rejects_bind_to_unknown_channel():
    spec = _load(
        "tests/payload_algebra_reference_suite/float_ref_slots_42_times_two.yaml"
    )
    cps = canonicalize_yaml_to_cpsv1(spec)
    validate_cpsv1(cps)

    cps_bad = copy.deepcopy(cps)
    cps_bad["nodes"][1]["bind"]["other"] = "channel:does_not_exist"

    with pytest.raises(ValueError, match="unknown channel"):
        validate_cpsv1_semantics(cps_bad)


def test_semantics_rejects_double_write_non_primary_channel():
    spec = _load(
        "tests/payload_algebra_reference_suite/float_ref_slots_43_add_second_source.yaml"
    )
    cps = canonicalize_yaml_to_cpsv1(spec)
    validate_cpsv1(cps)

    cps_bad = copy.deepcopy(cps)
    cps_bad["nodes"][0]["publish"]["channels"]["out"] = "addend"

    with pytest.raises(ValueError, match="published more than once"):
        validate_cpsv1_semantics(cps_bad)


def test_semantics_rejects_invalid_sourceref_prefix():
    spec = _load(
        "tests/payload_algebra_reference_suite/float_ref_slots_42_times_two.yaml"
    )
    cps = canonicalize_yaml_to_cpsv1(spec)
    validate_cpsv1(cps)

    cps_bad = copy.deepcopy(cps)
    cps_bad["nodes"][1]["bind"]["other"] = "foo:bar"

    with pytest.raises(ValueError, match="unsupported SourceRef"):
        validate_cpsv1_semantics(cps_bad)
