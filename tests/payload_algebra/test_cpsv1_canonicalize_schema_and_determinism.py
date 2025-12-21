import copy
from pathlib import Path

import yaml

from semantiva.pipeline.cpsv1.canonicalize import canonicalize_yaml_to_cpsv1
from semantiva.pipeline.cpsv1.identity import (
    compute_pipeline_id_cpsv1,
    compute_upstream_map_cpsv1,
)
from semantiva.pipeline.cpsv1.validation import (
    validate_cpsv1,
    validate_cpsv1_semantics,
)


def _load(path: str) -> dict:
    return yaml.safe_load(Path(path).read_text(encoding="utf-8"))


def test_canonicalize_outputs_validate_and_are_deterministic():
    spec = _load(
        "tests/payload_algebra_reference_suite/float_ref_slots_42_times_two.yaml"
    )
    cps1 = canonicalize_yaml_to_cpsv1(spec)
    cps2 = canonicalize_yaml_to_cpsv1(copy.deepcopy(spec))
    validate_cpsv1(cps1)
    validate_cpsv1(cps2)
    validate_cpsv1_semantics(cps1)
    validate_cpsv1_semantics(cps2)
    assert cps1 == cps2
    assert compute_pipeline_id_cpsv1(cps1) == compute_pipeline_id_cpsv1(cps2)

    upstream = compute_upstream_map_cpsv1(cps1)
    # Two nodes: second depends on first (primary channel)
    assert len(upstream) == 2
    assert upstream[cps1["nodes"][1]["node_uuid"]] == [cps1["nodes"][0]["node_uuid"]]


def test_canonicalize_applies_bind_defaults_and_data_key():
    spec = _load(
        "tests/payload_algebra_reference_suite/float_ref_slots_43_add_second_source.yaml"
    )
    cpsv1 = canonicalize_yaml_to_cpsv1(spec)
    validate_cpsv1(cpsv1)

    first, second, third = cpsv1["nodes"]

    assert first["publish"]["channels"]["out"] == "primary"
    assert first["bind"]["data"] == "channel:primary"

    assert second["publish"]["channels"]["out"] == "addend"
    assert second["bind"]["data"] == "channel:primary"

    assert third["bind"]["data"] == "channel:primary"
    assert third["bind"]["other"] == "channel:addend"
