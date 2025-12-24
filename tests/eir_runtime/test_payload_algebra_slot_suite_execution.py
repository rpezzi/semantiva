# Copyright 2025 Semantiva authors
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.

from __future__ import annotations

from pathlib import Path

from semantiva.context_processors.context_types import ContextType
from semantiva.data_types import NoDataType
from semantiva.examples.test_utils import FloatDataType
from semantiva.execution.orchestrator.orchestrator import LocalSemantivaOrchestrator
from semantiva.execution.transport.in_memory import InMemorySemantivaTransport
from semantiva.logger import Logger
from semantiva.pipeline.payload import Payload


def _repo_root() -> Path:
    return Path(__file__).resolve().parents[2]


def _fixture(name: str) -> str:
    return str(_repo_root() / "tests" / "payload_algebra_reference_suite" / name)


def _run(spec_path: str, context: dict) -> float:
    payload = Payload(NoDataType(), ContextType(context))
    out = LocalSemantivaOrchestrator().execute(
        pipeline_spec=spec_path,
        payload=payload,
        transport=InMemorySemantivaTransport(),
        logger=Logger(),
        execution_backend="eir_payload_algebra",
    )
    # semantiva-examples FloatDataType carries float in .data
    return float(out.data.data)


def test_slot_suite_41_baseline_context_injection_other() -> None:
    # §4.1 baseline: data flows from primary; other injected from context
    spec = _fixture("float_ref_slots_41_baseline.yaml")
    assert _run(spec, {"value": 1.0, "other": FloatDataType(2.0)}) == 3.0


def test_slot_suite_42_times_two_bind_other_primary() -> None:
    # §4.2 times-two: other bound to primary; no context other required
    spec = _fixture("float_ref_slots_42_times_two.yaml")
    assert _run(spec, {"value": 1.0}) == 2.0


def test_slot_suite_43_add_second_source_bind_other_addend() -> None:
    # §4.3: second source publishes to addend; other bound to addend
    spec = _fixture("float_ref_slots_43_add_second_source.yaml")
    assert _run(spec, {"value": 1.0}) == 3.0


def test_slot_suite_43_explicit_equivalent_is_equivalent() -> None:
    spec = _fixture("float_ref_slots_43_explicit_equivalent.yaml")
    assert _run(spec, {"value": 1.0}) == 3.0
