# Copyright 2025 Semantiva authors
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.

"""PA-03D: SER provenance validation tests for §4 reference suite."""

from __future__ import annotations

import json
from pathlib import Path
from typing import Any, Iterable

import jsonschema
import pytest
import yaml

from semantiva.context_processors.context_types import ContextType
from semantiva.data_types import NoDataType
from semantiva.examples.test_utils import FloatDataType
from semantiva.execution.orchestrator.orchestrator import LocalSemantivaOrchestrator
from semantiva.execution.transport import InMemorySemantivaTransport
from semantiva.logger import Logger
from semantiva.pipeline.payload import Payload
from semantiva.eir.runtime import build_payload_algebra_specs_from_pipeline_spec
from semantiva.trace.drivers.jsonl import JsonlTraceDriver


_REPO_ROOT = Path(__file__).resolve().parents[2]
_GOLDEN = yaml.safe_load(
    (
        _REPO_ROOT / "tests" / "payload_algebra" / "golden_provenance_pa03d.yaml"
    ).read_text(encoding="utf-8")
)
_SER_SCHEMA = json.loads(
    (
        _REPO_ROOT
        / "semantiva"
        / "trace"
        / "schema"
        / "semantic_execution_record_v1.schema.json"
    ).read_text()
)


def _ser_records(trace_path: Path) -> list[dict[str, Any]]:
    return [
        json.loads(line) for line in trace_path.read_text().splitlines() if line.strip()
    ]


def _run_with_trace(
    spec_name: str, context: dict[str, Any], tmp_path: Path
) -> list[dict[str, Any]]:
    spec_path = (
        _REPO_ROOT / "tests" / "payload_algebra_reference_suite" / f"{spec_name}.yaml"
    )
    trace_path = tmp_path / "trace.ser.jsonl"
    normalized_ctx = dict(context)
    if "other" in normalized_ctx and not isinstance(
        normalized_ctx["other"], FloatDataType
    ):
        normalized_ctx["other"] = FloatDataType(float(normalized_ctx["other"]))
    trace = JsonlTraceDriver(str(trace_path))
    orch = LocalSemantivaOrchestrator()
    orch.execute(
        pipeline_spec=str(spec_path),
        payload=Payload(NoDataType(), ContextType(normalized_ctx)),
        transport=InMemorySemantivaTransport(),
        logger=Logger(),
        trace=trace,
        execution_backend="eir_payload_algebra",
    )
    trace.close()
    return [rec for rec in _ser_records(trace_path) if rec.get("record_type") == "ser"]


def _find_record(
    records: Iterable[dict[str, Any]], ref_fragment: str
) -> dict[str, Any]:
    for rec in records:
        ref = rec.get("processor", {}).get("ref", "")
        if ref_fragment in ref:
            return rec
    raise AssertionError(f"SER record containing '{ref_fragment}' not found")


def _producer_node_for_channel(
    resolved_nodes: list[dict[str, Any]], channel: str
) -> str:
    """
    Determine producing node_uuid for a channel in §4 trace (attribution-correct).

    For primary: the FloatValueDataSource node
    For addend: the FloatValueDataSource node (second-source case)
    """
    for node in resolved_nodes:
        processor_ref = node.get("processor")
        if "FloatValueDataSource" not in str(processor_ref):
            continue

        publish = node.get("publish") or {}
        channels = publish.get("channels") if isinstance(publish, dict) else {}
        out_channel = channels.get("out") if isinstance(channels, dict) else None
        effective_channel = str(out_channel) if out_channel else "primary"
        if effective_channel == channel:
            node_uuid = node.get("node_uuid")
            if isinstance(node_uuid, str) and node_uuid:
                return node_uuid

    raise AssertionError(
        f"No FloatValueDataSource producer found for channel '{channel}'"
    )


@pytest.mark.parametrize(
    "spec_name,expectation",
    list(_GOLDEN["expectations"].items()),
    ids=list(_GOLDEN["expectations"].keys()),
)
def test_ser_provenance_matches_golden(
    spec_name: str, expectation: dict[str, Any], tmp_path: Path
) -> None:
    """Validate parameter_sources, refs, and upstream producers for §4 cases."""
    context = expectation.get("context", {})
    records = _run_with_trace(spec_name, context, tmp_path)
    spec_path = (
        _REPO_ROOT / "tests" / "payload_algebra_reference_suite" / f"{spec_name}.yaml"
    )
    _canonical, resolved_nodes = build_payload_algebra_specs_from_pipeline_spec(
        str(spec_path)
    )

    # Validate all SER against schema
    for rec in records:
        jsonschema.validate(rec, _SER_SCHEMA)

    add_record = _find_record(records, "FloatAddTwoInputsOperation")
    source_records = [
        rec
        for rec in records
        if "FloatValueDataSource" in rec.get("processor", {}).get("ref", "")
    ]
    source_ids = [rec["identity"]["node_id"] for rec in source_records]

    processor = add_record["processor"]
    param_sources = processor["parameter_sources"]
    expected_sources = expectation["parameter_sources"]
    for key, value in expected_sources.items():
        assert (
            param_sources.get(key) == value
        ), f"{spec_name}: expected {key} source {value}"

    param_refs = processor.get("parameter_source_refs", {})
    expected_refs = expectation["parameter_source_refs"]
    for key, expected_ref in expected_refs.items():
        actual = param_refs.get(key)
        assert actual is not None, f"{spec_name}: missing parameter_source_refs.{key}"
        assert actual["kind"] == expected_ref["kind"]

        # Validate channel/key identity
        if expected_ref["kind"] == "data":
            assert actual["channel"] == expected_ref["channel"]
        if expected_ref["kind"] == "context":
            assert actual["key"] == expected_ref["key"]

        # Validate structured producer (SSOT-correct)
        producer = actual.get("producer", {})
        expected_producer = expected_ref.get("producer", {})
        assert producer.get("kind") == expected_producer.get(
            "kind"
        ), f"{spec_name}.{key}: producer.kind mismatch"

        # For node producers, assert exact attribution + output_slot
        if producer.get("kind") == "node":
            expected_channel = expected_ref.get("channel")
            if expected_channel:
                expected_node_uuid = _producer_node_for_channel(
                    resolved_nodes, expected_channel
                )
                assert (
                    producer.get("node_uuid") == expected_node_uuid
                ), f"{spec_name}.{key}: wrong producer node for channel {expected_channel}"

            # PA-03D provenance gate: output_slot must be "out" for data refs
            if expected_ref["kind"] == "data":
                assert (
                    producer.get("output_slot") == "out"
                ), f"{spec_name}.{key}: missing or incorrect output_slot for data ref"

    upstream = add_record["dependencies"]["upstream"]
    assert len(upstream) == expectation["upstream_count"]
    if expectation["upstream_count"] > 0:
        for node_id in upstream:
            assert node_id in source_ids


@pytest.mark.parametrize(
    "spec_name",
    [
        "float_ref_slots_41_baseline",
        "float_ref_slots_42_times_two",
        "float_ref_slots_43_add_second_source",
    ],
)
def test_bind_is_not_reported_as_provenance(spec_name: str, tmp_path: Path) -> None:
    """Ensure no SER reports 'bind' as a provenance category."""
    context = _GOLDEN["expectations"][spec_name]["context"]
    records = _run_with_trace(spec_name, context, tmp_path)
    for rec in records:
        param_sources = rec.get("processor", {}).get("parameter_sources", {})
        assert all(source != "bind" for source in param_sources.values())
