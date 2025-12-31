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

"""PA-03D context producer last-writer regression test."""

from __future__ import annotations

import json
from pathlib import Path

from semantiva.context_processors.context_types import ContextType
from semantiva.data_types import NoDataType
from semantiva.execution.orchestrator.orchestrator import LocalSemantivaOrchestrator
from semantiva.execution.transport import InMemorySemantivaTransport
from semantiva.logger import Logger
from semantiva.pipeline.payload import Payload
from semantiva.trace.drivers.jsonl import JsonlTraceDriver


_REPO_ROOT = Path(__file__).resolve().parents[2]


def _ser_records(trace_path: Path) -> list[dict]:
    return [
        json.loads(line) for line in trace_path.read_text().splitlines() if line.strip()
    ]


def test_context_producer_last_writer_is_reported(tmp_path: Path) -> None:
    """Ensure context keys written by nodes report producer.kind=node with correct node_uuid."""
    spec = str(
        _REPO_ROOT
        / "tests"
        / "payload_algebra_reference_suite"
        / "float_ref_slots_44_context_written_then_read.yaml"
    )
    trace_path = tmp_path / "trace.jsonl"
    trace = JsonlTraceDriver(str(trace_path))

    orch = LocalSemantivaOrchestrator()
    transport = InMemorySemantivaTransport()
    logger = Logger()

    orch.execute(
        spec,
        Payload(data=NoDataType(), context=ContextType({})),
        transport,
        logger,
        trace=trace,
        execution_backend="eir_payload_algebra",
    )
    trace.close()

    records = [
        rec for rec in _ser_records(trace_path) if rec.get("record_type") == "ser"
    ]

    # Locate probe and add_operation records
    probe_record = None
    add_record = None
    for rec in records:
        ref = rec.get("processor", {}).get("ref", "")
        if "FloatCollectDataProbe" in ref:
            probe_record = rec
        elif "FloatAddTwoInputsOperation" in ref:
            add_record = rec

    assert probe_record is not None, "FloatCollectDataProbe SER not found"
    assert add_record is not None, "FloatAddTwoInputsOperation SER not found"

    probe_node_uuid = probe_record["identity"]["node_id"]

    # AddTwoInputs reads "other" from context (written by probe)
    param_source_refs = add_record.get("processor", {}).get("parameter_source_refs", {})
    other_ref = param_source_refs.get("other")
    assert other_ref is not None, "parameter_source_refs.other missing"
    assert other_ref["kind"] == "context"
    assert other_ref["key"] == "other"

    producer = other_ref.get("producer", {})
    assert (
        producer.get("kind") == "node"
    ), f"Expected producer.kind=node, got {producer.get('kind')}"
    assert (
        producer.get("node_uuid") == probe_node_uuid
    ), f"Expected producer.node_uuid={probe_node_uuid}, got {producer.get('node_uuid')}"
