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
from pathlib import Path
from typing import Any, Dict, List

from semantiva.context_processors.context_types import ContextType
from semantiva.data_types import NoDataType
from semantiva.execution.orchestrator.orchestrator import LocalSemantivaOrchestrator
from semantiva.execution.transport.in_memory import InMemorySemantivaTransport
from semantiva.logger import Logger
from semantiva.pipeline.payload import Payload
from semantiva.trace.drivers.jsonl import JsonlTraceDriver

from tests.trace_schemas._util import validate


def _repo_root() -> Path:
    return Path(__file__).resolve().parents[2]


def _load_jsonl(path: Path) -> List[Dict[str, Any]]:
    return [
        json.loads(line)
        for line in path.read_text(encoding="utf-8").splitlines()
        if line.strip()
    ]


def test_trace_gate_payload_algebra_ordering_and_schema_valid(tmp_path: Path) -> None:
    spec = str(_repo_root() / "tests" / "eir_reference_suite" / "float_ref_01.yaml")
    payload = Payload(NoDataType(), ContextType({"value": 1.0, "addend": 2.0}))

    trace_path = tmp_path / "eir_payload_algebra.jsonl"
    tracer = JsonlTraceDriver(str(trace_path))

    out = LocalSemantivaOrchestrator().execute(
        pipeline_spec=spec,
        payload=payload,
        transport=InMemorySemantivaTransport(),
        logger=Logger(),
        trace=tracer,
        execution_backend="eir_payload_algebra",
    )
    assert out.data.data == 3.0

    tracer.close()

    records = _load_jsonl(trace_path)
    assert records, "Trace must not be empty"
    assert records[0].get("record_type") == "pipeline_start"
    assert records[-1].get("record_type") == "pipeline_end"

    ser_count = sum(1 for r in records if r.get("record_type") == "ser")
    assert ser_count >= 1, "Expected at least one SER record"

    for i, rec in enumerate(records):
        rt = rec.get("record_type")
        if i == 0:
            assert rt == "pipeline_start"
            validate(rec, "semantiva/trace/schema/pipeline_start_event_v1.schema.json")
        elif i == len(records) - 1:
            assert rt == "pipeline_end"
            validate(rec, "semantiva/trace/schema/pipeline_end_event_v1.schema.json")
        else:
            assert rt == "ser"
            validate(
                rec,
                "semantiva/trace/schema/semantic_execution_record_v1.schema.json",
            )
