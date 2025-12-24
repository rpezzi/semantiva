# Copyright 2025 Semantiva authors
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.

from __future__ import annotations

import json
from pathlib import Path
from typing import Any, Dict, List

from semantiva.context_processors.context_types import ContextType
from semantiva.data_types import NoDataType
from semantiva.examples.test_utils import FloatDataType
from semantiva.execution.orchestrator.orchestrator import LocalSemantivaOrchestrator
from semantiva.execution.transport.in_memory import InMemorySemantivaTransport
from semantiva.logger import Logger
from semantiva.pipeline.payload import Payload
from semantiva.trace.drivers.jsonl import JsonlTraceDriver


def _repo_root() -> Path:
    return Path(__file__).resolve().parents[2]


def _fixture(name: str) -> str:
    return str(_repo_root() / "tests" / "payload_algebra_reference_suite" / name)


def _load_jsonl(path: Path) -> List[Dict[str, Any]]:
    return [
        json.loads(line)
        for line in path.read_text(encoding="utf-8").splitlines()
        if line.strip()
    ]


def test_slot_suite_trace_smoke_jsonl_exists(tmp_path: Path) -> None:
    """
    Plan PA-03C golden gate: minimal trace JSONL exists.
    Explicitly NO provenance assertions (PA-03D).
    Explicitly NO schema validation requirement for §4 (PA-03D).
    """
    spec = _fixture("float_ref_slots_41_baseline.yaml")
    payload = Payload(
        NoDataType(), ContextType({"value": 1.0, "other": FloatDataType(2.0)})
    )

    trace_path = tmp_path / "pa03c_slot_suite.jsonl"
    tracer = JsonlTraceDriver(str(trace_path))

    out = LocalSemantivaOrchestrator().execute(
        pipeline_spec=spec,
        payload=payload,
        transport=InMemorySemantivaTransport(),
        logger=Logger(),
        trace=tracer,
        execution_backend="eir_payload_algebra",
    )
    assert float(out.data.data) == 3.0

    tracer.close()

    assert trace_path.exists()
    records = _load_jsonl(trace_path)
    assert records, "Trace must not be empty"

    types = [r.get("record_type") for r in records]
    assert "pipeline_start" in types
    assert "ser" in types
