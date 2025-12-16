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
from importlib import resources
from pathlib import Path
from typing import Any, Dict, List

import pytest

try:
    import jsonschema
except Exception:  # pragma: no cover - optional dependency
    jsonschema = None
    pytest.skip("jsonschema not installed", allow_module_level=True)

from semantiva.context_processors.context_types import ContextType
from semantiva.data_types import NoDataType
from semantiva.execution.orchestrator.orchestrator import LocalSemantivaOrchestrator
from semantiva.execution.transport.in_memory import InMemorySemantivaTransport
from semantiva.logger import Logger
from semantiva.pipeline.payload import Payload
from semantiva.trace.drivers.jsonl import JsonlTraceDriver


def _repo_root() -> Path:
    return Path(__file__).resolve().parents[2]


def _load_jsonl(path: Path) -> List[Dict[str, Any]]:
    return [
        json.loads(line)
        for line in path.read_text(encoding="utf-8").splitlines()
        if line.strip()
    ]


def _normalize(rec: Dict[str, Any]) -> Dict[str, Any]:
    rec = dict(rec)
    for key in ("timestamp", "created_at", "duration_ms"):
        rec.pop(key, None)
    rec.pop("run_id", None)
    if isinstance(rec.get("identity"), dict):
        rec["identity"] = {k: v for k, v in rec["identity"].items() if k != "run_id"}
    if "summaries" in rec:
        rec.pop("summaries")
    rec.pop("timing", None)
    return rec


def _validator() -> jsonschema.Draft202012Validator:  # type: ignore[name-defined]
    schema_path = (
        resources.files("semantiva.trace.schema")
        / "semantic_execution_record_v1.schema.json"
    )
    schema = json.loads(schema_path.read_text(encoding="utf-8"))
    return jsonschema.Draft202012Validator(schema)  # type: ignore[attr-defined]


def test_trace_equivalence_legacy_vs_eir_scalar_float_ref_01(tmp_path: Path) -> None:
    spec = str(_repo_root() / "tests" / "eir_reference_suite" / "float_ref_01.yaml")
    payload_legacy = Payload(NoDataType(), ContextType({"value": 1.0, "addend": 2.0}))
    payload_eir = Payload(NoDataType(), ContextType({"value": 1.0, "addend": 2.0}))

    legacy_path = tmp_path / "legacy.jsonl"
    eir_path = tmp_path / "eir.jsonl"

    validator = _validator()

    orch_legacy = LocalSemantivaOrchestrator()
    orch_eir = LocalSemantivaOrchestrator()

    tracer_legacy = JsonlTraceDriver(str(legacy_path))
    tracer_eir = JsonlTraceDriver(str(eir_path))

    orch_legacy.execute(
        pipeline_spec=spec,
        payload=payload_legacy,
        transport=InMemorySemantivaTransport(),
        logger=Logger(),
        trace=tracer_legacy,
    )
    orch_eir.execute(
        pipeline_spec=spec,
        payload=payload_eir,
        transport=InMemorySemantivaTransport(),
        logger=Logger(),
        trace=tracer_eir,
        execution_backend="eir_scalar",
    )

    tracer_legacy.close()
    tracer_eir.close()

    legacy_raw = _load_jsonl(legacy_path)
    eir_raw = _load_jsonl(eir_path)

    for rec in legacy_raw + eir_raw:
        if rec.get("record_type") == "ser":
            validator.validate(rec)

    legacy = [_normalize(x) for x in legacy_raw]
    eir = [_normalize(x) for x in eir_raw]

    assert legacy == eir


def test_trace_equivalence_legacy_vs_eir_scalar_float_ref_01_python_spec(
    tmp_path: Path,
) -> None:
    from tests.eir_reference_suite_python.float_ref_01 import (
        build_pipeline_spec as build_py_ref_01,
    )

    spec = build_py_ref_01()
    payload_legacy = Payload(NoDataType(), ContextType({"value": 1.0, "addend": 2.0}))
    payload_eir = Payload(NoDataType(), ContextType({"value": 1.0, "addend": 2.0}))

    legacy_path = tmp_path / "legacy_py.jsonl"
    eir_path = tmp_path / "eir_py.jsonl"

    validator = _validator()

    orch_legacy = LocalSemantivaOrchestrator()
    orch_eir = LocalSemantivaOrchestrator()

    tracer_legacy = JsonlTraceDriver(str(legacy_path))
    tracer_eir = JsonlTraceDriver(str(eir_path))

    orch_legacy.execute(
        pipeline_spec=spec,
        payload=payload_legacy,
        transport=InMemorySemantivaTransport(),
        logger=Logger(),
        trace=tracer_legacy,
    )
    orch_eir.execute(
        pipeline_spec=spec,
        payload=payload_eir,
        transport=InMemorySemantivaTransport(),
        logger=Logger(),
        trace=tracer_eir,
        execution_backend="eir_scalar",
    )

    tracer_legacy.close()
    tracer_eir.close()

    legacy_raw = _load_jsonl(legacy_path)
    eir_raw = _load_jsonl(eir_path)

    for rec in legacy_raw + eir_raw:
        if rec.get("record_type") == "ser":
            validator.validate(rec)

    legacy = [_normalize(x) for x in legacy_raw]
    eir = [_normalize(x) for x in eir_raw]

    assert legacy == eir
