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

import hashlib
import json
from dataclasses import dataclass
from importlib import resources
from pathlib import Path
from typing import Any, Dict, List

import pytest
import yaml

from semantiva import Payload
from semantiva.configurations import load_pipeline_from_yaml
from semantiva.context_processors import ContextType
from semantiva.data_types import NoDataType
from semantiva.pipeline import Pipeline
from semantiva.pipeline.graph_builder import build_canonical_spec, compute_pipeline_id
from semantiva.trace.drivers.jsonl import JsonlTraceDriver

try:
    import jsonschema
except Exception:  # pragma: no cover - optional dependency
    jsonschema = None


REPO_ROOT = Path(__file__).parents[2]
LEDGER_PATH = REPO_ROOT / "docs" / "source" / "eir" / "eir_series_status.yaml"
SUITE_DIR = REPO_ROOT / "tests" / "eir_reference_suite"


@dataclass(frozen=True)
class RefCase:
    ref_id: str
    yaml_path: Path
    initial_context: Dict[str, Any]
    expected_data_float: float
    expected_added_context_keys: List[str]


REF_CASES: List[RefCase] = [
    RefCase(
        ref_id="float_ref_01",
        yaml_path=SUITE_DIR / "float_ref_01.yaml",
        initial_context={"value": 1.0, "addend": 2.0},
        expected_data_float=3.0,
        expected_added_context_keys=["result"],
    ),
    RefCase(
        ref_id="float_ref_02",
        yaml_path=SUITE_DIR / "float_ref_02.yaml",
        initial_context={"value": 2.0, "factor": 10.0, "addend": -1.0},
        expected_data_float=19.0,
        expected_added_context_keys=[],
    ),
]


def _sha256_file(path: Path) -> str:
    return hashlib.sha256(path.read_bytes()).hexdigest()


def _load_ledger() -> Dict[str, Any]:
    assert LEDGER_PATH.exists(), f"Missing SSOT ledger at {LEDGER_PATH}"
    data = yaml.safe_load(LEDGER_PATH.read_text(encoding="utf-8"))
    assert isinstance(data, dict) and "eir_series" in data
    return data


def _ledger_float_entries() -> Dict[str, Dict[str, Any]]:
    data = _load_ledger()
    suite = data["eir_series"]["reference_suite"]["float"]
    assert isinstance(suite, list)
    out: Dict[str, Dict[str, Any]] = {}
    for entry in suite:
        assert isinstance(entry, dict) and "id" in entry
        out[str(entry["id"])] = entry
    return out


def test_eir_series_ledger_contract_and_drift_detection() -> None:
    """Unit + golden: enforce ledger presence and detect reference drift."""
    entries = _ledger_float_entries()

    for case in REF_CASES:
        assert case.ref_id in entries, f"Ledger missing entry for {case.ref_id}"
        e = entries[case.ref_id]

        yaml_path = REPO_ROOT / str(e["yaml_path"])
        assert yaml_path.exists(), f"Ledger yaml_path missing: {yaml_path}"

        # Drift: raw YAML bytes
        assert e["yaml_sha256"] == _sha256_file(yaml_path)

        # Drift: canonical graph -> pipeline_id
        canonical, _ = build_canonical_spec(str(yaml_path))
        assert e["pipeline_id"] == compute_pipeline_id(canonical)


@pytest.mark.parametrize("case", REF_CASES, ids=[c.ref_id for c in REF_CASES])
def test_float_reference_suite_executes(case: RefCase) -> None:
    """Integration: execute the reference pipelines under current runtime tooling."""
    cfg = load_pipeline_from_yaml(str(case.yaml_path))
    pipeline = Pipeline(cfg.nodes)

    payload = Payload(NoDataType(), ContextType(dict(case.initial_context)))
    out = pipeline.process(payload)

    # Avoid importing FloatDataType directly; validate via expected data contract.
    assert hasattr(out.data, "data"), "Expected a FloatDataType-like output with .data"
    assert float(out.data.data) == case.expected_data_float

    before = set(case.initial_context.keys())
    after = set(out.context.keys())
    added = sorted(after - before)
    assert added == sorted(case.expected_added_context_keys)


def test_float_reference_suite_emits_ser_schema_conformant(tmp_path: Path) -> None:
    """Golden: ensure SER emission remains schema-valid for a reference pipeline."""
    if jsonschema is None:  # pragma: no cover
        pytest.skip("jsonschema not installed")

    case = REF_CASES[0]
    cfg = load_pipeline_from_yaml(str(case.yaml_path))

    trace_path = tmp_path / "eir_ref.ser.jsonl"
    tracer = JsonlTraceDriver(str(trace_path))
    pipeline = Pipeline(cfg.nodes, trace=tracer)

    payload = Payload(NoDataType(), ContextType(dict(case.initial_context)))
    pipeline.process(payload)
    tracer.close()

    schema_path = (
        resources.files("semantiva.trace.schema")
        / "semantic_execution_record_v1.schema.json"
    )
    schema = json.loads(schema_path.read_text(encoding="utf-8"))
    validator = jsonschema.Draft202012Validator(schema)

    for line in trace_path.read_text(encoding="utf-8").splitlines():
        rec = json.loads(line)
        if rec.get("record_type") == "ser":
            validator.validate(rec)
