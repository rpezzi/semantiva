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

"""
FP2 gate: runtime parity matrix for scalar reference suite.

Explicit 2x2 matrix (frontend x executor):

- YAML legacy executor
- YAML EIR-scalar executor
- Python legacy executor
- Python EIR-scalar executor

Parity definition (locked):
- Data float values MUST match exactly.
- Context dictionaries MUST match exactly.

This file is a parity-proof gate only; it must not require stronger equivalence
(e.g., wrapper class identity) than the locked definition.
"""

from __future__ import annotations

from dataclasses import dataclass
from pathlib import Path
from typing import Any, Dict, List

import pytest
import yaml

from semantiva.configurations.load_pipeline_from_yaml import load_pipeline_from_yaml
from semantiva.context_processors.context_types import ContextType
from semantiva.data_types import NoDataType
from semantiva.eir.compiler import compile_eir_v1
from semantiva.eir.execution_scalar import execute_eir_v1_scalar_plan
from semantiva.logger import Logger
from semantiva.pipeline.payload import Payload
from semantiva.pipeline.pipeline import Pipeline

from tests.eir_reference_suite_python.float_ref_01 import (
    build_pipeline_spec as build_py_ref_01,
)
from tests.eir_reference_suite_python.float_ref_02 import (
    build_pipeline_spec as build_py_ref_02,
)


def _repo_root() -> Path:
    return Path(__file__).resolve().parents[2]


LEDGER_PATH = _repo_root() / "docs" / "source" / "eir" / "eir_series_status.yaml"


@dataclass(frozen=True)
class ScalarRuntimeCase:
    ref_id: str
    yaml_path: Path
    initial_context: Dict[str, Any]
    expected_data_float: float
    expected_added_context_keys: List[str]


def _load_scalar_cases_from_ledger() -> List[ScalarRuntimeCase]:
    """
    FP2 is scoped to scalar reference suite (float_ref_01, float_ref_02).
    We read SSOT from eir_series_status.yaml to avoid drift.
    """
    raw = yaml.safe_load(LEDGER_PATH.read_text(encoding="utf-8"))
    assert isinstance(raw, dict)
    suite = raw["eir_series"]["reference_suite"]["float"]
    assert isinstance(suite, list)

    wanted = {"float_ref_01", "float_ref_02"}
    out: List[ScalarRuntimeCase] = []
    for entry in suite:
        if not isinstance(entry, dict):
            continue
        ref_id = str(entry.get("id") or "")
        if ref_id not in wanted:
            continue

        yaml_rel = entry["yaml_path"]
        yaml_path = _repo_root() / str(yaml_rel)

        rc = entry["runtime_contract"]
        init_ctx = dict(rc["initial_context"])
        expected = rc["expected"]
        expected_float = float(expected["data_float"])
        expected_added = list(expected.get("context_keys_added") or [])

        out.append(
            ScalarRuntimeCase(
                ref_id=ref_id,
                yaml_path=yaml_path,
                initial_context=init_ctx,
                expected_data_float=expected_float,
                expected_added_context_keys=expected_added,
            )
        )

    got_ids = {c.ref_id for c in out}
    assert got_ids == wanted, f"FP2 requires scalar cases {wanted}, got {got_ids}"
    return sorted(out, key=lambda c: c.ref_id)


def _extract_scalar_float(out: Payload) -> float:
    """
    Locked parity: compare float scalar values exactly.
    Avoid asserting output wrapper type identity.
    """
    assert hasattr(out.data, "data"), "Expected a FloatDataType-like output with .data"
    return float(out.data.data)


def _assert_locked_parity(a: Payload, b: Payload) -> None:
    assert _extract_scalar_float(a) == _extract_scalar_float(b)
    assert a.context.to_dict() == b.context.to_dict()


def _assert_matches_reference_contract(case: ScalarRuntimeCase, out: Payload) -> None:
    assert _extract_scalar_float(out) == case.expected_data_float

    before = set(case.initial_context.keys())
    after = set(out.context.to_dict().keys())
    added = sorted(after - before)
    assert added == sorted(case.expected_added_context_keys)

    # If the reference expects a 'result' key, its value must be exact.
    if "result" in case.expected_added_context_keys:
        assert out.context.get_value("result") == case.expected_data_float


def _run_yaml_legacy(case: ScalarRuntimeCase) -> Payload:
    cfg = load_pipeline_from_yaml(str(case.yaml_path))
    pipe = Pipeline(cfg.nodes)
    payload = Payload(NoDataType(), ContextType(dict(case.initial_context)))
    return pipe.process(payload)


def _run_yaml_eir(case: ScalarRuntimeCase) -> Payload:
    eir = compile_eir_v1(str(case.yaml_path))
    payload = Payload(NoDataType(), ContextType(dict(case.initial_context)))
    return execute_eir_v1_scalar_plan(eir, payload, logger=Logger())


def _run_py_legacy(case: ScalarRuntimeCase, py_spec: List[Dict[str, Any]]) -> Payload:
    pipe = Pipeline(py_spec)
    payload = Payload(NoDataType(), ContextType(dict(case.initial_context)))
    return pipe.process(payload)


def _run_py_eir(case: ScalarRuntimeCase, py_spec: List[Dict[str, Any]]) -> Payload:
    eir = compile_eir_v1(py_spec)
    payload = Payload(NoDataType(), ContextType(dict(case.initial_context)))
    return execute_eir_v1_scalar_plan(eir, payload, logger=Logger())


def _python_spec_for(ref_id: str) -> List[Dict[str, Any]]:
    if ref_id == "float_ref_01":
        return build_py_ref_01()
    if ref_id == "float_ref_02":
        return build_py_ref_02()
    raise AssertionError(f"Unsupported scalar ref_id for FP2: {ref_id}")


@pytest.mark.parametrize(
    "case", _load_scalar_cases_from_ledger(), ids=lambda c: c.ref_id
)
def test_fp2_runtime_parity_matrix_scalar(case: ScalarRuntimeCase) -> None:
    """
    Integration gate: execute all 4 matrix cells and enforce locked parity comparisons.

    MUST:
      - YAML legacy vs YAML EIR
      - Python legacy vs Python EIR

    SHOULD:
      - YAML legacy vs Python legacy (frontend equivalence independent of EIR)
    """
    py_spec = _python_spec_for(case.ref_id)

    out_yaml_legacy = _run_yaml_legacy(case)
    out_yaml_eir = _run_yaml_eir(case)
    out_py_legacy = _run_py_legacy(case, py_spec)
    out_py_eir = _run_py_eir(case, py_spec)

    # Golden substitute (deterministic contract assertions)
    _assert_matches_reference_contract(case, out_yaml_legacy)
    _assert_matches_reference_contract(case, out_yaml_eir)
    _assert_matches_reference_contract(case, out_py_legacy)
    _assert_matches_reference_contract(case, out_py_eir)

    # MUST comparisons
    _assert_locked_parity(out_yaml_legacy, out_yaml_eir)
    _assert_locked_parity(out_py_legacy, out_py_eir)

    # SHOULD comparison
    _assert_locked_parity(out_yaml_legacy, out_py_legacy)
