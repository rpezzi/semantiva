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

"""Smoke tests for Python reference suite (legacy + EIR scalar execution)."""

from __future__ import annotations

from typing import Any, Dict, List

from semantiva.context_processors.context_types import ContextType
from semantiva.data_types import NoDataType
from semantiva.eir.compiler import compile_eir_v1
from semantiva.eir.execution_scalar import execute_eir_v1_scalar_plan
from semantiva.logger import Logger
from semantiva.pipeline.payload import Payload
from semantiva.pipeline.pipeline import Pipeline

from tests.eir_reference_suite_python.float_ref_01 import (
    build_pipeline_spec as build_ref_01,
)
from tests.eir_reference_suite_python.float_ref_02 import (
    build_pipeline_spec as build_ref_02,
)


def _run_legacy(spec: List[Dict[str, Any]], payload: Payload) -> Payload:
    pipe = Pipeline(spec)
    return pipe.process(payload)


def _run_eir_scalar(spec: List[Dict[str, Any]], payload: Payload) -> Payload:
    eir = compile_eir_v1(spec)
    return execute_eir_v1_scalar_plan(eir, payload, logger=Logger())


def test_python_ref_01_import_and_smoke_legacy_and_eir_scalar() -> None:
    spec = build_ref_01()
    payload = Payload(NoDataType(), ContextType({"value": 1.0, "addend": 2.0}))

    out_legacy = _run_legacy(spec, payload)
    assert hasattr(out_legacy.data, "data")

    out_eir = _run_eir_scalar(spec, payload)
    assert out_eir.data.data == 3.0
    assert out_eir.context.get_value("result") == 3.0


def test_python_ref_02_import_and_smoke_legacy_and_eir_scalar() -> None:
    spec = build_ref_02()
    payload = Payload(
        NoDataType(), ContextType({"value": 2.0, "factor": 10.0, "addend": -1.0})
    )

    out_legacy = _run_legacy(spec, payload)
    assert hasattr(out_legacy.data, "data")

    out_eir = _run_eir_scalar(spec, payload)
    assert out_eir.data.data == 19.0
