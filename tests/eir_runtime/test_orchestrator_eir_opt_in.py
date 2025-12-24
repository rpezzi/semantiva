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

from pathlib import Path

import pytest

from semantiva.context_processors.context_types import ContextType
from semantiva.data_types import NoDataType
from semantiva.execution.orchestrator.orchestrator import LocalSemantivaOrchestrator
from semantiva.execution.transport.in_memory import InMemorySemantivaTransport
from semantiva.logger import Logger
from semantiva.pipeline.payload import Payload


def _repo_root() -> Path:
    return Path(__file__).resolve().parents[2]


def test_orchestrator_default_is_legacy() -> None:
    orch = LocalSemantivaOrchestrator()
    spec = str(_repo_root() / "tests" / "eir_reference_suite" / "float_ref_01.yaml")
    payload = Payload(NoDataType(), ContextType({"value": 1.0, "addend": 2.0}))

    out = orch.execute(
        pipeline_spec=spec,
        payload=payload,
        transport=InMemorySemantivaTransport(),
        logger=Logger(),
    )

    assert hasattr(out.data, "data")


def test_orchestrator_opt_in_eir_scalar_float_ref_01() -> None:
    orch = LocalSemantivaOrchestrator()
    spec = str(_repo_root() / "tests" / "eir_reference_suite" / "float_ref_01.yaml")
    payload = Payload(NoDataType(), ContextType({"value": 1.0, "addend": 2.0}))

    out = orch.execute(
        pipeline_spec=spec,
        payload=payload,
        transport=InMemorySemantivaTransport(),
        logger=Logger(),
        execution_backend="eir_scalar",
    )

    assert out.data.data == 3.0
    assert out.context.get_value("result") == 3.0


def test_orchestrator_opt_in_eir_scalar_float_ref_02() -> None:
    orch = LocalSemantivaOrchestrator()
    spec = str(_repo_root() / "tests" / "eir_reference_suite" / "float_ref_02.yaml")
    payload = Payload(
        NoDataType(), ContextType({"value": 2.0, "factor": 10.0, "addend": -1.0})
    )

    out = orch.execute(
        pipeline_spec=spec,
        payload=payload,
        transport=InMemorySemantivaTransport(),
        logger=Logger(),
        execution_backend="eir_scalar",
    )

    assert out.data.data == 19.0


def test_orchestrator_unknown_backend() -> None:
    orch = LocalSemantivaOrchestrator()
    payload = Payload(NoDataType(), ContextType({}))

    with pytest.raises(ValueError):
        orch.execute(
            pipeline_spec=[],
            payload=payload,
            transport=InMemorySemantivaTransport(),
            logger=Logger(),
            execution_backend="not-a-backend",  # type: ignore[arg-type]
        )


def test_orchestrator_opt_in_eir_scalar_accepts_python_list_spec() -> None:
    from tests.eir_reference_suite_python.float_ref_01 import (
        build_pipeline_spec as build_py_ref_01,
    )

    orch = LocalSemantivaOrchestrator()
    spec = build_py_ref_01()
    payload = Payload(NoDataType(), ContextType({"value": 1.0, "addend": 2.0}))

    out = orch.execute(
        pipeline_spec=spec,
        payload=payload,
        transport=InMemorySemantivaTransport(),
        logger=Logger(),
        execution_backend="eir_scalar",
    )

    assert out.data.data == 3.0


def test_execution_payload_algebra_module_importable() -> None:
    import semantiva.eir.execution_payload_algebra as _  # noqa: F401


def test_orchestrator_opt_in_eir_payload_algebra_float_ref_01() -> None:
    orch = LocalSemantivaOrchestrator()
    spec = str(_repo_root() / "tests" / "eir_reference_suite" / "float_ref_01.yaml")
    payload = Payload(NoDataType(), ContextType({"value": 1.0, "addend": 2.0}))

    out = orch.execute(
        pipeline_spec=spec,
        payload=payload,
        transport=InMemorySemantivaTransport(),
        logger=Logger(),
        execution_backend="eir_payload_algebra",
    )

    assert out.data.data == 3.0
    assert out.context.get_value("result") == 3.0
