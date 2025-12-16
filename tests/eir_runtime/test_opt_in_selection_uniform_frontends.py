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

from semantiva.context_processors.context_types import ContextType
from semantiva.data_types import NoDataType
from semantiva.execution.orchestrator.orchestrator import LocalSemantivaOrchestrator
from semantiva.execution.transport.in_memory import InMemorySemantivaTransport
from semantiva.logger import Logger
from semantiva.pipeline.payload import Payload
from tests.eir_reference_suite_python.float_ref_01 import (
    build_pipeline_spec as build_py_ref_01,
)
from tests.eir_reference_suite_python.float_ref_02 import (
    build_pipeline_spec as build_py_ref_02,
)


def _repo_root() -> Path:
    return Path(__file__).resolve().parents[2]


def test_opt_in_selector_reachable_from_yaml_and_python_frontends() -> None:
    orch = LocalSemantivaOrchestrator()
    transport = InMemorySemantivaTransport()
    logger = Logger()

    # YAML frontend
    yaml_01 = str(_repo_root() / "tests" / "eir_reference_suite" / "float_ref_01.yaml")
    payload_yaml = Payload(NoDataType(), ContextType({"value": 1.0, "addend": 2.0}))

    out_yaml_legacy = orch.execute(
        pipeline_spec=yaml_01,
        payload=payload_yaml,
        transport=transport,
        logger=logger,
    )
    out_yaml_eir = orch.execute(
        pipeline_spec=yaml_01,
        payload=Payload(NoDataType(), ContextType({"value": 1.0, "addend": 2.0})),
        transport=transport,
        logger=logger,
        execution_backend="eir_scalar",
    )

    assert out_yaml_legacy.data.data == 3.0
    assert out_yaml_eir.data.data == 3.0
    assert out_yaml_eir.context.get_value("result") == 3.0

    # Python frontend (developer workflow: list-of-node-dicts)
    py_02 = build_py_ref_02()
    payload_py = Payload(
        NoDataType(), ContextType({"value": 2.0, "factor": 10.0, "addend": -1.0})
    )

    out_py_legacy = orch.execute(
        pipeline_spec=py_02,
        payload=payload_py,
        transport=transport,
        logger=logger,
    )
    out_py_eir = orch.execute(
        pipeline_spec=py_02,
        payload=Payload(
            NoDataType(), ContextType({"value": 2.0, "factor": 10.0, "addend": -1.0})
        ),
        transport=transport,
        logger=logger,
        execution_backend="eir_scalar",
    )

    assert out_py_legacy.data.data == 19.0
    assert out_py_eir.data.data == 19.0


def test_opt_in_does_not_change_default_backend_for_python_specs() -> None:
    orch = LocalSemantivaOrchestrator()
    py_01 = build_py_ref_01()
    payload = Payload(NoDataType(), ContextType({"value": 1.0, "addend": 2.0}))

    out = orch.execute(
        pipeline_spec=py_01,
        payload=payload,
        transport=InMemorySemantivaTransport(),
        logger=Logger(),
    )

    assert out.data.data == 3.0
