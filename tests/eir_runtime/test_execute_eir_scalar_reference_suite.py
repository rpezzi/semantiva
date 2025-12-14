from __future__ import annotations

from pathlib import Path

from semantiva.eir.compiler import compile_eir_v1
from semantiva.eir.execution_scalar import execute_eir_v1_scalar_plan
from semantiva.pipeline.payload import Payload
from semantiva.data_types import NoDataType
from semantiva.context_processors.context_types import ContextType


def _repo_root() -> Path:
    return Path(__file__).resolve().parents[2]


def test_execute_float_ref_01_from_eir() -> None:
    spec = _repo_root() / "tests" / "eir_reference_suite" / "float_ref_01.yaml"
    eir = compile_eir_v1(str(spec))
    payload = Payload(NoDataType(), ContextType({"value": 1.0, "addend": 2.0}))

    out = execute_eir_v1_scalar_plan(eir, payload)

    assert hasattr(out.data, "data")
    assert out.data.data == 3.0
    assert out.context.get_value("result") == 3.0


def test_execute_float_ref_02_from_eir() -> None:
    spec = _repo_root() / "tests" / "eir_reference_suite" / "float_ref_02.yaml"
    eir = compile_eir_v1(str(spec))
    payload = Payload(
        NoDataType(), ContextType({"value": 2.0, "factor": 10.0, "addend": -1.0})
    )

    out = execute_eir_v1_scalar_plan(eir, payload)

    assert hasattr(out.data, "data")
    assert out.data.data == 19.0
