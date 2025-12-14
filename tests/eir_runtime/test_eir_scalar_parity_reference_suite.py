from __future__ import annotations

from pathlib import Path

import yaml

from semantiva.configurations.load_pipeline_from_yaml import parse_pipeline_config
from semantiva.context_processors.context_types import ContextType
from semantiva.data_types import NoDataType
from semantiva.eir.compiler import compile_eir_v1
from semantiva.eir.execution_scalar import execute_eir_v1_scalar_plan
from semantiva.pipeline.payload import Payload
from semantiva.pipeline import Pipeline


def _repo_root() -> Path:
    return Path(__file__).resolve().parents[2]


def _run_legacy_from_yaml(spec_path: Path, payload: Payload) -> Payload:
    raw = yaml.safe_load(spec_path.read_text(encoding="utf-8")) or {}
    cfg = parse_pipeline_config(raw, source_path=str(spec_path), base_dir=spec_path.parent)
    pipe = Pipeline(cfg.nodes)
    return pipe.process(payload)


def _run_eir_from_yaml(spec_path: Path, payload: Payload) -> Payload:
    eir = compile_eir_v1(str(spec_path))
    return execute_eir_v1_scalar_plan(eir, payload)


def _assert_scalar_payload_parity(legacy_out: Payload, eir_out: Payload) -> None:
    # Data parity (scalar): keep it strict but stable.
    assert type(legacy_out.data) is type(eir_out.data)
    assert hasattr(legacy_out.data, "data")
    assert hasattr(eir_out.data, "data")
    assert legacy_out.data.data == eir_out.data.data

    # Context parity: ContextType has no __eq__.
    assert legacy_out.context.to_dict() == eir_out.context.to_dict()


def test_parity_float_ref_01_legacy_vs_eir() -> None:
    spec = _repo_root() / "tests" / "eir_reference_suite" / "float_ref_01.yaml"
    payload = Payload(NoDataType(), ContextType({"value": 1.0, "addend": 2.0}))

    legacy_out = _run_legacy_from_yaml(spec, payload)
    eir_out = _run_eir_from_yaml(spec, payload)

    _assert_scalar_payload_parity(legacy_out, eir_out)


def test_parity_float_ref_02_legacy_vs_eir() -> None:
    spec = _repo_root() / "tests" / "eir_reference_suite" / "float_ref_02.yaml"
    payload = Payload(NoDataType(), ContextType({"value": 2.0, "factor": 10.0, "addend": -1.0}))

    legacy_out = _run_legacy_from_yaml(spec, payload)
    eir_out = _run_eir_from_yaml(spec, payload)

    _assert_scalar_payload_parity(legacy_out, eir_out)
