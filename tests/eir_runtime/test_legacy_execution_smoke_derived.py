from __future__ import annotations

from pathlib import Path
import importlib

import yaml

from semantiva.configurations.load_pipeline_from_yaml import parse_pipeline_config
from semantiva.context_processors.context_types import ContextType
from semantiva.data_types import NoDataType
from semantiva.pipeline.payload import Payload
from semantiva.pipeline import Pipeline


def _repo_root() -> Path:
    return Path(__file__).resolve().parents[2]


def _run_legacy_from_yaml(spec_path: Path, payload: Payload) -> Payload:
    raw = yaml.safe_load(spec_path.read_text(encoding="utf-8")) or {}
    cfg = parse_pipeline_config(
        raw, source_path=str(spec_path), base_dir=spec_path.parent
    )
    pipe = Pipeline(cfg.nodes)
    return pipe.process(payload)


def _run_legacy_from_python(module_name: str, payload: Payload) -> Payload:
    mod = importlib.import_module(module_name)
    spec = mod.build_pipeline_spec()
    pipe = Pipeline(spec)
    return pipe.process(payload)


def test_legacy_smoke_float_ref_slice_01_yaml() -> None:
    spec = _repo_root() / "tests" / "eir_reference_suite" / "float_ref_slice_01.yaml"
    payload = Payload(
        NoDataType(), ContextType({"values": [1.0, 2.0, 3.0], "factor": 10.0})
    )
    out = _run_legacy_from_yaml(spec, payload)

    assert hasattr(out.data, "data")
    assert out.data.data == 60.0
    assert out.context.to_dict()["result"] == 60.0


def test_legacy_smoke_float_ref_slice_01_python() -> None:
    payload = Payload(
        NoDataType(), ContextType({"values": [1.0, 2.0, 3.0], "factor": 10.0})
    )
    out = _run_legacy_from_python(
        "tests.eir_reference_suite_python.float_ref_slice_01", payload
    )

    assert hasattr(out.data, "data")
    assert out.data.data == 60.0
    assert out.context.to_dict()["result"] == 60.0


def test_legacy_smoke_float_ref_sweep_01_yaml() -> None:
    spec = _repo_root() / "tests" / "eir_reference_suite" / "float_ref_sweep_01.yaml"
    payload = Payload(NoDataType(), ContextType({"value": 1.0}))
    out = _run_legacy_from_yaml(spec, payload)

    assert hasattr(out.data, "data")
    assert out.data.data == 9.0
    assert out.context.to_dict()["result"] == 9.0


def test_legacy_smoke_float_ref_sweep_01_python() -> None:
    payload = Payload(NoDataType(), ContextType({"value": 1.0}))
    out = _run_legacy_from_python(
        "tests.eir_reference_suite_python.float_ref_sweep_01", payload
    )

    assert hasattr(out.data, "data")
    assert out.data.data == 9.0
    assert out.context.to_dict()["result"] == 9.0
