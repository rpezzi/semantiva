from __future__ import annotations

from pathlib import Path
import importlib
import pytest

from semantiva.pipeline.graph_builder import build_canonical_spec, compute_pipeline_id
from semantiva.registry.processor_registry import ProcessorRegistry

from ._normalize import normalize_canonical_spec

pytestmark = pytest.mark.usefixtures("isolated_processor_registry")


def _repo_root() -> Path:
    return Path(__file__).resolve().parents[2]


def _register_examples() -> None:
    ProcessorRegistry.register_modules(["semantiva.examples.test_utils"])


def _load_python_spec(module_name: str) -> list[dict]:
    mod = importlib.import_module(module_name)
    return mod.build_pipeline_spec()


def test_canonical_spec_bytes_parity_float_ref_slice_01() -> None:
    _register_examples()
    yaml_path = (
        _repo_root() / "tests" / "eir_reference_suite" / "float_ref_slice_01.yaml"
    )
    py_spec = _load_python_spec("tests.eir_reference_suite_python.float_ref_slice_01")

    canon_yaml, _ = build_canonical_spec(str(yaml_path))
    canon_py, _ = build_canonical_spec(py_spec)

    a = normalize_canonical_spec(canon_yaml)
    b = normalize_canonical_spec(canon_py)

    assert a == b

    # Golden-style: derive is present on the slice node and processor_ref is base FQCN.
    nodes = a["nodes"]
    slice_node = nodes[1]
    assert slice_node["processor_ref"].endswith(".FloatMultiplyOperation")
    assert slice_node["derive"] is not None
    assert "slice" in slice_node["derive"]
    pre = slice_node["derive"]["slice"]
    assert pre["type"] == "derive.slice"
    assert pre["version"] == 1
    assert pre["element_ref"].endswith(".FloatMultiplyOperation")
    assert pre["collection"].endswith(".FloatDataCollection")


def test_pipeline_id_parity_float_ref_slice_01() -> None:
    _register_examples()
    yaml_path = (
        _repo_root() / "tests" / "eir_reference_suite" / "float_ref_slice_01.yaml"
    )
    py_spec = _load_python_spec("tests.eir_reference_suite_python.float_ref_slice_01")

    canon_yaml, _ = build_canonical_spec(str(yaml_path))
    canon_py, _ = build_canonical_spec(py_spec)

    assert compute_pipeline_id(canon_yaml) == compute_pipeline_id(canon_py)


def test_canonical_spec_bytes_parity_float_ref_sweep_01() -> None:
    _register_examples()
    yaml_path = (
        _repo_root() / "tests" / "eir_reference_suite" / "float_ref_sweep_01.yaml"
    )
    py_spec = _load_python_spec("tests.eir_reference_suite_python.float_ref_sweep_01")

    canon_yaml, _ = build_canonical_spec(str(yaml_path))
    canon_py, _ = build_canonical_spec(py_spec)

    a = normalize_canonical_spec(canon_yaml)
    b = normalize_canonical_spec(canon_py)

    assert a == b

    # Golden-style: derive is present on the sweep node and processor_ref is base FQCN.
    nodes = a["nodes"]
    sweep_node = nodes[1]
    assert sweep_node["processor_ref"].endswith(".FloatAddOperation")
    assert sweep_node["derive"] is not None
    assert "parameter_sweep" in sweep_node["derive"]
    pre = sweep_node["derive"]["parameter_sweep"]
    assert pre["type"] == "derive.parameter_sweep"
    assert pre["version"] == 1
    assert pre["element_ref"].endswith(".FloatAddOperation")
    assert pre["collection"].endswith(".FloatDataCollection")
    assert "param_expressions" in pre
    assert "variables" in pre


def test_pipeline_id_parity_float_ref_sweep_01() -> None:
    _register_examples()
    yaml_path = (
        _repo_root() / "tests" / "eir_reference_suite" / "float_ref_sweep_01.yaml"
    )
    py_spec = _load_python_spec("tests.eir_reference_suite_python.float_ref_sweep_01")

    canon_yaml, _ = build_canonical_spec(str(yaml_path))
    canon_py, _ = build_canonical_spec(py_spec)

    assert compute_pipeline_id(canon_yaml) == compute_pipeline_id(canon_py)
