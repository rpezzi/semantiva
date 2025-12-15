from __future__ import annotations

import importlib.util
from pathlib import Path
from typing import Any, Callable

from semantiva.pipeline.graph_builder import build_canonical_spec, compute_pipeline_id
from tests.eir_frontend_parity._normalize import (
    canonical_bytes,
    normalize_canonical_spec,
)

REPO_ROOT = Path(__file__).parents[2]
YAML_SUITE = REPO_ROOT / "tests" / "eir_reference_suite"
PY_SUITE = REPO_ROOT / "tests" / "eir_reference_suite_python"


def _load_python_builder(py_path: Path) -> Callable[[], list[dict[str, Any]]]:
    spec = importlib.util.spec_from_file_location(py_path.stem, str(py_path))
    assert spec and spec.loader
    mod = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(mod)  # type: ignore[union-attr]
    fn = getattr(mod, "build_pipeline_spec", None)
    assert callable(fn), f"{py_path} must define build_pipeline_spec()"
    return fn  # type: ignore[return-value]


def _canonical_from_yaml_path(yaml_path: Path) -> dict[str, Any]:
    canon, _ = build_canonical_spec(str(yaml_path))
    return canon


def _canonical_from_python_peer(py_path: Path) -> dict[str, Any]:
    build_fn = _load_python_builder(py_path)
    nodes = build_fn()
    canon, _ = build_canonical_spec(nodes)
    return canon


def _assert_parity(ref_id: str) -> None:
    yaml_path = YAML_SUITE / f"{ref_id}.yaml"
    py_path = PY_SUITE / f"{ref_id}.py"
    assert yaml_path.exists(), yaml_path
    assert py_path.exists(), py_path

    canon_yaml = _canonical_from_yaml_path(yaml_path)
    canon_py = _canonical_from_python_peer(py_path)

    norm_yaml = normalize_canonical_spec(canon_yaml)
    norm_py = normalize_canonical_spec(canon_py)

    assert canonical_bytes(norm_yaml) == canonical_bytes(norm_py)
    assert compute_pipeline_id(canon_yaml) == compute_pipeline_id(canon_py)


def test_canonical_spec_parity_float_ref_01() -> None:
    _assert_parity("float_ref_01")


def test_canonical_spec_parity_float_ref_02() -> None:
    _assert_parity("float_ref_02")


def test_canonical_spec_deterministic_repeat_build() -> None:
    ref_id = "float_ref_01"
    yaml_path = YAML_SUITE / f"{ref_id}.yaml"
    py_path = PY_SUITE / f"{ref_id}.py"

    c1 = normalize_canonical_spec(_canonical_from_yaml_path(yaml_path))
    c2 = normalize_canonical_spec(_canonical_from_yaml_path(yaml_path))
    assert canonical_bytes(c1) == canonical_bytes(c2)

    p1 = normalize_canonical_spec(_canonical_from_python_peer(py_path))
    p2 = normalize_canonical_spec(_canonical_from_python_peer(py_path))
    assert canonical_bytes(p1) == canonical_bytes(p2)
