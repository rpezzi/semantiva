from __future__ import annotations

import importlib.util
from pathlib import Path
from typing import Any, Callable

from semantiva.eir import compile_eir_v1, validate_eir_v1
from tests.eir_frontend_parity._normalize import canonical_bytes, normalize_eir_doc

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


def _compile_yaml(ref_id: str) -> dict[str, Any]:
    yaml_path = YAML_SUITE / f"{ref_id}.yaml"
    assert yaml_path.exists(), yaml_path
    return compile_eir_v1(str(yaml_path))


def _compile_python(ref_id: str) -> dict[str, Any]:
    py_path = PY_SUITE / f"{ref_id}.py"
    assert py_path.exists(), py_path
    build_fn = _load_python_builder(py_path)
    nodes = build_fn()
    return compile_eir_v1(nodes)


def _assert_eir_semantics_parity(ref_id: str) -> None:
    eir_yaml = _compile_yaml(ref_id)
    eir_py = _compile_python(ref_id)

    # Integration: both must be schema-valid as produced.
    validate_eir_v1(eir_yaml)
    validate_eir_v1(eir_py)

    # Unit: semantic equality modulo provenance.
    norm_yaml = normalize_eir_doc(eir_yaml)
    norm_py = normalize_eir_doc(eir_py)

    assert canonical_bytes(norm_yaml) == canonical_bytes(norm_py)

    # Convenience: ensure identity is aligned (implicitly enforced by byte equality).
    assert norm_yaml.get("identity", {}).get("pipeline_id") == norm_py.get(
        "identity", {}
    ).get("pipeline_id")


def test_eir_semantics_parity_float_ref_01() -> None:
    _assert_eir_semantics_parity("float_ref_01")


def test_eir_semantics_parity_float_ref_02() -> None:
    _assert_eir_semantics_parity("float_ref_02")


def test_eir_compilation_deterministic_repeat_builds() -> None:
    """
    Golden substitute (FP0c): repeated builds produce identical normalized bytes.

    This specifically guards against accidental inclusion of ephemeral provenance
    into semantic-bearing sections (identity/graph/parameters/plan/semantics/lineage).
    """
    for ref_id in ("float_ref_01", "float_ref_02"):
        y1 = normalize_eir_doc(_compile_yaml(ref_id))
        y2 = normalize_eir_doc(_compile_yaml(ref_id))
        assert canonical_bytes(y1) == canonical_bytes(y2)

        p1 = normalize_eir_doc(_compile_python(ref_id))
        p2 = normalize_eir_doc(_compile_python(ref_id))
        assert canonical_bytes(p1) == canonical_bytes(p2)
