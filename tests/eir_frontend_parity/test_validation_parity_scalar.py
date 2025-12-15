from __future__ import annotations

import importlib.util
from pathlib import Path
from typing import Any, Callable

from semantiva.pipeline.graph_builder import build_canonical_spec
from semantiva.pipeline.preflight_validation import validate_canonical_spec_v1

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


def _strip_frontend(diags: list[Any]) -> list[dict[str, Any]]:
    out: list[dict[str, Any]] = []
    for d in diags:
        out.append(
            {
                "code": d.code,
                "severity": d.severity,
                "message": d.message,
                "hint": d.hint,
                "node_id": d.node_id,
                "user_id": d.user_id,
                "processor_fqcn": d.processor_fqcn,
                "details": d.details,
            }
        )
    return out


def test_validation_parity_valid_scalar_refs_are_clean() -> None:
    for ref_id in ("float_ref_01", "float_ref_02"):
        yaml_path = YAML_SUITE / f"{ref_id}.yaml"
        py_path = PY_SUITE / f"{ref_id}.py"

        canon_yaml = _canonical_from_yaml_path(yaml_path)
        canon_py = _canonical_from_python_peer(py_path)

        dy = validate_canonical_spec_v1(canon_yaml, frontend_kind="yaml")
        dp = validate_canonical_spec_v1(canon_py, frontend_kind="python")

        assert dy == []
        assert dp == []


def test_validation_parity_same_canonical_spec_same_diagnostics_modulo_frontend() -> (
    None
):
    ref_id = "float_ref_01"
    yaml_path = YAML_SUITE / f"{ref_id}.yaml"
    canon = _canonical_from_yaml_path(yaml_path)

    canon_bad = dict(canon)
    nodes = [dict(n) for n in canon_bad["nodes"]]
    nodes[0] = dict(nodes[0])
    nodes[0]["processor_ref"] = "NotAQualifiedName"
    canon_bad["nodes"] = nodes

    dy = validate_canonical_spec_v1(canon_bad, frontend_kind="yaml")
    dp = validate_canonical_spec_v1(canon_bad, frontend_kind="python")

    assert _strip_frontend(dy) == _strip_frontend(dp)

    assert dy and dp
    for d in dy:
        assert d.frontend_kind == "yaml"
        assert d.code.startswith("SVA")
        assert d.node_id is not None
        assert d.processor_fqcn is not None
        assert isinstance(d.hint, str) and d.hint

    for d in dp:
        assert d.frontend_kind == "python"
