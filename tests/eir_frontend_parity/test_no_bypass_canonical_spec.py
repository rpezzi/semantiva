from __future__ import annotations

from pathlib import Path
from typing import Any

import pytest

from semantiva.eir import compile_eir_v1
import semantiva.eir.compiler as eir_compiler
from semantiva.eir.runtime import build_scalar_specs_from_yaml


class _NoBypassSentinel(RuntimeError):
    pass


def _guard_build_canonical_spec(*args: Any, **kwargs: Any) -> Any:
    raise _NoBypassSentinel("FP1a:no-bypass:canonical-spec-required")


def test_no_bypass_yaml_string_frontend(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setattr(
        eir_compiler, "build_canonical_spec", _guard_build_canonical_spec
    )

    yaml_text = """
pipeline:
  nodes:
    - processor: DoesNotMatter
"""
    with pytest.raises(_NoBypassSentinel, match="canonical-spec-required"):
        compile_eir_v1(yaml_text)


def test_no_bypass_yaml_path_frontend(
    monkeypatch: pytest.MonkeyPatch, tmp_path: Path
) -> None:
    monkeypatch.setattr(
        eir_compiler, "build_canonical_spec", _guard_build_canonical_spec
    )

    # No extensions declared: avoid any plugin loading dependencies for this contract test.
    p = tmp_path / "pipeline.yaml"
    p.write_text(
        """
pipeline:
  nodes:
    - processor: DoesNotMatter
""",
        encoding="utf-8",
    )

    with pytest.raises(_NoBypassSentinel, match="canonical-spec-required"):
        compile_eir_v1(str(p))


def test_no_bypass_python_list_frontend(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setattr(
        eir_compiler, "build_canonical_spec", _guard_build_canonical_spec
    )

    nodes = [{"processor": "DoesNotMatter", "parameters": {}}]
    with pytest.raises(_NoBypassSentinel, match="canonical-spec-required"):
        compile_eir_v1(nodes)


def test_no_bypass_runtime_yaml_helper(
    monkeypatch: pytest.MonkeyPatch, tmp_path: Path
) -> None:
    """
    Runtime YAML wrapper is part of the user-facing surface: it MUST converge via
    CanonicalSpec as well (no frontend->EIR bypass hidden behind runtime helpers).
    """
    monkeypatch.setattr(
        eir_compiler, "build_canonical_spec", _guard_build_canonical_spec
    )

    p = tmp_path / "pipeline.yaml"
    p.write_text(
        """
pipeline:
  nodes:
    - processor: DoesNotMatter
""",
        encoding="utf-8",
    )

    with pytest.raises(_NoBypassSentinel, match="canonical-spec-required"):
        build_scalar_specs_from_yaml(str(p))
