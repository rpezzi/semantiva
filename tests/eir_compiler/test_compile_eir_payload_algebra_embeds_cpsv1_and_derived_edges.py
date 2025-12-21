from __future__ import annotations

from pathlib import Path

import pytest

from semantiva.eir import compile_eir_v1
from semantiva.pipeline.cpsv1.derived_edges import derive_edges_v1

REPO_ROOT = Path(__file__).parents[2]
SUITE = REPO_ROOT / "tests" / "payload_algebra_reference_suite"
REFS = sorted(SUITE.glob("*.yaml"))


@pytest.mark.parametrize("ref", REFS, ids=[p.stem for p in REFS])
def test_compiler_embeds_cpsv1_and_derived_edges(ref: Path) -> None:
    eir = compile_eir_v1(str(ref))

    assert "canonical_pipeline_spec" in eir
    cpsv1 = eir["canonical_pipeline_spec"]
    assert isinstance(cpsv1, dict)
    assert cpsv1.get("version") == 1
    assert isinstance(cpsv1.get("nodes"), list) and len(cpsv1["nodes"]) >= 1

    assert "derived" in eir
    derived = eir["derived"]
    assert isinstance(derived, dict)
    assert "edges" in derived and isinstance(derived["edges"], list)
    assert "plan" in derived and isinstance(derived["plan"], list)
    assert "diagnostics" in derived and isinstance(derived["diagnostics"], list)

    assert derived["edges"] == derive_edges_v1(cpsv1)
