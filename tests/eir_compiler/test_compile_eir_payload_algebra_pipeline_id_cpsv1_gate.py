from __future__ import annotations

from pathlib import Path
from typing import Any

import pytest
import yaml

from semantiva.eir import compile_eir_v1
from semantiva.pipeline.cpsv1.identity import compute_pipeline_id_cpsv1

REPO_ROOT = Path(__file__).parents[2]
SUITE = REPO_ROOT / "tests" / "payload_algebra_reference_suite"
REFS = sorted(SUITE.glob("*.yaml"))


def _uses_bind_or_data_key(yaml_path: Path) -> bool:
    doc = yaml.safe_load(yaml_path.read_text(encoding="utf-8"))
    nodes: list[Any] = []
    if isinstance(doc, dict) and isinstance(doc.get("pipeline"), dict):
        nodes = doc["pipeline"].get("nodes", []) or []
    for n in nodes:
        if isinstance(n, dict) and (
            n.get("bind") is not None or n.get("data_key") is not None
        ):
            if "bind" in n or "data_key" in n:
                return True
    return False


@pytest.mark.parametrize("ref", REFS, ids=[p.stem for p in REFS])
def test_pipeline_id_matches_cpsv1_for_bind_or_data_key(ref: Path) -> None:
    eir = compile_eir_v1(str(ref))
    cpsv1 = eir.get("canonical_pipeline_spec")

    if not _uses_bind_or_data_key(ref):
        return

    assert isinstance(cpsv1, dict)
    assert eir["identity"]["pipeline_id"] == compute_pipeline_id_cpsv1(cpsv1)
