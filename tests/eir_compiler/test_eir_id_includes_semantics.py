from __future__ import annotations

from pathlib import Path

from semantiva.eir import compile_eir_v1
from semantiva.eir.compiler import compute_eir_id

REPO_ROOT = Path(__file__).parents[2]
REF = REPO_ROOT / "tests" / "eir_reference_suite" / "float_ref_01.yaml"


def test_eir_id_changes_when_semantics_changes() -> None:
    eir = compile_eir_v1(str(REF))
    base = eir["identity"]["eir_id"]

    mutated = dict(eir)
    mutated["semantics"] = {"payload_forms": {"version": 1, "root_form": "scalar", "terminal_form": "scalar", "node_io": {}}}

    assert compute_eir_id(mutated) != base
