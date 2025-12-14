from __future__ import annotations

from pathlib import Path

import yaml

from semantiva.eir import compile_eir_v1

REPO_ROOT = Path(__file__).parents[2]
REF = REPO_ROOT / "tests" / "eir_reference_suite" / "float_ref_01.yaml"
LEDGER = REPO_ROOT / "docs" / "source" / "eir" / "eir_series_status.yaml"


def _ledger_expected() -> dict:
    doc = yaml.safe_load(LEDGER.read_text(encoding="utf-8"))
    c0p = doc["eir_series"]["phase_2"]["eir_scaffold_and_classic_compiler"]["checksums"]
    return c0p["float_ref_01_compiled_identity"]


def test_compiled_identity_matches_ledger_float_ref_01() -> None:
    expected = _ledger_expected()
    eir = compile_eir_v1(str(REF))
    ident = eir["identity"]

    assert ident["pipeline_id"] == expected["pipeline_id"]
    assert ident["pipeline_variant_id"] == expected["pipeline_variant_id"]
    assert ident["eir_id"] == expected["eir_id"]
