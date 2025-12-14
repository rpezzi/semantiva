from __future__ import annotations

from pathlib import Path

import yaml

from semantiva.eir import compile_eir_v1

REPO_ROOT = Path(__file__).parents[2]
SUITE = REPO_ROOT / "tests" / "eir_reference_suite"
LEDGER = REPO_ROOT / "docs" / "source" / "eir" / "eir_series_status.yaml"

REFS = [
    SUITE / "float_ref_01.yaml",
    SUITE / "float_ref_02.yaml",
    SUITE / "float_ref_channel_01.yaml",
    SUITE / "float_ref_slots_01.yaml",
    SUITE / "float_ref_lane_01.yaml",
]


def _expected() -> dict:
    doc = yaml.safe_load(LEDGER.read_text(encoding="utf-8"))
    return doc["eir_series"]["phase_2"]["eir_compiler_poc_forms_and_slots"]["checksums"]


def test_compiled_identities_match_ledger() -> None:
    exp = _expected()["compiled_identities"]
    for ref in REFS:
        eir = compile_eir_v1(str(ref))
        ident = eir["identity"]
        want = exp[ref.stem]
        assert ident["pipeline_id"] == want["pipeline_id"]
        assert ident["pipeline_variant_id"] == want["pipeline_variant_id"]
        assert ident["eir_id"] == want["eir_id"]
