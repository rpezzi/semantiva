from __future__ import annotations

from pathlib import Path

from semantiva.eir import compile_eir_v1

REPO_ROOT = Path(__file__).parents[2]
REF = REPO_ROOT / "tests" / "eir_reference_suite" / "float_ref_01.yaml"

# Golden anchor: once established, changes require explicit approval + ledger update.
EXPECTED_PREFIXES = ("plid-", "pvid-", "eirid-")


def test_golden_identity_prefixes_and_stability() -> None:
    eir = compile_eir_v1(str(REF))
    ident = eir["identity"]

    assert ident["pipeline_id"].startswith(EXPECTED_PREFIXES[0])
    assert ident["pipeline_variant_id"].startswith(EXPECTED_PREFIXES[1])
    assert ident["eir_id"].startswith(EXPECTED_PREFIXES[2])

    # Recompile and ensure identical IDs (strongest golden property for C0)
    eir2 = compile_eir_v1(str(REF))
    assert ident == eir2["identity"]
