"""Drift guard test: reference-suite pipeline IDs must match ledger.

This test prevents future pipeline_id/ledger divergence for the float
reference suite by recomputing canonical pipeline IDs and asserting they
match the snapshot values in docs/source/eir/eir_series_status.yaml.
"""

from __future__ import annotations

from pathlib import Path

import yaml

from semantiva.pipeline.graph_builder import build_canonical_spec, compute_pipeline_id

REPO_ROOT = Path(__file__).resolve().parents[2]
LEDGER_PATH = REPO_ROOT / "docs" / "source" / "eir" / "eir_series_status.yaml"
SUITE_DIR = REPO_ROOT / "tests" / "eir_reference_suite"

REFS = [
    "float_ref_01",
    "float_ref_02",
    "float_ref_channel_01",
    "float_ref_slots_01",
    "float_ref_lane_01",
]


def _load_ledger() -> dict:
    with LEDGER_PATH.open("r", encoding="utf-8") as f:
        return yaml.safe_load(f)


def _ledger_pipeline_id(ledger: dict, ref_id: str) -> str:
    # Minimal, defensive lookup to avoid binding to unrelated ledger structure.
    # Expected path: eir_series -> reference_suite -> float -> <ref_id> -> pipeline_id
    try:
        float_refs = ledger["eir_series"]["reference_suite"]["float"]
        for entry in float_refs:
            if entry["id"] == ref_id:
                return entry["pipeline_id"]
        raise KeyError(f"No entry for {ref_id} in reference_suite.float")
    except Exception as e:
        raise AssertionError(
            f"Missing ledger pipeline_id for reference_suite.float.{ref_id}"
        ) from e


def test_reference_suite_float_pipeline_ids_match_ledger() -> None:
    ledger = _load_ledger()

    for ref_id in REFS:
        yaml_path = SUITE_DIR / f"{ref_id}.yaml"
        if not yaml_path.exists():
            # Keep test tolerant if some refs are absent in a given repo snapshot.
            continue

        canon, _ = build_canonical_spec(str(yaml_path))
        computed = compute_pipeline_id(canon)
        expected = _ledger_pipeline_id(ledger, ref_id)

        assert (
            computed == expected
        ), f"{ref_id}: computed pipeline_id {computed} != ledger {expected}"
