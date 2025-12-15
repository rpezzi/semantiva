#!/usr/bin/env python3
"""Print computed pipeline IDs for float reference-suite YAMLs.

This helper script recomputes pipeline_id values after FP0d FQCN canonicalization
to enable updating the ledger (docs/source/eir/eir_series_status.yaml).
"""
from __future__ import annotations

from pathlib import Path
from semantiva.pipeline.graph_builder import build_canonical_spec, compute_pipeline_id

REPO_ROOT = Path(__file__).resolve().parents[2]
SUITE = REPO_ROOT / "tests" / "eir_reference_suite"

# Keep list in sync with docs/source/eir/eir_series_status.yaml reference_suite.float keys
REFS = [
    "float_ref_01",
    "float_ref_02",
    "float_ref_channel_01",
    "float_ref_slots_01",
    "float_ref_lane_01",
]

if __name__ == "__main__":
    for ref_id in REFS:
        path = SUITE / f"{ref_id}.yaml"
        if not path.exists():
            print(f"SKIP {ref_id:25s} (file not found)")
            continue
        canon, _ = build_canonical_spec(str(path))
        pid = compute_pipeline_id(canon)
        print(f"{ref_id:25s} {pid}")
