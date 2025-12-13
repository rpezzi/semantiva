#!/usr/bin/env python3
# Copyright 2025 Semantiva authors
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#     http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

"""Update the EIRv1 series ledger with deterministic reference-suite hashes.

Usage:
  python scripts/eir/update_series_status.py --check
  python scripts/eir/update_series_status.py --write

This recomputes:
- yaml_sha256: sha256 of the raw YAML bytes
- pipeline_id: plid-... from GraphV1 canonicalization
"""

from __future__ import annotations

import argparse
import hashlib
import sys
from pathlib import Path
from typing import Any, Dict, List

import yaml

REPO_ROOT = Path(__file__).resolve().parents[2]
LEDGER_PATH = REPO_ROOT / "docs" / "source" / "eir" / "eir_series_status.yaml"

# Ensure repo root is importable when executed from other working directories
if str(REPO_ROOT) not in sys.path:
    sys.path.insert(0, str(REPO_ROOT))

from semantiva.pipeline.graph_builder import build_canonical_spec, compute_pipeline_id  # noqa: E402


def sha256_file(path: Path) -> str:
    return hashlib.sha256(path.read_bytes()).hexdigest()


def load_ledger(path: Path) -> Dict[str, Any]:
    data = yaml.safe_load(path.read_text(encoding="utf-8"))
    if not isinstance(data, dict) or "eir_series" not in data:
        raise ValueError(f"Invalid ledger format: missing 'eir_series' at {path}")
    return data


def update_float_suite(data: Dict[str, Any]) -> List[str]:
    updated: List[str] = []
    suite = data["eir_series"]["reference_suite"]["float"]
    if not isinstance(suite, list):
        raise ValueError("ledger eir_series.reference_suite.float must be a list")

    for entry in suite:
        if not isinstance(entry, dict) or "yaml_path" not in entry or "id" not in entry:
            raise ValueError("each float reference entry must be a mapping with 'id' and 'yaml_path'")
        yaml_path = REPO_ROOT / str(entry["yaml_path"])
        if not yaml_path.exists():
            raise FileNotFoundError(f"reference yaml_path not found: {yaml_path}")

        entry["yaml_sha256"] = sha256_file(yaml_path)
        canonical, _ = build_canonical_spec(str(yaml_path))
        entry["pipeline_id"] = compute_pipeline_id(canonical)
        updated.append(str(entry["id"]))

    return updated


def main() -> int:
    ap = argparse.ArgumentParser()
    ap.add_argument("--check", action="store_true", help="fail if ledger differs from computed values")
    ap.add_argument("--write", action="store_true", help="rewrite ledger with computed values")
    args = ap.parse_args()

    if not LEDGER_PATH.exists():
        raise FileNotFoundError(f"ledger not found at {LEDGER_PATH}")

    original = LEDGER_PATH.read_text(encoding="utf-8")
    data = load_ledger(LEDGER_PATH)

    updated_ids = update_float_suite(data)

    rendered = yaml.safe_dump(data, sort_keys=False)

    if args.check:
        if rendered != original:
            print("Ledger differs from computed values. Re-run with --write and commit the result.")
            return 1
        print(f"OK: ledger matches computed values for: {', '.join(updated_ids)}")
        return 0

    if args.write:
        LEDGER_PATH.write_text(rendered, encoding="utf-8")
        print(f"Wrote updated ledger for: {', '.join(updated_ids)}")
        return 0

    print("No action taken (pass --check or --write).")
    return 2


if __name__ == "__main__":
    raise SystemExit(main())
