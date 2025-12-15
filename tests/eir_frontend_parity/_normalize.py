from __future__ import annotations

import json
from typing import Any

_PROVENANCE_KEYS = {
    "source",
    "provenance",
    "location",
    "extensions",
    "source_path",
    "base_dir",
    "created_at",
    "build",
    "compiler_version",
    "semantiva_version",
}


def _strip_provenance(obj: Any) -> Any:
    if isinstance(obj, dict):
        out: dict[str, Any] = {}
        for k, v in obj.items():
            if str(k) in _PROVENANCE_KEYS:
                continue
            out[str(k)] = _strip_provenance(v)
        return out
    if isinstance(obj, list):
        return [_strip_provenance(v) for v in obj]
    return obj


def normalize_canonical_spec(canon: dict[str, Any]) -> dict[str, Any]:
    copied = json.loads(json.dumps(canon, sort_keys=True))
    return _strip_provenance(copied)


def normalize_eir_doc(eir: dict[str, Any]) -> dict[str, Any]:
    """
    Normalize EIR documents for semantic parity comparisons.

    FP0c rule:
      - Exclude frontend/build provenance (eir.source.*, build timestamps/version strings).
      - Preserve semantic meaning: identity (including eir_id), graph, parameters, plan, semantics, lineage.
    """
    copied = json.loads(json.dumps(eir, sort_keys=True))
    return _strip_provenance(copied)


def canonical_bytes(obj: Any) -> bytes:
    return json.dumps(
        obj, sort_keys=True, separators=(",", ":"), ensure_ascii=False
    ).encode("utf-8")
