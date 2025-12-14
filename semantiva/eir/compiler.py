from __future__ import annotations

import hashlib
import json
from dataclasses import dataclass
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Dict, Tuple

from semantiva.pipeline.graph_builder import build_canonical_spec, compute_pipeline_id
from semantiva.registry.descriptors import descriptor_to_json


def _stable_dumps(obj: Any) -> str:
    return json.dumps(obj, sort_keys=True, separators=(",", ":"))


def _sha256_text(s: str) -> str:
    return hashlib.sha256(s.encode("utf-8")).hexdigest()


def _fingerprint_source(pipeline_or_spec: Any) -> Tuple[str, str]:
    """
    Return (kind, fingerprint) for the input source.

    Fingerprint is best-effort and NOT used in eir_id hashing (to avoid drift).
    """
    if isinstance(pipeline_or_spec, str):
        p = Path(pipeline_or_spec)
        if p.exists():
            return ("yaml_path", hashlib.sha256(p.read_bytes()).hexdigest())
        # YAML content string
        return ("yaml_string", _sha256_text(pipeline_or_spec))
    if hasattr(pipeline_or_spec, "pipeline_configuration"):
        return ("pipeline_object", "pipeline_object")
    if isinstance(pipeline_or_spec, (list, tuple)):
        return ("node_list", _sha256_text(_stable_dumps(list(pipeline_or_spec))))
    return ("unknown", "unknown")


@dataclass(frozen=True)
class CompileResult:
    eir: Dict[str, Any]


def compile_eir_v1(pipeline_or_spec: Any) -> Dict[str, Any]:
    """
    Compile a classic Semantiva pipeline into an EIRv1 document (Phase 2, C0).

    - Classic scalar pipelines only (GraphV1).
    - Deterministic `pipeline_id` (delegates to GraphV1).
    - Deterministic `eir_id` that EXCLUDES ephemeral build/source metadata.
    """
    canonical_graph, resolved_nodes = build_canonical_spec(pipeline_or_spec)
    pipeline_id = compute_pipeline_id(canonical_graph)

    # Classic-only variant id (captures enabled semantics modules without runtime values)
    variant_payload = {"eir_version": 1, "pipeline_id": pipeline_id, "modules": ["classic_scalar"]}
    pipeline_variant_id = "pvid-" + _sha256_text(_stable_dumps(variant_payload))

    # Parameters: store as separate objects keyed by node_uuid
    param_objects: Dict[str, Any] = {}
    node_order = []
    for node_canon, node_resolved in zip(canonical_graph["nodes"], resolved_nodes):
        node_uuid = str(node_canon["node_uuid"])
        node_order.append(node_uuid)
        params = node_resolved.get("parameters", {}) or {}
        param_objects[f"params:{node_uuid}"] = descriptor_to_json(params)

    # Minimal plan: single classic linear segment with deterministic node order
    plan = {"plan_version": 1, "segments": [{"kind": "classic_linear", "node_order": node_order}]}

    # Graph section: embed GraphV1 canonical form under a stable envelope
    graph = {
        "graph_version": int(canonical_graph.get("version", 1)),
        "nodes": canonical_graph.get("nodes", []),
        "edges": canonical_graph.get("edges", []),
    }

    # Compute eir_id from canonical subset (exclude build/source and identity.eir_id itself)
    canonical_for_hash = {
        "eir_version": 1,
        "identity": {"pipeline_id": pipeline_id, "pipeline_variant_id": pipeline_variant_id},
        "graph": graph,
        "parameters": {"objects": param_objects},
        "plan": plan,
    }
    eir_id = "eirid-" + _sha256_text(_stable_dumps(canonical_for_hash))

    source_kind, source_fp = _fingerprint_source(pipeline_or_spec)

    # Full EIR doc includes build/source but eir_id ignores them (by design)
    eir = {
        "eir_version": 1,
        "build": {
            "semantiva_version": "0.x",  # factual placeholder; runtime may overwrite later if desired
            "compiler_version": "eir-compiler-1",
            "created_at": datetime.now(timezone.utc).isoformat(),
        },
        "source": {"kind": source_kind, "pipeline_spec_fingerprint": source_fp},
        "identity": {"pipeline_id": pipeline_id, "pipeline_variant_id": pipeline_variant_id, "eir_id": eir_id},
        "graph": graph,
        "parameters": {"objects": param_objects},
        "plan": plan,
        "semantics": {},  # reserved for C1+
        "lineage": {},    # reserved for R1+
    }
    return eir
