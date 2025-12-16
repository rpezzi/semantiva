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

"""GraphV1: build and canonicalize pipeline graphs for tracing.

What this module does
    - Parses a pipeline spec (YAML path/string, list of node dicts, or Pipeline) into
        a canonical GraphV1 mapping: {"version": 1, "nodes": [...], "edges": [...]}.
    - Produces deterministic identifiers:
            - node_uuid: UUIDv5 derived from a canonical node mapping (see _canonical_node).
            - pipeline_id: "plid-<sha256>" of the canonical graph (see compute_pipeline_id).

Stability guarantees
    - Cosmetic changes (YAML whitespace, mapping key order) do not alter identities.
    - declaration_index/subindex are included in node canonical form to disambiguate
        otherwise-identical nodes declared in different positions.

Scope
    - Edges are emitted as a simple linear chain for demo/CLI pipelines; this preserves
        UUID semantics and can be extended later without breaking IDs.
"""

from __future__ import annotations

import json
import hashlib
import uuid
from pathlib import Path
from typing import Any, List, cast, Type

import yaml
from semantiva.pipeline.node_preprocess import preprocess_node_config
from semantiva.registry import resolve_parameters
from semantiva.registry.descriptors import descriptor_to_json
from semantiva.registry.resolve import resolve_symbol

# Namespace used for deterministic node UUID generation
# FP0b drift lock: DO NOT CHANGE without an explicit identity migration plan + approvals.
_NODE_NAMESPACE = uuid.UUID("00000000-0000-0000-0000-000000000000")


def _load_spec(pipeline_or_spec: Any) -> List[dict[str, Any]]:
    """Normalize input into a list of node specification dictionaries.

    Accepts:
      - Pipeline-like object with ``pipeline_configuration``
      - List/Tuple of node dicts
      - YAML path or YAML content (string). Supports top-level {pipeline: {nodes: [...]}}.

    Raises:
      TypeError: When input type is not supported.
    """
    if hasattr(pipeline_or_spec, "pipeline_configuration"):
        return list(pipeline_or_spec.pipeline_configuration)
    if isinstance(pipeline_or_spec, (list, tuple)):
        return list(pipeline_or_spec)
    if isinstance(pipeline_or_spec, str):
        path = Path(pipeline_or_spec)
        if path.exists():
            with path.open("r", encoding="utf-8") as f:
                loaded = yaml.safe_load(f)
        else:
            loaded = yaml.safe_load(pipeline_or_spec)
        if isinstance(loaded, dict) and "pipeline" in loaded:
            loaded = loaded["pipeline"].get("nodes", [])
        assert isinstance(loaded, list)
        return loaded
    raise TypeError(
        f"Unsupported pipeline specification type: {type(pipeline_or_spec)!r}"
    )


def _canonical_node(
    defn: dict[str, Any], declaration_index: int = 0, declaration_subindex: int = 0
) -> dict[str, Any]:
    """Return canonical node mapping used to derive node_uuid.

    Canonical fields:
        - role, processor_ref (dotted FQCN), params (shallow), ports (declared),
          derive payload, declaration_index, declaration_subindex

    FP0d identity rule (frontend parity):
      - Node config may specify either ``processor`` or ``processor_ref`` (mutually exclusive).
      - Canonical processor_ref is always the dotted FQCN of the resolved class
        (``<module>.<qualname>``) regardless of input form.
      - Derived meaning remains structured via ``derive``; generated derived class
        identity is not used for canonical identity.

    Note: declaration_index and declaration_subindex provide a stable positional
    discriminator so that nodes with identical configuration but different
    declaration positions receive distinct UUIDs.
    """

    role = defn.get("role") or "processor"
    processor = defn.get("processor")
    processor_ref_in = defn.get("processor_ref")

    if processor is not None and processor_ref_in is not None:
        raise ValueError(
            "Node config must not set both 'processor' and 'processor_ref'"
        )
    if processor is None and processor_ref_in is None:
        raise ValueError("Node config must set either 'processor' or 'processor_ref'")

    if processor is not None:
        if isinstance(processor, type):
            proc_cls = processor
        else:
            # processor is expected to be a string here (short-name or FQN)
            proc_cls = resolve_symbol(cast(str, processor))
    else:
        # processor_ref_in is expected to be a non-None string here
        assert processor_ref_in is not None
        proc_cls = resolve_symbol(cast(str, processor_ref_in))

    canonical_processor_ref = f"{proc_cls.__module__}.{proc_cls.__qualname__}"

    params = defn.get("parameters") or defn.get("params") or {}
    ports = defn.get("ports") or {}
    derive = defn.get("derive")
    preproc = None

    # Derived processor normalization: prefer base element_ref + structured derive payload.
    # Avoid eager metadata calls for non-derived processors. Only call
    # `_define_metadata` on classes that are semantiva components to satisfy mypy.
    try:
        from semantiva.core.semantiva_component import _SemantivaComponent

        is_semantiva_component = isinstance(proc_cls, type) and issubclass(
            cast(type, proc_cls), _SemantivaComponent
        )
    except Exception:
        # In case of import cycles or other import errors, conservatively avoid
        # calling into metadata.
        is_semantiva_component = False

    if is_semantiva_component and (
        hasattr(proc_cls, "_element") or hasattr(proc_cls, "_slice_element")
    ):
        try:
            # Narrow proc_cls for the type checker and call _define_metadata
            meta = cast(
                dict[str, Any],
                cast(Type[_SemantivaComponent], proc_cls)._define_metadata(),
            )
            preproc = meta.get("preprocessor") if isinstance(meta, dict) else None
        except Exception:  # pragma: no cover - defensive: metadata should not raise
            preproc = None

    if isinstance(preproc, dict):
        ptype = preproc.get("type")
        element_ref = preproc.get("element_ref")
        if isinstance(element_ref, str) and element_ref:
            if ptype in {"derive.parameter_sweep", "derive.slice"}:
                canonical_processor_ref = element_ref
                if derive is None:
                    if ptype == "derive.parameter_sweep":
                        derive = {"parameter_sweep": dict(preproc)}
                    elif ptype == "derive.slice":
                        derive = {"slice": dict(preproc)}
    canon = {
        "role": role,
        "processor_ref": canonical_processor_ref,
        "params": params,
        "ports": ports,
        "derive": derive,
        "declaration_index": declaration_index,
        "declaration_subindex": declaration_subindex,
    }
    return canon


def build_canonical_spec(
    pipeline_or_spec: Any,
) -> tuple[dict[str, Any], List[dict[str, Any]]]:
    """Return canonical GraphV1 spec and resolved node descriptors.

    This function normalizes the input pipeline specification, applies any
    configuration preprocessors, resolves parameters into descriptors (never
    instantiating runtime objects), and produces a JSON-serializable canonical
    graph with stable positional identities.

    Args:
        pipeline_or_spec: YAML path, mapping, or Pipeline-like object.

    Returns:
        tuple (canonical_spec, resolved_spec):
            canonical_spec: JSON-serializable GraphV1 mapping.
            resolved_spec:  List of node configs with descriptors for later
                            instantiation.

    FP0d: Node configs may specify either ``processor`` or ``processor_ref``
    (mutually exclusive). Canonical processor_ref is always the dotted FQCN of
    the resolved class; short-name ambiguity surfaces as an error during
    resolution.

    """
    spec = _load_spec(pipeline_or_spec)
    nodes: List[dict[str, Any]] = []
    resolved: List[dict[str, Any]] = []
    node_uuids: List[str] = []
    for declaration_index, raw in enumerate(spec):
        declaration_subindex = 0
        cfg = preprocess_node_config(dict(raw))
        if "processor" in cfg and "processor_ref" in cfg:
            if (
                cfg.get("processor") is not None
                and cfg.get("processor_ref") is not None
            ):
                raise ValueError(
                    "Node config must not define both 'processor' and 'processor_ref'"
                )
        params = resolve_parameters(cfg.get("parameters", {}))
        cfg["parameters"] = params
        resolved.append(cfg)
        canon = _canonical_node(cfg, declaration_index, declaration_subindex)
        canon["params"] = descriptor_to_json(params)
        node_json = json.dumps(canon, sort_keys=True, separators=(",", ":"))
        node_uuid = str(uuid.uuid5(_NODE_NAMESPACE, node_json))
        canon_with_uuid = dict(canon)
        canon_with_uuid["node_uuid"] = node_uuid
        nodes.append(canon_with_uuid)
        node_uuids.append(node_uuid)
    edges = [
        {"source": node_uuids[i], "target": node_uuids[i + 1]}
        for i in range(len(node_uuids) - 1)
    ]
    return ({"version": 1, "nodes": nodes, "edges": edges}, resolved)


def build_graph(pipeline_or_spec: Any) -> dict[str, Any]:
    """Build canonical graph specification from a pipeline or spec dictionary."""
    canonical, _ = build_canonical_spec(pipeline_or_spec)
    return canonical


def compute_pipeline_id(canonical_spec: dict[str, Any]) -> str:
    """Compute deterministic PipelineId for a GraphV1.

    Stable under cosmetic changes (whitespace, key order).
    Returns: "plid-" + sha256(canonical_spec JSON).
    """
    spec_json = json.dumps(canonical_spec, sort_keys=True, separators=(",", ":"))
    return "plid-" + hashlib.sha256(spec_json.encode("utf-8")).hexdigest()


def compute_upstream_map(canonical_spec: dict[str, Any]) -> dict[str, list[str]]:
    """Return mapping of node_uuid -> list of upstream node_uuids."""

    mapping: dict[str, list[str]] = {
        n["node_uuid"]: [] for n in canonical_spec.get("nodes", [])
    }
    for edge in canonical_spec.get("edges", []):
        mapping.setdefault(edge["target"], []).append(edge["source"])
    return mapping
