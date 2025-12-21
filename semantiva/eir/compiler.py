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

from __future__ import annotations

import hashlib
import importlib
import json
from dataclasses import dataclass
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Dict, List, Optional, Tuple

import yaml

from semantiva.data_types import BaseDataType, LaneBundleDataType, MultiChannelDataType
from semantiva.eir.slot_inference import infer_data_slots
from semantiva.pipeline.cpsv1.canonicalize import canonicalize_yaml_to_cpsv1
from semantiva.pipeline.cpsv1.derived_edges import derive_edges_v1
from semantiva.pipeline.cpsv1.identity import compute_pipeline_id_cpsv1
from semantiva.pipeline.graph_builder import build_canonical_spec, compute_pipeline_id
from semantiva.registry import load_extensions, resolve_symbol
from semantiva.registry.descriptors import descriptor_to_json


def _fqcn(proc_cls: type) -> str:
    return f"{proc_cls.__module__}.{proc_cls.__qualname__}"


def _stable_dumps(obj: Any) -> str:
    return json.dumps(obj, sort_keys=True, separators=(",", ":"))


def _lossy_jsonable(obj: object) -> object:
    """Return a deterministic, JSON-friendly representation without mem-ids."""

    if obj is None or isinstance(obj, (bool, int, float, str)):
        return obj
    if isinstance(obj, (list, tuple)):
        return [_lossy_jsonable(item) for item in obj]
    if isinstance(obj, dict):
        items: list[tuple[str, object]] = []
        for key, value in obj.items():
            ks = key if isinstance(key, str) else f"key:{type(key).__name__}"
            items.append((ks, _lossy_jsonable(value)))
        return {k: v for k, v in sorted(items, key=lambda pair: pair[0])}

    if isinstance(obj, type):
        return f"pyclass:{obj.__module__}.{obj.__qualname__}"

    qualname = getattr(obj, "__qualname__", None)
    modname = getattr(obj, "__module__", None)
    if callable(obj) and isinstance(qualname, str) and isinstance(modname, str):
        return f"callable:{modname}.{qualname}"

    cls = getattr(obj, "__class__", None)
    if isinstance(cls, type):
        return f"object:{cls.__module__}.{cls.__qualname__}"

    type_name = getattr(type(obj), "__name__", None) or "unknown"
    return f"unserializable:{type_name}"


def _sha256_text(s: str) -> str:
    return hashlib.sha256(s.encode("utf-8")).hexdigest()


def _fingerprint_source(pipeline_or_spec: Any) -> Tuple[str, str]:
    """
    Return (kind, fingerprint) for the input source.

    Fingerprint is best-effort and excluded from canonical pipeline identity hashing (to avoid drift).
    """
    if isinstance(pipeline_or_spec, str):
        p = Path(pipeline_or_spec)
        if p.exists():
            return ("yaml_path", hashlib.sha256(p.read_bytes()).hexdigest())
        return ("yaml_string", _sha256_text(pipeline_or_spec))
    if hasattr(pipeline_or_spec, "pipeline_configuration"):
        return ("pipeline_object", "pipeline_object")
    if isinstance(pipeline_or_spec, (list, tuple)):
        # Best-effort fingerprint for node lists: convert class refs to strings
        try:
            normalized = []
            for item in pipeline_or_spec:
                if isinstance(item, dict):
                    normalized_item = dict(item)
                    proc = normalized_item.get("processor")
                    if isinstance(proc, type):
                        normalized_item["processor"] = (
                            f"{proc.__module__}.{proc.__qualname__}"
                        )
                    normalized.append(normalized_item)
                else:
                    normalized.append(item)
            payload = _stable_dumps(normalized)
            return ("node_list", _sha256_text(payload))
        except Exception:
            lossy = _lossy_jsonable(normalized)
            payload = _stable_dumps(lossy)
            return ("node_list_lossy", _sha256_text(payload))
    return ("unknown", "unknown")


def _extract_extensions(doc: Any) -> List[str]:
    if not isinstance(doc, dict):
        return []
    top = doc.get("extensions")
    if top:
        if isinstance(top, (list, tuple)):
            return [str(x) for x in top]
        return [str(top)]
    pipe = doc.get("pipeline")
    if isinstance(pipe, dict):
        nested = pipe.get("extensions")
        if nested:
            if isinstance(nested, (list, tuple)):
                return [str(x) for x in nested]
            return [str(nested)]
    return []


def _maybe_load_extensions(pipeline_or_spec: Any) -> None:
    """
    Best-effort: when compiling from YAML, load declared extensions so processor
    symbols can be resolved deterministically (entry points supported).
    """
    if not isinstance(pipeline_or_spec, str):
        return
    p = Path(pipeline_or_spec)
    try:
        raw = p.read_text(encoding="utf-8") if p.exists() else pipeline_or_spec
        doc = yaml.safe_load(raw)
    except Exception:
        return

    exts = _extract_extensions(doc)
    if exts:
        load_extensions(exts)


def _load_authoring_doc(obj: Any) -> Any | None:
    """Best-effort loader for YAML-ish authoring specs."""

    if isinstance(obj, (dict, list)):
        return obj
    if isinstance(obj, str):
        try:
            p = Path(obj)
            if p.exists():
                return yaml.safe_load(p.read_text(encoding="utf-8"))
            return yaml.safe_load(obj)
        except Exception:
            return None
    if hasattr(obj, "pipeline_configuration"):
        try:
            return list(obj.pipeline_configuration)
        except Exception:
            return None
    return None


def _extract_nodes(doc: Any) -> list[dict[str, Any]]:
    if isinstance(doc, dict) and isinstance(doc.get("pipeline"), dict):
        nodes = doc["pipeline"].get("nodes") or []
        return (
            [n for n in nodes if isinstance(n, dict)] if isinstance(nodes, list) else []
        )
    if isinstance(doc, dict):
        nodes = doc.get("nodes") or []
        return (
            [n for n in nodes if isinstance(n, dict)] if isinstance(nodes, list) else []
        )
    if isinstance(doc, list):
        return [n for n in doc if isinstance(n, dict)]
    return []


def _uses_bind_or_data_key(nodes: list[dict[str, Any]]) -> bool:
    for n in nodes:
        if "bind" in n and n.get("bind") is not None:
            return True
        if "data_key" in n and n.get("data_key") is not None:
            return True
    return False


def _try_import_dotted(symbol: str) -> Optional[type]:
    """
    Safety net: support 'pkg.mod.Class' dotted import in addition to 'pkg.mod:Class'.
    Never raises; returns None on failure.
    """
    if ":" in symbol or "." not in symbol:
        return None
    mod_name, _, cls_name = symbol.rpartition(".")
    if not mod_name or not cls_name:
        return None
    try:
        mod = importlib.import_module(mod_name)
        cand = getattr(mod, cls_name, None)
        return cand if isinstance(cand, type) else None
    except Exception:
        return None


def _resolve_processor(proc_spec: Any) -> Tuple[Optional[type], str]:
    """
    Resolve a processor specification into a class (best-effort) + a stable ref string.
    Never raises.
    """
    if isinstance(proc_spec, type):
        return proc_spec, _fqcn(proc_spec)
    ref = str(proc_spec or "")
    if not ref:
        return None, ""
    try:
        cls = resolve_symbol(ref)
        return cls, ref
    except Exception:
        dotted = _try_import_dotted(ref)
        if dotted is not None:
            return dotted, ref
        return None, ref


def _resolve_processor_for_eir(proc_spec: Any) -> Tuple[type, str]:
    """Resolve processor specification and return (class, fqcn).

    Unlike `_resolve_processor`, this is strict and intended for EIR emission:
    EIR must not embed registry-relative identifiers.
    """

    if isinstance(proc_spec, type):
        return proc_spec, _fqcn(proc_spec)

    ref = str(proc_spec or "").strip()
    if not ref:
        raise ValueError("EIR compilation requires a non-empty processor reference")

    try:
        proc_cls = resolve_symbol(ref)
        return proc_cls, _fqcn(proc_cls)
    except Exception as exc:
        # Try dotted import as a backward-compat safety net.
        dotted = _try_import_dotted(ref)
        if dotted is not None:
            return dotted, _fqcn(dotted)

        # Make errors deterministic/actionable regardless of resolver exception type.
        from semantiva.registry.processor_registry import ProcessorRegistry

        cands = ProcessorRegistry.get_candidates(ref)
        # get_candidates may return 0, 1 or many candidate FQCNs. Only >1
        # indicates true ambiguity. If exactly one candidate exists but the
        # resolver still failed, surface a helpful resolution-failed message
        # (single candidate exists but resolution could not complete). If no
        # candidates are known, report unknown processor reference.
        if len(cands) > 1:
            raise ValueError(
                "Ambiguous processor reference "
                f"{ref!r}. Use a dotted fully-qualified class name (FQCN). Candidates: {cands}"
            ) from exc
        if len(cands) == 1:
            cand = list(cands)[0]
            raise ValueError(
                "Could not resolve processor reference "
                f"{ref!r}. A candidate exists ({cand}) but resolution failed; "
                "ensure extensions are loaded or use the dotted FQCN."
            ) from exc
        # No candidates recorded for this short name: unknown symbol.
        raise ValueError(
            "Unknown processor reference "
            f"{ref!r}. Ensure extensions are loaded and/or use a dotted FQCN."
        ) from exc


def _type_name(t: Optional[type]) -> Optional[str]:
    if t is None:
        return None
    try:
        return t.__name__
    except Exception:
        return None


def _declared_io_types(processor_cls: type) -> Tuple[Optional[type], Optional[type]]:
    """
    Return (input_type, output_type) as BaseDataType subclasses when available.
    Must never raise.
    """
    in_t: Optional[type] = None
    out_t: Optional[type] = None

    try:
        cand = (
            processor_cls.input_data_type()
            if hasattr(processor_cls, "input_data_type")
            else None
        )
        if isinstance(cand, type) and issubclass(cand, BaseDataType):
            in_t = cand
    except Exception:
        in_t = None

    try:
        cand = (
            processor_cls.output_data_type()
            if hasattr(processor_cls, "output_data_type")
            else None
        )
        if isinstance(cand, type) and issubclass(cand, BaseDataType):
            out_t = cand
    except Exception:
        out_t = None

    if in_t is not None and out_t is None:
        out_t = in_t

    return in_t, out_t


def _form_for_datatype(t: Optional[type]) -> str:
    """
    Map a BaseDataType class to a payload form enum.
    Conservative: unknown -> scalar.
    """
    if t is None:
        return "scalar"
    try:
        if issubclass(t, MultiChannelDataType):
            return "channel"
        if issubclass(t, LaneBundleDataType):
            return "lane_bundle"
    except Exception:
        return "scalar"
    return "scalar"


@dataclass(frozen=True)
class CompileResult:
    eir: Dict[str, Any]


def compile_eir_v1(pipeline_or_spec: Any) -> Dict[str, Any]:
    """
    Compile a classic Semantiva pipeline into an EIRv1 document.

    C0: GraphV1 + params + classic plan.
    C1: additionally emit compiled facts in semantics:
        - payload form propagation: scalar|channel|lane_bundle
        - metadata-only inferred slot candidates (from _process_logic annotations)

    No runtime execution semantics change.
    """
    _maybe_load_extensions(pipeline_or_spec)

    canonical_graph, resolved_nodes = build_canonical_spec(pipeline_or_spec)
    pipeline_id = compute_pipeline_id(canonical_graph)

    node_io: Dict[str, Dict[str, Any]] = {}
    node_slots: Dict[str, Dict[str, Any]] = {}

    for node_canon, node_resolved in zip(canonical_graph["nodes"], resolved_nodes):
        node_uuid = str(node_canon["node_uuid"])
        proc_spec = (
            node_resolved.get("processor") or node_canon.get("processor_ref") or ""
        )
        proc_cls, proc_ref = _resolve_processor_for_eir(proc_spec)

        in_t: Optional[type] = None
        out_t: Optional[type] = None
        inferred_slots: Dict[str, Any] = {"inputs": [], "output": None}

        if proc_cls is not None:
            in_t, out_t = _declared_io_types(proc_cls)
            try:
                inferred_slots = infer_data_slots(proc_cls).to_dict()
            except Exception:
                inferred_slots = {"inputs": [], "output": None}

        input_form = _form_for_datatype(in_t)
        output_form = _form_for_datatype(out_t)

        node_io[node_uuid] = {
            "processor_ref": proc_ref,
            "input_type": _type_name(in_t),
            "output_type": _type_name(out_t),
            "input_form": input_form,
            "output_form": output_form,
        }
        node_slots[node_uuid] = {
            "declared_io": {
                "input_type": _type_name(in_t),
                "output_type": _type_name(out_t),
            },
            "inferred_slots": {
                "inputs": list(inferred_slots.get("inputs", [])),
                "output": inferred_slots.get("output"),
            },
        }

    param_objects: Dict[str, Any] = {}
    node_order: List[str] = []
    for node_canon, node_resolved in zip(canonical_graph["nodes"], resolved_nodes):
        node_uuid = str(node_canon["node_uuid"])
        node_order.append(node_uuid)
        params = node_resolved.get("parameters", {}) or {}
        param_objects[f"params:{node_uuid}"] = descriptor_to_json(params)

    plan = {
        "plan_version": 1,
        "segments": [{"kind": "classic_linear", "node_order": node_order}],
    }
    graph = {
        "graph_version": int(canonical_graph.get("version", 1)),
        "nodes": canonical_graph.get("nodes", []),
        "edges": canonical_graph.get("edges", []),
    }

    root_form = "scalar"
    terminal_form = "scalar"
    if node_order:
        first = node_io[node_order[0]]
        last = node_io[node_order[-1]]
        root_form = (
            first["input_form"]
            if first.get("input_type") is not None
            else first["output_form"]
        )
        terminal_form = last["output_form"]

    semantics: Dict[str, Any] = {
        "payload_forms": {
            "version": 1,
            "root_form": root_form,
            "terminal_form": terminal_form,
            "node_io": node_io,
        },
        "slots": {"version": 1, "node_slots": node_slots},
    }

    source_kind, source_fp = _fingerprint_source(pipeline_or_spec)

    eir: Dict[str, Any] = {
        "eir_version": 1,
        "build": {
            "semantiva_version": "0.x",
            "compiler_version": "eir-compiler-1",
            "created_at": datetime.now(timezone.utc).isoformat(),
        },
        "source": {"kind": source_kind, "pipeline_spec_fingerprint": source_fp},
        "identity": {"pipeline_id": pipeline_id},
        "graph": graph,
        "parameters": {"objects": param_objects},
        "plan": plan,
        "semantics": semantics,
        "lineage": {},
    }

    runtime_extensions: list[str] = []
    if isinstance(pipeline_or_spec, str):
        try:
            raw = (
                Path(pipeline_or_spec).read_text(encoding="utf-8")
                if Path(pipeline_or_spec).exists()
                else pipeline_or_spec
            )
            doc = yaml.safe_load(raw)
            runtime_extensions = _extract_extensions(doc)
        except Exception:
            runtime_extensions = []

    node_runtime: dict[str, dict[str, Any]] = {}
    for node_canon, node_resolved in zip(canonical_graph["nodes"], resolved_nodes):
        node_uuid = str(node_canon["node_uuid"])
        ck = node_resolved.get("context_key")
        if isinstance(ck, str) and ck.strip():
            node_runtime.setdefault(node_uuid, {})["context_key"] = ck

    eir["source"]["extensions"] = runtime_extensions
    if node_runtime:
        eir["source"]["node_runtime"] = node_runtime

    # Embed CPSV1 + derived edges, and enforce CPSV1-based pipeline_id for bind/data_key specs.
    _authoring_doc = _load_authoring_doc(pipeline_or_spec)
    _authoring_nodes = _extract_nodes(_authoring_doc)

    cpsv1 = None
    if _authoring_doc is not None:
        try:
            cpsv1 = canonicalize_yaml_to_cpsv1(_authoring_doc)
        except Exception:
            cpsv1 = None

    if _uses_bind_or_data_key(_authoring_nodes):
        if cpsv1 is None:
            raise ValueError(
                "Bind/data_key authoring spec requires CPSV1 canonicalization, but canonicalization failed."
            )
        pipeline_id = compute_pipeline_id_cpsv1(cpsv1)
        eir["identity"]["pipeline_id"] = pipeline_id

    if cpsv1 is not None:
        eir["canonical_pipeline_spec"] = cpsv1
        eir.setdefault("derived", {})
        eir["derived"] = {
            "edges": derive_edges_v1(cpsv1),
            "plan": [],
            "diagnostics": [],
        }
    return eir
