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
from semantiva.pipeline.graph_builder import build_canonical_spec, compute_pipeline_id
from semantiva.registry import load_extensions, resolve_symbol
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
        return ("yaml_string", _sha256_text(pipeline_or_spec))
    if hasattr(pipeline_or_spec, "pipeline_configuration"):
        return ("pipeline_object", "pipeline_object")
    if isinstance(pipeline_or_spec, (list, tuple)):
        return ("node_list", _sha256_text(_stable_dumps(list(pipeline_or_spec))))
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
        return proc_spec, f"{proc_spec.__module__}.{proc_spec.__qualname__}"
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
        cand = processor_cls.input_data_type() if hasattr(processor_cls, "input_data_type") else None
        if isinstance(cand, type) and issubclass(cand, BaseDataType):
            in_t = cand
    except Exception:
        in_t = None

    try:
        cand = processor_cls.output_data_type() if hasattr(processor_cls, "output_data_type") else None
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


def _canonicalize_for_eir_id(eir_doc: Dict[str, Any]) -> Dict[str, Any]:
    """
    Canonical subset used for deterministic identity.

    MUST include structural meaning (graph/parameters/plan/semantics/lineage).
    MUST exclude ephemeral build/source metadata.
    MUST exclude identity.eir_id itself.
    """

    ident = eir_doc.get("identity", {}) or {}
    return {
        "eir_version": eir_doc.get("eir_version", 1),
        "identity": {
            "pipeline_id": ident.get("pipeline_id", ""),
            "pipeline_variant_id": ident.get("pipeline_variant_id", ""),
        },
        "graph": eir_doc.get("graph", {}),
        "parameters": eir_doc.get("parameters", {}),
        "plan": eir_doc.get("plan", {}),
        "semantics": eir_doc.get("semantics", {}),
        "lineage": eir_doc.get("lineage", {}),
    }


def compute_eir_id(eir_doc: Dict[str, Any]) -> str:
    canonical = _canonicalize_for_eir_id(eir_doc)
    return "eirid-" + _sha256_text(_stable_dumps(canonical))


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

    observed_modules: set[str] = {"classic_scalar", "compile_semantics_v1"}

    node_io: Dict[str, Dict[str, Any]] = {}
    node_slots: Dict[str, Dict[str, Any]] = {}

    for node_canon, node_resolved in zip(canonical_graph["nodes"], resolved_nodes):
        node_uuid = str(node_canon["node_uuid"])
        proc_spec = node_resolved.get("processor") or node_canon.get("processor_ref") or ""
        proc_cls, proc_ref = _resolve_processor(proc_spec)

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

        if input_form == "channel" or output_form == "channel":
            observed_modules.add("payload_channel")
        if input_form == "lane_bundle" or output_form == "lane_bundle":
            observed_modules.add("payload_lane_bundle")
        if inferred_slots.get("inputs") or inferred_slots.get("output") is not None:
            observed_modules.add("slot_inference_v1")

        node_io[node_uuid] = {
            "processor_ref": proc_ref,
            "input_type": _type_name(in_t),
            "output_type": _type_name(out_t),
            "input_form": input_form,
            "output_form": output_form,
        }
        node_slots[node_uuid] = {
            "declared_io": {"input_type": _type_name(in_t), "output_type": _type_name(out_t)},
            "inferred_slots": {"inputs": list(inferred_slots.get("inputs", [])), "output": inferred_slots.get("output")},
        }

    param_objects: Dict[str, Any] = {}
    node_order: List[str] = []
    for node_canon, node_resolved in zip(canonical_graph["nodes"], resolved_nodes):
        node_uuid = str(node_canon["node_uuid"])
        node_order.append(node_uuid)
        params = node_resolved.get("parameters", {}) or {}
        param_objects[f"params:{node_uuid}"] = descriptor_to_json(params)

    plan = {"plan_version": 1, "segments": [{"kind": "classic_linear", "node_order": node_order}]}
    graph = {
        "graph_version": int(canonical_graph.get("version", 1)),
        "nodes": canonical_graph.get("nodes", []),
        "edges": canonical_graph.get("edges", []),
    }

    modules = sorted(observed_modules)
    variant_payload = {"eir_version": 1, "pipeline_id": pipeline_id, "modules": modules}
    pipeline_variant_id = "pvid-" + _sha256_text(_stable_dumps(variant_payload))

    root_form = "scalar"
    terminal_form = "scalar"
    if node_order:
        first = node_io[node_order[0]]
        last = node_io[node_order[-1]]
        root_form = first["input_form"] if first.get("input_type") is not None else first["output_form"]
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
        "identity": {"pipeline_id": pipeline_id, "pipeline_variant_id": pipeline_variant_id, "eir_id": ""},
        "graph": graph,
        "parameters": {"objects": param_objects},
        "plan": plan,
        "semantics": semantics,
        "lineage": {},
    }

    eir["identity"]["eir_id"] = compute_eir_id(eir)
    return eir
