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
"""Canonicalize YAML/Python pipeline specs into CanonicalPipelineSpecV1."""

from __future__ import annotations

import uuid
from copy import deepcopy
from typing import Any, Iterable, Mapping

from semantiva.exceptions.pipeline_exceptions import PipelineConfigurationError
from semantiva.pipeline.cpsv1.stable_json import stable_json_dumps_v1
from semantiva.registry.resolve import resolve_symbol

# Namespace used for deterministic node UUID generation (CPSV1-specific)
_NODE_NAMESPACE = uuid.UUID("00000000-0000-0000-0000-000000000000")


def _normalize_source_ref(source: str) -> str:
    if not isinstance(source, str) or not source:
        raise PipelineConfigurationError(
            "bind source references must be non-empty strings"
        )

    if ":" in source:
        prefix, value = source.split(":", 1)
        if prefix in {"channel", "context"} and value:
            return f"{prefix}:{value}"
        raise PipelineConfigurationError(
            f"Unsupported SourceRef '{source}'. Expected channel:<name> or context:<key>."
        )

    # Default: treat unprefixed names as channels
    return f"channel:{source}"


def _normalize_bind(bind: Any) -> dict[str, str]:
    if bind is None:
        bind = {}
    if not isinstance(bind, Mapping):
        raise PipelineConfigurationError("bind must be a mapping if provided")

    normalized: dict[str, str] = {}
    for param, source in bind.items():
        normalized[str(param)] = _normalize_source_ref(source)

    if "data" not in normalized:
        normalized["data"] = "channel:primary"
    return normalized


def _normalize_publish(node: Mapping[str, Any]) -> dict[str, Any]:
    publish = node.get("publish")
    if publish is None:
        publish = {}
    if not isinstance(publish, Mapping):
        raise PipelineConfigurationError("publish must be a mapping if provided")

    channels_in = publish.get("channels") or {}
    if not isinstance(channels_in, Mapping):
        raise PipelineConfigurationError("publish.channels must be a mapping")

    channels = {str(slot): str(name) for slot, name in channels_in.items()}

    data_key = node.get("data_key")
    if data_key is not None:
        if not isinstance(data_key, str) or not data_key:
            raise PipelineConfigurationError("data_key must be a non-empty string")
        channels["out"] = data_key

    if "out" not in channels:
        channels["out"] = "primary"

    context_key = node.get("context_key")
    if context_key is None:
        context_key = publish.get("context_key")

    if context_key is not None and (
        not isinstance(context_key, str) or not context_key
    ):
        raise PipelineConfigurationError(
            "context_key must be a non-empty string when set"
        )

    return {"channels": channels, "context_key": context_key}


def _resolve_processor_ref(node: Mapping[str, Any]) -> str:
    processor = node.get("processor")
    processor_ref = node.get("processor_ref")

    if processor is not None and processor_ref is not None:
        raise PipelineConfigurationError(
            "Node config must not set both 'processor' and 'processor_ref'"
        )
    if processor is None and processor_ref is None:
        raise PipelineConfigurationError(
            "Node config must set either 'processor' or 'processor_ref'"
        )

    symbol = processor if processor is not None else processor_ref
    assert symbol is not None
    if isinstance(symbol, str):
        proc_cls = resolve_symbol(symbol)
    elif isinstance(symbol, type):
        proc_cls = symbol
    else:
        raise PipelineConfigurationError(
            "processor must be a string or class; processor_ref must be a string"
        )

    return f"{proc_cls.__module__}.{proc_cls.__qualname__}"


def _canonicalize_node(
    node: Mapping[str, Any], declaration_index: int
) -> dict[str, Any]:
    role = node.get("role", "processor")
    processor_ref = _resolve_processor_ref(node)
    parameters = deepcopy(node.get("parameters") or node.get("params") or {})
    if not isinstance(parameters, Mapping):
        raise PipelineConfigurationError(
            "parameters/params must be a mapping if provided"
        )

    ports = deepcopy(node.get("ports") or {})
    if not isinstance(ports, Mapping):
        raise PipelineConfigurationError("ports must be a mapping if provided")

    derive = deepcopy(node.get("derive"))
    declaration_subindex = int(node.get("declaration_subindex", 0))

    bind = _normalize_bind(node.get("bind"))
    publish = _normalize_publish(node)

    basis = {
        "role": role,
        "processor_ref": processor_ref,
        "parameters": parameters,
        "ports": ports,
        "derive": derive,
        "bind": bind,
        "publish": publish,
        "declaration_index": declaration_index,
        "declaration_subindex": declaration_subindex,
    }

    node_uuid = str(uuid.uuid5(_NODE_NAMESPACE, stable_json_dumps_v1(basis)))

    canonical_node = dict(basis)
    canonical_node["node_uuid"] = node_uuid
    return canonical_node


def canonicalize_nodes_to_cpsv1(
    nodes: Iterable[Mapping[str, Any]], *, _extensions: Iterable[str] | None = None
) -> dict[str, Any]:
    canonical_nodes = []
    for idx, node in enumerate(nodes):
        if not isinstance(node, Mapping):
            raise PipelineConfigurationError("each node must be a mapping")
        canonical_nodes.append(_canonicalize_node(node, idx))

    return {"version": 1, "nodes": canonical_nodes}


def canonicalize_yaml_to_cpsv1(pipeline_spec: Any) -> dict[str, Any]:
    if isinstance(pipeline_spec, Mapping) and "pipeline" in pipeline_spec:
        pipeline = pipeline_spec.get("pipeline") or {}
        if not isinstance(pipeline, Mapping):
            raise PipelineConfigurationError("pipeline must be a mapping")
        nodes = pipeline.get("nodes", [])
        extensions = pipeline_spec.get("extensions")
    elif isinstance(pipeline_spec, Mapping) and "nodes" in pipeline_spec:
        nodes = pipeline_spec.get("nodes", [])
        extensions = pipeline_spec.get("extensions")
    elif isinstance(pipeline_spec, Iterable) and not isinstance(
        pipeline_spec, (str, bytes)
    ):
        nodes = pipeline_spec
        extensions = None
    else:
        raise PipelineConfigurationError(
            "Unsupported pipeline specification shape; expected mapping with 'pipeline'/'nodes' or an iterable of nodes."
        )

    if not isinstance(nodes, Iterable) or isinstance(nodes, (str, bytes)):
        raise PipelineConfigurationError("nodes must be an iterable of node mappings")

    return canonicalize_nodes_to_cpsv1(nodes, _extensions=extensions)
