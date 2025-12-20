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
"""Deterministic derived-edge computation for CanonicalPipelineSpecV1."""

from __future__ import annotations

from typing import Any, Dict, List, Mapping, Optional, Tuple

from semantiva.exceptions.pipeline_exceptions import (
    PipelineConfigurationError,
    PipelineTopologyError,
)

DerivedEdgeV1 = Dict[str, str]
ProducerInfo = Tuple[str, str, int]  # (node_uuid, output_slot, declaration_index)


def derive_edges_v1(cpsv1: Mapping[str, Any]) -> List[DerivedEdgeV1]:
    """
    Derive non-normative dependency edges from CanonicalPipelineSpecV1.

    This function is:
      - pure
      - deterministic
      - runtime-independent
      - excluded from identity semantics
    """

    nodes = cpsv1.get("nodes")
    if not isinstance(nodes, list):
        raise PipelineConfigurationError("CPSV1 must contain a 'nodes' list.")

    edges: List[DerivedEdgeV1] = []
    channel_producer: Dict[str, ProducerInfo] = {}

    prev_node_uuid: Optional[str] = None

    for idx, node in enumerate(nodes):
        node_uuid = node.get("node_uuid")
        if not isinstance(node_uuid, str):
            raise PipelineConfigurationError(f"Node at index {idx} missing node_uuid.")

        publish = node.get("publish", {})
        channels = publish.get("channels", {})
        if not isinstance(channels, dict):
            raise PipelineConfigurationError(
                f"publish.channels must be an object (node {node_uuid})."
            )

        for out_slot, channel_name in sorted(channels.items()):
            if channel_name == "primary":
                continue

            if channel_name in channel_producer:
                prev_uuid, prev_slot, prev_idx = channel_producer[channel_name]
                raise PipelineTopologyError(
                    f"Channel '{channel_name}' has multiple writers: "
                    f"{prev_uuid} (idx={prev_idx}) and {node_uuid} (idx={idx})."
                )

            channel_producer[channel_name] = (node_uuid, str(out_slot), idx)

        bind = node.get("bind", {})
        if not isinstance(bind, dict):
            raise PipelineConfigurationError(
                f"bind must be an object (node {node_uuid})."
            )

        for param in sorted(bind.keys()):
            source_ref = bind[param]

            if not isinstance(source_ref, str):
                raise PipelineConfigurationError(
                    f"SourceRef for param '{param}' must be a string (node {node_uuid})."
                )

            if source_ref.startswith("context:"):
                continue

            if source_ref == "channel:primary":
                if prev_node_uuid is None:
                    continue

                edges.append(
                    {
                        "source_node_uuid": prev_node_uuid,
                        "target_node_uuid": node_uuid,
                        "target_param": param,
                        "source_ref": source_ref,
                    }
                )
                continue

            if source_ref.startswith("channel:"):
                channel = source_ref.split(":", 1)[1]
                producer = channel_producer.get(channel)
                if producer is None:
                    raise PipelineTopologyError(
                        f"Missing producer for channel '{channel}' "
                        f"(node {node_uuid}, param {param})."
                    )

                prod_uuid, prod_slot, prod_idx = producer
                if prod_idx >= idx:
                    raise PipelineTopologyError(
                        f"Producer must precede consumer for channel '{channel}'."
                    )

                edge = {
                    "source_node_uuid": prod_uuid,
                    "target_node_uuid": node_uuid,
                    "target_param": param,
                    "source_ref": source_ref,
                }
                if prod_slot != "out":
                    edge["source_output_slot"] = prod_slot

                edges.append(edge)
                continue

            raise PipelineConfigurationError(
                f"Unsupported SourceRef '{source_ref}' (node {node_uuid})."
            )

        prev_node_uuid = node_uuid

    return edges
