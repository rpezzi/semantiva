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
"""Identity helpers for CanonicalPipelineSpecV1."""

from __future__ import annotations

import hashlib
from typing import Any, Mapping

from semantiva.pipeline.cpsv1.derived_edges import derive_edges_v1
from semantiva.pipeline.cpsv1.stable_json import stable_json_bytes_v1


def compute_pipeline_id_cpsv1(cpsv1: Mapping[str, Any]) -> str:
    """Compute deterministic pipeline identity for CPSV1 specs."""

    return "plid-" + hashlib.sha256(stable_json_bytes_v1(cpsv1)).hexdigest()


def compute_upstream_map_cpsv1(cpsv1: Mapping[str, Any]) -> dict[str, list[str]]:
    """Return mapping of node_uuid -> list of upstream node_uuids derived from binds."""

    upstream: dict[str, set[str]] = {
        n["node_uuid"]: set() for n in cpsv1.get("nodes", [])
    }
    for edge in derive_edges_v1(cpsv1):
        upstream.setdefault(edge["target_node_uuid"], set()).add(
            edge["source_node_uuid"]
        )
    return {node_uuid: sorted(sources) for node_uuid, sources in upstream.items()}
