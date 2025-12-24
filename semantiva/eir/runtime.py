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

from typing import Any, Dict, List, Optional, Sequence, Tuple

from semantiva.eir.compiler import compile_eir_v1
from semantiva.eir.execution_scalar import EIRExecutionError, execute_eir_v1_scalar_plan
from semantiva.logger import Logger
from semantiva.pipeline.payload import Payload


def _extract_linear_scalar_segment(
    eir: Dict[str, Any],
) -> Tuple[List[str], Dict[str, Any]]:
    sem = eir.get("semantics") or {}
    forms = sem.get("payload_forms") or {}
    if forms.get("root_form") != "scalar" or forms.get("terminal_form") != "scalar":
        raise EIRExecutionError(
            "R0c eir_scalar backend supports scalar->scalar payloads only."
        )

    plan = eir.get("plan") or {}
    segments = plan.get("segments") or []
    if not (isinstance(segments, list) and len(segments) == 1):
        raise EIRExecutionError(
            "R0c eir_scalar backend supports exactly one plan segment."
        )

    seg0 = segments[0] or {}
    if seg0.get("kind") != "classic_linear":
        raise EIRExecutionError(
            "R0c eir_scalar backend supports classic_linear plans only."
        )

    node_order = seg0.get("node_order") or []
    if not (
        isinstance(node_order, list)
        and all(isinstance(x, str) and x for x in node_order)
    ):
        raise EIRExecutionError("Invalid classic_linear node_order in plan.")

    return node_order, eir


def _resolved_nodes_from_eir(
    eir: Dict[str, Any],
) -> Tuple[dict[str, Any], List[dict[str, Any]]]:
    node_order, _ = _extract_linear_scalar_segment(eir)

    graph = eir.get("graph") or {}
    nodes = graph.get("nodes") or []
    edges = graph.get("edges") or []

    node_by_uuid: dict[str, dict[str, Any]] = {}
    for node in nodes:
        if isinstance(node, dict) and isinstance(node.get("node_uuid"), str):
            node_by_uuid[node["node_uuid"]] = node

    params_obj = (eir.get("parameters") or {}).get("objects") or {}
    runtime = (
        (eir.get("source") or {}).get("node_runtime")
        if isinstance(eir.get("source"), dict)
        else None
    )

    resolved: list[dict[str, Any]] = []
    for node_uuid in node_order:
        spec = node_by_uuid.get(node_uuid)
        if not spec:
            raise EIRExecutionError(f"Node {node_uuid} not found in eir.graph.nodes")

        node_def: dict[str, Any] = {
            "processor": spec.get("processor_ref"),
            "parameters": params_obj.get(f"params:{node_uuid}", {}) or {},
        }

        runtime_row = runtime.get(node_uuid) if isinstance(runtime, dict) else None
        if isinstance(runtime_row, dict) and "context_key" in runtime_row:
            node_def["context_key"] = runtime_row["context_key"]

        resolved.append(node_def)

    canonical = {"version": 1, "nodes": list(nodes), "edges": list(edges)}
    return canonical, resolved


def build_scalar_specs_from_pipeline_spec(
    pipeline_or_spec: Any,
) -> Tuple[dict[str, Any], List[dict[str, Any]]]:
    """
    Compile a pipeline specification (YAML path or Python node list) into EIRv1 and
    return canonical/resolved specs for scalar execution.

    This helper intentionally constrains scope to classic_linear scalar pipelines for
    the FP-series opt-in execution backend. It serves orchestrator routing to reuse
    the legacy SER lifecycle while sourcing execution order and parameters from the
    compiled EIR document.
    """

    eir = compile_eir_v1(pipeline_or_spec)
    return _resolved_nodes_from_eir(eir)


def build_payload_algebra_specs_from_pipeline_spec(
    pipeline_or_spec: Any,
) -> Tuple[dict[str, Any], List[dict[str, Any]]]:
    """
    PA-03B routing seam for execution_backend="eir_payload_algebra".

    PA-03B is plumbing-only: this aliases the existing scalar compilation/resolution.
    Payload algebra semantics (channel store + bind/emit) are implemented in PA-03C.

    Note: intentionally not exported as a public API commitment in PA-03B.
    """

    return build_scalar_specs_from_pipeline_spec(pipeline_or_spec)


def build_scalar_specs_from_yaml(
    pipeline_yaml_path: str,
) -> Tuple[dict[str, Any], List[dict[str, Any]]]:
    """
    Backward-compatible wrapper around build_scalar_specs_from_pipeline_spec.
    """

    return build_scalar_specs_from_pipeline_spec(pipeline_yaml_path)


def run_eir_scalar_from_yaml(
    pipeline_yaml_path: str, payload: Payload, *, logger: Optional[Logger] = None
) -> Payload:
    """
    Compile a YAML classic pipeline to EIRv1 and execute it via the scalar harness.

    Scope: classic_linear + scalar->scalar only (enforced by execute_eir_v1_scalar_plan).
    """

    eir = compile_eir_v1(pipeline_yaml_path)
    return execute_eir_v1_scalar_plan(eir, payload, logger=logger)


__all__: Sequence[str] = [
    "build_scalar_specs_from_pipeline_spec",
    "build_scalar_specs_from_yaml",
    "run_eir_scalar_from_yaml",
]
