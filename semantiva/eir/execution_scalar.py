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

from typing import Any, Dict, Optional

from semantiva.eir.validation import validate_eir_v1
from semantiva.logger import Logger
from semantiva.pipeline.payload import Payload
from semantiva.pipeline.nodes._pipeline_node_factory import _pipeline_node_factory
from semantiva.registry import load_extensions
from semantiva.registry.descriptors import instantiate_from_descriptor


class EIRExecutionError(RuntimeError):
    """Raised when EIR execution cannot proceed due to unsupported features or invalid inputs."""


def execute_eir_v1_scalar_plan(
    eir: Dict[str, Any],
    payload: Payload,
    *,
    logger: Optional[Logger] = None,
) -> Payload:
    """
    Execute a classic scalar Semantiva pipeline from an EIRv1 document.

    Scope:
      - classic_linear only
      - scalar payload form only
      - no branching, no nesting, no channel/lane execution
      - internal-only (no runtime switch, no parity claim)

    Raises:
      - jsonschema.ValidationError via validate_eir_v1
      - EIRExecutionError for unsupported / missing runtime requirements
    """
    validate_eir_v1(eir)

    src = eir.get("source") or {}
    exts = src.get("extensions")
    if isinstance(exts, list) and all(isinstance(x, str) for x in exts):
        try:
            load_extensions(exts)
        except Exception:
            pass

    sem = eir.get("semantics") or {}
    forms = sem.get("payload_forms") or {}
    if forms.get("root_form") != "scalar" or forms.get("terminal_form") != "scalar":
        raise EIRExecutionError(
            "R0a supports scalar->scalar only (root_form/terminal_form must be 'scalar')."
        )

    plan = eir.get("plan") or {}
    segments = plan.get("segments") or []
    if not (isinstance(segments, list) and len(segments) == 1):
        raise EIRExecutionError("R0a supports exactly one plan segment.")
    seg0 = segments[0] or {}
    if seg0.get("kind") != "classic_linear":
        raise EIRExecutionError(
            "R0a supports plan.segments[0].kind == 'classic_linear' only."
        )
    node_order = seg0.get("node_order") or []
    if not (
        isinstance(node_order, list)
        and all(isinstance(x, str) and x for x in node_order)
    ):
        raise EIRExecutionError("Invalid classic_linear node_order in plan.")

    graph = eir.get("graph") or {}
    nodes = graph.get("nodes") or []
    node_by_uuid: Dict[str, Dict[str, Any]] = {}
    for n in nodes:
        if isinstance(n, dict):
            uu = n.get("node_uuid")
            if isinstance(uu, str) and uu:
                node_by_uuid[uu] = n

    params_obj = (eir.get("parameters") or {}).get("objects") or {}
    node_runtime = (src.get("node_runtime") or {}) if isinstance(src, dict) else {}

    log = logger
    if log is None:
        from semantiva.logger.logger import Logger as _Logger

        log = _Logger()

    out = payload
    for node_uuid in node_order:
        node_spec = node_by_uuid.get(node_uuid)
        if not node_spec:
            raise EIRExecutionError(f"Node {node_uuid} not found in eir.graph.nodes")

        proc_ref = node_spec.get("processor_ref")
        if not isinstance(proc_ref, str) or not proc_ref:
            raise EIRExecutionError(f"Node {node_uuid} missing processor_ref")

        raw_params = params_obj.get(f"params:{node_uuid}", {}) or {}
        hydrated_params = instantiate_from_descriptor(raw_params)

        node_def: Dict[str, Any] = {
            "processor": proc_ref,
            "parameters": hydrated_params,
        }

        runtime_row = (
            node_runtime.get(node_uuid) if isinstance(node_runtime, dict) else None
        )
        if isinstance(runtime_row, dict) and "context_key" in runtime_row:
            node_def["context_key"] = runtime_row["context_key"]

        node = _pipeline_node_factory(node_def, log)
        try:
            setattr(node, "node_uuid", node_uuid)
        except Exception:
            pass

        out = node.process(out)

    return out
