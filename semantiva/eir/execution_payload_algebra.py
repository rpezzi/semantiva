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

"""PA-03C payload algebra execution backend with channel store and bind/publish semantics."""

from __future__ import annotations

from dataclasses import dataclass
from typing import Any, Mapping, MutableMapping, Tuple

from semantiva.context_processors.context_types import ContextType
from semantiva.eir.payload_algebra_contracts import (
    MISSING,
    ParameterSource,
    parse_source_ref,
)
from semantiva.eir.runtime import _resolved_nodes_from_eir
from semantiva.logger import Logger
from semantiva.pipeline._param_resolution import param_resolution_overlay
from semantiva.pipeline.nodes._pipeline_node_factory import _pipeline_node_factory
from semantiva.pipeline.payload import Payload
from semantiva.registry.descriptors import instantiate_from_descriptor


class PayloadAlgebraResolutionError(RuntimeError):
    """Deterministic runtime error for PA-03C bind/publish resolution failures."""


def _context_keys(context: Mapping[str, Any] | ContextType) -> set[str]:
    if isinstance(context, Mapping):
        return {str(k) for k in context.keys()}
    getter = getattr(context, "keys", None)
    if callable(getter):
        try:
            return {str(k) for k in getter()}
        except Exception:  # pragma: no cover - defensive
            return set()
    return set()


def _context_get(context: Mapping[str, Any] | ContextType, key: str) -> Any:
    if isinstance(context, Mapping):
        return context.get(key, MISSING)
    getter = getattr(context, "get_value", None)
    if callable(getter):
        try:
            return getter(key)
        except Exception:  # pragma: no cover - defensive
            return MISSING
    return MISSING


class InMemoryChannelStore:
    """
    PA-03C channel store implementation.

    Plan requirement: seeded with primary = initial_payload.data.
    """

    def __init__(self) -> None:
        self._channels: dict[str, Any] = {}

    def seed_primary(self, value: Any) -> None:
        """Seed primary channel with initial payload data."""
        self._channels["primary"] = value

    def get(self, name: str) -> Any:
        """Get channel value by name, returns MISSING if not found."""
        return self._channels.get(name, MISSING)

    def set(self, name: str, value: Any) -> None:
        """Set channel value by name."""
        self._channels[name] = value


def resolve_param_value(
    param_name: str,
    *,
    binds: Mapping[str, str],
    node_params: Mapping[str, Any],
    context: Mapping[str, Any] | ContextType,
    channels: InMemoryChannelStore,
    default: Any = MISSING,
) -> Tuple[Any, ParameterSource]:
    """
    PA-03C resolver implementing Plan precedence:

      1) bind
      2) node parameters
      3) context-by-name
      4) python default

    Special case: data defaults to channel:primary unless explicitly bound.
    Reject ambiguity among (1)-(3).
    """
    normalized_binds: MutableMapping[str, str] = {
        str(k): str(v) for k, v in (binds or {}).items()
    }
    if param_name == "data" and param_name not in normalized_binds:
        normalized_binds["data"] = "channel:primary"

    bind_ref = None
    if param_name in normalized_binds:
        ref_raw = normalized_binds[param_name]
        bind_ref = parse_source_ref(ref_raw)

        conflicts: list[str] = []
        if param_name in node_params:
            conflicts.append("node parameters")
        if param_name in _context_keys(context):
            conflicts.append("context")
        if conflicts:
            conflict_locs = " and ".join(conflicts)
            raise PayloadAlgebraResolutionError(
                f"Ambiguous resolution for '{param_name}': bind conflicts with {conflict_locs}."
            )

        if bind_ref.kind == "channel":
            value = channels.get(bind_ref.key)
            if value is MISSING:
                raise PayloadAlgebraResolutionError(
                    f"Channel '{bind_ref.key}' is not available for parameter '{param_name}'."
                )
            return value, "bind"

        if bind_ref.kind == "context":
            if bind_ref.key not in _context_keys(context):
                raise PayloadAlgebraResolutionError(
                    f"Context key '{bind_ref.key}' is not available for parameter '{param_name}'."
                )
            return _context_get(context, bind_ref.key), "bind"

    if param_name in node_params:
        return node_params[param_name], "node"

    if param_name in _context_keys(context):
        return _context_get(context, param_name), "context"

    if default is not MISSING:
        return default, "default"

    raise PayloadAlgebraResolutionError(
        f"Unable to resolve parameter '{param_name}' via bind/node/context/default."
    )


@dataclass(frozen=True)
class PublishPlanV1:
    """Publication plan for payload algebra nodes."""

    out_channel: str

    @classmethod
    def from_cpsv1(cls, node_spec: Mapping[str, Any]) -> "PublishPlanV1":
        """Create publish plan from CPSV1 node spec."""
        publish = node_spec.get("publish") or {}
        channels = publish.get("channels") or {}
        out = channels.get("out") or "primary"
        return cls(out_channel=str(out))

    def apply(self, output_value: Any, channels: InMemoryChannelStore) -> None:
        """Apply publication plan: set output value to target channel."""
        if self.out_channel == "primary":
            channels.set("primary", output_value)
            return
        channels.set(self.out_channel, output_value)


def execute_eir_payload_algebra(
    eir: Mapping[str, Any],
    payload: Payload,
    *,
    trace_hook=None,
) -> Payload:
    """
    Execute a classic_linear payload-algebra plan (channel store + bind/publish).

    This harness intentionally mirrors PA-03C orchestrator semantics without
    altering SER/provenance. It is suitable for lightweight execution or tests.
    """
    canonical, resolved_spec = _resolved_nodes_from_eir(dict(eir))
    nodes: list = []
    node_defs: list[dict[str, Any]] = []
    for node_def in resolved_spec:
        hydrated_params = instantiate_from_descriptor(node_def.get("parameters", {}))
        nd = dict(node_def)
        nd["parameters"] = hydrated_params
        node_defs.append(nd)
        nodes.append(_pipeline_node_factory(nd, Logger()))

    channels = InMemoryChannelStore()
    channels.seed_primary(payload.data)
    data = payload.data
    context = payload.context

    node_spec_by_uuid: dict[str, dict[str, Any]] = {
        str(n.get("node_uuid")): n for n in canonical.get("nodes", []) if n
    }

    for node, node_def in zip(nodes, node_defs):
        node_uuid = getattr(node, "node_uuid", None) or node_def.get("node_uuid", "")
        node_spec = node_spec_by_uuid.get(node_uuid, node_def)
        binds = node_spec.get("bind") or {}
        publish_plan = PublishPlanV1.from_cpsv1(node_spec)

        params_cfg = node_def.get("parameters") or {}
        param_names: list[str] = list(
            getattr(node.processor, "get_processing_parameter_names", lambda: [])()
        )
        resolved: dict[str, Any] = {}
        for name in param_names:
            resolved[name], _ = resolve_param_value(
                name,
                binds=binds,
                node_params=params_cfg,
                context=context,
                channels=channels,
            )

        input_data, _ = resolve_param_value(
            "data",
            binds=binds,
            node_params=params_cfg,
            context=context,
            channels=channels,
        )

        # Use overlay instead of mutating processor_config

        with param_resolution_overlay(resolved):
            result = node.process(Payload(input_data, context))

        publish_plan.apply(result.data, channels)
        data = channels.get("primary")
        if data is MISSING:
            raise PayloadAlgebraResolutionError(
                "Primary channel missing after publish; ensure a source populated it."
            )
        context = result.context

    # Emit minimal trace hook notification if provided
    if callable(trace_hook):
        trace_hook("end", {"nodes": len(nodes)})

    return Payload(data, context)
