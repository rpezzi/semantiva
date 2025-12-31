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
from typing import Any, Mapping, MutableMapping

from semantiva.trace._utils import serialize

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
    """Deterministic runtime error for PA-03C/D bind/publish resolution failures."""


@dataclass(frozen=True)
class ProducerRef:
    kind: str  # "pipeline_input_context" | "pipeline_input_data" | "node"
    node_uuid: str | None = None
    output_slot: str = "out"

    def to_dict(self) -> dict[str, Any]:
        d: dict[str, Any] = {"kind": self.kind}
        if self.node_uuid is not None:
            d["node_uuid"] = self.node_uuid
        if self.kind == "node":
            d["output_slot"] = self.output_slot
        return d


@dataclass
class ChannelEntry:
    value: Any
    producer: ProducerRef


@dataclass(frozen=True)
class ResolvedParam:
    value: Any
    source: ParameterSource
    source_ref: dict[str, Any] | None = None


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
    PA-03C/D channel store implementation with producer tracking.

    Plan requirement: seeded with primary = initial_payload.data.
    """

    def __init__(self) -> None:
        self._channels: dict[str, ChannelEntry] = {}

    def seed_primary(self, value: Any) -> None:
        """Seed primary channel with initial payload data."""
        self._channels["primary"] = ChannelEntry(
            value=value, producer=ProducerRef(kind="pipeline_input_data")
        )

    def get(self, name: str) -> Any:
        """Get channel value by name, returns MISSING if not found."""
        entry = self._channels.get(name)
        return entry.value if entry is not None else MISSING

    def get_entry(self, name: str) -> ChannelEntry | None:
        """Get full channel entry including producer (PA-03D)."""
        return self._channels.get(name)

    def set(self, name: str, value: Any) -> None:
        """
        Set channel value by name.

        Contract signature (PA-03A) does not include producer metadata;
        preserve existing producer when possible.
        """
        existing = self._channels.get(name)
        producer = (
            existing.producer
            if existing is not None
            else ProducerRef(kind="pipeline_input_data")
        )
        self._channels[name] = ChannelEntry(value=value, producer=producer)

    def set_entry(
        self,
        name: str,
        *,
        value: Any,
        producer: ProducerRef,
        carry_forward_from: str | None = None,
    ) -> None:
        """
        Internal helper to set channel value with producer metadata.

        If carry_forward_from is provided and the value matches the source
        channel, carry forward the existing producer identity.
        """
        if carry_forward_from is not None:
            existing = self._channels.get(carry_forward_from)
            if existing is not None:
                same = existing.value is value
                if not same:
                    try:
                        same = existing.value == value
                    except Exception:  # pragma: no cover - defensive equality
                        same = False
                if same:
                    self._channels[name] = ChannelEntry(
                        value=value, producer=existing.producer
                    )
                    return

        self._channels[name] = ChannelEntry(value=value, producer=producer)


class ContextProducerStore:
    """
    PA-03D context producer tracking (last-writer semantics).

    Tracks per-key producer identity so consumed context provenance reflects
    the value actually read (Plan legacy behavior note for multi-writers).
    """

    def __init__(self, initial_keys: set[str] | None = None) -> None:
        """Initialize with all initial context keys attributed to pipeline input."""
        self._producers: dict[str, ProducerRef] = {}
        for key in initial_keys or set():
            self._producers[key] = ProducerRef(kind="pipeline_input_context")

    def get_producer(self, key: str) -> ProducerRef:
        """Get producer for a context key (defaults to pipeline_input_context)."""
        return self._producers.get(key, ProducerRef(kind="pipeline_input_context"))

    def mark_written(self, keys: set[str], node_uuid: str) -> None:
        """Mark keys as written by a node (last-writer update)."""
        producer = ProducerRef(kind="node", node_uuid=node_uuid)
        for key in keys:
            self._producers[key] = producer

    def snapshot(self) -> dict[str, ProducerRef]:
        """Return current producer mapping (for reads during resolution)."""
        return dict(self._producers)


def resolve_param_value(
    param_name: str,
    *,
    binds: Mapping[str, str],
    node_params: Mapping[str, Any],
    context: Mapping[str, Any] | ContextType,
    channels: InMemoryChannelStore,
    default: Any = MISSING,
    context_producer: ProducerRef | None = None,
    context_producers: Mapping[str, ProducerRef] | None = None,
) -> ResolvedParam:
    """
    PA-03C/D resolver implementing Plan precedence with value-origin provenance:

      1) bind
      2) node parameters
      3) context-by-name
      4) python default

    Special case: data defaults to channel:primary unless explicitly bound.
    Reject ambiguity among (1)-(3).
    ``context_producer`` (deprecated) or ``context_producers`` identifies origin of context keys.
    """
    normalized_binds: MutableMapping[str, str] = {
        str(k): str(v) for k, v in (binds or {}).items()
    }
    if param_name == "data" and param_name not in normalized_binds:
        normalized_binds["data"] = "channel:primary"

    # Use per-key producers if available, else fallback to single producer
    if context_producers is None:
        context_producer = context_producer or ProducerRef(
            kind="pipeline_input_context"
        )

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
            entry = channels.get_entry(bind_ref.key)
            if entry is None:
                raise PayloadAlgebraResolutionError(
                    f"Channel '{bind_ref.key}' is not available for parameter '{param_name}'."
                )
            return ResolvedParam(
                value=entry.value,
                source="data",
                source_ref={
                    "kind": "data",
                    "channel": bind_ref.key,
                    "producer": entry.producer.to_dict(),
                },
            )

        if bind_ref.kind == "context":
            if bind_ref.key not in _context_keys(context):
                raise PayloadAlgebraResolutionError(
                    f"Context key '{bind_ref.key}' is not available for parameter '{param_name}'."
                )
            # Use per-key producer if available
            if context_producers:
                effective_producer = context_producers.get(bind_ref.key)
                if effective_producer is None:
                    effective_producer = (
                        context_producer
                        if context_producer
                        else ProducerRef(kind="pipeline_input_context")
                    )
            else:
                effective_producer = (
                    context_producer
                    if context_producer
                    else ProducerRef(kind="pipeline_input_context")
                )
            return ResolvedParam(
                value=_context_get(context, bind_ref.key),
                source="context",
                source_ref={
                    "kind": "context",
                    "key": bind_ref.key,
                    "producer": effective_producer.to_dict(),
                },
            )

    if param_name in node_params:
        return ResolvedParam(value=node_params[param_name], source="node")

    if param_name in _context_keys(context):
        # Use per-key producer if available
        if context_producers:
            effective_producer = context_producers.get(param_name)
            if effective_producer is None:
                effective_producer = (
                    context_producer
                    if context_producer
                    else ProducerRef(kind="pipeline_input_context")
                )
        else:
            effective_producer = (
                context_producer
                if context_producer
                else ProducerRef(kind="pipeline_input_context")
            )
        return ResolvedParam(
            value=_context_get(context, param_name),
            source="context",
            source_ref={
                "kind": "context",
                "key": param_name,
                "producer": effective_producer.to_dict(),
            },
        )

    if default is not MISSING:
        return ResolvedParam(value=default, source="default")

    raise PayloadAlgebraResolutionError(
        f"Unable to resolve parameter '{param_name}' via bind/node/context/default."
    )


def _is_passthrough_node(node: Any) -> bool:
    """
    Determine if a node is pass-through for data (ADR-0004 §5).

    Pass-through nodes: ContextProcessor, DataProbe, DataSink
    """
    processor = getattr(node, "processor", None)
    if processor is None:
        return False

    processor_cls = type(processor)
    name = processor_cls.__name__
    if "ContextProcessor" in name:
        return True

    if hasattr(processor_cls, "__mro__"):
        for cls in processor_cls.__mro__:
            if cls.__name__ in {"DataProbe", "DataSink"}:
                return True
    return False


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

    def apply(
        self,
        output_value: Any,
        channels: InMemoryChannelStore,
    ) -> None:
        """
        Apply publication plan: set output value to target channel.
        """
        channels.set(self.out_channel, output_value)


def execute_eir_payload_algebra(
    eir: Mapping[str, Any],
    payload: Payload,
    *,
    trace_hook=None,
) -> Payload:
    """
    Execute a classic_linear payload-algebra plan (channel store + bind/publish).

    PA-03C: core execution semantics
    PA-03D: provenance tracking for SER emission (with last-writer context producers)
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

    # PA-03D: per-key context producer tracking (last-writer)
    initial_keys = set()
    if hasattr(context, "keys") and callable(context.keys):
        initial_keys = set(context.keys())
    elif isinstance(context, dict):
        initial_keys = set(context.keys())
    context_producer_store = ContextProducerStore(initial_keys)

    node_spec_by_uuid: dict[str, dict[str, Any]] = {
        str(n.get("node_uuid")): n for n in canonical.get("nodes", []) if n
    }

    provenance: list[dict[str, Any]] = []

    for node, node_def in zip(nodes, node_defs):
        node_uuid = getattr(node, "node_uuid", None) or node_def.get("node_uuid", "")
        node_spec = node_spec_by_uuid.get(node_uuid, node_def)
        binds = node_spec.get("bind") or {}
        publish_plan = PublishPlanV1.from_cpsv1(node_spec)

        params_cfg = node_def.get("parameters") or {}
        param_names: list[str] = list(
            getattr(node.processor, "get_processing_parameter_names", lambda: [])()
        )

        # Snapshot context producers before resolution
        context_producers_snapshot = context_producer_store.snapshot()

        resolved: dict[str, Any] = {}
        param_sources: dict[str, str] = {}
        param_source_refs: dict[str, dict[str, Any]] = {}
        upstream: list[str] = []

        for name in param_names:
            res = resolve_param_value(
                name,
                binds=binds,
                node_params=params_cfg,
                context=context,
                channels=channels,
                context_producers=context_producers_snapshot,
            )
            resolved[name] = res.value
            param_sources[name] = res.source
            if res.source_ref is not None:
                param_source_refs[name] = res.source_ref
                producer = res.source_ref.get("producer", {})
                if producer.get("kind") == "node" and producer.get("node_uuid"):
                    if producer["node_uuid"] not in upstream:
                        upstream.append(producer["node_uuid"])

        data_result = resolve_param_value(
            "data",
            binds=binds,
            node_params=params_cfg,
            context=context,
            channels=channels,
            context_producers=context_producers_snapshot,
        )
        input_data = data_result.value
        param_sources["data"] = data_result.source
        if data_result.source_ref is not None:
            param_source_refs["data"] = data_result.source_ref
            producer = data_result.source_ref.get("producer", {})
            if producer.get("kind") == "node" and producer.get("node_uuid"):
                if producer["node_uuid"] not in upstream:
                    upstream.append(producer["node_uuid"])

        input_channel = "primary"
        if "data" in binds:
            bind_ref = parse_source_ref(binds["data"])
            if bind_ref.kind == "channel":
                input_channel = bind_ref.key

        # Capture pre-execution context fingerprints to detect writes.
        # This must happen before execution to detect in-place mutation.
        pre_context_keys = set()
        if hasattr(context, "keys") and callable(context.keys):
            pre_context_keys = set(context.keys())
        elif isinstance(context, dict):
            pre_context_keys = set(context.keys())

        pre_fingerprints: dict[str, bytes] = {}
        for key in pre_context_keys:
            try:
                pre_fingerprints[key] = serialize(_context_get(context, key))
            except Exception:
                # Conservative: if we can't fingerprint, assume it may be written
                pre_fingerprints[key] = b"__semantiva_unfingerprintable__"

        with param_resolution_overlay(resolved):
            result = node.process(Payload(input_data, context))

        # Detect context writes and update producer tracking
        post_context_keys = set()
        if hasattr(result.context, "keys") and callable(result.context.keys):
            post_context_keys = set(result.context.keys())
        elif isinstance(result.context, dict):
            post_context_keys = set(result.context.keys())

        written_keys = post_context_keys - pre_context_keys

        # Also check for updated keys using pre/post fingerprints.
        for key in pre_context_keys & post_context_keys:
            try:
                post_fp = serialize(_context_get(result.context, key))
            except Exception:
                written_keys.add(key)
                continue

            pre_fp = pre_fingerprints.get(key)
            if pre_fp is None:
                written_keys.add(key)
                continue

            if pre_fp != post_fp:
                written_keys.add(key)

        if written_keys:
            context_producer_store.mark_written(written_keys, node_uuid)

        is_passthrough = _is_passthrough_node(node)
        node_producer = ProducerRef(kind="node", node_uuid=node_uuid, output_slot="out")

        publish_plan.apply(result.data, channels)
        out_channel = getattr(publish_plan, "out_channel", "primary")
        if is_passthrough and out_channel == input_channel:
            in_entry = channels.get_entry(input_channel)
            if in_entry is not None:
                channels.set_entry(
                    out_channel,
                    value=result.data,
                    producer=in_entry.producer,
                    carry_forward_from=input_channel,
                )
            else:
                channels.set_entry(
                    out_channel,
                    value=result.data,
                    producer=ProducerRef(kind="pipeline_input_data"),
                    carry_forward_from=input_channel,
                )
        else:
            channels.set_entry(
                out_channel,
                value=result.data,
                producer=node_producer,
            )
        data = channels.get("primary")
        if data is MISSING:
            raise PayloadAlgebraResolutionError(
                "Primary channel missing after publish; ensure a source populated it."
            )
        context = result.context

        provenance.append(
            {
                "node_uuid": node_uuid,
                "param_sources": param_sources,
                "param_source_refs": param_source_refs,
                "upstream": upstream,
            }
        )

    # Emit trace hook notification with provenance if provided
    if callable(trace_hook):
        trace_hook("end", {"nodes": len(nodes), "provenance": provenance})

    return Payload(data, context)
