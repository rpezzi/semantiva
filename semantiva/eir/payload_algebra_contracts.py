# Copyright 2025 Semantiva authors
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.

from __future__ import annotations

from dataclasses import dataclass
from typing import Any, Callable, Literal, Mapping, Protocol

from semantiva.pipeline.payload import Payload

# PA-03A: contracts only. Must not be wired into runtime execution paths in this epic.

SourceKind = Literal["channel", "context"]
ParameterSource = Literal["context", "data", "node", "default"]
ProducerKind = Literal["pipeline_input_context", "pipeline_input_data", "node"]

TraceHook = Callable[[str, Mapping[str, Any]], None]

MISSING: Any = object()


@dataclass(frozen=True)
class SourceRef:
    kind: SourceKind
    key: str


@dataclass(frozen=True)
class ProducerRef:
    """Identifies the producer of a value for provenance tracking (ADR-0004)."""

    kind: ProducerKind
    node_uuid: str | None = None
    output_slot: str = "out"

    def to_dict(self) -> dict[str, Any]:
        """Serialize to JSON-compatible dict for SER emission."""
        result: dict[str, Any] = {"kind": self.kind}
        if self.node_uuid is not None:
            result["node_uuid"] = self.node_uuid
        if self.kind == "node":
            result["output_slot"] = self.output_slot
        return result


@dataclass
class ChannelEntry:
    """Channel store entry with value and producer identity (PA-03D)."""

    value: Any
    producer: ProducerRef


@dataclass(frozen=True)
class ResolvedParam:
    """Resolution result with value, source category, and optional ref (PA-03D)."""

    value: Any
    source: ParameterSource
    source_ref: dict[str, Any] | None = None  # ContextSourceRef or DataSourceRef


def parse_source_ref(raw: str) -> SourceRef:
    """
    Grammar:
      - "channel:<name>"
      - "context:<key>"
      - "<unprefixed>" defaults to "channel:<unprefixed>"

    This helper is part of the PA-03 contract/grammar lock and is exercised
    by the payload algebra runtime.
    """
    candidate = raw.strip()
    if ":" not in candidate:
        if not candidate:
            raise ValueError("Invalid SourceRef: empty string")
        return SourceRef(kind="channel", key=candidate)

    kind, key = candidate.split(":", 1)
    kind = kind.strip()
    key = key.strip()
    if not kind or not key:
        raise ValueError(f"Invalid SourceRef: {raw!r}")

    if kind == "channel":
        return SourceRef(kind="channel", key=key)
    if kind == "context":
        return SourceRef(kind="context", key=key)

    raise ValueError(
        f"Invalid SourceRef prefix: {kind!r} (expected 'channel' or 'context')"
    )


class ChannelStore(Protocol):
    def get(self, name: str) -> Any: ...
    def set(self, name: str, value: Any) -> None: ...
    def seed_primary(self, value: Any) -> None: ...


class BindResolver(Protocol):
    def resolve_param(
        self,
        param_name: str,
        *,
        binds: Mapping[str, str],
        node_params: Mapping[str, Any],
        context: Mapping[str, Any],
        channels: ChannelStore,
        default: Any = MISSING,
    ) -> ResolvedParam: ...


class PublishPlan(Protocol):
    @classmethod
    def from_cpsv1(cls, node_spec: Mapping[str, Any]) -> "PublishPlan": ...

    def apply(self, output_value: Any, channels: ChannelStore) -> None: ...


def execute_eir_payload_algebra(
    eir: Mapping[str, Any],
    payload: Payload,
    *,
    trace_hook: TraceHook | None = None,
) -> Payload:
    """
    Plan-required entry point contract (PA-03A locks the signature only).
    Runtime implementation is explicitly out of scope until PA-03C.
    """
    raise NotImplementedError("PA-03A contract stub (implemented in PA-03C)")
