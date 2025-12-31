# Copyright 2025 Semantiva authors
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.

from __future__ import annotations

from dataclasses import dataclass
from typing import Any, Callable, Literal, Mapping, Protocol, Tuple

from semantiva.pipeline.payload import Payload

# PA-03A: contracts only. This module must not be wired into runtime execution paths.

SourceKind = Literal["channel", "context"]
ParameterSource = Literal["context", "data", "node", "default"]

TraceHook = Callable[[str, Mapping[str, Any]], None]

MISSING: Any = object()


@dataclass(frozen=True)
class SourceRef:
    kind: SourceKind
    key: str


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
    ) -> Tuple[Any, ParameterSource]: ...


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
