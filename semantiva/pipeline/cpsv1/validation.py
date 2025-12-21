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

import json
from importlib import resources
import inspect
from typing import Any, Callable, Dict, Optional

import jsonschema

from semantiva.registry.resolve import (
    AmbiguousProcessorError,
    UnknownProcessorError,
    resolve_symbol,
)


def load_canonical_pipeline_spec_v1_schema() -> Dict[str, Any]:
    """Load the packaged CanonicalPipelineSpecV1 JSON schema."""
    schema_path = (
        resources.files("semantiva.pipeline.schema")
        / "canonical_pipeline_spec_v1.schema.json"
    )
    return json.loads(schema_path.read_text(encoding="utf-8"))


def validate_canonical_pipeline_spec_v1(spec: Dict[str, Any]) -> None:
    """Validate a CPSV1 document against the packaged CPSV1 schema."""
    schema = load_canonical_pipeline_spec_v1_schema()
    jsonschema.Draft202012Validator(schema).validate(spec)


def validate_cpsv1(spec: Dict[str, Any]) -> None:
    """Alias for validate_canonical_pipeline_spec_v1 for ergonomics."""

    validate_canonical_pipeline_spec_v1(spec)


def validate_cpsv1_semantics(
    cpsv1: dict[str, Any],
    *,
    resolve_processor: Optional[Callable[[str], type]] = None,
    registry: Any | None = None,
) -> None:
    """
    Semantic preflight validation for CPSV1 beyond JSON Schema.
    """

    if resolve_processor is not None or registry is not None:
        _ = (resolve_processor, registry)

    def _bindable_param_names_for_processor(proc_cls: type) -> tuple[set[str], bool]:
        """
        Return (bindable_param_names, accepts_var_kwargs).

        - Excludes `self` / `cls` and var-positional params.
        - If the chosen callable accepts **kwargs, accepts_var_kwargs=True.
        """

        candidate_attrs = (
            "_process_logic",
            "_send_data",
            "_get_data",
            "_send_payload",
            "_get_payload",
            "process",
            "__call__",
        )

        for attr in candidate_attrs:
            fn = getattr(proc_cls, attr, None)
            if fn is None or not callable(fn):
                continue
            try:
                sig = inspect.signature(fn)
            except (TypeError, ValueError):
                continue

            accepts_var_kwargs = any(
                p.kind == inspect.Parameter.VAR_KEYWORD for p in sig.parameters.values()
            )

            names: set[str] = set()
            for p in sig.parameters.values():
                if p.name in {"self", "cls"}:
                    continue
                if p.kind in {
                    inspect.Parameter.VAR_POSITIONAL,
                    inspect.Parameter.VAR_KEYWORD,
                }:
                    continue
                names.add(p.name)

            return names, accepts_var_kwargs

        return set(), False

    nodes = cpsv1.get("nodes", [])
    if not isinstance(nodes, list):
        raise ValueError("CPSV1 semantic error: 'nodes' must be a list.")

    known_channels: set[str] = {"primary"}
    non_primary_writers: dict[str, str] = {}

    for node in nodes:
        node_uuid = node.get("node_uuid", "<missing-node_uuid>")

        processor_ref = node.get("processor_ref")
        if not isinstance(processor_ref, str) or not processor_ref:
            raise ValueError(
                f"CPSV1 semantic error: node {node_uuid} must set a non-empty processor_ref."
            )

        try:
            if resolve_processor is not None:
                proc_cls = resolve_processor(processor_ref)
            else:
                proc_cls = resolve_symbol(processor_ref)
        except (UnknownProcessorError, AmbiguousProcessorError) as exc:
            raise ValueError(
                f"CPSV1 semantic error: node {node_uuid} processor_ref could not be resolved "
                f"({processor_ref!r}): {exc}"
            ) from exc

        bindable_params, accepts_var_kwargs = _bindable_param_names_for_processor(
            proc_cls
        )

        bind = node.get("bind") or {}
        if not isinstance(bind, dict):
            raise ValueError(
                f"CPSV1 semantic error: node {node_uuid} bind must be an object."
            )

        if not accepts_var_kwargs:
            for param in bind.keys():
                if param == "data":
                    continue
                if param not in bindable_params:
                    raise ValueError(
                        f"CPSV1 semantic error: node {node_uuid} binds unknown parameter "
                        f"{param!r} for processor_ref={processor_ref!r}."
                    )

        for param, source in bind.items():
            if not isinstance(source, str):
                raise ValueError(
                    f"CPSV1 semantic error: bind.{param} must be a string, "
                    f"got {type(source).__name__}."
                )
            if source.startswith("channel:"):
                channel = source.split("channel:", 1)[1]
                if channel not in known_channels:
                    raise ValueError(
                        f"CPSV1 semantic error: bind.{param} references unknown "
                        f"channel '{channel}' at node {node_uuid}."
                    )
            elif source.startswith("context:"):
                pass
            else:
                raise ValueError(
                    f"CPSV1 semantic error: bind.{param} has unsupported SourceRef "
                    f"'{source}'. Expected channel:<name> or context:<key>."
                )

        publish = node.get("publish") or {}
        channels = (publish.get("channels") or {}) if isinstance(publish, dict) else {}
        out = channels.get("out") if isinstance(channels, dict) else None
        out = out or "primary"

        if not isinstance(out, str) or not out:
            raise ValueError(
                f"CPSV1 semantic error: node {node_uuid} publish.channels.out must "
                "be a non-empty string."
            )

        if out != "primary":
            previous_writer = non_primary_writers.get(out)
            if previous_writer is not None and previous_writer != node_uuid:
                raise ValueError(
                    f"CPSV1 semantic error: channel '{out}' published more than once "
                    f"(first {previous_writer}, again {node_uuid})."
                )
            non_primary_writers[out] = node_uuid

        known_channels.add(out)
