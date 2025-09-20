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

"""Orchestrator implementations and the template-method execution lifecycle.

The canonical documentation for the orchestrator lifecycle and SER integration lives in:
- docs/source/execution.rst (Execution lifecycle and template method)
- docs/source/ser.rst (SER structure, checks, and environment pins)
"""

from __future__ import annotations

import time
import uuid
from abc import ABC, abstractmethod
from datetime import datetime
from typing import Any, Callable, Iterable, List, Optional, Sequence, TypeVar, cast

from semantiva.data_processors.data_processors import ParameterInfo, _NO_DEFAULT
from semantiva.execution.executor.executor import (
    SemantivaExecutor,
    SequentialSemantivaExecutor,
)
from semantiva.execution.transport import SemantivaTransport
from semantiva.logger import Logger
from semantiva.pipeline.graph_builder import (
    build_canonical_spec,
    compute_pipeline_id,
    compute_upstream_map,
)
from semantiva.pipeline.nodes._pipeline_node_factory import _pipeline_node_factory
from semantiva.pipeline.nodes.nodes import _PipelineNode
from semantiva.pipeline.payload import Payload
from semantiva.registry.descriptors import instantiate_from_descriptor
from semantiva.trace._utils import (
    canonical_json_bytes,
    collect_env_pins as _collect_env_pins_util,
    context_to_kv_repr,
    safe_repr,
    serialize,
    serialize_json_safe,
    sha256_bytes,
)
from semantiva.trace.delta_collector import DeltaCollector
from semantiva.trace.model import IODelta, SERRecord, TraceDriver

T = TypeVar("T")


def _const_supplier(value: T) -> Callable[[], T]:
    def _supplier() -> T:
        return value

    return _supplier


class SemantivaOrchestrator(ABC):
    """Template-method orchestrator that centralises SER composition.

    See also: docs/source/execution.rst for the lifecycle and extension hooks,
    and docs/source/ser.rst for the evidence (SER) format produced by this
    runtime.
    """

    def __init__(self) -> None:
        self._last_nodes: list[_PipelineNode] = []
        self._next_run_metadata: dict[str, Any] | None = None
        self._current_run_metadata: dict[str, Any] | None = None

    @property
    def last_nodes(self) -> List[_PipelineNode]:
        return self._last_nodes

    def configure_run_metadata(self, metadata: dict[str, Any] | None) -> None:
        """Stage metadata to be used for the next :meth:`execute` call."""

        self._next_run_metadata = dict(metadata or {})

    # ------------------------------------------------------------------
    # Public lifecycle
    # ------------------------------------------------------------------
    def execute(
        self,
        pipeline_spec: List[dict[str, Any]],
        payload: Payload,
        transport: SemantivaTransport,
        logger: Logger,
        trace: TraceDriver | None = None,
        canonical_spec: dict[str, Any] | None = None,
        run_metadata: dict[str, Any] | None = None,
    ) -> Payload:
        """Run the pipeline and emit SER records via the template method.

        For behaviour details (checks, IO delta, timing, env pins), refer to
        docs/source/execution.rst and docs/source/ser.rst.
        """

        data = payload.data
        context = payload.context

        pending_meta = run_metadata
        if pending_meta is None and self._next_run_metadata is not None:
            pending_meta = self._next_run_metadata
        self._next_run_metadata = None
        self._current_run_metadata = dict(pending_meta or {})

        canonical = canonical_spec
        resolved_spec: Sequence[dict[str, Any]] = pipeline_spec
        if canonical is None:
            canonical, resolved_spec = build_canonical_spec(pipeline_spec)

        run_id: str | None = None
        pipeline_id: str | None = None
        node_uuids: list[str] = []
        upstream_map: dict[str, list[str]] = {}
        trace_opts = self._trace_options(trace)

        if trace is not None:
            pipeline_id = compute_pipeline_id(canonical)
            node_uuids = [n["node_uuid"] for n in canonical.get("nodes", [])]
            upstream_map = compute_upstream_map(canonical)
            run_id = f"run-{uuid.uuid4().hex}"
            meta = {"num_nodes": len(node_uuids)}
            try:
                trace.on_pipeline_start(
                    pipeline_id, run_id, canonical, meta, pipeline_input=payload
                )
            except TypeError:
                trace.on_pipeline_start(pipeline_id, run_id, canonical, meta)

        nodes, node_defs = self._instantiate_nodes(resolved_spec, logger)
        self._last_nodes = list(nodes)

        trace_active = (
            trace is not None and run_id is not None and pipeline_id is not None
        )
        trace_driver = cast(TraceDriver, trace) if trace_active else None
        run_token = cast(str, run_id) if trace_active else ""
        pipeline_token = cast(str, pipeline_id) if trace_active else ""
        env_pins_static = self._collect_env_pins() if trace_driver is not None else {}
        if trace_driver is not None:
            try:
                from semantiva.registry.bootstrap import current_profile

                env_pins_static = dict(env_pins_static)
                env_pins_static["registry.fingerprint"] = (
                    current_profile().fingerprint()
                )
            except Exception:
                pass

        try:
            for index, node in enumerate(nodes):
                node_def = node_defs[index]
                node_id = node_uuids[index] if index < len(node_uuids) else ""
                pre_ctx_view = self._context_snapshot(context)
                required_keys = self._required_keys_for(node, node_def)
                params, param_sources = self._resolve_params_with_sources(
                    node, node_def, pre_ctx_view, required_keys
                )
                pre_checks = self._build_pre_checks(
                    node, pre_ctx_view, data, required_keys
                ) + self._extra_pre_checks(node, pre_ctx_view, data, required_keys)

                collector = DeltaCollector(
                    enable_hash=bool(trace_opts.get("hash")),
                    enable_repr=bool(trace_opts.get("repr")),
                )
                hooks = SemantivaExecutor.SERHooks(
                    upstream=upstream_map.get(node_id, []),
                    trigger="dependency",
                    upstream_evidence=[
                        {"node_id": u, "state": "completed"}
                        for u in upstream_map.get(node_id, [])
                    ],
                    io_delta_provider=lambda: collector.compute(
                        pre_ctx=pre_ctx_view,
                        post_ctx=self._context_snapshot(context),
                        required_keys=required_keys,
                    ),
                    pre_checks=pre_checks,
                    post_checks_provider=_const_supplier([]),
                    env_pins_provider=_const_supplier(env_pins_static),
                    redaction_policy_provider=_const_supplier({}),
                )

                start_wall = start_cpu = 0.0
                start_iso = ""
                summaries: dict[str, dict[str, object]] = {}
                if trace_driver is not None:
                    start_wall, start_cpu, start_iso = self._start_timing()
                    summaries = self._init_summaries(data, pre_ctx_view, trace_opts)

                def node_callable() -> Payload:
                    return node.process(Payload(data, context))

                try:
                    result = self._submit_and_wait(node_callable, ser_hooks=hooks)
                    if not isinstance(result, Payload):
                        raise TypeError("Node execution must return a Payload instance")
                    payload = result
                    data, context = payload.data, payload.context
                    post_ctx_view = self._context_snapshot(context)
                    io_delta = self._ensure_io_delta(
                        hooks.io_delta_provider() if hooks.io_delta_provider else {}
                    )
                    post_checks = self._build_post_checks(
                        node, post_ctx_view, data, io_delta
                    ) + self._extra_post_checks(node, post_ctx_view, data, io_delta)
                    hooks.post_checks_provider = _const_supplier(post_checks)

                    if trace_driver is not None:
                        end_iso, duration_ms, cpu_ms = self._end_timing(
                            start_wall, start_cpu
                        )
                        summaries = self._augment_output_summaries(
                            summaries, data, post_ctx_view, trace_opts
                        )
                        ser = self._make_ser_record(
                            status="completed",
                            node=node,
                            node_id=node_id,
                            pipeline_id=pipeline_token,
                            run_id=run_token,
                            upstream_ids=upstream_map.get(node_id, []),
                            trigger=hooks.trigger,
                            upstream_evidence=hooks.upstream_evidence,
                            pre_checks=pre_checks,
                            post_checks=post_checks,
                            env_pins=env_pins_static,
                            io_delta=io_delta,
                            timing={
                                "start": start_iso,
                                "end": end_iso,
                                "duration_ms": duration_ms,
                                "cpu_ms": cpu_ms,
                            },
                            params=params,
                            param_sources=param_sources,
                            summaries=summaries,
                            error=None,
                        )
                        trace_driver.on_node_event(ser)

                except Exception as exc:
                    if trace_driver is not None:
                        post_ctx_view = self._context_snapshot(context)
                        io_delta = self._ensure_io_delta(
                            hooks.io_delta_provider() if hooks.io_delta_provider else {}
                        )
                        post_checks = (
                            [
                                {
                                    "code": type(exc).__name__,
                                    "result": "FAIL",
                                    "details": {"error": str(exc)},
                                }
                            ]
                            + self._build_post_checks(
                                node, post_ctx_view, data, io_delta
                            )
                            + self._extra_post_checks(
                                node, post_ctx_view, data, io_delta
                            )
                        )
                        hooks.post_checks_provider = _const_supplier(post_checks)
                        end_iso, duration_ms, cpu_ms = self._end_timing(
                            start_wall, start_cpu
                        )
                        summaries = self._augment_output_summaries(
                            summaries, data, post_ctx_view, trace_opts
                        )
                        ser = self._make_ser_record(
                            status="error",
                            node=node,
                            node_id=node_id,
                            pipeline_id=pipeline_token,
                            run_id=run_token,
                            upstream_ids=upstream_map.get(node_id, []),
                            trigger=hooks.trigger,
                            upstream_evidence=hooks.upstream_evidence,
                            pre_checks=pre_checks,
                            post_checks=post_checks,
                            env_pins=env_pins_static,
                            io_delta=io_delta,
                            timing={
                                "start": start_iso,
                                "end": end_iso,
                                "duration_ms": duration_ms,
                                "cpu_ms": cpu_ms,
                            },
                            params=params,
                            param_sources=param_sources,
                            summaries=summaries,
                            error={
                                "type": type(exc).__name__,
                                "message": str(exc),
                            },
                        )
                        trace_driver.on_node_event(ser)
                    raise

                self._publish(node, data, context, transport)

            if trace_driver is not None:
                trace_driver.on_pipeline_end(run_token, {"status": "ok"})
        except Exception as exc:
            if trace_driver is not None:
                trace_driver.on_pipeline_end(
                    run_token, {"status": "error", "error": str(exc)}
                )
            raise
        finally:
            self._current_run_metadata = None
            if trace_driver is not None:
                trace_driver.flush()
                trace_driver.close()

        return Payload(data, context)

    # ------------------------------------------------------------------
    # Abstract hooks for concrete orchestrators
    # ------------------------------------------------------------------
    @abstractmethod
    def _submit_and_wait(
        self,
        node_callable: Callable[[], Payload],
        *,
        ser_hooks: SemantivaExecutor.SERHooks,
    ) -> Payload:
        """Submit ``node_callable`` for execution and block until completion."""

    @abstractmethod
    def _publish(
        self,
        node: _PipelineNode,
        data: Any,
        context: Any,
        transport: SemantivaTransport,
    ) -> None:
        """Publish the node output through the orchestrator's transport."""

    # ------------------------------------------------------------------
    # Shared helper utilities
    # ------------------------------------------------------------------
    def _context_snapshot(self, ctx: Any) -> dict[str, Any]:
        if hasattr(ctx, "to_dict"):
            try:
                return dict(ctx.to_dict())
            except Exception:
                return {}
        if isinstance(ctx, dict):
            return dict(ctx)
        try:
            return dict(ctx)  # type: ignore[arg-type]
        except Exception:
            return {}

    def _normalize_keys(self, candidate: object) -> list[str]:
        if candidate is None:
            return []
        if isinstance(candidate, (list, tuple, set)):
            return [str(k) for k in candidate]
        return []

    def _processor_config_for(
        self, node: _PipelineNode, node_def: dict[str, Any] | None
    ) -> dict[str, Any]:
        cfg = getattr(node, "processor_config", None)
        if isinstance(cfg, dict):
            return cfg
        if node_def is None:
            return {}
        params = node_def.get("parameters")
        return params if isinstance(params, dict) else {}

    def _parameter_defaults(self, processor: Any) -> dict[str, Any]:
        metadata: dict[str, Any] = {}
        getter = getattr(processor, "get_metadata", None)
        if callable(getter):
            try:
                metadata = getter() or {}
            except Exception:
                metadata = {}
        params = metadata.get("parameters", {}) if isinstance(metadata, dict) else {}
        return params if isinstance(params, dict) else {}

    def _infer_context_parameters(
        self, node: _PipelineNode, node_def: dict[str, Any] | None
    ) -> list[str]:
        config = self._processor_config_for(node, node_def)
        param_getter = getattr(node.processor, "get_processing_parameter_names", None)
        try:
            param_names = list(param_getter()) if callable(param_getter) else []
        except Exception:
            param_names = []
        defaults_map = self._parameter_defaults(node.processor)
        required: list[str] = []
        for name in param_names:
            if name in config:
                continue
            info = defaults_map.get(name)
            has_default = False
            if isinstance(info, ParameterInfo):
                has_default = info.default is not _NO_DEFAULT
            elif isinstance(info, dict):
                has_default = info.get("default", _NO_DEFAULT) is not _NO_DEFAULT
            if has_default:
                continue
            required.append(str(name))
        return required

    def _required_keys_for(
        self, node: _PipelineNode, node_def: dict[str, Any] | None
    ) -> list[str]:
        keys: set[str] = set()
        for target in (
            node,
            node.__class__,
            node.processor,
            node.processor.__class__,
        ):
            func = getattr(target, "get_required_keys", None)
            if callable(func):
                try:
                    keys.update(self._normalize_keys(func()))
                except Exception:
                    continue
        attr = getattr(node, "input_context_keyword", None)
        if isinstance(attr, str):
            keys.add(attr)
        declared = getattr(node, "required_context_keys", None)
        keys.update(self._normalize_keys(declared))
        keys.update(self._infer_context_parameters(node, node_def))
        return sorted(keys)

    def _format_expected_type(self, expected: object) -> str:
        if expected is None:
            return "Any"
        if isinstance(expected, tuple):
            parts = [self._format_expected_type(part) for part in expected]
            return " | ".join(parts)
        if isinstance(expected, type):
            return expected.__name__
        return repr(expected)

    def _normalize_expected(self, expected: object) -> tuple[type, ...] | None:
        if expected is None:
            return None
        if isinstance(expected, tuple):
            types = tuple(t for t in expected if isinstance(t, type))
            return types or None
        if isinstance(expected, type):
            return (expected,)
        return None

    def _type_check_entry(
        self, code: str, expected: object, value: object
    ) -> dict[str, Any]:
        normalized = self._normalize_expected(expected)
        actual_type = type(value).__name__ if value is not None else "NoneType"
        result = "PASS"
        if normalized is not None:
            if not any(isinstance(value, t) for t in normalized):
                result = "FAIL"
        details = {
            "expected": self._format_expected_type(expected),
            "actual": actual_type,
        }
        return {"code": code, "result": result, "details": details}

    def _build_pre_checks(
        self,
        node: _PipelineNode,
        context_view: dict[str, Any],
        data: Any,
        required_keys: list[str],
    ) -> list[dict[str, Any]]:
        checks: list[dict[str, Any]] = []
        missing = [key for key in required_keys if key not in context_view]
        checks.append(
            {
                "code": "required_keys_present",
                "result": "PASS" if not missing else "FAIL",
                "details": {"expected": required_keys, "missing": missing},
            }
        )
        input_expected = getattr(node.processor, "input_data_type", lambda: None)()
        checks.append(self._type_check_entry("input_type_ok", input_expected, data))
        invalid = getattr(node, "invalid_parameters", None)
        if invalid is not None:
            invalid_serialized = serialize_json_safe(invalid)
            checks.append(
                {
                    "code": "config_valid",
                    "result": "PASS" if not invalid else "WARN",
                    "details": {"invalid": invalid_serialized},
                }
            )
        return checks

    def _extract_delta_lists(self, io_delta: Any) -> tuple[list[str], list[str]]:
        if isinstance(io_delta, IODelta):
            created = list(io_delta.created)
            updated = list(io_delta.updated)
        elif isinstance(io_delta, dict):
            created = list(io_delta.get("created", []))
            updated = list(io_delta.get("updated", []))
        else:
            created, updated = [], []
        return sorted(created), sorted(updated)

    def _build_post_checks(
        self,
        node: _PipelineNode,
        context_view: dict[str, Any],
        data: Any,
        io_delta: Any,
    ) -> list[dict[str, Any]]:
        checks: list[dict[str, Any]] = []
        output_expected = getattr(node.processor, "output_data_type", lambda: None)()
        checks.append(self._type_check_entry("output_type_ok", output_expected, data))
        created, updated = self._extract_delta_lists(io_delta)
        missing = [k for k in created + updated if k not in context_view]
        details = {"created": created, "updated": updated, "missing": missing}
        checks.append(
            {
                "code": "context_writes_realized",
                "result": "PASS" if not missing else "FAIL",
                "details": details,
            }
        )
        return checks

    def _resolve_params_with_sources(
        self,
        node: _PipelineNode,
        node_def: dict[str, Any] | None,
        ctx_view: dict[str, Any],
        required_keys: Iterable[str],
    ) -> tuple[dict[str, Any], dict[str, str]]:
        params_out: dict[str, Any] = {}
        source_out: dict[str, str] = {}
        declared = (node_def or {}).get("parameters", {}) or {}
        defaults = getattr(node.processor, "get_default_params", lambda: {})() or {}
        for k, v in declared.items():
            params_out[k] = serialize_json_safe(v)
            source_out[k] = "node"
        for k in required_keys:
            if k not in params_out and k in ctx_view:
                params_out[k] = serialize_json_safe(ctx_view[k])
                source_out[k] = "context"
        for k, v in defaults.items():
            if k not in params_out:
                params_out[k] = serialize_json_safe(v)
                source_out[k] = "default"
        return params_out, source_out

    def _extra_pre_checks(
        self,
        node: _PipelineNode,
        context_view: dict[str, Any],
        data: Any,
        required_keys: Iterable[str],
    ) -> list[dict[str, Any]]:
        return []

    def _extra_post_checks(
        self,
        node: _PipelineNode,
        context_view: dict[str, Any],
        data: Any,
        io_delta: Any,
    ) -> list[dict[str, Any]]:
        return []

    def _collect_env_pins(self) -> dict[str, str | None]:
        return _collect_env_pins_util()

    def _ensure_io_delta(self, delta: Any) -> IODelta:
        if isinstance(delta, IODelta):
            return delta
        if not isinstance(delta, dict):
            return IODelta(read=[], created=[], updated=[], summaries={})
        return IODelta(
            read=list(delta.get("read", [])),
            created=list(delta.get("created", [])),
            updated=list(delta.get("updated", [])),
            summaries=dict(delta.get("summaries", {})),
        )

    def _data_summary(self, data: Any, trace_opts: dict[str, Any]) -> dict[str, object]:
        if not trace_opts.get("hash") and not trace_opts.get("repr"):
            return {}
        summary: dict[str, object] = {"dtype": type(data).__name__}
        try:
            summary["rows"] = len(data)  # type: ignore[arg-type]
        except Exception:
            pass
        if trace_opts.get("hash"):
            try:
                summary["sha256"] = sha256_bytes(serialize(data))
            except Exception:
                pass
        if trace_opts.get("repr"):
            try:
                summary["repr"] = safe_repr(data)
            except Exception:
                pass
        return summary

    def _context_summary(
        self, context_view: dict[str, Any], trace_opts: dict[str, Any]
    ) -> dict[str, object]:
        summary: dict[str, object] = {}
        if trace_opts.get("hash"):
            try:
                summary["sha256"] = sha256_bytes(canonical_json_bytes(context_view))
            except Exception:
                pass
        if trace_opts.get("repr") and trace_opts.get("context"):
            try:
                summary["repr"] = context_to_kv_repr(context_view)
            except Exception:
                pass
        return summary

    def _init_summaries(
        self,
        data: Any,
        context_view: dict[str, Any],
        trace_opts: dict[str, Any],
    ) -> dict[str, dict[str, object]]:
        summaries: dict[str, dict[str, object]] = {}
        data_summary = self._data_summary(data, trace_opts)
        if data_summary:
            summaries["input_data"] = data_summary
        ctx_summary = self._context_summary(context_view, trace_opts)
        if ctx_summary:
            summaries["pre_context"] = ctx_summary
        return summaries

    def _augment_output_summaries(
        self,
        summaries: dict[str, dict[str, object]],
        data: Any,
        context_view: dict[str, Any],
        trace_opts: dict[str, Any],
    ) -> dict[str, dict[str, object]]:
        data_summary = self._data_summary(data, trace_opts)
        if data_summary:
            summaries["output_data"] = data_summary
        ctx_summary = self._context_summary(context_view, trace_opts)
        if ctx_summary:
            summaries["post_context"] = ctx_summary
        return summaries

    def _start_timing(self) -> tuple[float, float, str]:
        return time.time(), time.process_time(), self._iso_now()

    def _end_timing(self, start_wall: float, start_cpu: float) -> tuple[str, int, int]:
        end_iso = self._iso_now()
        duration_ms = int((time.time() - start_wall) * 1000)
        cpu_ms = int((time.process_time() - start_cpu) * 1000)
        return end_iso, duration_ms, cpu_ms

    def _iso_now(self) -> str:
        return datetime.now().isoformat(timespec="milliseconds") + "Z"

    def _instantiate_nodes(
        self, pipeline_spec: Sequence[dict[str, Any]], logger: Logger
    ) -> tuple[list[_PipelineNode], list[dict[str, Any]]]:
        nodes: list[_PipelineNode] = []
        node_defs: list[dict[str, Any]] = []
        for node_def in pipeline_spec:
            params = instantiate_from_descriptor(node_def.get("parameters", {}))
            nd = dict(node_def)
            nd["parameters"] = params
            node = _pipeline_node_factory(nd, logger)
            nodes.append(node)
            node_defs.append(nd)
        return nodes, node_defs

    def _make_ser_record(
        self,
        *,
        status: str,
        node: _PipelineNode,
        node_id: str,
        pipeline_id: str | None,
        run_id: str | None,
        upstream_ids: list[str],
        trigger: str,
        upstream_evidence: list[dict[str, Any]],
        pre_checks: list[dict[str, Any]],
        post_checks: list[dict[str, Any]],
        env_pins: dict[str, Any],
        io_delta: IODelta,
        timing: dict[str, Any],
        params: dict[str, Any],
        param_sources: dict[str, str],
        summaries: dict[str, dict[str, object]] | None,
        error: dict[str, Any] | None,
    ) -> SERRecord:
        if pipeline_id is None or run_id is None:
            raise RuntimeError("SER construction requires pipeline and run identifiers")
        why_run = {
            "trigger": trigger,
            "upstream_evidence": upstream_evidence,
            "pre": pre_checks,
            "policy": [],
        }
        args_payload: dict[str, Any] = {}
        if self._current_run_metadata:
            args_payload.update(self._current_run_metadata.get("args", {}))
        why_ok = {
            "post": post_checks,
            "invariants": [],
            "env": env_pins,
            "redaction": {},
            "args": args_payload,
        }
        return SERRecord(
            type="ser",
            schema_version=0,
            ids={"run_id": run_id, "pipeline_id": pipeline_id, "node_id": node_id},
            topology={"upstream": upstream_ids},
            action={
                "op_ref": node.processor.__class__.__name__,
                "params": params,
                "param_source": param_sources,
            },
            io_delta=io_delta,
            checks={"why_run": why_run, "why_ok": why_ok},
            timing=timing,
            status=status,  # type: ignore[arg-type]
            error=error,
            labels={"node_fqn": node.processor.__class__.__name__},
            summaries=summaries or None,
        )

    def _trace_options(self, trace: TraceDriver | None) -> dict[str, Any]:
        defaults = {"hash": False, "repr": False, "context": False}
        if trace is None:
            return defaults
        getter = getattr(trace, "get_options", None)
        if callable(getter):
            try:
                options = getter() or {}
            except Exception:
                options = {}
            if isinstance(options, dict):
                merged = dict(defaults)
                for key in defaults:
                    if key in options:
                        merged[key] = bool(options[key])
                for key, value in options.items():
                    if key not in merged:
                        merged[key] = value
                return merged
            return defaults
        merged = dict(defaults)
        merged["hash"] = True
        return merged


class LocalSemantivaOrchestrator(SemantivaOrchestrator):
    """Local orchestrator that delegates execution to a SemantivaExecutor.

    Behaviour is inherited from :class:`SemantivaOrchestrator`; see docs for
    full details and extension points.
    """

    def __init__(self, executor: Optional[SemantivaExecutor] = None) -> None:
        super().__init__()
        self.executor = executor or SequentialSemantivaExecutor()

    def _submit_and_wait(
        self,
        node_callable: Callable[[], Payload],
        *,
        ser_hooks: SemantivaExecutor.SERHooks,
    ) -> Payload:
        future = self.executor.submit(node_callable, ser_hooks=ser_hooks)
        return future.result()

    def _publish(
        self,
        node: _PipelineNode,
        data: Any,
        context: Any,
        transport: SemantivaTransport,
    ) -> None:
        transport.publish(
            channel=node.processor.semantic_id(), data=data, context=context
        )
