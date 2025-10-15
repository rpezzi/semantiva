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

"""Trace model for Step Evidence Record (SER v1).

This module defines the dataclasses used by the tracing subsystem. The Step
Evidence Record (SER) is a single record emitted for each completed pipeline
step. Drivers switch on the ``record_type`` field and ignore unknown fields for
forward compatibility.
The description of SER fields and semantics lives in
docs/source/ser.rst.
"""

from __future__ import annotations

from dataclasses import dataclass
from typing import Any, Dict, List, Optional, Protocol, Literal

from semantiva.pipeline.payload import Payload


Result = Literal["PASS", "FAIL", "WARN"]


@dataclass
class Check:
    """Result of a single check performed for a node."""

    code: str
    result: Result
    details: Optional[Dict[str, Any]] = None


@dataclass
class UpstreamEvidence:
    """Evidence of an upstream node's state when determining why a node ran."""

    node_id: str
    state: str
    digest: Optional[str] = None


@dataclass
class ContextDelta:
    """Context delta for a node execution."""

    read_keys: List[str]
    created_keys: List[str]
    updated_keys: List[str]
    key_summaries: Dict[str, Dict[str, Any]]


@dataclass
class SERRecord:
    """Step Evidence Record emitted for each executed pipeline node."""

    record_type: Literal["ser"]
    schema_version: int
    identity: Dict[str, str]
    dependencies: Dict[str, List[str]]
    processor: Dict[str, Any]
    context_delta: ContextDelta
    assertions: Dict[str, Any]
    timing: Dict[str, Any]
    status: Literal["succeeded", "error", "skipped", "cancelled"]
    error: Optional[Dict[str, Any]] = None
    tags: Optional[Dict[str, Any]] = None
    summaries: Optional[Dict[str, Any]] = None


class TraceDriver(Protocol):
    """Driver API for Semantiva tracing using SER records."""

    def on_pipeline_start(
        self,
        pipeline_id: str,
        run_id: str,
        canonical_spec: dict,
        meta: dict,
        pipeline_input: Optional[Payload] = None,
    ) -> None:
        """Emit a ``pipeline_start`` record."""

    def on_node_event(self, event: SERRecord) -> None:
        """Emit a single SER record for a completed node."""

    def on_pipeline_end(self, run_id: str, summary: dict) -> None:
        """Emit a ``pipeline_end`` record summarising the run."""

    def flush(self) -> None:
        """Flush any internal buffers."""

    def close(self) -> None:
        """Release resources and close the driver."""
