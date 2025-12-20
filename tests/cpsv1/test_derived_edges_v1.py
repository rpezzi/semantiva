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
from pathlib import Path
from typing import Dict

import jsonschema
import pytest

from semantiva.exceptions.pipeline_exceptions import (
    PipelineConfigurationError,
    PipelineTopologyError,
)
from semantiva.pipeline.cpsv1.derived_edges import derive_edges_v1
from semantiva.pipeline.cpsv1.validation import (
    load_canonical_pipeline_spec_v1_schema,
    validate_canonical_pipeline_spec_v1,
)

# Deterministic UUID fixtures (schema-truthful)
NODE_A = "11111111-1111-1111-1111-111111111111"
NODE_B = "22222222-2222-2222-2222-222222222222"
NODE_C = "33333333-3333-3333-3333-333333333333"


def _node(
    node_uuid: str,
    *,
    bind: Dict[str, object],
    publish_channels: Dict[str, str],
    declaration_index: int,
) -> Dict[str, object]:
    return {
        "node_uuid": node_uuid,
        "role": "processor",
        "processor_ref": f"example.{node_uuid}",
        "parameters": {},
        "ports": {},
        "derive": None,
        "bind": bind,
        "publish": {"channels": publish_channels, "context_key": None},
        "declaration_index": declaration_index,
        "declaration_subindex": 0,
    }


def _validate_edges_against_schema(edges: list[dict]) -> None:
    schema = load_canonical_pipeline_spec_v1_schema()
    # Build a self-contained schema that carries the CPSV1 $defs so $ref resolution works
    edge_schema = {"$ref": "#/$defs/DerivedEdgeV1", "$defs": schema["$defs"]}
    v = jsonschema.Draft202012Validator(edge_schema)
    for e in edges:
        v.validate(e)


def test_derive_edges_v1_matches_golden_file_and_is_schema_truthful() -> None:
    # NOTE: This fixture is deliberately CPSV1-schema-valid (UUID node_uuid + bind.data everywhere).
    cpsv1 = {
        "version": 1,
        "nodes": [
            _node(
                NODE_A,
                bind={"data": "channel:primary"},
                publish_channels={"metrics": "metrics", "out": "primary"},
                declaration_index=0,
            ),
            _node(
                NODE_B,
                bind={
                    "data": "channel:primary",
                    "ignored_context": "context:foo",
                    "metrics_in": "channel:metrics",
                },
                publish_channels={"out": "primary", "side": "side"},
                declaration_index=1,
            ),
            _node(
                NODE_C,
                # bind.data is required in canonical CPSV1
                bind={
                    "alpha": "channel:side",
                    "beta": "channel:primary",
                    "data": "channel:primary",
                },
                publish_channels={"out": "primary"},
                declaration_index=2,
            ),
        ],
    }

    # CPSV1 schema validity guard (locks drift)
    validate_canonical_pipeline_spec_v1(cpsv1)

    edges = derive_edges_v1(cpsv1)

    # Derived edge shape guard (locks drift against $defs.DerivedEdgeV1)
    _validate_edges_against_schema(edges)

    golden_path = Path(__file__).with_name("golden_derived_edges_v1.json")
    golden = json.loads(golden_path.read_text(encoding="utf-8"))

    assert edges == golden


def test_derive_edges_v1_rejects_missing_producer() -> None:
    cpsv1 = {
        "version": 1,
        "nodes": [
            _node(
                NODE_A,
                bind={"data": "channel:missing"},
                publish_channels={"out": "primary"},
                declaration_index=0,
            ),
        ],
    }

    with pytest.raises(PipelineTopologyError):
        derive_edges_v1(cpsv1)


def test_derive_edges_v1_rejects_multiple_writers() -> None:
    cpsv1 = {
        "version": 1,
        "nodes": [
            _node(
                NODE_A,
                bind={"data": "channel:primary"},
                publish_channels={"secondary": "alt", "out": "primary"},
                declaration_index=0,
            ),
            _node(
                NODE_B,
                bind={"data": "channel:primary"},
                publish_channels={"duplicate": "alt", "out": "primary"},
                declaration_index=1,
            ),
        ],
    }

    with pytest.raises(PipelineTopologyError):
        derive_edges_v1(cpsv1)


def test_derive_edges_v1_rejects_consumer_preceding_producer() -> None:
    cpsv1 = {
        "version": 1,
        "nodes": [
            _node(
                NODE_A,
                bind={"data": "channel:primary", "loop": "channel:loop"},
                publish_channels={"loop": "loop", "out": "primary"},
                declaration_index=0,
            ),
        ],
    }

    with pytest.raises(PipelineTopologyError):
        derive_edges_v1(cpsv1)


def test_derive_edges_v1_rejects_non_string_source_ref() -> None:
    cpsv1 = {
        "version": 1,
        "nodes": [
            _node(
                NODE_A,
                bind={"data": 123},
                publish_channels={"out": "primary"},
                declaration_index=0,
            ),
        ],
    }

    with pytest.raises(PipelineConfigurationError):
        derive_edges_v1(cpsv1)


@pytest.mark.parametrize("bad_nodes", [None, "not-a-list", 123])
def test_derive_edges_v1_rejects_non_list_nodes(bad_nodes: object) -> None:
    with pytest.raises(PipelineConfigurationError):
        derive_edges_v1({"nodes": bad_nodes})


@pytest.mark.parametrize("bad_bind", [None, "not-a-dict", []])
def test_derive_edges_v1_rejects_malformed_bind(bad_bind: object) -> None:
    cpsv1: Dict[str, object] = {
        "version": 1,
        "nodes": [
            _node(
                NODE_A,
                bind={},
                publish_channels={"out": "primary"},
                declaration_index=0,
            ),
        ],
    }
    cpsv1_nodes = cpsv1["nodes"]
    assert isinstance(cpsv1_nodes, list)
    cpsv1_node = cpsv1_nodes[0]
    assert isinstance(cpsv1_node, dict)
    cpsv1_node["bind"] = bad_bind

    with pytest.raises(PipelineConfigurationError):
        derive_edges_v1(cpsv1)


@pytest.mark.parametrize("bad_channels", [None, "not-a-dict", []])
def test_derive_edges_v1_rejects_malformed_publish_channels(
    bad_channels: object,
) -> None:
    cpsv1: Dict[str, object] = {
        "version": 1,
        "nodes": [
            _node(
                NODE_A,
                bind={"data": "channel:primary"},
                publish_channels={"out": "primary"},
                declaration_index=0,
            ),
        ],
    }
    cpsv1_nodes = cpsv1["nodes"]
    assert isinstance(cpsv1_nodes, list)
    cpsv1_node = cpsv1_nodes[0]
    assert isinstance(cpsv1_node, dict)
    cpsv1_publish = cpsv1_node["publish"]
    assert isinstance(cpsv1_publish, dict)
    cpsv1_publish["channels"] = bad_channels

    with pytest.raises(PipelineConfigurationError):
        derive_edges_v1(cpsv1)
