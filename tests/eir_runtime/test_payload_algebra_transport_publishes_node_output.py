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

"""Integration test: transport publishes node output even when publish.out != primary."""

from __future__ import annotations

from pathlib import Path

import yaml

from semantiva.context_processors.context_types import ContextType
from semantiva.data_types import NoDataType
from semantiva.execution.orchestrator.orchestrator import LocalSemantivaOrchestrator
from semantiva.execution.transport.in_memory import InMemorySemantivaTransport
from semantiva.logger import Logger
from semantiva.pipeline.payload import Payload


def test_payload_algebra_transport_publishes_node_output_even_when_out_is_non_primary(
    tmp_path: Path,
) -> None:
    """
    When a node publishes to a non-primary channel (e.g. out -> addend),
    transport should receive the node's actual output data, not the primary channel value.

    This test validates the PA-03C-FIX requirement that orchestrator publishes
    `output_data` (node.process result) instead of post-publish primary channel.
    """
    spec = {
        "extensions": ["semantiva-examples"],
        "pipeline": {
            "nodes": [
                {
                    "processor": "FloatValueDataSource",
                    "data_key": "addend",  # maps to publish.channels.out = addend
                }
            ]
        },
    }
    spec_path = tmp_path / "one_node_out_addend.yaml"
    spec_path.write_text(yaml.safe_dump(spec), encoding="utf-8")

    transport = InMemorySemantivaTransport()
    payload = Payload(NoDataType(), ContextType({"value": 2.0}))

    LocalSemantivaOrchestrator().execute(
        pipeline_spec=str(spec_path),
        payload=payload,
        transport=transport,
        logger=Logger(),
        execution_backend="eir_payload_algebra",
    )

    # Drain all messages (one node => one publish).
    msgs = list(transport.subscribe("*"))
    assert len(msgs) == 1
    # FloatValueDataSource produces FloatDataType; check its .data attribute
    assert hasattr(msgs[0].data, "data"), "Expected FloatDataType with .data attribute"
    assert (
        msgs[0].data.data == 2.0
    ), "Transport should receive node output (FloatDataType.data=2.0)"
