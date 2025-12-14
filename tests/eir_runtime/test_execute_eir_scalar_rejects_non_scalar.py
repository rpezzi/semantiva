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

import pytest

from semantiva.eir.execution_scalar import EIRExecutionError, execute_eir_v1_scalar_plan
from semantiva.pipeline.payload import Payload
from semantiva.data_types import NoDataType
from semantiva.context_processors.context_types import ContextType


def test_rejects_non_scalar_forms() -> None:
    eir = {
        "eir_version": 1,
        "identity": {"pipeline_id": "x", "pipeline_variant_id": "y", "eir_id": "z"},
        "graph": {"graph_version": 1, "nodes": [], "edges": []},
        "parameters": {"objects": {}},
        "plan": {
            "plan_version": 1,
            "segments": [{"kind": "classic_linear", "node_order": ["n1"]}],
        },
        "semantics": {
            "payload_forms": {
                "version": 1,
                "root_form": "channel",
                "terminal_form": "channel",
                "node_io": {},
            }
        },
        "lineage": {},
    }
    with pytest.raises(EIRExecutionError):
        execute_eir_v1_scalar_plan(eir, Payload(NoDataType(), ContextType({})))
