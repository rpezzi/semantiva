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
