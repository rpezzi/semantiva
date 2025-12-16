# tests/eir_reference_suite_python/float_ref_sweep_01.py

from __future__ import annotations


def build_pipeline_spec() -> list[dict]:
    return [
        {"processor": "FloatValueDataSource"},
        {
            "processor": "FloatAddOperation",
            "derive": {
                "parameter_sweep": {
                    "parameters": {"addend": "t"},
                    "variables": {"t": [1.0, 2.0, 3.0]},
                    "collection": "FloatDataCollection",
                    "mode": "combinatorial",
                }
            },
        },
        {"processor": "FloatCollectionSumOperation"},
        {"processor": "FloatCollectValueProbe", "context_key": "result"},
    ]
