# tests/eir_reference_suite_python/float_ref_slice_01.py

from __future__ import annotations

from semantiva.data_processors.data_slicer_factory import slice as slice_processor
from semantiva.examples.test_utils import (
    FloatCollectionFromContextSource,
    FloatMultiplyOperation,
    FloatDataCollection,
    FloatCollectionSumOperation,
    FloatCollectValueProbe,
)


def build_pipeline_spec() -> list[dict]:
    return [
        {"processor": FloatCollectionFromContextSource},
        {"processor": slice_processor(FloatMultiplyOperation, FloatDataCollection)},
        {"processor": FloatCollectionSumOperation},
        {"processor": FloatCollectValueProbe, "context_key": "result"},
    ]
