"""EIR (Execution Intermediate Representation) package.

Phase 2 note:
- Contains compiler + schema for classic EIR artifacts (no runtime execution yet).
"""

from .slot_inference import SlotInference, infer_data_slots  # noqa: F401
from .compiler import compile_eir_v1  # noqa: F401
