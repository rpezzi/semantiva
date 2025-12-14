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

"""EIR (Execution Intermediate Representation) package.

Phase 2 note:
- Contains compiler + schema for classic EIR artifacts (no runtime execution yet).
"""

from .slot_inference import SlotInference, infer_data_slots  # noqa: F401
from .compiler import compile_eir_v1  # noqa: F401
from .validation import validate_eir_v1  # noqa: F401
