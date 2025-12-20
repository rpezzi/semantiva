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
from typing import Any


def stable_json_dumps_v1(obj: Any) -> str:
    """Return a deterministic, strict-JSON string representation.

    Contract (identity-safe):
      - Deterministic key ordering (sort_keys=True)
      - Compact separators (",", ":")
      - UTF-8 friendly (ensure_ascii=False)
      - Strict JSON floats (allow_nan=False): rejects NaN/Inf/-Inf
      - No fallback-to-repr: failures must raise to avoid silent instability.
    """
    return json.dumps(
        obj,
        sort_keys=True,
        separators=(",", ":"),
        ensure_ascii=False,
        allow_nan=False,
    )


def stable_json_bytes_v1(obj: Any) -> bytes:
    """Return UTF-8 encoded stable JSON bytes (see stable_json_dumps_v1)."""
    return stable_json_dumps_v1(obj).encode("utf-8")
