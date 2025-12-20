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

import math

import pytest

from semantiva.pipeline.cpsv1.stable_json import stable_json_bytes_v1


def test_stable_json_bytes_v1_is_deterministic_under_key_ordering() -> None:
    a = {"b": 2, "a": 1, "c": {"y": 2, "x": 1}}
    b = {"c": {"x": 1, "y": 2}, "a": 1, "b": 2}  # different insertion order

    assert stable_json_bytes_v1(a) == stable_json_bytes_v1(b)


def test_stable_json_bytes_v1_rejects_nan_inf() -> None:
    with pytest.raises(ValueError):
        stable_json_bytes_v1({"x": float("nan")})

    with pytest.raises(ValueError):
        stable_json_bytes_v1({"x": float("inf")})

    with pytest.raises(ValueError):
        stable_json_bytes_v1({"x": -float("inf")})

    # sanity: normal floats are allowed
    assert stable_json_bytes_v1({"x": math.pi}).startswith(b"{")
