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

"""Tests for Python spec fingerprinting behavior."""

from __future__ import annotations

from semantiva.eir.compiler import _fingerprint_source
from semantiva.examples.test_utils import FloatAddOperation, FloatValueDataSource


def test_fingerprint_source_python_spec_normalizes_and_is_deterministic() -> None:
    spec = [
        {"processor": FloatValueDataSource},
        {"processor": FloatAddOperation},
    ]
    string_spec = [
        {"processor": "semantiva.examples.test_utils.FloatValueDataSource"},
        {"processor": "semantiva.examples.test_utils.FloatAddOperation"},
    ]

    first = _fingerprint_source(spec)
    second = _fingerprint_source(spec)
    assert first == second
    assert first == _fingerprint_source(string_spec)


def test_fingerprint_source_fallback_is_different_for_different_specs() -> None:
    spec1 = [
        {
            "processor": FloatValueDataSource,
            "parameters": {"bad": object()},
        }
    ]
    spec2 = [
        {
            "processor": FloatValueDataSource,
            "parameters": {"bad": object(), "extra": 1},
        }
    ]

    fingerprint1 = _fingerprint_source(spec1)
    fingerprint2 = _fingerprint_source(spec2)

    assert fingerprint1[0] == "node_list_lossy"
    assert fingerprint2[0] == "node_list_lossy"
    assert fingerprint1[1] != fingerprint2[1]
