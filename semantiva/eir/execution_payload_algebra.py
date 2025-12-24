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

"""
PA-03B: Plumbing-only module for the payload algebra execution backend.

This module intentionally contains *no runtime semantics* in PA-03B.
It exists as a stable import path for PA-03C.

IMPORTANT:
- Do not wire this module into runtime execution paths in PA-03B.
- execute_eir_payload_algebra remains a contract stub until PA-03C.
"""

# Import contract stubs for forward compatibility; runtime semantics land in PA-03C.
