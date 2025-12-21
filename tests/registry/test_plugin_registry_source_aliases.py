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

from semantiva.registry import plugin_registry
from semantiva.registry.plugin_registry import load_extensions


@pytest.mark.no_auto_examples
def test_load_extensions_can_resolve_examples_extension_name_from_source_tree() -> None:
    """Verify alias-based loading of semantiva-examples from source tree.

    This test ensures that the built-in extension name 'semantiva-examples'
    can be resolved even when entry points are not installed, preventing
    Gate 4 regressions in CLI tests.
    """
    # Ensure this test is meaningful even if other tests ran before it.
    plugin_registry._LOADED_EXTENSIONS.discard("semantiva-examples")

    load_extensions(["semantiva-examples"])
    assert "semantiva-examples" in plugin_registry._LOADED_EXTENSIONS
