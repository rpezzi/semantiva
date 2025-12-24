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

"""Unit tests for param_resolution_overlay context manager."""

from semantiva.context_processors.context_types import ContextType
from semantiva.pipeline._param_resolution import (
    param_resolution_overlay,
    resolve_runtime_value,
)


class _DummyProcessor:
    """Minimal processor for testing."""

    @staticmethod
    def get_metadata():
        return {"parameters": {}}


def test_param_resolution_overlay_wins_over_config_and_context():
    """Overlay should take precedence over config and context."""
    ctx = ContextType({"x": 3})
    config = {"x": 2}
    with param_resolution_overlay({"x": 1}):
        assert (
            resolve_runtime_value(
                name="x",
                processor_cls=_DummyProcessor,
                processor_config=config,
                context=ctx,
            )
            == 1
        )


def test_param_resolution_overlay_falls_back_to_config():
    """When overlay doesn't have a key, fallback to config."""
    ctx = ContextType({"x": 3})
    config = {"x": 2}
    with param_resolution_overlay({"y": 1}):
        assert (
            resolve_runtime_value(
                name="x",
                processor_cls=_DummyProcessor,
                processor_config=config,
                context=ctx,
            )
            == 2
        )


def test_param_resolution_overlay_falls_back_to_context():
    """When overlay and config don't have a key, fallback to context."""
    ctx = ContextType({"x": 3})
    config = {"y": 2}
    with param_resolution_overlay({"z": 1}):
        assert (
            resolve_runtime_value(
                name="x",
                processor_cls=_DummyProcessor,
                processor_config=config,
                context=ctx,
            )
            == 3
        )


def test_param_resolution_no_overlay_uses_config():
    """Without overlay, resolution follows config > context > default."""
    ctx = ContextType({"x": 3})
    config = {"x": 2}
    assert (
        resolve_runtime_value(
            name="x",
            processor_cls=_DummyProcessor,
            processor_config=config,
            context=ctx,
        )
        == 2
    )


def test_param_resolution_overlay_none_acts_as_no_overlay():
    """Passing None as overlay should act as if no overlay was set."""
    ctx = ContextType({"x": 3})
    config = {"x": 2}
    with param_resolution_overlay(None):
        assert (
            resolve_runtime_value(
                name="x",
                processor_cls=_DummyProcessor,
                processor_config=config,
                context=ctx,
            )
            == 2
        )
