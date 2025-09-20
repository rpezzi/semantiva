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

"""Tests for the stringbuild context processor factory."""

import pytest

from semantiva.context_processors.context_observer import _ValidatingContextObserver
from semantiva.context_processors.context_types import ContextType
from semantiva.registry import ClassRegistry


ClassRegistry.initialize_default_modules()


def test_stringbuild_factory_contract_and_execution():
    cls = ClassRegistry.get_class('stringbuild:"exp_{subject}_{run}.png":filename')

    assert cls.get_processing_parameter_names() == ["subject", "run"]
    assert cls.get_created_keys() == ["filename"]
    assert cls.get_suppressed_keys() == []
    assert cls.context_keys() == ["filename"]

    processor = cls()
    context = ContextType({"subject": "mouse01", "run": 3})
    observer = _ValidatingContextObserver(
        context_keys=cls.get_created_keys(),
        suppressed_keys=cls.get_suppressed_keys(),
        logger=None,
    )
    observer.observer_context = context
    processor._set_context_observer(observer)
    processor._process_logic(subject="mouse01", run=3)

    assert context.get_value("filename") == "exp_mouse01_3.png"


def test_stringbuild_missing_key_raises_keyerror():
    cls = ClassRegistry.get_class('stringbuild:"exp_{s}_{r}.png":filename')
    processor = cls()
    context = ContextType({"s": "m1"})
    observer = _ValidatingContextObserver(
        context_keys=cls.get_created_keys(),
        suppressed_keys=cls.get_suppressed_keys(),
        logger=None,
    )
    observer.observer_context = context
    processor._set_context_observer(observer)

    with pytest.raises(KeyError):
        processor._process_logic(s="m1")


@pytest.mark.parametrize(
    "spec",
    [
        'stringbuild:"exp_{value:.2f}.png":filename',
        'stringbuild:"exp_{1invalid}.png":filename',
        'stringbuild:"exp_static.png":filename',
    ],
)
def test_stringbuild_invalid_templates(spec: str) -> None:
    with pytest.raises(ValueError):
        ClassRegistry.get_class(spec)
