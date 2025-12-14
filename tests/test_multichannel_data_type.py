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

import pytest

from semantiva.data_types import BaseDataType, MultiChannelDataType


class DummyData(BaseDataType[int]):
    """Simple data type for MultiChannelDataType tests."""

    def validate(self, data: int) -> bool:  # pragma: no cover - trivial
        if not isinstance(data, int):
            raise TypeError("Data must be an int")
        return True


def test_multichannel_validates_structure():
    first = DummyData(1)
    second = DummyData(2)
    mcdt = MultiChannelDataType({"a": first, "b": second})

    assert set(mcdt.keys()) == {"a", "b"}
    assert mcdt.get("a") is first


@pytest.mark.parametrize(
    "payload",
    [
        123,
        {"a": 1},
        {1: DummyData(1)},
    ],
)
def test_multichannel_rejects_invalid_payloads(payload):
    with pytest.raises(TypeError):
        MultiChannelDataType(payload)  # type: ignore[arg-type]


def test_multichannel_with_channel_returns_new_instance():
    first = DummyData(1)
    mcdt = MultiChannelDataType({"a": first})
    updated = mcdt.with_channel("b", DummyData(2))

    assert mcdt is not updated
    assert set(updated.keys()) == {"a", "b"}
    assert updated.get("a") is first
