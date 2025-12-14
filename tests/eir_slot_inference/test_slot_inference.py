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

from semantiva.eir.slot_inference import infer_data_slots
from semantiva.examples.test_utils import FloatAddTwoInputsOperation, FloatDataType


def test_slot_inference_two_inputs_and_return() -> None:
    spec = infer_data_slots(FloatAddTwoInputsOperation)

    assert list(spec.inputs.keys()) == ["data", "other"]
    assert spec.inputs["data"] is FloatDataType
    assert spec.inputs["other"] is FloatDataType
    assert spec.output is FloatDataType


def test_slot_inference_is_deterministic() -> None:
    first = infer_data_slots(FloatAddTwoInputsOperation).to_dict()
    for _ in range(10):
        assert infer_data_slots(FloatAddTwoInputsOperation).to_dict() == first
