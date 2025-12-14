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
