import pytest

from semantiva.data_types import LaneBundleDataType, MultiChannelDataType
from semantiva.examples.test_utils import (
    FloatDataType,
    FloatLaneMapAddOperation,
    LaneMergeToMultiChannelOperation,
)


def test_lane_bundle_datatype_validation_rejects_non_mapping() -> None:
    with pytest.raises(TypeError):
        LaneBundleDataType(data=[("a", FloatDataType(1.0))])  # type: ignore[arg-type]


def test_lane_map_add_applies_to_each_lane() -> None:
    lb = LaneBundleDataType({"a": FloatDataType(1.0), "b": FloatDataType(2.0)})
    out = FloatLaneMapAddOperation().process(lb, addend=0.5)
    assert isinstance(out, LaneBundleDataType)
    assert float(out.get("a").data) == 1.5
    assert float(out.get("b").data) == 2.5


def test_lane_merge_to_multichannel_uses_lane_keys_as_channels() -> None:
    lb = LaneBundleDataType({"a": FloatDataType(1.0), "b": FloatDataType(2.0)})
    out = LaneMergeToMultiChannelOperation().process(lb, prefix="")
    assert isinstance(out, MultiChannelDataType)
    assert isinstance(out.get("a"), FloatDataType)
    assert isinstance(out.get("b"), FloatDataType)
