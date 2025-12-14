from __future__ import annotations

from typing import cast

from semantiva import Payload
from semantiva.context_processors import ContextType
from semantiva.data_types import MultiChannelDataType
from semantiva.examples.test_utils import FloatDataType

from examples.eir_algebra_demos.algebra_demos import (
    align_to_ref,
    derive_feature,
    rename,
    select,
)


def run() -> dict:
    P = Payload(
        MultiChannelDataType(
            {
                "float_ref_channel_01": FloatDataType(0.5),
                "raw_signal": FloatDataType(4.0),
            }
        ),
        ContextType({}),
    )

    projected = select(P, ["float_ref_channel_01", "raw_signal"])
    proj_data = cast(MultiChannelDataType, projected.data)
    ref = proj_data.get("float_ref_channel_01")
    raw = proj_data.get("raw_signal")
    assert isinstance(ref, FloatDataType)
    assert isinstance(raw, FloatDataType)

    aligned = align_to_ref(ref=ref, raw_signal=raw)
    feat = derive_feature(aligned=aligned)

    out = Payload(
        MultiChannelDataType(
            {
                "float_ref_channel_01": ref,
                "feat": feat,
            }
        ),
        projected.context,
    )
    out = rename(out, {"float_ref_channel_01": "ref"})

    data = cast(MultiChannelDataType, out.data)
    return {
        "channels": sorted(data.keys()),
        "ref": data.get("ref").data,
        "feat": data.get("feat").data,
    }


if __name__ == "__main__":
    print(run())
