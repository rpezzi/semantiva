from __future__ import annotations

from typing import cast

from semantiva import Payload
from semantiva.context_processors import ContextType
from semantiva.data_types import MultiChannelDataType
from semantiva.examples.test_utils import FloatDataType

# from examples.eir_algebra_demos.algebra import (
from examples.eir_algebra_demos.algebra_demos import (
    map_channel,
    normalize_uint8_like_to_float,
    rename,
    select,
)


def run() -> dict:
    # 1) Start P with {img, mask, float_ref_channel_01} (float-only stand-ins)
    P = Payload(
        MultiChannelDataType(
            {
                "img": FloatDataType(128.0),  # “uint8-like” float
                "mask": FloatDataType(1.0),
                "float_ref_channel_01": FloatDataType(0.25),
            }
        ),
        ContextType({}),
    )

    # 2) Project
    P1 = select(P, ["img", "mask", "float_ref_channel_01"])

    # 3) Transform one channel value
    P2 = map_channel(P1, on="img", fn=normalize_uint8_like_to_float)

    # 4) Rename ref channel
    P3 = rename(P2, {"float_ref_channel_01": "ref"})

    out = cast(MultiChannelDataType, P3.data)
    return {
        "channels": sorted(out.keys()),
        "img": out.get("img").data,
        "mask": out.get("mask").data,
        "ref": out.get("ref").data,
    }


if __name__ == "__main__":
    print(run())
