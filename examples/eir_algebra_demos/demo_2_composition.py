from __future__ import annotations

from typing import cast

from semantiva import Payload
from semantiva.context_processors import ContextType
from semantiva.data_types import MultiChannelDataType
from semantiva.examples.test_utils import FloatDataType

from examples.eir_algebra_demos.algebra_demos import merge, select


def run() -> dict:
    A = Payload(
        MultiChannelDataType({"ref": FloatDataType(1.0), "feat_a": FloatDataType(2.0)}),
        ContextType({"source": "A"}),
    )
    B = Payload(
        MultiChannelDataType({"ref": FloatDataType(1.0), "feat_b": FloatDataType(3.0)}),
        ContextType({"source": "B"}),
    )

    # Explicit, deterministic collision handling: namespace both sides.
    C = merge(A, B, on_conflict="namespace", left_ns="A", right_ns="B")

    # Optional projection (keeps the story tight and deterministic)
    C2 = select(C, ["A.ref", "A.feat_a", "B.ref", "B.feat_b"])

    out = cast(MultiChannelDataType, C2.data)
    return {
        "channels": sorted(out.keys()),
        "A.ref": out.get("A.ref").data,
        "A.feat_a": out.get("A.feat_a").data,
        "B.ref": out.get("B.ref").data,
        "B.feat_b": out.get("B.feat_b").data,
        "context": C2.context.to_dict(),
    }


if __name__ == "__main__":
    print(run())
