from __future__ import annotations

from typing import Callable, Dict, Mapping

from semantiva import Payload
from semantiva.context_processors import ContextType
from semantiva.data_types import BaseDataType, MultiChannelDataType
from semantiva.examples.test_utils import FloatDataType

ChannelMap = Dict[str, BaseDataType]


def _require_multichannel(payload: Payload) -> MultiChannelDataType:
    if not isinstance(payload.data, MultiChannelDataType):
        raise TypeError(
            "payload.data must be MultiChannelDataType for channel algebra demos"
        )
    return payload.data


def select(payload: Payload, channels: list[str]) -> Payload:
    """Project a payload to a subset of channels (deterministic, eager)."""
    data = _require_multichannel(payload)
    missing = [c for c in channels if c not in data.keys()]
    if missing:
        raise KeyError(f"Missing channels: {missing}")
    projected: ChannelMap = {c: data.get(c) for c in channels}
    return Payload(MultiChannelDataType(projected), payload.context)


def rename(payload: Payload, mapping: Mapping[str, str]) -> Payload:
    """Rename channels with explicit mapping; collisions are an error."""
    data = _require_multichannel(payload)
    out: ChannelMap = {}
    for k in data.keys():
        new_k = mapping.get(k, k)
        if new_k in out:
            raise ValueError(f"rename collision: '{new_k}' already exists")
        out[new_k] = data.get(k)
    return Payload(MultiChannelDataType(out), payload.context)


def map_channel(
    payload: Payload, on: str, fn: Callable[[BaseDataType], BaseDataType]
) -> Payload:
    """Apply a pure transform to one channel value (eager)."""
    data = _require_multichannel(payload)
    if on not in data.keys():
        raise KeyError(f"Missing channel: {on}")
    out: ChannelMap = {k: data.get(k) for k in data.keys()}
    out[on] = fn(out[on])
    return Payload(MultiChannelDataType(out), payload.context)


def merge(
    left: Payload,
    right: Payload,
    *,
    on_conflict: str = "error",
    left_ns: str = "A",
    right_ns: str = "B",
) -> Payload:
    """
    Merge two MultiChannel payloads deterministically.

    on_conflict:
      - "error": raise if any key overlaps
      - "namespace": keep both by prefixing with "{left_ns}." and "{right_ns}."
    """
    l_chan = _require_multichannel(left)
    r_chan = _require_multichannel(right)

    # Context merge policy for demos: left context wins on key collisions (deterministic).
    merged_ctx = right.context.to_dict()
    merged_ctx.update(left.context.to_dict())

    lk = set(l_chan.keys())
    rk = set(r_chan.keys())
    overlap = lk.intersection(rk)

    out: ChannelMap = {}

    if overlap and on_conflict == "error":
        raise ValueError(f"merge conflict on channels: {sorted(overlap)}")

    if on_conflict == "namespace":
        for k in sorted(l_chan.keys()):
            out[f"{left_ns}.{k}"] = l_chan.get(k)
        for k in sorted(r_chan.keys()):
            out[f"{right_ns}.{k}"] = r_chan.get(k)
        return Payload(MultiChannelDataType(out), ContextType(merged_ctx))

    if not overlap:
        for k in sorted(l_chan.keys()):
            out[k] = l_chan.get(k)
        for k in sorted(r_chan.keys()):
            out[k] = r_chan.get(k)
        return Payload(MultiChannelDataType(out), ContextType(merged_ctx))

    raise ValueError(f"Unknown on_conflict policy: {on_conflict}")


# --- Small float-only helpers used by demos ---


def normalize_uint8_like_to_float(x: BaseDataType) -> BaseDataType:
    """
    Float-only demo normalization: interpret x as 0..255 float and map to 0..1 float.
    """
    if not isinstance(x, FloatDataType):
        raise TypeError("normalize expects FloatDataType (float-only program)")
    return FloatDataType(x.data / 255.0)


def align_to_ref(*, ref: FloatDataType, raw_signal: FloatDataType) -> FloatDataType:
    """Pure in-memory alignment: define raw_signal in ref-space deterministically."""
    return FloatDataType(raw_signal.data * ref.data)


def derive_feature(*, aligned: FloatDataType) -> FloatDataType:
    """Pure in-memory feature derivation (toy, deterministic)."""
    return FloatDataType(aligned.data * aligned.data)
