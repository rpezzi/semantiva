"""Small, pure helpers for EIRv1 channel algebra examples.

This module lives under ``examples/`` and provides a tiny "payload algebra"
vocabulary for working with :class:`~semantiva.data_types.MultiChannelDataType`.

Design notes:

* All transforms are eager and deterministic.
* Helpers return new :class:`~semantiva.Payload` objects (no in-place mutation).
* Any context merge policy used here is specific to these examples.
"""

from __future__ import annotations

from typing import Callable, Dict, Mapping

from semantiva import Payload
from semantiva.context_processors import ContextType
from semantiva.data_types import BaseDataType, MultiChannelDataType
from semantiva.examples.test_utils import FloatDataType

ChannelMap = Dict[str, BaseDataType]


def _require_multichannel(payload: Payload) -> MultiChannelDataType:
    """Validate that ``payload.data`` is multi-channel and return it.

    Role in the demos:
    This is the central guard that keeps example code short and makes failures
    explicit when a payload is accidentally in scalar form.

    Args:
        payload: The payload to validate.

    Returns:
        The payload's :class:`~semantiva.data_types.MultiChannelDataType`.

    Raises:
        TypeError: If ``payload.data`` is not a ``MultiChannelDataType``.
    """
    if not isinstance(payload.data, MultiChannelDataType):
        raise TypeError(
            "payload.data must be MultiChannelDataType for channel algebra demos"
        )
    return payload.data


def select(payload: Payload, channels: list[str]) -> Payload:
    """Project a multi-channel payload to a subset of channels.

    This is the channel-algebra analogue of a deterministic "projection".
    The output preserves the original context object.

    Args:
        payload: Input payload in channel form.
        channels: Ordered list of channel names to keep.

    Returns:
        A new payload whose data contains exactly the requested channels.

    Raises:
        TypeError: If ``payload.data`` is not a ``MultiChannelDataType``.
        KeyError: If any requested channel is missing.
    """
    data = _require_multichannel(payload)
    missing = [c for c in channels if c not in data.keys()]
    if missing:
        raise KeyError(f"Missing channels: {missing}")
    projected: ChannelMap = {c: data.get(c) for c in channels}
    return Payload(MultiChannelDataType(projected), payload.context)


def rename(payload: Payload, mapping: Mapping[str, str]) -> Payload:
    """Rename channels using an explicit old->new mapping.

    Channels not present in ``mapping`` are preserved.

    Args:
        payload: Input payload in channel form.
        mapping: Mapping from existing channel name to new channel name.

    Returns:
        A new payload with the renamed channel keys.

    Raises:
        TypeError: If ``payload.data`` is not a ``MultiChannelDataType``.
        ValueError: If two channels map to the same output name.
    """
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
    """Apply a pure, in-memory transform to a single channel value.

    Args:
        payload: Input payload in channel form.
        on: Channel name to transform.
        fn: Function mapping ``BaseDataType -> BaseDataType``.

    Returns:
        A new payload with the same channels, except ``on`` replaced by
        ``fn(original_value)``.

    Raises:
        TypeError: If ``payload.data`` is not a ``MultiChannelDataType``.
        KeyError: If ``on`` does not exist.
    """
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
    """Merge two multi-channel payloads into one, deterministically.

    Data merge policy:
        * If there are no overlapping channel names, channels are combined.
        * If channels overlap and ``on_conflict == "error"``, an error is raised.
        * If channels overlap and ``on_conflict == "namespace"``, both sides are
          preserved by prefixing channel names with ``left_ns`` and ``right_ns``.

    Context merge policy (demo-only):
        The returned context is the right context updated with the left context
        (i.e. left wins on collisions). This makes the examples deterministic and
        explicit but is *not* a normative Semantiva context policy.

    Args:
        left: Left input payload.
        right: Right input payload.
        on_conflict: Conflict resolution policy ("error" or "namespace").
        left_ns: Namespace prefix used when ``on_conflict == "namespace"``.
        right_ns: Namespace prefix used when ``on_conflict == "namespace"``.

    Returns:
        A new payload containing the merged channels and merged context.

    Raises:
        TypeError: If either payload is not in channel form.
        ValueError: If channels overlap and conflicts can't be resolved.
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
    """Normalize a float-only "uint8-like" value to 0..1.

    This is a demo helper used to make examples resemble typical image
    preprocessing (e.g. mapping uint8 pixels to float intensities) while
    remaining in the repository's float-only PoC constraints.

    Args:
        x: A :class:`~semantiva.examples.test_utils.FloatDataType` interpreted as
           a value in the range 0..255.

    Returns:
        A new ``FloatDataType`` with ``x / 255.0``.

    Raises:
        TypeError: If ``x`` is not a ``FloatDataType``.
    """
    if not isinstance(x, FloatDataType):
        raise TypeError("normalize expects FloatDataType (float-only program)")
    return FloatDataType(x.data / 255.0)


def align_to_ref(*, ref: FloatDataType, raw_signal: FloatDataType) -> FloatDataType:
    """Compute a deterministic "aligned" signal in reference space.

    Args:
        ref: Reference scalar.
        raw_signal: Raw scalar signal.

    Returns:
        A new scalar in "ref-space".
    """
    return FloatDataType(raw_signal.data * ref.data)


def derive_feature(*, aligned: FloatDataType) -> FloatDataType:
    """Derive a toy feature from an aligned scalar.

    The implementation is deliberately simple and deterministic.

    Args:
        aligned: The aligned signal.

    Returns:
        A derived feature value.
    """
    return FloatDataType(aligned.data * aligned.data)
