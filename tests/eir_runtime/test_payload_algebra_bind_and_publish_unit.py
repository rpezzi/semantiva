# Copyright 2025 Semantiva authors
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.

from __future__ import annotations

import pytest

from semantiva.eir.execution_payload_algebra import ProducerRef
from semantiva.eir.payload_algebra_contracts import parse_source_ref


def test_parse_source_ref_defaults_to_channel() -> None:
    ref = parse_source_ref("primary")
    assert ref.kind == "channel"
    assert ref.key == "primary"


@pytest.mark.parametrize(
    "raw,kind,key",
    [
        ("channel:primary", "channel", "primary"),
        ("context:other", "context", "other"),
        ("addend", "channel", "addend"),
    ],
)
def test_parse_source_ref_supported_forms(raw: str, kind: str, key: str) -> None:
    ref = parse_source_ref(raw)
    assert ref.kind == kind
    assert ref.key == key


def test_bind_precedence_and_ambiguity_rules() -> None:
    """
    Plan/§3.1 ambiguity rule: reject when bind conflicts with parameters or context.
    This is a unit test for the resolver helper that PA-03C introduces in
    semantiva/eir/execution_payload_algebra.py.

    NOTE: This test intentionally does NOT check SER provenance.
    """
    from semantiva.eir.execution_payload_algebra import (
        InMemoryChannelStore,
        PayloadAlgebraResolutionError,
        resolve_param_value,
    )

    channels = InMemoryChannelStore()
    channels.seed_primary("P")
    channels.set_entry(
        "addend",
        value="A",
        producer=ProducerRef(kind="node", node_uuid="source-1", output_slot="out"),
    )

    # data default: when not explicitly bound, must resolve from primary
    res = resolve_param_value(
        "data",
        binds={},
        node_params={},
        context={},
        channels=channels,
        default="D",
    )
    assert res.value == "P"
    assert res.source == "data"

    # explicit bind wins
    res2 = resolve_param_value(
        "other",
        binds={"other": "channel:addend"},
        node_params={},
        context={},
        channels=channels,
        default=None,
    )
    assert res2.value == "A"
    assert res2.source == "data"

    # ambiguity: bind + node params
    with pytest.raises(PayloadAlgebraResolutionError) as excinfo:
        resolve_param_value(
            "other",
            binds={"other": "channel:addend"},
            node_params={"other": "NODE"},
            context={},
            channels=channels,
            default=None,
        )
    assert "other" in str(excinfo.value).lower()

    # ambiguity: bind + context
    with pytest.raises(PayloadAlgebraResolutionError) as excinfo2:
        resolve_param_value(
            "other",
            binds={"other": "channel:addend"},
            node_params={},
            context={"other": "CTX"},
            channels=channels,
            default=None,
        )
    assert "other" in str(excinfo2.value).lower()


def test_publish_semantics_non_primary_does_not_clobber_primary() -> None:
    """
    Plan publish rule: non-primary publication must not clobber primary.
    """
    from semantiva.eir.execution_payload_algebra import (
        InMemoryChannelStore,
        PublishPlanV1,
    )

    channels = InMemoryChannelStore()
    channels.seed_primary("PRIMARY-OLD")

    node_spec = {
        # Minimal CPSV1-shaped fragment required by PublishPlanV1
        "publish": {"channels": {"out": "addend"}, "context_key": None}
    }
    plan = PublishPlanV1.from_cpsv1(node_spec)
    plan.apply(
        "OUT",
        channels,
    )
    channels.set_entry(
        "addend",
        value="OUT",
        producer=ProducerRef(kind="node", node_uuid="node-out", output_slot="out"),
    )

    assert channels.get("addend") == "OUT"
    assert channels.get("primary") == "PRIMARY-OLD"


def test_error_missing_channel_is_deterministic() -> None:
    from semantiva.eir.execution_payload_algebra import (
        InMemoryChannelStore,
        PayloadAlgebraResolutionError,
        resolve_param_value,
    )

    channels = InMemoryChannelStore()
    channels.seed_primary("P")

    with pytest.raises(PayloadAlgebraResolutionError) as excinfo:
        resolve_param_value(
            "other",
            binds={"other": "channel:does_not_exist"},
            node_params={},
            context={},
            channels=channels,
            default=None,
        )
    assert "does_not_exist" in str(excinfo.value)
