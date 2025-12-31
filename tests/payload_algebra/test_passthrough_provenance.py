# Copyright 2025 Semantiva authors
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.

"""PA-03D: Pass-through producer carry-forward tests (ADR-0004 §5)."""

from __future__ import annotations

from semantiva.eir.execution_payload_algebra import (
    InMemoryChannelStore,
    ProducerRef,
)


class TestPassthroughCarryForward:
    """Verify pass-through nodes do not hijack producer attribution."""

    def test_carry_forward_preserves_producer(self) -> None:
        """When carry_forward_from is used with same value, producer is preserved."""
        channels = InMemoryChannelStore()
        original_producer = ProducerRef(
            kind="node", node_uuid="source-123", output_slot="out"
        )

        channels.set_entry("primary", value=42, producer=original_producer)
        channels.set_entry(
            "primary",
            value=42,
            producer=ProducerRef(kind="node", node_uuid="passthrough"),
            carry_forward_from="primary",
        )

        entry = channels.get_entry("primary")
        assert entry is not None
        assert entry.producer == original_producer

    def test_new_value_updates_producer(self) -> None:
        """When value changes, producer is updated to new node."""
        channels = InMemoryChannelStore()
        original_producer = ProducerRef(
            kind="node", node_uuid="source-123", output_slot="out"
        )
        new_producer = ProducerRef(
            kind="node", node_uuid="transform-456", output_slot="out"
        )

        channels.set_entry("primary", value=42, producer=original_producer)
        channels.set_entry("primary", value=84, producer=new_producer)

        entry = channels.get_entry("primary")
        assert entry is not None
        assert entry.producer == new_producer

    def test_pipeline_input_producer(self) -> None:
        """Pipeline input data has correct producer identity."""
        channels = InMemoryChannelStore()
        channels.seed_primary(value=100)

        entry = channels.get_entry("primary")
        assert entry is not None
        assert entry.producer.kind == "pipeline_input_data"
        assert entry.producer.node_uuid is None
