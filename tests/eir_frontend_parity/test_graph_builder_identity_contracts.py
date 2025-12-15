from __future__ import annotations

import uuid
from pathlib import Path

import yaml

from semantiva.pipeline import graph_builder
from semantiva.registry.processor_registry import ProcessorRegistry


def test_node_namespace_frozen() -> None:
    assert graph_builder._NODE_NAMESPACE == uuid.UUID(
        "00000000-0000-0000-0000-000000000000"
    )


def test_class_processor_canonicalizes_to_short_name_when_unambiguous() -> None:
    class LocalProc:  # local-only sentinel
        pass

    # No registry candidates recorded => treated as not ambiguous => short name.
    canon = graph_builder._canonical_node({"processor": LocalProc, "parameters": {}})
    assert canon["processor_ref"] == "LocalProc"


def test_ambiguous_short_name_falls_back_to_fqcn() -> None:
    # Create two distinct classes with different FQCNs and register both under same short name.
    A = type("AmbigProc", (), {"__module__": "m1"})
    B = type("AmbigProc", (), {"__module__": "m2"})
    ProcessorRegistry.register_processor("AmbigProc", A)
    ProcessorRegistry.register_processor("AmbigProc", B)
    canon = graph_builder._canonical_node({"processor": A, "parameters": {}})
    assert canon["processor_ref"] == "m1.AmbigProc"


def test_canonicalization_does_not_load_yaml_extensions(tmp_path: Path) -> None:
    # Drift protection: graph_builder must not try to load extensions during canonicalization.
    doc = {
        "extensions": ["does-not-exist-extension-xyz"],
        "pipeline": {"nodes": [{"processor": "SomeProcessor"}]},
    }
    p = tmp_path / "no_side_effects.yaml"
    p.write_text(yaml.safe_dump(doc), encoding="utf-8")

    canon, _ = graph_builder.build_canonical_spec(str(p))

    assert canon["version"] == 1
    assert len(canon["nodes"]) == 1
