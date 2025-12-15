from __future__ import annotations

import uuid
from pathlib import Path

import pytest
import yaml

from semantiva.pipeline import graph_builder
from semantiva.registry.processor_registry import ProcessorRegistry
from semantiva.registry.resolve import AmbiguousProcessorError


def test_node_namespace_frozen() -> None:
    assert graph_builder._NODE_NAMESPACE == uuid.UUID(
        "00000000-0000-0000-0000-000000000000"
    )


def test_class_processor_canonicalizes_to_fqcn() -> None:
    from semantiva.examples.test_utils import FloatAddOperation

    node = graph_builder._canonical_node(
        {"processor": FloatAddOperation, "parameters": {}},
        declaration_index=0,
        declaration_subindex=0,
    )
    assert (
        node["processor_ref"]
        == f"{FloatAddOperation.__module__}.{FloatAddOperation.__qualname__}"
    )


@pytest.mark.usefixtures("isolated_processor_registry")
def test_mutual_exclusion_processor_and_processor_ref_errors() -> None:
    from semantiva.examples.test_utils import FloatAddOperation

    with pytest.raises(ValueError):
        graph_builder._canonical_node(
            {
                "processor": FloatAddOperation,
                "processor_ref": f"{FloatAddOperation.__module__}.{FloatAddOperation.__qualname__}",
            },
            declaration_index=0,
            declaration_subindex=0,
        )


@pytest.mark.usefixtures("isolated_processor_registry")
def test_ambiguous_short_name_errors_deterministically() -> None:
    # Create two distinct classes with different FQCNs and register both under same short name.
    A = type("AmbigProc", (), {"__module__": "m1"})
    B = type("AmbigProc", (), {"__module__": "m2"})
    ProcessorRegistry.register_processor("AmbigProc", A)
    ProcessorRegistry.register_processor("AmbigProc", B)

    with pytest.raises(AmbiguousProcessorError) as e:
        graph_builder._canonical_node(
            {"processor": "AmbigProc", "parameters": {}},
            declaration_index=0,
            declaration_subindex=0,
        )
    msg = str(e.value)
    assert "Ambiguous" in msg
    assert "processor_ref" in msg or "explicit" in msg


@pytest.mark.usefixtures("isolated_processor_registry")
def test_canonicalization_does_not_load_yaml_extensions(
    monkeypatch: pytest.MonkeyPatch, tmp_path: Path
) -> None:
    # Drift protection: graph_builder must not try to load extensions during canonicalization.
    import semantiva.registry.plugin_registry as pr

    monkeypatch.setattr(
        pr,
        "load_extensions",
        lambda *a, **k: (_ for _ in ()).throw(
            AssertionError("extension loading forbidden during canonicalization")
        ),
    )

    # Pre-register FloatAddOperation so string resolution works without extension loading.
    from semantiva.examples.test_utils import FloatAddOperation
    from semantiva.registry.processor_registry import ProcessorRegistry

    ProcessorRegistry.register_processor("FloatAddOperation", FloatAddOperation)

    doc = {
        "extensions": ["does-not-exist-extension-xyz"],
        "pipeline": {"nodes": [{"processor": "FloatAddOperation"}]},
    }
    p = tmp_path / "no_side_effects.yaml"
    p.write_text(yaml.safe_dump(doc), encoding="utf-8")

    canon, _ = graph_builder.build_canonical_spec(str(p))

    assert canon["version"] == 1
    assert len(canon["nodes"]) == 1
    assert "processor_ref" in canon["nodes"][0]


def test_derived_meaning_is_base_ref_plus_derive_payload() -> None:
    from semantiva.examples.test_utils import FloatAddOperation

    derive_payload = {"parameter_sweep": {"addend": [1.0, 2.0]}}
    node = graph_builder._canonical_node(
        {"processor": FloatAddOperation, "parameters": {}, "derive": derive_payload},
        declaration_index=0,
        declaration_subindex=0,
    )

    assert (
        node["processor_ref"]
        == f"{FloatAddOperation.__module__}.{FloatAddOperation.__qualname__}"
    )
    assert node["derive"] == derive_payload
