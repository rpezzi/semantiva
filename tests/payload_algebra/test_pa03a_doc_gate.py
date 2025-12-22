from __future__ import annotations

from pathlib import Path

REPO_ROOT = Path(__file__).parents[2]


def test_pa03a_design_ssot_doc_exists_and_has_minimum_required_markers() -> None:
    doc = REPO_ROOT / "docs" / "dev" / "pa-03" / "payload_algebra_design_ssot.md"
    assert (
        doc.exists()
    ), "PA-03A doc gate: missing docs/dev/pa-03/payload_algebra_design_ssot.md"

    text = doc.read_text(encoding="utf-8")

    # Plan-mandated minimum contents (token-based to avoid brittle formatting coupling)
    assert "execute_eir_payload_algebra" in text
    assert "ChannelStore" in text
    assert "SourceRef" in text
    assert "parse_source_ref" in text
    assert "BindResolver.resolve_param" in text
    assert "PublishPlan" in text
    assert "parameter_sources" in text

    # Cross-epic doc gate contribution: a stable reference block for future epics
    assert "Reference block for future PA-03* epics" in text

    # Integration points must be explicitly named (plan requirement)
    assert "TraceDriver" in text
    assert "JsonlTraceDriver" in text
    assert "Orchestrator" in text
    assert "Executors" in text or "executor" in text.lower()
    assert "transport" in text.lower()
