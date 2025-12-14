from __future__ import annotations

from pathlib import Path

from semantiva.eir import compile_eir_v1

REPO_ROOT = Path(__file__).parents[2]
SUITE = REPO_ROOT / "tests" / "eir_reference_suite"


def _order(eir: dict) -> list[str]:
    return list(eir["plan"]["segments"][0]["node_order"])


def test_channel_pipeline_emits_channel_transition() -> None:
    eir = compile_eir_v1(str(SUITE / "float_ref_channel_01.yaml"))
    order = _order(eir)
    node_io = eir["semantics"]["payload_forms"]["node_io"]

    assert node_io[order[2]]["output_form"] == "channel"
    assert node_io[order[3]]["input_form"] == "channel"
    assert eir["semantics"]["payload_forms"]["terminal_form"] == "scalar"


def test_lane_pipeline_emits_lane_bundle_and_merge_transition() -> None:
    eir = compile_eir_v1(str(SUITE / "float_ref_lane_01.yaml"))
    order = _order(eir)
    node_io = eir["semantics"]["payload_forms"]["node_io"]

    assert node_io[order[0]]["output_form"] == "lane_bundle"
    assert node_io[order[2]]["input_form"] == "lane_bundle"
    assert node_io[order[2]]["output_form"] == "channel"
    assert node_io[order[3]]["input_form"] == "channel"
    assert eir["semantics"]["payload_forms"]["terminal_form"] == "scalar"


def test_multi_input_operation_emits_inferred_slots() -> None:
    eir = compile_eir_v1(str(SUITE / "float_ref_slots_01.yaml"))
    order = _order(eir)
    node_slots = eir["semantics"]["slots"]["node_slots"]

    inferred = node_slots[order[1]]["inferred_slots"]
    names = [s["name"] for s in inferred["inputs"]]
    assert "data" in names
    assert "other" in names
