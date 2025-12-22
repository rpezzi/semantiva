from __future__ import annotations

import inspect
from pathlib import Path
from typing import Any, Mapping

import yaml

from semantiva.eir import payload_algebra_contracts as pa

REPO_ROOT = Path(__file__).parents[2]
DOC = REPO_ROOT / "docs" / "dev" / "pa-03" / "payload_algebra_design_ssot.md"

START = "<!-- PA-03A-SIGNATURE-LEDGER-START -->"
END = "<!-- PA-03A-SIGNATURE-LEDGER-END -->"


def _extract_sig_parts(obj: Any) -> dict[str, Any]:
    sig = inspect.signature(obj)
    pos = []
    kwonly = []
    var_pos = None
    var_kw = None
    for name, p in sig.parameters.items():
        if p.kind in (
            inspect.Parameter.POSITIONAL_ONLY,
            inspect.Parameter.POSITIONAL_OR_KEYWORD,
        ):
            pos.append(name)
        elif p.kind is inspect.Parameter.KEYWORD_ONLY:
            kwonly.append(name)
        elif p.kind is inspect.Parameter.VAR_POSITIONAL:
            var_pos = name
        elif p.kind is inspect.Parameter.VAR_KEYWORD:
            var_kw = name
    return {
        "positional_or_keyword": pos,
        "keyword_only": kwonly,
        "var_positional": var_pos,
        "var_keyword": var_kw,
    }


def _load_embedded_ledger() -> Mapping[str, Any]:
    assert DOC.exists(), "PA-03A signature gate: SSOT doc missing"
    text = DOC.read_text(encoding="utf-8")

    i = text.find(START)
    j = text.find(END)
    assert (
        i != -1 and j != -1 and j > i
    ), "PA-03A signature gate: ledger markers missing or malformed"

    block = text[i:j]
    # Find the fenced yaml content inside the marker range.
    fence = "```yaml"
    k = block.find(fence)
    assert k != -1, "PA-03A signature gate: missing ```yaml fence inside ledger block"
    k2 = block.find("```", k + len(fence))
    assert k2 != -1, "PA-03A signature gate: missing closing ``` fence for yaml block"
    yaml_text = block[k + len(fence) : k2].strip()

    doc = yaml.safe_load(yaml_text)
    assert doc["module"] == "semantiva.eir.payload_algebra_contracts"
    return doc


def test_pa03a_new_stubs_match_ssot_doc_signatures() -> None:
    ledger = _load_embedded_ledger()

    qmap = {
        "execute_eir_payload_algebra": pa.execute_eir_payload_algebra,
        "parse_source_ref": pa.parse_source_ref,
        "ChannelStore.get": pa.ChannelStore.get,
        "ChannelStore.set": pa.ChannelStore.set,
        "ChannelStore.seed_primary": pa.ChannelStore.seed_primary,
        "BindResolver.resolve_param": pa.BindResolver.resolve_param,
        "PublishPlan.from_cpsv1": pa.PublishPlan.from_cpsv1.__func__,  # type: ignore[attr-defined]
        "PublishPlan.apply": pa.PublishPlan.apply,
    }

    for entry in ledger["signatures"]:
        qual = entry["qualname"]
        want = entry["params"]
        got = _extract_sig_parts(qmap[qual])
        assert got == want, f"Signature mismatch for {qual}\nwant={want}\n got={got}"
