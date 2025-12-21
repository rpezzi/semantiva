from __future__ import annotations

from dataclasses import dataclass, field
from typing import Any, Dict, List, Optional, Tuple


@dataclass(frozen=True)
class PreflightDiagnostic:
    """CanonicalPipelineSpecV1 pre-flight diagnostic.

    Design intent:
      - stable-coded diagnostics (SVA-style)
      - deterministic ordering
      - frontend-tagged (yaml/python) while remaining semantically identical across frontends
    """

    code: str
    severity: str
    message: str
    hint: str
    frontend_kind: str
    node_id: Optional[str]
    user_id: Optional[str]
    processor_fqcn: Optional[str]
    details: Dict[str, Any] = field(default_factory=dict)


def _is_fqcn(s: Any) -> bool:
    return isinstance(s, str) and "." in s and " " not in s and s.strip() == s


def validate_canonical_spec_v1(
    canonical_spec: Dict[str, Any], *, frontend_kind: str
) -> List[PreflightDiagnostic]:
    """Validate a CanonicalPipelineSpecV1 (GraphV1 mapping) and return diagnostics.

    The validator is deterministic and does not depend on registry state,
    extension loading, or runtime instantiation.
    """

    fk = frontend_kind if frontend_kind in {"yaml", "python"} else "unknown"
    diags: List[PreflightDiagnostic] = []

    version = canonical_spec.get("version")
    nodes = canonical_spec.get("nodes")
    edges = canonical_spec.get("edges")

    if version != 1:
        diags.append(
            PreflightDiagnostic(
                code="SVA400",
                severity="error",
                message=f"CanonicalSpec version must be 1 (got {version!r}).",
                hint="Rebuild the CanonicalSpec using build_canonical_spec() from the current Semantiva version.",
                frontend_kind=fk,
                node_id=None,
                user_id=None,
                processor_fqcn=None,
                details={"got": version},
            )
        )

    if not isinstance(nodes, list):
        diags.append(
            PreflightDiagnostic(
                code="SVA401",
                severity="error",
                message="CanonicalSpec.nodes must be a list.",
                hint="Ensure CanonicalSpec is a GraphV1 mapping with 'nodes: [...]'.",
                frontend_kind=fk,
                node_id=None,
                user_id=None,
                processor_fqcn=None,
                details={"got_type": type(nodes).__name__},
            )
        )
        nodes = []

    if not isinstance(edges, list):
        diags.append(
            PreflightDiagnostic(
                code="SVA402",
                severity="error",
                message="CanonicalSpec.edges must be a list.",
                hint="Ensure CanonicalSpec is a GraphV1 mapping with 'edges: [...]'.",
                frontend_kind=fk,
                node_id=None,
                user_id=None,
                processor_fqcn=None,
                details={"got_type": type(edges).__name__},
            )
        )
        edges = []

    node_ids: List[str] = []
    for i, n in enumerate(nodes):
        if not isinstance(n, dict):
            diags.append(
                PreflightDiagnostic(
                    code="SVA410",
                    severity="error",
                    message=f"Node {i} must be a mapping.",
                    hint="Ensure each node in CanonicalSpec.nodes is a dict with node_uuid/processor_ref.",
                    frontend_kind=fk,
                    node_id=None,
                    user_id=None,
                    processor_fqcn=None,
                    details={"index": i, "got_type": type(n).__name__},
                )
            )
            continue

        node_uuid = n.get("node_uuid")
        proc_ref = n.get("processor_ref")

        if not isinstance(node_uuid, str) or not node_uuid:
            diags.append(
                PreflightDiagnostic(
                    code="SVA411",
                    severity="error",
                    message=f"Node {i} missing required node_uuid.",
                    hint="Rebuild CanonicalSpec via build_canonical_spec(); do not hand-edit node_uuid.",
                    frontend_kind=fk,
                    node_id=None,
                    user_id=None,
                    processor_fqcn=_safe_str(proc_ref),
                    details={"index": i},
                )
            )
        else:
            node_ids.append(node_uuid)

        if proc_ref is None:
            diags.append(
                PreflightDiagnostic(
                    code="SVA412",
                    severity="error",
                    message=f"Node {i} missing required processor_ref.",
                    hint="Ensure node config specifies processor/processor_ref so canonicalization can derive processor_ref.",
                    frontend_kind=fk,
                    node_id=node_uuid if isinstance(node_uuid, str) else None,
                    user_id=None,
                    processor_fqcn=None,
                    details={"index": i},
                )
            )
        elif not _is_fqcn(proc_ref):
            diags.append(
                PreflightDiagnostic(
                    code="SVA413",
                    severity="error",
                    message=f"Node {i} processor_ref must be a dotted FQCN (got {proc_ref!r}).",
                    hint="Use '<module>.<qualname>' processor_ref (or specify 'processor' and let canonicalization normalize it).",
                    frontend_kind=fk,
                    node_id=node_uuid if isinstance(node_uuid, str) else None,
                    user_id=None,
                    processor_fqcn=_safe_str(proc_ref),
                    details={"index": i, "got": proc_ref},
                )
            )

    known = set(node_ids)
    for e_i, e in enumerate(edges):
        if not isinstance(e, dict):
            diags.append(
                PreflightDiagnostic(
                    code="SVA420",
                    severity="error",
                    message=f"Edge {e_i} must be a mapping.",
                    hint="Ensure each edge is a dict with 'source' and 'target' node_uuid strings.",
                    frontend_kind=fk,
                    node_id=None,
                    user_id=None,
                    processor_fqcn=None,
                    details={"index": e_i, "got_type": type(e).__name__},
                )
            )
            continue

        src = e.get("source")
        tgt = e.get("target")
        if not isinstance(src, str) or not isinstance(tgt, str):
            diags.append(
                PreflightDiagnostic(
                    code="SVA421",
                    severity="error",
                    message=f"Edge {e_i} must have string source/target.",
                    hint="Ensure edge source/target are node_uuid strings from CanonicalSpec.nodes.",
                    frontend_kind=fk,
                    node_id=None,
                    user_id=None,
                    processor_fqcn=None,
                    details={"index": e_i, "source": src, "target": tgt},
                )
            )
            continue

        if src == tgt:
            diags.append(
                PreflightDiagnostic(
                    code="SVA422",
                    severity="error",
                    message=f"Edge {e_i} must not be a self-loop (source == target).",
                    hint="Fix pipeline wiring so a node does not depend on itself.",
                    frontend_kind=fk,
                    node_id=src,
                    user_id=None,
                    processor_fqcn=None,
                    details={"index": e_i, "node_id": src},
                )
            )

        if src not in known or tgt not in known:
            diags.append(
                PreflightDiagnostic(
                    code="SVA423",
                    severity="error",
                    message=f"Edge {e_i} refers to unknown node_uuid(s).",
                    hint="Rebuild CanonicalSpec from a valid pipeline spec; do not hand-edit edges.",
                    frontend_kind=fk,
                    node_id=None,
                    user_id=None,
                    processor_fqcn=None,
                    details={"index": e_i, "source": src, "target": tgt},
                )
            )

    def _key(d: PreflightDiagnostic) -> Tuple[str, str, str]:
        return (d.code, d.node_id or "", d.message)

    return sorted(diags, key=_key)


def _safe_str(v: Any) -> Optional[str]:
    if v is None:
        return None
    return str(v)
