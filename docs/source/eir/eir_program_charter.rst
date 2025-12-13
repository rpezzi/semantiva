EIRv1 Program Charter
=====================

Purpose
-------

EIRv1 (Execution Intermediate Representation v1) is a **multi-epic implementation series**
that introduces a single compiled representation for Semantiva execution semantics.

This charter is a drift-resistant SSOT anchor. It documents only what is true on the
current branch and what is contractually frozen for the series.

SSOT artifacts (Phase 0)
------------------------

- Technical Design Report: ``TDR_EIRv1.md`` (project artifact; not part of the published docs tree)
- Work breakdown: ``EIRv1_work_breakdown_PoC.md`` (project artifact; not part of the published docs tree)
- Series status ledger (this repo): ``docs/source/eir/eir_series_status.yaml``
- Reference suite (this repo): ``tests/eir_reference_suite/*``

Phase 0 non-goals
-----------------

- No new payload forms (no Channel/LaneBundle).
- No EIR schema/compiler/runtime.
- No CLI changes.
- No changes to classic execution behavior.

Frozen contracts (Phase 0)
--------------------------

The following contracts MUST remain unchanged in Phase 0 epics:

- Classic pipeline/node execution remains **Payload -> Payload**.
- ``Payload(data, context)`` is unchanged.
- Existing tracing (SER v1) remains valid and schema-conformant.

Reference suite policy (non-drifting)
-------------------------------------

The float reference suite is the regression backbone for the entire EIRv1 series.

Rules:

1) Any change to ``tests/eir_reference_suite/*.yaml`` MUST be accompanied by a
   ledger update in ``docs/source/eir/eir_series_status.yaml``.
2) CI enforces drift detection via:
   - YAML ``sha256`` checks
   - deterministic ``pipeline_id`` checks (GraphV1 canonicalization)
3) If a future epic needs additional reference pipelines, it must add them
   **without rewriting** the meaning of existing references.

PoC execution strategy (future epics)
-------------------------------------

When a future epic introduces semantics that are not supported by the current
execution tooling (e.g., MultiChannel/Lane payload behavior), examples/tests MUST:

- Prefer existing Semantiva execution tooling when it supports the semantics cleanly, OR
- Use a **hard-coded PoC runner** that demonstrates the intended behavior explicitly,
  with comments indicating later generalization by the execution layer.

This keeps the series unblocked by tooling gaps while remaining honest about what is
implemented vs what is demonstrated.
