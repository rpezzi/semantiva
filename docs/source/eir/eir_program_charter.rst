EIRv1 Program Charter
=====================

Purpose
-------

EIRv1 (Execution Intermediate Representation v1) is a **multi-epic implementation series**
that introduces a single compiled representation for Semantiva execution semantics.

This charter is a drift-resistant SSOT anchor. It documents only what is true on the
current branch and what is contractually frozen for the series.

SSOT artifacts
--------------

- Series status ledger (this repo): ``docs/source/eir/eir_series_status.yaml``
- Reference suite (this repo): ``tests/eir_reference_suite/*``

Phase 0 non-goals
-----------------

- No new payload forms (no Channel/LaneBundle).
- No EIR schema/compiler/runtime.
- No CLI changes.
- No changes to classic execution behavior.

Frozen contracts
----------------

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

Phase 1 facts (as landed)
-------------------------

- Core ``MultiChannelDataType`` exists as a ``BaseDataType`` mapping
  ``channel_name -> BaseDataType``.
- The reference suite includes a non-drifting channel PoC pipeline,
  ``float_ref_channel_01`` (scalar -> channel -> scalar) executed under
  classic runtime tooling.
- No orchestrator or executor semantics changed; payload boundaries
  remain ``Payload -> Payload``.
- Core slot inference utility exists (metadata-only): it can extract data slot
  candidates from ``_process_logic`` type annotations (including multi-input
  signatures).
- The reference suite includes a non-drifting multi-input PoC pipeline,
  ``float_ref_slots_01`` (source -> add_two_inputs) executed under classic runtime
  tooling via context injection for the secondary ``FloatDataType``.
- Core ``LaneBundleDataType`` exists as a ``BaseDataType`` mapping
  ``lane_key -> BaseDataType`` (Phase 1 PoC data-model).
- The reference suite includes a non-drifting lane bundle PoC pipeline,
  ``float_ref_lane_01`` (lane_bundle -> lane_map -> merge -> select) executed
  under classic runtime tooling with no orchestrator/executor semantic changes.
- Safety rule (Phase 1): LaneBundle semantics are demonstrated in the payload
  data-plane first; lane elements are carried in ``payload.data`` and not stored
  as ``BaseDataType`` objects in context.

Phase 2 facts (as landed)
-------------------------

- EIRv1 schema skeleton exists: ``semantiva/eir/schema/eir_v1.schema.json``.
- Classic pipelines can be compiled to an EIRv1 document via ``semantiva.eir.compile_eir_v1``.
- The compiled EIR is schema-validated in CI.
- ``eir_id`` is deterministic across compiles and is computed from a canonical subset that:
  - includes: ``graph``, ``parameters``, ``plan``, ``semantics``, ``lineage``
  - excludes: ephemeral ``build``/``source`` metadata (timestamps, environment drift)
- No runtime execution semantics changed; pipeline execution remains **Payload -> Payload**.
- Phase 2 C1 extends compilation to emit deterministic compiled facts in ``semantics``:
  - payload form propagation per node (scalar/channel/lane_bundle)
  - metadata-only inferred slot candidates per node from ``_process_logic`` annotations
- ``eir_id`` is computed from a canonical subset that includes ``graph``, ``parameters``, ``plan``, ``semantics``, and ``lineage``,
  while excluding ephemeral ``build``/``source`` metadata (timestamps, environment drift).

Ledger semantics (historical snapshots)
---------------------------------------

``docs/source/eir/eir_series_status.yaml`` is a **historical ledger**.

- Each epic entry records the state *as landed for that epic* (checksums, compiled identities).
- These values are **not** a “latest state” assertion for the current branch across all time.
- Automated checks MUST select a specific epic snapshot when comparing checksums.
  For example, Phase 2 schema checksum comparisons should target the latest Phase 2 epic
  snapshot (currently C1), and MUST NOT assume older snapshot checksums match the current schema.


Phase 3 runtime mapping note: ``classic_linear``
------------------------------------------------

EIRv1 Phase 2 compilation emits a plan segment of kind ``classic_linear``:

- ``plan.segments[].kind == "classic_linear"``
- ``plan.segments[].node_order`` lists node ids in execution order.

**Mapping guidance for Phase 3 runtime epics (courtesy, not yet implemented):**

A Phase 3 execution-from-EIR runner may treat ``classic_linear`` as the
direct analogue of Semantiva's current classic pipeline execution:

1) Resolve each ``node_id`` in ``node_order`` to the corresponding entry in ``graph.nodes``.
2) Construct an equivalent classic runtime pipeline in that order (processor refs + parameters).
3) Execute sequentially under existing classic runtime semantics (**Payload -> Payload**),
   producing the same payload outputs as the corresponding YAML pipeline would.

This mapping provides a stable baseline for the first execution-from-EIR milestone while
preserving Phase 2's constraint: **no runtime behavior changes**.

Phase 3 runtime (R0a harness)
-----------------------------

Phase 3 (runtime): R0a introduces an internal harness that can execute a classic scalar pipeline
from an EIRv1 document (classic_linear only). This does not change default execution paths and
does not assert parity; it exists to prove EIR-backed execution is mechanically feasible.
