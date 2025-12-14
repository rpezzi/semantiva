# semantiva.eir

Canonical namespace for the **EIRv1** implementation series.

## Phase 2 status (C0 + C1) — **No runtime behavior** is implemented here yet

- EIRv1 JSON schema ships in `semantiva/eir/schema/eir_v1.schema.json`.
- Classic pipelines can be compiled to EIR via `semantiva.eir.compile_eir_v1(...)`.
- C1 compiler emits deterministic compiled facts under `semantics`:
  - `semantics.payload_forms` (scalar/channel/lane_bundle per node)
  - `semantics.slots` (metadata-only slot candidates from `_process_logic` annotations)
- Runtime preflight helper: `semantiva.eir.validate_eir_v1(eir)` validates an EIR document
  against the packaged `eir_v1.schema.json` (useful for Phase 3 runtime epics).

## Not implemented yet

- Execution-from-EIR runtime.
- Channel/Lane compilation into executable plan segments.
- Lineage-aware runtime trace emission from EIR.

See:
- `docs/source/eir/eir_program_charter.rst`
- `docs/source/eir/eir_series_status.yaml`
- `TDR_EIRv1.md` (project artifact)

Note: `docs/source/eir/eir_series_status.yaml` is a historical per-epic snapshot ledger.
Do not interpret earlier epic checksums as "latest state" for the current branch.
