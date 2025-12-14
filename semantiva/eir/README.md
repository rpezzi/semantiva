# semantiva.eir

Canonical namespace for the **EIRv1** implementation series.

## Phase 2 status (C0)

- EIRv1 JSON schema ships in `semantiva/eir/schema/eir_v1.schema.json`.
- Classic pipelines can be compiled to EIR via `semantiva.eir.compile_eir_v1(...)`.
- Identity rules:
  - `pipeline_id` is computed via GraphV1 `compute_pipeline_id(canonical_graph)`.
  - `eir_id` is deterministic and computed from a canonical subset of the EIR that
    **includes** `graph`, `parameters`, `plan`, `semantics`, `lineage`,
    and **excludes** ephemeral `build`/`source` metadata (timestamps, env, etc.).

## Not implemented yet

- Execution-from-EIR runtime.
- Channel/Lane compilation (Epic C1 and later).
- Plan execution segments beyond classic scalar.
