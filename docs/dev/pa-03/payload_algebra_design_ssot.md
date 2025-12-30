# Payload Algebra Design SSOT (PA-03A → PA-03D)

**Status:** SSOT (repo-visible). This file freezes PA‑03 interfaces + invariants for payload algebra execution and SER correction.  
**Scope:** Payload algebra execution backend + SER correction (PA‑03D)

**Drift rule:** Any change that affects meaning/identity/provenance must be treated as a deliberate decision with explicit acceptance tests. Do not “fill gaps” ad-hoc during implementation.

---

## 1) Non-goals (PA‑03 series)

- No new identity axes (no `pipeline_variant_id`, no `eir_id`, no `execution_id`).
- No bypass around CPSV1. YAML/Python authoring must converge through CPSV1 before EIR execution semantics are defined.
- No multi-output runtime commitment in PA‑03D.
- No lane semantics (planned-next only).
- No transport changes. SemantivaTransport channels are pub/sub topics and are **not** payload-algebra data channels.

---

## 2) SEAMS_INVENTORY: preserved public signatures

These are “no breaking changes” contracts for the PA‑03 implementation series.

### A) Preserved public signatures (verbatim intent)

1. Trace boundary (TraceDriver protocol)
- `TraceDriver.on_pipeline_start(...)`
- `TraceDriver.on_node_event(event: SERRecord) -> None`
- `TraceDriver.on_pipeline_end(...)`

2. Orchestrator public surface
- `SemantivaOrchestrator.execute(self, pipeline_spec: dict, payload: Payload, *, transport: Optional[SemantivaTransport] = None, logger: Optional[logging.Logger] = None, trace_driver: Optional[TraceDriver] = None, ...) -> Payload`
  - PA‑03 may extend `execution_backend` literals additively only.

3. Executor substrate
- `SemantivaExecutor.submit(self, fn: Callable[..., Any], *args: Any, ser_hooks: Optional[SERHooks] = None, **kwargs: Any) -> Future[Any]`

4. Transport substrate (explicitly *not* payload channels)
- `SemantivaTransport.publish(self, channel: str, data: Any, context: ContextType, metadata: Optional[dict] = None, require_ack: bool = False) -> Optional[Future]`

### B) Lifecycle invariants (must remain true under payload-algebra backends)

- Single owner invariant: Orchestrator remains the owner of TraceDriver lifecycle calls and SER emission (no parallel tracer inside payload algebra runtime).
- Ordering invariant: pipeline_start exactly once → node_event exactly once per node execution → pipeline_end exactly once.
- Identity invariant: `pipeline_id`/`run_id` used in start/end must match across the run; `pipeline_spec_canonical` is the meaning substrate the trace commits to.
- Channel disambiguation invariant: payload-algebra channel store is internal execution state; SemantivaTransport channels are pub/sub topics; no implicit bridging.

---

## 3) Normative semantics that PA‑03D MUST implement (do not reinterpret)

### 3.1 CPSV1 explicit defaults
Canonical CPSV1 must be explicit:
- every node has `publish.channels.out` (default `"primary"` injected if omitted)
- every node has `bind.data` (default `"channel:primary"` injected if omitted)

ContextProcessor nodes are data pass-through at the node boundary under default-flow continuity and therefore still participate in the defaults above.

### 3.2 SER value-origin provenance
SER MUST report value origin for each resolved parameter using **only**:
- `context | data | node | default`

Bind is configuration, not provenance. SER must never report `bind` as a provenance category.

SER MUST provide structured refs for context/data origins (`parameter_source_refs`) including effective key/channel and producer identity.

### 3.3 Pass-through nodes must not corrupt producer attribution
Pass-through nodes include at minimum:
- ContextProcessor nodes
- DataProbe nodes (pass-through by contract: SVA320/SVA321)
- DataSink nodes (pass-through by contract: SVA310/SVA311) — **confirmed: sinks must remain pass-through**

Producer carry-forward rule: if a node forwards a data value unchanged, it MUST NOT become the data producer for provenance purposes; the producer identity must carry forward unchanged.

---

## 4) Scope freeze decisions for PA‑03D (explicit)

These are required to avoid scope explosion while we fix SER and upstream dependencies.

1) **`emit` handling (freeze; enforcement):** until a component-level multi-output contract exists, authored `emit` MUST be rejected deterministically (hard error) by CPSV1 canonicalization and/or payload-algebra execution preflight.  
Rationale: without a multi-output contract, publication/provenance/validation cannot be guaranteed.

2) **Single-output assumption (temporary):** PA‑03D assumes only the primary output slot `out` is published/consumed at runtime.

3) **Channel overwrite constraints (must be enforced):**
- non-`primary` channels are single-writer (deterministic compile-time or preflight runtime error)
- `primary` overwrite is allowed only by explicit publish to `primary` (default flow), with deterministic “current value” semantics

---

## 5) PA‑03D acceptance targets (minimum)

PA‑03D is “done enough” when:

1) SER schema supports:
- `parameter_sources` including `data`
- `parameter_source_refs` for `context` and `data`

2) Runtime+SER correctness:
- provenance is **value-origin** (implicit vs explicit bind produces same provenance)
- pass-through nodes do not hijack producer attribution (ContextProcessor, DataProbe, DataSink)
- multi-input nodes correctly populate `dependencies.upstream` (multiple producers possible)
- summaries reflect actual node inputs/outputs including non-primary publications

3) Drift resistance gate:
- schema validation + integration tests + golden provenance expectations fail deterministically on semantic drift.

---

## 6) Signature ledger (machine-checkable; used by PA-03A signature gate)

<!-- PA-03A-SIGNATURE-LEDGER-START -->
```yaml
module: "semantiva.eir.payload_algebra_contracts"

signatures:
  - qualname: "execute_eir_payload_algebra"
    params:
      positional_or_keyword: ["eir", "payload"]
      keyword_only: ["trace_hook"]
      var_positional: null
      var_keyword: null

  - qualname: "parse_source_ref"
    params:
      positional_or_keyword: ["raw"]
      keyword_only: []
      var_positional: null
      var_keyword: null

  - qualname: "ChannelStore.get"
    params:
      positional_or_keyword: ["self", "name"]
      keyword_only: []
      var_positional: null
      var_keyword: null

  - qualname: "ChannelStore.set"
    params:
      positional_or_keyword: ["self", "name", "value"]
      keyword_only: []
      var_positional: null
      var_keyword: null

  - qualname: "ChannelStore.seed_primary"
    params:
      positional_or_keyword: ["self", "value"]
      keyword_only: []
      var_positional: null
      var_keyword: null

  - qualname: "BindResolver.resolve_param"
    params:
      positional_or_keyword: ["self", "param_name"]
      keyword_only: ["binds", "node_params", "context", "channels", "default"]
      var_positional: null
      var_keyword: null

  - qualname: "PublishPlan.from_cpsv1"
    params:
      positional_or_keyword: ["cls", "node_spec"]
      keyword_only: []
      var_positional: null
      var_keyword: null

  - qualname: "PublishPlan.apply"
    params:
      positional_or_keyword: ["self", "output_value", "channels"]
      keyword_only: []
      var_positional: null
      var_keyword: null
```
<!-- PA-03A-SIGNATURE-LEDGER-END -->