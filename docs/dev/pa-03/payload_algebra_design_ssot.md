# Payload Algebra Design SSOT (PA-03A)

Status: Frozen by PA-03A (architecture-only; no runtime implementation here)

## Reference block for future PA-03* epics (copy/paste)
This epic freezes contracts in:
- docs/dev/pa-03/payload_algebra_design_ssot.md

Future PA-03* epics MUST reference this file as their contract SSOT.

## Non-goals (PA-03A)
- No runtime execution semantics (no channel store implementation; no bind/publish runtime).
- No schema changes.
- No CLI changes.
- Orchestrator remains the owner of trace lifecycle + SER emission; no parallel tracing.


## SEAMS_INVENTORY: preserved_signatures

A) Preserved public signatures (verbatim, as they exist in the repo snapshot)

1. Trace boundary (TraceDriver protocol)
  * `TraceDriver.on_pipeline_start(pipeline_id: str, run_id: str, pipeline_spec_canonical: dict, meta: dict, pipeline_input: Optional[dict] = None, pipeline_context: Optional[dict] = None, detail: Optional[dict] = None) -> None`
  * `TraceDriver.on_node_event(event: SERRecord) -> None`
  * `TraceDriver.on_pipeline_end(pipeline_id: str, run_id: str, status: Literal["success","error"], meta: dict, duration_s: Optional[float] = None, error: Optional[dict] = None, pipeline_output: Optional[dict] = None, pipeline_context: Optional[dict] = None, detail: Optional[dict] = None) -> None`

2. Orchestrator public surface
  * `SemantivaOrchestrator.execute(self, pipeline_spec: dict, payload: Payload, *, transport: Optional[SemantivaTransport] = None, logger: Optional[logging.Logger] = None, trace_driver: Optional[TraceDriver] = None, trace_detail: Optional[TraceDetail] = None, trace_meta: Optional[dict] = None, run_id: Optional[str] = None, execution_backend: Literal["legacy","eir_scalar"] = "legacy") -> Payload`
  (Important: PA-03A freezes this as “no breaking changes”; PA-03B may extend the Literal additively.)

3. Executor substrate
  * `SemantivaExecutor.submit(self, fn: Callable[..., Any], *args: Any, ser_hooks: Optional[SERHooks] = None, **kwargs: Any) -> Future[Any]`

4. Transport substrate (explicitly *not* payload channels)
  * `SemantivaTransport.publish(self, channel: str, data: Any, context: ContextType, metadata: Optional[dict] = None, require_ack: bool = False) -> Optional[Future]`

B) Lifecycle invariants (must remain true under payload-algebra backends)
These are declarative rules, not code.

* Single owner invariant: Orchestrator remains the owner of TraceDriver lifecycle calls and SER emission (no “parallel tracer” in the payload algebra module).
* Ordering invariant: pipeline_start exactly once → node_event exactly once per node execution → pipeline_end exactly once.
* Identity invariant: pipeline_id/run_id used in start/end must match across the run; pipeline_spec_canonical is the identity substrate the trace commits to.
* Channel disambiguation invariant: payload-algebra channel store is internal execution state; SemantivaTransport channels are pub/sub topics; no implicit bridging.


## Required minimum contents (Plan PA-03A)

### 1) Entry point contract
`execute_eir_payload_algebra(eir, payload, *, trace_hook=None) -> Payload`

### 2) Internal runtime contracts (pseudo-code only; signatures frozen once approved)

- ChannelStore API: get/set; seed primary; publish rules (semantics deferred)
- SourceRef and parse_source_ref rules:
  - `channel:<name>`, `context:<key>`, unprefixed defaults to `channel:<raw>`
- BindResolver.resolve_param precedence + error modes:
  - precedence intent: explicit bind > node parameters > implicit context-by-name > default
  - must include error mode names for invalid/unknown binds and missing channel
- PublishPlan rules:
  - `data_key` → publish.out
  - do not clobber primary unless publishing to primary
- Provenance mapping rules for SER:
  - `parameter_sources`: context | data | node | default
  - `parameter_source_refs`: per-parameter structured refs for `context` and `data` parameters to disambiguate key/channel and producer identity. `bind` is configuration-only.

### 3) Existing Semantiva integration points (explicitly name)
- TraceDriver + JsonlTraceDriver lifecycle ordering is preserved.
- Orchestrator remains the owner of SER emission and pre/post checks (unless explicitly moved later).
- Executors and transports remain unchanged unless a later epic says otherwise.

## Signature ledger (machine-checkable; used by PA-03A signature gate)

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
