# EIR Algebra Demos (Phase-2 feasible, no execution infra)

This folder contains **three documentation-grade, happy-path** demos of Semantiva’s
payload algebra that are feasible at Phase 2: **construct payloads**, apply **pure
transforms**, and do deterministic **in-memory** composition.

## Demos

1) **Demo 1 — rewrite story**
   - select channels
   - map a value transform on one channel
   - rename a channel (ref)

2) **Demo 2 — composition story**
   - merge two payloads deterministically
   - handle collisions explicitly (namespace)

3) **Demo 3 — ref-anchored derivation**
   - align a signal to a reference channel (in-memory)
   - derive a feature and output `{ref, feat}`

## How CI verifies these
Pytest executes each demo module and asserts:
- the demo runs without side effects
- outputs match a small golden snapshot (to prevent silent drift)

These demos are intentionally **eager and explicit** (no DAGs, no lazy eval, no scheduling).
