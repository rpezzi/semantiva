EIRv1 Schema (v1)
=================

This page is factual: it exposes the current EIRv1 JSON schema shipped with Semantiva.

Normative interpretation (TDRv3 alignment)
------------------------------------------
* ``canonical_pipeline_spec`` is **CPSV1-only** (purely normative): it contains only the CPS meaning payload (``version`` + ``nodes``). It MUST NOT contain any derived/non-normative views.
* All derived and potentially unstable compilation artifacts MUST live under ``eir.derived.*``.

   * ``eir.derived.edges`` — derived dependency edges / upstream relations (compiler-produced).
   * ``eir.derived.plan`` — execution plan segments.

* **Identity hashing rule:** ``pipeline_id`` is computed from CPSV1 only. ``eir.derived.*`` (and provenance under ``eir.source.*``) MUST NOT influence identity hashing.

.. literalinclude:: ../../../semantiva/eir/schema/eir_v1.schema.json
   :language: json
