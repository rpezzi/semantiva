EIRv1 Schema (v1)
=================

Normative interpretation
------------------------

Semantiva contains shipped schema and compiler support carrying a CPSV1 meaning layer and compiler-derived wiring artifacts **without breaking the existing scalar EIR path**:

* ``canonical_pipeline_spec`` (optional) embeds **CanonicalPipelineSpecV1 (CPSV1)** as the normative meaning payload (``version`` + ``nodes``).
* ``derived`` (optional) carries **non-normative** artifacts excluded from identity hashing:

  * ``derived.edges`` — derived dependency edges computed deterministically from CPSV1 bind/publish.
  * ``derived.plan`` — placeholder array for future payload algebra scheduling.
  * ``derived.diagnostics`` — placeholder array for compiler diagnostics.

Identity rule:
* For authoring specs that use ``bind`` and/or ``data_key``, ``eir.identity.pipeline_id`` MUST be computed from the embedded CPSV1 only.
* For legacy pipelines that do not use ``bind``/``data_key``, identity behavior remains unchanged in attaching these optional fields (pipeline_id preservation until PA-03).

.. literalinclude:: ../../../semantiva/eir/schema/eir_v1.schema.json
   :language: json
