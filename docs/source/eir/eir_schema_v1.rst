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

Identity Contract
-----------------

Canonical pipeline identity
~~~~~~~~~~~~~~~~~~~~~~~~~~~
The canonical identity for “what pipeline this is” is **only**:

* ``pipeline_id`` — deterministic compile-time identity.

For authoring specs that use payload-algebra features (e.g., ``bind`` / ``data_key``),
the compiler embeds a CPSV1 meaning layer and MUST set ``pipeline_id`` equal to the
CPSV1 identity (computed from CPSV1 stable JSON). Derived artifacts MUST NOT influence
canonical identity hashing.

Prohibited identity fields
~~~~~~~~~~~~~~~~~~~~~~~~~~
All non-canonical identity axes have been removed from the EIR surface:

* Variant and artifact identity fields are not present in the schema or compiler output.
* Tests and docs treat ``pipeline_id`` as the only canonical identifier.

Hashing exclusions (normative)
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
The following MUST NOT influence canonical identity hashing:

* any ``derived.*`` compiler artifacts,
* diagnostics and timestamps,
* build/source provenance fields.

.. literalinclude:: ../../../semantiva/eir/schema/eir_v1.schema.json
   :language: json
