# Legacy ETL Pipelines

Three earlier OMOP → MEDS ETL implementations, superseded by [`meds_biobank.etl_pipelines.omop_meds`](./etl_pipelines.md). Kept for reference and reproducing older experiments. Each module is self-contained (does not import the others), and each re-implements its own `extract_events` / `prune_events` / `post_process_events` / `format_*` with the differences noted below.

---

## OMOP MEDS-ETL 0.1.3 (nested)

**Source:** `src/meds_biobank/etl_pipelines/legacy/omop_meds_nested.py`

Re-implementation of `src/meds_etl/omop.py` from `meds_etl` v0.1.3. Converts OMOP v5.4/5.3 → **MEDS v0.1.3, nested format** (one row per patient, events grouped by timestamp). Intended for use with CLMBR-T-base / FEMR v0.2.3.

### Supported tables

`person` · `visit_occurrence` · `procedure_occurrence` · `condition_occurrence` · `drug_exposure` · `observation` · `measurement` · `death`

### `extract_events(df, table)`

| Name | Type | Description |
| --- | --- | --- |
| `df` | `pyspark.sql.DataFrame` | OMOP table, schema `\|person_id\|concept_id\|{table}_start_date\|...`. |
| `table` | `str` | OMOP table name. |

**Returns:** `events` (`pyspark.sql.DataFrame`), schema `\|patient_id\|start\|concept_id\|value\|META_end\|META_event_type\|META_visit_id\|META_unit\|`. `patient_id = crc32(person_id)`; `concept_id` is always the **standard** OMOP concept id (there is no source-concept fallback, unlike the flat/PMBB variants below). `visit_occurrence` only emits an admission event (no discharge event — that was added later, in `omop_meds_flat.py`). **Raises** `Exception` for an unsupported `table`.

### `gather_event_dfs(event_dfs)`

Unions a `List[pyspark.sql.DataFrame]` of the schema above by name, with `allowMissingColumns=True` (looser than the modern `gather_events`, which requires an exact column match).

### `prune_events(events)`

Same two-pass dedup as the modern `prune_events` (null-value drop within a `(patient_id, concept_id, date)` group, then delta-encoding by `(patient_id, concept_id)` ordered by `start`), just against `start`/`concept_id` instead of `time`/`code`.

> **Difference from the upstream `meds_etl` source:** here, pruning (`delta_encode`/`remove_nones`) happens *before* the MEDS mapping is finalized, rather than downstream as part of the tokenizer (as in FEMR 0.2.3's `transforms` sub-module).

### `post_process_events(events, concepts)`

| Name | Type | Description |
| --- | --- | --- |
| `events` | `pyspark.sql.DataFrame` | Schema `\|patient_id\|start\|concept_id\|value\|META_end\|META_event_type\|META_visit_id\|META_unit\|`. |
| `concepts` | `pyspark.sql.DataFrame` | OMOP `concept` table, schema `\|concept_id\|vocabulary_id\|concept_code\|...`. |

**Returns:** `events` (`pyspark.sql.DataFrame`), schema `\|patient_id\|time\|code\|text_value\|numeric_value\|metadata\|`, where `code = vocabulary_id/concept_code` (a **string** code, unlike the modern pipeline's integer concept id) and `metadata` is a struct of `visit_id`/`unit`/`end`.

### `nest_patient(meds_events, patient_id)`

Converts one patient's flat MEDS rows into the nested v0.1.3 shape: `{"patient_id": ..., "static_measurements": [], "events": [{"time": ..., "measurements": [...]}]}`, sorted by `time`.

### `meds_flat_to_patients_df(meds_df)`

Vectorized (Spark-native) version of `nest_patient` applied to an entire flat MEDS DataFrame at once, returning one row per patient with columns `patient_id`, `static_measurements`, `events` (`array<struct<time, measurements>>`, sorted by time).

---

## OMOP MEDS-ETL 0.3.11 (flat)

**Source:** `src/meds_biobank/etl_pipelines/legacy/omop_meds_flat.py`

Re-implementation of `src/meds_etl/omop.py` from `meds_etl` v0.3.11. Converts OMOP v5.4/5.3 → **MEDS v0.3.3, flat format**, and adds visit-discharge handling that v0.1.3 lacked.

### Supported tables

`person` · `visit_occurrence` · `procedure_occurrence` · `condition_occurrence` · `drug_exposure` · `observation` · `measurement` · `death`

### `extract_events(df, table)`

Same signature and output schema as the nested variant's `extract_events` (`\|patient_id\|start\|concept_id\|value\|META_end\|META_event_type\|META_visit_id\|META_unit\|`), with two differences: `visit_occurrence` now emits both admission **and** discharge events (unioned), and `concept_id` is always the standard concept id (no source-concept preference).

### `gather_event_dfs`, `prune_events`

Identical in behavior to the v0.1.3 module's versions of the same functions.

### `post_process_events(events, concepts)`

Same signature and behavior as v0.1.3's `post_process_events`, but casts `numeric_value` via `try_cast(value AS FLOAT)` (v0.1.3 does the same) and outputs `string_value` instead of `text_value` as the non-numeric column name.

### `format_df(meds_events)`

**Parameters:** `meds_events` (`pyspark.sql.DataFrame`).
**Returns:** the same DataFrame ordered by `patient_id`, `time`.

> **Difference from the upstream `meds_etl` source:** pruning happens *within this ETL* (steps above), rather than as a separate tokenizer-time transform.

---

## PMBB OMOP MEDS-ETL

**Source:** `src/meds_biobank/etl_pipelines/legacy/pmbb_meds.py`

OMOP → MEDS ETL specialized for the PMBB cohort: adds source-concept-id fallback, pre-extracted `labs_*`/`vitals_*` tables (see [Standardizers](./standardizers.md)), and outputs the same flat `code`/`time`/`event_type` shape the modern pipeline later adopted.

### Schema

`\|patient_id\|code\|time\|end\|numeric_value\|text_value\|unit\|event_type\|visit_id\|`

| Field | Description |
| --- | --- |
| `patient_id` | `crc32(person_id)`. |
| `code` | OMOP concept id. Standard concept id by default; falls back to the source concept id when `use_omop_cid=False` is passed to `extract_events`. |
| `time` | Coalesced start datetime → start date → date. |
| `end` | Coalesced end datetime → end date, where applicable. |
| `numeric_value` | Value pre-ETL'd into `labs_*`/`vitals_*` tables for a large subset of measurements. |
| `text_value` | Expected to be empty for most rows. |
| `unit` | Value pre-ETL'd into `labs_*`/`vitals_*` tables. |
| `event_type` | Source OMOP domain for most tables, but `"labs_" + name` / `"vitals_" + name` (e.g. `"labs_albumin"`) for pre-extracted lab/vital tables. |
| `visit_id` | Id of the visit that generated the event. |

### Supported tables

`person` · `visit_occurrence` · `visit_occurrence_supplement` · `procedure_occurrence` · `condition_occurrence` · `drug_exposure` · `observation` · `measurement` · `labs_*` / `vitals_*` · `death`

### `extract_events(df, table, use_omop_cid=True)`

| Name | Type | Default | Description |
| --- | --- | --- | --- |
| `df` | `pyspark.sql.DataFrame` | — | OMOP table, or a pre-extracted `labs_*`/`vitals_*` table. |
| `table` | `str` | — | OMOP table name, or `labs_*`/`vitals_*`. |
| `use_omop_cid` | `bool` | `True` | Whether to keep `code` as the standard concept id (`omop_concept_id`), or fall back to each table's source-concept id when `False`. |

**Returns:** `events` (`pyspark.sql.DataFrame`), schema above plus a `measurement_id` column (dropped later by `gather_events`, used to de-duplicate measurement rows already covered by `labs_*`/`vitals_*`).

**Behavior differences from `omop_meds.py`'s `extract_events`:**
- `visit_occurrence_supplement` support (visit-flag pivoting via `concepts.VISIT_FLAGS`), same as the modern pipeline.
- `labs_*`/`vitals_*` tables: filtered to rows with a non-null `value_converted` (from [`standardize_numeric_values_and_units`](./standardizers.md)); `event_type` is set to the literal table name; raises `Exception` if the table has zero valid rows.
- `measurement` rows always use the raw `measurement_concept_id`/`value_as_number`/`value_source_value`/`unit_source_value` (no `concept_id_std` fast path).

**Raises:** `Exception` for an unsupported `table`, or (for `labs_*`/`vitals_*`) if no rows survive the `value_converted` filter.

### `gather_events(event_dfs, measurement_events)`

| Name | Type | Description |
| --- | --- | --- |
| `event_dfs` | `List[pyspark.sql.DataFrame]` | Non-measurement event tables (including any `labs_*`/`vitals_*` tables), **excluding** `measurement_events`. |
| `measurement_events` | `pyspark.sql.DataFrame` \| `None` | Output of `extract_events(measurement_df, "measurement")`. |

**Returns:** `all_events` (`pyspark.sql.DataFrame`), schema `\|patient_id\|code\|time\|end\|value\|unit\|event_type\|visit_id\|` (`measurement_id` dropped). If `measurement_events` is given, any raw measurement row whose `measurement_id` already appears in a `labs_*`/`vitals_*` table is dropped first — the pre-extracted, higher-quality lab/vital row wins.

### `prune_events(events)`, `post_process_events(events)`, `format_events(events)`

Behaviorally identical to the modern pipeline's [`prune_events`](./etl_pipelines.md#prune_eventsevents), [`post_process_events`](./etl_pipelines.md#post_process_eventsevents), and [`format_events`](./etl_pipelines.md#format_eventsevents) (this module is where those three functions originated; `omop_meds.py` carried them forward largely unchanged, minus the `event_type` column which the modern pipeline derives separately via `create_concept_schema`), except `format_events` here sorts by `patient_id`, `time` only (no `visit_id` tiebreak).
