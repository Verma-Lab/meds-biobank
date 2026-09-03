# meds_biobank.etl_pipelines

The stable OMOP → MEDS ETL: a set of top-level functions that turn OMOP CDM tables into a flat MEDS event stream, a concept metadata table, and a patient train/val/test/task split.

**Source:** `src/meds_biobank/etl_pipelines/omop_meds.py`

For older, module-specific ETL implementations (nested MEDS format, PMBB-specific), see [Legacy ETL Pipelines](./legacy_etl_pipelines.md).

---

## Schemas

### MEDS DataSchema (events)

`schemas.MEDS_EVENT_SCHEMA` — the output of `extract_events` / `gather_events` / ... / `format_events`.

| Field | Type | Nullable | Description |
| --- | --- | --- | --- |
| `patient_id` | `long` | No | `crc32(person_id)` — hashed patient id. |
| `code` | `int` | No | Standardized OMOP concept id for this event. |
| `time` | `timestamp` | No | Event time. |
| `end` | `timestamp` | Yes | End time, if the event has a duration. |
| `numeric_value` | `double` | Yes | Numeric value, if this is a measurement/observation with one. |
| `unit` | `string` | Yes | Unit, if `numeric_value` is set. |
| `text_value` | `string` | Yes | Text value, if this is a measurement/observation without a numeric value. |
| `visit_id` | `int` | Yes | OMOP `visit_occurrence_id`, where applicable (not for demographics). |

- **Requirement:** every event that goes in comes out (`extract_events` → `gather_events` don't drop rows; `prune_events` intentionally removes exact duplicates).
- **Requirement:** results are ordered by patient, then time, then visit id (`format_events`).

### MEDS ConceptSchema

`schemas.MEDS_CONCEPT_SCHEMA` — the output of `create_concept_schema`.

| Field | Type | Nullable | Description |
| --- | --- | --- | --- |
| `code` | `int` | No | OMOP concept id. |
| `name` | `string` | No | Concept name. |
| `ancestors` | `array<struct<ancestor_concept_id: int, min_levels_of_separation: int>>` | Yes | All OMOP ancestors of `code` with their distance. |
| `factors` | `array<int>` | Yes | Same-domain ancestors of `code`, nearest first — a "decomposition" of the code. |
| `event_type` | `string` | No | Lowercased OMOP `domain_id`, or `"special_concept"` for birth/death/visit-flag codes. |
| `units` | `array<string>` | Yes | All units observed for `code` in the data (may contain `null` entries). |

- **Requirement:** every code that occurs in the events is present in the concept schema; codes that don't occur are not.

### MEDS SplitSchema

`schemas.MEDS_SPLIT_SCHEMA` — the output of `extract_splits`.

| Field | Type | Nullable | Description |
| --- | --- | --- | --- |
| `patient_id` | `long` | No | `crc32(person_id)` — same hash space as `MEDS_EVENT_SCHEMA.patient_id`. |
| `split` | `string` | No | One of `"train"`, `"val"`, `"test"`, `"task"`. |

- **Requirement:** the split is a deterministic function of `person_id` (fixed seed) and honors the requested proportions.

### MEDS TaskSchema

`schemas.MEDS_TASK_SCHEMA` — the intended output of `extract_tasks` (not yet implemented; see below).

| Field | Type | Nullable | Description |
| --- | --- | --- | --- |
| `patient_id` | `long` | No | Hashed patient id. |
| `prediction_time` | `timestamp` | No | Cutoff time to truncate a patient's journey before prediction. |
| `label` | `string` | No | The prediction target. |
| `split` | `string` | No | One of `"train"`, `"val"`, `"test"`. |

---

## Workflow

1. `extract_events` per OMOP table → one MEDS-shaped DataFrame per table.
2. `gather_events` — union them into one DataFrame.
3. `prune_events` — deduplicate patient event streams.
4. `post_process_events` — split the raw `value` string into `numeric_value` / `text_value`.
5. `format_events` — sort by patient, time, visit.
6. `create_concept_schema` — build the accompanying `ConceptSchema` from `concept` / `concept_ancestor`.
7. `extract_splits` — assign each patient to train/val/test/task.

### Supported OMOP tables

`person` · `visit_occurrence` · `visit_occurrence_supplement` · `procedure_occurrence` · `condition_occurrence` · `drug_exposure` · `observation` · `measurement` · `death`

---

## `extract_events(df, table)`

Converts one OMOP table into an unordered MEDS-shaped DataFrame, dispatching on `table`.

**Parameters**

| Name | Type | Description |
| --- | --- | --- |
| `df` | `pyspark.sql.DataFrame` | An OMOP table conforming to the matching `schemas.OMOP_*_SCHEMA`. |
| `table` | `str` | One of the supported table names above. |

**Returns:** `events` (`pyspark.sql.DataFrame`), schema `\|patient_id\|code\|time\|end\|value\|unit\|visit_id\|` (`value` is still a raw string here — `numeric_value`/`text_value` split happens in `post_process_events`). `patient_id` is `crc32(person_id)`; `code` is the **standardized** OMOP concept id (not the source concept id).

**Raises:** `Exception` if `table` is not one of the supported names.

**Per-table behavior**

| `table` | Emits |
| --- | --- |
| `person` | Birth event (`concepts.OMOP_BIRTH`) at `birth_datetime`, plus one demographic event each for gender/race/ethnicity where the concept id is nonzero. |
| `death` | One event at `death_date` using `concepts.OMOP_DEATH`. |
| `visit_occurrence` | Admission event at `visit_start_datetime`/`visit_start_date` (falls back to concept id `8` if `visit_concept_id` is `0`), plus a discharge event at `visit_end_*` when `discharge_to_concept_id` is present and nonzero. Both carry `visit_id` and `end`. |
| `visit_occurrence_supplement` | Pivots the boolean visit-flag columns named in `concepts.VISIT_FLAGS` into one row per flag that's `1`, with `code` mapped through `VISIT_FLAGS`. |
| `drug_exposure` / `condition_occurrence` | One event per row at the coalesced start datetime/date, filtered to nonzero concept id, carrying `visit_id` and `end`. |
| `procedure_occurrence` | One event per row at the coalesced start datetime/date, filtered to nonzero concept id, carrying `visit_id` (no `end`). |
| `observation` | Two unioned sub-streams: the observation itself (`code = observation_concept_id`, `value = coalesce(value_as_number, value_as_string)`), and a second event per row where `value_as_concept_id` is set (`code = value_as_concept_id`, no `value`). Both filtered to nonzero `code`. |
| `measurement` | Deduplicated on `measurement_id` first. If a `concept_id_std` column is present (i.e. the table was already run through [`standardize_measurement_concept_id`](./standardizers.md)), uses `concept_id_std`/`numeric_value_std`/`text_value_std`/`unit_std`; otherwise falls back to the raw `measurement_concept_id`/`value_as_number`/`value_source_value`/`unit_source_value`. |

Missing `value`/`end`/`visit_id`/`unit` columns are filled with `null` after dispatch; `concept_id` is renamed to `code`.

---

## `gather_events(event_dfs)`

Unions a list of per-table event DataFrames (from `extract_events`) into one.

**Parameters**

| Name | Type | Description |
| --- | --- | --- |
| `event_dfs` | `List[pyspark.sql.DataFrame]` | Each with schema `\|patient_id\|code\|time\|end\|value\|unit\|visit_id\|`. |

**Returns:** `all_events` (`pyspark.sql.DataFrame`), same schema, unioned by name (`allowMissingColumns=False` — every input must have identical columns).

**Raises:** `Exception` if `event_dfs` is empty.

---

## `prune_events(events)`

Deduplicates each patient's event stream in two passes.

**Parameters**

| Name | Type | Description |
| --- | --- | --- |
| `events` | `pyspark.sql.DataFrame` | Schema `\|patient_id\|code\|time\|end\|value\|unit\|visit_id\|`. |

**Returns:** `pruned_events` (`pyspark.sql.DataFrame`), same schema.

**Behavior**

1. Within the same `(patient_id, code, date)` group, if at least one row has a non-null `value`, drop the null-`value` rows (prefer the informative copy).
2. Delta-encode: for consecutive rows of the same `(patient_id, code)` ordered by `time`, drop a row if it has the same `value` as the immediately preceding row on the same calendar date (null-safe equality).

---

## `post_process_events(events)`

Splits the raw string `value` column into typed `numeric_value`/`text_value` columns.

**Parameters**

| Name | Type | Description |
| --- | --- | --- |
| `events` | `pyspark.sql.DataFrame` | Schema `\|patient_id\|code\|time\|end\|value\|unit\|visit_id\|`. |

**Returns:** `processed_events` (`pyspark.sql.DataFrame`), schema `\|patient_id\|code\|time\|end\|numeric_value\|text_value\|unit\|visit_id\|`. `numeric_value = try_cast(value AS DOUBLE)`; `text_value = value` wherever that cast fails, else `null`. `value` is dropped.

---

## `format_events(events)`

**Parameters:** `events` (`pyspark.sql.DataFrame`) — schema `\|patient_id\|code\|time\|end\|numeric_value\|text_value\|unit\|visit_id\|`.

**Returns:** the same DataFrame ordered by `patient_id`, `time`, `visit_id`.

> **Known gap (TODO in source):** visit-admission events are not currently forced to the front, nor visit-discharge events to the end, of events sharing the same timestamp.

---

## `create_concept_schema(events, concept, concept_ancestor)`

Builds the `MEDS_CONCEPT_SCHEMA` metadata table for every code that actually occurs in `events`.

**Parameters**

| Name | Type | Description |
| --- | --- | --- |
| `events` | `pyspark.sql.DataFrame` | Schema `\|patient_id\|code\|time\|end\|numeric_value\|text_value\|unit\|visit_id\|`. Only used to determine which codes occur. |
| `concept` | `pyspark.sql.DataFrame` | OMOP `concept` table (`schemas.OMOP_CONCEPT_SCHEMA`). |
| `concept_ancestor` | `pyspark.sql.DataFrame` | OMOP `concept_ancestor` table (`schemas.OMOP_CONCEPT_ANCESTOR_SCHEMA`). |

**Returns:** `concept_schema` (`pyspark.sql.DataFrame`), conforming exactly to `schemas.MEDS_CONCEPT_SCHEMA`.

**Behavior**

1. Restrict `concept`/`concept_ancestor` to codes that occur in `events`.
2. `ancestors`: every `(ancestor_concept_id, min_levels_of_separation)` pair for the code (excluding itself).
3. `factors`: the subset of those ancestors in the **same OMOP domain** as the code, sorted nearest-first — the code's same-domain "decomposition".
4. Add synthetic rows for special concepts (`concepts.VISIT_FLAGS`, `concepts.OMOP_BIRTH`, `concepts.OMOP_DEATH`) that occur in the data but don't already have a real `concept` row, with `null` `ancestors`/`factors`.
5. `event_type` = `"special_concept"` for special/visit-flag codes, else the concept's OMOP `domain_id`, lowercased.
6. `units`: every unit observed for the code in `events` (may include `null`).

---

## `extract_splits(spark, person, train=0.7, val=0.1, test=0.1, task=0.1)`

Assigns every patient in `person` to a `train`/`val`/`test`/`task` split.

**Parameters**

| Name | Type | Default | Description |
| --- | --- | --- | --- |
| `spark` | `pyspark.sql.SparkSession` | — | Used to build the output DataFrame. |
| `person` | `pyspark.sql.DataFrame` | — | Must conform to `schemas.OMOP_PERSON_SCHEMA`. |
| `train`, `val`, `test`, `task` | `float` | `0.7, 0.1, 0.1, 0.1` | Proportions; must sum to `1.0` (within `1e-9`). |

**Returns:** `splits` (`pyspark.sql.DataFrame`), conforming to `schemas.MEDS_SPLIT_SCHEMA`.

**Behavior:** seeds Python's `random` module with a **fixed seed (`42`)** — every call produces the same shuffle for the same `person` input, regardless of when it's called. `person_id`s are shuffled and sliced by count into the four buckets, then hashed to `patient_id` via `crc32` (the same hash used by `extract_events`), so splits key on the same patient-id space as the event stream.

**Raises:** `ValueError` if `person` isn't a `DataFrame`, doesn't match `OMOP_PERSON_SCHEMA`, or the four proportions don't sum to `1.0`.

---

## `extract_tasks(concept, concept_ancestor, condition_occurrence, death, drug_exposure, measurement, observation, person, procedure_occurrence, visit_occurrence, splits)`

**Not yet implemented.** The function signature exists but the body is a placeholder (`pass`) — calling it does nothing and returns `None`. The source lists intended task categories as TODOs: administrative tasks (length of stay, 30-day readmission), condition-onset tasks (sepsis, diabetes, heart failure, sudden cardiac death), phenotyping tasks, and response tasks (medication adverse response). None of these are extracted yet.
