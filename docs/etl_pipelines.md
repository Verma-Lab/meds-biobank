# ETL Pipelines

---

## Legacy ETL Pipelines

See [docs](./legacy_etl_pipelines.md) for details.

---

## Stable OMOP MEDS-ETL

### Path

`/meds-biobank/src/meds-biobank/etl_pipelines/omop_meds.py`

### Description

Modern custom OMOP -> MEDS ETL. Minor modifications on OMOP MEDS-ETL 0.3.11.

### Output Schemas

1. MEDS DataSchema

```bash
# required fields
|patient_id|code|time|event_type|
patient_id (string): unique hashed id for patient
code (long): standardized OMOP concept id for this event
time (timestamp): time of the event
event_type (string): type of the event (measurement, drug, etc.)

# optional fields
|end|numeric_value|text_value|unit|visit_id|
end (timestamp): end time of the event if present
numeric_value (float): numeric value of the event if it is a measurement (or observation)
text_value (string): string value of the event if it is a measurement (or observation) without a numeric value
unit (string): unit of the event if it is a measurement (or observation) with a numeric value
visit_id (string): id of the visit (OMOP visit_occurrence_id) if this makes sense (does for most events but not demographics)
```

2. MEDS ConceptSchema

```bash
# required fields
|code|ancestors|domain_ancestors|name|
code (long): OMOP concept id
ancestors (list<tuple<long, int>>): tuples OMOP concept id of ancestor, ontological distance
domain_ancestors (list<long>): OMOP concept ids of ancestors in the same domain as the code
name (string): concept name
```

3. MEDS SplitSchema

```bash
# required fields
|patient_id|task_split|
patient_id (string): unique hashed id for patient
task_split (string): model_train/model_val/model_test/task
```

### Workflow

1. Extract all events
2. Gather into one df
3. Prune / deduplicate patient event streams
4. Post-process value fields
5. Order event streams by patient, time

### Output Format

- |patient_id|code|time|end|numeric_value|text_value|unit|event_type|visit_id|
- patient_id: id of the patient
- code: OMOP concept id. Unless explicitly specified via boolean flag, use standardized OMOP concept id. Otherwise try to use source concept id.
- time: time of event. coalesce in order (start datetime, start date, datetime date)
- end: (optional) end time of event. Coalesce in order (end datetime, end date).
- numeric_value: contains source value cast to numeric if possible
- text_value: contains source value as string if numeric cast fails
- unit: contains source unit
- event_type: source OMOP domain for the table contianing the event (e.g. measurement, procedure, etc.)
- visit_id: id of visit that generated the event

### Supported Tables

`person` · `visit_occurrence` · `procedure_occurrence` · `condition_occurrence` · `drug_exposure` · `observation` · `measurements` · `death`
