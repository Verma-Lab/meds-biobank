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

### Schemas

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

- Requirement: every event that went in, comes out
- Requirement: results are ordered by patient, then time
- Future requirement: visit occurence and flag events at beginning of timestamp, visit end at end?

2. MEDS ConceptSchema

```bash
# required fields
|code|ancestors|factors|name|
code (long): OMOP concept id
ancestors (list<tuple<long, int>>): tuples OMOP concept id of ancestor, ontological distance
factors (list<long>): OMOP concept ids of ancestors in the same domain as the code, in order of distance from code (ascending) [this is a "decomposition" of the code]
name (string): concept name
```

- Requirement: every occurrent code is in ConceptSchema but not the non-occurrent ones

3. MEDS SplitSchema

```bash
# required fields
|patient_id|task_split|
patient_id (string): unique hashed id for patient
task_split (string): model_train/model_val/model_test/task
```

- Requirement: task train/val/test split is deterministic based on patient id (still seeded random)
- Requirement: splits according to specified proportion

4. MEDS TaskSchema

```bash
# required fields
|patient_id|task_split|prediction_date|label|
patient_id (string): unique hashed id for patient
task_split (string): train/val/test
prediction_date (timestamp): cutoff date to truncate patient journey context
label (boolean/int/float/string): the label to predict for this patient
metalabels (any type): used to define subgroups for performance analysis
```

- Requirement: task train/val/test split is deterministic based on patient id (still seeded random)
- Requirement: splits according to specified proportion

### Workflow

1. Extract all events
2. Gather into one df
3. Prune / deduplicate patient event streams
4. Post-process value fields
5. Order event streams by patient, time

### Supported Tables

`person` · `visit_occurrence` · `procedure_occurrence` · `condition_occurrence` · `drug_exposure` · `observation` · `measurements` · `death`
