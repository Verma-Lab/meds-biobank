# meds-biobank

> Unofficial, lightweight Python re-implementation of parts of the **MEDS** software ecosystem, built to operate on in-memory tables loaded via PySpark rather than directly on disk. Designed for small-to-medium biobanks queried through cloud services in interactive Python notebooks.

**Primary target:** Penn Medicine Biobank

---

## Table of Contents

- [Features](#features)
  - [ETL](#etl)
    - [OMOP MEDS-ETL 0.1.3 (Nested)](#-omop-meds-etl-013)
    - [OMOP MEDS-ETL 0.3.11 (Flat)](#-omop-meds-etl-0311)
  - [Ontologies](#ontologies)
  - [Tokenizers](#tokenizers)

---

## Features

### ETL

#### OMOP MEDS-ETL 0.1.3

| | |
|---|---|
| **Path** | `/meds-biobank/src/meds-biobank/etl_pipelines/omop_meds_nested.py` |
| **Description** | Re-implementation of `src/meds_etl/omop.py` from `meds_etl` v0.1.3. Converts OMOP v5.4/5.3 → MEDS v0.1.3 (nested format). Use with CLMBR-T-base / FEMR v0.2.3. |

**Workflow**
1. Extract all events
2. Gather into one df
3. Prune / deduplicate patient event streams
4. Post-process value field and concept code field.
5. Convert event streams into nested patient representations

**Differences from source**
Pruning (`delta_encode`, `remove_nones`) happens *before* finalizing the MEDS mapping.

**Supported Tables**
`person` · `visit_occurrence` · `procedure_occurrence` · `condition_occurrence` · `drug_exposure` · `observation` · `measurements` · `death`

---

#### OMOP MEDS-ETL 0.3.11

| | |
|---|---|
| **Path** | `/meds-biobank/src/meds-biobank/etl_pipelines/omop_nested_flat.py` |
| **Description** | Re-implementation of `src/meds_etl/omop.py` from `meds_etl` v0.3.11. Converts OMOP v5.4/5.3 → MEDS v0.3.3 (flat format, handles visit discharge). |

**Workflow**
1. Extract all events
2. Gather into one df
3. Prune / deduplicate patient event streams
4. Post-process value field and concept code field.
5. Order event streams by patient, time

**Differences from source**
Pruning (`delta_encode`, `remove_nones`) happens *within the ETL*, rather than as part of the tokenizer (FEMR 0.2.3 `transforms` sub-module).

**Supported Tables**
`person` · `visit_occurrence` · `procedure_occurrence` · `condition_occurrence` · `drug_exposure` · `observation` · `measurements` · `death`

---

#### PMBB OMOP MEDS-ETL

| | |
|---|---|
| **Path** | `/meds-biobank/src/meds-biobank/etl_pipelines/pmbb_meds.py` |
| **Description** | OMOP -> MEDS ETL for PMBB in particular. Converts PMBB OMOP v5.4/5.3 → MEDS|

**Workflow**
1. Extract all events
2. Gather into one df
3. Prune / deduplicate patient event streams
4. Post-process value fields
5. Order event streams by patient, time

**Format**
- |patient_id|code|time|end|numeric_value|text_value|unit|event_type|visit_id|
- patient_id: id of the patient
- code: OMOP concept id. Unless explicitly specified via boolean flag, use standardized OMOP concept id. Otherwise try to use source concept id.
- time: time of event. coalesce in order (start datetime, start date, datetime date)
- end: (optional) end time of event. Coalesce in order (end datetime, end date).
- numeric_value: contains ETL'd value from value_converted col for a large subset of measurements pre-extracted into labs_ and vitals_ tables
- text_value: should be empty (check)
- unit: contains ETL'd value from unit_converted col for a large subset of measurements pre-extracted into labs_ and vitals_ tables
- event_type: source OMOP domain for the table contianing the event (e.g. measurement, procedure, etc.) EXCEPT for labs and vitals which are formatted as "labs_"/"vitals_" + name e.g. "labs_albumin"
- visit_id: id of visit that generated the event

**Supported Tables**
`person` · `visit_occurrence` · `procedure_occurrence` · `condition_occurrence` · `drug_exposure` · `observation` · `measurements` · `labs_/vitals_` ·  `death`

---

#### Stable OMOP MEDS-ETL

| | |
|---|---|
| **Path** | `/meds-biobank/src/meds-biobank/etl_pipelines/omop_meds.py` |
| **Description** | Modern custom OMOP -> MEDS ETL. Minor modifications on OMOP MEDS-ETL 0.3.11.

**Workflow**
1. Extract all events
2. Gather into one df
3. Prune / deduplicate patient event streams
4. Post-process value fields
5. Order event streams by patient, time

**Format**
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

**Supported Tables**
`person` · `visit_occurrence` · `procedure_occurrence` · `condition_occurrence` · `drug_exposure` · `observation` · `measurements` · `death`

---

### Ontologies

---

### Tokenizers

---