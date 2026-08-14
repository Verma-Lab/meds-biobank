# Legacy ETL Pipelines

---

## OMOP MEDS-ETL 0.1.3

### Path

`/meds-biobank/src/meds-biobank/etl_pipelines/omop_meds_nested.py`

### Description

Re-implementation of `src/meds_etl/omop.py` from `meds_etl` v0.1.3. Converts OMOP v5.4/5.3 → MEDS v0.1.3 (nested format). Use with CLMBR-T-base / FEMR v0.2.3.

### Workflow

1. Extract all events
2. Gather into one df
3. Prune / deduplicate patient event streams
4. Post-process value field and concept code field.
5. Convert event streams into nested patient representations

### Differences from source

Pruning (`delta_encode`, `remove_nones`) happens *before* finalizing the MEDS mapping.

### Supported Tables

`person` · `visit_occurrence` · `procedure_occurrence` · `condition_occurrence` · `drug_exposure` · `observation` · `measurements` · `death`

---

## OMOP MEDS-ETL 0.3.11

### Path

`/meds-biobank/src/meds-biobank/etl_pipelines/omop_nested_flat.py`

### Description

Re-implementation of `src/meds_etl/omop.py` from `meds_etl` v0.3.11. Converts OMOP v5.4/5.3 → MEDS v0.3.3 (flat format, handles visit discharge).

### Workflow

1. Extract all events
2. Gather into one df
3. Prune / deduplicate patient event streams
4. Post-process value field and concept code field.
5. Order event streams by patient, time

### Differences from source

Pruning (`delta_encode`, `remove_nones`) happens *within the ETL*, rather than as part of the tokenizer (FEMR 0.2.3 `transforms` sub-module).

### Supported Tables

`person` · `visit_occurrence` · `procedure_occurrence` · `condition_occurrence` · `drug_exposure` · `observation` · `measurements` · `death`

---

## PMBB OMOP MEDS-ETL

### Path

`/meds-biobank/src/meds-biobank/etl_pipelines/pmbb_meds.py`

### Description

OMOP -> MEDS ETL for PMBB in particular. Converts PMBB OMOP v5.4/5.3 → MEDS|

### Workflow

1. Extract all events
2. Gather into one df
3. Prune / deduplicate patient event streams
4. Post-process value fields
5. Order event streams by patient, time

### Format

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

### Supported Tables
`person` · `visit_occurrence` · `procedure_occurrence` · `condition_occurrence` · `drug_exposure` · `observation` · `measurements` · `labs_/vitals_` ·  `death`

### Example

```bash
# imports
from pathlib import Path
from pyspark.sql import SparkSession
import shutil
import os
from dotenv import load_dotenv

# create spark session
spark = (
    SparkSession.builder
    .master("local[2]")
    .appName("meds-biobank-etl")
    .config("spark.sql.shuffle.partitions", "2")
    .getOrCreate()
)

# set data dir
load_dotenv()
data_dir = Path(__file__).resolve().parents[3] / os.environ["PMBB_OMOP_DATA_DIR"]

# read data tables
tables = [
    spark.read.csv(str(data_dir / "condition_occurrence.csv"), header=True, inferSchema=True),
    spark.read.csv(str(data_dir / "death.csv"), header=True, inferSchema=True),
    spark.read.csv(str(data_dir / "drug_exposure.csv"), header=True, inferSchema=True),
    spark.read.csv(str(data_dir / "observation.csv"), header=True, inferSchema=True),
    spark.read.csv(str(data_dir / "person.csv"), header=True, inferSchema=True),
    spark.read.csv(str(data_dir / "procedure_occurrence.csv"), header=True, inferSchema=True),
    spark.read.csv(str(data_dir / "visit_occurrence.csv"), header=True, inferSchema=True)
]
labs = [
    spark.read.csv(str(data_dir / "labs_creatinine.csv"), header=True, inferSchema=True),
    spark.read.csv(str(data_dir / "labs_glucose.csv"), header=True, inferSchema=True),
    spark.read.csv(str(data_dir / "labs_hba1c.csv"), header=True, inferSchema=True)
]
vitals = [
    spark.read.csv(str(data_dir / "vitals_heart_rate.csv"), header=True, inferSchema=True),
    spark.read.csv(str(data_dir / "vitals_spo2.csv"), header=True, inferSchema=True),
    spark.read.csv(str(data_dir / "vitals_systolic_bp.csv"), header=True, inferSchema=True)
]
measurement = spark.read.csv(str(data_dir / "measurement.csv"), header=True, inferSchema=True)

# record table names
table_names = ["condition_occurrence", "death", "drug_exposure", "observation", "person", "procedure_occurrence", "visit_occurrence"]
lab_names = ["labs_creatinine", "labs_glucose", "labs_hba1c"]
vital_names = ["vitals_heart_rate", "vitals_spo2", "vitals_systolic_bp"]

# extract events
event_dfs = []
for table, name in zip(tables, table_names):
    result = extract_events(table, name)
    event_dfs.append(result)
for table, name in zip(labs, lab_names):
    result = extract_events(table, name)
    event_dfs.append(result)
for table, name in zip(vitals, vital_names):
    result = extract_events(table, name)
    event_dfs.append(result)

# extract measurement events
measurement_events = extract_events(measurement, "measurement")

# gather events together
gathered_events = gather_events(event_dfs, measurement_events)

# prune events
pruned_events = prune_events(gathered_events)

# post process events
post_processed_events = post_process_events(pruned_events)

# format events
formatted_events = format_events(post_processed_events)

# show
print(formatted_events.limit(25).toPandas())

# set write dir
write_dir = Path(__file__).resolve().parents[3] / os.environ["MEDS_DATA_DIR"] / pmbb_meds.csv
formatted_events.toPandas().to_csv(str(write_dir), index=False)
```
