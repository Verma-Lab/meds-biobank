# Ontologies

## Workflow

1. Load a saved ontology, or:
2. Create one
    - Compute concept ontology from events, concept, and concept_ancestor
    - Bucketize measurements (deciles but handle 0-case separately first)
    - Perform concept rollup mapping

## Inputs

- events: |patient_id|code|time|end|numeric_value|text_value|unit|event_type|visit_id|mtype|
- concept: OMOP concept table
- concept_ancestor: OMOP concept ancestor table
- qualifications (optional): map of codes to qualifiers

## Ontology Fields

- codes: list of all concept codes
- etypes: list of all used OMOP table types (event_types)
- mtypes: list of all measurement types used for decile binning
- deciles: list of all decile bins used (d0, d1, ...)
- special codes: dict mapping special concept name to id
- code_to_etype: maps code to event type
- code_to_mtype: maps code to measurement type
- code_to_name: maps code to name
- code_to_qualifiers: maps code to qualifiers
- code_to_parents: maps code to immediate ancestor codes
- mtype_to_decile_ranges: maps measurement type and decile bin d0-10 to min/max
- mtype_to_unit: maps measurement type to unit
- rollup_map: maps unregistered codes to registered parent codes where possible

## Example

```bash
# imports
from pyspark.sql import SparkSession
from dotenv import load_dotenv
from pathlib import Path
import os

# init spark session
spark = (
    SparkSession.builder
    .master("local[2]")
    .appName("meds-ontology")
    .config("spark.sql.shuffle.partitions", "2")
    .getOrCreate()
)

# read concept, concept_ancestor, qualifications, and events
load_dotenv()
REPO_ROOT = Path(__file__).resolve().parents[3]
events_path = REPO_ROOT / os.environ["MEDS_DATA_DIR"] / "meds.csv"
data_dir = REPO_ROOT / os.environ["OMOP_DATA_DIR"]
events = spark.read.csv(str(events_path), header=True, inferSchema=True)
concept = spark.read.csv(str(data_dir / "concept.csv"), header=True, inferSchema=True)
concept_ancestor = spark.read.csv(str(data_dir / "concept_ancestor.csv"), header=True, inferSchema=True)

# set dirname
ontology_dir = REPO_ROOT / os.environ["ONTOLOGY_DATA_DIR"]

# create ontology object, fit, and save
ontology = Ontology()
ontology.compute_concept_ontology(events, concept, concept_ancestor)
ontology.bin_measurements(events)
ontology.rollup_concepts(events, concept_ancestor)
ontology.save_to_disk(str(ontology_dir), override=True)

# load saved ontology object
new_ontology = Ontology()
new_ontology.load_from_disk(str(ontology_dir))
```
