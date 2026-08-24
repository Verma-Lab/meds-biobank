import pytest
from pathlib import Path
import os
from dotenv import load_dotenv
from meds_biobank.ontologies.ontologies import Ontology
from meds_biobank.tokenizers.tokenizers import BaseTokenizer
from meds_biobank import schemas
import random
import pyspark.sql.functions as F

# seed random
random.seed(42)

@pytest.fixture(scope="session")
def meds_events(spark):
    REPO_ROOT = Path(__file__).resolve().parents[2]
    MEDS_DATA_DIR = REPO_ROOT / os.environ["MEDS_DATA_DIR"] / "generated-standard"
    meds_events_path = MEDS_DATA_DIR / "meds_events.parquet"
    return spark.read.parquet(str(meds_events_path), schema=schemas.MEDS_EVENT_SCHEMA)

@pytest.fixture(scope="session")
def ontology(spark):
    REPO_ROOT = Path(__file__).resolve().parents[2]
    ontology_data_dir = REPO_ROOT / os.environ["ONTOLOGY_DATA_DIR"] / "generated-standard-rolled"
    ontology = Ontology()
    ontology.load_from_disk(str(ontology_data_dir), overwrite=False)
    return ontology

def test_base_tokenizer(meds_events, ontology):
    
    # create base tokenizer
    bt = BaseTokenizer(
        ontology,
        qualifiers=False,
        event_types=False,
        factors=True,
        factor_types=["drug", "procedure", "measurement", "condition", "observation"]
    )
    bt.build_vocab()

    # load a patient
    pt_ids = [row["patient_id"] for row in meds_events.select("patient_id").distinct().collect()]
    rand_id = random.choice(pt_ids)
    pt_rows = meds_events.filter(F.col("patient_id") == rand_id).orderBy("time") # ensure ordering
    events = [
        {
            "patient_id": row["patient_id"],
            "code": row["code"],
            "time": row["time"],
            "end": row["end"],
            "numeric_value": row["numeric_value"],
            "text_value": row["text_value"],
            "unit": row["unit"],
            "visit_id": row["visit_id"]
        } for row in pt_rows.collect()
    ]

    # tokenize the patient
    tokens = bt.tokenize(events)

    # detokenize the patient
    decoded_events = bt.detokenize(tokens)

    # TODO: test that measurement values were tokenized

    # TODO: test that tokenizer round-trips with respect to ontology codes

    # TODO: test that tokenizer sub-fields round-trip for true ontology codes

    # TODO: test that all requested ontology symbols are in the tokenizer symbol fields

    # TODO: what happens when qualifiers are requested but there are no qualifiers?

# TODO: test schemas

# TODO: test time tokenizer