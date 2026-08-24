import pytest
from meds_biobank.standardizers.standardizers import standardize_measurement_concept_id, standardize_numeric_values_and_units, standardize_text_value
from meds_biobank.etl_pipelines.omop_meds import extract_events, gather_events, prune_events, post_process_events, format_events, create_concept_schema
from meds_biobank.ontologies.ontologies import Ontology
from meds_biobank import schemas
from dotenv import load_dotenv
from pathlib import Path
import os

@pytest.fixture(scope="session")
def standardized_measurement(measurement, concept, concept_ancestor):
    std = standardize_measurement_concept_id(measurement, concept, concept_ancestor)
    std = standardize_numeric_values_and_units(std)
    std = standardize_text_value(std)
    return std

@pytest.fixture(scope="session")
def meds_events(spark):
    REPO_ROOT = Path(__file__).resolve().parents[2]
    MEDS_DATA_DIR = REPO_ROOT / os.environ["MEDS_DATA_DIR"] / "generated-standard"
    meds_events_path = MEDS_DATA_DIR / "meds_events.parquet"
    return spark.read.parquet(str(meds_events_path), schema=schemas.MEDS_EVENT_SCHEMA)


@pytest.fixture(scope="session")
def meds_concept_schema(spark):
    REPO_ROOT = Path(__file__).resolve().parents[2]
    MEDS_DATA_DIR = REPO_ROOT / os.environ["MEDS_DATA_DIR"] / "generated-standard"
    meds_events_path = MEDS_DATA_DIR / "meds_concept_schema.parquet"
    return spark.read.parquet(str(meds_events_path), schema=schemas.MEDS_CONCEPT_SCHEMA)

def test_ontologies(spark, meds_events, meds_concept_schema):

    # create ontology and compute
    ontology = Ontology()
    ontology.compute_concept_ontology(meds_events, meds_concept_schema)
    ontology.bin_measurements(meds_events)
    ontology.bin_text_values(meds_events)
    ontology.rollup_concepts(meds_events, meds_concept_schema, threshold=0.05)

    # test fields are not none
    assert (ontology.codes is not None)

    # test measurements were not rolled up
    for k in ontology.rollup_map:
        assert (ontology.code_to_event_type[k] is not "measurement")

    # test every target of rollup map is in self.codes
    for k,v in ontology.rollup_map.items():
        assert (v in ontology.codes)

# TODO: test correctness of rollup

# TODO: test coverage of binning

# TODO: test what actually happens when we use qualifiers

# TODO: test that save and load round-trips

# TODO: test schemas