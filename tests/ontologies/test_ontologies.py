import pytest
from meds_biobank.standardizers.standardizers import standardize
from meds_biobank.etl_pipelines.omop_meds import extract_events, gather_events, prune_events, post_process_events, format_events, create_concept_schema
from meds_biobank.ontologies.ontologies import Ontology
from dotenv import load_dotenv
from pathlib import Path
import os

@pytest.fixture(scope="session")
def standardized_measurement(measurement, concept, concept_ancestor):
    return standardize(measurement, concept, concept_ancestor)

@pytest.fixture(scope="session")
def meds_events(
    spark,
    concept,
    concept_ancestor,
    condition_occurrence,
    death,
    drug_exposure,
    standardized_measurement,
    observation,
    person,
    procedure_occurrence,
    visit_occurrence
):
    # register OMOP data tables
    tables = [
       condition_occurrence,
        death,
        drug_exposure,
        observation,
        person,
        procedure_occurrence,
        visit_occurrence,
        standardized_measurement
    ]

    # record table names
    table_names = ["condition_occurrence", "death", "drug_exposure", "observation", "person", "procedure_occurrence", "visit_occurrence", "measurement"]

    # extract events
    event_dfs = []
    for table, name in zip(tables, table_names):
        result = extract_events(table, name, measurements_prestandardized=False)
        event_dfs.append(result)

    # gather events together
    gathered_events = gather_events(event_dfs)

    # prune events
    pruned_events = prune_events(gathered_events)

    # post process events
    post_processed_events = post_process_events(pruned_events)

    # format events
    formatted_events = format_events(post_processed_events)

    return formatted_events

@pytest.fixture(scope="session")
def meds_concept_schema(
    spark,
    concept,
    concept_ancestor,
    meds_events
):

    # compute concept schema
    concept_schema = create_concept_schema(meds_events, concept, concept_ancestor)

    return concept_schema

def test_ontologies(spark, meds_events, meds_concept_schema):
    ontology = Ontology()
    ontology.compute_concept_ontology(meds_events, meds_concept_schema)
    ontology.bin_measurements(meds_events)
    ontology.rollup_concepts(meds_events, meds_concept_schema, threshold=0.05)