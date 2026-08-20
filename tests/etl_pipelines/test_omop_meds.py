import pytest
import pyspark.sql.functions as F
from meds_biobank.etl_pipelines.omop_meds import extract_events, gather_events, prune_events, post_process_events, format_events, create_concept_schema
from meds_biobank.standardizers.standardizers import standardize

@pytest.fixture(scope="session")
def standardized_measurement(measurement, concept, concept_ancestor):
    return standardize(measurement, concept, concept_ancestor)

def test_extract_meds_events(
    concept,
    concept_ancestor,
    condition_occurrence,
    death,
    drug_exposure,
    measurement,
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
        measurement
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

    # compute concept schema
    concept_schema = create_concept_schema(formatted_events, concept, concept_ancestor)

    # ensure that each code maps to exactly one event type
    test_count = (
        formatted_events
        .groupBy("code")
        .agg(F.collect_set("event_type").alias("event_types"))
        .withColumn("num_event_types", F.size("event_types"))
        .filter(F.col("num_event_types") != 1)
    ).count()
    assert (test_count == 0)

def test_extract_std_meds_events(
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
        result = extract_events(table, name, measurements_prestandardized=True)
        event_dfs.append(result)

    # gather events together
    gathered_events = gather_events(event_dfs)

    # prune events
    pruned_events = prune_events(gathered_events)

    # post process events
    post_processed_events = post_process_events(pruned_events)

    # format events
    formatted_events = format_events(post_processed_events)

    # compute concept schema
    concept_schema = create_concept_schema(formatted_events, concept, concept_ancestor)

    # ensure that each code maps to exactly one event type
    test_count = (
        formatted_events
        .groupBy("code")
        .agg(F.collect_set("event_type").alias("event_types"))
        .withColumn("num_event_types", F.size("event_types"))
        .filter(F.col("num_event_types") != 1)
    ).count()
    assert (test_count == 0)

# TODO: ensure that all required fields are never null

# TODO: ensure that all fields are of correct type

# TODO: add schema tests