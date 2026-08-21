import pytest
import pyspark.sql.functions as F
from meds_biobank.etl_pipelines.omop_meds import extract_events, gather_events, prune_events, post_process_events, format_events, create_concept_schema
from meds_biobank.standardizers.standardizers import standardize_measurement_concept_id, standardize_numeric_values_and_units, standardize_text_value
from meds_biobank import schemas
from pyspark.testing import assertSchemaEqual

@pytest.fixture(scope="session")
def standardized_measurement(measurement, concept, concept_ancestor):
    std = standardize_measurement_concept_id(measurement, concept, concept_ancestor)
    std = standardize_numeric_values_and_units(std)
    std = standardize_text_value(std)
    return std

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
        result = extract_events(table, name)
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

    # add schema tests
    assertSchemaEqual(formatted_events.schema, schemas.MEDS_EVENT_SCHEMA, ignoreColumnOrder=True)
    assertSchemaEqual(concept_schema.schema, schemas.MEDS_CONCEPT_SCHEMA, ignoreColumnOrder=True)

    # ensure that every occurring concept made it into concept schema
    all_codes = formatted_events.select("code").distinct()
    cs_codes = concept_schema.select("code").distinct()
    assert (all_codes.count() == cs_codes.count())


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
        result = extract_events(table, name)
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

    # add schema tests
    assertSchemaEqual(formatted_events.schema, schemas.MEDS_EVENT_SCHEMA, ignoreColumnOrder=True)
    assertSchemaEqual(concept_schema.schema, schemas.MEDS_CONCEPT_SCHEMA, ignoreColumnOrder=True)

    # ensure that every occurring concept made it into concept schema
    all_codes = formatted_events.select("code").distinct()
    cs_codes = concept_schema.select("code").distinct()
    assert (all_codes.count() == cs_codes.count())