import pytest
import pyspark.sql.functions as F
from meds_biobank.etl_pipelines.omop_meds import extract_events, gather_events, prune_events, post_process_events, format_events, create_concept_schema, extract_splits
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

    # schema tests
    assertSchemaEqual(formatted_events.schema, schemas.MEDS_EVENT_SCHEMA, ignoreColumnOrder=True)
    assertSchemaEqual(concept_schema.schema, schemas.MEDS_CONCEPT_SCHEMA, ignoreColumnOrder=True)

    # ensure that every occurring concept made it into concept schema
    all_codes = formatted_events.select("code").distinct()
    cs_codes = concept_schema.select("code").distinct()
    assert (all_codes.count() == cs_codes.count())

    # test no concepts with id 0
    assert (all_codes.filter(F.col("code") == 0).count() == 0)


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

    # schema tests
    assertSchemaEqual(formatted_events.schema, schemas.MEDS_EVENT_SCHEMA, ignoreColumnOrder=True)
    assertSchemaEqual(concept_schema.schema, schemas.MEDS_CONCEPT_SCHEMA, ignoreColumnOrder=True)

    # ensure that every occurring concept made it into concept schema
    all_codes = formatted_events.select("code").distinct()
    cs_codes = concept_schema.select("code").distinct()
    assert (all_codes.count() == cs_codes.count())

    # test no concepts with id 0
    assert (all_codes.filter(F.col("code") == 0).count() == 0)

def test_extract_splits(spark, person):
    # valid split
    splits = extract_splits(spark, person)
    assertSchemaEqual(splits.schema, schemas.MEDS_SPLIT_SCHEMA, ignoreColumnOrder=True)

    # every patient appears exactly once
    n_persons = person.select("person_id").distinct().count()
    assert splits.count() == n_persons
    assert splits.select("patient_id").distinct().count() == n_persons

    # only valid split labels
    labels = {row["split"] for row in splits.select("split").distinct().collect()}
    assert labels <= {"train", "val", "test", "task"}

    # non-DataFrame input rejected
    with pytest.raises(ValueError):
        extract_splits(spark, [1, 2, 3])

    # wrong-schema input rejected
    with pytest.raises(ValueError):
        extract_splits(spark, person.drop("gender_concept_id"))

    # percentages must sum to 1.0
    with pytest.raises(ValueError):
        extract_splits(spark, person, train=0.5, val=0.1, test=0.1, task=0.1)