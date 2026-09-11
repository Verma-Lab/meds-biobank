import pytest
import pyspark.sql.functions as F
from meds_biobank.etl_pipelines.omop_meds import extract_events, gather_events, prune_events, post_process_events, format_events, create_concept_schema, extract_splits
from meds_biobank.etl_pipelines.omop_meds import _extract_demographics_events
from meds_biobank.standardizers.standardizers import standardize_measurement_concept_id, standardize_numeric_values_and_units, standardize_text_value
from meds_biobank import schemas, concepts
from pyspark.testing import assertSchemaEqual
from datetime import datetime

@pytest.fixture(scope="session")
def standardized_measurement(measurement, concept, concept_ancestor):
    std = standardize_measurement_concept_id(measurement, concept, concept_ancestor)
    std = standardize_numeric_values_and_units(std)
    std = standardize_text_value(std)
    return std

def test_extract_demographics(person):

    # cast source value columns
    for col in person.columns:
        if col.endswith("_source_value"):
            person = person.withColumn(col, F.col(col).cast("string"))

    # all source tables, hash patient id
    events = person.withColumn("patient_id", F.xxhash64(F.col("person_id").cast("string"))) # hash to patient id

    # test extract demo events runs
    events = _extract_demographics_events(events)

    # ensure specific patient information
    person_id = "1"

    # extract just this patients events
    patient_events = events.filter(F.col("patient_id") == F.xxhash64(F.lit(person_id)))

    # ensure there are some
    assert (patient_events.count() > 0)

    # filter to their events only
    patient_events = (
        events
        .filter(F.col("patient_id") == F.xxhash64(F.lit(person_id)))
        .filter(F.col("concept_id") == concepts.OMOP_BIRTH)
    )

    # extract their birthdate and check it is correct
    extracted_birthdate = patient_events.select("time").collect()[0]["time"]
    assert (extracted_birthdate == datetime(1943, 12, 9))

    # extract other fields and ensure correctness
    gender_event_row = patient_events.select(F.col("concept_id") == F.lit(8507))
    assert (gender_event_row.count() == 1)
    race_event_row = patient_events.select(F.col("concept_id") == F.lit(8516))
    assert (race_event_row.count() == 1)
    ethnicity_event_row = patient_events.select(F.col("concept_id") == F.lit(38003563))
    assert (ethnicity_event_row.count() == 1)

    # ensure another specific patient information
    person_id = "19"

    # extract just this patients events
    patient_events = events.filter(F.col("patient_id") == F.xxhash64(F.lit(person_id)))

    # ensure there are some
    assert (patient_events.count() > 0)

    # filter to their events only
    patient_events = (
        events
        .filter(F.col("patient_id") == F.xxhash64(F.lit(person_id)))
        .filter(F.col("concept_id") == concepts.OMOP_BIRTH)
    )

    # extract their birthdate and check it is correct
    extracted_birthdate = patient_events.select("time").collect()[0]["time"]
    assert (extracted_birthdate == datetime(1969, 8, 26))

    # extract other fields and ensure correctness
    gender_event_row = patient_events.select(F.col("concept_id") == F.lit(8532))
    assert (gender_event_row.count() == 1)
    race_event_row = patient_events.select(F.col("concept_id") == F.lit(8557))
    assert (race_event_row.count() == 1)
    ethnicity_event_row = patient_events.select(F.col("concept_id") == F.lit(38003564))
    assert (ethnicity_event_row.count() == 1)


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

    # ensure ancestor/factor content
    diabetes_row = concept_schema.filter(F.col("code") == 201826).collect()[0]
    ancestor_ids = {a["ancestor_concept_id"] for a in diabetes_row["ancestors"]}
    assert 201820 in ancestor_ids
    assert 201820 in diabetes_row["factors"]


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

    # ensure ancestor/factor content
    diabetes_row = concept_schema.filter(F.col("code") == 201826).collect()[0]
    ancestor_ids = {a["ancestor_concept_id"] for a in diabetes_row["ancestors"]}
    assert 201820 in ancestor_ids
    assert 201820 in diabetes_row["factors"]

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

    # percentages must sum to 1.0
    with pytest.raises(ValueError):
        extract_splits(spark, person, train=0.5, val=0.1, test=0.1, task=0.1)