import pytest
from meds_biobank.etl_pipelines.omop_meds import extract_events, gather_events, prune_events, post_process_events, format_events, create_concept_schema

@pytest.fixture(scope="session")
def meds_events(
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

    return formatted_events

def test_extract_meds_events(meds_events):

    # TODO: test that there are no concept id = 0 rows

    # TODO: test remove nones, delta encode worked
    
    # TODO: ensure that order is ascending by patient, time

    assert True