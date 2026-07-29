from meds_biobank.etl_pipelines.pmbb_meds import extract_events, gather_events, prune_events, post_process_events, format_events

def test_omop_pipeline(
    spark,
    condition_occurrence,
    death,
    drug_exposure,
    measurement,
    observation,
    person,
    procedure_occurrence,
    visit_occurrence
):

    # collect tables into list
    tables = [condition_occurrence, death, drug_exposure, observation, person, procedure_occurrence, visit_occurrence]
    names = ["condition_occurrence", "death", "drug_exposure", "observation", "person", "procedure_occurrence", "visit_occurrence"]

    # TEST: extract events
    event_dfs = []
    for table, name in zip(tables, names):
        result = extract_events(table, name)
        assert set(result.columns) == {"patient_id", "time", "code", "event_type", "value", "end", "visit_id", "unit", "measurement_id"}
        result.collect()
        event_dfs.append(result)
    
    # extract measurement events separately
    measurement_events = extract_events(measurement, "measurement")
    assert set(measurement_events.columns) == {"patient_id", "time", "code", "event_type", "value", "end", "visit_id", "unit", "measurement_id"}
    measurement_events.collect()
    
    # TEST: gather events
    gathered_events = gather_events(event_dfs, measurement_events)
    assert set(gathered_events.columns) == {"patient_id", "time", "code", "event_type", "value", "end", "visit_id", "unit"}
    gathered_events.collect()
    
    # TEST: prune events
    pruned_events = prune_events(gathered_events)
    assert set(pruned_events.columns) == {"patient_id", "time", "code", "event_type", "value", "end", "visit_id", "unit"}
    pruned_events.collect()

    # TEST: post-process events
    post_processed_events = post_process_events(pruned_events)
    assert set(post_processed_events.columns) == {"patient_id", "time", "code", "event_type", "text_value", "numeric_value", "end", "visit_id", "unit"}
    post_processed_events.collect()

    # TEST: format events
    formatted_events = format_events(post_processed_events)
    assert set(formatted_events.columns) == {"patient_id", "time", "code", "event_type", "text_value", "numeric_value", "end", "visit_id", "unit"}
    formatted_events.collect()

def test_pmbb_omop_pipeline(
    spark,
    pmbb_condition_occurrence,
    pmbb_death,
    pmbb_drug_exposure,
    pmbb_measurement,
    pmbb_observation,
    pmbb_person,
    pmbb_procedure_occurrence,
    pmbb_visit_occurrence,
    pmbb_labs,
    pmbb_vitals
):
    # collect tables into list
    tables = [
        pmbb_condition_occurrence,
        pmbb_death,
        pmbb_drug_exposure,
        pmbb_observation,
        pmbb_person,
        pmbb_procedure_occurrence,
        pmbb_visit_occurrence
    ]
    table_names = ["condition_occurrence", "death", "drug_exposure", "observation", "person", "procedure_occurrence", "visit_occurrence"]
    lab_names = ["labs_creatinine", "labs_glucose", "labs_hba1c"]
    vital_names = ["vitals_heart_rate", "vitals_spo2", "vitals_systolic_bp"]

    # TEST: extract events
    event_dfs = []
    for table, name in zip(tables, table_names):
        result = extract_events(table, name)
        assert set(result.columns) == {"patient_id", "time", "code", "event_type", "value", "end", "visit_id", "unit", "measurement_id"}
        result.collect()
        event_dfs.append(result)
    for table, name in zip(pmbb_labs, lab_names):
        result = extract_events(table, name)
        assert set(result.columns) == {"patient_id", "time", "code", "event_type", "value", "end", "visit_id", "unit", "measurement_id"}
        result.collect()
        event_dfs.append(result)
    for table, name in zip(pmbb_vitals, vital_names):
        result = extract_events(table, name)
        assert set(result.columns) == {"patient_id", "time", "code", "event_type", "value", "end", "visit_id", "unit", "measurement_id"}
        result.collect()
        event_dfs.append(result)

    # extract measurement events separately
    measurement_events = extract_events(pmbb_measurement, "measurement")
    assert set(measurement_events.columns) == {"patient_id", "time", "code", "event_type", "value", "end", "visit_id", "unit", "measurement_id"}
    measurement_events.collect()

    # TEST: gather events
    gathered_events = gather_events(event_dfs, measurement_events)
    assert set(gathered_events.columns) == {"patient_id", "time", "code", "event_type", "value", "end", "visit_id", "unit"}
    gathered_events.collect()

    # TEST: prune events
    pruned_events = prune_events(gathered_events)
    assert set(pruned_events.columns) == {"patient_id", "time", "code", "event_type", "value", "end", "visit_id", "unit"}
    pruned_events.collect()

    # TEST: post-process events
    post_processed_events = post_process_events(pruned_events)
    assert set(post_processed_events.columns) == {"patient_id", "time", "code", "event_type", "text_value", "numeric_value", "end", "visit_id", "unit"}
    post_processed_events.collect()

    # TEST: format events
    formatted_events = format_events(post_processed_events)
    assert set(formatted_events.columns) == {"patient_id", "time", "code", "event_type", "text_value", "numeric_value", "end", "visit_id", "unit"}
    formatted_events.collect()