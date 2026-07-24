from meds_biobank.transforms.transforms import get_labs_mapping, bin_measurements

def test_get_labs_mapping(spark, meds_events):
    labs_mapping = get_labs_mapping(meds_events)
    assert set(labs_mapping.columns) == {"event_type", "code"}

def test_bin_measurements(spark, meds_events):
    binned_events, decile_mapping = bin_measurements(meds_events)
    assert set(binned_events.columns) == {"patient_id", "time", "code", "event_type", "text_value", "numeric_value", "end", "visit_id", "unit"}
    binned_events.collect()