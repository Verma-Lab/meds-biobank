from pyspark.sql import SparkSession
from pyspark.sql.types import (
    StructType, StructField, DecimalType,
    LongType, IntegerType, DoubleType,
    DateType, TimestampType, StringType,
    ArrayType
)

OMOP_CONCEPT_SCHEMA = StructType([
    StructField("concept_id", IntegerType(), False),
    StructField("concept_name", StringType(), False),
    StructField("domain_id", StringType(), False),
    StructField("vocabulary_id", StringType(), False),
    StructField("concept_class_id", StringType(), False),
    StructField("standard_concept", StringType(), True),
    StructField("concept_code", StringType(), False),
    StructField("valid_start_date", DateType(), False),
    StructField("valid_end_date", DateType(), False),
    StructField("invalid_reason", StringType(), True),
])

OMOP_CONCEPT_ANCESTOR_SCHEMA = StructType([
    StructField("ancestor_concept_id", IntegerType(), False),
    StructField("descendant_concept_id", IntegerType(), False),
    StructField("min_levels_of_separation", IntegerType(), False),
    StructField("max_levels_of_separation", IntegerType(), False),
])

OMOP_MEASUREMENT_SCHEMA = StructType([
    StructField("measurement_id", LongType(), False),
    StructField("person_id", StringType(), False),
    StructField("measurement_concept_id", IntegerType(), False),
    StructField("measurement_date", DateType(), False),
    StructField("measurement_datetime", TimestampType(), True),
    StructField("measurement_time", StringType(), True),
    StructField("measurement_type_concept_id", IntegerType(), True),
    StructField("operator_concept_id", IntegerType(), True),
    StructField("value_as_number", DoubleType(), True),
    StructField("value_as_concept_id", IntegerType(), True),
    StructField("unit_concept_id", IntegerType(), True),
    StructField("range_low", DoubleType(), True),
    StructField("range_high", DoubleType(), True),
    StructField("provider_id", LongType(), True),
    StructField("visit_occurrence_id", IntegerType(), True),
    StructField("visit_detail_id", IntegerType(), True),
    StructField("measurement_source_value", StringType(), True),
    StructField("measurement_source_concept_id", IntegerType(), True),
    StructField("unit_source_value", StringType(), True),
    StructField("unit_source_concept_id", IntegerType(), True),
    StructField("value_source_value", StringType(), True),
    StructField("meas_event_field_concept_id", IntegerType(), True),
])

STD_OMOP_MEASUREMENT_SCHEMA = StructType([
    StructField("measurement_id", LongType(), False),
    StructField("person_id", StringType(), False),
    StructField("measurement_concept_id", IntegerType(), False),
    StructField("measurement_date", DateType(), False),
    StructField("measurement_datetime", TimestampType(), True),
    StructField("measurement_time", StringType(), True),
    StructField("measurement_type_concept_id", IntegerType(), True),
    StructField("operator_concept_id", IntegerType(), True),
    StructField("value_as_number", DoubleType(), True),
    StructField("value_as_concept_id", IntegerType(), True),
    StructField("unit_concept_id", IntegerType(), True),
    StructField("range_low", DoubleType(), True),
    StructField("range_high", DoubleType(), True),
    StructField("provider_id", LongType(), True),
    StructField("visit_occurrence_id", IntegerType(), True),
    StructField("visit_detail_id", IntegerType(), True),
    StructField("measurement_source_value", StringType(), True),
    StructField("measurement_source_concept_id", IntegerType(), True),
    StructField("unit_source_value", StringType(), True),
    StructField("unit_source_concept_id", IntegerType(), True),
    StructField("value_source_value", StringType(), True),
    StructField("meas_event_field_concept_id", IntegerType(), True),
    StructField("numeric_value_std", DoubleType(), True),
    StructField("unit_std", StringType(), True),
    StructField("text_value_std", StringType(), True),
    StructField("concept_id_std", IntegerType(), False),
])

OMOP_PERSON_SCHEMA = StructType([
    StructField("person_id", StringType(), False),
    StructField("gender_concept_id", IntegerType(), False),
    StructField("year_of_birth", IntegerType(), False),
    StructField("month_of_birth", IntegerType(), True),
    StructField("day_of_birth", IntegerType(), True),
    StructField("birth_datetime", TimestampType(), True),
    StructField("race_concept_id", IntegerType(), False),
    StructField("ethnicity_concept_id", IntegerType(), False),
    StructField("location_id", LongType(), True),
    StructField("provider_id", LongType(), True),
    StructField("care_site_id", LongType(), True),
    StructField("person_source_value", StringType(), True),
    StructField("gender_source_value", StringType(), True),
    StructField("gender_source_concept_id", IntegerType(), True),
    StructField("race_source_value", StringType(), True),
    StructField("race_source_concept_id", IntegerType(), True),
    StructField("ethnicity_source_value", StringType(), True),
    StructField("ethnicity_source_concept_id", IntegerType(), True),
])

OMOP_DEATH_SCHEMA = StructType([
    StructField("person_id", StringType(), False),
    StructField("death_date", DateType(), False),
    StructField("death_datetime", TimestampType(), True),
    StructField("death_type_concept_id", IntegerType(), True),
    StructField("cause_concept_id", IntegerType(), True),
    StructField("cause_source_value", StringType(), True),
    StructField("cause_source_concept_id", IntegerType(), True),
])

OMOP_VISIT_OCCURRENCE_SCHEMA = StructType([
    StructField("visit_occurrence_id", IntegerType(), False),
    StructField("person_id", StringType(), False),
    StructField("visit_concept_id", IntegerType(), False),
    StructField("visit_start_date", DateType(), False),
    StructField("visit_start_datetime", TimestampType(), True),
    StructField("visit_end_date", DateType(), False),
    StructField("visit_end_datetime", TimestampType(), True),
    StructField("visit_type_concept_id", IntegerType(), False),
    StructField("provider_id", LongType(), True),
    StructField("care_site_id", LongType(), True),
    StructField("visit_source_value", StringType(), True),
    StructField("visit_source_concept_id", IntegerType(), True),
    StructField("admitted_from_concept_id", IntegerType(), True),
    StructField("admitted_from_source_value", StringType(), True),
    StructField("discharge_to_concept_id", IntegerType(), True), # in OMOP 5.4 this became discharged
    StructField("discharge_to_source_value", StringType(), True), # in OMOP 5.4 this became discharged
    StructField("preceding_visit_occurrence_id", IntegerType(), True),
])

OMOP_CONDITION_OCCURRENCE_SCHEMA = StructType([
    StructField("condition_occurrence_id", LongType(), False),
    StructField("person_id", StringType(), False),
    StructField("condition_concept_id", IntegerType(), False),
    StructField("condition_start_date", DateType(), False),
    StructField("condition_start_datetime", TimestampType(), True),
    StructField("condition_end_date", DateType(), True),
    StructField("condition_end_datetime", TimestampType(), True),
    StructField("condition_type_concept_id", IntegerType(), False),
    StructField("condition_status_concept_id", IntegerType(), True),
    StructField("stop_reason", StringType(), True),
    StructField("provider_id", LongType(), True),
    StructField("visit_occurrence_id", IntegerType(), True),
    StructField("visit_detail_id", IntegerType(), True),
    StructField("condition_source_value", StringType(), True),
    StructField("condition_source_concept_id", IntegerType(), True),
    StructField("condition_status_source_value", StringType(), True),
])

OMOP_DRUG_EXPOSURE_SCHEMA = StructType([
    StructField("drug_exposure_id", LongType(), False),
    StructField("person_id", StringType(), False),
    StructField("drug_concept_id", IntegerType(), False),
    StructField("drug_exposure_start_date", DateType(), False),
    StructField("drug_exposure_start_datetime", TimestampType(), True),
    StructField("drug_exposure_end_date", DateType(), False),
    StructField("drug_exposure_end_datetime", TimestampType(), True),
    StructField("verbatim_end_date", DateType(), True),
    StructField("drug_type_concept_id", IntegerType(), False),
    StructField("stop_reason", StringType(), True),
    StructField("refills", IntegerType(), True),
    StructField("quantity", DoubleType(), True),
    StructField("days_supply", IntegerType(), True),
    StructField("sig", StringType(), True),
    StructField("route_concept_id", IntegerType(), True),
    StructField("lot_number", StringType(), True),
    StructField("provider_id", LongType(), True),
    StructField("visit_occurrence_id", IntegerType(), True),
    StructField("visit_detail_id", IntegerType(), True),
    StructField("drug_source_value", StringType(), True),
    StructField("drug_source_concept_id", IntegerType(), True),
    StructField("route_source_value", StringType(), True),
    StructField("dose_unit_source_value", StringType(), True),
])

OMOP_PROCEDURE_OCCURRENCE_SCHEMA = StructType([
    StructField("procedure_occurrence_id", LongType(), False),
    StructField("person_id", StringType(), False),
    StructField("procedure_concept_id", IntegerType(), False),
    StructField("procedure_date", DateType(), False),
    StructField("procedure_datetime", TimestampType(), True),
    StructField("procedure_end_date", DateType(), True),
    StructField("procedure_end_datetime", TimestampType(), True),
    StructField("procedure_type_concept_id", IntegerType(), False),
    StructField("modifier_concept_id", IntegerType(), True),
    StructField("quantity", IntegerType(), True),
    StructField("provider_id", LongType(), True),
    StructField("visit_occurrence_id", IntegerType(), True),
    StructField("visit_detail_id", IntegerType(), True),
    StructField("procedure_source_value", StringType(), True),
    StructField("procedure_source_concept_id", IntegerType(), True),
    StructField("modifier_source_value", StringType(), True),
])

OMOP_OBSERVATION_SCHEMA = StructType([
    StructField("observation_id", LongType(), False),
    StructField("person_id", StringType(), False),
    StructField("observation_concept_id", IntegerType(), False),
    StructField("observation_date", DateType(), False),
    StructField("observation_datetime", TimestampType(), True),
    StructField("observation_type_concept_id", IntegerType(), False),
    StructField("value_as_number", DoubleType(), True),
    StructField("value_as_string", StringType(), True),
    StructField("value_as_concept_id", IntegerType(), True),
    StructField("qualifier_concept_id", IntegerType(), True),
    StructField("unit_concept_id", IntegerType(), True),
    StructField("provider_id", LongType(), True),
    StructField("visit_occurrence_id", IntegerType(), True),
    StructField("visit_detail_id", IntegerType(), True),
    StructField("observation_source_value", StringType(), True),
    StructField("observation_source_concept_id", IntegerType(), True),
    StructField("unit_source_value", StringType(), True),
    StructField("qualifier_source_value", StringType(), True),
    StructField("value_source_value", StringType(), True),
    StructField("observation_event_id", LongType(), True),
    StructField("obs_event_field_concept_id", IntegerType(), True),
])

PMBB_VISIT_OCCURRENCE_SUPPLEMENT_SCHEMA = StructType([
    StructField("visit_occurrence_id", IntegerType(), False),
    StructField("visit_source_value", StringType(), False),
    StructField("base_class", StringType(), True),
    StructField("encounter_type", StringType(), True),
    StructField("IsCancel", IntegerType(), True),
    StructField("IsHospitalAdmission", IntegerType(), True),
    StructField("IsInpatientAdmission", IntegerType(), True),
    StructField("IsObservation", IntegerType(), True),
    StructField("IsEdVisit", IntegerType(), True),
    StructField("IsOutpatientFaceToFaceVisit", IntegerType(), True),
    StructField("IsVideoVisit", IntegerType(), True),
])

# add MEDS events schema
MEDS_EVENT_SCHEMA = StructType([
    StructField("patient_id", LongType(), False),
    StructField("code", IntegerType(), False),
    StructField("time", TimestampType(), False),
    StructField("event_type", StringType(), False),
    StructField("end", TimestampType(), True),
    StructField("numeric_value", DoubleType(), True),
    StructField("unit", StringType(), True),
    StructField("text_value", StringType(), True),
    StructField("visit_id", IntegerType(), True)
])

# add meds concept schema
MEDS_CONCEPT_SCHEMA = StructType([
    StructField("code", IntegerType(), False),
    StructField("name", StringType(), False),
    StructField(
        "ancestors",
        ArrayType(StructType([
            StructField("ancestor_concept_id", IntegerType(), False),
            StructField("min_levels_of_separation", IntegerType(), False)
        ]))
    ),
    StructField("factors", ArrayType(IntegerType()))
])

# TODO: add meds split schema

# TODO: add meds task schema(s)