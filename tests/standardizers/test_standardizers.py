from meds_biobank.standardizers.standardizers import standardize_measurement_concept_id, standardize_measurement_concept_id_fast, standardize_numeric_values_and_units, standardize_text_value
from meds_biobank import schemas
import pyspark.sql.functions as F
from pyspark.sql.types import StringType, IntegerType, DoubleType
from pyspark.testing import assertSchemaEqual
import pytest

def test_standardizers(spark, measurement, concept, concept_ancestor):

    # test schema
    assertSchemaEqual(measurement.schema, schemas.OMOP_MEASUREMENT_SCHEMA, ignoreColumnOrder=True)

    # standardize measurement concept id
    std = standardize_measurement_concept_id_fast(measurement, concept, concept_ancestor)

    # standardize nums and units
    std = standardize_numeric_values_and_units(std)

    # test that each measurement concept id is associated to no more than one unit
    test_count = (
        std
        .groupBy("measurement_concept_id")
        .agg(F.collect_set("unit_source_value").alias("units"))
        .withColumn("num_units", F.size("units"))
        .filter(F.col("num_units") > 1)
    ).count()
    assert (test_count == 0)

    # test that every measurement id that went in, came out
    all_msmt_ids = measurement.select("measurement_id").distinct()
    std_msmt_ids = std.select("measurement_id").distinct()
    assert (all_msmt_ids.count() == std_msmt_ids.count())

    # standardize text values
    std = standardize_text_value(std)

    # test schema
    assertSchemaEqual(std.schema, schemas.STD_OMOP_MEASUREMENT_SCHEMA, ignoreColumnOrder=True)

def test_text_standardizers(spark, measurement):

    # standardize text values
    std = standardize_text_value(measurement)

    # test that no numeric values were standardized
    before = measurement.select("measurement_id", F.col("value_source_value").alias("before"))
    after = std.select("measurement_id", F.col("value_source_value").alias("after"))
    compared = before.join(after, "measurement_id")

    # rows that were already numeric should come out identical, untouched
    changed_numeric = compared.filter(
        F.expr("try_cast(before AS DOUBLE)").isNotNull() & (F.col("before") != F.col("after"))
    )
    assert changed_numeric.count() == 0

    # test that every measurement id that went in, came out
    all_msmt_ids = measurement.select("measurement_id").distinct()
    std_msmt_ids = std.select("measurement_id").distinct()
    assert (all_msmt_ids.count() == std_msmt_ids.count())

def test_guards(spark, measurement, concept, concept_ancestor):
    with pytest.raises(ValueError):
        std_msmt = standardize_measurement_concept_id("foo", concept, concept_ancestor)
        std_msmt = standardize_measurement_concept_id(measurement, "foo", concept_ancestor)
        std_msmt = standardize_measurement_concept_id(measurement, concept, "bar")
        std_msmt = standardize_measurement_concept_id(measurement, concept, False)
