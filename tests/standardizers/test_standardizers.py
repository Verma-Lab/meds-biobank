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

def test_unit_ladder_not_crossed(spark):
    # same measurement_concept_id, majority in mmol/L, one row in Unit/L -- both map to
    # canonical factor 1.0 in the flat conversion table despite being unrelated units
    rows = [
        (1, 999, "5.0", "mmol/L"),
        (2, 999, "5.1", "mmol/L"),
        (3, 999, "4.9", "mmol/L"),
        (4, 999, "10.0", "Unit/L"),
    ]
    df = spark.createDataFrame(rows, ["measurement_id", "measurement_concept_id", "value_source_value", "unit_source_value"])

    std = standardize_numeric_values_and_units(df, mcid_standardized=False)

    # the Unit/L row must not be silently relabeled as mmol/L just because both hit factor 1.0
    row4 = std.filter(F.col("measurement_id") == 4).collect()[0]
    assert row4["unit_std"] is None
    assert row4["numeric_value_std"] is None

    # the mmol/L rows should still convert/pass through normally
    mmol_rows = std.filter(F.col("unit_source_value") == "mmol/L").collect()
    assert all(r["unit_std"] == "mmol/L" for r in mmol_rows)

def test_guards(spark, measurement, concept, concept_ancestor):
    with pytest.raises(ValueError):
        std_msmt = standardize_measurement_concept_id("foo", concept, concept_ancestor)
        std_msmt = standardize_measurement_concept_id(measurement, "foo", concept_ancestor)
        std_msmt = standardize_measurement_concept_id(measurement, concept, "bar")
        std_msmt = standardize_measurement_concept_id(measurement, concept, False)
