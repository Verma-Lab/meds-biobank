from meds_biobank.standardizers.standardizers import standardize, autostd
from meds_biobank import schemas
import pyspark.sql.functions as F
from pyspark.sql.types import StringType, IntegerType, DoubleType
from pyspark.testing import assertSchemaEqual
import pytest

def test_standardizers(spark, measurement, concept, concept_ancestor):

    # test schema
    assertSchemaEqual(measurement.schema, schemas.OMOP_MEASUREMENT_SCHEMA, ignoreColumnOrder=True)

    # run autostd function
    autostd_msmt = autostd(measurement)

    # test schema
    assertSchemaEqual(autostd_msmt.schema, schemas.STD_OMOP_MEASUREMENT_SCHEMA, ignoreColumnOrder=True)

    # test that each measurement concept id is associated to no more than one unit
    test_count = (
        autostd_msmt
        .groupBy("std_concept_id")
        .agg(F.collect_set("unit_converted").alias("units"))
        .withColumn("num_units", F.size("units"))
        .filter(F.col("num_units") > 1)
    ).count()
    assert (test_count == 0)

    # test that every measurement id that went in, came out
    all_msmt_ids = measurement.select("measurement_id").distinct()
    autostd_msmt_ids = autostd_msmt.select("measurement_id").distinct()
    assert (all_msmt_ids.count() == autostd_msmt_ids.count())

    # run standardization function
    std_msmt = standardize(measurement, concept, concept_ancestor)
    
    # test that each measurement concept id is associated to no more than one unit
    test_count = (
        std_msmt
        .groupBy("std_concept_id")
        .agg(F.collect_set("unit_converted").alias("units"))
        .withColumn("num_units", F.size("units"))
        .filter(F.col("num_units") > 1)
    ).count()
    assert (test_count == 0)

    # test that every measurement id that went in, came out
    std_msmt_ids = std_msmt.select("measurement_id").distinct()
    assert (all_msmt_ids.count() == std_msmt_ids.count())

    # ensure no null std concept ids
    assert (std_msmt.filter(F.col("std_concept_id").isNull()).count() == 0)

    # test schema
    assertSchemaEqual(std_msmt.schema, schemas.STD_OMOP_MEASUREMENT_SCHEMA, ignoreColumnOrder=True)

def test_malformed_input_types(spark, measurement, concept, concept_ancestor):
    with pytest.raises(ValueError):
        std_msmt = standardize("foo", concept, concept_ancestor)
        std_msmt = standardize(measurement, "foo", concept_ancestor)
        std_msmt = standardize(measurement, concept, "bar")
        std_msmt = standardize(measurement, concept, False)
        std_msmt = autostd("foo")
        std_msmt = autostd(True)
