from meds_biobank.standardizers.standardizers import standardize, autostd
import pyspark.sql.functions as F
from pyspark.sql.types import StringType, IntegerType, DoubleType

def test_standardizers(spark, measurement, concept, concept_ancestor):

    # run autostd function
    autostd_msmt = autostd(measurement)

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

    # test that field data types are correct
    assert isinstance(std_msmt.schema["std_concept_id"].dataType, IntegerType)
    assert isinstance(std_msmt.schema["value_converted"].dataType, DoubleType)
    assert isinstance(std_msmt.schema["unit_converted"].dataType, StringType)

    # ensure no null std concept ids
    assert (std_msmt.filter(F.col("std_concept_id").isNull()).count() == 0)

# TODO: add schema tests
