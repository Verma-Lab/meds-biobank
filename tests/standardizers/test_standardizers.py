from meds_biobank.standardizers.standardizers import standardize, autostd
import pyspark.sql.functions as F

def test_standardizers(spark, measurement, concept, concept_ancestor):

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
    all_msmt_ids = measurement.select("measurement_id").distinct()
    num_msmt_ids = all_msmt_ids.count()
    filt_msmt_ids = std_msmt.select("measurement_id").distinct()
    num_filt_msmt_ids = filt_msmt_ids.count()
    assert (num_msmt_ids == num_filt_msmt_ids) # check count is the same
