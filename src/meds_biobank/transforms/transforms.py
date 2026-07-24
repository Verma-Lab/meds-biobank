import pyspark.sql.functions as F
from pyspark.sql import Window

def etl_labs_vitals(events):
    # TODO: transform labs and vitals units and measurments, tag event_type w/ labs_/vitals_<name>
    pass

def get_labs_mapping(events):
    """
    Args:
        events (pyspark.sql.DataFrame):
            Desc: MEDS events table
            Schema: |patient_id|code|time|end|numeric_value|text_value|unit|event_type|visit_id|

    Returns:
        labs_mapping (pandas.DataFrame):
            Schema: |event_type|code|
            Desc: mapping from code to labs_/vitals_ event_type
    """
    temp = (
        events
        .filter(
            F.col("event_type").startswith("labs_") | F.col("event_type").startswith("vitals_")
        )
        .select("event_type", "code")
        .distinct()
    )
    return temp.toPandas()

def bin_measurements(events):
    """
    Args:
        events (pyspark.sql.DataFrame):
            Desc: MEDS events table
            Schema: |patient_id|code|time|end|numeric_value|text_value|unit|event_type|visit_id|

    Returns:
        events (pyspark.sql.DataFrame):
            Desc:
            Schema: |patient_id|code|time|end|numeric_value|text_value|unit|event_type|visit_id|
        decile_mapping (pandas.DataFrame):
            Schema: |event_type|decile|min_value|max_value|
            Desc: map from table to decile bin values
    
    Notes:
        • Bins based on event_type, NOT code
    """

    # extract labs, vitals, other
    lv = events.filter(
        F.col("event_type").startswith("labs_") | F.col("event_type").startswith("vitals_")
    )
    other = events.filter(
        ~F.col("event_type").startswith("labs_") & ~F.col("event_type").startswith("vitals_")
    )

    # replace all negative measurements with zero
    lv = lv.withColumn(
        "numeric_value",
        F.when(F.col("numeric_value") < 0, 0).otherwise(F.col("numeric_value"))
    )

    # handle all-zero measurement concepts in labs and vitals (erase value)
    w = Window.partitionBy("code")
    lv = lv.withColumn(
        "is_homogeneous",
        F.max("numeric_value").over(w) == F.min("numeric_value").over(w)
    ).withColumn(
        "numeric_value",
        F.when(F.col("is_homogeneous"), F.lit(None)).otherwise(F.col("numeric_value"))
    ).drop("is_homogeneous")

    # transform values via log1p
    lv = lv.withColumn("numeric_value", F.log1p(F.col("numeric_value")))

    # split zero / nonzero / null so ntile only sees nonzero values
    zero_df = lv.filter(F.col("numeric_value") == 0)
    nonzero_df = lv.filter(F.col("numeric_value").isNotNull() & (F.col("numeric_value") != 0))
    null_df = lv.filter(F.col("numeric_value").isNull())

    # bucketize
    w_ntile = Window.partitionBy("event_type").orderBy("numeric_value")
    nonzero_df = nonzero_df.withColumn("decile", F.ntile(10).over(w_ntile))  # 1-10
    zero_df = zero_df.withColumn("decile", F.lit(0))
    null_df = null_df.withColumn("decile", F.lit(None).cast("int"))

    # rejoin
    lv = zero_df.unionByName(nonzero_df).unionByName(null_df)

    # bin -> value range mapping, keyed on code+event_type+decile
    mapping = (
        lv.filter(F.col("decile").isNotNull())
        .groupBy("event_type", "decile")
        .agg(F.min("numeric_value").alias("min_value"), F.max("numeric_value").alias("max_value"))
        .orderBy("event_type", "decile")
    )

    # clean up
    lv = lv.withColumn("numeric_value", F.col("decile")).drop("decile")

    # final rejoin
    events_out = other.unionByName(lv, allowMissingColumns=True)

    return events_out, mapping.toPandas()

def rollup_concepts(events, concept_ancestor):
    """
    Args:
        events (pyspark.sql.DataFrame):
            Desc: MEDS events table
            Schema: |patient_id|code|time|end|numeric_value|text_value|unit|event_type|visit_id|
        concept_ancestor (pyspark.sql.DataFrame):
            Desc:
            Schema:

    Returns:
        events (pyspark.sql.DataFrame):
            Desc:
            Schema: |patient_id|code|time|end|numeric_value|text_value|unit|event_type|visit_id|
    
    Notes:
        • Do not roll up labs and vitals
    """
    # TODO: option to rollup infrequent concepts into frequent ancestors, and optionally drop non-mapped
    pass