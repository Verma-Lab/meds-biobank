import pyspark.sql.functions as F
from pyspark.sql import Window

def standardize(msmt, concept_ancestor):
    """
    - For each mtype, filter df and homogenize unit and value

    Args:
        msmt (pyspark.sql.DataFrame):
            Desc: OMOP measurements table
            Schema:
                measurement_id:long
                person_id:string
                measurement_concept_id:integer
                measurement_date:date
                measurement_datetime:timestamp
                measurement_type_concept_id:integer
                operator_concept_id:integer
                value_as_number:decimal(10,3)
                value_as_concept_id:integer
                unit_concept_id:integer
                range_low:decimal(24,3)
                range_high:decimal(24,3)
                visit_occurrence_id:long
                measurement_source_value:string
                measurement_source_concept_id:integer
                unit_source_value:string
                value_source_value:string
        concept_ancestor (pyspark.sql.DataFrame): 
            Desc: ...
            Schema: ancestor_concept_id|descendant_concept_id|min_levels_of_separation|max_levels_of_separation|

    Returns:
        measurements (pyspark.sql.DataFrame):
            Desc: OMOP measurements table
    """

    # FUTURE TODO: correct text values

    # TODO: extract values for explicitly named types

    # join msmt with concept ancestor
    msmt = msmt.join(
        concept_ancestor,
        msmt.measurement_concept_id == concept_ancestor.descendant_concept_id,
        "inner"
    )

    # alt
    labs_alt = (
        msmt
        .filter(F.col("ancestor_concept_id") == 40652525)
        .withColumn("std_concept_id", F.lit(40652525))
        .withColumn(
            "value_converted",
            F.when(
                F.lower(F.col("unit_source_value")).rlike('^.*i?u.*/l$'),
                F.round(F.try_cast(F.col("value_source_value"), "double"), 3)
            ).otherwise(F.lite(None))
        )
        .withColumn("unit_converted", F.lit("Unit/L"))
    )

    # albumin
    labs_albumin = (
        msmt
        .filter(F.col("ancestor_concept_id") == 40652534)
        .withColumn("std_concept_id", F.lit(40652534))
        .withColumn(
            "value_converted",
            F.when(
                F.lower(F.col("unit_source_value")).rlike('^gm?/dl$'),
                F.round(F.try_cast(F.col("value_source_value"), "double"), 3)
            ).when(
                F.lower(F.col("unit_source_value")).rlike('mg/dl'),
                F.round(F.try_cast(F.col("value_source_value"), "double")/1000, 3)
            )
            .when(
                F.lower(F.col("unit_source_value")).rlike('mg/l'),
                F.round(F.try_cast(F.col("value_source_value"), "double")/1000, 3)
            ).otherwise(F.lit(None))
        )
        .withColumn("unit_converted", F.lit("g/dL"))
    )

    # alp
    labs_alp = (
        msmt
        .filter(F.col("ancestor_concept_id") == 40652549)
        .withColumn("std_concept_id", F.lit(40652549))
        .withColumn(
            "value_converted",
            F.when(
                F.lower(F.col("unit_source_value")).rlike('^.*i?u.*/l$'),
                F.round(F.try_cast(F.col("value_source_value"), "double"), 3)
            ).otherwise(F.lite(None))
        )
        .withColumn("unit_converted", F.lit("Unit/L"))
    )

    # anion gap
    labs_anion_gap = (
        msmt
        .filter(F.col("ancestor_concept_id") == 40652611)
        .withColumn("std_concept_id", F.lit(40652611))
        .withColumn(
            "value_converted",
            F.when(
                F.lower(F.col("unit_source_value")).rlike('mmo%/l'),
                F.round(F.try_cast(F.col("value_source_value"), "double"), 3)
            ).otherwise(F.lite(None))
        )
        .withColumn("unit_converted", F.lit("mmol/L"))
    )

    # apo_b
    labs_apo_b = (
        msmt
        .filter(F.col("ancestor_concept_id") == 40652616)
        .withColumn("std_concept_id", F.lit(40652616))
        .withColumn(
            "value_converted",
            F.when(
                F.lower()
            )
        )
    )

    # ...

    # TODO: perform fallback logic for all unmapped types (lacks std concept id)
    std_concept_ids = ...