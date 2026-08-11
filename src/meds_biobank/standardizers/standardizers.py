import pyspark.sql.functions as F
from pyspark.sql import Window

# TODO: PROMISE - each measurement id only has one unit

COVERED_ANCESTORS = [
    1618784,1621295,1990626,1990650,2212742,4028908,36662633,40652525,40652534,40652549,40652611,40652616,40652640,40652709,
    40652733,40652745,40652796,40652802,40652808,40652867,40652870,40652885,40652982,40652995,40653085,40653141,40653151,
    40653158,40653291,40653357,40653550,40653596,40653598,40653626,40653663,40653762,40653808,40653836,40653862,40653873,
    40653874,40653900,40653984,40653994,40654005,40654016,40654026,40654045,40654064,40654069,40654083,40654086,40654088,
    40654106,40654115,40654162,40654163,40654164,40654168,40654479,40654572,40654576,40654637,40654905,40654984,40655033,
    40655090,40655204,40655429,40655804,40655805,40656057,40656264,40656506,40656529,40656531,40657685,40657691,40657703,
    40657704,40657714
]

COVERED_PARTICULARS = [
    2212731,3004249,3005897,3008939,3009596,3012392,3012888,3014051,3015586,3015688,3018199,3020460,3021614,3022709,3024153,
    3024507,3024763,3027270,3034426,3035511,3037110,3037121,4245997,40760845,44783982
]

def standardize(msmt, concept_ancestor, concept):
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
        concept (pyspark.sql.DataFrame):
            Desc: OMOP concept table
            Schema: concept_id|concept_name|domain_id|vocabulary_id|concept_class_id|standard_concept|concept_code|valid_start_date|valid_end_date|invalid_reason|
        person (pyspark.sql.DataFrame):
            Desc: OMOP person table
            Schema: person_id|gender_concept_id|year_of_birth|month_of_birth|day_of_birth|birth_datetime|race_concept_id|ethnicity_concept_id|location_id|provider_id|care_site_id|person_source_value|gender_source_value|gender_source_concept_id|race_source_value|race_source_concept_id|ethnicity_source_value|ethnicity_source_concept_id|

    Returns:
        measurements (pyspark.sql.DataFrame):
            Desc: OMOP measurements table
    """

    # TODO: map text values?

    # join msmt with concept ancestor
    msmt_ca = msmt.join(
        concept_ancestor,
        msmt.measurement_concept_id == concept_ancestor.descendant_concept_id,
        "inner"
    )

    # alt
    labs_alt = (
        msmt_ca
        .filter(F.col("ancestor_concept_id") == 40652525)
        .withColumn("std_concept_id", F.lit(40652525))
        .withColumn(
            "value_converted",
            F.when(
                F.lower(F.col("unit_source_value")).rlike('^.*i?u.*/l$'),
                F.round(F.try_cast(F.col("value_source_value"), "double"), 3)
            ).otherwise(F.lit(None))
        )
        .withColumn("unit_converted", F.lit("Unit/L"))
    )

    # albumin
    labs_albumin = (
        msmt_ca
        .filter(F.col("ancestor_concept_id") == 40652534)
        .withColumn("std_concept_id", F.lit(40652534))
        .withColumn(
            "value_converted",
            F.when(
                F.lower(F.col("unit_source_value")).rlike('^gm?/dl$'),
                F.round(F.try_cast(F.col("value_source_value"), "double"), 3)
            ).when(
                F.lower(F.col("unit_source_value")) == "mg/dl",
                F.round(F.try_cast(F.col("value_source_value"), "double")/1000, 3)
            )
            .when(
                F.lower(F.col("unit_source_value")) == "mg/l",
                F.round(F.try_cast(F.col("value_source_value"), "double")/10000, 3)
            ).otherwise(F.lit(None))
        )
        .withColumn("unit_converted", F.lit("g/dL"))
    )

    # alp
    labs_alp = (
        msmt_ca
        .filter(F.col("ancestor_concept_id") == 40652549)
        .withColumn("std_concept_id", F.lit(40652549))
        .withColumn(
            "value_converted",
            F.when(
                F.lower(F.col("unit_source_value")).rlike('^.*i?u.*/l$'),
                F.round(F.try_cast(F.col("value_source_value"), "double"), 3)
            ).otherwise(F.lit(None))
        )
        .withColumn("unit_converted", F.lit("Unit/L"))
    )

    # anion gap
    labs_anion_gap = (
        msmt_ca
        .filter(F.col("ancestor_concept_id") == 40652611)
        .withColumn("std_concept_id", F.lit(40652611))
        .withColumn(
            "value_converted",
            F.when(
                F.lower(F.col("unit_source_value")).like('mmo%/l'),
                F.round(F.try_cast(F.col("value_source_value"), "double"), 3)
            ).otherwise(F.lit(None))
        )
        .withColumn("unit_converted", F.lit("mmol/L"))
    )

    # apo_b
    labs_apo_b = (
        msmt_ca
        .filter(F.col("ancestor_concept_id") == 40652616)
        .withColumn("std_concept_id", F.lit(40652616))
        .withColumn(
            "value_converted",
            F.when(
                F.lower(F.col("unit_source_value")).rlike(r'^mg/\s?dl$'),
                F.round(F.try_cast(F.col("value_source_value"), "double"), 3)
            ).when(
                F.lower(F.col("unit_source_value")) == "g/l",
                F.round(F.try_cast(F.col("value_source_value"), "double") / 100, 3)
            ).when(
                F.lower(F.col("unit_source_value")) == "mg/l",
                F.round(F.try_cast(F.col("value_source_value"), "double") / 10, 3)
            ).when(
                F.lower(F.col("unit_source_value")) == "mg/ml",
                F.round(F.try_cast(F.col("value_source_value"), "double") * 100, 3)
            ).otherwise(F.lit(None))
        )
        .withColumn("unit_converted", F.lit("mg/dL"))
    )

    # amh
    labs_amh = (
        msmt_ca
        .filter(F.col("ancestor_concept_id") == 40653357)
        .withColumn("std_concept_id", F.lit(40653357))
        .withColumn(
            "value_converted",
            F.when(
                F.lower(F.col("unit_source_value")) == "ng/ml",
                F.round(F.try_cast(F.col("value_source_value"), "double"), 3)
            ).when(
                F.lower(F.col("unit_source_value")) == "ng/dl",
                F.round(F.try_cast(F.col("value_source_value"), "double") / 100, 3)
            ).when(
                F.lower(F.col("unit_source_value")) == "ng/l",
                F.round(F.try_cast(F.col("value_source_value"), "double") / 1000, 3)
            ).otherwise(F.lit(None))
        )
        .withColumn("unit_converted", F.lit("ng/mL"))
    )

    # ast
    labs_ast = (
        msmt_ca
        .filter(F.col("ancestor_concept_id") == 40652640)
        .withColumn("std_concept_id", F.lit(40652640))
        .withColumn(
            "value_converted",
            F.when(
                F.lower(F.col("unit_source_value")).rlike(r'^.*i?u.*/l$'),
                F.round(F.try_cast(F.col("value_source_value"), "double"), 3)
            ).otherwise(F.lit(None))
        )
        .withColumn("unit_converted", F.lit("Unit/L"))
    )

    # basophils
    labs_basophils = (
        msmt_ca
        .filter(F.col("ancestor_concept_id") == 40653984)
        .withColumn("std_concept_id", F.lit(40653984))
        .withColumn(
            "value_converted",
            F.when(
                F.lower(F.col("unit_source_value")).like("th%/ul")
                | F.lower(F.col("unit_source_value")).like("th%/mcl")
                | F.lower(F.col("unit_source_value")).like("%10%3/ul")
                | F.lower(F.col("unit_source_value")).like("%10%3/mcl")
                | F.lower(F.col("unit_source_value")).rlike(r'^k/[ucm].+$'),
                F.round(F.try_cast(F.col("value_source_value"), "double"), 3)
            ).when(
                F.lower(F.col("unit_source_value")).rlike(r'^cell.*/[ucm].+$'),
                F.round(F.try_cast(F.col("value_source_value"), "double") / 1000, 3)
            ).otherwise(F.lit(None))
        )
        .withColumn("unit_converted", F.lit("Thousand/uL"))
    )

    # beta_globulin
    labs_beta_globulin = (
        msmt_ca
        .filter(F.col("ancestor_concept_id") == 1990626)
        .withColumn("std_concept_id", F.lit(1990626))
        .withColumn(
            "value_converted",
            F.when(
                F.lower(F.col("unit_source_value")).rlike(r'^gm?/dl$'),
                F.round(F.try_cast(F.col("value_source_value"), "double"), 3)
            ).when(
                F.lower(F.col("unit_source_value")) == "mg/dl",
                F.round(F.try_cast(F.col("value_source_value"), "double") / 1000, 3)
            ).when(
                F.lower(F.col("unit_source_value")) == "mg/l",
                F.round(F.try_cast(F.col("value_source_value"), "double") / 10000, 3)
            ).otherwise(F.lit(None))
        )
        .withColumn("unit_converted", F.lit("g/dL"))
    )

    # bilirubin_total
    labs_bilirubin_total = (
        msmt_ca
        .filter(F.col("ancestor_concept_id") == 40652709)
        .withColumn("std_concept_id", F.lit(40652709))
        .withColumn(
            "value_converted",
            F.when(
                F.lower(F.col("unit_source_value")).rlike(r'^mg/\s?dl$'),
                F.round(F.try_cast(F.col("value_source_value"), "double"), 3)
            ).when(
                F.lower(F.col("unit_source_value")) == "g/l",
                F.round(F.try_cast(F.col("value_source_value"), "double") / 100, 3)
            ).when(
                F.lower(F.col("unit_source_value")) == "mg/l",
                F.round(F.try_cast(F.col("value_source_value"), "double") / 10, 3)
            ).when(
                F.lower(F.col("unit_source_value")) == "mg/ml",
                F.round(F.try_cast(F.col("value_source_value"), "double") * 100, 3)
            ).otherwise(F.lit(None))
        )
        .withColumn("unit_converted", F.lit("mg/dL"))
    )

    # bun
    labs_bun = (
        msmt_ca
        .filter(F.col("ancestor_concept_id") == 40653900)
        .withColumn("std_concept_id", F.lit(40653900))
        .withColumn(
            "value_converted",
            F.when(
                F.lower(F.col("unit_source_value")).rlike(r'^mg/\s?dl$'),
                F.round(F.try_cast(F.col("value_source_value"), "double"), 3)
            ).when(
                F.lower(F.col("unit_source_value")) == "g/l",
                F.round(F.try_cast(F.col("value_source_value"), "double") / 100, 3)
            ).when(
                F.lower(F.col("unit_source_value")) == "mg/l",
                F.round(F.try_cast(F.col("value_source_value"), "double") / 10, 3)
            ).when(
                F.lower(F.col("unit_source_value")) == "mg/ml",
                F.round(F.try_cast(F.col("value_source_value"), "double") * 100, 3)
            ).otherwise(F.lit(None))
        )
        .withColumn("unit_converted", F.lit("mg/dL"))
    )

    # crp
    labs_crp = (
        msmt_ca
        .filter((F.col("ancestor_concept_id") == 40652733) | (F.col("measurement_concept_id") == 3020460))
        .withColumn("std_concept_id", F.lit(40652733))
        .withColumn(
            "value_converted",
            F.when(
                F.lower(F.col("unit_source_value")).rlike(r'^mg/\s?dl$'),
                F.round(F.try_cast(F.col("value_source_value"), "double"), 3)
            ).when(
                F.lower(F.col("unit_source_value")) == "g/l",
                F.round(F.try_cast(F.col("value_source_value"), "double") / 100, 3)
            ).when(
                F.lower(F.col("unit_source_value")) == "mg/l",
                F.round(F.try_cast(F.col("value_source_value"), "double") / 10, 3)
            ).when(
                F.lower(F.col("unit_source_value")) == "mg/ml",
                F.round(F.try_cast(F.col("value_source_value"), "double") * 100, 3)
            ).otherwise(F.lit(None))
        )
        .withColumn("unit_converted", F.lit("mg/dL"))
    )

    # crp_hs
    labs_crp_hs = (
        msmt_ca
        .filter(F.col("ancestor_concept_id") == 40654479)
        .withColumn("std_concept_id", F.lit(40654479))
        .withColumn(
            "value_converted",
            F.when(
                F.lower(F.col("unit_source_value")) == "mg/l",
                F.round(F.try_cast(F.col("value_source_value"), "double"), 3)
            ).when(
                F.lower(F.col("unit_source_value")) == "mg/dl",
                F.round(F.try_cast(F.col("value_source_value"), "double") * 10, 3)
            ).otherwise(F.lit(None))
        )
        .withColumn("unit_converted", F.lit("mg/L"))
    )

    # calcium
    labs_calcium = (
        msmt_ca
        .filter(F.col("ancestor_concept_id") == 40652745)
        .withColumn("std_concept_id", F.lit(40652745))
        .withColumn(
            "value_converted",
            F.when(
                F.lower(F.col("unit_source_value")).rlike(r'^mg/\s?dl$'),
                F.round(F.try_cast(F.col("value_source_value"), "double"), 3)
            ).when(
                F.lower(F.col("unit_source_value")) == "g/l",
                F.round(F.try_cast(F.col("value_source_value"), "double") / 100, 3)
            ).when(
                F.lower(F.col("unit_source_value")) == "mg/l",
                F.round(F.try_cast(F.col("value_source_value"), "double") / 10, 3)
            ).when(
                F.lower(F.col("unit_source_value")) == "mg/ml",
                F.round(F.try_cast(F.col("value_source_value"), "double") * 100, 3)
            ).otherwise(F.lit(None))
        )
        .withColumn("unit_converted", F.lit("mg/dL"))
    )

    # chloride
    labs_chloride = (
        msmt_ca
        .filter(F.col("ancestor_concept_id") == 40652796)
        .withColumn("std_concept_id", F.lit(40652796))
        .withColumn(
            "value_converted",
            F.when(
                F.lower(F.col("unit_source_value")).like("mmo%/l"),
                F.round(F.try_cast(F.col("value_source_value"), "double"), 3)
            ).otherwise(F.lit(None))
        )
        .withColumn("unit_converted", F.lit("mmol/L"))
    )

    # chol_hdl
    labs_chol_hdl = (
        msmt_ca
        .filter(F.col("ancestor_concept_id") == 40652802)
        .withColumn("std_concept_id", F.lit(40652802))
        .withColumn(
            "value_converted",
            F.when(
                F.lower(F.col("unit_source_value")).rlike(r'^mg/\s?dl$'),
                F.round(F.try_cast(F.col("value_source_value"), "double"), 3)
            ).when(
                F.lower(F.col("unit_source_value")) == "g/l",
                F.round(F.try_cast(F.col("value_source_value"), "double") / 100, 3)
            ).when(
                F.lower(F.col("unit_source_value")) == "mg/l",
                F.round(F.try_cast(F.col("value_source_value"), "double") / 10, 3)
            ).when(
                F.lower(F.col("unit_source_value")) == "mg/ml",
                F.round(F.try_cast(F.col("value_source_value"), "double") * 100, 3)
            ).otherwise(F.lit(None))
        )
        .withColumn("unit_converted", F.lit("mg/dL"))
    )

    # chol_ldl
    labs_chol_ldl = (
        msmt_ca
        .filter(F.col("ancestor_concept_id") == 40654572)
        .withColumn("std_concept_id", F.lit(40654572))
        .withColumn(
            "value_converted",
            F.when(
                F.lower(F.col("unit_source_value")).rlike(r'^mg/\s?dl$'),
                F.round(F.try_cast(F.col("value_source_value"), "double"), 3)
            ).when(
                F.lower(F.col("unit_source_value")) == "g/l",
                F.round(F.try_cast(F.col("value_source_value"), "double") / 100, 3)
            ).when(
                F.lower(F.col("unit_source_value")) == "mg/l",
                F.round(F.try_cast(F.col("value_source_value"), "double") / 10, 3)
            ).when(
                F.lower(F.col("unit_source_value")) == "mg/ml",
                F.round(F.try_cast(F.col("value_source_value"), "double") * 100, 3)
            ).otherwise(F.lit(None))
        )
        .withColumn("unit_converted", F.lit("mg/dL"))
    )

    # chol_vldl
    labs_chol_vldl = (
        msmt_ca
        .filter((F.col("ancestor_concept_id") == 40654576) | (F.col("measurement_concept_id") == 3009596))
        .withColumn("std_concept_id", F.lit(40654576))
        .withColumn(
            "value_converted",
            F.when(
                F.lower(F.col("unit_source_value")).rlike(r'^mg/\s?dl$'),
                F.round(F.try_cast(F.col("value_source_value"), "double"), 3)
            ).when(
                F.lower(F.col("unit_source_value")) == "g/l",
                F.round(F.try_cast(F.col("value_source_value"), "double") / 100, 3)
            ).when(
                F.lower(F.col("unit_source_value")) == "mg/l",
                F.round(F.try_cast(F.col("value_source_value"), "double") / 10, 3)
            ).when(
                F.lower(F.col("unit_source_value")) == "mg/ml",
                F.round(F.try_cast(F.col("value_source_value"), "double") * 100, 3)
            ).otherwise(F.lit(None))
        )
        .withColumn("unit_converted", F.lit("mg/dL"))
    )

    # chol_total
    labs_chol_total = (
        msmt_ca
        .filter(F.col("ancestor_concept_id") == 40652808)
        .withColumn("std_concept_id", F.lit(40652808))
        .withColumn(
            "value_converted",
            F.when(
                F.lower(F.col("unit_source_value")).rlike(r'^mg/\s?dl$'),
                F.round(F.try_cast(F.col("value_source_value"), "double"), 3)
            ).when(
                F.lower(F.col("unit_source_value")) == "g/l",
                F.round(F.try_cast(F.col("value_source_value"), "double") / 100, 3)
            ).when(
                F.lower(F.col("unit_source_value")) == "mg/l",
                F.round(F.try_cast(F.col("value_source_value"), "double") / 10, 3)
            ).when(
                F.lower(F.col("unit_source_value")) == "mg/ml",
                F.round(F.try_cast(F.col("value_source_value"), "double") * 100, 3)
            ).otherwise(F.lit(None))
        )
        .withColumn("unit_converted", F.lit("mg/dL"))
    )

    # c4
    labs_c4 = (
        msmt_ca
        .filter(F.col("ancestor_concept_id") == 40654637)
        .withColumn("std_concept_id", F.lit(40654637))
        .withColumn(
            "value_converted",
            F.when(
                F.lower(F.col("unit_source_value")).rlike(r'^mg/\s?dl$'),
                F.round(F.try_cast(F.col("value_source_value"), "double"), 3)
            ).when(
                F.lower(F.col("unit_source_value")) == "g/l",
                F.round(F.try_cast(F.col("value_source_value"), "double") / 100, 3)
            ).when(
                F.lower(F.col("unit_source_value")) == "mg/l",
                F.round(F.try_cast(F.col("value_source_value"), "double") / 10, 3)
            ).when(
                F.lower(F.col("unit_source_value")) == "mg/ml",
                F.round(F.try_cast(F.col("value_source_value"), "double") * 100, 3)
            ).otherwise(F.lit(None))
        )
        .withColumn("unit_converted", F.lit("mg/dL"))
    )

    # covid
    labs_covid = (
        msmt_ca
        .filter(F.col("ancestor_concept_id") == 36662633)
        .withColumn("std_concept_id", F.lit(36662633))
        .withColumn("value_converted", F.lit("NA"))
        .withColumn("unit_converted", F.lit("NA"))
    )

    # ck
    labs_ck = (
        msmt_ca
        .filter(F.col("ancestor_concept_id") == 40652867)
        .withColumn("std_concept_id", F.lit(40652867))
        .withColumn(
            "value_converted",
            F.when(
                F.lower(F.col("unit_source_value")) == "ng/ml",
                F.round(F.try_cast(F.col("value_source_value"), "double"), 3)
            ).when(
                F.lower(F.col("unit_source_value")) == "ng/dl",
                F.round(F.try_cast(F.col("value_source_value"), "double") / 100, 3)
            ).when(
                F.lower(F.col("unit_source_value")) == "ng/l",
                F.round(F.try_cast(F.col("value_source_value"), "double") / 1000, 3)
            ).otherwise(F.lit(None))
        )
        .withColumn("unit_converted", F.lit("ng/mL"))
    )

    # creatinine
    labs_creatinine = (
        msmt_ca
        .filter(F.col("ancestor_concept_id") == 40652870)
        .withColumn("std_concept_id", F.lit(40652870))
        .withColumn(
            "value_converted",
            F.when(
                F.lower(F.col("unit_source_value")).rlike(r'^mg/\s?dl$'),
                F.round(F.try_cast(F.col("value_source_value"), "double"), 3)
            ).when(
                F.lower(F.col("unit_source_value")) == "g/l",
                F.round(F.try_cast(F.col("value_source_value"), "double") / 100, 3)
            ).when(
                F.lower(F.col("unit_source_value")) == "mg/l",
                F.round(F.try_cast(F.col("value_source_value"), "double") / 10, 3)
            ).when(
                F.lower(F.col("unit_source_value")) == "mg/ml",
                F.round(F.try_cast(F.col("value_source_value"), "double") * 100, 3)
            ).otherwise(F.lit(None))
        )
        .withColumn("unit_converted", F.lit("mg/dL"))
    )

    # creatinine_urine
    labs_creatinine_urine = (
        msmt_ca
        .filter(F.col("ancestor_concept_id") == 40656057)
        .withColumn("std_concept_id", F.lit(40656057))
        .withColumn(
            "value_converted",
            F.when(
                F.lower(F.col("unit_source_value")).rlike(r'^mg/\s?dl$'),
                F.round(F.try_cast(F.col("value_source_value"), "double"), 3)
            ).when(
                F.lower(F.col("unit_source_value")) == "g/l",
                F.round(F.try_cast(F.col("value_source_value"), "double") / 100, 3)
            ).when(
                F.lower(F.col("unit_source_value")) == "mg/l",
                F.round(F.try_cast(F.col("value_source_value"), "double") / 10, 3)
            ).when(
                F.lower(F.col("unit_source_value")) == "mg/ml",
                F.round(F.try_cast(F.col("value_source_value"), "double") * 100, 3)
            ).otherwise(F.lit(None))
        )
        .withColumn("unit_converted", F.lit("mg/dL"))
    )

    # ccpab
    labs_ccpab = (
        msmt_ca
        .filter(F.col("ancestor_concept_id") == 40652885)
        .withColumn("std_concept_id", F.lit(40652885))
        .withColumn("value_converted", F.lit("NA"))
        .withColumn("unit_converted", F.lit("NA"))
    )

    # eosinophils
    labs_eosinophils = (
        msmt_ca
        .filter(F.col("ancestor_concept_id") == 40653994)
        .withColumn("std_concept_id", F.lit(40653994))
        .withColumn(
            "value_converted",
            F.when(
                F.lower(F.col("unit_source_value")).like("th%/ul")
                | F.lower(F.col("unit_source_value")).like("th%/mcl")
                | F.lower(F.col("unit_source_value")).like("%10%3/ul")
                | F.lower(F.col("unit_source_value")).like("%10%3/mcl")
                | F.lower(F.col("unit_source_value")).rlike(r'^k/[ucm].+$'),
                F.round(F.try_cast(F.col("value_source_value"), "double"), 3)
            ).when(
                F.lower(F.col("unit_source_value")).rlike(r'^cell.*/[ucm].+$'),
                F.round(F.try_cast(F.col("value_source_value"), "double") / 1000, 3)
            ).otherwise(F.lit(None))
        )
        .withColumn("unit_converted", F.lit("Thousand/uL"))
    )

    # rbc
    labs_rbc = (
        msmt_ca
        .filter(F.col("ancestor_concept_id") == 40654005)
        .withColumn("std_concept_id", F.lit(40654005))
        .withColumn(
            "value_converted",
            F.when(
                F.lower(F.col("unit_source_value")).like("m%/ul")
                | F.lower(F.col("unit_source_value")).like("m%/mcl")
                | F.lower(F.col("unit_source_value")).like("m%/mm3")
                | F.lower(F.col("unit_source_value")).rlike(r'^m.*/cu?mm$')
                | F.lower(F.col("unit_source_value")).like("%10%6/ul"),
                F.round(F.try_cast(F.col("value_source_value"), "double"), 3)
            ).otherwise(F.lit(None))
        )
        .withColumn("unit_converted", F.lit("Million/uL"))
    )

    # rbc_urine
    labs_rbc_urine = (
        msmt_ca
        .filter(F.col("ancestor_concept_id") == 40657685)
        .withColumn("std_concept_id", F.lit(40657685))
        .withColumn("value_converted", F.lit("NA"))
        .withColumn("unit_converted", F.lit("NA"))
    )

    # esr
    labs_esr = (
        msmt_ca
        .join(concept, msmt_ca.descendant_concept_id == concept.concept_id, "inner")
        .filter(
            (F.col("ancestor_concept_id") == 4028908)
            & F.lower(F.col("concept_name")).like("%eryth%")
            & F.lower(F.col("concept_name")).like("%sed%")
        )
        .withColumn("std_concept_id", F.lit(4028908))
        .withColumn(
            "value_converted",
            F.when(
                F.lower(F.col("unit_source_value")).like("mm/h%"),
                F.round(F.try_cast(F.col("value_source_value"), "double"), 3)
            ).otherwise(F.lit(None))
        )
        .withColumn("unit_converted", F.lit("mm/h"))
    )

    # ggt
    labs_ggt = (
        msmt_ca
        .join(concept, msmt_ca.descendant_concept_id == concept.concept_id, "inner")
        .filter(F.lower(F.col("concept_name")).like("%glutamyl%transferase%"))
        .dropDuplicates(["measurement_id"])
        .withColumn("std_concept_id", F.lit(2212371)) # TODO: review semi-arbitrary choice of CPT4 code
        .withColumn(
            "value_converted",
            F.when(
                F.lower(F.col("unit_source_value")).rlike(r'^.*i?u.*/l$'),
                F.round(F.try_cast(F.col("value_source_value"), "double"), 3)
            ).otherwise(F.lit(None))
        )
        .withColumn("unit_converted", F.lit("Unit/L"))
    )

    # ferritin
    labs_ferritin = (
        msmt_ca
        .filter(F.col("ancestor_concept_id") == 40652982)
        .withColumn("std_concept_id", F.lit(40652982))
        .withColumn(
            "value_converted",
            F.when(
                F.lower(F.col("unit_source_value")) == "ng/ml",
                F.round(F.try_cast(F.col("value_source_value"), "double"), 3)
            ).when(
                F.lower(F.col("unit_source_value")) == "ng/dl",
                F.round(F.try_cast(F.col("value_source_value"), "double") / 100, 3)
            ).when(
                F.lower(F.col("unit_source_value")) == "ng/l",
                F.round(F.try_cast(F.col("value_source_value"), "double") / 1000, 3)
            ).otherwise(F.lit(None))
        )
        .withColumn("unit_converted", F.lit("ng/mL"))
    )

    # folate
    labs_folate = (
        msmt_ca
        .filter(F.col("ancestor_concept_id") == 40652995)
        .withColumn("std_concept_id", F.lit(40652995))
        .withColumn(
            "value_converted",
            F.when(
                F.lower(F.col("unit_source_value")) == "ng/ml",
                F.round(F.try_cast(F.col("value_source_value"), "double"), 3)
            ).when(
                F.lower(F.col("unit_source_value")) == "ng/dl",
                F.round(F.try_cast(F.col("value_source_value"), "double") / 100, 3)
            ).when(
                F.lower(F.col("unit_source_value")) == "ng/l",
                F.round(F.try_cast(F.col("value_source_value"), "double") / 1000, 3)
            ).otherwise(F.lit(None))
        )
        .withColumn("unit_converted", F.lit("ng/mL"))
    )

    # glucose
    labs_glucose = (
        msmt_ca
        .filter(
            (F.col("ancestor_concept_id") == 40653085)
            & (~F.col("value_source_value").like("%,%"))
        )
        .withColumn("std_concept_id", F.lit(40653085))
        .withColumn(
            "value_converted",
            F.when(
                F.lower(F.col("unit_source_value")).rlike(r'^mg/\s?dl$'),
                F.round(F.try_cast(F.col("value_source_value"), "double"), 3)
            ).when(
                F.lower(F.col("unit_source_value")) == "g/l",
                F.round(F.try_cast(F.col("value_source_value"), "double") / 100, 3)
            ).when(
                F.lower(F.col("unit_source_value")) == "mg/l",
                F.round(F.try_cast(F.col("value_source_value"), "double") / 10, 3)
            ).when(
                F.lower(F.col("unit_source_value")) == "mg/ml",
                F.round(F.try_cast(F.col("value_source_value"), "double") * 100, 3)
            ).otherwise(F.lit(None))
        )
        .withColumn("unit_converted", F.lit("mg/dL"))
    )

    # glucose_fasting
    labs_glucose_fasting = (
        msmt_ca
        .filter(F.col("measurement_source_concept_id") == 3037110)
        .dropDuplicates(["measurement_id"])
        .withColumn("std_concept_id", F.lit(3037110))
        .withColumn(
            "value_converted",
            F.when(
                F.lower(F.col("unit_source_value")).rlike(r'^mg/\s?dl$'),
                F.round(F.try_cast(F.col("value_source_value"), "double"), 3)
            ).when(
                F.lower(F.col("unit_source_value")) == "g/l",
                F.round(F.try_cast(F.col("value_source_value"), "double") / 100, 3)
            ).when(
                F.lower(F.col("unit_source_value")) == "mg/l",
                F.round(F.try_cast(F.col("value_source_value"), "double") / 10, 3)
            ).when(
                F.lower(F.col("unit_source_value")) == "mg/ml",
                F.round(F.try_cast(F.col("value_source_value"), "double") * 100, 3)
            ).otherwise(F.lit(None))
        )
        .withColumn("unit_converted", F.lit("mg/dL"))
    )

    # glucose_urine
    labs_glucose_urine = (
        msmt_ca
        .filter(F.col("ancestor_concept_id") == 40657691)
        .withColumn("std_concept_id", F.lit(40657691))
        .withColumn(
            "value_converted",
            F.when(
                F.lower(F.col("unit_source_value")).rlike(r'^mg/\s?dl$'),
                F.round(F.try_cast(F.col("value_source_value"), "double"), 3)
            ).when(
                F.lower(F.col("unit_source_value")) == "g/l",
                F.round(F.try_cast(F.col("value_source_value"), "double") / 100, 3)
            ).when(
                F.lower(F.col("unit_source_value")) == "mg/l",
                F.round(F.try_cast(F.col("value_source_value"), "double") / 10, 3)
            ).when(
                F.lower(F.col("unit_source_value")) == "mg/ml",
                F.round(F.try_cast(F.col("value_source_value"), "double") * 100, 3)
            ).otherwise(F.lit(None))
        )
        .withColumn("unit_converted", F.lit("mg/dL"))
    )

    # granulocytes
    labs_granulocytes = (
        msmt_ca
        .filter(F.col("ancestor_concept_id") == 40654016)
        .withColumn("std_concept_id", F.lit(40654016))
        .withColumn("value_converted", F.lit("NA"))
        .withColumn("unit_converted", F.lit("NA"))
    )

    # hemoglobin
    labs_hemoglobin = (
        msmt_ca
        .filter(F.col("ancestor_concept_id") == 40654905)
        .withColumn("std_concept_id", F.lit(40654905))
        .withColumn(
            "value_converted",
            F.when(
                F.lower(F.col("unit_source_value")).rlike(r'^gm?/dl$'),
                F.round(F.try_cast(F.col("value_source_value"), "double"), 3)
            ).when(
                F.lower(F.col("unit_source_value")) == "mg/dl",
                F.round(F.try_cast(F.col("value_source_value"), "double") / 1000, 3)
            ).when(
                F.lower(F.col("unit_source_value")) == "mg/l",
                F.round(F.try_cast(F.col("value_source_value"), "double") / 10000, 3)
            ).otherwise(F.lit(None))
        )
        .withColumn("unit_converted", F.lit("g/dL"))
    )

    # hba1c
    labs_hba1c = (
        msmt_ca
        .filter(F.col("ancestor_concept_id") == 1621295)
        .withColumn("std_concept_id", F.lit(1621295))
        .withColumn(
            "value_converted",
            F.when(
                F.col("unit_source_value").rlike(r'^%.*$'),
                F.round(F.try_cast(F.col("value_source_value"), "double"), 3)
            ).otherwise(F.lit(None))
        )
        .withColumn("unit_converted", F.lit("%"))
    )

    # iga
    labs_iga = (
        msmt_ca
        .filter(F.col("ancestor_concept_id") == 40653141)
        .withColumn("std_concept_id", F.lit(40653141))
        .withColumn(
            "value_converted",
            F.when(
                F.lower(F.col("unit_source_value")).rlike(r'^mg/\s?dl$'),
                F.round(F.try_cast(F.col("value_source_value"), "double"), 3)
            ).when(
                F.lower(F.col("unit_source_value")) == "g/l",
                F.round(F.try_cast(F.col("value_source_value"), "double") / 100, 3)
            ).when(
                F.lower(F.col("unit_source_value")) == "mg/l",
                F.round(F.try_cast(F.col("value_source_value"), "double") / 10, 3)
            ).when(
                F.lower(F.col("unit_source_value")) == "mg/ml",
                F.round(F.try_cast(F.col("value_source_value"), "double") * 100, 3)
            ).otherwise(F.lit(None))
        )
        .withColumn("unit_converted", F.lit("mg/dL"))
    )

    # igg
    labs_igg = (
        msmt_ca
        .filter(F.col("ancestor_concept_id") == 40653151)
        .withColumn("std_concept_id", F.lit(40653151))
        .withColumn(
            "value_converted",
            F.when(
                F.lower(F.col("unit_source_value")).rlike(r'^mg/\s?dl$'),
                F.round(F.try_cast(F.col("value_source_value"), "double"), 3)
            ).when(
                F.lower(F.col("unit_source_value")) == "g/l",
                F.round(F.try_cast(F.col("value_source_value"), "double") / 100, 3)
            ).when(
                F.lower(F.col("unit_source_value")) == "mg/l",
                F.round(F.try_cast(F.col("value_source_value"), "double") / 10, 3)
            ).when(
                F.lower(F.col("unit_source_value")) == "mg/ml",
                F.round(F.try_cast(F.col("value_source_value"), "double") * 100, 3)
            ).otherwise(F.lit(None))
        )
        .withColumn("unit_converted", F.lit("mg/dL"))
    )

    # igm
    labs_igm = (
        msmt_ca
        .filter(F.col("ancestor_concept_id") == 40653158)
        .withColumn("std_concept_id", F.lit(40653158))
        .withColumn(
            "value_converted",
            F.when(
                F.lower(F.col("unit_source_value")).rlike(r'^mg/\s?dl$'),
                F.round(F.try_cast(F.col("value_source_value"), "double"), 3)
            ).when(
                F.lower(F.col("unit_source_value")) == "g/l",
                F.round(F.try_cast(F.col("value_source_value"), "double") / 100, 3)
            ).when(
                F.lower(F.col("unit_source_value")) == "mg/l",
                F.round(F.try_cast(F.col("value_source_value"), "double") / 10, 3)
            ).when(
                F.lower(F.col("unit_source_value")) == "mg/ml",
                F.round(F.try_cast(F.col("value_source_value"), "double") * 100, 3)
            ).otherwise(F.lit(None))
        )
        .withColumn("unit_converted", F.lit("mg/dL"))
    )

    # inr
    labs_inr = (
        msmt_ca
        .join(concept, msmt_ca.descendant_concept_id == concept.concept_id, "inner")
        .filter(F.lower(F.col("concept_name")).rlike(r'.*\binr\b.*'))
        .dropDuplicates(["measurement_id"])
        .withColumn("std_concept_id", F.lit(85610)) # TODO: review semi-arbitrary choice of CPT4 code
        .withColumn("value_converted", F.round(F.try_cast(F.col("value_source_value"), "double"), 3))
        .withColumn("unit_converted", F.lit("ratio"))
    )

    # iron
    labs_iron = (
        msmt_ca
        .filter(F.col("ancestor_concept_id") == 40654984)
        .withColumn("std_concept_id", F.lit(40654984))
        .withColumn(
            "value_converted",
            F.when(
                (F.lower(F.col("unit_source_value")) == "ug/dl") | (F.lower(F.col("unit_source_value")) == "mcg/dl"),
                F.round(F.try_cast(F.col("value_source_value"), "double"), 3)
            ).otherwise(F.lit(None))
        )
        .withColumn("unit_converted", F.lit("ug/dL"))
    )

    # ketones
    labs_ketones = (
        msmt_ca
        .filter((F.col("ancestor_concept_id") == 40656264) | (F.col("measurement_source_value") == "5797-6"))
        .withColumn("std_concept_id", F.lit(40656264))
        .withColumn(
            "value_converted",
            F.when(
                F.lower(F.col("unit_source_value")).rlike(r'^mg/\s?dl$'),
                F.round(F.try_cast(F.col("value_source_value"), "double"), 3)
            ).when(
                F.lower(F.col("unit_source_value")) == "g/l",
                F.round(F.try_cast(F.col("value_source_value"), "double") / 100, 3)
            ).when(
                F.lower(F.col("unit_source_value")) == "mg/l",
                F.round(F.try_cast(F.col("value_source_value"), "double") / 10, 3)
            ).when(
                F.lower(F.col("unit_source_value")) == "mg/ml",
                F.round(F.try_cast(F.col("value_source_value"), "double") * 100, 3)
            ).otherwise(F.lit(None))
        )
        .withColumn("unit_converted", F.lit("mg/dL"))
    )

    # leukocyte_esterase
    labs_leukocyte_esterase = (
        msmt_ca
        .filter(F.col("ancestor_concept_id") == 40657703)
        .withColumn("std_concept_id", F.lit(40657703))
        .withColumn(
            "value_converted",
            F.when(
                F.lower(F.col("unit_source_value")) == "leu/ul",
                F.round(F.try_cast(F.col("value_source_value"), "double"), 3)
            ).otherwise(F.lit(None))
        )
        .withColumn("unit_converted", F.lit("Leu/uL"))
    )

    # wbc
    labs_wbc = (
        msmt_ca
        .filter(F.col("ancestor_concept_id") == 40654026)
        .withColumn("std_concept_id", F.lit(40654026))
        .withColumn(
            "value_converted",
            F.when(
                F.lower(F.col("unit_source_value")).like("th%/ul")
                | F.lower(F.col("unit_source_value")).like("th%/mcl")
                | F.lower(F.col("unit_source_value")).like("%10%3/ul")
                | F.lower(F.col("unit_source_value")).like("%10%3/mcl")
                | F.lower(F.col("unit_source_value")).rlike(r'^k/[ucm].+$'),
                F.round(F.try_cast(F.col("value_source_value"), "double"), 3)
            ).when(
                F.lower(F.col("unit_source_value")).rlike(r'^cell.*/[ucm].+$'),
                F.round(F.try_cast(F.col("value_source_value"), "double") / 1000, 3)
            ).otherwise(F.lit(None))
        )
        .withColumn("unit_converted", F.lit("Thousand/uL"))
    )

    # wbc_urine
    labs_wbc_urine = (
        msmt_ca
        .filter(F.col("ancestor_concept_id") == 40657704)
        .withColumn("std_concept_id", F.lit(40657704))
        .withColumn("value_converted", F.lit("NA"))
        .withColumn("unit_converted", F.lit("NA"))
    )

    # lipoprotein_a
    labs_lipoprotein_a = (
        msmt_ca
        .filter(F.col("ancestor_concept_id") == 40655033)
        .withColumn("std_concept_id", F.lit(40655033))
        .withColumn(
            "value_converted",
            F.when(
                F.lower(F.col("unit_source_value")).rlike(r'^mg/\s?dl$'),
                F.round(F.try_cast(F.col("value_source_value"), "double"), 3)
            ).when(
                F.lower(F.col("unit_source_value")) == "g/l",
                F.round(F.try_cast(F.col("value_source_value"), "double") / 100, 3)
            ).when(
                F.lower(F.col("unit_source_value")) == "mg/l",
                F.round(F.try_cast(F.col("value_source_value"), "double") / 10, 3)
            ).when(
                F.lower(F.col("unit_source_value")) == "mg/ml",
                F.round(F.try_cast(F.col("value_source_value"), "double") * 100, 3)
            ).otherwise(F.lit(None))
        )
        .withColumn("unit_converted", F.lit("mg/dL"))
    )

    # lymphocytes
    labs_lymphocytes = (
        msmt_ca
        .filter(F.col("ancestor_concept_id") == 40654045)
        .withColumn("std_concept_id", F.lit(40654045))
        .withColumn(
            "value_converted",
            F.when(
                F.lower(F.col("unit_source_value")).like("th%/ul")
                | F.lower(F.col("unit_source_value")).like("th%/mcl")
                | F.lower(F.col("unit_source_value")).like("%10%3/ul")
                | F.lower(F.col("unit_source_value")).like("%10%3/mcl")
                | F.lower(F.col("unit_source_value")).rlike(r'^k/[ucm].+$'),
                F.round(F.try_cast(F.col("value_source_value"), "double"), 3)
            ).when(
                F.lower(F.col("unit_source_value")).rlike(r'^cell.*/[ucm].+$'),
                F.round(F.try_cast(F.col("value_source_value"), "double") / 1000, 3)
            ).otherwise(F.lit(None))
        )
        .withColumn("unit_converted", F.lit("Thousand/uL"))
    )

    # wbc (dedupe against lymphocytes)
    labs_wbc = labs_wbc.join(
        labs_lymphocytes.select("person_id", "measurement_date", "value_source_value").distinct(),
        on=["person_id", "measurement_date", "value_source_value"],
        how="left_anti"
    )

    # magnesium
    labs_magnesium = (
        msmt_ca
        .filter(F.col("ancestor_concept_id") == 40653291)
        .withColumn("std_concept_id", F.lit(40653291))
        .withColumn(
            "value_converted",
            F.when(
                F.lower(F.col("unit_source_value")).rlike(r'^mg/\s?dl$'),
                F.round(F.try_cast(F.col("value_source_value"), "double"), 3)
            ).when(
                F.lower(F.col("unit_source_value")) == "g/l",
                F.round(F.try_cast(F.col("value_source_value"), "double") / 100, 3)
            ).when(
                F.lower(F.col("unit_source_value")) == "mg/l",
                F.round(F.try_cast(F.col("value_source_value"), "double") / 10, 3)
            ).when(
                F.lower(F.col("unit_source_value")) == "mg/ml",
                F.round(F.try_cast(F.col("value_source_value"), "double") * 100, 3)
            ).otherwise(F.lit(None))
        )
        .withColumn("unit_converted", F.lit("mg/dL"))
    )

    # metamyelocytes
    labs_metamyelocytes = (
        msmt_ca
        .filter(
            (F.col("ancestor_concept_id") == 40654064)
            | (F.col("measurement_concept_id").isin(3012392, 3024507))
        )
        .withColumn("std_concept_id", F.lit(40654064))
        .withColumn(
            "value_converted",
            F.when(
                F.lower(F.col("unit_source_value")) == "cells/ul",
                F.round(F.try_cast(F.col("value_source_value"), "double"), 3)
            ).when(
                F.lower(F.col("unit_source_value")).rlike(r'^10.3/ul$'),
                F.round(F.try_cast(F.col("value_source_value"), "double") * 1000, 3)
            ).otherwise(F.lit(None))
        )
        .withColumn("unit_converted", F.lit("Cells/uL"))
    )

    # metanephrine
    labs_metanephrine = (
        msmt_ca
        .filter(F.col("ancestor_concept_id") == 40655090)
        .withColumn("std_concept_id", F.lit(40655090))
        .withColumn(
            "value_converted",
            F.when(
                F.lower(F.col("unit_source_value")) == "nmol/l",
                F.round(F.try_cast(F.col("value_source_value"), "double"), 3)
            ).otherwise(F.lit(None))
        )
        .withColumn("unit_converted", F.lit("nmol/L"))
    )

    # microalbumin
    labs_microalbumin = (
        msmt_ca
        .filter(F.col("ancestor_concept_id") == 40656529)
        .withColumn("std_concept_id", F.lit(40656529))
        .withColumn(
            "value_converted",
            F.when(
                (F.lower(F.col("unit_source_value")) == "mcg/ml") | (F.lower(F.col("unit_source_value")) == "ug/ml"),
                F.round(F.try_cast(F.col("value_source_value"), "double"), 3)
            ).when(
                F.lower(F.col("unit_source_value")) == "mcg/dl",
                F.round(F.try_cast(F.col("value_source_value"), "double") / 100, 3)
            ).otherwise(F.lit(None))
        )
        .withColumn("unit_converted", F.lit("mcg/mL"))
    )

    # microalbumin_creatinine_ratio
    labs_microalbumin_creatinine_ratio = (
        msmt_ca
        .filter(F.col("ancestor_concept_id") == 40656531)
        .withColumn("std_concept_id", F.lit(40656531))
        .withColumn(
            "value_converted",
            F.when(
                F.lower(F.col("unit_source_value")).like("mcg/mg%"),
                F.round(F.try_cast(F.col("value_source_value"), "double"), 3)
            ).otherwise(F.lit(None))
        )
        .withColumn("unit_converted", F.lit("mcg/mg"))
    )

    # monocytes
    labs_monocytes = (
        msmt_ca
        .filter(F.col("ancestor_concept_id") == 40654069)
        .withColumn("std_concept_id", F.lit(40654069))
        .withColumn(
            "value_converted",
            F.when(
                F.lower(F.col("unit_source_value")).like("th%/ul")
                | F.lower(F.col("unit_source_value")).like("th%/mcl")
                | F.lower(F.col("unit_source_value")).like("%10%3/ul")
                | F.lower(F.col("unit_source_value")).like("%10%3/mcl")
                | F.lower(F.col("unit_source_value")).rlike(r'^k/[ucm].+$'),
                F.round(F.try_cast(F.col("value_source_value"), "double"), 3)
            ).when(
                F.lower(F.col("unit_source_value")).rlike(r'^cell.*/[ucm].+$'),
                F.round(F.try_cast(F.col("value_source_value"), "double") / 1000, 3)
            ).otherwise(F.lit(None))
        )
        .withColumn("unit_converted", F.lit("Thousand/uL"))
    )

    # neutrophils
    labs_neutrophils = (
        msmt_ca
        .filter(F.col("ancestor_concept_id") == 40654088)
        .withColumn("std_concept_id", F.lit(40654088))
        .withColumn(
            "value_converted",
            F.when(
                F.lower(F.col("unit_source_value")).like("th%/ul")
                | F.lower(F.col("unit_source_value")).like("th%/mcl")
                | F.lower(F.col("unit_source_value")).like("%10%3/ul")
                | F.lower(F.col("unit_source_value")).like("%10%3/mcl")
                | F.lower(F.col("unit_source_value")).rlike(r'^k/[ucm].+$'),
                F.round(F.try_cast(F.col("value_source_value"), "double"), 3)
            ).when(
                F.lower(F.col("unit_source_value")).rlike(r'^cell.*/[ucm].+$'),
                F.round(F.try_cast(F.col("value_source_value"), "double") / 1000, 3)
            ).otherwise(F.lit(None))
        )
        .withColumn("unit_converted", F.lit("Thousand/uL"))
    )

    # neutrophils_bandform
    labs_neutrophils_bandform = (
        msmt_ca
        .filter(
            (F.col("ancestor_concept_id") == 40654083)
            | (F.col("measurement_concept_id").isin(3008939, 3018199))
        )
        .withColumn("std_concept_id", F.lit(40654083))
        .withColumn(
            "value_converted",
            F.when(
                F.lower(F.col("unit_source_value")).like("th%/ul")
                | F.lower(F.col("unit_source_value")).like("th%/mcl")
                | F.lower(F.col("unit_source_value")).like("%10%3/ul")
                | F.lower(F.col("unit_source_value")).like("%10%3/mcl")
                | F.lower(F.col("unit_source_value")).rlike(r'^k/[ucm].+$'),
                F.round(F.try_cast(F.col("value_source_value"), "double"), 3)
            ).when(
                F.lower(F.col("unit_source_value")).rlike(r'^cell.*/[ucm].+$'),
                F.round(F.try_cast(F.col("value_source_value"), "double") / 1000, 3)
            ).otherwise(F.lit(None))
        )
        .withColumn("unit_converted", F.lit("Thousand/uL"))
    )

    # neutrophils_segmented
    labs_neutrophils_segmented = (
        msmt_ca
        .filter(
            (F.col("ancestor_concept_id") == 40654086)
            | (F.col("measurement_concept_id").isin(3027270, 3015586))
        )
        .withColumn("std_concept_id", F.lit(40654086))
        .withColumn(
            "value_converted",
            F.when(
                F.lower(F.col("unit_source_value")).like("th%/ul")
                | F.lower(F.col("unit_source_value")).like("th%/mcl")
                | F.lower(F.col("unit_source_value")).like("%10%3/ul")
                | F.lower(F.col("unit_source_value")).like("%10%3/mcl")
                | F.lower(F.col("unit_source_value")).rlike(r'^k/[ucm].+$'),
                F.round(F.try_cast(F.col("value_source_value"), "double"), 3)
            ).when(
                F.lower(F.col("unit_source_value")).rlike(r'^cell.*/[ucm].+$'),
                F.round(F.try_cast(F.col("value_source_value"), "double") / 1000, 3)
            ).otherwise(F.lit(None))
        )
        .withColumn("unit_converted", F.lit("Thousand/uL"))
    )

    # normetanephrine
    labs_normetanephrine = (
        msmt_ca
        .filter(F.col("ancestor_concept_id") == 40655204)
        .withColumn("std_concept_id", F.lit(40655204))
        .withColumn(
            "value_converted",
            F.when(
                F.lower(F.col("unit_source_value")) == "nmol/l",
                F.round(F.try_cast(F.col("value_source_value"), "double"), 3)
            ).otherwise(F.lit(None))
        )
        .withColumn("unit_converted", F.lit("nmol/L"))
    )

    # ptt
    labs_ptt = (
        msmt_ca
        .join(concept, msmt_ca.descendant_concept_id == concept.concept_id, "inner")
        .filter(
            F.lower(F.col("concept_name")).like("%ptt%")
            & F.col("ancestor_concept_id").isin(1618784, 2212742)
        )
        .dropDuplicates(["measurement_id"])
        .withColumn("std_concept_id", F.lit(2212742)) # TODO: review arbitrary choice of code
        .withColumn(
            "value_converted",
            F.when(
                F.lower(F.col("unit_source_value")).like("s%"),
                F.round(F.try_cast(F.col("value_source_value"), "double"), 3)
            ).otherwise(F.lit(None))
        )
        .withColumn("unit_converted", F.lit("Second(s)"))
    )

    # phosphate
    labs_phosphate = (
        msmt_ca
        .filter(F.col("ancestor_concept_id") == 40653550)
        .withColumn("std_concept_id", F.lit(40653550))
        .withColumn(
            "value_converted",
            F.when(
                F.lower(F.col("unit_source_value")).rlike(r'^mg/\s?dl$'),
                F.round(F.try_cast(F.col("value_source_value"), "double"), 3)
            ).when(
                F.lower(F.col("unit_source_value")) == "g/l",
                F.round(F.try_cast(F.col("value_source_value"), "double") / 100, 3)
            ).when(
                F.lower(F.col("unit_source_value")) == "mg/l",
                F.round(F.try_cast(F.col("value_source_value"), "double") / 10, 3)
            ).when(
                F.lower(F.col("unit_source_value")) == "mg/ml",
                F.round(F.try_cast(F.col("value_source_value"), "double") * 100, 3)
            ).otherwise(F.lit(None))
        )
        .withColumn("unit_converted", F.lit("mg/dL"))
    )

    # platelets
    labs_platelets = (
        msmt_ca
        .filter(F.col("ancestor_concept_id") == 40654106)
        .withColumn("std_concept_id", F.lit(40654106))
        .withColumn(
            "value_converted",
            F.when(
                F.lower(F.col("unit_source_value")).like("th%/ul")
                | F.lower(F.col("unit_source_value")).like("th%/mcl")
                | F.lower(F.col("unit_source_value")).like("%10%3/ul")
                | F.lower(F.col("unit_source_value")).like("%10%3/mcl")
                | F.lower(F.col("unit_source_value")).rlike(r'^k/[ucm].+$'),
                F.round(F.try_cast(F.col("value_source_value"), "double"), 3)
            ).when(
                F.lower(F.col("unit_source_value")).rlike(r'^cell.*/[ucm].+$'),
                F.round(F.try_cast(F.col("value_source_value"), "double") / 1000, 3)
            ).otherwise(F.lit(None))
        )
        .withColumn("unit_converted", F.lit("Thousand/uL"))
    )

    # potassium
    labs_potassium = (
        msmt_ca
        .filter(F.col("ancestor_concept_id") == 40653596)
        .withColumn("std_concept_id", F.lit(40653596))
        .withColumn(
            "value_converted",
            F.when(
                F.lower(F.col("unit_source_value")).like("mmo%/l"),
                F.round(F.try_cast(F.col("value_source_value"), "double"), 3)
            ).otherwise(F.lit(None))
        )
        .withColumn("unit_converted", F.lit("mmol/L"))
    )

    # prealbumin
    labs_prealbumin = (
        msmt_ca
        .filter(F.col("ancestor_concept_id") == 40653598)
        .withColumn("std_concept_id", F.lit(40653598))
        .withColumn(
            "value_converted",
            F.when(
                F.lower(F.col("unit_source_value")).rlike(r'^mg/\s?dl$'),
                F.round(F.try_cast(F.col("value_source_value"), "double"), 3)
            ).when(
                F.lower(F.col("unit_source_value")) == "g/l",
                F.round(F.try_cast(F.col("value_source_value"), "double") / 100, 3)
            ).when(
                F.lower(F.col("unit_source_value")) == "mg/l",
                F.round(F.try_cast(F.col("value_source_value"), "double") / 10, 3)
            ).when(
                F.lower(F.col("unit_source_value")) == "mg/ml",
                F.round(F.try_cast(F.col("value_source_value"), "double") * 100, 3)
            ).otherwise(F.lit(None))
        )
        .withColumn("unit_converted", F.lit("mg/dL"))
    )

    # promyelocytes
    labs_promyelocytes = (
        msmt_ca
        .filter(
            (F.col("ancestor_concept_id") == 40654115)
            | (F.col("measurement_concept_id").isin(3022709, 3024153))
        )
        .withColumn("std_concept_id", F.lit(40654115))
        .withColumn(
            "value_converted",
            F.when(
                F.lower(F.col("unit_source_value")) == "cells/ul",
                F.round(F.try_cast(F.col("value_source_value"), "double"), 3)
            ).when(
                F.lower(F.col("unit_source_value")).rlike(r'^10.3/ul$'),
                F.round(F.try_cast(F.col("value_source_value"), "double") * 1000, 3)
            ).otherwise(F.lit(None))
        )
        .withColumn("unit_converted", F.lit("Cells/uL"))
    )

    # protein
    labs_protein = (
        msmt_ca
        .filter(F.col("ancestor_concept_id") == 40653626)
        .withColumn("std_concept_id", F.lit(40653626))
        .withColumn(
            "value_converted",
            F.when(
                F.lower(F.col("unit_source_value")).rlike(r'^gm?/dl$'),
                F.round(F.try_cast(F.col("value_source_value"), "double"), 3)
            ).when(
                F.lower(F.col("unit_source_value")) == "mg/dl",
                F.round(F.try_cast(F.col("value_source_value"), "double") / 1000, 3)
            ).when(
                F.lower(F.col("unit_source_value")) == "mg/l",
                F.round(F.try_cast(F.col("value_source_value"), "double") / 10000, 3)
            ).otherwise(F.lit(None))
        )
        .withColumn("unit_converted", F.lit("g/dL"))
    )

    # protein_urine
    labs_protein_urine = (
        msmt_ca
        .filter(
            (F.col("ancestor_concept_id") == 40657714)
            | (F.col("measurement_concept_id").isin(3005897, 3035511, 3014051, 3037121, 40760845))
        )
        .withColumn("std_concept_id", F.lit(40657714))
        .withColumn(
            "value_converted",
            F.when(
                F.lower(F.col("unit_source_value")).rlike(r'^mg/\s?dl$'),
                F.round(F.try_cast(F.col("value_source_value"), "double"), 3)
            ).when(
                F.lower(F.col("unit_source_value")) == "g/l",
                F.round(F.try_cast(F.col("value_source_value"), "double") / 100, 3)
            ).when(
                F.lower(F.col("unit_source_value")) == "mg/l",
                F.round(F.try_cast(F.col("value_source_value"), "double") / 10, 3)
            ).when(
                F.lower(F.col("unit_source_value")) == "mg/ml",
                F.round(F.try_cast(F.col("value_source_value"), "double") * 100, 3)
            ).otherwise(F.lit(None))
        )
        .withColumn("unit_converted", F.lit("mg/dL"))
    )

    # pt
    labs_pt = (
        msmt_ca
        .filter(F.col("measurement_concept_id").isin(3034426, 2212731))
        .dropDuplicates(["measurement_id"])
        .withColumn("std_concept_id", F.lit(3034426)) # TODO: review arbitrary choice of code
        .withColumn(
            "value_converted",
            F.when(
                F.lower(F.col("unit_source_value")).like("s%"),
                F.round(F.try_cast(F.col("value_source_value"), "double"), 3)
            ).otherwise(F.lit(None))
        )
        .withColumn("unit_converted", F.lit("Second(s)"))
    )

    # rf
    labs_rf = (
        msmt_ca
        .filter(
            (F.col("ancestor_concept_id") == 40653663)
            | (F.col("measurement_concept_id").isin(3021614, 3015688, 3024763))
        )
        .withColumn("std_concept_id", F.lit(40653663))
        .withColumn(
            "value_converted",
            F.when(
                F.lower(F.col("unit_source_value")) == "iu/ml",
                F.round(F.try_cast(F.col("value_source_value"), "double"), 3)
            ).otherwise(F.lit(None))
        )
        .withColumn("unit_converted", F.lit("International Units/mL"))
    )

    # shbg
    labs_shbg = (
        msmt_ca
        .filter(F.col("ancestor_concept_id") == 40655429)
        .withColumn("std_concept_id", F.lit(40655429))
        .withColumn(
            "value_converted",
            F.when(
                F.lower(F.col("unit_source_value")) == "nmol/l",
                F.round(F.try_cast(F.col("value_source_value"), "double"), 3)
            ).otherwise(F.lit(None))
        )
        .withColumn("unit_converted", F.lit("nmol/L"))
    )

    # sodium
    labs_sodium = (
        msmt_ca
        .filter(F.col("ancestor_concept_id") == 40653762)
        .withColumn("std_concept_id", F.lit(40653762))
        .withColumn(
            "value_converted",
            F.when(
                F.lower(F.col("unit_source_value")).like("mmo%/l"),
                F.round(F.try_cast(F.col("value_source_value"), "double"), 3)
            ).otherwise(F.lit(None))
        )
        .withColumn("unit_converted", F.lit("mmol/L"))
    )

    # testosterone_free
    labs_testosterone_free = (
        msmt_ca
        .filter(F.col("ancestor_concept_id") == 40653808)
        .withColumn("std_concept_id", F.lit(40653808))
        .withColumn(
            "value_converted",
            F.when(
                F.lower(F.col("unit_source_value")) == "pg/ml",
                F.round(F.try_cast(F.col("value_source_value"), "double"), 3)
            ).when(
                F.lower(F.col("unit_source_value")) == "ng/dl",
                F.round(F.try_cast(F.col("value_source_value"), "double") * 10, 3)
            ).otherwise(F.lit(None))
        )
        .withColumn("unit_converted", F.lit("pg/mL"))
    )

    # tsh
    labs_tsh = (
        msmt_ca
        .filter(F.col("ancestor_concept_id") == 40653836)
        .withColumn("std_concept_id", F.lit(40653836))
        .withColumn(
            "value_converted",
            F.when(
                F.lower(F.col("unit_source_value")).rlike(r'^u.?iu.?/ml$')
                | F.lower(F.col("unit_source_value")).rlike(r'^mc?i?u.*/l$')
                | (F.lower(F.col("unit_source_value")) == "mciu/ml"),
                F.round(F.try_cast(F.col("value_source_value"), "double"), 3)
            ).otherwise(F.lit(None))
        )
        .withColumn("unit_converted", F.lit("uIU/mL"))
    )

    # transferrin
    labs_transferrin = (
        msmt_ca
        .filter(F.col("ancestor_concept_id") == 1990650)
        .withColumn("std_concept_id", F.lit(1990650))
        .withColumn(
            "value_converted",
            F.when(
                F.lower(F.col("unit_source_value")).rlike(r'^mg/\s?dl$'),
                F.round(F.try_cast(F.col("value_source_value"), "double"), 3)
            ).when(
                F.lower(F.col("unit_source_value")) == "g/l",
                F.round(F.try_cast(F.col("value_source_value"), "double") / 100, 3)
            ).when(
                F.lower(F.col("unit_source_value")) == "mg/l",
                F.round(F.try_cast(F.col("value_source_value"), "double") / 10, 3)
            ).when(
                F.lower(F.col("unit_source_value")) == "mg/ml",
                F.round(F.try_cast(F.col("value_source_value"), "double") * 100, 3)
            ).otherwise(F.lit(None))
        )
        .withColumn("unit_converted", F.lit("mg/dL"))
    )

    # triglycerides
    labs_triglycerides = (
        msmt_ca
        .filter(F.col("ancestor_concept_id") == 40653862)
        .withColumn("std_concept_id", F.lit(40653862))
        .withColumn(
            "value_converted",
            F.when(
                F.lower(F.col("unit_source_value")).rlike(r'^mg/\s?dl$'),
                F.round(F.try_cast(F.col("value_source_value"), "double"), 3)
            ).when(
                F.lower(F.col("unit_source_value")) == "g/l",
                F.round(F.try_cast(F.col("value_source_value"), "double") / 100, 3)
            ).when(
                F.lower(F.col("unit_source_value")) == "mg/l",
                F.round(F.try_cast(F.col("value_source_value"), "double") / 10, 3)
            ).when(
                F.lower(F.col("unit_source_value")) == "mg/ml",
                F.round(F.try_cast(F.col("value_source_value"), "double") * 100, 3)
            ).otherwise(F.lit(None))
        )
        .withColumn("unit_converted", F.lit("mg/dL"))
    )

    # troponin_i
    labs_troponin_i = (
        msmt_ca
        .filter(F.col("ancestor_concept_id") == 40653873)
        .withColumn("std_concept_id", F.lit(40653873))
        .withColumn(
            "value_converted",
            F.when(
                F.lower(F.col("unit_source_value")) == "ng/ml",
                F.round(F.try_cast(F.col("value_source_value"), "double"), 3)
            ).when(
                F.lower(F.col("unit_source_value")) == "ng/dl",
                F.round(F.try_cast(F.col("value_source_value"), "double") / 100, 3)
            ).when(
                F.lower(F.col("unit_source_value")) == "ng/l",
                F.round(F.try_cast(F.col("value_source_value"), "double") / 1000, 3)
            ).otherwise(F.lit(None))
        )
        .withColumn("unit_converted", F.lit("ng/mL"))
    )

    # troponin_t
    labs_troponin_t = (
        msmt_ca
        .filter(F.col("ancestor_concept_id") == 40653874)
        .withColumn("std_concept_id", F.lit(40653874))
        .withColumn(
            "value_converted",
            F.when(
                F.lower(F.col("unit_source_value")) == "ng/ml",
                F.round(F.try_cast(F.col("value_source_value"), "double"), 3)
            ).when(
                F.lower(F.col("unit_source_value")) == "ng/dl",
                F.round(F.try_cast(F.col("value_source_value"), "double") / 100, 3)
            ).when(
                F.lower(F.col("unit_source_value")) == "ng/l",
                F.round(F.try_cast(F.col("value_source_value"), "double") / 1000, 3)
            ).otherwise(F.lit(None))
        )
        .withColumn("unit_converted", F.lit("ng/mL"))
    )

    # urobilinogen
    labs_urobilinogen = (
        msmt_ca
        .filter(F.col("ancestor_concept_id") == 40656506)
        .withColumn("std_concept_id", F.lit(40656506))
        .withColumn(
            "value_converted",
            F.when(
                F.lower(F.col("unit_source_value")).rlike(r'^mg/\s?dl$'),
                F.round(F.try_cast(F.col("value_source_value"), "double"), 3)
            ).when(
                F.lower(F.col("unit_source_value")) == "g/l",
                F.round(F.try_cast(F.col("value_source_value"), "double") / 100, 3)
            ).when(
                F.lower(F.col("unit_source_value")) == "mg/l",
                F.round(F.try_cast(F.col("value_source_value"), "double") / 10, 3)
            ).when(
                F.lower(F.col("unit_source_value")) == "mg/ml",
                F.round(F.try_cast(F.col("value_source_value"), "double") * 100, 3)
            ).otherwise(F.lit(None))
        )
        .withColumn("unit_converted", F.lit("mg/dL"))
    )

    # vitals_bp_diastolic
    vitals_bp_diastolic = (
        msmt_ca
        .filter(F.col("measurement_concept_id") == 3012888)
        .dropDuplicates(["measurement_id"])
        .withColumn("std_concept_id", F.lit(3012888))
        .withColumn(
            "value_converted",
            F.when(
                F.lower(F.col("unit_source_value")) == "mmhg",
                F.round(F.try_cast(F.col("value_source_value"), "double"), 3)
            ).otherwise(F.lit(None))
        )
        .withColumn("unit_converted", F.lit("mmHg"))
    )

    # vitals_bp_systolic
    vitals_bp_systolic = (
        msmt_ca
        .filter(F.col("measurement_concept_id") == 3004249)
        .dropDuplicates(["measurement_id"])
        .withColumn("std_concept_id", F.lit(3004249))
        .withColumn(
            "value_converted",
            F.when(
                F.lower(F.col("unit_source_value")) == "mmhg",
                F.round(F.try_cast(F.col("value_source_value"), "double"), 3)
            ).otherwise(F.lit(None))
        )
        .withColumn("unit_converted", F.lit("mmHg"))
    )

    # vitals_bmi
    vitals_bmi = (
        msmt_ca
        .filter(F.col("measurement_concept_id").isin(44783982, 4245997))
        .dropDuplicates(["measurement_id"])
        .withColumn("std_concept_id", F.lit(44783982)) # TODO: review arbitrary choice of code
        .withColumn(
            "value_converted",
            F.when(
                F.lower(F.col("unit_source_value")) == "kg/m^2",
                F.round(F.try_cast(F.col("value_source_value"), "double"), 3)
            ).otherwise(F.lit(None))
        )
        .withColumn("unit_converted", F.lit("kg/m^2"))
    )

    # vitals_height
    vitals_height = (
        msmt_ca
        .filter(F.col("ancestor_concept_id") == 40655804)
        .withColumn("std_concept_id", F.lit(40655804))
        .withColumn(
            "value_converted",
            F.when(
                F.lower(F.col("unit_source_value")).like("in%"),
                F.round(F.try_cast(F.col("value_source_value"), "double"), 3)
            ).when(
                F.lower(F.col("unit_source_value")) == "ft",
                F.round(F.try_cast(F.col("value_source_value"), "double") * 12, 3)
            ).otherwise(F.lit(None))
        )
        .withColumn("unit_converted", F.lit("Inches"))
    )

    # vitals_o2sat
    vitals_o2sat = (
        msmt_ca
        .filter(F.col("ancestor_concept_id") == 40654168)
        .withColumn("std_concept_id", F.lit(40654168))
        .withColumn(
            "value_converted",
            F.when(
                F.col("unit_source_value").rlike(r'^%.*$'),
                F.round(F.try_cast(F.col("value_source_value"), "double"), 3)
            ).otherwise(F.lit(None))
        )
        .withColumn("unit_converted", F.lit("%"))
    )

    # vitals_pulse
    vitals_pulse = (
        msmt_ca
        .filter(F.col("ancestor_concept_id") == 40654164)
        .withColumn("std_concept_id", F.lit(40654164))
        .withColumn(
            "value_converted",
            F.when(
                F.lower(F.col("unit_source_value")) == "counts/min",
                F.round(F.try_cast(F.col("value_source_value"), "double"), 3)
            ).otherwise(F.lit(None))
        )
        .withColumn("unit_converted", F.lit("Counts/Min"))
    )

    # vitals_resp_rate
    vitals_resp_rate = (
        msmt_ca
        .filter(F.col("ancestor_concept_id") == 40654163)
        .withColumn("std_concept_id", F.lit(40654163))
        .withColumn(
            "value_converted",
            F.when(
                F.lower(F.col("unit_source_value")) == "counts/min",
                F.round(F.try_cast(F.col("value_source_value"), "double"), 3)
            ).otherwise(F.lit(None))
        )
        .withColumn("unit_converted", F.lit("Counts/Min"))
    )

    # vitals_temperature
    vitals_temperature = (
        msmt_ca
        .filter(F.col("ancestor_concept_id") == 40654162)
        .withColumn("std_concept_id", F.lit(40654162))
        .withColumn(
            "value_converted",
            F.when(
                F.lower(F.col("unit_source_value")) == "f",
                F.round(F.try_cast(F.col("value_source_value"), "double"), 3)
            ).when(
                F.lower(F.col("unit_source_value")).like("%c%"),
                F.round(F.try_cast(F.col("value_source_value"), "double") * (9 / 5) + 32, 3)
            ).otherwise(F.lit(None))
        )
        .withColumn("unit_converted", F.lit("Degrees Fahrenheit"))
    )

    # vitals_weight
    vitals_weight = (
        msmt_ca
        .filter(F.col("ancestor_concept_id") == 40655805)
        .withColumn("std_concept_id", F.lit(40655805))
        .withColumn(
            "value_converted",
            F.when(
                F.lower(F.col("unit_source_value")).like("o%"),
                F.round(F.try_cast(F.col("value_source_value"), "double"), 3)
            ).when(
                F.lower(F.col("unit_source_value")).like("lb%"),
                F.round(F.try_cast(F.col("value_source_value"), "double") * 16, 3)
            ).otherwise(F.lit(None))
        )
        .withColumn("unit_converted", F.lit("oz"))
    )

    # TODO: add more!

    # union all labs/vitals frames, pared to the input measurement schema + std_concept_id/value_converted/unit_converted
    OUTPUT_COLUMNS = [
        "measurement_id", "person_id", "measurement_concept_id", "measurement_date", "measurement_datetime",
        "measurement_type_concept_id", "operator_concept_id", "value_as_number", "value_as_concept_id",
        "unit_concept_id", "range_low", "range_high", "visit_occurrence_id", "measurement_source_value",
        "measurement_source_concept_id", "unit_source_value", "value_source_value",
        "std_concept_id", "value_converted", "unit_converted",
    ]
    all_frames = [
        labs_albumin, labs_alp, labs_alt, labs_amh, labs_anion_gap, labs_apo_b, labs_ast, labs_basophils,
        labs_beta_globulin, labs_bilirubin_total, labs_bun, labs_c4, labs_calcium, labs_ccpab, labs_chloride,
        labs_chol_hdl, labs_chol_ldl, labs_chol_total, labs_chol_vldl, labs_ck, labs_covid, labs_creatinine,
        labs_creatinine_urine, labs_crp, labs_crp_hs, labs_eosinophils, labs_esr,
        labs_ferritin, labs_folate, labs_ggt, labs_glucose, labs_glucose_fasting, labs_glucose_urine,
        labs_granulocytes, labs_hba1c, labs_hemoglobin, labs_iga, labs_igg, labs_igm, labs_inr, labs_iron,
        labs_ketones, labs_leukocyte_esterase, labs_lipoprotein_a, labs_lymphocytes, labs_magnesium,
        labs_metamyelocytes, labs_metanephrine, labs_microalbumin, labs_microalbumin_creatinine_ratio,
        labs_monocytes, labs_neutrophils, labs_neutrophils_bandform, labs_neutrophils_segmented,
        labs_normetanephrine, labs_phosphate, labs_platelets, labs_potassium, labs_prealbumin,
        labs_promyelocytes, labs_protein, labs_protein_urine, labs_pt, labs_ptt, labs_rbc, labs_rbc_urine,
        labs_rf, labs_shbg, labs_sodium, labs_testosterone_free, labs_transferrin, labs_triglycerides,
        labs_troponin_i, labs_troponin_t, labs_tsh, labs_urobilinogen, labs_wbc, labs_wbc_urine,
        vitals_bmi, vitals_bp_diastolic, vitals_bp_systolic, vitals_height, vitals_o2sat, vitals_pulse,
        vitals_resp_rate, vitals_temperature, vitals_weight,
    ]
    all_frames = [df.select(*[c for c in OUTPUT_COLUMNS if c in df.columns]) for df in all_frames]
    labs_vitals_union = all_frames[0]
    for frame in all_frames[1:]:
        labs_vitals_union = labs_vitals_union.unionByName(frame, allowMissingColumns=True)
    labs_vitals_union = labs_vitals_union.withColumnRenamed("value_converted", "numeric_value_converted")

    # perform fallback logic for all unmapped measurements (for now, null all values)
    unmapped_msmt = msmt.join(
        labs_vitals_union.select("measurement_id").distinct(),
        on="measurement_id",
        how="left_anti"
    )
    fallback_msmt = (
        unmapped_msmt
        .withColumn("numeric_value_converted", F.lit(None))
        .withColumn("text_value_converted", F.lit(None))
        .withColumn("unit_converted", F.lit(None))
        .withColumn("std_concept_id", F.col("measurement_concept_id"))
    ) # TODO: smarter fallback to ensure unit is homogenous w/in std_concept_id

    # union all data frames and return
    final_msmt = labs_vitals_union.unionByName(fallback_msmt, allowMissingColumns=True)

    return final_msmt

# TODO: TEST each measurement type has at most one unit
# TODO: TEST each measurement id is still present