import pyspark.sql.functions as F
from pyspark.sql import Window
from pyspark.sql import functions as F, Window

def standardize(msmt, concept, concept_ancestor):
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

    # init msmts trackers
    all_frames = []

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
                F.round(F.col("value_source_value").try_cast("double"), 3)
            ).otherwise(F.lit(None))
        )
        .withColumn("unit_converted", F.lit("Unit/L"))
    )
    all_frames.append(labs_alt)

    # albumin
    labs_albumin = (
        msmt_ca
        .filter(F.col("ancestor_concept_id") == 40652534)
        .withColumn("std_concept_id", F.lit(40652534))
        .withColumn(
            "value_converted",
            F.when(
                F.lower(F.col("unit_source_value")).rlike('^gm?/dl$'),
                F.round(F.col("value_source_value").try_cast("double"), 3)
            ).when(
                F.lower(F.col("unit_source_value")) == "mg/dl",
                F.round(F.col("value_source_value").try_cast("double")/1000, 3)
            )
            .when(
                F.lower(F.col("unit_source_value")) == "mg/l",
                F.round(F.col("value_source_value").try_cast("double")/10000, 3)
            ).otherwise(F.lit(None))
        )
        .withColumn("unit_converted", F.lit("g/dL"))
    )
    all_frames.append(labs_albumin)

    # alp
    labs_alp = (
        msmt_ca
        .filter(F.col("ancestor_concept_id") == 40652549)
        .withColumn("std_concept_id", F.lit(40652549))
        .withColumn(
            "value_converted",
            F.when(
                F.lower(F.col("unit_source_value")).rlike('^.*i?u.*/l$'),
                F.round(F.col("value_source_value").try_cast("double"), 3)
            ).otherwise(F.lit(None))
        )
        .withColumn("unit_converted", F.lit("Unit/L"))
    )
    all_frames.append(labs_alp)

    # anion gap
    labs_anion_gap = (
        msmt_ca
        .filter(F.col("ancestor_concept_id") == 40652611)
        .withColumn("std_concept_id", F.lit(40652611))
        .withColumn(
            "value_converted",
            F.when(
                F.lower(F.col("unit_source_value")).like('mmo%/l'),
                F.round(F.col("value_source_value").try_cast("double"), 3)
            ).otherwise(F.lit(None))
        )
        .withColumn("unit_converted", F.lit("mmol/L"))
    )
    all_frames.append(labs_anion_gap)

    # apo_b
    labs_apo_b = (
        msmt_ca
        .filter(F.col("ancestor_concept_id") == 40652616)
        .withColumn("std_concept_id", F.lit(40652616))
        .withColumn(
            "value_converted",
            F.when(
                F.lower(F.col("unit_source_value")).rlike(r'^mg/\s?dl$'),
                F.round(F.col("value_source_value").try_cast("double"), 3)
            ).when(
                F.lower(F.col("unit_source_value")) == "g/l",
                F.round(F.col("value_source_value").try_cast("double") / 100, 3)
            ).when(
                F.lower(F.col("unit_source_value")) == "mg/l",
                F.round(F.col("value_source_value").try_cast("double") / 10, 3)
            ).when(
                F.lower(F.col("unit_source_value")) == "mg/ml",
                F.round(F.col("value_source_value").try_cast("double") * 100, 3)
            ).otherwise(F.lit(None))
        )
        .withColumn("unit_converted", F.lit("mg/dL"))
    )
    all_frames.append(labs_apo_b)

    # amh
    labs_amh = (
        msmt_ca
        .filter(F.col("ancestor_concept_id") == 40653357)
        .withColumn("std_concept_id", F.lit(40653357))
        .withColumn(
            "value_converted",
            F.when(
                F.lower(F.col("unit_source_value")) == "ng/ml",
                F.round(F.col("value_source_value").try_cast("double"), 3)
            ).when(
                F.lower(F.col("unit_source_value")) == "ng/dl",
                F.round(F.col("value_source_value").try_cast("double") / 100, 3)
            ).when(
                F.lower(F.col("unit_source_value")) == "ng/l",
                F.round(F.col("value_source_value").try_cast("double") / 1000, 3)
            ).otherwise(F.lit(None))
        )
        .withColumn("unit_converted", F.lit("ng/mL"))
    )
    all_frames.append(labs_amh)

    # ast
    labs_ast = (
        msmt_ca
        .filter(F.col("ancestor_concept_id") == 40652640)
        .withColumn("std_concept_id", F.lit(40652640))
        .withColumn(
            "value_converted",
            F.when(
                F.lower(F.col("unit_source_value")).rlike(r'^.*i?u.*/l$'),
                F.round(F.col("value_source_value").try_cast("double"), 3)
            ).otherwise(F.lit(None))
        )
        .withColumn("unit_converted", F.lit("Unit/L"))
    )
    all_frames.append(labs_ast)

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
                F.round(F.col("value_source_value").try_cast("double"), 3)
            ).when(
                F.lower(F.col("unit_source_value")).rlike(r'^cell.*/[ucm].+$'),
                F.round(F.col("value_source_value").try_cast("double") / 1000, 3)
            ).otherwise(F.lit(None))
        )
        .withColumn("unit_converted", F.lit("Thousand/uL"))
    )
    all_frames.append(labs_basophils)

    # beta_globulin
    labs_beta_globulin = (
        msmt_ca
        .filter(F.col("ancestor_concept_id") == 1990626)
        .withColumn("std_concept_id", F.lit(1990626))
        .withColumn(
            "value_converted",
            F.when(
                F.lower(F.col("unit_source_value")).rlike(r'^gm?/dl$'),
                F.round(F.col("value_source_value").try_cast("double"), 3)
            ).when(
                F.lower(F.col("unit_source_value")) == "mg/dl",
                F.round(F.col("value_source_value").try_cast("double") / 1000, 3)
            ).when(
                F.lower(F.col("unit_source_value")) == "mg/l",
                F.round(F.col("value_source_value").try_cast("double") / 10000, 3)
            ).otherwise(F.lit(None))
        )
        .withColumn("unit_converted", F.lit("g/dL"))
    )
    all_frames.append(labs_beta_globulin)

    # bilirubin_total
    labs_bilirubin_total = (
        msmt_ca
        .filter(F.col("ancestor_concept_id") == 40652709)
        .withColumn("std_concept_id", F.lit(40652709))
        .withColumn(
            "value_converted",
            F.when(
                F.lower(F.col("unit_source_value")).rlike(r'^mg/\s?dl$'),
                F.round(F.col("value_source_value").try_cast("double"), 3)
            ).when(
                F.lower(F.col("unit_source_value")) == "g/l",
                F.round(F.col("value_source_value").try_cast("double") / 100, 3)
            ).when(
                F.lower(F.col("unit_source_value")) == "mg/l",
                F.round(F.col("value_source_value").try_cast("double") / 10, 3)
            ).when(
                F.lower(F.col("unit_source_value")) == "mg/ml",
                F.round(F.col("value_source_value").try_cast("double") * 100, 3)
            ).otherwise(F.lit(None))
        )
        .withColumn("unit_converted", F.lit("mg/dL"))
    )
    all_frames.append(labs_bilirubin_total)

    # bun
    labs_bun = (
        msmt_ca
        .filter(F.col("ancestor_concept_id") == 40653900)
        .withColumn("std_concept_id", F.lit(40653900))
        .withColumn(
            "value_converted",
            F.when(
                F.lower(F.col("unit_source_value")).rlike(r'^mg/\s?dl$'),
                F.round(F.col("value_source_value").try_cast("double"), 3)
            ).when(
                F.lower(F.col("unit_source_value")) == "g/l",
                F.round(F.col("value_source_value").try_cast("double") / 100, 3)
            ).when(
                F.lower(F.col("unit_source_value")) == "mg/l",
                F.round(F.col("value_source_value").try_cast("double") / 10, 3)
            ).when(
                F.lower(F.col("unit_source_value")) == "mg/ml",
                F.round(F.col("value_source_value").try_cast("double") * 100, 3)
            ).otherwise(F.lit(None))
        )
        .withColumn("unit_converted", F.lit("mg/dL"))
    )
    all_frames.append(labs_bun)

    # crp
    labs_crp = (
        msmt_ca
        .filter((F.col("ancestor_concept_id") == 40652733) | (F.col("measurement_concept_id") == 3020460))
        .dropDuplicates(["measurement_id"])
        .withColumn("std_concept_id", F.lit(40652733))
        .withColumn(
            "value_converted",
            F.when(
                F.lower(F.col("unit_source_value")).rlike(r'^mg/\s?dl$'),
                F.round(F.col("value_source_value").try_cast("double"), 3)
            ).when(
                F.lower(F.col("unit_source_value")) == "g/l",
                F.round(F.col("value_source_value").try_cast("double") / 100, 3)
            ).when(
                F.lower(F.col("unit_source_value")) == "mg/l",
                F.round(F.col("value_source_value").try_cast("double") / 10, 3)
            ).when(
                F.lower(F.col("unit_source_value")) == "mg/ml",
                F.round(F.col("value_source_value").try_cast("double") * 100, 3)
            ).otherwise(F.lit(None))
        )
        .withColumn("unit_converted", F.lit("mg/dL"))
    )
    all_frames.append(labs_crp)

    # crp_hs
    labs_crp_hs = (
        msmt_ca
        .filter(F.col("ancestor_concept_id") == 40654479)
        .withColumn("std_concept_id", F.lit(40654479))
        .withColumn(
            "value_converted",
            F.when(
                F.lower(F.col("unit_source_value")) == "mg/l",
                F.round(F.col("value_source_value").try_cast("double"), 3)
            ).when(
                F.lower(F.col("unit_source_value")) == "mg/dl",
                F.round(F.col("value_source_value").try_cast("double") * 10, 3)
            ).otherwise(F.lit(None))
        )
        .withColumn("unit_converted", F.lit("mg/L"))
    )
    all_frames.append(labs_crp_hs)

    # calcium
    labs_calcium = (
        msmt_ca
        .filter(F.col("ancestor_concept_id") == 40652745)
        .withColumn("std_concept_id", F.lit(40652745))
        .withColumn(
            "value_converted",
            F.when(
                F.lower(F.col("unit_source_value")).rlike(r'^mg/\s?dl$'),
                F.round(F.col("value_source_value").try_cast("double"), 3)
            ).when(
                F.lower(F.col("unit_source_value")) == "g/l",
                F.round(F.col("value_source_value").try_cast("double") / 100, 3)
            ).when(
                F.lower(F.col("unit_source_value")) == "mg/l",
                F.round(F.col("value_source_value").try_cast("double") / 10, 3)
            ).when(
                F.lower(F.col("unit_source_value")) == "mg/ml",
                F.round(F.col("value_source_value").try_cast("double") * 100, 3)
            ).otherwise(F.lit(None))
        )
        .withColumn("unit_converted", F.lit("mg/dL"))
    )
    all_frames.append(labs_calcium)

    # chloride
    labs_chloride = (
        msmt_ca
        .filter(F.col("ancestor_concept_id") == 40652796)
        .withColumn("std_concept_id", F.lit(40652796))
        .withColumn(
            "value_converted",
            F.when(
                F.lower(F.col("unit_source_value")).like("mmo%/l"),
                F.round(F.col("value_source_value").try_cast("double"), 3)
            ).otherwise(F.lit(None))
        )
        .withColumn("unit_converted", F.lit("mmol/L"))
    )
    all_frames.append(labs_chloride)

    # chol_hdl
    labs_chol_hdl = (
        msmt_ca
        .filter(F.col("ancestor_concept_id") == 40652802)
        .withColumn("std_concept_id", F.lit(40652802))
        .withColumn(
            "value_converted",
            F.when(
                F.lower(F.col("unit_source_value")).rlike(r'^mg/\s?dl$'),
                F.round(F.col("value_source_value").try_cast("double"), 3)
            ).when(
                F.lower(F.col("unit_source_value")) == "g/l",
                F.round(F.col("value_source_value").try_cast("double") / 100, 3)
            ).when(
                F.lower(F.col("unit_source_value")) == "mg/l",
                F.round(F.col("value_source_value").try_cast("double") / 10, 3)
            ).when(
                F.lower(F.col("unit_source_value")) == "mg/ml",
                F.round(F.col("value_source_value").try_cast("double") * 100, 3)
            ).otherwise(F.lit(None))
        )
        .withColumn("unit_converted", F.lit("mg/dL"))
    )
    all_frames.append(labs_chol_hdl)

    # chol_ldl
    labs_chol_ldl = (
        msmt_ca
        .filter(F.col("ancestor_concept_id") == 40654572)
        .withColumn("std_concept_id", F.lit(40654572))
        .withColumn(
            "value_converted",
            F.when(
                F.lower(F.col("unit_source_value")).rlike(r'^mg/\s?dl$'),
                F.round(F.col("value_source_value").try_cast("double"), 3)
            ).when(
                F.lower(F.col("unit_source_value")) == "g/l",
                F.round(F.col("value_source_value").try_cast("double") / 100, 3)
            ).when(
                F.lower(F.col("unit_source_value")) == "mg/l",
                F.round(F.col("value_source_value").try_cast("double") / 10, 3)
            ).when(
                F.lower(F.col("unit_source_value")) == "mg/ml",
                F.round(F.col("value_source_value").try_cast("double") * 100, 3)
            ).otherwise(F.lit(None))
        )
        .withColumn("unit_converted", F.lit("mg/dL"))
    )
    all_frames.append(labs_chol_ldl)

    # chol_vldl
    labs_chol_vldl = (
        msmt_ca
        .filter((F.col("ancestor_concept_id") == 40654576) | (F.col("measurement_concept_id") == 3009596))
        .dropDuplicates(["measurement_id"])
        .withColumn("std_concept_id", F.lit(40654576))
        .withColumn(
            "value_converted",
            F.when(
                F.lower(F.col("unit_source_value")).rlike(r'^mg/\s?dl$'),
                F.round(F.col("value_source_value").try_cast("double"), 3)
            ).when(
                F.lower(F.col("unit_source_value")) == "g/l",
                F.round(F.col("value_source_value").try_cast("double") / 100, 3)
            ).when(
                F.lower(F.col("unit_source_value")) == "mg/l",
                F.round(F.col("value_source_value").try_cast("double") / 10, 3)
            ).when(
                F.lower(F.col("unit_source_value")) == "mg/ml",
                F.round(F.col("value_source_value").try_cast("double") * 100, 3)
            ).otherwise(F.lit(None))
        )
        .withColumn("unit_converted", F.lit("mg/dL"))
    )
    all_frames.append(labs_chol_vldl)  

    # chol_total
    labs_chol_total = (
        msmt_ca
        .filter(F.col("ancestor_concept_id") == 40652808)
        .withColumn("std_concept_id", F.lit(40652808))
        .withColumn(
            "value_converted",
            F.when(
                F.lower(F.col("unit_source_value")).rlike(r'^mg/\s?dl$'),
                F.round(F.col("value_source_value").try_cast("double"), 3)
            ).when(
                F.lower(F.col("unit_source_value")) == "g/l",
                F.round(F.col("value_source_value").try_cast("double") / 100, 3)
            ).when(
                F.lower(F.col("unit_source_value")) == "mg/l",
                F.round(F.col("value_source_value").try_cast("double") / 10, 3)
            ).when(
                F.lower(F.col("unit_source_value")) == "mg/ml",
                F.round(F.col("value_source_value").try_cast("double") * 100, 3)
            ).otherwise(F.lit(None))
        )
        .withColumn("unit_converted", F.lit("mg/dL"))
    )
    all_frames.append(labs_chol_total)

    # c4
    labs_c4 = (
        msmt_ca
        .filter(F.col("ancestor_concept_id") == 40654637)
        .withColumn("std_concept_id", F.lit(40654637))
        .withColumn(
            "value_converted",
            F.when(
                F.lower(F.col("unit_source_value")).rlike(r'^mg/\s?dl$'),
                F.round(F.col("value_source_value").try_cast("double"), 3)
            ).when(
                F.lower(F.col("unit_source_value")) == "g/l",
                F.round(F.col("value_source_value").try_cast("double") / 100, 3)
            ).when(
                F.lower(F.col("unit_source_value")) == "mg/l",
                F.round(F.col("value_source_value").try_cast("double") / 10, 3)
            ).when(
                F.lower(F.col("unit_source_value")) == "mg/ml",
                F.round(F.col("value_source_value").try_cast("double") * 100, 3)
            ).otherwise(F.lit(None))
        )
        .withColumn("unit_converted", F.lit("mg/dL"))
    )
    all_frames.append(labs_c4)

    # covid
    labs_covid = (
        msmt_ca
        .filter(F.col("ancestor_concept_id") == 36662633)
        .withColumn("std_concept_id", F.lit(36662633))
        .withColumn("value_converted", F.lit("NA"))
        .withColumn("unit_converted", F.lit("NA"))
    )
    all_frames.append(labs_covid)

    # ck
    labs_ck = (
        msmt_ca
        .filter(F.col("ancestor_concept_id") == 40652867)
        .withColumn("std_concept_id", F.lit(40652867))
        .withColumn(
            "value_converted",
            F.when(
                F.lower(F.col("unit_source_value")) == "ng/ml",
                F.round(F.col("value_source_value").try_cast("double"), 3)
            ).when(
                F.lower(F.col("unit_source_value")) == "ng/dl",
                F.round(F.col("value_source_value").try_cast("double") / 100, 3)
            ).when(
                F.lower(F.col("unit_source_value")) == "ng/l",
                F.round(F.col("value_source_value").try_cast("double") / 1000, 3)
            ).otherwise(F.lit(None))
        )
        .withColumn("unit_converted", F.lit("ng/mL"))
    )
    all_frames.append(labs_ck)

    # creatinine
    labs_creatinine = (
        msmt_ca
        .filter(F.col("ancestor_concept_id") == 40652870)
        .withColumn("std_concept_id", F.lit(40652870))
        .withColumn(
            "value_converted",
            F.when(
                F.lower(F.col("unit_source_value")).rlike(r'^mg/\s?dl$'),
                F.round(F.col("value_source_value").try_cast("double"), 3)
            ).when(
                F.lower(F.col("unit_source_value")) == "g/l",
                F.round(F.col("value_source_value").try_cast("double") / 100, 3)
            ).when(
                F.lower(F.col("unit_source_value")) == "mg/l",
                F.round(F.col("value_source_value").try_cast("double") / 10, 3)
            ).when(
                F.lower(F.col("unit_source_value")) == "mg/ml",
                F.round(F.col("value_source_value").try_cast("double") * 100, 3)
            ).otherwise(F.lit(None))
        )
        .withColumn("unit_converted", F.lit("mg/dL"))
    )
    all_frames.append(labs_creatinine)

    # creatinine_urine
    labs_creatinine_urine = (
        msmt_ca
        .filter(F.col("ancestor_concept_id") == 40656057)
        .withColumn("std_concept_id", F.lit(40656057))
        .withColumn(
            "value_converted",
            F.when(
                F.lower(F.col("unit_source_value")).rlike(r'^mg/\s?dl$'),
                F.round(F.col("value_source_value").try_cast("double"), 3)
            ).when(
                F.lower(F.col("unit_source_value")) == "g/l",
                F.round(F.col("value_source_value").try_cast("double") / 100, 3)
            ).when(
                F.lower(F.col("unit_source_value")) == "mg/l",
                F.round(F.col("value_source_value").try_cast("double") / 10, 3)
            ).when(
                F.lower(F.col("unit_source_value")) == "mg/ml",
                F.round(F.col("value_source_value").try_cast("double") * 100, 3)
            ).otherwise(F.lit(None))
        )
        .withColumn("unit_converted", F.lit("mg/dL"))
    )
    all_frames.append(labs_creatinine_urine)

    # ccpab
    labs_ccpab = (
        msmt_ca
        .filter(F.col("ancestor_concept_id") == 40652885)
        .withColumn("std_concept_id", F.lit(40652885))
        .withColumn("value_converted", F.lit("NA"))
        .withColumn("unit_converted", F.lit("NA"))
    )
    all_frames.append(labs_ccpab)

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
                F.round(F.col("value_source_value").try_cast("double"), 3)
            ).when(
                F.lower(F.col("unit_source_value")).rlike(r'^cell.*/[ucm].+$'),
                F.round(F.col("value_source_value").try_cast("double") / 1000, 3)
            ).otherwise(F.lit(None))
        )
        .withColumn("unit_converted", F.lit("Thousand/uL"))
    )
    all_frames.append(labs_eosinophils)

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
                F.round(F.col("value_source_value").try_cast("double"), 3)
            ).otherwise(F.lit(None))
        )
        .withColumn("unit_converted", F.lit("Million/uL"))
    )
    all_frames.append(labs_rbc)

    # rbc_urine
    labs_rbc_urine = (
        msmt_ca
        .filter(F.col("ancestor_concept_id") == 40657685)
        .withColumn("std_concept_id", F.lit(40657685))
        .withColumn("value_converted", F.lit("NA"))
        .withColumn("unit_converted", F.lit("NA"))
    )
    all_frames.append(labs_rbc_urine)

    # esr
    labs_esr = (
        msmt_ca
        .join(concept, msmt_ca.descendant_concept_id == concept.concept_id, "inner")
        .filter(
            (F.col("ancestor_concept_id") == 4028908)
            & F.lower(F.col("concept_name")).like("%eryth%")
            & F.lower(F.col("concept_name")).like("%sed%")
        )
        .withColumn("std_concept_id", F.lit(3015183))
        .withColumn(
            "value_converted",
            F.when(
                F.lower(F.col("unit_source_value")).like("mm/h%"),
                F.round(F.col("value_source_value").try_cast("double"), 3)
            ).otherwise(F.lit(None))
        )
        .withColumn("unit_converted", F.lit("mm/h"))
    )
    all_frames.append(labs_esr)

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
                F.round(F.col("value_source_value").try_cast("double"), 3)
            ).otherwise(F.lit(None))
        )
        .withColumn("unit_converted", F.lit("Unit/L"))
    )
    all_frames.append(labs_ggt)

    # ferritin
    labs_ferritin = (
        msmt_ca
        .filter(F.col("ancestor_concept_id") == 40652982)
        .withColumn("std_concept_id", F.lit(40652982))
        .withColumn(
            "value_converted",
            F.when(
                F.lower(F.col("unit_source_value")) == "ng/ml",
                F.round(F.col("value_source_value").try_cast("double"), 3)
            ).when(
                F.lower(F.col("unit_source_value")) == "ng/dl",
                F.round(F.col("value_source_value").try_cast("double") / 100, 3)
            ).when(
                F.lower(F.col("unit_source_value")) == "ng/l",
                F.round(F.col("value_source_value").try_cast("double") / 1000, 3)
            ).otherwise(F.lit(None))
        )
        .withColumn("unit_converted", F.lit("ng/mL"))
    )
    all_frames.append(labs_ferritin)

    # folate
    labs_folate = (
        msmt_ca
        .filter(F.col("ancestor_concept_id") == 40652995)
        .withColumn("std_concept_id", F.lit(40652995))
        .withColumn(
            "value_converted",
            F.when(
                F.lower(F.col("unit_source_value")) == "ng/ml",
                F.round(F.col("value_source_value").try_cast("double"), 3)
            ).when(
                F.lower(F.col("unit_source_value")) == "ng/dl",
                F.round(F.col("value_source_value").try_cast("double") / 100, 3)
            ).when(
                F.lower(F.col("unit_source_value")) == "ng/l",
                F.round(F.col("value_source_value").try_cast("double") / 1000, 3)
            ).otherwise(F.lit(None))
        )
        .withColumn("unit_converted", F.lit("ng/mL"))
    )
    all_frames.append(labs_folate)

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
                F.round(F.col("value_source_value").try_cast("double"), 3)
            ).when(
                F.lower(F.col("unit_source_value")) == "g/l",
                F.round(F.col("value_source_value").try_cast("double") / 100, 3)
            ).when(
                F.lower(F.col("unit_source_value")) == "mg/l",
                F.round(F.col("value_source_value").try_cast("double") / 10, 3)
            ).when(
                F.lower(F.col("unit_source_value")) == "mg/ml",
                F.round(F.col("value_source_value").try_cast("double") * 100, 3)
            ).otherwise(F.lit(None))
        )
        .withColumn("unit_converted", F.lit("mg/dL"))
    )
    all_frames.append(labs_glucose)

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
                F.round(F.col("value_source_value").try_cast("double"), 3)
            ).when(
                F.lower(F.col("unit_source_value")) == "g/l",
                F.round(F.col("value_source_value").try_cast("double") / 100, 3)
            ).when(
                F.lower(F.col("unit_source_value")) == "mg/l",
                F.round(F.col("value_source_value").try_cast("double") / 10, 3)
            ).when(
                F.lower(F.col("unit_source_value")) == "mg/ml",
                F.round(F.col("value_source_value").try_cast("double") * 100, 3)
            ).otherwise(F.lit(None))
        )
        .withColumn("unit_converted", F.lit("mg/dL"))
    )
    all_frames.append(labs_glucose_fasting)

    # glucose_urine
    labs_glucose_urine = (
        msmt_ca
        .filter(F.col("ancestor_concept_id") == 40657691)
        .withColumn("std_concept_id", F.lit(40657691))
        .withColumn(
            "value_converted",
            F.when(
                F.lower(F.col("unit_source_value")).rlike(r'^mg/\s?dl$'),
                F.round(F.col("value_source_value").try_cast("double"), 3)
            ).when(
                F.lower(F.col("unit_source_value")) == "g/l",
                F.round(F.col("value_source_value").try_cast("double") / 100, 3)
            ).when(
                F.lower(F.col("unit_source_value")) == "mg/l",
                F.round(F.col("value_source_value").try_cast("double") / 10, 3)
            ).when(
                F.lower(F.col("unit_source_value")) == "mg/ml",
                F.round(F.col("value_source_value").try_cast("double") * 100, 3)
            ).otherwise(F.lit(None))
        )
        .withColumn("unit_converted", F.lit("mg/dL"))
    )
    all_frames.append(labs_glucose_urine)

    # granulocytes
    labs_granulocytes = (
        msmt_ca
        .filter(F.col("ancestor_concept_id") == 40654016)
        .withColumn("std_concept_id", F.lit(40654016))
        .withColumn("value_converted", F.lit("NA"))
        .withColumn("unit_converted", F.lit("NA"))
    )
    all_frames.append(labs_granulocytes)

    # hemoglobin
    labs_hemoglobin = (
        msmt_ca
        .filter(F.col("ancestor_concept_id") == 40654905)
        .withColumn("std_concept_id", F.lit(40654905))
        .withColumn(
            "value_converted",
            F.when(
                F.lower(F.col("unit_source_value")).rlike(r'^gm?/dl$'),
                F.round(F.col("value_source_value").try_cast("double"), 3)
            ).when(
                F.lower(F.col("unit_source_value")) == "mg/dl",
                F.round(F.col("value_source_value").try_cast("double") / 1000, 3)
            ).when(
                F.lower(F.col("unit_source_value")) == "mg/l",
                F.round(F.col("value_source_value").try_cast("double") / 10000, 3)
            ).otherwise(F.lit(None))
        )
        .withColumn("unit_converted", F.lit("g/dL"))
    )
    all_frames.append(labs_hemoglobin)

    # hba1c
    labs_hba1c = (
        msmt_ca
        .filter(F.col("ancestor_concept_id") == 1621295)
        .withColumn("std_concept_id", F.lit(1621295))
        .withColumn(
            "value_converted",
            F.when(
                F.col("unit_source_value").rlike(r'^%.*$'),
                F.round(F.col("value_source_value").try_cast("double"), 3)
            ).otherwise(F.lit(None))
        )
        .withColumn("unit_converted", F.lit("%"))
    )
    all_frames.append(labs_hba1c)

    # iga
    labs_iga = (
        msmt_ca
        .filter(F.col("ancestor_concept_id") == 40653141)
        .withColumn("std_concept_id", F.lit(40653141))
        .withColumn(
            "value_converted",
            F.when(
                F.lower(F.col("unit_source_value")).rlike(r'^mg/\s?dl$'),
                F.round(F.col("value_source_value").try_cast("double"), 3)
            ).when(
                F.lower(F.col("unit_source_value")) == "g/l",
                F.round(F.col("value_source_value").try_cast("double") / 100, 3)
            ).when(
                F.lower(F.col("unit_source_value")) == "mg/l",
                F.round(F.col("value_source_value").try_cast("double") / 10, 3)
            ).when(
                F.lower(F.col("unit_source_value")) == "mg/ml",
                F.round(F.col("value_source_value").try_cast("double") * 100, 3)
            ).otherwise(F.lit(None))
        )
        .withColumn("unit_converted", F.lit("mg/dL"))
    )
    all_frames.append(labs_iga)

    # igg
    labs_igg = (
        msmt_ca
        .filter(F.col("ancestor_concept_id") == 40653151)
        .withColumn("std_concept_id", F.lit(40653151))
        .withColumn(
            "value_converted",
            F.when(
                F.lower(F.col("unit_source_value")).rlike(r'^mg/\s?dl$'),
                F.round(F.col("value_source_value").try_cast("double"), 3)
            ).when(
                F.lower(F.col("unit_source_value")) == "g/l",
                F.round(F.col("value_source_value").try_cast("double") / 100, 3)
            ).when(
                F.lower(F.col("unit_source_value")) == "mg/l",
                F.round(F.col("value_source_value").try_cast("double") / 10, 3)
            ).when(
                F.lower(F.col("unit_source_value")) == "mg/ml",
                F.round(F.col("value_source_value").try_cast("double") * 100, 3)
            ).otherwise(F.lit(None))
        )
        .withColumn("unit_converted", F.lit("mg/dL"))
    )
    all_frames.append(labs_igg)

    # igm
    labs_igm = (
        msmt_ca
        .filter(F.col("ancestor_concept_id") == 40653158)
        .withColumn("std_concept_id", F.lit(40653158))
        .withColumn(
            "value_converted",
            F.when(
                F.lower(F.col("unit_source_value")).rlike(r'^mg/\s?dl$'),
                F.round(F.col("value_source_value").try_cast("double"), 3)
            ).when(
                F.lower(F.col("unit_source_value")) == "g/l",
                F.round(F.col("value_source_value").try_cast("double") / 100, 3)
            ).when(
                F.lower(F.col("unit_source_value")) == "mg/l",
                F.round(F.col("value_source_value").try_cast("double") / 10, 3)
            ).when(
                F.lower(F.col("unit_source_value")) == "mg/ml",
                F.round(F.col("value_source_value").try_cast("double") * 100, 3)
            ).otherwise(F.lit(None))
        )
        .withColumn("unit_converted", F.lit("mg/dL"))
    )
    all_frames.append(labs_igm)

    # inr
    labs_inr = (
        msmt_ca
        .join(concept, msmt_ca.descendant_concept_id == concept.concept_id, "inner")
        .filter(F.lower(F.col("concept_name")).rlike(r'.*\binr\b.*'))
        .dropDuplicates(["measurement_id"])
        .withColumn("std_concept_id", F.lit(85610)) # TODO: review semi-arbitrary choice of CPT4 code
        .withColumn("value_converted", F.round(F.col("value_source_value").try_cast("double"), 3))
        .withColumn("unit_converted", F.lit("ratio"))
    )
    all_frames.append(labs_inr)

    # iron
    labs_iron = (
        msmt_ca
        .filter(F.col("ancestor_concept_id") == 40654984)
        .withColumn("std_concept_id", F.lit(40654984))
        .withColumn(
            "value_converted",
            F.when(
                (F.lower(F.col("unit_source_value")) == "ug/dl") | (F.lower(F.col("unit_source_value")) == "mcg/dl"),
                F.round(F.col("value_source_value").try_cast("double"), 3)
            ).otherwise(F.lit(None))
        )
        .withColumn("unit_converted", F.lit("ug/dL"))
    )
    all_frames.append(labs_iron)

    # ketones
    labs_ketones = (
        msmt_ca
        .filter((F.col("ancestor_concept_id") == 40656264) | (F.col("measurement_source_value") == "5797-6"))
        .dropDuplicates(["measurement_id"])
        .withColumn("std_concept_id", F.lit(40656264))
        .withColumn(
            "value_converted",
            F.when(
                F.lower(F.col("unit_source_value")).rlike(r'^mg/\s?dl$'),
                F.round(F.col("value_source_value").try_cast("double"), 3)
            ).when(
                F.lower(F.col("unit_source_value")) == "g/l",
                F.round(F.col("value_source_value").try_cast("double") / 100, 3)
            ).when(
                F.lower(F.col("unit_source_value")) == "mg/l",
                F.round(F.col("value_source_value").try_cast("double") / 10, 3)
            ).when(
                F.lower(F.col("unit_source_value")) == "mg/ml",
                F.round(F.col("value_source_value").try_cast("double") * 100, 3)
            ).otherwise(F.lit(None))
        )
        .withColumn("unit_converted", F.lit("mg/dL"))
    )
    all_frames.append(labs_ketones)

    # leukocyte_esterase
    labs_leukocyte_esterase = (
        msmt_ca
        .filter(F.col("ancestor_concept_id") == 40657703)
        .withColumn("std_concept_id", F.lit(40657703))
        .withColumn(
            "value_converted",
            F.when(
                F.lower(F.col("unit_source_value")) == "leu/ul",
                F.round(F.col("value_source_value").try_cast("double"), 3)
            ).otherwise(F.lit(None))
        )
        .withColumn("unit_converted", F.lit("Leu/uL"))
    )
    all_frames.append(labs_leukocyte_esterase)

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
                F.round(F.col("value_source_value").try_cast("double"), 3)
            ).when(
                F.lower(F.col("unit_source_value")).rlike(r'^cell.*/[ucm].+$'),
                F.round(F.col("value_source_value").try_cast("double") / 1000, 3)
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
    all_frames.append(labs_wbc_urine)

    # lipoprotein_a
    labs_lipoprotein_a = (
        msmt_ca
        .filter(F.col("ancestor_concept_id") == 40655033)
        .withColumn("std_concept_id", F.lit(40655033))
        .withColumn(
            "value_converted",
            F.when(
                F.lower(F.col("unit_source_value")).rlike(r'^mg/\s?dl$'),
                F.round(F.col("value_source_value").try_cast("double"), 3)
            ).when(
                F.lower(F.col("unit_source_value")) == "g/l",
                F.round(F.col("value_source_value").try_cast("double") / 100, 3)
            ).when(
                F.lower(F.col("unit_source_value")) == "mg/l",
                F.round(F.col("value_source_value").try_cast("double") / 10, 3)
            ).when(
                F.lower(F.col("unit_source_value")) == "mg/ml",
                F.round(F.col("value_source_value").try_cast("double") * 100, 3)
            ).otherwise(F.lit(None))
        )
        .withColumn("unit_converted", F.lit("mg/dL"))
    )
    all_frames.append(labs_lipoprotein_a)

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
                F.round(F.col("value_source_value").try_cast("double"), 3)
            ).when(
                F.lower(F.col("unit_source_value")).rlike(r'^cell.*/[ucm].+$'),
                F.round(F.col("value_source_value").try_cast("double") / 1000, 3)
            ).otherwise(F.lit(None))
        )
        .withColumn("unit_converted", F.lit("Thousand/uL"))
    )
    all_frames.append(labs_lymphocytes)

    # wbc (dedupe against lymphocytes)
    labs_wbc = labs_wbc.join(
        labs_lymphocytes.select("person_id", "measurement_date", "value_source_value").distinct(),
        on=["person_id", "measurement_date", "value_source_value"],
        how="left_anti"
    )
    all_frames.append(labs_wbc)

    # magnesium
    labs_magnesium = (
        msmt_ca
        .filter(F.col("ancestor_concept_id") == 40653291)
        .withColumn("std_concept_id", F.lit(40653291))
        .withColumn(
            "value_converted",
            F.when(
                F.lower(F.col("unit_source_value")).rlike(r'^mg/\s?dl$'),
                F.round(F.col("value_source_value").try_cast("double"), 3)
            ).when(
                F.lower(F.col("unit_source_value")) == "g/l",
                F.round(F.col("value_source_value").try_cast("double") / 100, 3)
            ).when(
                F.lower(F.col("unit_source_value")) == "mg/l",
                F.round(F.col("value_source_value").try_cast("double") / 10, 3)
            ).when(
                F.lower(F.col("unit_source_value")) == "mg/ml",
                F.round(F.col("value_source_value").try_cast("double") * 100, 3)
            ).otherwise(F.lit(None))
        )
        .withColumn("unit_converted", F.lit("mg/dL"))
    )
    all_frames.append(labs_magnesium)

    # metamyelocytes
    labs_metamyelocytes = (
        msmt_ca
        .filter(
            (F.col("ancestor_concept_id") == 40654064)
            | (F.col("measurement_concept_id").isin(3012392, 3024507))
        )
        .dropDuplicates(["measurement_id"])
        .withColumn("std_concept_id", F.lit(40654064))
        .withColumn(
            "value_converted",
            F.when(
                F.lower(F.col("unit_source_value")) == "cells/ul",
                F.round(F.col("value_source_value").try_cast("double"), 3)
            ).when(
                F.lower(F.col("unit_source_value")).rlike(r'^10.3/ul$'),
                F.round(F.col("value_source_value").try_cast("double") * 1000, 3)
            ).otherwise(F.lit(None))
        )
        .withColumn("unit_converted", F.lit("Cells/uL"))
    )
    all_frames.append(labs_metamyelocytes)

    # metanephrine
    labs_metanephrine = (
        msmt_ca
        .filter(F.col("ancestor_concept_id") == 40655090)
        .withColumn("std_concept_id", F.lit(40655090))
        .withColumn(
            "value_converted",
            F.when(
                F.lower(F.col("unit_source_value")) == "nmol/l",
                F.round(F.col("value_source_value").try_cast("double"), 3)
            ).otherwise(F.lit(None))
        )
        .withColumn("unit_converted", F.lit("nmol/L"))
    )
    all_frames.append(labs_metanephrine)

    # microalbumin
    labs_microalbumin = (
        msmt_ca
        .filter(F.col("ancestor_concept_id") == 40656529)
        .withColumn("std_concept_id", F.lit(40656529))
        .withColumn(
            "value_converted",
            F.when(
                (F.lower(F.col("unit_source_value")) == "mcg/ml") | (F.lower(F.col("unit_source_value")) == "ug/ml"),
                F.round(F.col("value_source_value").try_cast("double"), 3)
            ).when(
                F.lower(F.col("unit_source_value")) == "mcg/dl",
                F.round(F.col("value_source_value").try_cast("double") / 100, 3)
            ).otherwise(F.lit(None))
        )
        .withColumn("unit_converted", F.lit("mcg/mL"))
    )
    all_frames.append(labs_microalbumin)

    # microalbumin_creatinine_ratio
    labs_microalbumin_creatinine_ratio = (
        msmt_ca
        .filter(F.col("ancestor_concept_id") == 40656531)
        .withColumn("std_concept_id", F.lit(40656531))
        .withColumn(
            "value_converted",
            F.when(
                F.lower(F.col("unit_source_value")).like("mcg/mg%"),
                F.round(F.col("value_source_value").try_cast("double"), 3)
            ).otherwise(F.lit(None))
        )
        .withColumn("unit_converted", F.lit("mcg/mg"))
    )
    all_frames.append(labs_microalbumin_creatinine_ratio)

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
                F.round(F.col("value_source_value").try_cast("double"), 3)
            ).when(
                F.lower(F.col("unit_source_value")).rlike(r'^cell.*/[ucm].+$'),
                F.round(F.col("value_source_value").try_cast("double") / 1000, 3)
            ).otherwise(F.lit(None))
        )
        .withColumn("unit_converted", F.lit("Thousand/uL"))
    )
    all_frames.append(labs_monocytes)

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
                F.round(F.col("value_source_value").try_cast("double"), 3)
            ).when(
                F.lower(F.col("unit_source_value")).rlike(r'^cell.*/[ucm].+$'),
                F.round(F.col("value_source_value").try_cast("double") / 1000, 3)
            ).otherwise(F.lit(None))
        )
        .withColumn("unit_converted", F.lit("Thousand/uL"))
    )
    all_frames.append(labs_neutrophils)

    # neutrophils_bandform
    labs_neutrophils_bandform = (
        msmt_ca
        .filter(
            (F.col("ancestor_concept_id") == 40654083)
            | (F.col("measurement_concept_id").isin(3008939, 3018199))
        )
        .dropDuplicates(["measurement_id"])
        .withColumn("std_concept_id", F.lit(40654083))
        .withColumn(
            "value_converted",
            F.when(
                F.lower(F.col("unit_source_value")).like("th%/ul")
                | F.lower(F.col("unit_source_value")).like("th%/mcl")
                | F.lower(F.col("unit_source_value")).like("%10%3/ul")
                | F.lower(F.col("unit_source_value")).like("%10%3/mcl")
                | F.lower(F.col("unit_source_value")).rlike(r'^k/[ucm].+$'),
                F.round(F.col("value_source_value").try_cast("double"), 3)
            ).when(
                F.lower(F.col("unit_source_value")).rlike(r'^cell.*/[ucm].+$'),
                F.round(F.col("value_source_value").try_cast("double") / 1000, 3)
            ).otherwise(F.lit(None))
        )
        .withColumn("unit_converted", F.lit("Thousand/uL"))
    )
    all_frames.append(labs_neutrophils_bandform)

    # neutrophils_segmented
    labs_neutrophils_segmented = (
        msmt_ca
        .filter(
            (F.col("ancestor_concept_id") == 40654086)
            | (F.col("measurement_concept_id").isin(3027270, 3015586))
        )
        .dropDuplicates(["measurement_id"])
        .withColumn("std_concept_id", F.lit(40654086))
        .withColumn(
            "value_converted",
            F.when(
                F.lower(F.col("unit_source_value")).like("th%/ul")
                | F.lower(F.col("unit_source_value")).like("th%/mcl")
                | F.lower(F.col("unit_source_value")).like("%10%3/ul")
                | F.lower(F.col("unit_source_value")).like("%10%3/mcl")
                | F.lower(F.col("unit_source_value")).rlike(r'^k/[ucm].+$'),
                F.round(F.col("value_source_value").try_cast("double"), 3)
            ).when(
                F.lower(F.col("unit_source_value")).rlike(r'^cell.*/[ucm].+$'),
                F.round(F.col("value_source_value").try_cast("double") / 1000, 3)
            ).otherwise(F.lit(None))
        )
        .withColumn("unit_converted", F.lit("Thousand/uL"))
    )
    all_frames.append(labs_neutrophils_segmented)

    # normetanephrine
    labs_normetanephrine = (
        msmt_ca
        .filter(F.col("ancestor_concept_id") == 40655204)
        .withColumn("std_concept_id", F.lit(40655204))
        .withColumn(
            "value_converted",
            F.when(
                F.lower(F.col("unit_source_value")) == "nmol/l",
                F.round(F.col("value_source_value").try_cast("double"), 3)
            ).otherwise(F.lit(None))
        )
        .withColumn("unit_converted", F.lit("nmol/L"))
    )
    all_frames.append(labs_normetanephrine)

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
                F.round(F.col("value_source_value").try_cast("double"), 3)
            ).otherwise(F.lit(None))
        )
        .withColumn("unit_converted", F.lit("Second(s)"))
    )
    all_frames.append(labs_ptt)

    # phosphate
    labs_phosphate = (
        msmt_ca
        .filter(F.col("ancestor_concept_id") == 40653550)
        .withColumn("std_concept_id", F.lit(40653550))
        .withColumn(
            "value_converted",
            F.when(
                F.lower(F.col("unit_source_value")).rlike(r'^mg/\s?dl$'),
                F.round(F.col("value_source_value").try_cast("double"), 3)
            ).when(
                F.lower(F.col("unit_source_value")) == "g/l",
                F.round(F.col("value_source_value").try_cast("double") / 100, 3)
            ).when(
                F.lower(F.col("unit_source_value")) == "mg/l",
                F.round(F.col("value_source_value").try_cast("double") / 10, 3)
            ).when(
                F.lower(F.col("unit_source_value")) == "mg/ml",
                F.round(F.col("value_source_value").try_cast("double") * 100, 3)
            ).otherwise(F.lit(None))
        )
        .withColumn("unit_converted", F.lit("mg/dL"))
    )
    all_frames.append(labs_phosphate)

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
                F.round(F.col("value_source_value").try_cast("double"), 3)
            ).when(
                F.lower(F.col("unit_source_value")).rlike(r'^cell.*/[ucm].+$'),
                F.round(F.col("value_source_value").try_cast("double") / 1000, 3)
            ).otherwise(F.lit(None))
        )
        .withColumn("unit_converted", F.lit("Thousand/uL"))
    )
    all_frames.append(labs_platelets)

    # potassium
    labs_potassium = (
        msmt_ca
        .filter(F.col("ancestor_concept_id") == 40653596)
        .withColumn("std_concept_id", F.lit(40653596))
        .withColumn(
            "value_converted",
            F.when(
                F.lower(F.col("unit_source_value")).like("mmo%/l"),
                F.round(F.col("value_source_value").try_cast("double"), 3)
            ).otherwise(F.lit(None))
        )
        .withColumn("unit_converted", F.lit("mmol/L"))
    )
    all_frames.append(labs_potassium)

    # prealbumin
    labs_prealbumin = (
        msmt_ca
        .filter(F.col("ancestor_concept_id") == 40653598)
        .withColumn("std_concept_id", F.lit(40653598))
        .withColumn(
            "value_converted",
            F.when(
                F.lower(F.col("unit_source_value")).rlike(r'^mg/\s?dl$'),
                F.round(F.col("value_source_value").try_cast("double"), 3)
            ).when(
                F.lower(F.col("unit_source_value")) == "g/l",
                F.round(F.col("value_source_value").try_cast("double") / 100, 3)
            ).when(
                F.lower(F.col("unit_source_value")) == "mg/l",
                F.round(F.col("value_source_value").try_cast("double") / 10, 3)
            ).when(
                F.lower(F.col("unit_source_value")) == "mg/ml",
                F.round(F.col("value_source_value").try_cast("double") * 100, 3)
            ).otherwise(F.lit(None))
        )
        .withColumn("unit_converted", F.lit("mg/dL"))
    )
    all_frames.append(labs_prealbumin)

    # promyelocytes
    labs_promyelocytes = (
        msmt_ca
        .filter(
            (F.col("ancestor_concept_id") == 40654115)
            | (F.col("measurement_concept_id").isin(3022709, 3024153))
        )
        .dropDuplicates(["measurement_id"])
        .withColumn("std_concept_id", F.lit(40654115))
        .withColumn(
            "value_converted",
            F.when(
                F.lower(F.col("unit_source_value")) == "cells/ul",
                F.round(F.col("value_source_value").try_cast("double"), 3)
            ).when(
                F.lower(F.col("unit_source_value")).rlike(r'^10.3/ul$'),
                F.round(F.col("value_source_value").try_cast("double") * 1000, 3)
            ).otherwise(F.lit(None))
        )
        .withColumn("unit_converted", F.lit("Cells/uL"))
    )
    all_frames.append(labs_promyelocytes)

    # protein
    labs_protein = (
        msmt_ca
        .filter(F.col("ancestor_concept_id") == 40653626)
        .withColumn("std_concept_id", F.lit(40653626))
        .withColumn(
            "value_converted",
            F.when(
                F.lower(F.col("unit_source_value")).rlike(r'^gm?/dl$'),
                F.round(F.col("value_source_value").try_cast("double"), 3)
            ).when(
                F.lower(F.col("unit_source_value")) == "mg/dl",
                F.round(F.col("value_source_value").try_cast("double") / 1000, 3)
            ).when(
                F.lower(F.col("unit_source_value")) == "mg/l",
                F.round(F.col("value_source_value").try_cast("double") / 10000, 3)
            ).otherwise(F.lit(None))
        )
        .withColumn("unit_converted", F.lit("g/dL"))
    )
    all_frames.append(labs_protein)

    # protein_urine
    labs_protein_urine = (
        msmt_ca
        .filter(
            (F.col("ancestor_concept_id") == 40657714)
            | (F.col("measurement_concept_id").isin(3005897, 3035511, 3014051, 3037121, 40760845))
        )
        .dropDuplicates(["measurement_id"])
        .withColumn("std_concept_id", F.lit(40657714))
        .withColumn(
            "value_converted",
            F.when(
                F.lower(F.col("unit_source_value")).rlike(r'^mg/\s?dl$'),
                F.round(F.col("value_source_value").try_cast("double"), 3)
            ).when(
                F.lower(F.col("unit_source_value")) == "g/l",
                F.round(F.col("value_source_value").try_cast("double") / 100, 3)
            ).when(
                F.lower(F.col("unit_source_value")) == "mg/l",
                F.round(F.col("value_source_value").try_cast("double") / 10, 3)
            ).when(
                F.lower(F.col("unit_source_value")) == "mg/ml",
                F.round(F.col("value_source_value").try_cast("double") * 100, 3)
            ).otherwise(F.lit(None))
        )
        .withColumn("unit_converted", F.lit("mg/dL"))
    )
    all_frames.append(labs_protein_urine)

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
                F.round(F.col("value_source_value").try_cast("double"), 3)
            ).otherwise(F.lit(None))
        )
        .withColumn("unit_converted", F.lit("Second(s)"))
    )
    all_frames.append(labs_pt)

    # rf
    labs_rf = (
        msmt_ca
        .filter(
            (F.col("ancestor_concept_id") == 40653663)
            | (F.col("measurement_concept_id").isin(3021614, 3015688, 3024763))
        )
        .dropDuplicates(["measurement_id"])
        .withColumn("std_concept_id", F.lit(40653663))
        .withColumn(
            "value_converted",
            F.when(
                F.lower(F.col("unit_source_value")) == "iu/ml",
                F.round(F.col("value_source_value").try_cast("double"), 3)
            ).otherwise(F.lit(None))
        )
        .withColumn("unit_converted", F.lit("International Units/mL"))
    )
    all_frames.append(labs_rf)

    # shbg
    labs_shbg = (
        msmt_ca
        .filter(F.col("ancestor_concept_id") == 40655429)
        .withColumn("std_concept_id", F.lit(40655429))
        .withColumn(
            "value_converted",
            F.when(
                F.lower(F.col("unit_source_value")) == "nmol/l",
                F.round(F.col("value_source_value").try_cast("double"), 3)
            ).otherwise(F.lit(None))
        )
        .withColumn("unit_converted", F.lit("nmol/L"))
    )
    all_frames.append(labs_shbg)

    # sodium
    labs_sodium = (
        msmt_ca
        .filter(F.col("ancestor_concept_id") == 40653762)
        .withColumn("std_concept_id", F.lit(40653762))
        .withColumn(
            "value_converted",
            F.when(
                F.lower(F.col("unit_source_value")).like("mmo%/l"),
                F.round(F.col("value_source_value").try_cast("double"), 3)
            ).otherwise(F.lit(None))
        )
        .withColumn("unit_converted", F.lit("mmol/L"))
    )
    all_frames.append(labs_sodium)

    # testosterone_free
    labs_testosterone_free = (
        msmt_ca
        .filter(F.col("ancestor_concept_id") == 40653808)
        .withColumn("std_concept_id", F.lit(40653808))
        .withColumn(
            "value_converted",
            F.when(
                F.lower(F.col("unit_source_value")) == "pg/ml",
                F.round(F.col("value_source_value").try_cast("double"), 3)
            ).when(
                F.lower(F.col("unit_source_value")) == "ng/dl",
                F.round(F.col("value_source_value").try_cast("double") * 10, 3)
            ).otherwise(F.lit(None))
        )
        .withColumn("unit_converted", F.lit("pg/mL"))
    )
    all_frames.append(labs_testosterone_free)

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
                F.round(F.col("value_source_value").try_cast("double"), 3)
            ).otherwise(F.lit(None))
        )
        .withColumn("unit_converted", F.lit("uIU/mL"))
    )
    all_frames.append(labs_tsh)

    # transferrin
    labs_transferrin = (
        msmt_ca
        .filter(F.col("ancestor_concept_id") == 1990650)
        .withColumn("std_concept_id", F.lit(1990650))
        .withColumn(
            "value_converted",
            F.when(
                F.lower(F.col("unit_source_value")).rlike(r'^mg/\s?dl$'),
                F.round(F.col("value_source_value").try_cast("double"), 3)
            ).when(
                F.lower(F.col("unit_source_value")) == "g/l",
                F.round(F.col("value_source_value").try_cast("double") / 100, 3)
            ).when(
                F.lower(F.col("unit_source_value")) == "mg/l",
                F.round(F.col("value_source_value").try_cast("double") / 10, 3)
            ).when(
                F.lower(F.col("unit_source_value")) == "mg/ml",
                F.round(F.col("value_source_value").try_cast("double") * 100, 3)
            ).otherwise(F.lit(None))
        )
        .withColumn("unit_converted", F.lit("mg/dL"))
    )
    all_frames.append(labs_transferrin)

    # triglycerides
    labs_triglycerides = (
        msmt_ca
        .filter(F.col("ancestor_concept_id") == 40653862)
        .withColumn("std_concept_id", F.lit(40653862))
        .withColumn(
            "value_converted",
            F.when(
                F.lower(F.col("unit_source_value")).rlike(r'^mg/\s?dl$'),
                F.round(F.col("value_source_value").try_cast("double"), 3)
            ).when(
                F.lower(F.col("unit_source_value")) == "g/l",
                F.round(F.col("value_source_value").try_cast("double") / 100, 3)
            ).when(
                F.lower(F.col("unit_source_value")) == "mg/l",
                F.round(F.col("value_source_value").try_cast("double") / 10, 3)
            ).when(
                F.lower(F.col("unit_source_value")) == "mg/ml",
                F.round(F.col("value_source_value").try_cast("double") * 100, 3)
            ).otherwise(F.lit(None))
        )
        .withColumn("unit_converted", F.lit("mg/dL"))
    )
    all_frames.append(labs_triglycerides)

    # troponin_i
    labs_troponin_i = (
        msmt_ca
        .filter(F.col("ancestor_concept_id") == 40653873)
        .withColumn("std_concept_id", F.lit(40653873))
        .withColumn(
            "value_converted",
            F.when(
                F.lower(F.col("unit_source_value")) == "ng/ml",
                F.round(F.col("value_source_value").try_cast("double"), 3)
            ).when(
                F.lower(F.col("unit_source_value")) == "ng/dl",
                F.round(F.col("value_source_value").try_cast("double") / 100, 3)
            ).when(
                F.lower(F.col("unit_source_value")) == "ng/l",
                F.round(F.col("value_source_value").try_cast("double") / 1000, 3)
            ).otherwise(F.lit(None))
        )
        .withColumn("unit_converted", F.lit("ng/mL"))
    )
    all_frames.append(labs_troponin_i)

    # troponin_t
    labs_troponin_t = (
        msmt_ca
        .filter(F.col("ancestor_concept_id") == 40653874)
        .withColumn("std_concept_id", F.lit(40653874))
        .withColumn(
            "value_converted",
            F.when(
                F.lower(F.col("unit_source_value")) == "ng/ml",
                F.round(F.col("value_source_value").try_cast("double"), 3)
            ).when(
                F.lower(F.col("unit_source_value")) == "ng/dl",
                F.round(F.col("value_source_value").try_cast("double") / 100, 3)
            ).when(
                F.lower(F.col("unit_source_value")) == "ng/l",
                F.round(F.col("value_source_value").try_cast("double") / 1000, 3)
            ).otherwise(F.lit(None))
        )
        .withColumn("unit_converted", F.lit("ng/mL"))
    )
    all_frames.append(labs_troponin_t)

    # urobilinogen
    labs_urobilinogen = (
        msmt_ca
        .filter(F.col("ancestor_concept_id") == 40656506)
        .withColumn("std_concept_id", F.lit(40656506))
        .withColumn(
            "value_converted",
            F.when(
                F.lower(F.col("unit_source_value")).rlike(r'^mg/\s?dl$'),
                F.round(F.col("value_source_value").try_cast("double"), 3)
            ).when(
                F.lower(F.col("unit_source_value")) == "g/l",
                F.round(F.col("value_source_value").try_cast("double") / 100, 3)
            ).when(
                F.lower(F.col("unit_source_value")) == "mg/l",
                F.round(F.col("value_source_value").try_cast("double") / 10, 3)
            ).when(
                F.lower(F.col("unit_source_value")) == "mg/ml",
                F.round(F.col("value_source_value").try_cast("double") * 100, 3)
            ).otherwise(F.lit(None))
        )
        .withColumn("unit_converted", F.lit("mg/dL"))
    )
    all_frames.append(labs_urobilinogen)

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
                F.round(F.col("value_source_value").try_cast("double"), 3)
            ).otherwise(F.lit(None))
        )
        .withColumn("unit_converted", F.lit("mmHg"))
    )
    all_frames.append(vitals_bp_diastolic)

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
                F.round(F.col("value_source_value").try_cast("double"), 3)
            ).otherwise(F.lit(None))
        )
        .withColumn("unit_converted", F.lit("mmHg"))
    )
    all_frames.append(vitals_bp_systolic)

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
                F.round(F.col("value_source_value").try_cast("double"), 3)
            ).otherwise(F.lit(None))
        )
        .withColumn("unit_converted", F.lit("kg/m^2"))
    )
    all_frames.append(vitals_bmi)

    # vitals_height
    vitals_height = (
        msmt_ca
        .filter(F.col("ancestor_concept_id") == 40655804)
        .withColumn("std_concept_id", F.lit(40655804))
        .withColumn(
            "value_converted",
            F.when(
                F.lower(F.col("unit_source_value")).like("in%"),
                F.round(F.col("value_source_value").try_cast("double"), 3)
            ).when(
                F.lower(F.col("unit_source_value")) == "ft",
                F.round(F.col("value_source_value").try_cast("double") * 12, 3)
            ).otherwise(F.lit(None))
        )
        .withColumn("unit_converted", F.lit("Inches"))
    )
    all_frames.append(vitals_height)

    # vitals_o2sat
    vitals_o2sat = (
        msmt_ca
        .filter(F.col("ancestor_concept_id") == 40654168)
        .withColumn("std_concept_id", F.lit(40654168))
        .withColumn(
            "value_converted",
            F.when(
                F.col("unit_source_value").rlike(r'^%.*$'),
                F.round(F.col("value_source_value").try_cast("double"), 3)
            ).otherwise(F.lit(None))
        )
        .withColumn("unit_converted", F.lit("%"))
    )
    all_frames.append(vitals_o2sat)

    # vitals_pulse
    vitals_pulse = (
        msmt_ca
        .filter(F.col("ancestor_concept_id") == 40654164)
        .withColumn("std_concept_id", F.lit(40654164))
        .withColumn(
            "value_converted",
            F.when(
                F.lower(F.col("unit_source_value")) == "counts/min",
                F.round(F.col("value_source_value").try_cast("double"), 3)
            ).otherwise(F.lit(None))
        )
        .withColumn("unit_converted", F.lit("Counts/Min"))
    )
    all_frames.append(vitals_pulse)

    # vitals_resp_rate
    vitals_resp_rate = (
        msmt_ca
        .filter(F.col("ancestor_concept_id") == 40654163)
        .withColumn("std_concept_id", F.lit(40654163))
        .withColumn(
            "value_converted",
            F.when(
                F.lower(F.col("unit_source_value")) == "counts/min",
                F.round(F.col("value_source_value").try_cast("double"), 3)
            ).otherwise(F.lit(None))
        )
        .withColumn("unit_converted", F.lit("Counts/Min"))
    )
    all_frames.append(vitals_resp_rate)

    # vitals_temperature
    vitals_temperature = (
        msmt_ca
        .filter(F.col("ancestor_concept_id") == 40654162)
        .withColumn("std_concept_id", F.lit(40654162))
        .withColumn(
            "value_converted",
            F.when(
                F.lower(F.col("unit_source_value")) == "f",
                F.round(F.col("value_source_value").try_cast("double"), 3)
            ).when(
                F.lower(F.col("unit_source_value")).like("%c%"),
                F.round(F.col("value_source_value").try_cast("double") * (9 / 5) + 32, 3)
            ).otherwise(F.lit(None))
        )
        .withColumn("unit_converted", F.lit("Degrees Fahrenheit"))
    )
    all_frames.append(vitals_temperature)

    # vitals_weight
    vitals_weight = (
        msmt_ca
        .filter(F.col("ancestor_concept_id") == 40655805)
        .withColumn("std_concept_id", F.lit(40655805))
        .withColumn(
            "value_converted",
            F.when(
                F.lower(F.col("unit_source_value")).like("o%"),
                F.round(F.col("value_source_value").try_cast("double"), 3)
            ).when(
                F.lower(F.col("unit_source_value")).like("lb%"),
                F.round(F.col("value_source_value").try_cast("double") * 16, 3)
            ).otherwise(F.lit(None))
        )
        .withColumn("unit_converted", F.lit("oz"))
    )
    all_frames.append(vitals_weight)

    # union all labs/vitals frames, pared to the input measurement schema + std_concept_id/value_converted/unit_converted
    OUTPUT_COLUMNS = [
        "measurement_id", "person_id", "measurement_concept_id", "measurement_date", "measurement_datetime",
        "measurement_type_concept_id", "operator_concept_id", "value_as_number", "value_as_concept_id",
        "unit_concept_id", "range_low", "range_high", "visit_occurrence_id", "measurement_source_value",
        "measurement_source_concept_id", "unit_source_value", "value_source_value",
        "std_concept_id", "value_converted", "unit_converted",
    ]
    all_frames = [df.select(*[c for c in OUTPUT_COLUMNS if c in df.columns]) for df in all_frames]
    labs_vitals_union = all_frames[0]
    for frame in all_frames[1:]:
        labs_vitals_union = labs_vitals_union.unionByName(frame, allowMissingColumns=True)

    # perform fallback logic for all unmapped measurements (for now, null all values)
    unmapped_msmt = msmt.join(
        labs_vitals_union.select("measurement_id").distinct(),
        on="measurement_id",
        how="left_anti"
    )

    # perform fallback logic for all other measurement types
    fallback_msmt = autostd(unmapped_msmt)

    # union all data frames and return
    final_msmt = labs_vitals_union.unionByName(fallback_msmt, allowMissingColumns=True)

    return final_msmt

def autostd(msmt):

    # map units to canonical value
    unit_lower = F.lower(F.col("unit_source_value"))
    normalized_unit = (
        # mass-concentration ladder: mg/dL <-> g/dL <-> g/L <-> mg/L <-> mg/mL
        F.when(unit_lower.rlike(r'^mg/\s?dl$'), F.lit("mg/dL"))
        .when(unit_lower.rlike(r'^gm?/dl$'), F.lit("g/dL"))
        .when(unit_lower == "g/l", F.lit("g/L"))
        .when(unit_lower == "mg/l", F.lit("mg/L"))
        .when(unit_lower == "mg/ml", F.lit("mg/mL"))
        # hormone/protein ladder: pg/mL <-> ng/L <-> ng/dL <-> ng/mL
        .when(unit_lower == "pg/ml", F.lit("pg/mL"))
        .when(unit_lower == "ng/l", F.lit("ng/L"))
        .when(unit_lower == "ng/dl", F.lit("ng/dL"))
        .when(unit_lower == "ng/ml", F.lit("ng/mL"))
        # microgram ladder: ug/mL <-> ug/dL ("mcg" is a synonym for "ug")
        .when((unit_lower == "ug/ml") | (unit_lower == "mcg/ml"), F.lit("ug/mL"))
        .when((unit_lower == "ug/dl") | (unit_lower == "mcg/dl"), F.lit("ug/dL"))
        .when(unit_lower.like("mcg/mg%"), F.lit("mcg/mg"))
        # cell-count ladder: Cells/uL <-> Thousand/uL <-> Million/uL
        .when(
            unit_lower.like("th%/ul") | unit_lower.like("th%/mcl")
            | unit_lower.like("%10%3/ul") | unit_lower.like("%10%3/mcl")
            | unit_lower.rlike(r'^k/[ucm].+$') | unit_lower.rlike(r'^10.3/ul$'),
            F.lit("Thousand/uL")
        )
        .when(unit_lower.rlike(r'^cell.*/[ucm].+$'), F.lit("Cells/uL"))
        .when(
            unit_lower.like("m%/ul") | unit_lower.like("m%/mcl") | unit_lower.like("m%/mm3")
            | unit_lower.rlike(r'^m.*/cu?mm$') | unit_lower.like("%10%6/ul"),
            F.lit("Million/uL")
        )
        # enzyme/antibody activity units
        .when(unit_lower == "iu/ml", F.lit("International Units/mL"))
        .when(
            unit_lower.rlike(r'^u.?iu.?/ml$') | unit_lower.rlike(r'^mc?i?u.*/l$') | (unit_lower == "mciu/ml"),
            F.lit("uIU/mL")
        )
        .when(unit_lower.rlike(r'^.*i?u.*/l$'), F.lit("Unit/L"))
        .when(unit_lower == "leu/ul", F.lit("Leu/uL"))
        # electrolytes / catecholamines
        .when(unit_lower.like("mmo%/l"), F.lit("mmol/L"))
        .when(unit_lower == "nmol/l", F.lit("nmol/L"))
        # misc single-unit families
        .when(unit_lower == "mmhg", F.lit("mmHg"))
        .when(unit_lower.like("s%"), F.lit("Second(s)"))
        .when(F.col("unit_source_value").rlike(r'^%.*$'), F.lit("%"))
        .when(unit_lower == "counts/min", F.lit("Counts/Min"))
        .when(unit_lower.like("mm/h%"), F.lit("mm/h"))
        .when(unit_lower == "kg/m^2", F.lit("kg/m^2"))
        # anthropometrics (height/weight; temperature intentionally excluded, see note below)
        .when(unit_lower.like("in%"), F.lit("Inches"))
        .when(unit_lower == "ft", F.lit("Feet"))
        .when(unit_lower.like("o%"), F.lit("oz"))
        .when(unit_lower.like("lb%"), F.lit("lb"))
        .otherwise(F.col("unit_source_value"))
    )

    # normalize unit
    df = msmt.withColumn("unit_norm", normalized_unit)

    # perform value conversion to measurement concept mode unit where possible, drop if not possible
    unit_to_canonical = {
        # mass-concentration ladder, relative to mg/dL
        "mg/dL": 1.0,
        "g/dL": 1000.0,
        "g/L": 0.01,
        "mg/L": 0.1,
        "mg/mL": 100.0,
        # hormone/protein ladder, relative to pg/mL
        "pg/mL": 1.0,
        "ng/L": 1.0,
        "ng/dL": 10.0,
        "ng/mL": 1000.0,
        # microgram ladder, relative to ug/mL
        "ug/mL": 1.0,
        "ug/dL": 0.01,
        # cell-count ladder, relative to Cells/uL
        "Cells/uL": 1.0,
        "Thousand/uL": 1000.0,
        "Million/uL": 1000000.0,
        # anthropometrics
        "Inches": 1.0,
        "Feet": 12.0,
        "oz": 1.0,
        "lb": 16.0,
        # single-unit families: identity, kept so same-unit pairs still resolve via the lookup
        "Unit/L": 1.0,
        "International Units/mL": 1.0,
        "uIU/mL": 1.0,
        "Leu/uL": 1.0,
        "mmol/L": 1.0,
        "nmol/L": 1.0,
        "mmHg": 1.0,
        "Second(s)": 1.0,
        "%": 1.0,
        "Counts/Min": 1.0,
        "mm/h": 1.0,
        "kg/m^2": 1.0,
        "mcg/mg": 1.0,
    }

    # create map of normalized unit to conversion factor
    factor_map = F.create_map(*[F.lit(x) for kv in unit_to_canonical.items() for x in kv])

    # compute the target unit for each row of measurements
    pair_w = Window.partitionBy("measurement_concept_id", "unit_norm")
    df = df.withColumn("pair_count", F.count("*").over(pair_w))
    # tiebreak on unit_norm itself so the mode pick is deterministic when two units are equally common
    rank_w = Window.partitionBy("measurement_concept_id").orderBy(F.desc("pair_count"), F.col("unit_norm"))
    df = df.withColumn("unit_rank", F.row_number().over(rank_w))
    mode_w = Window.partitionBy("measurement_concept_id")
    df = df.withColumn(
        "target_unit",
        F.min(F.when(F.col("unit_rank") == 1, F.col("unit_norm"))).over(mode_w)
    )

    # perform unit and value conversion for non-mode units
    df = df.withColumn("factor_from", factor_map[F.col("unit_norm")])
    df = df.withColumn("factor_to", factor_map[F.col("target_unit")])
    already_target = F.col("unit_norm") == F.col("target_unit")
    convertible = F.col("factor_from").isNotNull() & F.col("factor_to").isNotNull()
    success = already_target | convertible
    value_num = F.col("value_source_value").try_cast("double")
    df = df.withColumn(
        "value_std",
        F.when(already_target, F.round(value_num, 3))
         .when(convertible, F.round(value_num * F.col("factor_from") / F.col("factor_to"), 3))
    )
    df = df.withColumn(
        "unit_std",
        F.when(success, F.col("target_unit"))
    )

    # reformat columns
    df = df.drop("pair_count", "unit_rank", "factor_from", "factor_to")
    df = df.withColumnRenamed("value_std", "value_converted")
    df = df.withColumnRenamed("unit_std", "unit_converted")
    df = df.withColumn("std_concept_id", F.col("measurement_concept_id"))

    return df.drop("pair_count", "unit_rank", "factor_from", "factor_to")

if __name__ == "__main__":

    # perform imports
    from pyspark.sql import SparkSession
    from pathlib import Path
    from dotenv import load_dotenv
    import os

    # create spark session
    spark = (
        SparkSession
        .builder
        .master("local[2]")
        .appName("meds-standardizers")
        .config("spark.sql.shuffle.partitions", "2")
        .getOrCreate()
    )

    # register data paths
    load_dotenv()
    REPO_ROOT = Path(__file__).parents().resolve[3]
    OMOP_DATA_DIR = REPO_ROOT / os.environ["OMOP_DATA_DIR"] 
    msmt_path = OMOP_DATA_DIR / "measurement.csv"
    concept_path = OMOP_DATA_DIR / "concept.csv"
    concept_ancestor_path = OMOP_DATA_DIR / "concept_ancestor.csv"

    # read measurements and metadata
    msmt = spark.read.csv(str(msmt_path), header=True, inferSchema=True)
    concept = spark.read.csv(str(concept_path), header=True, inferSchema=True)
    concept_ancestor = spark.read.csv(str(concept_ancestor_path), header=True, inferSchema=true)

    # standardize measurements
    std_msmt = standardize(msmt, concept, concept_ancestor)