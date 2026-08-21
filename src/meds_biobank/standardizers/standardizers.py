import pyspark.sql.functions as F
from pyspark.sql import Window
from pyspark.sql import functions as F, Window
from meds_biobank import schemas
from pyspark.sql import DataFrame
from pyspark.testing import assertSchemaEqual

# (label, predicate_fn, std_concept_id)
GROUPINGS = [
    ("labs_alt", lambda: F.col("ancestor_concept_id") == 40652525, 40652525),
    ("labs_albumin", lambda: F.col("ancestor_concept_id") == 40652534, 40652534),
    ("labs_alp", lambda: F.col("ancestor_concept_id") == 40652549, 40652549),
    ("labs_anion_gap", lambda: F.col("ancestor_concept_id") == 40652611, 40652611),
    ("labs_apo_b", lambda: F.col("ancestor_concept_id") == 40652616, 40652616),
    ("labs_amh", lambda: F.col("ancestor_concept_id") == 40653357, 40653357),
    ("labs_ast", lambda: F.col("ancestor_concept_id") == 40652640, 40652640),
    ("labs_basophils", lambda: F.col("ancestor_concept_id") == 40653984, 40653984),
    ("labs_beta_globulin", lambda: F.col("ancestor_concept_id") == 1990626, 1990626),
    ("labs_bilirubin_total", lambda: F.col("ancestor_concept_id") == 40652709, 40652709),
    ("labs_bun", lambda: F.col("ancestor_concept_id") == 40653900, 40653900),
    ("labs_crp", lambda: (F.col("ancestor_concept_id") == 40652733) | (F.col("measurement_concept_id") == 3020460), 40652733),
    ("labs_crp_hs", lambda: F.col("ancestor_concept_id") == 40654479, 40654479),
    ("labs_calcium", lambda: F.col("ancestor_concept_id") == 40652745, 40652745),
    ("labs_chloride", lambda: F.col("ancestor_concept_id") == 40652796, 40652796),
    ("labs_chol_hdl", lambda: F.col("ancestor_concept_id") == 40652802, 40652802),
    ("labs_chol_ldl", lambda: F.col("ancestor_concept_id") == 40654572, 40654572),
    ("labs_chol_vldl", lambda: (F.col("ancestor_concept_id") == 40654576) | (F.col("measurement_concept_id") == 3009596), 40654576),
    ("labs_chol_total", lambda: F.col("ancestor_concept_id") == 40652808, 40652808),
    ("labs_c4", lambda: F.col("ancestor_concept_id") == 40654637, 40654637),
    ("labs_covid", lambda: F.col("ancestor_concept_id") == 36662633, 36662633),
    ("labs_ck", lambda: F.col("ancestor_concept_id") == 40652867, 40652867),
    ("labs_creatinine", lambda: F.col("ancestor_concept_id") == 40652870, 40652870),
    ("labs_creatinine_urine", lambda: F.col("ancestor_concept_id") == 40656057, 40656057),
    ("labs_ccpab", lambda: F.col("ancestor_concept_id") == 40652885, 40652885),
    ("labs_eosinophils", lambda: F.col("ancestor_concept_id") == 40653994, 40653994),
    ("labs_rbc", lambda: F.col("ancestor_concept_id") == 40654005, 40654005),
    ("labs_rbc_urine", lambda: F.col("ancestor_concept_id") == 40657685, 40657685),
    ("labs_esr", lambda: (F.col("ancestor_concept_id") == 4028908)
        & F.lower(F.col("concept_name")).like("%eryth%")
        & F.lower(F.col("concept_name")).like("%sed%"), 3015183),
    ("labs_ggt", lambda: F.lower(F.col("concept_name")).like("%glutamyl%transferase%"), 2212371),
    ("labs_ferritin", lambda: F.col("ancestor_concept_id") == 40652982, 40652982),
    ("labs_folate", lambda: F.col("ancestor_concept_id") == 40652995, 40652995),
    ("labs_glucose", lambda: (F.col("ancestor_concept_id") == 40653085)
        & (~F.col("value_source_value").like("%,%")), 40653085),
    ("labs_glucose_fasting", lambda: F.col("measurement_source_concept_id") == 3037110, 3037110),
    ("labs_glucose_urine", lambda: F.col("ancestor_concept_id") == 40657691, 40657691),
    ("labs_granulocytes", lambda: F.col("ancestor_concept_id") == 40654016, 40654016),
    ("labs_hemoglobin", lambda: F.col("ancestor_concept_id") == 40654905, 40654905),
    ("labs_hba1c", lambda: F.col("ancestor_concept_id") == 1621295, 1621295),
    ("labs_iga", lambda: F.col("ancestor_concept_id") == 40653141, 40653141),
    ("labs_igg", lambda: F.col("ancestor_concept_id") == 40653151, 40653151),
    ("labs_igm", lambda: F.col("ancestor_concept_id") == 40653158, 40653158),
    ("labs_inr", lambda: F.lower(F.col("concept_name")).rlike(r'.*\binr\b.*'), 85610),
    ("labs_iron", lambda: F.col("ancestor_concept_id") == 40654984, 40654984),
    ("labs_ketones", lambda: (F.col("ancestor_concept_id") == 40656264) | (F.col("measurement_source_value") == "5797-6"), 40656264),
    ("labs_leukocyte_esterase", lambda: F.col("ancestor_concept_id") == 40657703, 40657703),
    ("labs_wbc", lambda: F.col("ancestor_concept_id") == 40654026, 40654026),
    ("labs_wbc_urine", lambda: F.col("ancestor_concept_id") == 40657704, 40657704),
    ("labs_lipoprotein_a", lambda: F.col("ancestor_concept_id") == 40655033, 40655033),
    ("labs_lymphocytes", lambda: F.col("ancestor_concept_id") == 40654045, 40654045),
    ("labs_magnesium", lambda: F.col("ancestor_concept_id") == 40653291, 40653291),
    ("labs_metamyelocytes", lambda: (F.col("ancestor_concept_id") == 40654064)
        | (F.col("measurement_concept_id").isin(3012392, 3024507)), 40654064),
    ("labs_metanephrine", lambda: F.col("ancestor_concept_id") == 40655090, 40655090),
    ("labs_microalbumin", lambda: F.col("ancestor_concept_id") == 40656529, 40656529),
    ("labs_microalbumin_creatinine_ratio", lambda: F.col("ancestor_concept_id") == 40656531, 40656531),
    ("labs_monocytes", lambda: F.col("ancestor_concept_id") == 40654069, 40654069),
    ("labs_neutrophils", lambda: F.col("ancestor_concept_id") == 40654088, 40654088),
    ("labs_neutrophils_bandform", lambda: (F.col("ancestor_concept_id") == 40654083)
        | (F.col("measurement_concept_id").isin(3008939, 3018199)), 40654083),
    ("labs_neutrophils_segmented", lambda: (F.col("ancestor_concept_id") == 40654086)
        | (F.col("measurement_concept_id").isin(3027270, 3015586)), 40654086),
    ("labs_normetanephrine", lambda: F.col("ancestor_concept_id") == 40655204, 40655204),
    ("labs_ptt", lambda: F.lower(F.col("concept_name")).like("%ptt%")
        & F.col("ancestor_concept_id").isin(1618784, 2212742), 2212742),
    ("labs_phosphate", lambda: F.col("ancestor_concept_id") == 40653550, 40653550),
    ("labs_platelets", lambda: F.col("ancestor_concept_id") == 40654106, 40654106),
    ("labs_potassium", lambda: F.col("ancestor_concept_id") == 40653596, 40653596),
    ("labs_prealbumin", lambda: F.col("ancestor_concept_id") == 40653598, 40653598),
    ("labs_promyelocytes", lambda: (F.col("ancestor_concept_id") == 40654115)
        | (F.col("measurement_concept_id").isin(3022709, 3024153)), 40654115),
    ("labs_protein", lambda: F.col("ancestor_concept_id") == 40653626, 40653626),
    ("labs_protein_urine", lambda: (F.col("ancestor_concept_id") == 40657714)
        | (F.col("measurement_concept_id").isin(3005897, 3035511, 3014051, 3037121, 40760845)), 40657714),
    ("labs_pt", lambda: F.col("measurement_concept_id").isin(3034426, 2212731), 3034426),
    ("labs_rf", lambda: (F.col("ancestor_concept_id") == 40653663)
        | (F.col("measurement_concept_id").isin(3021614, 3015688, 3024763)), 40653663),
    ("labs_shbg", lambda: F.col("ancestor_concept_id") == 40655429, 40655429),
    ("labs_sodium", lambda: F.col("ancestor_concept_id") == 40653762, 40653762),
    ("labs_testosterone_free", lambda: F.col("ancestor_concept_id") == 40653808, 40653808),
    ("labs_tsh", lambda: F.col("ancestor_concept_id") == 40653836, 40653836),
    ("labs_transferrin", lambda: F.col("ancestor_concept_id") == 1990650, 1990650),
    ("labs_triglycerides", lambda: F.col("ancestor_concept_id") == 40653862, 40653862),
    ("labs_troponin_i", lambda: F.col("ancestor_concept_id") == 40653873, 40653873),
    ("labs_troponin_t", lambda: F.col("ancestor_concept_id") == 40653874, 40653874),
    ("labs_urobilinogen", lambda: F.col("ancestor_concept_id") == 40656506, 40656506),
    ("vitals_bp_diastolic", lambda: F.col("measurement_concept_id") == 3012888, 3012888),
    ("vitals_bp_systolic", lambda: F.col("measurement_concept_id") == 3004249, 3004249),
    ("vitals_bmi", lambda: F.col("measurement_concept_id").isin(44783982, 4245997), 44783982),
    ("vitals_height", lambda: F.col("ancestor_concept_id") == 40655804, 40655804),
    ("vitals_o2sat", lambda: F.col("ancestor_concept_id") == 40654168, 40654168),
    ("vitals_pulse", lambda: F.col("ancestor_concept_id") == 40654164, 40654164),
    ("vitals_resp_rate", lambda: F.col("ancestor_concept_id") == 40654163, 40654163),
    ("vitals_temperature", lambda: F.col("ancestor_concept_id") == 40654162, 40654162),
    ("vitals_weight", lambda: F.col("ancestor_concept_id") == 40655805, 40655805),
]


def standardize_measurement_concept_id(measurement, concept, concept_ancestor):
    """
    Apply GROUPINGS (the per-domain ancestor/concept matching that standardize() encodes)
    to relabel measurement_concept_id to each grouping's standardized concept id.

    For every grouping, select the matching rows, copy them, and overwrite
    measurement_concept_id with the grouping's std_concept_id. Union all grouping copies
    together, then union back in the measurements that matched no grouping, unchanged.

    Args:
        measurement (pyspark.sql.DataFrame): OMOP measurement table
        concept (pyspark.sql.DataFrame): OMOP concept table
        concept_ancestor (pyspark.sql.DataFrame): OMOP concept_ancestor table

    Returns:
        pyspark.sql.DataFrame: measurement, with measurement_concept_id relabeled for
            every row that matched a grouping. Same columns as measurement. A row that
            matches more than one grouping appears once per match, same as standardize()'s
            per-domain unions do today.
    """

    # type guards
    if not isinstance(measurement, DataFrame):
        raise ValueError()
    if not isinstance(concept, DataFrame):
        raise ValueError()
    if not isinstance(concept_ancestor, DataFrame):
        raise ValueError()
    
    # schema guards
    try:
        assertSchemaEqual(measurement.schema, schemas.OMOP_MEASUREMENT_SCHEMA, ignoreColumnOrder=True)
        assertSchemaEqual(concept.schema, schemas.OMOP_CONCEPT_SCHEMA, ignoreColumnOrder=True)
        assertSchemaEqual(concept_ancestor.schema, schemas.OMOP_CONCEPT_ANCESTOR_SCHEMA, ignoreColumnOrder=True)
    except:
        raise ValueError()

    # build the same joined base the groupings' predicates are written against
    base = (
        measurement
        .join(concept_ancestor, measurement.measurement_concept_id == concept_ancestor.descendant_concept_id, "left")
        .join(concept, F.col("descendant_concept_id") == concept.concept_id, "left")
    )

    # get msmt cols
    measurement_cols = measurement.columns

    # dropDuplicates(["measurement_id"]) guards against the ancestor join's fan-out: a
    grouped_frames = [
        base
        .filter(predicate())
        .dropDuplicates(["measurement_id"])
        .withColumn("measurement_concept_id", F.lit(std_concept_id))
        .select(*measurement_cols)
        for _, predicate, std_concept_id in GROUPINGS
    ]

    # union groups
    covered = grouped_frames[0]
    for frame in grouped_frames[1:]:
        covered = covered.unionByName(frame)

    # measurements that matched no grouping pass through unchanged
    uncovered = measurement.join(
        covered.select("measurement_id").distinct(),
        on="measurement_id",
        how="left_anti"
    )

    return covered.unionByName(uncovered)

def standardize_numeric_values_and_units(measurement):
    """
    Apply unit/value homogenization (or nullification) to measurements by measurement_concept_id.

    Args:
        measurement (pyspark.sql.DataFrame): OMOP measurement table
        concept (pyspark.sql.DataFrame): OMOP concept table
        concept_ancestor (pyspark.sql.DataFrame): OMOP concept_ancestor table

    Returns:
        pyspark.sql.DataFrame: measurement, with value_as_number/unit_source_value
            homogenized to each measurement_concept_id's modal unit where possible
    """

    # type guard
    if not isinstance(measurement, DataFrame):
        raise ValueError

    # schema guard
    try:
        assertSchemaEqual(measurement.schema, schemas.OMOP_MEASUREMENT_SCHEMA)
    except:
        raise ValueError()

    # map units to canonical value
    unit_lower = F.lower(F.col("unit_source_value"))
    normalized_unit = (
        # mass-concentration ladder: mg/dL <-> g/dL <-> g/L <-> mg/L <-> mg/mL
        F.when(unit_lower.rlike(r'^mg/\s?dl$'), F.lit("mg/dL"))
        .when(unit_lower.rlike(r'^gm?/dl(\s*\(calc\))?$'), F.lit("g/dL"))
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
        .when(unit_lower.rlike(r'^cell.*/[ucm].+$') | (unit_lower == "/ul"), F.lit("Cells/uL"))
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
        # renal function: eGFR, normalize spacing/suffix variants to one label
        .when(unit_lower.rlike(r'^ml/min/1\.73\s?m2$') | (unit_lower == "ml/min/1.73"), F.lit("mL/min/1.73m2"))
        # standalone hematology indices (no ratio partner in this data; identity so repeats don't flag as inconsistent units)
        .when(unit_lower == "pg", F.lit("pg"))
        .when(unit_lower == "fl", F.lit("fL"))
        .when(unit_lower == "/hpf", F.lit("/hpf"))
        .otherwise(F.col("unit_source_value"))
    )

    # normalize unit
    df = measurement.withColumn("unit_norm", normalized_unit)

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
        "mL/min/1.73m2": 1.0,
        "pg": 1.0,
        "fL": 1.0,
        "/hpf": 1.0,
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

    # write the standardized value/unit back into the real OMOP fields
    df = df.withColumn("value_as_number", F.col("value_std"))
    df = df.withColumn("unit_source_value", F.col("unit_std"))

    return df.drop("pair_count", "unit_rank", "factor_from", "factor_to", "unit_norm", "target_unit", "value_std", "unit_std")

def standardize_text_value(measurement):
    """
    Apply text value simplification to measurements without parseable numeric values:
    strip surrounding whitespace, collapse repeated internal whitespace, and lowercase
    value_source_value so that e.g. "POSITIVE", "  positive", and "Positive  " all
    normalize to the same "positive".

    Args:
        measurement (pyspark.sql.DataFrame): OMOP measurement table

    Returns:
        pyspark.sql.DataFrame: measurement, with value_source_value cleaned for every row
            that doesn't already have a parseable numeric value. Rows with a parseable
            numeric value (or a null value_source_value) pass through unchanged.
    """
    # text-valued rows
    has_text_value = measurement.filter(
        (F.col("value_source_value").isNotNull()) &
        (F.expr("try_cast(value_source_value AS DOUBLE)").isNull())
    )

    # collapse internal whitespace runs to a single space, then trim the leading/trailing space that leaves behind, then lowercase
    cleaned_value = F.lower(F.trim(F.regexp_replace(F.col("value_source_value"), r"\s+", " ")))
    text_cleaned = has_text_value.withColumn("value_source_value", cleaned_value)

    # numeric-valued and null-valued rows pass through untouched
    not_text_value = measurement.join(
        has_text_value.select("measurement_id").distinct(),
        on="measurement_id",
        how="left_anti"
    )

    return text_cleaned.unionByName(not_text_value)

def lv_standardize(msmt, concept, concept_ancestor):
    """
    - For each mtype, filter df and homogenize unit and value

    Args:
        msmt (pyspark.sql.DataFrame):
        concept_ancestor (pyspark.sql.DataFrame):
        concept (pyspark.sql.DataFrame):
        person (pyspark.sql.DataFrame):
    Returns:
        measurements (pyspark.sql.DataFrame):
    """

    # type guards
    if not isinstance(msmt, DataFrame):
        raise ValueError()
    if not isinstance(concept, DataFrame):
        raise ValueError()
    if not isinstance(concept_ancestor, DataFrame):
        raise ValueError()

    # schema guards
    try:
        assertSchemaEqual(msmt.schema, schemas.OMOP_MEASUREMENT_SCHEMA)
        assertSchemaEqual(concept.schema, schemas.OMOP_CONCEPT_SCHEMA)
        assertSchemaEqual(concept_ancestor.schema, schemas.OMOP_CONCEPT_ANCESTOR_SCHEMA)
    except:
        raise ValueError()

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
        .withColumn("value_converted", F.lit(None))
        .withColumn("unit_converted", F.lit(None))
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
        .withColumn("value_converted", F.lit(None))
        .withColumn("unit_converted", F.lit(None))
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
        .withColumn("value_converted", F.lit(None))
        .withColumn("unit_converted", F.lit(None))
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
        .withColumn("value_converted", F.lit(None))
        .withColumn("unit_converted", F.lit(None))
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
        .withColumn("value_converted", F.lit(None))
        .withColumn("unit_converted", F.lit(None))
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
        *schemas.STD_OMOP_MEASUREMENT_SCHEMA.fieldNames(),
    ]
    all_frames = [df.select(*[c for c in OUTPUT_COLUMNS if c in df.columns]) for df in all_frames]
    labs_vitals_union = all_frames[0]
    for frame in all_frames[1:]:
        labs_vitals_union = labs_vitals_union.unionByName(frame, allowMissingColumns=True)

    # write the standardized concept id/value/unit back into the real OMOP fields, then
    # drop the custom columns -- output conforms to plain OMOP_MEASUREMENT_SCHEMA, not
    # STD_OMOP_MEASUREMENT_SCHEMA's separate std_concept_id/value_converted/unit_converted
    labs_vitals_union = (
        labs_vitals_union
        .withColumn("measurement_concept_id", F.col("std_concept_id"))
        .withColumn("value_as_number", F.col("value_converted").cast("double"))
        .withColumn("unit_source_value", F.col("unit_converted"))
        .select(*schemas.OMOP_MEASUREMENT_SCHEMA.fieldNames())
    )

    # perform fallback logic for all unmapped measurements (for now, null all values)
    unmapped_msmt = msmt.join(
        labs_vitals_union.select("measurement_id").distinct(),
        on="measurement_id",
        how="left_anti"
    )

    # perform fallback logic for all other measurement types
    fallback_msmt = standardize_numeric_values_and_units(unmapped_msmt)

    # union all data frames and return
    final_msmt = labs_vitals_union.unionByName(fallback_msmt, allowMissingColumns=True)

    return final_msmt

if __name__ == "__main__":

    # perform imports
    from pyspark.sql import SparkSession
    from pathlib import Path
    from dotenv import load_dotenv
    import os
    from meds_biobank import schemas

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
    REPO_ROOT = Path(__file__).resolve().parents[3]
    OMOP_DATA_DIR = REPO_ROOT / os.environ["OMOP_DATA_DIR"] / "generated-standard"
    msmt_path = OMOP_DATA_DIR / "measurement.csv"
    concept_path = OMOP_DATA_DIR / "concept.csv"
    concept_ancestor_path = OMOP_DATA_DIR / "concept_ancestor.csv"

    # read measurements and metadata
    msmt = spark.read.csv(str(msmt_path), schema=schemas.OMOP_MEASUREMENT_SCHEMA, header=True)
    concept = spark.read.csv(str(concept_path), schema=schemas.OMOP_CONCEPT_SCHEMA, header=True)
    concept_ancestor = spark.read.csv(str(concept_ancestor_path), schema=schemas.OMOP_CONCEPT_ANCESTOR_SCHEMA, header=True)

    # standardize measurements
    std_msmt = lv_standardize(msmt, concept, concept_ancestor)