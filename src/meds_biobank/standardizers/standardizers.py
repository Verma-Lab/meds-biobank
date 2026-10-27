import pyspark.sql.functions as F
from pyspark.sql import Window
from pyspark.sql import functions as F, Window
from meds_biobank import schemas
from pyspark.sql import DataFrame
from pyspark.testing import assertSchemaEqual

# groupings for measurement concept id standardization
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

# ancestor ids a GROUPINGS predicate tests that differ from its own concept_id_std (labs_esr, labs_ptt) -- keep in sync with GROUPINGS
_EXTRA_GROUPING_ANCESTOR_IDS = {4028908, 1618784}

def standardize_measurement_concept_id_fast(measurement, concept, concept_ancestor):
    if not isinstance(measurement, DataFrame):
        raise ValueError()
    if not isinstance(concept, DataFrame):
        raise ValueError()
    if not isinstance(concept_ancestor, DataFrame):
        raise ValueError()

    # filter concept_ancestor's full transitive closure down to the ids GROUPINGS actually tests, to avoid fanning out every measurement row by its whole ancestry
    grouping_ancestor_ids = {std_concept_id for _, _, std_concept_id in GROUPINGS} | _EXTRA_GROUPING_ANCESTOR_IDS
    concept_ancestor_filtered = concept_ancestor.filter(F.col("ancestor_concept_id").isin(grouping_ancestor_ids))

    # concept_name predicates need measurement's own concept even when no ancestor row survives the filter above, so look it up directly instead of through concept_ancestor
    concept_by_id = concept.select("concept_id", "concept_name")

    # base msmts df to filter
    base = (
        measurement

        # add concept name to measurements
        .join(concept_by_id, measurement.measurement_concept_id == concept_by_id.concept_id, "left")

        # fan-out on parent concepts
        .join(concept_ancestor_filtered, measurement.measurement_concept_id == concept_ancestor_filtered.descendant_concept_id, "left")
    )

    # cols of msmt data
    measurement_cols = measurement.columns

    # convert match conditions into F.when commands
    match_structs = F.array(*[
        F.when(predicate(), F.struct(F.lit(i).alias("_grp"), F.lit(std_concept_id).alias("concept_id_std")))
        for i, (_, predicate, std_concept_id) in enumerate(GROUPINGS)
    ])

    # apply F.when commands
    base_matched = base.withColumn("matches", F.filter(match_structs, lambda x: x.isNotNull()))

    # explode matches to return
    exploded = (
        base_matched
        .withColumn("m", F.explode("matches"))
        .select(*measurement_cols, F.col("m._grp").alias("_grp"), F.col("m.concept_id_std").alias("concept_id_std"))
        .dropDuplicates(["measurement_id", "_grp"])
        .drop("_grp")
    )

    # recover measurements not grouped to re-join
    uncovered = measurement.join(
        exploded.select("measurement_id").distinct(),
        on="measurement_id",
        how="left_anti"
    ).withColumn("concept_id_std", F.col("measurement_concept_id"))

    # re-join
    return exploded.unionByName(uncovered)


def standardize_measurement_concept_id(measurement, concept, concept_ancestor):
    """
    Apply GROUPINGS (the per-domain ancestor/concept matching that standardize() encodes)
    to compute each measurement's standardized concept id.

    For every grouping, select the matching rows, copy them, and add concept_id_std set to
    the grouping's std_concept_id. Union all grouping copies together, then union back in
    the measurements that matched no grouping with concept_id_std set to their own
    (unchanged) measurement_concept_id.

    Args:
        measurement (pyspark.sql.DataFrame): OMOP measurement table
        concept (pyspark.sql.DataFrame): OMOP concept table
        concept_ancestor (pyspark.sql.DataFrame): OMOP concept_ancestor table

    Returns:
        pyspark.sql.DataFrame: measurement, plus a new concept_id_std column holding the
            standardized concept id for every row (grouping's std_concept_id for a match,
            or the original measurement_concept_id otherwise). measurement_concept_id
            itself is untouched. A row that matches more than one grouping appears once
            per match, same as standardize()'s per-domain unions do today.
    """

    # type guards
    if not isinstance(measurement, DataFrame):
        raise ValueError()
    if not isinstance(concept, DataFrame):
        raise ValueError()
    if not isinstance(concept_ancestor, DataFrame):
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
        .withColumn("concept_id_std", F.lit(std_concept_id))
        .select(*measurement_cols, "concept_id_std")
        for _, predicate, std_concept_id in GROUPINGS
    ]

    # union groups
    covered = grouped_frames[0]
    for frame in grouped_frames[1:]:
        covered = covered.unionByName(frame)

    # measurements that matched no grouping pass through
    uncovered = measurement.join(
        covered.select("measurement_id").distinct(),
        on="measurement_id",
        how="left_anti"
    ).withColumn("concept_id_std", F.col("measurement_concept_id"))

    return covered.unionByName(uncovered)

def standardize_numeric_values_and_units_reference(measurement, concept_schema, mcid_standardized=True):
    """
    Like standardize_numeric_values_and_units, but instead of picking a per-code target unit
    from this data's own distribution, matches each measurement against the unit(s) already
    recorded for its code in concept_schema (from a prior standardization run) and converts
    onto whichever one is reachable.

    Args:
        measurement (pyspark.sql.DataFrame): OMOP measurement table, already run through
            standardize_measurement_concept_id/_fast if mcid_standardized
        concept_schema (pyspark.sql.DataFrame): MEDS concept schema from a prior
            standardization run |code|name|ancestors|factors|event_type|units|
        mcid_standardized (bool): key on concept_id_std (True) or measurement_concept_id (False)

    Returns:
        pyspark.sql.DataFrame: measurement, plus new numeric_value_std/unit_std columns, set
            only when the row's unit matches or converts to one of concept_schema's known
            valid units for its code, null otherwise. The row/code itself is always retained.
            value_as_number/unit_source_value themselves are untouched.
    """

    # type guards
    if not isinstance(measurement, DataFrame):
        raise ValueError()
    if not isinstance(concept_schema, DataFrame):
        raise ValueError()

    if mcid_standardized:
        cid_col = "concept_id_std"
    else:
        cid_col = "measurement_concept_id"

    # map units to canonical value (mirrors standardize_numeric_values_and_units's ladder)
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

    df = measurement.withColumn("unit_norm", normalized_unit)

    # same physical-unit conversion factors as standardize_numeric_values_and_units
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
    factor_map = F.create_map(*[F.lit(x) for kv in unit_to_canonical.items() for x in kv])

    # which physical-unit family each canonical unit belongs to -- conversion is only valid within the same ladder, since each ladder's 1.0 basepoint is arbitrary and not on a shared scale with the others
    unit_to_ladder = {
        "mg/dL": "mass_concentration", "g/dL": "mass_concentration", "g/L": "mass_concentration",
        "mg/L": "mass_concentration", "mg/mL": "mass_concentration",
        "pg/mL": "hormone_protein", "ng/L": "hormone_protein", "ng/dL": "hormone_protein", "ng/mL": "hormone_protein",
        "ug/mL": "microgram", "ug/dL": "microgram",
        "Cells/uL": "cell_count", "Thousand/uL": "cell_count", "Million/uL": "cell_count",
        "Inches": "length", "Feet": "length",
        "oz": "weight", "lb": "weight",
        # single-unit families have no conversion partner, so each is its own ladder
        "Unit/L": "Unit/L", "International Units/mL": "International Units/mL", "uIU/mL": "uIU/mL",
        "Leu/uL": "Leu/uL", "mmol/L": "mmol/L", "nmol/L": "nmol/L", "mmHg": "mmHg", "Second(s)": "Second(s)",
        "%": "%", "Counts/Min": "Counts/Min", "mm/h": "mm/h", "kg/m^2": "kg/m^2", "mcg/mg": "mcg/mg",
        "mL/min/1.73m2": "mL/min/1.73m2", "pg": "pg", "fL": "fL", "/hpf": "/hpf",
    }
    ladder_map = F.create_map(*[F.lit(x) for kv in unit_to_ladder.items() for x in kv])

    df = df.withColumn("factor_from", factor_map[F.col("unit_norm")])
    df = df.withColumn("ladder_from", ladder_map[F.col("unit_norm")])

    # one row per (code, valid reference unit) pair this data's codes could match against
    reference_units = (
        concept_schema
        .select(F.col("code").alias("_ref_code"), F.explode("units").alias("_ref_unit"))
        .withColumn("_ref_factor", factor_map[F.col("_ref_unit")])
        .withColumn("_ref_ladder", ladder_map[F.col("_ref_unit")])
    )

    # bring in every candidate reference unit for the row's code, keep only ones actually reachable from unit_norm, and only within the same unit ladder
    candidates = (
        df.join(reference_units, F.col(cid_col) == F.col("_ref_code"), "inner")
        .withColumn("_already_target", F.col("unit_norm") == F.col("_ref_unit"))
        .filter(F.col("_already_target") | (F.col("factor_from").isNotNull() & F.col("_ref_factor").isNotNull() & (F.col("ladder_from") == F.col("_ref_ladder"))))
    )

    # a row can reach more than one valid unit (its own spelling plus a convertible one); prefer the exact match, then break ties deterministically
    rank_w = Window.partitionBy("measurement_id").orderBy(F.desc("_already_target"), F.col("_ref_unit"))
    matched = (
        candidates
        .withColumn("_rank", F.row_number().over(rank_w))
        .filter(F.col("_rank") == 1)
        .withColumn(
            "numeric_value_std",
            F.when(F.col("_already_target"), F.round(F.col("value_source_value").try_cast("double"), 3))
             .otherwise(F.round(F.col("value_source_value").try_cast("double") * F.col("factor_from") / F.col("_ref_factor"), 3))
        )
        .withColumn("unit_std", F.col("_ref_unit"))
        .select(*measurement.columns, "numeric_value_std", "unit_std")
    )

    # codes/rows with no reachable reference unit keep their row but get no value/unit
    unmatched = (
        measurement.join(matched.select("measurement_id").distinct(), on="measurement_id", how="left_anti")
        .withColumn("numeric_value_std", F.lit(None).cast("double"))
        .withColumn("unit_std", F.lit(None).cast("string"))
    )

    return matched.unionByName(unmatched)

def standardize_numeric_values_and_units(measurement, mcid_standardized=True):
    """
    Apply unit/value homogenization (or nullification) to measurements by measurement_concept_id.

    Args:
        measurement (pyspark.sql.DataFrame): OMOP measurement table
        concept (pyspark.sql.DataFrame): OMOP concept table
        concept_ancestor (pyspark.sql.DataFrame): OMOP concept_ancestor table

    Returns:
        pyspark.sql.DataFrame: measurement, plus new numeric_value_std/unit_std columns
            holding each measurement homogenized to its measurement_concept_id's modal
            unit where possible (null where not). value_as_number/unit_source_value
            themselves are untouched.
    """

    # type guard
    if not isinstance(measurement, DataFrame):
        raise ValueError
    
    if mcid_standardized:
        cid_col = "concept_id_std"
    else:
        cid_col = "measurement_concept_id"

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

    # which physical-unit family each canonical unit belongs to -- conversion is only valid within the same ladder, since each ladder's 1.0 basepoint is arbitrary and not on a shared scale with the others
    unit_to_ladder = {
        "mg/dL": "mass_concentration", "g/dL": "mass_concentration", "g/L": "mass_concentration",
        "mg/L": "mass_concentration", "mg/mL": "mass_concentration",
        "pg/mL": "hormone_protein", "ng/L": "hormone_protein", "ng/dL": "hormone_protein", "ng/mL": "hormone_protein",
        "ug/mL": "microgram", "ug/dL": "microgram",
        "Cells/uL": "cell_count", "Thousand/uL": "cell_count", "Million/uL": "cell_count",
        "Inches": "length", "Feet": "length",
        "oz": "weight", "lb": "weight",
        # single-unit families have no conversion partner, so each is its own ladder
        "Unit/L": "Unit/L", "International Units/mL": "International Units/mL", "uIU/mL": "uIU/mL",
        "Leu/uL": "Leu/uL", "mmol/L": "mmol/L", "nmol/L": "nmol/L", "mmHg": "mmHg", "Second(s)": "Second(s)",
        "%": "%", "Counts/Min": "Counts/Min", "mm/h": "mm/h", "kg/m^2": "kg/m^2", "mcg/mg": "mcg/mg",
        "mL/min/1.73m2": "mL/min/1.73m2", "pg": "pg", "fL": "fL", "/hpf": "/hpf",
    }
    ladder_map = F.create_map(*[F.lit(x) for kv in unit_to_ladder.items() for x in kv])

    # compute the target unit for each row of measurements
    pair_w = Window.partitionBy(cid_col, "unit_norm")
    df = df.withColumn("pair_count", F.count("*").over(pair_w))

    # tiebreak on unit_norm itself so the mode pick is deterministic when two units are equally common
    rank_w = Window.partitionBy(cid_col).orderBy(F.desc("pair_count"), F.col("unit_norm"))
    df = df.withColumn("unit_rank", F.row_number().over(rank_w))
    mode_w = Window.partitionBy(cid_col)
    df = df.withColumn(
        "target_unit",
        F.min(F.when(F.col("unit_rank") == 1, F.col("unit_norm"))).over(mode_w)
    )

    # perform unit and value conversion for non-mode units
    df = df.withColumn("factor_from", factor_map[F.col("unit_norm")])
    df = df.withColumn("factor_to", factor_map[F.col("target_unit")])
    df = df.withColumn("ladder_from", ladder_map[F.col("unit_norm")])
    df = df.withColumn("ladder_to", ladder_map[F.col("target_unit")])
    already_target = F.col("unit_norm") == F.col("target_unit")
    # ladders' 1.0 basepoints aren't on a shared scale, so a factor lookup hit on both sides isn't enough -- require same ladder too
    convertible = F.col("factor_from").isNotNull() & F.col("factor_to").isNotNull() & (F.col("ladder_from") == F.col("ladder_to"))
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

    # rename the standardized value to its final column name; unit_std is already named
    # correctly. Neither collides with an existing column, since value_as_number/
    # unit_source_value are left untouched -- these are new columns, not overwrites.
    df = df.withColumnRenamed("value_std", "numeric_value_std")

    return df.drop("pair_count", "unit_rank", "factor_from", "factor_to", "ladder_from", "ladder_to", "unit_norm", "target_unit")


def standardize_text_value(measurement):
    """
    Apply text value simplification to measurements without parseable numeric values:
    strip surrounding whitespace, collapse repeated internal whitespace, and lowercase
    value_source_value so that e.g. "POSITIVE", "  positive", and "Positive  " all
    normalize to the same "positive".

    Args:
        measurement (pyspark.sql.DataFrame): OMOP measurement table

    Returns:
        pyspark.sql.DataFrame: measurement, plus a new text_value_std column: the cleaned
            value_source_value for every row that doesn't have a parseable numeric value,
            and null for rows that do (or have a null value_source_value).
            value_source_value itself is untouched.
    """
    # text-valued rows
    has_text_value = measurement.filter(
        (F.col("value_source_value").isNotNull()) &
        (F.expr("try_cast(value_source_value AS DOUBLE)").isNull())
    )

    # collapse internal whitespace runs to a single space, then trim the leading/trailing space that leaves behind, then lowercase
    cleaned_value = F.lower(F.trim(F.regexp_replace(F.col("value_source_value"), r"\s+", " ")))
    text_cleaned = has_text_value.withColumn("text_value_std", cleaned_value)

    # numeric-valued and null-valued rows get a null text_value_std
    not_text_value = measurement.join(
        has_text_value.select("measurement_id").distinct(),
        on="measurement_id",
        how="left_anti"
    ).withColumn("text_value_std", F.lit(None).cast("string"))

    return text_cleaned.unionByName(not_text_value)