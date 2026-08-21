import pyspark.sql.functions as F

from meds_biobank.standardizers.standardizers import autostd

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


def standardize_mid(measurement, concept, concept_ancestor):
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

    # build the same joined base the groupings' predicates are written against
    base = (
        measurement
        .join(concept_ancestor, measurement.measurement_concept_id == concept_ancestor.descendant_concept_id, "left")
        .join(concept, F.col("descendant_concept_id") == concept.concept_id, "left")
    )

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

def standardize_numeric_value(measurement, concept, concept_ancestor):
    """
    Apply autostd's unit/value normalization to measurements whose measurement_concept_id
    has already been relabeled by standardize_mid(). In theory this covers every grouped
    mcid the same way standardize()'s per-domain hardcoded unit targets did, just via one
    generic pass instead of ~90 bespoke ones -- so it should agree with standardize() in
    the end, just better factored.

    Args:
        measurement (pyspark.sql.DataFrame): OMOP measurement table
        concept (pyspark.sql.DataFrame): OMOP concept table
        concept_ancestor (pyspark.sql.DataFrame): OMOP concept_ancestor table

    Returns:
        pyspark.sql.DataFrame: autostd() output on the mid-standardized measurements
    """
    return autostd(standardize_mid(measurement, concept, concept_ancestor))

def standardize_text_value(measurement):
    pass
