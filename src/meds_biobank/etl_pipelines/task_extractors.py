from meds_biobank import concepts
import pyspark.sql.functions as F
from pyspark.sql import Window

def extract_disease_cohort(pt_ids, cond_phex, proc, concept, x_codes, proc_codes, excl_codes, onset_cids, delta=180):
    """
    Doc:
    - Create a case and control cohort from ANY phecodes and procedure codes
    - Case = phecode w/in 90 days of procedure code
    - this function extracts a case cohort of people having an x_code and a proc_code within 90 days
    - dx time is the earliest date of an event that is part of a qualifying (x_code, proc_code) pair
    - this function extracts a control cohort of people not having the x_code at all
    NOTE: cond_phex is a conditions phecodes mapping where condition_source_value contains the phecode
    NOTE: must ensure all patients are from task split

    Input: pt_ids
        format: |patient_id|
    Input: cond_phex
        format: |person_id|condition_source_value|condition_start_datetime|condition_start_date|visit_occurrence_id|*
    Input: proc
        format: |person_id|procedure_source_value|procedure_concept_id|procedure_datetime|procedure_date|*
    Input: concept
        format: |concept_id|concept_name|vocabulary_id|*
    Input: x_codes
        format: List[condition_source_value (phecode) (String)]
    Input: proc_codes
        format: List[procedure_source_value (CPT4 code) (String)]
    Return: patients
        format: |patient_id|dx_time|
    Return: controls
        format: |patient_id|
    """

    # cast patient id to match meds splits dataframe
    cond_phex = cond_phex.withColumn("patient_id", F.xxhash64(F.col("person_id"))).drop("person_id")
    proc = proc.withColumn("patient_id", F.xxhash64(F.col("person_id"))).drop("person_id")

    # filter for positive dx codes and stamp time
    cond = (

        # conditions phecodes mapping
        cond_phex

        # filter for matching dx
        .filter(F.col("condition_source_value").isin(x_codes))

        # stamp time
        .withColumn(
            "phecode_time",
            F.date_trunc("day", F.coalesce(F.col("condition_start_datetime"), F.to_timestamp(F.col("condition_start_date"))))
        )
    )

    # compute positive patients
    cases = (

        # positive dx codes
        cond

        # get first dx time and number of codes
        .groupBy("patient_id").agg(F.countDistinct("phecode_time").alias("code_days"))

        # filter for two or more dx codes
        .filter(F.col("code_days") >= 2)

        # select ids
        .select("patient_id")

    ) # |patient_id|phecode_time|

    # compute negative patients
    controls = pt_ids.join(cond, "patient_id", "leftanti").select("patient_id") # |patient_id|

    # filter controls based on exclusion criteria
    excl = cond_phex.filter(F.col("condition_source_value").isin(excl_codes)).select("patient_id")
    controls = controls.join(excl, "patient_id", "leftanti")

    # for cases, extract phecode time as BOD
    cond = (

        # positive dx codes
        cond

        # filter for cases only
        .join(cases, "patient_id", "inner")

        # filter for times that were extracted
        .filter(F.col("phecode_time").isNotNull())

        # select only patient id and phecode time
        .select("patient_id", "phecode_time")
    ) # |patient_id|phecode_time|

    # match user-specified procedures
    proc = (

        # procedures
        proc

        # join concepts for filtration
        .join(
            concept.select("concept_id", "vocabulary_id"),
            proc.procedure_concept_id == concept.concept_id,
            "inner"
        )

        # drop un-needed col
        .drop("concept_id")

        # filter for matching procedures
        .filter((F.col("vocabulary_id") == "CPT4") & (F.col("procedure_source_value").isin(proc_codes)))

        # compute procedure time
        .withColumn(
            "proc_time",
            F.date_trunc("day", F.coalesce(F.col("procedure_datetime"), F.to_timestamp(F.col("procedure_date"))))
        )

        # drop procs where date failed to extract
        .filter(F.col("proc_time").isNotNull())

        # select only pt id and proc time
        .select("patient_id", "proc_time")
    ) # |patient_id|proc_time|

    # join procedure events to phecode events
    proc_cond = proc.join(cond, "patient_id", "inner") # |patient_id|phecode_time|proc_time|

    # filter for phecode time w/in delta days of proc time
    proc_cond = proc_cond.filter(F.abs(F.datediff(F.col("proc_time"), F.col("phecode_time"))) <= delta) # |patient_id|phecode_time|proc_time|

    # select earliest date for each pair as index date, then earliest of these for each patient as dx date
    cases = (
        proc_cond
        .withColumn("index_date", F.least(F.col("proc_time"), F.col("phecode_time")))
        .groupBy("patient_id")
        .agg(F.min("index_date").alias("dx_time"))
        .select("patient_id", "dx_time")
    ) # |patient_id|dx_time|

    # roll back dx date in case exists code before qualifier
    first_phe = (
        cond_phex
        .filter(F.col("condition_source_value").isin(x_codes) | F.col("condition_source_concept_id").isin(onset_cids))
        .withColumn(
            "phecode_time",
            F.date_trunc("day", F.coalesce(F.col("condition_start_datetime"), F.to_timestamp(F.col("condition_start_date"))))
        )
        .groupBy("patient_id").agg(F.min("phecode_time").alias("first_phe"))
    )
    cases = (
        cases
        .join(first_phe, "patient_id")
        .withColumn("dx_time", F.least("dx_time", "first_phe"))
        .select("patient_id", "dx_time")
    ) # |patient_id|dx_time|

    return cases, controls

def create_onset_task(cases, controls, visits, gap=1, lookback=0, followup=1, multiplier=1):
    """
    Doc:
    - create an onset task from ANY case and control cohort
    - samples one pre-dx visit per case patient, and one per control
    - autobalanced to have same number of cases and controls

    Input: cases
        format: |patient_id|dx_time|
    Input: controls
        format: |patient_id|
    Input: visits
        format: |person_id|visit_start_datetime|visit_start_date|visit_ocurrence_id|*
    Return onset
        format: |patient_id|pred_time|n_days|label|
    """

    # read visit occurrence df, compute date
    visits = (

        # visits
        visits

        # compute patient id for matches
        .withColumn("patient_id", F.xxhash64(F.col("person_id")))

        # compute visit date
        .withColumn(
            "visit_date",
            F.date_trunc("day", F.coalesce(F.col("visit_start_datetime"), F.to_timestamp(F.col("visit_start_date"))))
        )

        # extract first visit for each patient
        .withColumn("first_visit", F.min("visit_date").over(Window.partitionBy("patient_id")))

        # only retain visits if they have enough lookback
        .filter(F.datediff(F.col("visit_date"), F.col("first_visit")) >= lookback)

        # select cols
        .select("patient_id", "visit_occurrence_id", "visit_date")

    ) # |patient_id|visit_occurrence_id|visit_date|

    # join with disease patients -> filter for visit_date < dx_date -> extract pred time and n days
    onset_pos = (

        # cases
        cases

        # fan-out join with visits
        .join(visits, "patient_id", "left")

        # retain only the visits at least GAP days before dx time
        .filter(F.datediff(F.col("dx_time"), F.col("visit_date")) >= gap)

        # stamp pred time
        .withColumnRenamed("visit_date", "pred_time")

        # compute number of days from pred time to censor time
        .withColumn("n_days", F.datediff(F.col("dx_time"), F.col("pred_time")))

        # drop dx time
        .drop("dx_time")

        # select cols
        .select("patient_id", "pred_time", "n_days")
    ) # |patient_id|pred_time|n_days| -> one per visit BEFORE dx date

    # pre-sample reduce each patient to just one visit
    w_pos = Window.partitionBy("patient_id").orderBy(F.xxhash64("patient_id", "pred_time", F.lit(42)), "pred_time")
    onset_pos = onset_pos.withColumn("r", F.row_number().over(w_pos)).filter("r == 1").drop("r")

    # determine censor time for control patients (last visit date)
    onset_neg = (

        # controls
        controls

        # fan-out join with visits
        .join(visits.select("patient_id", "visit_date"), "patient_id", "inner")

        # determine last visit date by patient
        .groupBy("patient_id").agg(F.max("visit_date").alias("last_visit"))

        # set censor time to last visit date for each control patient
        .withColumnRenamed("last_visit", "censor_time")
    ) # |patient_id|censor_time|

    # for controls: sample any visits
    onset_neg = (

        # onset neg
        onset_neg

        # fan-out join on visits
        .join(visits.select("patient_id", "visit_occurrence_id", "visit_date"), "patient_id", "inner")

        # filter for visit date at least max(GAP, FOLLOWUP) days before censor date
        .filter(F.datediff(F.col("censor_time"), F.col("visit_date")) >= max(gap, followup))
    )

    # sample one visit per patient
    w_neg = Window.partitionBy("patient_id").orderBy(F.xxhash64("patient_id", "visit_date", F.lit(42)), "visit_date")
    onset_neg = onset_neg.withColumn("r", F.row_number().over(w_neg)).filter("r == 1").drop("r")

    # number of controls to keep to match number of positives
    n_neg = int(round(multiplier * onset_pos.count()))

    # sample controls to match number of positives
    onset_neg = (

        # onset neg
        onset_neg

        # keep first n_neg controls in a fixed hash order
        .orderBy(F.xxhash64("patient_id", F.lit(42)), "patient_id")
        .limit(n_neg)

        # rename columns
        .withColumnRenamed("visit_date", "pred_time")

        # compute n days between censor time and pred time
        .withColumn("n_days", F.datediff(F.col("censor_time"), F.col("pred_time")))

        # drop col
        .drop("censor_time")

        # select cols
        .select("patient_id", "pred_time", "n_days")
    ) # |patient_id|pred_time|n_days|

    # combine controls with cases for TTE
    onset_pos = onset_pos.withColumn("label", F.lit(1))
    onset_neg = onset_neg.withColumn("label", F.lit(0))
    onset = onset_pos.union(onset_neg)

    return onset

def create_onset_with_geno_task(
    cases,
    controls,
    visits,
    variants,
    established,
    limited_moderate,
    gap=1,
    lookback=0,
    followup=1,
    multiplier=1
):
    """
    Doc:
    - create an onset task from ANY case and control cohort
    - samples one pre-dx visit per case patient, and one per control
    - autobalanced to have same number of cases and controls

    Input: cases
        format: |patient_id|dx_time|
    Input: controls
        format: |patient_id|
    Input: visits
        format: |person_id|visit_start_datetime|visit_start_date|visit_ocurrence_id|*
    Input: variants
        format: |person_id|**variant_cols|*
    Input: established/limited_moderate/not_established
        format(s): List[variant (variant_col) (String)]
    Return onset
        format: |patient_id|pred_time|n_days|label|
    """

    # process variant labels
    variant_labels = (

        # variants
        variants

        # cast pt id
        .withColumn("patient_id", F.xxhash64(F.col("person_id"))).drop("person_id")

        # compute gene positive
        .withColumn(
            "gene_positive",
            (F.expr(" + ".join(established)) >= 1).cast("int")
        )

        # compute gene negative
        .withColumn(
            "gene_negative",
            (F.expr(" + ".join(established + limited_moderate)) == 0).cast("int")
        )

        # filter for gene positive or negative
        .filter(
            (F.col("gene_positive") == 1) | (F.col("gene_negative") == 1)
        )

        # select cols
        .select("patient_id", "gene_positive")

    )
    
    # gather variant ids
    variant_ids = variant_labels.select("patient_id").distinct()
    
    # read visit occurrence df, compute date
    visits = (

        # visits
        visits

        # compute patient id for matches
        .withColumn("patient_id", F.xxhash64(F.col("person_id")))

        # join with has variant info
        .join(variant_ids, "patient_id", "inner")

        # compute visit date
        .withColumn(
            "visit_date",
            F.date_trunc("day", F.coalesce(F.col("visit_start_datetime"), F.to_timestamp(F.col("visit_start_date"))))
        )

        # extract first visit for each patient
        .withColumn("first_visit", F.min("visit_date").over(Window.partitionBy("patient_id")))

        # only retain visits if they have enough lookback
        .filter(F.datediff(F.col("visit_date"), F.col("first_visit")) >= lookback)

        # select cols
        .select("patient_id", "visit_occurrence_id", "visit_date")

    ) # |patient_id|visit_occurrence_id|visit_date|

    # join with disease patients -> filter for visit_date < dx_date -> extract pred time and n days
    onset_pos = (

        # cases
        cases

        # fan-out join with visits
        .join(visits, "patient_id", "left")

        # retain only the visits at least GAP days before dx time
        .filter(F.datediff(F.col("dx_time"), F.col("visit_date")) >= gap)

        # stamp pred time
        .withColumnRenamed("visit_date", "pred_time")

        # compute number of days from pred time to censor time
        .withColumn("n_days", F.datediff(F.col("dx_time"), F.col("pred_time")))

        # drop dx time
        .drop("dx_time")

        # select cols
        .select("patient_id", "pred_time", "n_days")

    ) # |patient_id|pred_time|n_days| -> one per visit BEFORE dx date

    # pre-sample reduce each patient to just one visit
    w_pos = Window.partitionBy("patient_id").orderBy(F.xxhash64("patient_id", "pred_time", F.lit(42)), "pred_time")
    onset_pos = onset_pos.withColumn("r", F.row_number().over(w_pos)).filter("r == 1").drop("r")

    # determine censor time for control patients (last visit date)
    onset_neg = (

        # controls
        controls

        # fan-out join with visits
        .join(visits.select("patient_id", "visit_date"), "patient_id", "inner")

        # determine last visit date by patient
        .groupBy("patient_id").agg(F.max("visit_date").alias("last_visit"))

        # set censor time to last visit date for each control patient
        .withColumnRenamed("last_visit", "censor_time")

    ) # |patient_id|censor_time|

    # for controls: sample any visits
    onset_neg = (

        # onset neg
        onset_neg

        # fan-out join on visits
        .join(visits.select("patient_id", "visit_occurrence_id", "visit_date"), "patient_id", "inner")

        # filter for visit date at least max(GAP, FOLLOWUP) days before censor date
        .filter(F.datediff(F.col("censor_time"), F.col("visit_date")) >= max(gap, followup))

    )

    # sample one visit per patient
    w_neg = Window.partitionBy("patient_id").orderBy(F.xxhash64("patient_id", "visit_date", F.lit(42)), "visit_date")
    onset_neg = onset_neg.withColumn("r", F.row_number().over(w_neg)).filter("r == 1").drop("r")

    # number of controls to keep to match number of positives
    n_neg = int(round(multiplier * onset_pos.count()))

    # sample controls to match number of positives
    onset_neg = (

        # onset neg
        onset_neg

        # keep first n_neg controls in a fixed hash order
        .orderBy(F.xxhash64("patient_id", F.lit(42)), "patient_id")
        .limit(n_neg)

        # rename columns
        .withColumnRenamed("visit_date", "pred_time")

        # compute n days between censor time and pred time
        .withColumn("n_days", F.datediff(F.col("censor_time"), F.col("pred_time")))

        # drop col
        .drop("censor_time")

        # select cols
        .select("patient_id", "pred_time", "n_days")

    ) # |patient_id|pred_time|n_days|

    # combine controls with cases for TTE
    onset_pos = onset_pos.withColumn("label", F.lit(1))
    onset_neg = onset_neg.withColumn("label", F.lit(0))
    onset = onset_pos.union(onset_neg) # |patient_id|pred_time|n_days|label|

    # attach variant labels
    onset = onset.join(variant_labels, "patient_id", "inner") # |patient_id|pred_time|n_days|label|gene_positive|

    return onset

# TODO: rewrite below more cleanly/clearly

def create_geno_task(cases, variants, established, limited_moderate, not_established):
    """
    Create the genetic variant prediction task from ANY cases and variants dfs along w/ variant meaning lists

    Input: cases has format ...
    Input: variants has format |person_id|**variant_cols|*
    Input: established/limited_moderate/not_established each have format List[variant (variant_col) (String)]
    """

    # fix id in variants df
    variants = variants.withColumn("patient_id", F.xxhash64(F.col("person_id"))).drop("person_id")

    # join variants to cases
    cases = cases.join(
        variants,
        "patient_id",
        "inner"
    )  # person_id, dx_time, variants

    # tag gene positive and gene negative variants
    cases = cases.withColumn(
        "gene_positive",
        (F.expr(" + ".join(established)) >= 1).cast("int")
    )
    cases = cases.withColumn(
        "gene_negative",
        (F.expr(" + ".join(established + limited_moderate)) == 0).cast("int")
    )

    # get even no gene positive and gene negative patients
    pos_cases = cases.filter(F.col("gene_positive") == 1).drop("gene_positive", "gene_negative").withColumn("label", F.lit(1))
    neg_cases = cases.filter(F.col("gene_negative") == 1).drop("gene_positive", "gene_negative").withColumn("label", F.lit(0))
    if neg_cases.count() == 0:
        frac_neg = 0
    else:
        frac_neg = (3 * pos_cases.count())/neg_cases.count()
    neg_cases = neg_cases.sample(fraction=min(frac_neg, 1.0), seed=42)
    cases = pos_cases.union(neg_cases).select("patient_id", "dx_time", "label")

    # rename cols
    cases = cases.withColumnRenamed("dx_time", "pred_time")

    return cases

def _time_until_first_event(cases, events, visit, drop_if_event_before_pred=False):
    """
    Extract time until first event after pred time for cases, or date of last visit if no event
    
    Note: if drop_if_event_before_pred is True, drop patients where event occurs before pred time

    Input: cases |patient_id|pred_time|
    Input: events |patient_id|event_time|
    Input: visit |patient_id|visit_date|
    
    Return: |patient_id|pred_time|n_days|label|
    """

    # copy original cases
    orig_cases = cases
    
    # join events to cases on pt id
    cases = cases.join(events, "patient_id", "inner")

    # if drop_if_event_before_pred is True, drop all patients for whom any event occurs before pred time
    patients_to_drop = None
    if drop_if_event_before_pred:
        patients_to_drop = cases.filter(F.col("event_time") < F.col("pred_time")).select("patient_id").distinct()
        cases = cases.join(patients_to_drop, "patient_id", "left_anti")

    # filter for events that occur after pred_time
    cases = cases.filter(F.col("event_time") > F.col("pred_time"))

    # group by patient id and take min of event_time as censor_time, preserve pred_time
    cases = cases.groupBy("patient_id").agg(F.min("event_time").alias("censor_time"), F.first("pred_time").alias("pred_time"))

    # assign label
    cases = cases.withColumn("label", F.lit(1))

    # anti-join with cases to get patients with no events
    no_events = orig_cases.join(cases, "patient_id", "left_anti")
    if patients_to_drop is not None:
        no_events = no_events.join(patients_to_drop, "patient_id", "left_anti")

    # join with visit to get last visit date, assign to censor_time, propagate pred_time
    no_events = no_events.join(visit, "patient_id", "inner")
    no_events = no_events.groupBy("patient_id").agg(F.max("visit_date").alias("censor_time"), F.first("pred_time").alias("pred_time"))

    # assign label
    no_events = no_events.withColumn("label", F.lit(0))

    # combine
    cases = cases.unionByName(no_events, allowMissingColumns=True)

    # calculate n_days as difference between censor_time and pred_time
    cases = cases.withColumn("n_days", F.datediff(F.col("censor_time"), F.col("pred_time")))

    # return |patient_id|pred_time|n_days|label|
    cases = cases.select("patient_id", "pred_time", "n_days", "label")
    
    return cases

def _get_hfh_events(visit, visit_supp, cond_phex, xcodes_hf):
    # return |patient_id|event_time|
    
    # extract hospitalizations
    hosp = (
        visit
        .join(
            visit_supp.select("visit_occurrence_id", "IsHospitalAdmission"),
            "visit_occurrence_id",
            "inner"
        )
        .filter(F.col("IsHospitalAdmission") == 1)
        .withColumn(
            "hosp_time",
            F.date_trunc("day", F.coalesce(F.col("visit_start_datetime"), F.to_timestamp(F.col("visit_start_date"))))
        )
        .select("visit_occurrence_id", "person_id", "hosp_time")
    )

    # extract hf cases
    hf_cond = (
        cond_phex.filter(F.col("condition_source_value").isin(xcodes_hf))
        .select("visit_occurrence_id")
        .distinct()
    )

    # filter hospitalizations for occurs w/ hf xcode event
    hf_hosp = (
        hosp
        .join(hf_cond, "visit_occurrence_id", "inner")
        .select("person_id", "hosp_time")
    ) # person_id, hosp_time

    # prepare hf_hosp as event df
    hf_hosp = hf_hosp.withColumn("patient_id", F.xxhash64(F.col("person_id"))).drop("person_id")
    hf_hosp = hf_hosp.withColumn("event_time", F.col("hosp_time")).drop("hosp_time")
    hf_hosp = hf_hosp.select("patient_id", "event_time")

    return hf_hosp

def create_hfh_task(cases, visit, visit_supp, cond_phex, xcodes_hf):
    """
    return: hfh task w/ format |patient_id|pred_time|n_days|label|
    """
    
    # extract hfh events
    hf_hosp = _get_hfh_events(visit, visit_supp, cond_phex, xcodes_hf)

    # prepare cases as case df
    cases = cases.withColumn("pred_time", F.col("dx_time")).select("patient_id", "pred_time")

    # prepare visit
    visit = visit.withColumn(
        "visit_date",
        F.date_trunc("day", F.coalesce(F.col("visit_start_datetime"), F.to_timestamp(F.col("visit_start_date"))))
    )
    visit = visit.withColumn("patient_id", F.xxhash64(F.col("person_id")))
    visit = visit.select("patient_id", "visit_date")

    return _time_until_first_event(cases, hf_hosp, visit, drop_if_event_before_pred=False)

def _get_death_events(death):
    """
    death: |person_id|death_date|
    return: |person_id|event_date|
    """
    # get death events
    death = death.withColumn("patient_id", F.xxhash64("person_id"))
    return death.select("patient_id", "death_date").withColumnRenamed("death_date", "event_time")

def create_acm_task(cases, death, visit):
    """
    return: acm task w/ format |patient_id|pred_time|n_days|label|
    """
    
    # extract death events
    death_events = _get_death_events(death)

    # prepare cases
    cases = cases.withColumn("pred_time", F.col("dx_time")).select("patient_id", "pred_time")

    # prepare visit
    visit = visit.withColumn(
        "visit_date",
        F.date_trunc("day", F.coalesce(F.col("visit_start_datetime"), F.to_timestamp(F.col("visit_start_date"))))
    )
    visit = visit.withColumn("patient_id", F.xxhash64(F.col("person_id")))
    visit = visit.select("patient_id", "visit_date")

    # get time until death for each patient
    return _time_until_first_event(cases, death_events, visit, drop_if_event_before_pred=True)

if __name__ == "__main__":

    # read in necessary data
    cond_phex = ... # |person_id|condition_source_value|condition_start_datetime|condition_start_date|*
    proc = ... # |person_id|procedure_source_value|procedure_datetime|procedure_date|*
    concept = ... # |concept_id|concept_name|vocabulary_id|*
    visits = ... # |patient_id|visit_date|visit_occurrence_id|*
    visit_supp = ... # |visit_occurrence_id|IsHospitalAdmission|
    variants = ... # |person_id|**variant_cols|*

    # extract disease patients
    hcm_cases, hcm_controls = extract_disease_cohort(cond_phex, proc, concept, HCM_PHECODES, ECHO_CPT4_CODES)
    dcm_cases, dcm_controls = extract_disease_cohort(cond_phex, proc, concept, DCM_PHECODES, ECHO_CPT4_CODES)

    # extract onset tasks
    hcm_onset = create_onset_task(hcm_cases, hcm_controls, visits)
    dcm_onset = create_onset_task(dcm_cases, dcm_controls, visits)

    # extract genotype tasks
    hcm_geno = create_geno_task(hcm_cases, variants, HCM_ESTABLISHED, HCM_LIMITED_MODERATE, HCM_NOT_ESTABLISHED)
    dcm_geno = create_geno_task(dcm_cases, variants, DCM_ESTABLISHED, DCM_LIMITED_MODERATE, DCM_NOT_ESTABLISHED)

    # extract tasks
    hcm_hfh_task = create_hfh_task(hcm_cases, visit, visit_supp, cond_phex, HF_PHECODES)
    dcm_hfh_task = create_hfh_task(dcm_cases, visit, visit_supp, cond_phex, HF_PHECODES)