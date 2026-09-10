from meds_biobank import concepts
import pyspark.sql.functions as F

def extract_disease_cohort(splits, cond_phex, proc, concept, x_codes, proc_codes):
    """
    Create a case and control cohort from ANY phecodes and procedure codes
    Case = phecode w/in 90 days of procedure code

    Input: splits format |patient_id|split|
    Input: cond_phex format |person_id|condition_source_value|condition_start_datetime|condition_start_date|*
    Input: proc format |person_id|procedure_source_value|procedure_datetime|procedure_date|*
    Input: concept format |concept_id|concept_name|vocabulary_id|*
    Input: x_codes format List[condition_source_value (phecode) (String)]
    Input: proc_codes format List[procedure_source_value (CPT4 code) (String)]

    Note: cond_phex is a conditions phecodes mapping where condition_source_value contains the phecode
    Note: must ensure all patients are from task split
    Description:
        - this function extracts a case cohort of people having an x_code and a proc_code within 90 days
        - dx time is the earliest date of an event that is part of a qualifying (x_code, proc_code) pair
        - this function extracts a control cohort of people not having the x_code at all


    Return: patients with |patient_id|dx_time
    Return: controls with |patient_id|
    """
    
    # subset to task patients
    task_pt_ids = splits.filter(F.col("split") == "task").select("patient_id").distinct()

    # cast patient id to match meds splits dataframe
    cond_phex = cond_phex.withColumn("patient_id", F.xxhash64(F.col("person_id"))).drop("person_id")
    proc = proc.withColumn("patient_id", F.xxhash64(F.col("person_id"))).drop("person_id")

    # filter for task patients only
    cond_phex = cond_phex.join(
        task_pt_ids,
        "patient_id",
        "inner"
    )
    proc = proc.join(
        task_pt_ids,
        "patient_id",
        "inner"
    )

    # filter conditions into hcm and dcm conditions
    cond = cond_phex.filter(F.col("condition_source_value").isin(x_codes))

    # extract patients who have no hcm/dcm diagnosis as controls
    controls = (
        task_pt_ids
        .join(
            cond.select("patient_id").distinct(),
            "patient_id",
            "leftanti"
        )
        .select("patient_id")
    )

    # for cases, extract phecode time as BOD for coalesce(condition_start_datetime/condition_start_date)
    cond = (
        cond
        .withColumn(
            "phecode_time",
            F.date_trunc("day", F.coalesce(F.col("condition_start_datetime"), F.to_timestamp(F.col("condition_start_date"))))
        )
        .filter(F.col("phecode_time").isNotNull())
        .select("patient_id", "phecode_time")
    )

    # extract echo procedures
    proc = (
        proc
        .join(
            concept.select("concept_id", "vocabulary_id"),
            proc.procedure_concept_id == concept.concept_id,
            "inner"
        )
        .drop("concept_id")
        .filter((F.col("vocabulary_id") == "CPT4") & (F.col("procedure_source_value").isin(proc_codes)))
        .withColumn(
            "proc_time",
            F.date_trunc("day", F.coalesce(F.col("procedure_datetime"), F.to_timestamp(F.col("procedure_date"))))
        )
        .filter(F.col("proc_time").isNotNull())
        .select("patient_id", "proc_time")
    )

    # join echo procedures to HCM/DCM cond events
    proc_cond = proc.join(cond, "patient_id", "inner") # patient_id, phecode_time, proc_time

    # filter for all pairs where phecode time is w/in 90 days of proc time
    proc_cond = (
        proc_cond.filter(
            F.abs(F.datediff(F.col("proc_time"), F.col("phecode_time"))) <= 90
        )
    )

    # select earliest date for each pair as index date, then earliest of these for each patient as dx date
    cases = (
        proc_cond
        .withColumn("index_date", F.least(F.col("proc_time"), F.col("phecode_time")))
        .groupBy("patient_id")
        .agg(F.min("index_date").alias("dx_time"))
        .select("patient_id", "dx_time")
    ) # patient_id, dx_time

    return cases, controls

def create_onset_task(cases, controls, visits, npos=1000, nneg=1000):
    """
    Create an onset task from ANY case and control cohort

    Input: cases has format |patient_id|dx_time|
    Input: controls has format |patient_id|
    Input: visits has format |person_id|visit_start_datetime|visit_start_date|visit_ocurrence_id|*
    
    Return "onset" with format |patient_id|pred_time|n_days|label|
    """

    # read visit occurrence df, compute date
    visits = visits.withColumn("patient_id", F.xxhash64(F.col("person_id")))
    visits = visits.withColumn(
        "visit_date",
        F.date_trunc("day", F.coalesce(F.col("visit_start_datetime"), F.to_timestamp(F.col("visit_start_date"))))
    )

    # join with disease patients -> filter for visit_date < dx_date -> extract pred time and n days
    cases = (
        cases
        .join(
            visits.select("patient_id", "visit_occurrence_id", "visit_date"),
            "patient_id",
            "left"
        )
        .filter(F.col("visit_date") < F.col("dx_time"))
        .withColumnRenamed("visit_date", "pred_time")
        .withColumn("n_days", F.datediff(F.col("dx_time"), F.col("pred_time"))) # label is number of days from pred_time to dx_time
        .drop("dx_time")
        .select("patient_id", "pred_time", "n_days")
    ) # patient_id, pred_time, n_days

    # sample up to n_pos such
    if cases.count() == 0:
        frac_pos = 0
    else:
        frac_pos = (npos / cases.count())
    onset_pos = cases.sample(withReplacement=False, fraction=min(frac_pos, 1.0), seed=42)

    # determine censor time for control patients (last visit date)
    controls = (
        controls
        .join(
            visits.select("patient_id", "visit_date"),
            "patient_id",
            "inner"
        )
        .groupBy("patient_id")
        .agg(F.max("visit_date").alias("last_visit"))
        .withColumnRenamed("last_visit", "censor_time")
    ) # patient_id, censor_time

    # for controls: sample any visits
    onset_neg = (
        controls
        .join(
            visits.select("patient_id", "visit_occurrence_id", "visit_date"),
            "patient_id",
            "inner"
        )
    )
    if onset_neg.count() == 0:
        frac_neg = 0
    else:
        frac_neg = (nneg / onset_neg.count())
    onset_neg = (
        onset_neg
        .sample(withReplacement=False, fraction=min(frac_neg, 1.0), seed=42)
        .withColumnRenamed("visit_date", "pred_time")
        .withColumn("n_days", F.datediff(F.col("censor_time"), F.col("pred_time")))
        .drop("censor_time")
        .select("patient_id", "pred_time", "n_days")
    ) # patient_id, pred_time, n_days

    # combine
    onset_pos = onset_pos.withColumn("label", F.lit(1))
    onset_neg = onset_neg.withColumn("label", F.lit(0))
    onset = onset_pos.union(onset_neg)

    return onset

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

def _extract_hfh_events(visit, visit_supp, cond_phex, xcodes_hf):
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
    
    # extract hfh events
    hf_hosp = _extract_hfh_events(visit, visit_supp, cond_phex, xcodes_hf)

    # prepare cases as case df
    cases = cases.withColumn("pred_time", F.col("dx_time"))
    cases = cases.select("patient_id", "pred_time")

    # prepare visit
    visit = visit.withColumn(
        "visit_date",
        F.date_trunc("day", F.coalesce(F.col("visit_start_datetime"), F.to_timestamp(F.col("visit_start_date"))))
    )
    visit = visit.withColumn("patient_id", F.xxhash64(F.col("person_id")))
    visit = visit.select("patient_id", "visit_date")

    return _time_until_first_event(cases, hf_hosp, visit, drop_if_event_before_pred=False)

if __name__ == "__main__":

    # read in necessary data
    splits = ... # |patient_id|split|
    cond_phex = ... # |person_id|condition_source_value|condition_start_datetime|condition_start_date|*
    proc = ... # |person_id|procedure_source_value|procedure_datetime|procedure_date|*
    concept = ... # |concept_id|concept_name|vocabulary_id|*
    visits = ... # |patient_id|visit_date|visit_occurrence_id|*
    visit_supp = ... # |visit_occurrence_id|IsHospitalAdmission|
    variants = ... # |person_id|**variant_cols|*

    # extract disease patients
    hcm_cases, hcm_controls = extract_disease_cohort(splits, cond_phex, proc, concept, HCM_PHECODES, ECHO_CPT4_CODES)
    dcm_cases, dcm_controls = extract_disease_cohort(splits, cond_phex, proc, concept, DCM_PHECODES, ECHO_CPT4_CODES)

    # extract onset tasks
    hcm_onset = create_onset_task(hcm_cases, hcm_controls, visits)
    dcm_onset = create_onset_task(dcm_cases, dcm_controls, visits)

    # extract genotype tasks
    hcm_geno = create_geno_task(hcm_cases, variants, HCM_ESTABLISHED, HCM_LIMITED_MODERATE, HCM_NOT_ESTABLISHED)
    dcm_geno = create_geno_task(dcm_cases, variants, DCM_ESTABLISHED, DCM_LIMITED_MODERATE, DCM_NOT_ESTABLISHED)

    # extract tasks
    hcm_hfh_task = create_hfh_task(hcm_cases, visit, visit_supp, cond_phex, HF_PHECODES)
    dcm_hfh_task = create_hfh_task(dcm_cases, visit, visit_supp, cond_phex, HF_PHECODES)