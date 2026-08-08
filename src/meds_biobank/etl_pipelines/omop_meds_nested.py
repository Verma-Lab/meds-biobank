import pyspark.sql.functions as F
from pyspark.sql import Window

"""
This is the OMOP-meds ETL from meds_etl 0.1.3
"""

OMOP_BIRTH = 4083587
OMOP_DEATH = 4306655

def extract_events(df, table):
    """
    Convert an OMOP table into an unordered MEDS-DataSchema-LIKE table in flat format, containing all events

    Args:
        df (pyspark.sql.DataFrame):
            Desc: OMOP events table
            Schema: |person_id|concept_id|{table_name}_start_date|...
        table (str):
            Desc: OMOP table name (e.g. "visit_occurrence", "drug_exposure")

    Returns:
        events (pyspark.sql.DataFrame):
            Desc: a MEDS-DataSchema-LIKE table in flat format, containing all events
            Schema:
                |patient_id|start|concept_id|value|META_end|META_event_type|META_visit_id|META_unit|
                - patient_id: bigint
                - time: timestamp
                - code: string
                - string_value: string
                - numeric_value: float
                - META_end: timestamp
                - META_event_type: string
                - META_visit_id: long
                - META_unit: string
    """

    # all source tables
    events = df.withColumn("patient_id", F.crc32(F.col("person_id"))) # hash to patient id

    # person source table
    if table == "person":

        # get start as birthdate
        events = events.withColumn("start", F.to_timestamp(F.col("birth_datetime")))

        # get birth events
        birth_events = (
            events
            .withColumn("concept_id", F.lit(OMOP_BIRTH))
            .withColumn("META_event_type", F.lit("birth"))
            .withColumn("META_end", F.lit(None))
            .withColumn("META_visit_id", F.lit(None))
            .withColumn("META_unit", F.lit(None))
            .withColumn("value", F.lit(None))
            .select("patient_id", "start", "concept_id", "value", "META_end", "META_event_type", "META_visit_id", "META_unit")
        )

        # get demographic events: gender
        gender_events = (
            events
            .filter(F.col("gender_concept_id") != 0)
            .withColumn(
                "concept_id",
                (
                    F.when(F.col("gender_source_concept_id") != 0, F.col("gender_source_concept_id"))
                    .otherwise(F.col("gender_concept_id"))
                )
            )
            .withColumn("META_event_type", F.lit("gender"))
            .withColumn("META_end", F.lit(None))
            .withColumn("META_visit_id", F.lit(None))
            .withColumn("META_unit", F.lit(None))
            .withColumn("value", F.lit(None))
            .select("patient_id", "start", "concept_id", "value", "META_end", "META_event_type", "META_visit_id", "META_unit")
        )

        # get demographic events: race
        race_events = (
            events
            .filter(F.col("race_concept_id") != 0)
            .withColumn(
                "concept_id",
                (
                    F.when(F.col("race_source_concept_id") != 0, F.col("race_source_concept_id"))
                    .otherwise(F.col("race_concept_id"))
                )
            )
            .withColumn("META_event_type", F.lit("race"))
            .withColumn("META_end", F.lit(None))
            .withColumn("META_visit_id", F.lit(None))
            .withColumn("META_unit", F.lit(None))
            .withColumn("value", F.lit(None))
            .select("patient_id", "start", "concept_id", "value", "META_end", "META_event_type", "META_visit_id", "META_unit")
        )

        # get demographic events: ethnicity
        ethnicity_events = (
            events
            .filter(F.col("ethnicity_concept_id") != 0)
            .withColumn(
                "concept_id",
                (
                    F.when(F.col("ethnicity_source_concept_id") != 0, F.col("ethnicity_source_concept_id"))
                    .otherwise(F.col("ethnicity_concept_id"))
                )
            )
            .withColumn("META_event_type", F.lit("ethnicity"))
            .withColumn("META_end", F.lit(None))
            .withColumn("META_visit_id", F.lit(None))
            .withColumn("META_unit", F.lit(None))
            .withColumn("value", F.lit(None))
            .select("patient_id", "start", "concept_id", "value", "META_end", "META_event_type", "META_visit_id", "META_unit")
        )

        # form events
        events = birth_events.union(gender_events).union(race_events).union(ethnicity_events)
    
    # death source table
    elif table == "death":

        # get death events
        events = (
            events
            .withColumn("concept_id", F.lit(OMOP_DEATH))
            .withColumn("start", F.to_timestamp(F.col("death_date")))
            .withColumn("META_event_type", F.lit("death"))
            .withColumn("META_end", F.lit(None))
            .withColumn("META_visit_id", F.lit(None))
            .withColumn("META_unit", F.lit(None))
            .withColumn("value", F.lit(None))
            .select("patient_id", "start", "concept_id", "value", "META_end", "META_event_type", "META_visit_id", "META_unit")
        )

    # visit_occurrence source table
    elif table == "visit_occurrence":
        
        # get visit (start) events
        events = (
            events
            .withColumn("start", F.to_timestamp(F.col("visit_start_datetime")))
            .withColumn(
                "concept_id",
                (
                    F.when(F.col("visit_source_concept_id") != 0, F.col("visit_source_concept_id"))
                    .when(F.col("visit_concept_id") != 0, F.col("visit_concept_id"))
                    .otherwise(F.lit(8))
                )
            )
            .withColumn("META_event_type", F.lit("visit"))
            .withColumn("META_visit_id", F.col("visit_occurrence_id"))
            .withColumn("META_end", F.coalesce(F.col("visit_end_datetime"), F.to_timestamp(F.col("visit_end_date"))))
            .withColumn("META_unit", F.lit(None))
            .withColumn("value", F.lit(None))
            .select("patient_id", "start", "concept_id", "value", "META_end", "META_event_type", "META_visit_id", "META_unit")
        )
    
    # drug_occurrence table
    elif table == "drug_exposure":

        # get drug events
        events = (
            events
            .withColumn("start", F.coalesce(F.col("drug_exposure_start_datetime"), F.to_timestamp(F.col("drug_exposure_start_date"))))
            .withColumn(
                "concept_id",
                (
                    F.when(F.col("drug_source_concept_id") != 0, F.col("drug_source_concept_id"))
                    .otherwise(F.col("drug_concept_id"))
                )
            )
            .filter(F.col("concept_id") != 0)
            .withColumn("META_event_type", F.lit("drug"))
            .withColumn("META_visit_id", F.col("visit_occurrence_id"))
            .withColumn("META_end", F.coalesce(F.col("drug_exposure_end_datetime"), F.to_timestamp(F.col("drug_exposure_end_date"))))
            .withColumn("META_unit", F.lit(None))
            .withColumn("value", F.lit(None))
            .select("patient_id", "start", "concept_id", "value", "META_end", "META_event_type", "META_visit_id", "META_unit")
        )
    
    # condition table
    elif table == "condition_occurrence":

        # get condition events
        events = (
            events
            .withColumn("start", F.coalesce(F.col("condition_start_datetime"), F.to_timestamp(F.col("condition_start_date"))))
            .withColumn(
                "concept_id",
                (
                    F.when(F.col("condition_source_concept_id") != 0, F.col("condition_source_concept_id"))
                    .otherwise(F.col("condition_concept_id"))
                )
            )
            .filter(F.col("concept_id") != 0)
            .withColumn("META_event_type", F.lit("condition"))
            .withColumn("META_visit_id", F.col("visit_occurrence_id"))
            .withColumn("META_end", F.coalesce(F.col("condition_end_datetime"), F.to_timestamp(F.col("condition_end_date"))))
            .withColumn("META_unit", F.lit(None))
            .withColumn("value", F.lit(None))
            .select("patient_id", "start", "concept_id", "value", "META_end", "META_event_type", "META_visit_id", "META_unit")
        )
    
    # procedure table
    elif table == "procedure_occurrence":

        # get procedure events
        events = (
            events
            .withColumn("start", F.coalesce(F.col("procedure_datetime"), F.to_timestamp(F.col("procedure_date"))))
            .withColumn(
                "concept_id",
                (
                    F.when(F.col("procedure_source_concept_id") != 0, F.col("procedure_source_concept_id"))
                    .otherwise(F.col("procedure_concept_id"))
                )
            )
            .filter(F.col("concept_id") != 0)
            .withColumn("META_event_type", F.lit("procedure"))
            .withColumn("META_visit_id", F.col("visit_occurrence_id"))
            .withColumn("META_end", F.lit(None))
            .withColumn("META_unit", F.lit(None))
            .withColumn("value", F.lit(None))
            .select("patient_id", "start", "concept_id", "value", "META_end", "META_event_type", "META_visit_id", "META_unit")
        )
    
    # observation table
    elif table == "observation":

        # get observation events
        events = (
            events.withColumn("start", F.coalesce(F.col("observation_datetime"), F.to_timestamp(F.col("observation_date"))))
            .withColumn(
                "concept_id",
                (
                    F.when(F.col("observation_source_concept_id") != 0, F.col("observation_source_concept_id"))
                    .otherwise(F.col("observation_concept_id"))
                )
            )
            .filter(F.col("concept_id") != 0)
            .withColumn("value", F.coalesce(F.col("value_as_number").cast("string"), F.col("value_as_string")))
            .withColumn(
                "value",
                (
                    F.when(F.col("value").isNotNull(), F.col("value"))
                    .when(
                        (F.col("value_as_concept_id").isNotNull()) & (F.col("value_as_concept_id") != 0) & (F.col("observation_source_concept_id") == 0) & (F.col("observation_source_value") != ""),
                        F.concat(F.lit("SOURCE_CODE/"), F.col("observation_source_value"))
                    )
                    .otherwise(F.lit(None))
                )
            )
            .withColumn("META_event_type", F.lit("observation"))
            .withColumn("META_visit_id", F.col("visit_occurrence_id"))
            .withColumn("META_unit", F.col("unit_source_value"))
            .withColumn("META_end", F.lit(None))
            .select("patient_id", "start", "concept_id", "value", "META_end", "META_event_type", "META_visit_id", "META_unit")
        )

    elif table == "measurement":

        # get measurement events
        events = (
            events.withColumn("start", F.coalesce(F.col("measurement_datetime"), F.to_timestamp(F.col("measurement_date"))))
            .withColumn(
                "concept_id",
                (
                    F.when(F.col("measurement_source_concept_id") != 0, F.col("measurement_source_concept_id"))
                    .otherwise(F.col("measurement_concept_id"))
                )
            )
            .filter(F.col("concept_id") != 0)
            .withColumn("value", F.coalesce(F.col("value_as_number").cast("string"), F.col("value_source_value")))
            .withColumn(
                "value",
                (
                    F.when(F.col("value").isNotNull(), F.col("value"))
                    .when(
                        (F.col("value_as_concept_id").isNotNull()) & (F.col("value_as_concept_id") != 0) & (F.col("measurement_source_concept_id") == 0) & (F.col("measurement_source_value") != ""),
                        F.concat(F.lit("SOURCE_CODE/"), F.col("measurement_source_value"))
                    )
                    .otherwise(F.lit(None))
                )
            )
            .withColumn("META_event_type", F.lit("measurement"))
            .withColumn("META_visit_id", F.col("visit_occurrence_id"))
            .withColumn("META_unit", F.col("unit_source_value"))
            .withColumn("META_end", F.lit(None))
            .select("patient_id", "start", "concept_id", "value", "META_end", "META_event_type", "META_visit_id", "META_unit")
        )

    # undefined table
    else:
        raise Exception(f"Table {table} not supported")
    
    # cast visit id to correct type
    events = events.withColumn("META_visit_id", F.col("META_visit_id").cast("long"))
        
    return events

def gather_event_dfs(event_dfs):
    """
    Compile separate events tables into a single table
    event_dfs is a list of dfs which are events acc. schema |patient_id|start|concept_id|value|META_end|META_event_type|META_visit_id|META_unit|
    """
    # union all meds event dfs
    event_df = event_dfs[0]
    for df in event_dfs[1:]:
        event_df = event_df.unionByName(df, allowMissingColumns=True)
    return event_df

def prune_events(events):
    """
    Args:
        events (pyspark.sql.DataFrame):
            Desc: 
            Schema: |patient_id|start|concept_id|value|META_end|META_event_type|META_visit_id|META_unit|
    Returns:
        pruned_events (pyspark.sql.DataFrame):
            Desc: 
            Schema: |patient_id|start|concept_id|value|META_end|META_event_type|META_visit_id|META_unit|
    """
    # remove nones
    w = Window.partitionBy("patient_id", "concept_id", F.to_date("start"))
    pruned_events = (
        events
        .withColumn(
            "_has_nonull",
            F.max(F.col("value").isNotNull().cast("int")).over(w)
        )
        .filter(
            ~(
                (F.col("_has_nonull") == 1) & (F.col("value").isNull())
            )
        )
        .drop("_has_nonull")
    )
    
    # delta encode
    w = Window.partitionBy("patient_id", "concept_id").orderBy("start")
    pruned_events = (
        pruned_events
        .withColumn("_last_start", F.lag("start").over(w))
        .withColumn("_last_value", F.lag("value").over(w))
        .filter(
            ~(
                (F.col("value").eqNullSafe(F.col("_last_value"))) & (F.to_date(F.col("start")).eqNullSafe(F.to_date(F.col("_last_start"))))
            )
        )
        .drop("_last_start", "_last_value")
    )

    return pruned_events

def post_process_events(events, concepts):
    """
    Convert an OMOP table into an unordered MEDS DataSchema-formatted table in flat format, containing all events

    Args:
        events (pyspark.sql.DataFrame):
            Desc: a MEDS-DataSchema-LIKE table in flat format, containing all events
            Schema: |patient_id|start|concept_id|value|META_end|META_event_type|META_visit_id|META_unit|
        concepts (pyspark.sql.DataFrame):
            Desc: OMOP concepts table
            Schema: |concept_id|vocabulary_id|concept_code|...

    Returns:
        events (pyspark.sql.DataFrame):
            Desc: a MEDS-DataSchema table in flat format, containing all events
            Schema:
                |patient_id|time|code|string_value|numeric_value|metadata|
                - patient_id: bigint
                - time: timestamp
                - code: string
                - string_value: string
                - numeric_value: float
                - metadata: struct
    """

    # handle value
    events = (
        events
        .withColumn("numeric_value", F.expr("try_cast(value AS FLOAT)"))
        .withColumn("text_value", F.when(F.expr("try_cast(value AS FLOAT)").isNull(), F.col("value")).otherwise(F.lit(None)))
        .drop("value")
    )

    # handle other fields
    concept_to_code = (
        concepts
        .withColumn("code", F.concat(F.col("vocabulary_id"), F.lit("/"), F.col("concept_code")))
        .select("concept_id", "code")
    )
    events = (
        events.withColumn("time", F.col("start"))                   # time
        .join(
            concept_to_code,
            events.concept_id == concept_to_code.concept_id,
            "inner"
        ).drop(concept_to_code.concept_id)                          # code
        .withColumn(                                                # metadata
            "metadata",
            F.struct(
                F.col("META_visit_id").alias("visit_id"),
                F.col("META_unit").alias("unit"),
                F.col("META_end").alias("end")
            )
        )
        .select("patient_id", "time", "code", "text_value", "numeric_value", "metadata")
    )
        
    return events

def nest_patient(meds_events, patient_id):
    """
    Convert patient into nested meds schema and return, ordered by time
    """
    rows = (
        meds_events
        .filter(F.col("patient_id") == patient_id)
        .select(
            "time",
            F.struct(
                F.col("code"),
                F.col("metadata"),
                F.col("text_value"),
                F.col("numeric_value"),
            ).alias("measurement"),
        )
        .groupBy("time")
        .agg(F.collect_list("measurement").alias("measurements"))
        .collect()
    )

    events = sorted(
        (
            {"time": row["time"], "measurements": [m.asDict(recursive=True) for m in row["measurements"]]}
            for row in rows
        ),
        key=lambda e: e["time"],
    )

    return {"patient_id": patient_id, "static_measurements": [], "events": events}


def meds_flat_to_patients_df(meds_df):
    """
    ARGS
        - meds_df: a meds-flat 0.1.3 compliant DF
    RETURNS
        - patients_df: Spark DF with cols patient_id, static_measurements, events
                       (events is array<struct<time, measurements>>, sorted by time)
    """
    measurement_col = F.struct(
        F.col("code"),
        F.col("metadata"),
        F.col("text_value"),
        F.col("numeric_value"),
    ).alias("measurement")

    # Level 1: one row per (patient_id, start) -> array of measurement structs
    per_time = (
        meds_df
        .select("patient_id", "time", measurement_col)
        .groupBy("patient_id", "time")
        .agg(F.collect_list("measurement").alias("measurements"))
    )

    # Level 2: one row per patient_id -> array of {time, measurements} structs
    patients_df = (
        per_time
        .groupBy("patient_id")
        .agg(
            F.expr("""
                array_sort(
                    collect_list(struct(time, measurements)),
                    (a, b) -> case when a.time < b.time then -1
                                   when a.time > b.time then 1
                                   else 0 end
                )
            """).alias("events")
        )
        .withColumn("static_measurements", F.array().cast("array<string>"))
        .select("patient_id", "static_measurements", "events")
    )

    return patients_df

if __name__ == "__main__":

    # read tables
    visit_occurrence = spark.sql("SELECT * FROM visit_occurrence")
    drug_exposure = spark.sql("SELECT * FROM drug_exposure")
    concept = spark.sql("SELECT * FROM concept")

    # pack for ETL
    dfs = [visit_occurrence, drug_exposure]
    tables = ["visit_occurrence", "drug_exposure"]

    # perform ETL
    event_dfs = [extract_events(dfs[i], tables[i]) for i in range(len(dfs))]
    event_df = gather_event_dfs(event_dfs)
    pruned_event_df = prune_events(event_df)
    meds_events = post_process_events(pruned_event_df, concept)

    # visualize a patient
    patient_id = ...
    patient = nest_patient(meds_events, patient_id)
    print(patient)