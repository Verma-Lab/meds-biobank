import pyspark.sql.functions as F
from pyspark.sql import Window
from meds_biobank import concepts

"""
This is the OMOP-meds ETL from meds_etl 0.1.3
"""

OMOP_BIRTH = concepts.OMOP_BIRTH
OMOP_DEATH = concepts.OMOP_DEATH

def _extract_demographics_events(events):

    # get start as birthdate
    events = events.withColumn("start", F.to_timestamp(F.col("birth_datetime")))

    # get birth events
    birth_events = (
        events
        .withColumn("concept_id", F.lit(OMOP_BIRTH))
        .withColumn("event_type", F.lit("birth"))
        .select("patient_id", "start", "concept_id", "event_type")
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
        .withColumn("event_type", F.lit("gender"))
        .select("patient_id", "start", "concept_id", "event_type")
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
        .withColumn("event_type", F.lit("race"))
        .select("patient_id", "start", "concept_id", "event_type")
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
        .withColumn("event_type", F.lit("ethnicity"))
        .select("patient_id", "start", "concept_id", "event_type")
    )

    # form events
    events = birth_events.union(gender_events).union(race_events).union(ethnicity_events)

    return events

def _extract_death_events(events):
    
    # get death events
    events = (
        events
        .withColumn("concept_id", F.lit(OMOP_DEATH))
        .withColumn("start", F.to_timestamp(F.col("death_date")))
        .withColumn("event_type", F.lit("death"))
        .select("patient_id", "start", "concept_id", "event_type")
    )

    return events

def _extract_visit_events(events):

    # extract visit events
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
        .withColumn("event_type", F.lit("visit"))
        .withColumn("visit_id", F.col("visit_occurrence_id"))
        .withColumn("end", F.coalesce(F.col("visit_end_datetime"), F.to_timestamp(F.col("visit_end_date"))))
        .select("patient_id", "start", "concept_id", "event_type", "visit_id", "end")
    )

    return events

def _extract_drug_events(events):

    # etract drug events
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
        .withColumn("event_type", F.lit("drug"))
        .withColumn("visit_id", F.col("visit_occurrence_id"))
        .withColumn("end", F.coalesce(F.col("drug_exposure_end_datetime"), F.to_timestamp(F.col("drug_exposure_end_date"))))
        .select("patient_id", "start", "concept_id", "event_type", "visit_id", "end")
    )

    return events

def _extract_condition_events(events):
    
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
        .withColumn("event_type", F.lit("condition"))
        .withColumn("visit_id", F.col("visit_occurrence_id"))
        .withColumn("end", F.coalesce(F.col("condition_end_datetime"), F.to_timestamp(F.col("condition_end_date"))))
        .select("patient_id", "start", "concept_id", "end", "event_type", "visit_id")
    )

    return events

def _extract_procedure_events(events):
    
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
        .withColumn("event_type", F.lit("procedure"))
        .withColumn("visit_id", F.col("visit_occurrence_id"))
        .select("patient_id", "start", "concept_id", "event_type", "visit_id")
    )

    return events

def _extract_observation_events(events):
    
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
        .withColumn("event_type", F.lit("observation"))
        .withColumn("visit_id", F.col("visit_occurrence_id"))
        .withColumn("unit", F.col("unit_source_value"))
        .select("patient_id", "start", "concept_id", "value", "event_type", "visit_id", "unit")
    )

    return events

def _extract_measurement_events(events):
    
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
        .withColumn("event_type", F.lit("measurement"))
        .withColumn("visit_id", F.col("visit_occurrence_id"))
        .withColumn("unit", F.col("unit_source_value"))
        .select("patient_id", "start", "concept_id", "value", "event_type", "visit_id", "unit")
    )

    return events

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
                |patient_id|start|concept_id|value|end|event_type|visit_id|unit|
                - patient_id: bigint
                - time: timestamp
                - code: string
                - string_value: string
                - numeric_value: float
                - end: timestamp
                - event_type: string
                - visit_id: long
                - unit: string
    """

    # all source tables
    events = df.withColumn("patient_id", F.xxhash64(F.col("person_id"))) # hash to patient id

    # person source table
    if table == "person":

        # get person/demographic events
        events = _extract_demographics_events(events)
    
    # death source table
    elif table == "death":

        # get death events
        events = _extract_death_events(events)

    # visit_occurrence source table
    elif table == "visit_occurrence":
        
        # get visit (start) events
        events = _extract_visit_events(events)
    
    # drug_occurrence table
    elif table == "drug_exposure":

        # get drug events
        events = _extract_drug_events(events)
    
    # condition table
    elif table == "condition_occurrence":

        # get condition events
        events = _extract_condition_events(events)
    
    # procedure table
    elif table == "procedure_occurrence":

        # get procedure events
        events = _extract_procedure_events(events)
    
    # observation table
    elif table == "observation":

        # get observation events
        events = _extract_observation_events(events)

    elif table == "measurement":

        # get measurement events
        events = _extract_measurement_events(events)

    # undefined table
    else:
        raise Exception(f"Table {table} not supported")
    
    # cast visit id to correct type
    events = events.withColumn("visit_id", F.col("visit_id").cast("long"))

    # catch missing columns
    for col in ["end", "visit_id", "unit", "value"]:
        if col not in events.columns:
            events = events.withColumn(col, F.lit(None))
        
    return events

def gather_event_dfs(event_dfs):
    """
    Compile separate events tables into a single table
    event_dfs is a list of dfs which are events acc. schema |patient_id|start|concept_id|value|end|event_type|visit_id|unit|
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
            Schema: |patient_id|start|concept_id|value|end|event_type|visit_id|unit|
    Returns:
        pruned_events (pyspark.sql.DataFrame):
            Desc: 
            Schema: |patient_id|start|concept_id|value|end|event_type|visit_id|unit|
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
                F.col("visit_id").alias("visit_id"),
                F.col("unit").alias("unit"),
                F.col("end").alias("end")
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