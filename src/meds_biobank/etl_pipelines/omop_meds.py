import pyspark.sql.functions as F
from pyspark.sql import Window
from meds_biobank import concepts
from meds_biobank.standardizers.standardizers import standardize

OMOP_BIRTH = concepts.OMOP_BIRTH
OMOP_DEATH = concepts.OMOP_DEATH
VISIT_FLAGS = concepts.VISIT_FLAGS
SPECIAL_CONCEPTS = {k:v for k,v in concepts.VISIT_FLAGS.items()}
SPECIAL_CONCEPTS["OMOP_BIRTH"] = OMOP_BIRTH
SPECIAL_CONCEPTS["OMOP_DEATH"] = OMOP_DEATH

def extract_events(df, table, measurements_prestandardized=True):
    """
    Convert an OMOP table into an unordered MEDS-DataSchema-LIKE table in flat format, containing all events
    Reads: visit_occurrence (admission and discharge), visit_supplement, drug_exposure, condition_occurrence, observation, procedure_occurrence, measurement, labs_, vitals_, death, person

    Args:
        df (pyspark.sql.DataFrame):
            Desc: OMOP events table
            Schema: |person_id|concept_id|{table_name}_start_date|...
        table (str):
            Desc: OMOP table name (e.g. "visit_occurrence", "drug_exposure")

    Returns:
        events (pyspark.sql.DataFrame):
            Desc: a MEDS table in flat format w/ metadata expanded, containing all events
            Schema: |patient_id|code|time|end|value|unit|event_type|visit_id|
            Notes:
                code = concept_id (NOT vocabulary_id/code)
    """

    # fix source val cols
    for col in df.columns:
        if col.endswith("_source_value"):
            df = df.withColumn(col, F.col(col).cast("string"))

    # all source tables, hash patient id
    events = df.withColumn("patient_id", F.crc32(F.col("person_id").cast("string"))) # hash to patient id

    # person source table
    if table == "person":

        # get time as birthdate
        events = events.withColumn("time", F.to_timestamp(F.col("birth_datetime")))

        # get birth events
        birth_events = (
            events
            .withColumn("concept_id", F.lit(OMOP_BIRTH))
            .withColumn("event_type", F.lit("birth"))
            .select("patient_id", "time", "concept_id", "event_type")
        )

        # get demographic events: gender
        gender_events = (
            events
            .filter(F.col("gender_concept_id") != 0)
            .withColumn("concept_id", F.col("gender_concept_id"))
            .withColumn("event_type", F.lit("gender"))
            .select("patient_id", "time", "concept_id", "event_type")
        )

        # get demographic events: race
        race_events = (
            events
            .filter(F.col("race_concept_id") != 0)
            .withColumn("concept_id", F.col("race_concept_id"))
            .withColumn("event_type", F.lit("race"))
            .select("patient_id", "time", "concept_id", "event_type")
        )

        # get demographic events: ethnicity
        ethnicity_events = (
            events
            .filter(F.col("ethnicity_concept_id") != 0)
            .withColumn("concept_id", F.col("ethnicity_concept_id"))
            .withColumn("event_type", F.lit("ethnicity"))
            .select("patient_id", "time", "concept_id", "event_type")
        )

        # form events
        events = birth_events.union(gender_events).union(race_events).union(ethnicity_events)
    
    # death source table
    elif table == "death":

        # get death events
        events = (
            events
            .withColumn("concept_id", F.lit(OMOP_DEATH))
            .withColumn("time", F.to_timestamp(F.col("death_date")))
            .withColumn("event_type", F.lit("death"))
            .select("patient_id", "time", "concept_id", "event_type")
        )

    # visit_occurrence source table
    elif table == "visit_occurrence":
        
        # get visit (start) events
        admission_events = (
            events
            .withColumn("time", F.to_timestamp(F.col("visit_start_datetime")))
            .withColumn(
                "concept_id",
                (
                    F.when(F.col("visit_concept_id") != 0, F.col("visit_concept_id"))
                    .otherwise(F.lit(8))
                )
            )
            .withColumn("event_type", F.lit("visit_admission"))
            .withColumn("visit_id", F.col("visit_occurrence_id"))
            .withColumn("end", F.coalesce(F.col("visit_end_datetime"), F.to_timestamp(F.col("visit_end_date"))))
            .select("patient_id", "time", "concept_id", "end", "event_type", "visit_id")
        )

        # get visit discharge events
        discharge_events = (
            events
            .filter(F.col("discharge_to_concept_id").isNotNull())
            .filter(F.col("discharge_to_concept_id") != 0)
            .withColumnRenamed("discharge_to_concept_id", "concept_id")
            .withColumn("time", F.coalesce(F.col("visit_end_datetime"), F.to_timestamp(F.col("visit_end_date"))))
            .filter(F.col("time").isNotNull())
            .withColumn("event_type", F.lit("visit_discharge"))
            .withColumn("visit_id", F.col("visit_occurrence_id"))
            .withColumn("end", F.coalesce(F.col("visit_end_datetime"), F.to_timestamp(F.col("visit_end_date"))))
            .select("patient_id", "time", "concept_id", "end", "event_type", "visit_id")
        )

        # union
        events = admission_events.unionByName(discharge_events, allowMissingColumns=False)
    
    # handle visit occurrence supplement
    elif table == "visit_occurrence_supplement":
        flags = list(VISIT_FLAGS.keys())
        stack_args = ", ".join(f"'{c}', {c}" for c in flags)
        mapping_expr = F.create_map([F.lit(x) for pair in VISIT_FLAGS.items() for x in pair])
        events = (
            events
            .withColumn("time", F.to_timestamp(F.col("visit_start_datetime")))
            .selectExpr("*", f"stack({len(flags)}, {stack_args}) as (code, value)")
            .drop(*flags)
            .filter(F.col("value") == 1)
            .withColumn("code", mapping_expr[F.col("code")])
            .withColumn("event_type", F.lit("visit_flag"))
            .withColumn("visit_id", F.col("visit_occurrence_id"))
            .select("patient_id", "time", "code", "event_type", "visit_id")
        )
    
    # drug_occurrence table
    elif table == "drug_exposure":

        # get drug events
        events = (
            events
            .withColumn("time", F.coalesce(F.col("drug_exposure_start_datetime"), F.to_timestamp(F.col("drug_exposure_start_date"))))
            .withColumn("concept_id", F.col("drug_concept_id"))
            .filter(F.col("concept_id") != 0)
            .withColumn("event_type", F.lit("drug"))
            .withColumn("visit_id", F.col("visit_occurrence_id"))
            .withColumn("end", F.coalesce(F.col("drug_exposure_end_datetime"), F.to_timestamp(F.col("drug_exposure_end_date"))))
            .select("patient_id", "time", "concept_id", "end", "event_type", "visit_id")
        )
    
    # condition table
    elif table == "condition_occurrence":

        # get condition events
        events = (
            events
            .withColumn("time", F.coalesce(F.col("condition_start_datetime"), F.to_timestamp(F.col("condition_start_date"))))
            .withColumn("concept_id", F.col("condition_concept_id"))
            .filter(F.col("concept_id") != 0)
            .withColumn("event_type", F.lit("condition"))
            .withColumn("visit_id", F.col("visit_occurrence_id"))
            .withColumn("end", F.coalesce(F.col("condition_end_datetime"), F.to_timestamp(F.col("condition_end_date"))))
            .select("patient_id", "time", "concept_id", "end", "event_type", "visit_id")
        )
    
    # procedure table
    elif table == "procedure_occurrence":

        # get procedure events
        events = (
            events
            .withColumn("time", F.coalesce(F.col("procedure_datetime"), F.to_timestamp(F.col("procedure_date"))))
            .withColumn("concept_id", F.col("procedure_concept_id"))
            .filter(F.col("concept_id") != 0)
            .withColumn("event_type", F.lit("procedure"))
            .withColumn("visit_id", F.col("visit_occurrence_id"))
            .select("patient_id", "time", "concept_id", "event_type", "visit_id")
        )
    
    # observation table
    elif table == "observation":

        # get observation events
        events = (
            events
            .filter(F.col("observation_concept_id") != OMOP_BIRTH) # make sure we only read birth and death events from OMOP birth and death tables
            .filter(F.col("observation_concept_id") != OMOP_DEATH) # ditto
            .withColumn("time", F.coalesce(F.col("observation_datetime"), F.to_timestamp(F.col("observation_date"))))
            .withColumn("concept_id", F.col("observation_concept_id"))
            .filter(F.col("concept_id") != 0)
            .withColumn("value", F.coalesce(F.col("value_as_number").cast("string"), F.col("value_as_string")))
            .withColumn("event_type", F.lit("observation"))
            .withColumn("visit_id", F.col("visit_occurrence_id"))
            .withColumn("unit", F.col("unit_source_value"))
            .select("patient_id", "time", "concept_id", "value", "event_type", "visit_id", "unit")
        )

    elif table == "measurement":

        # dedup
        events = events.dropDuplicates(["measurement_id"])

        if not measurements_prestandardized:

            # get measurement events
            events = (
                events.withColumn("time", F.coalesce(F.col("measurement_datetime"), F.to_timestamp(F.col("measurement_date"))))
                .withColumn("concept_id", F.col("measurement_concept_id"))
                .filter(F.col("concept_id") != 0)
                .withColumn("value", F.coalesce(F.col("value_as_number").cast("string"), F.col("value_source_value"))) # basically either a float or smthg like "negative" or "-", etc.
                .withColumn("event_type", F.lit("measurement"))
                .withColumn("visit_id", F.col("visit_occurrence_id"))
                .withColumn("unit", F.col("unit_source_value"))
                .select("patient_id", "time", "concept_id", "value", "event_type", "visit_id", "unit")
            )
        
        else:

            # get measurement events, but note that unit, value, and concept_id should come from special fields
            events = (
                events.withColumn("time", F.coalesce(F.col("measurement_datetime"), F.to_timestamp(F.col("measurement_date"))))
                .withColumn("concept_id", F.col("std_concept_id"))
                .filter(F.col("concept_id") != 0)
                .withColumn("value", F.col("value_converted")) # basically either a float or smthg like "negative" or "-", etc.
                .withColumn("event_type", F.lit("measurement"))
                .withColumn("visit_id", F.col("visit_occurrence_id"))
                .withColumn("unit", F.col("unit_converted"))
                .select("patient_id", "time", "concept_id", "value", "event_type", "visit_id", "unit")
            )

    # undefined table
    else:
        raise Exception(f"Table {table} not supported")

    # catch missing cols
    catch_cols = ["value", "end", "event_type", "visit_id", "unit", "measurement_id"]
    for col in catch_cols:
        if col not in events.columns:
            events = events.withColumn(col, F.lit(None))
    
    # handle concept_id
    if "code" not in events.columns:
        events = events.withColumn("code", F.col("concept_id")).drop("concept_id")

    # cast visit id to correct type
    events = events.withColumn("visit_id", F.col("visit_id").cast("long"))
        
    return events

def gather_events(event_dfs):
    """
    Args:
        event_dfs (List<pyspark.sql.DataFrame>):
            Desc: 
            Df Schema: |patient_id|code|time|end|value|unit|event_type|visit_id|, |measurement_id| (drop later)
    Returns:
        all_events (pyspark.sql.DataFrame):
            Desc: 
            Schema: |patient_id|code|time|end|value|unit|event_type|visit_id|
    """

    # handle error case
    if len(event_dfs) == 0:
        raise Exception("ERROR: No tables to join in gather_events()")

    # handle all event dfs
    final_df = event_dfs[0]
    for df in event_dfs[1:]:
        final_df = final_df.unionByName(df, allowMissingColumns=False)
    
    return final_df


def prune_events(events):
    """
    Args:
        events (pyspark.sql.DataFrame):
            Desc: 
            Schema: |patient_id|code|time|end|value|unit|event_type|visit_id|
    Returns:
        pruned_events (pyspark.sql.DataFrame):
            Desc: 
            Schema: |patient_id|code|time|end|value|unit|event_type|visit_id|
    """
    # remove nones
    w = Window.partitionBy("patient_id", "code", F.to_date("time"))
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
    w = Window.partitionBy("patient_id", "code").orderBy("time")
    pruned_events = (
        pruned_events
        .withColumn("_last_time", F.lag("time").over(w))
        .withColumn("_last_value", F.lag("value").over(w))
        .filter(
            ~(
                (F.col("value").eqNullSafe(F.col("_last_value"))) & (F.to_date(F.col("time")).eqNullSafe(F.to_date(F.col("_last_time"))))
            )
        )
        .drop("_last_time", "_last_value")
    )

    return pruned_events

def post_process_events(events):
    """
    Args:
        events (pyspark.sql.DataFrame):
            Desc: 
            Schema: |patient_id|code|time|end|value|unit|event_type|visit_id|
    Returns:
        processed_events (pyspark.sql.DataFrame):
            Desc: 
            Schema: |patient_id|code|time|end|numeric_value|text_value|unit|event_type|visit_id|
    """
    events = ( # TODO: replace with pyspark sql pattern (avoid SQL injections)
        events
        .withColumn("numeric_value", F.expr("try_cast(value AS FLOAT)"))
        .withColumn("text_value", F.when(F.expr("try_cast(value AS FLOAT)").isNull(), F.col("value")).otherwise(F.lit(None)))
        .drop("value")
    )
    return events

def format_events(events):
    """
    Args:
        events (pyspark.sql.DataFrame):
            Desc: 
            Schema: |patient_id|code|time|end|numeric_value|text_value|unit|event_type|visit_id|
    Returns:
        formatted_events (pyspark.sql.DataFrame):
            Desc: 
            Schema: |patient_id|code|time|end|numeric_value|text_value|unit|event_type|visit_id|
    """
    return events.orderBy("patient_id", "time")

def create_concept_schema(events, concept, concept_ancestor):
    """
    Args:
        events (pyspark.sql.DataFrame):
            Desc: 
            Schema: |patient_id|code|time|end|numeric_value|text_value|unit|event_type|visit_id|
        concept (pyspark.sql.DataFrame): |concept_id|metadata|
        concept_ancestor (pyspark.sql.DataFrame): |ancestor_concept_id|descendant_concept_id|min_levels_of_separation|max_levels_of_separation|
    Returns:
        concept_schema (pyspark.sql.DataFrame):
            Desc: metadata schema for all concepts that can (do!) occur in the data
            Schema: |code|name|ancestors|factors|
            ancestors: array<struct<ancestor_concept_id, min_levels_of_separation>>
    """

    # select all concepts and names
    cn = concept.select("concept_id", "concept_name")

    # collect (ancestor, distance) pairs into one array of structs — keeps them bound
    # together so there's no risk of two separate arrays misaligning
    ca = (
        concept_ancestor
        .filter(F.col("descendant_concept_id") != F.col("ancestor_concept_id"))
        .groupBy("descendant_concept_id")
        .agg(
            F.collect_list(
                F.struct("ancestor_concept_id", "min_levels_of_separation")
            ).alias("ancestors")
        )
    )  # descendant_concept_id, ancestors: array<struct<ancestor_concept_id, min_levels_of_separation>>

    # join to get concept and name together with ancestors
    cn = cn.join(
        ca,
        cn.concept_id == ca.descendant_concept_id,
        "left"
    ).drop(ca.descendant_concept_id).withColumnRenamed("concept_name", "name") # concept_id, name, ancestors

    # join domain to descendants
    ca_domain_scoped = (
        concept_ancestor.select("ancestor_concept_id", "descendant_concept_id", "min_levels_of_separation")
        .join(
            concept.select("concept_id", "domain_id"),
            concept_ancestor.descendant_concept_id == concept.concept_id,
            "inner"
        )
        .drop(concept.concept_id)
        .withColumnRenamed("domain_id", "descendant_domain")
    ) # ancestor_concept_id, descendant_concept_id, descendant_domain

    # join domain to ancestors
    ca_domain_scoped = (
        ca_domain_scoped
        .join(
            concept.select("concept_id", "domain_id"),
            ca_domain_scoped.ancestor_concept_id == concept.concept_id,
            "inner"
        )
        .drop(concept.concept_id)
        .withColumnRenamed("domain_id", "ancestor_domain")
    ) # ancestor_concept_id, descendant_concept_id, descendant_domain, ancestor_domain

    # filter out trivial rows
    ca_domain_scoped = ca_domain_scoped.filter(F.col("ancestor_concept_id") != F.col("descendant_concept_id"))

    # filter where ancestor domain = descendant domain
    ca_domain_scoped = ca_domain_scoped.filter(F.col("descendant_domain") == F.col("ancestor_domain")).drop("descendant_domain", "ancestor_domain") # ancestor_concept_id, descendant_concept_id

    # group domain ancestors by concept (resulting arrays are ordered by specificity -> decreasing)
    cad = (
        ca_domain_scoped
        .groupBy("descendant_concept_id")
        .agg(
            F.sort_array(
                F.collect_list(F.struct("min_levels_of_separation", "ancestor_concept_id"))
            ).alias("sorted_pairs")
        )
        .withColumn(
            "factors",
            F.transform("sorted_pairs", lambda x: x["ancestor_concept_id"])
        )
        .drop("sorted_pairs")
        .withColumnRenamed("descendant_concept_id", "concept_id")
    )

    # join to concept, name, ancestor df
    cn = cn.join(
        cad,
        cn.concept_id == cad.concept_id,
        "left"
    ).drop(cad.concept_id)

    # filter results for occurring concepts only
    oc = events.select("code").distinct()
    cn = cn.join(
        oc,
        cn.concept_id == oc.code,
        "inner"
    ).drop(cn.concept_id)

    # add special concepts (e.g. visit flags)
    special_df = (
        events.sparkSession.createDataFrame(
            [(code, name) for name, code in SPECIAL_CONCEPTS.items()],
            ["code", "name"]
        )
        .withColumn("ancestors", F.lit(None).cast(cn.schema["ancestors"].dataType))
        .withColumn("factors", F.lit(None).cast(cn.schema["factors"].dataType))

    )

    # drop special concepts that already have a real row in cn (e.g. birth/death concept ids that also occur as genuine OMOP vocabulary concepts)
    special_df = special_df.join(cn.select("code"), on="code", how="left_anti")

    cn = cn.unionByName(special_df)

    return cn

def extract_tasks(
    concept,
    concept_ancestor,
    condition_occurrence,
    death,
    drug_exposure,
    measurement,
    observation,
    person,
    procedure_occurrence,
    visit_occurrence
):
    pass

if __name__ == "__main__":

    """
    create example events data for transforms and tokenizer tests from test data for etl process
    """
    
    # imports
    from pathlib import Path
    from pyspark.sql import SparkSession
    import shutil
    from dotenv import load_dotenv
    import os
    from meds_biobank.standardizers.standardizers import standardize

    # create spark session
    spark = (
        SparkSession.builder
        .master("local[2]")
        .appName("meds-biobank-etl")
        .config("spark.sql.shuffle.partitions", "2")
        .getOrCreate()
    )

    # set data dir
    load_dotenv()
    REPO_ROOT = Path(__file__).resolve().parents[3]
    data_dir = REPO_ROOT / os.environ["OMOP_DATA_DIR"]

    # read concept and concept ancestor dataframes
    concept = spark.read.csv(str(data_dir / "concept.csv"), header=True, inferSchema=True)
    concept_ancestor = spark.read.csv(str(data_dir / "concept_ancestor.csv"), header=True, inferSchema=True)

    # read and standardize measurements
    measurement = spark.read.csv(str(data_dir / "measurement.csv"), header=True, inferSchema=True)
    std_measurement = standardize(measurement, concept, concept_ancestor)

    # read data tables
    tables = [
        spark.read.csv(str(data_dir / "condition_occurrence.csv"), header=True, inferSchema=True),
        spark.read.csv(str(data_dir / "death.csv"), header=True, inferSchema=True),
        spark.read.csv(str(data_dir / "drug_exposure.csv"), header=True, inferSchema=True),
        spark.read.csv(str(data_dir / "observation.csv"), header=True, inferSchema=True),
        spark.read.csv(str(data_dir / "person.csv"), header=True, inferSchema=True),
        spark.read.csv(str(data_dir / "procedure_occurrence.csv"), header=True, inferSchema=True),
        spark.read.csv(str(data_dir / "visit_occurrence.csv"), header=True, inferSchema=True),
        std_measurement
    ]

    # record table names
    table_names = ["condition_occurrence", "death", "drug_exposure", "observation", "person", "procedure_occurrence", "visit_occurrence", "measurement"]

    # extract events
    event_dfs = []
    for table, name in zip(tables, table_names):
        result = extract_events(table, name, measurements_prestandardized=False)
        event_dfs.append(result)

    # gather events together
    gathered_events = gather_events(event_dfs)

    # prune events
    pruned_events = prune_events(gathered_events)

    # post process events
    post_processed_events = post_process_events(pruned_events)

    # format events
    formatted_events = format_events(post_processed_events)

    # compute concept schema
    concept_schema = create_concept_schema(formatted_events, concept, concept_ancestor)

    # show
    print(formatted_events.limit(25).toPandas())

    # set write dir and write
    events_write_path = REPO_ROOT / os.environ["MEDS_DATA_DIR"] / "meds_events.csv"
    formatted_events.toPandas().to_csv(str(events_write_path), index=False)
    schema_write_path = REPO_ROOT / os.environ["MEDS_DATA_DIR"] / "meds_concept_schema.csv"
    concept_schema.toPandas().to_csv(str(schema_write_path), index=False)
