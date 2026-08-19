import pyspark.sql.functions as F
from pyspark.sql import Window
import json
import os

class Ontology():

    def __init__(self):
        self.codes = None
        self.factors = None
        self.event_types = None
        self.qualifiers = None
        self.bins = None
        self.units = None
        self.code_to_event_type = None
        self.code_to_name = None
        self.code_to_qualifiers = None
        self.code_to_factors = None
        self.code_to_unit = None
        self.code_to_bin_ranges = None
        self.rollup_map = {}

    def compute_concept_ontology(self, events, concept_schema, qualifiers=None):
        """
        Args:
            events (pyspark.sql.DataFrame): |patient_id|code|time|end|numeric_value|text_value|unit|event_type|visit_id|
            concept_schema (pyspark.sql.DataFrame): |code|name|ancestors
            qualifications (pyspark.sql.DataFrame): |code|qualifier|source|
        Compute/Set:
            - codes <List<code>>
            - event_types <List<event_type>>
            - qualifiers <List<qualifier>>
            - bins <List<bin>>
            - units <List<unit>>
            - code_to_event_type <Map<code, event_type>>
            - code_to_name <Map<code, name>>
            - code_to_qualifiers <Map<code, wualifier>>
            - code_to_unit <Map<code, unit>>
        """

        # list of structures
        sets = ["codes", "factors", "event_types", "qualifiers", "bins", "units"]
        maps = ["code_to_event_type", "code_to_name", "code_to_qualifiers", "code_to_factors", "code_to_unit"]

        try:

            # init maps
            for struct in sets:
                setattr(self, struct, set())
            for struct in maps:
                setattr(self, struct, {})

            # build flat lists
            self.codes = {row["code"] for row in concept_schema.select("code").distinct().collect()}
            for row in concept_schema.collect():
                if row["factors"] is not None:
                    self.factors.update(set(row["factors"]))
            self.event_types = {row["event_type"] for row in events.select("event_type").distinct().collect()}
            if qualifiers is not None:
                self.qualifiers = {row["qualifier"] for row in qualifiers.select("qualifier").distinct().collect()}
            self.bins = {f"bin_{i}" for i in range(11)}
            self.units = {row["unit"] for row in events.filter(F.col("event_type") == "measurement").select("unit").distinct().collect()}

            # build maps
            code_to_event_type_df = events.groupBy("code").agg(F.first("event_type").alias("event_type"))
            self.code_to_event_type = {row["code"]:row["event_type"] for row in code_to_event_type_df.collect()}
            self.code_to_name = {row["code"]:row["name"] for row in concept_schema.collect()}
            if qualifiers:
                code_to_qualifiers_df = qualifiers.groupBy("code").agg(F.collect_set("qualifier").alias("qualifiers"))
                self.code_to_qualifiers = {row["code"]: list(row["qualifiers"]) for row in code_to_qualifiers_df.collect()}
            code_to_unit_df = events.filter(F.col("event_type") == "measurement").groupBy("code").agg(F.first("unit").alias("unit"))
            self.code_to_unit = {row["code"]:row["unit"] for row in code_to_unit_df.collect()}

            # build code_to_factors
            self.code_to_factors = {row["code"]: list((row["factors"] or [])) for row in concept_schema.select("code", "factors").collect()}
        
        except Exception:

            # de-init maps and then raise error
            for struct in sets + maps:
                setattr(self, struct, None)
            raise
    
    def bin_measurements(self, events):
        """
        Args:
            events (pyspark.sql.DataFrame): |patient_id|code|time|end|numeric_value|text_value|unit|event_type|visit_id|
        Compute:
            - code_to_bin_ranges <Map<code, Map<bin, Map<min, max>>>>
        """

        try:

            # init map
            self.code_to_bin_ranges = {}

            # filter to only measurements
            msmt = events.filter(F.col("event_type") == "measurement")

            # replace all negative measurements with zero
            msmt = msmt.withColumn(
                "numeric_value",
                F.when(F.col("numeric_value") < 0, 0).otherwise(F.col("numeric_value"))
            )

            # handle all-zero measurement concepts in labs and vitals (erase value)
            w = Window.partitionBy("code")
            msmt = msmt.withColumn(
                "is_homogeneous",
                F.max("numeric_value").over(w) == F.min("numeric_value").over(w)
            ).withColumn(
                "numeric_value",
                F.when(F.col("is_homogeneous"), F.lit(None)).otherwise(F.col("numeric_value"))
            ).drop("is_homogeneous")

            # filter out nulls and zeros
            msmt = msmt.filter((F.col("numeric_value").isNotNull()) & (F.col("numeric_value") != 0))

            # transform values via log1p
            msmt = msmt.withColumn("numeric_value", F.log1p(F.col("numeric_value")))

            # bucketize
            w_ntile = Window.partitionBy("code").orderBy("numeric_value")
            msmt = msmt.withColumn("decile_value", F.ntile(10).over(w_ntile))  # 1-10

            # bin: value range mapping, keyed on code+event_type+decile then convert to nested dict domain -> decile -> min/max
            mapping = (
                msmt
                .groupBy("code", "decile_value")
                .agg(F.min("numeric_value").alias("min_value"), F.max("numeric_value").alias("max_value"))
                .orderBy("code", "decile_value")
            )
            self.code_to_bin_ranges = {}
            for row in mapping.collect():
                self.code_to_bin_ranges.setdefault(row["code"], {})[row["decile_value"]] = {
                    "min": row["min_value"],
                    "max": row["max_value"],
                }
        except Exception:

            # de-init map
            self.code_to_bin_ranges = None

            # error
            raise
    
    def rollup_concepts(self, events, concept_schema, threshold=0.01):
        """
        Args:
            events (pyspark.sql.DataFrame): |patient_id|code|time|end|numeric_value|text_value|decile_value|unit|event_type|visit_id|
        Compute:
            - rollup_map <Map<code, ancestor>>
        """

        try:
            # re-init map
            self.rollup_map = {}

            # count frequency of every occurent concept wrt patient id
            cf = events.filter(F.col("event_type") != "measurement").groupBy("code").agg((F.countDistinct("patient_id") / events.select("patient_id").distinct().count()).alias("prop")) # code, prop (percent of patients with this code)

            # get list of concepts that are below threshold: these will be dropped from the ontology
            below_thresh = [row["code"] for row in cf.filter(F.col("prop") < threshold).select("code").collect()]

            # drop code from codes if below thresh (remains in other ontology fields)
            for bt in below_thresh:
                self.codes.discard(bt)

            # explode concept schema to have unique row for each code, ancestor
            cs_exp = concept_schema.select(
                "code",
                F.explode("ancestors").alias("pair")
            ).select(
                "code",
                F.col("pair.ancestor_concept_id").alias("ancestor"),
                F.col("pair.min_levels_of_separation").alias("distance")
            ) # code, ancestor, distance

            # build df of code, code_prop, ancestor, ancestor_prop, distance via joining code proportions
            # (project cf into two distinctly-named frames first, rather than joining
            # the same cf object into cs_exp twice, to avoid an ambiguous self-join)
            code_freq = cf.select(F.col("code"), F.col("prop").alias("code_prop"))
            ancestor_freq = cf.select(F.col("code").alias("ancestor"), F.col("prop").alias("ancestor_prop"))
            cs_exp = cs_exp.join(code_freq, on="code", how="inner")
            cs_exp = cs_exp.join(ancestor_freq, on="ancestor", how="inner")

            # filter for rows where code is below thresh and ancestor is above
            cs_exp = cs_exp.filter(
                (F.col("code_prop") < threshold) & (F.col("ancestor_prop") >= threshold)
            )

            # for each such code, keep only the most specific (nearest) ancestor that is above thresh
            rank_w = Window.partitionBy("code").orderBy(F.asc("distance"))
            cs_exp = cs_exp.withColumn("rank", F.row_number().over(rank_w))
            cs_exp = cs_exp.filter(F.col("rank") == 1).drop("rank")
            self.rollup_map = {row["code"]: row["ancestor"] for row in cs_exp.collect()}
        
        except Exception:

            # re-init map
            self.rollup_map = {}
            raise
    
    def load_from_disk(self, ontology_data_dir, overwrite=True):
        """
        Load ontology from disk
        """

        # guard against path not exists
        if not os.path.exists(ontology_data_dir):
            raise Exception(f"Error: ontologies.load_from_disk: Provided ontology_data_dir {ontology_data_dir} does not exist yet.")

        # list of structures
        sets = ["codes", "factors", "event_types", "qualifiers", "bins", "units"]
        maps = ["code_to_event_type", "code_to_name", "code_to_qualifiers", "code_to_factors", "code_to_unit", "code_to_bin_ranges", "rollup_map"]

        # guard against overwrite false and ontology already read in
        if not overwrite:
            for struct in sets + maps:
                if struct == "rollup_map":
                    if getattr(self, struct) != {}:
                        raise Exception(f"Error: ontologies.load_from_disk: Overwrite set to False but {struct} already loaded.")
                    continue
                if getattr(self, struct) is not None:
                    raise Exception(f"Error: ontologies.load_from_disk: Overwrite set to False but {struct} already loaded.")

        # guard against structure files do not exist
        for struct in sets + maps:
            if not os.path.exists(os.path.join(ontology_data_dir, struct + ".json")):
                raise Exception(f'Error: ontologies.load_from_disk: File {struct + ".json"} does not exist to read from.')
        
        # if we get here, init maps
        for struct in sets:
            setattr(self, struct, set())
        for struct in maps:
            setattr(self, struct, {})
        
        # try to read
        try:
            
            # read sets
            for struct in sets:
                path = os.path.join(ontology_data_dir, f"{struct}.json")
                with open(path, "r") as file:
                    setattr(self, struct, set(json.load(file)))
            
            # read maps
            for struct in maps:
                path = os.path.join(ontology_data_dir, f"{struct}.json")
                with open(path, "r") as file:
                    raw = json.load(file)
                if struct == "code_to_bin_ranges":
                    raw = {int(k): {int(dk): dv for dk, dv in v.items()} for k, v in raw.items()}
                else:
                    raw = {int(k): v for k, v in raw.items()}
                setattr(self, struct, raw)
        
        except Exception:

            # deinit sets and maps then raise error
            for struct in sets + maps:
                if struct == "rollup_map":
                    setattr(self, struct, {})
                    continue
                setattr(self, struct, None)
            raise
    
    # TEST: measurements were not rolled up
    # TEST: every target of rollup map is in self.codes

    
    def save_to_disk(self, ontology_data_dir, overwrite=True):
        """
        Save ontology to disk
        """
        
        # guard against directory does not exist
        if not os.path.exists(ontology_data_dir):
            raise Exception(f"Error: ontologies.save_to_disk: Provided ontology_data_dir {ontology_data_dir} does not exist yet.")

        # list of structures
        sets = ["codes", "factors", "event_types", "qualifiers", "bins", "units"
        ]
        maps = [
            "code_to_event_type", "code_to_name", "code_to_qualifiers", "code_to_factors", "code_to_unit", "code_to_bin_ranges", "rollup_map"
        ]

        # guard against one of the ontology fields is None
        for struct in sets + maps:
            if getattr(self, struct) is None:
                raise Exception(f"Error: ontologies.save_to_disk: Required ontology field {struct} is unloaded (=None).")

        # guard against overwrite set to False and files already exist
        if not overwrite:
            for struct in sets + maps:
                if os.path.exists(os.path.join(ontology_data_dir, struct + ".json")):
                    raise Exception(f'Error: ontologies.save_to_disk: Overwrite set to False but {struct + ".json"} exists already.')

        # try to write
        for struct in sets:
            path = os.path.join(ontology_data_dir, struct + ".json")
            with open(path, "w") as file:
                json.dump(list(getattr(self, struct)), file, indent=4)
        for struct in maps:
            path = os.path.join(ontology_data_dir, struct + ".json")
            with open(path, "w") as file:
                json.dump(getattr(self, struct), file, indent=4)
        
        
if __name__ == "__main__":

    # imports
    from pyspark.sql import SparkSession
    from dotenv import load_dotenv
    from pathlib import Path
    import os

    # init spark session
    spark = (
        SparkSession.builder
        .master("local[2]")
        .appName("meds-biobank:ontology")
        .config("spark.sql.shuffle.partitions", "2")
        .getOrCreate()
    )

    # read concept, concept_ancestor, qualifications, and events
    load_dotenv()
    REPO_ROOT = Path(__file__).resolve().parents[3]
    meds_data_dir = REPO_ROOT / os.environ["MEDS_DATA_DIR"] / "generated-standard"
    events_path = meds_data_dir / "meds_events.parquet"
    concept_schema_path = meds_data_dir / "meds_concept_schema.parquet"
    events = spark.read.parquet(str(events_path))
    concept_schema = spark.read.parquet(str(concept_schema_path))

    # set dirname
    ontology_data_dir = REPO_ROOT / os.environ["ONTOLOGY_DATA_DIR"] / "generated-standard-rolled"

    # create ontology object, fit, and save
    ontology = Ontology()
    ontology.compute_concept_ontology(events, concept_schema)
    ontology.bin_measurements(events)
    ontology.rollup_concepts(events, concept_schema)
    ontology.save_to_disk(str(ontology_data_dir), overwrite=True)

    # load saved ontology object
    new_ontology = Ontology()
    new_ontology.load_from_disk(str(ontology_data_dir), overwrite=False)