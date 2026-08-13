import pyspark.sql.functions as F
from pyspark.sql import Window
import json
import os
from meds_biobank import concepts

CUSTOM_CONCEPTS = concepts.CUSTOM_CONCEPTS

class Ontology():
    
    # TODO: rework given upstream changes to standardization/etl, move codes to a global config file

    def __init__(self):
        self.SPECIAL_CODES = CUSTOM_CONCEPTS
        self.codes = None # e.g. 1203, 1407
        self.event_types = None # e.g. measurement, drug, observation
        self.qualifiers = None # e.g. phecodes/cardiomyopathy, ATC/class
        self.bins = None # vals: d0-d10
        self.units = None
        self.code_to_event_type = None # e.g. 1203: condition, 1407: labs_albumin
        self.code_to_name = None # e.g. 1203: myocardial infarction
        self.code_to_qualifiers = None # e.g. 1203: [phecodes/cardiomyopathy, ATC/class]
        self.code_to_ancestors = None # e.g. 1203: [1252, 242, 197]
        self.code_to_unit = None # e.g. labs_albumin: %
        self.code_to_bin_ranges = None # e.g. labs_albumin: d1: (min: x, max: x')
        self.rollup_map = None # e.g. 10454: 10234123

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
            - code_to_ancestors <Map<code, List<Tuple<ancestor, distance>>>>
            - code_to_unit <Map<code, unit>>
        """

        try:

            # init maps
            self.codes = {}
            self.event_types = {}
            self.qualifiers = {}
            self.bins = {}
            self.units = {}
            self.code_to_event_type = {}
            self.code_to_name = {}
            self.code_to_qualifiers = {}
            self.code_to_ancestors = {}
            self.code_to_unit = {}

            # build flat lists
            self.codes = [row["code"] for row in concept_schema.select("code").distinct().collect()]
            self.event_types = [row["event_type"] for row in events.select("event_type").distinct().collect()]
            if qualifiers is not None:
                self.qualifiers = [row["qualifier"] for row in qualifiers.select("qualifier").distinct().collect()]
            self.bins = [f"bin-{i}" for i in range(11)]
            self.units = [row["unit"] for row in events.filter(F.col("event_type") == "measurement").select("unit").distinct().collect()]

            # build maps
            code_to_event_type_df = events.groupBy("code").agg(F.first("event_type").alias("event_type"))
            self.code_to_event_type = {row["code"]:row["event_type"] for row in code_to_event_type_df.collect()}
            self.code_to_name = {row["code"]:row["name"] for row in concept_schema.collect()}
            if qualifiers:
                code_to_qualifiers_df = qualifiers.groupBy("code").agg(F.collect_set("qualifier").alias("qualifiers"))
                self.code_to_qualifiers = {row["code"]: list(row["qualifiers"]) for row in code_to_qualifiers_df.collect()}
            self.code_to_ancestors = {
                row["code"]: (
                    [(a["ancestor_concept_id"], a["min_levels_of_separation"]) for a in row["ancestors"]]
                    if row["ancestors"] is not None else []
                )
                for row in concept_schema.collect()
            }
            code_to_unit_df = events.filter(F.col("event_type") == "measurement").groupBy("code").agg(F.first("unit").alias("unit"))
            self.code_to_unit = {row["code"]:row["unit"] for row in code_to_unit_df.collect()}
        
        except Exception:

            # de-init maps
            self.codes = None
            self.event_types = None
            self.qualifiers = None
            self.bins = None
            self.units = None
            self.code_to_event_type = None
            self.code_to_name = None
            self.code_to_qualifiers = None
            self.code_to_ancestors = None
            self.code_to_unit = None

            # error
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
            # init map
            self.rollup_map = {}

            # count frequency of every occurent concept wrt patient id
            cf = events.groupBy("code").agg((F.countDistinct("patient_id") / events.select("patient_id").distinct().count()).alias("prop")) # code, prop (percent of patients with this code)

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
            cs_exp = cs_exp.join(
                cf,
                cs_exp.code == cf.code,
                "inner"
            ).drop(cf.code).withColumnRenamed("prop", "code_prop")
            cs_exp = cs_exp.join(
                cf,
                cs_exp.ancestor == cf.code,
                "inner"
            ).drop(cf.code).withColumnRenamed("prop", "ancestor_prop")

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
            self.rollup_map = None
            raise
    
    def load_from_disk(self, ontology_data_dir, overwrite=True):
        """
        Load ontology from disk
        """

        # guard against path not exists
        if not os.path.exists(ontology_data_dir):
            raise Exception(f"Error: ontologies.load_from_disk: Provided ontology_data_dir {ontology_data_dir} does not exist yet.")

        # list of structures
        structures = [
            "codes",
            "event_types",
            "qualifiers",
            "bins",
            "units",
            "code_to_event_type",
            "code_to_name",
            "code_to_qualifiers",
            "code_to_ancestors",
            "code_to_unit",
            "code_to_bin_ranges",
            "rollup_map"
        ]

        # guard against overwrite false and ontology already read in
        if not overwrite:
            for struct in structures:
                if getattr(self, struct) is not None:
                    raise Exception(f"Error: ontologies.load_from_disk: Overwrite set to False but {struct} already loaded.")

        # guard against structure files do not exist
        for struct in structures:
            if not os.path.exists(os.path.join(ontology_data_dir, struct + ".json")):
                raise Exception(f'Error: ontologies.load_from_disk: File {struct + ".json"} does not exist to read from.')
        
        # if we get here, init maps
        self.codes = {}
        self.event_types = {}
        self.qualifiers = {}
        self.bins = {}
        self.units = {}
        self.code_to_event_type = {}
        self.code_to_name = {}
        self.code_to_qualifiers = {}
        self.code_to_ancestors = {}
        self.code_to_unit = {}
        self.code_to_bin_ranges = {}
        self.rollup_map = {}
        
        # try to read
        try:
            
            for struct in structures:
                path = os.path.join(ontology_data_dir, f"{struct}.json")
                with open(path, "r") as file:
                    setattr(self, struct, json.load(file))
        
        except Exception:

            # de-init maps
            self.codes = None
            self.event_types = None
            self.qualifiers = None
            self.bins = None
            self.units = None
            self.code_to_event_type = None
            self.code_to_name = None
            self.code_to_qualifiers = None
            self.code_to_ancestors = None
            self.code_to_unit = None
            self.code_to_bin_ranges = None
            self.rollup_map = None
            raise

    
    def save_to_disk(self, ontology_data_dir, overwrite=True):
        """
        Save ontology to disk
        """
        
        # guard against directory does not exist
        if not os.path.exists(ontology_data_dir):
            raise Exception(f"Error: ontologies.save_to_disk: Provided ontology_data_dir {ontology_data_dir} does not exist yet.")

        # list of structures
        structures = [
            "codes",
            "event_types",
            "qualifiers",
            "bins",
            "units",
            "code_to_event_type",
            "code_to_name",
            "code_to_qualifiers",
            "code_to_ancestors",
            "code_to_unit",
            "code_to_bin_ranges",
            "rollup_map"
        ]

        # guard against one of the ontology fields is None
        for struct in structures:
            if getattr(self, struct) is None:
                raise Exception(f"Error: ontologies.save_to_disk: Required ontology field {struct} is unloaded (=None).")

        # guard against overwrite set to False and files already exist
        if not overwrite:
            for struct in structures:
                if os.path.exists(os.path.join(ontology_data_dir, struct + ".json")):
                    raise Exception(f'Error: ontologies.save_to_disk: Overwrite set to False but {struct + ".json"} exists already.')

        # try to write
        for struct in structures:
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
        .appName("meds-ontology")
        .config("spark.sql.shuffle.partitions", "2")
        .getOrCreate()
    )

    # read concept, concept_ancestor, qualifications, and events
    load_dotenv()
    REPO_ROOT = Path(__file__).resolve().parents[3]
    meds_data_dir = REPO_ROOT / os.environ["MEDS_DATA_DIR"]
    events_path = meds_data_dir / "meds.csv"
    concept_schema_path = meds_data_dir / "concept_schema.csv"
    events = spark.read.csv(str(events_path), header=True, inferSchema=True)
    concept_schema = spark.read.csv(str(meds_data_dir / "concept.csv"), header=True, inferSchema=True)

    # set dirname
    ontology_data_dir = REPO_ROOT / os.environ["ONTOLOGY_DATA_DIR"]

    # create ontology object, fit, and save
    ontology = Ontology()
    ontology.compute_concept_ontology(events, concept_schema)
    ontology.bin_measurements(events)
    ontology.rollup_concepts(events, concept_schema)
    ontology.save_to_disk(str(ontology_data_dir), overwrite=True)

    # load saved ontology object
    new_ontology = Ontology()
    new_ontology.load_from_disk(str(ontology_data_dir), overwrite=False)