import pyspark.sql.functions as F
from pyspark.sql import Window
import json
import os

class Ontology():
    
    # TODO: rework given upstream changes to standardization/etl, move codes to a global config file

    def __init__(self):
        self.SPECIAL_CODES = {
            "IsHospitalAdmission": 700000001,
            "IsInpatientAdmission": 700000002,
            "IsObservation": 700000003,
            "IsEdVisit": 700000004,
            "IsOutpatientFaceToFaceVisit": 700000005,
            "IsVideoVisit": 700000007,
        }
        self.codes = None # e.g. 1203, 1407
        self.domains = None # e.g. measurement, drug, labs_albumin
        self.qualifiers = None # e.g. phecodes/cardiomyopathy, ATC/class
        self.deciles = None # vals: d0-d10
        self.code_to_domain = None # e.g. 1203: condition, 1407: labs_albumin
        self.code_to_name = None # e.g. 1203: myocardial infarction
        self.code_to_qualifiers = None # e.g. 1203: [phecodes/cardiomyopathy, ATC/class]
        self.code_to_parents = None # e.g. 1203: [1252, 242, 197]
        self.domain_to_unit = None # e.g. labs_albumin: %
        self.domain_to_decile_ranges = None # e.g. labs_albumin: d1: (min: x, max: x')
        self.rollup_map = None # e.g. 10454: 10234123

    def compute_concept_ontology(self, events, concept, concept_ancestor, qualifications=None):
        """
        Args:
            concept (pyspark.sql.DataFrame): |concept_id|metadata|
            concept_ancestor (pyspark.sql.DataFrame): |ancestor_concept_id|descendant_concept_id|min_levels_of_separation|max_levels_of_separation|
            qualifications (pyspark.sql.DataFrame): |code|qualification|source|
            events (pyspark.sql.DataFrame): |patient_id|code|time|end|numeric_value|text_value|unit|event_type|visit_id|
        Compute:
            - code_to_domain: maps code to domain
            - code_to_name: maps code to name
            - code_to_qualifiers: maps code to qualifiers
            - code_to_parents: maps code to immediate ancestor codes
        """

        # compute concept ontology
        self.codes = [row["concept_id"] for row in concept.select("concept_id").distinct().collect()] + list(self.SPECIAL_CODES.values())
        self.code_to_name = {row["concept_id"]: row["concept_name"] for row in concept.select("concept_id", "concept_name").collect()} | {v:k for k,v in self.SPECIAL_CODES.items()}
        if qualifications is not None:
            qualifications_temp = qualifications.withColumn("temp", F.concat(F.col("source"), F.lit("/"), F.col("qualification")))
            self.qualifiers = list({row["temp"] for row in qualifications_temp.select("temp").collect()})
            qualifications_temp = qualifications_temp.groupBy("code").agg(F.collect_list("temp").alias("temp"))
            self.code_to_qualifiers = {row["code"]: list(row["temp"]) for row in qualifications_temp.select("code", "temp").collect()}
        ancestors_temp = concept_ancestor.drop(F.col("max_levels_of_separation")).filter(F.col("min_levels_of_separation") == 1)
        ancestors_temp = ancestors_temp.groupBy(F.col("descendant_concept_id")).agg(F.collect_list("ancestor_concept_id").alias("parents"))
        self.code_to_parents = {row["descendant_concept_id"]: list(row["parents"]) for row in ancestors_temp.collect()}
        events_temp = events.groupBy("code").agg(F.first("event_type").alias("domain"))
        events_comp = concept.groupBy("concept_id").agg(F.lower(F.first("domain_id")).alias("domain_id"))
        events_comp = events_comp.withColumnRenamed("concept_id", "code").withColumnRenamed("domain_id", "domain")
        events_temp = events_temp.unionByName(
            events_comp.join(events_temp, on="code", how="leftanti")
        )
        self.code_to_domain = {row["code"]: row["domain"] for row in events_temp.collect()} | {v:"visit_flag" for k,v in self.SPECIAL_CODES.items()}
        self.domains = list(set(self.code_to_domain.values()))
    
    def normalize_measurements(self, events):
        # for each measurement concept, label by lab/vital and convert value and unit to standard scale if possible, fallback if not
        pass
    
    def bin_measurements(self, events):
        """
        Args:
            events (pyspark.sql.DataFrame): |patient_id|code|time|end|numeric_value|text_value|unit|event_type|visit_id|
        Compute:
            - domain_to_decile_ranges: maps domain and decile bin d0-10 to min/max
            - domain_to_unit: maps domain to unit used for decile bins
        """

        # extract labs, vitals, other
        lv = events.filter(
            F.col("event_type").startswith("labs_") | F.col("event_type").startswith("vitals_")
        )
        other = events.filter(
            ~F.col("event_type").startswith("labs_") & ~F.col("event_type").startswith("vitals_")
        )

        # compute domain to unit
        temp = lv.groupBy("event_type").agg(F.first("unit").alias("unit"))
        self.domain_to_unit = {row["event_type"]: row["unit"] for row in temp.collect()}

        # guard against misuse
        if lv.count() == 0:
            raise Exception("No labs_ or vitals_ events to map! (Are you working with non PMBB-OMOP data and haven't run etl_labs_vitals()?)")

        # replace all negative measurements with zero
        lv = lv.withColumn(
            "numeric_value",
            F.when(F.col("numeric_value") < 0, 0).otherwise(F.col("numeric_value"))
        )

        # handle all-zero measurement concepts in labs and vitals (erase value)
        w = Window.partitionBy("code")
        lv = lv.withColumn(
            "is_homogeneous",
            F.max("numeric_value").over(w) == F.min("numeric_value").over(w)
        ).withColumn(
            "numeric_value",
            F.when(F.col("is_homogeneous"), F.lit(None)).otherwise(F.col("numeric_value"))
        ).drop("is_homogeneous")

        # guard against misuse
        if lv.count() == 0:
            raise Exception("Getting rid of homogenous measures deleted all labs and vitals info")

        # transform values via log1p
        lv = lv.withColumn("numeric_value", F.log1p(F.col("numeric_value")))

        # split zero / nonzero / null so ntile only sees nonzero values
        zero_df = lv.filter(F.col("numeric_value") == 0)
        nonzero_df = lv.filter(F.col("numeric_value").isNotNull() & (F.col("numeric_value") != 0))
        null_df = lv.filter(F.col("numeric_value").isNull())

        # bucketize
        w_ntile = Window.partitionBy("event_type").orderBy("numeric_value")
        nonzero_df = nonzero_df.withColumn("decile_value", F.ntile(10).over(w_ntile))  # 1-10
        zero_df = zero_df.withColumn("decile_value", F.lit(0))
        null_df = null_df.withColumn("decile_value", F.lit(None).cast("int"))

        # rejoin
        lv = zero_df.unionByName(nonzero_df).unionByName(null_df)

        # bin -> value range mapping, keyed on code+event_type+decile
        mapping = (
            lv.filter(F.col("decile_value").isNotNull())
            .groupBy("event_type", "decile_value")
            .agg(F.min("numeric_value").alias("min_value"), F.max("numeric_value").alias("max_value"))
            .orderBy("event_type", "decile_value")
        )

        # convert to nested dict domain -> decile -> min/max
        self.domain_to_decile_ranges = {}
        for row in mapping.collect():
            self.domain_to_decile_ranges.setdefault(row["event_type"], {})[row["decile_value"]] = {
                "min": row["min_value"],
                "max": row["max_value"],
            }
        
        # set deciles
        self.deciles = [f"decile{i}" for i in range(11)]
    
    def load_from_disk(self, dirname):
        """
        Load ontology from disk
        """

        # catch error: directory does not exist
        if not os.path.exists(dirname):
            raise Exception(f"ERROR: Unable to locate path {dirname} to save ontology. Does it exist yet? (save_to_disk does not create it)")

        # catch error: one of the CORE ontology files does not exist
        paths = ["codes", "domains", "deciles", "special_codes", "code_to_domain", "code_to_name", "code_to_parents", "domain_to_unit", "domain_to_decile_ranges", "rollup_map"]
        paths = [path + ".json" for path in paths]
        for path in paths:
            if not os.path.exists(os.path.join(dirname, path)):
                raise Exception(f"ERROR: Core ontology file {path} does not exist in location {dirname}.")
        
        # if we get here, read the files
        with open(os.path.join(dirname, "codes.json"), "r") as file:
            self.codes = json.load(file)
        with open(os.path.join(dirname, "domains.json"), "r") as file:
            self.domains = json.load(file)
        with open(os.path.join(dirname, "deciles.json"), "r") as file:
            self.deciles = json.load(file)
        with open(os.path.join(dirname, "special_codes.json"), "r") as file:
            self.SPECIAL_CODES = json.load(file)
        with open(os.path.join(dirname, "code_to_domain.json"), "r") as file:
            self.code_to_domain = json.load(file)
        with open(os.path.join(dirname, "code_to_name.json"), "r") as file:
            self.code_to_name = json.load(file)
        with open(os.path.join(dirname, "code_to_parents.json"), "r") as file:
            self.code_to_parents = json.load(file)
        with open(os.path.join(dirname, "domain_to_unit.json"), "r") as file:
            self.domain_to_unit = json.load(file)
        with open(os.path.join(dirname, "domain_to_decile_ranges.json"), "r") as file:
            self.domain_to_decile_ranges = json.load(file)
        with open(os.path.join(dirname, "rollup_map.json"), "r") as file:
            self.rollup_map = json.load(file)
        
        # read qualifier files as well if they exist
        if os.path.exists(os.path.join(dirname, "qualifiers.json")):
            with open(os.path.join(dirname, "qualifiers.json"), "r") as file:
                self.qualifiers = json.load(file)
            with open(os.path.join(dirname, "code_to_qualifiers.json"), "r") as file:
                self.code_to_qualifiers = json.load(file)
    
    def save_to_disk(self, dirname, override=False):
        """
        Save ontology to disk
        """

        # catch error: directory does not exist
        if not os.path.exists(dirname):
            raise Exception(f"ERROR: Unable to locate path {dirname} to save ontology. Does it exist yet? (save_to_disk does not create it)")
        
        # catch error: files already exist when override set to False
        if not override:
            paths = ["codes", "domains", "qualifiers", "deciles", "special_codes", "code_to_domain", "code_to_name", "code_to_qualifiers", "code_to_parents", "domain_to_unit", "domain_to_decile_ranges", "rollup_map"]
            paths = [path + ".json" for path in paths]
            for path in paths:
                if os.path.exists(os.path.join(dirname, path)):
                    raise Exception(f"ERROR: File {path} already exists in location {dirname} and override was set to False.")
        
        # TODO: create helper/lib and move this
        # if we get here, save the ontology
        with open(os.path.join(dirname, "codes.json"), "w") as file:
            json.dump(self.codes, file, indent=4)
        with open(os.path.join(dirname, "domains.json"), "w") as file:
            json.dump(self.domains, file, indent=4)
        with open(os.path.join(dirname, "deciles.json"), "w") as file:
            json.dump(self.deciles, file, indent=4)
        with open(os.path.join(dirname, "special_codes.json"), "w") as file:
            json.dump(self.SPECIAL_CODES, file, indent=4)
        with open(os.path.join(dirname, "code_to_domain.json"), "w") as file:
            json.dump(self.code_to_domain, file, indent=4)
        with open(os.path.join(dirname, "code_to_name.json"), "w") as file:
            json.dump(self.code_to_name, file, indent=4)
        with open(os.path.join(dirname, "code_to_parents.json"), "w") as file:
            json.dump(self.code_to_parents, file, indent=4)
        with open(os.path.join(dirname, "domain_to_unit.json"), "w") as file:
            json.dump(self.domain_to_unit, file, indent=4)
        with open(os.path.join(dirname, "domain_to_decile_ranges.json"), "w") as file:
            json.dump(self.domain_to_decile_ranges, file, indent=4)
        with open(os.path.join(dirname, "rollup_map.json"), "w") as file:
            json.dump(self.rollup_map, file, indent=4)

        # if we have qualifiers to write, write them
        if self.qualifiers is not None:
            with open(os.path.join(dirname, "qualifiers.json"), "w") as file:
                json.dump(self.qualifiers, file, indent=4)
            with open(os.path.join(dirname, "code_to_qualifiers.json"), "w") as file:
                json.dump(self.code_to_qualifiers, file, indent=4)
    
    def rollup_concepts(self, events, concept_ancestor, threshold=0.01):
        """
        Args:
            events (pyspark.sql.DataFrame): |patient_id|code|time|end|numeric_value|text_value|decile_value|unit|event_type|visit_id|
        Notes:
            • Detect concepts below threshold wrt patients. See if there is a parent above threshold. If so, rollup. Drop concepts still below threshold.
        """

        #TODO: why not try/execpt block here???
        # catch error
        if self.code_to_domain is None:
            raise Exception("ERROR: Rollup function called prior to compute_concept_ontology.")
        elif self.domain_to_decile_ranges is None:
            raise Exception("ERROR: Rollup function called prior to bin_measurements.")

        # count raw code frequencies
        n_ppl = events.select("patient_id").distinct().count()
        code_freq = events.groupBy("code").agg((F.countDistinct("patient_id")/n_ppl).alias("freq"))

        # compute rollup map
        ca_freq = concept_ancestor.drop("max_levels_of_separation")
        ca_freq = ca_freq.join(
            code_freq.select("code", "freq"),
            concept_ancestor.ancestor_concept_id == code_freq.code,
            "inner"
        ).drop(code_freq.code).withColumnRenamed("freq", "ancestor_freq")
        ca_freq = ca_freq.join(
            code_freq.select("code", "freq"),
            ca_freq.descendant_concept_id == code_freq.code,
            "inner"
        ).drop(code_freq.code).withColumnRenamed("freq", "descendant_freq") # |ancestor_cid|ancestor_freq|descendant_cid|descendant_freq|min_lvls_of_sep
        below_thresh = ca_freq.filter(F.col("descendant_freq") < threshold)
        has_saving_ancestor = below_thresh.filter(F.col("ancestor_freq") >= threshold)
        w = Window.partitionBy("descendant_concept_id").orderBy(F.asc("min_levels_of_separation"))
        rollup_map = has_saving_ancestor.withColumn(
            "ancestor_concept_id", F.first(F.col("ancestor_concept_id")).over(w)
        ).select("descendant_concept_id", "ancestor_concept_id") # |descendant_cid|target_code|
        self.rollup_map = {row["descendant_concept_id"]: row["ancestor_concept_id"] for row in rollup_map.collect()}

        # drop codes from ontology mappings and lists if they are not above threshold or never observed: do not drop special
        special_vals = set(self.SPECIAL_CODES.values())
        code_freq_rows = code_freq.collect()
        observed_codes = {row["code"] for row in code_freq_rows}
        below_thresh_codes = {row["code"] for row in code_freq_rows if row["freq"] < threshold}
        unobserved_codes = set(self.codes) - observed_codes
        codes_to_drop = (below_thresh_codes | unobserved_codes) - special_vals
        for mapping in (self.code_to_domain, self.code_to_name, self.code_to_parents):
            for code in codes_to_drop:
                mapping.pop(code, None)
        if self.code_to_qualifiers is not None:
            for code in codes_to_drop:
                self.code_to_qualifiers.pop(code, None)
        self.codes = list(set(self.codes) - set(codes_to_drop))
        
        
if __name__ == "__main__":

    # imports
    from pyspark.sql import SparkSession

    # init spark session
    spark = (
        SparkSession.builder
        .master("local[2]")
        .appName("meds-ontology")
        .config("spark.sql.shuffle.partitions", "2")
        .getOrCreate()
    )

    #TODO: move to YAML!!!!
    # read concept, concept_ancestor, qualifications, and events
    events = spark.read.csv("/Users/zolensky/Code/meds-biobank/data/MEDS/pmbb_meds.csv", header=True, inferSchema=True)
    concept = spark.read.csv("/Users/zolensky/Code/meds-biobank/data/PMBB-OMOP/concept.csv", header=True, inferSchema=True)
    concept_ancestor = spark.read.csv("/Users/zolensky/Code/meds-biobank/data/PMBB-OMOP/concept_ancestor.csv", header=True, inferSchema=True)

    # set dirname
    dirname = "/Users/zolensky/Code/meds-biobank/data/ontologies"

    # create ontology object, fit, and save
    ontology = Ontology()
    ontology.compute_concept_ontology(events, concept, concept_ancestor)
    ontology.bin_measurements(events)
    ontology.rollup_concepts(events, concept_ancestor)
    ontology.save_to_disk(dirname, override=True)

    # load saved ontology object
    new_ontology = Ontology()
    new_ontology.load_from_disk(dirname)