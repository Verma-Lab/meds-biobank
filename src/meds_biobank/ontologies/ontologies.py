import pyspark.sql.functions as F
from pyspark.sql import Window

class Ontology():
    """
    Stores:
        ** concept ontology **
            - code_to_domain: maps code to domain
            - code_to_name: maps code to name
            - code_to_qualifiers: maps code to qualifiers
            - code_ancestor: maps code to immediate ancestor codes
        ** measurement ontology **
            - domain_to_decile_ranges: maps domain and decile bin d0-10 to min/max
            - domain_to_unit: maps domain to unit used for decile bins
    """
    def __init__(self):
        pass

    def compute_concept_ontology(self, concept, concept_ancestor, qualifications, events):
        """
        Args:
            concept (pyspark.sql.DataFrame): |concept_id|metadata|
            concept_ancestor (pyspark.sql.DataFrame): |ancestor_concept_id|descendant_concept_id|min_levels_of_separation|max_levels_of_separation|
            qualifications (pyspark.sql.DataFrame): |code|qualification|source|
            events (pyspark.sql.DataFrame): |patient_id|code|time|end|numeric_value|text_value|decile_value|unit|event_type|visit_id|
        Compute:
            - code_to_domain: maps code to domain
            - code_to_name: maps code to name
            - code_to_qualifiers: maps code to qualifiers
            - code_ancestor: maps code to immediate ancestor codes
        """

        # compute concept ontology
        self.code_to_name = {row["concept_id"]: row["concept_name"] for row in concept.select("concept_id", "concept_name").collect()}
        qualifications_temp = qualifications.withColumn("temp", F.concat(F.col("source"), F.lit("/"), F.col("qualification")))
        qualifications_temp = qualifications_temp.groupBy("code").agg(F.collect_list("temp").alias("temp"))
        self.code_to_qualifiers = {row["code"]: list(row["temp"]) for row in qualifications_temp.select("code", "temp")}
        ancestors_temp = concept_ancestor.drop(F.col("max_levels_of_separation")).filter(F.col("min_levels_of_separation") == 1)
        ancestors_temp = ancestors_temp.groupBy(F.col("descendant_concept_id")).agg(F.collect_list("ancestor_concept_id").alias("parents"))
        self.code_to_ancestors = {row["descendant_concept_id"]: list(row["parents"]) for row in ancestors_temp.collect()}
        events_temp = events.groupBy("code").agg(F.first("event_type").alias("domain"))
        events_comp = concept.groupBy("concept_id").agg(F.lower(F.first("domain_id")))
        events_comp = events_comp.withColumnRenamed("concept_id", "code").withColumnRenamed("domain_id", "domain")
        events_temp = events_temp.unionByName(
            events_comp.join(events_temp, on="code", how="leftanti")
        )
        self.code_to_domain = {row["code"]: row["domain"] for row in events_temp.collect()}
    
    def bin_measurements(self, events):
        """
        Args:
            events (pyspark.sql.DataFrame): |patient_id|code|time|end|numeric_value|text_value|decile_value|unit|event_type|visit_id|
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
    
    def rollup_concepts(self, events):
        pass
    