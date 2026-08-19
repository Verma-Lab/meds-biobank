from pyspark.sql import SparkSession

spark = (
    SparkSession
    .builder
    .master("local[2]")
    .appName("test")
    .config("pyspark.sql.shuffle.paritions", "2")
    .getOrCreate()
)

concept = spark.read.csv("./data/OMOP/generated-standard/concept.csv", inferSchema=True, header=True)
print(concept)