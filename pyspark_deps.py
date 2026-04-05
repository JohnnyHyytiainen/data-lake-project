from pyspark.sql import SparkSession

spark = (
    SparkSession.builder.master("local[*]").appName("github-data-lake").getOrCreate()
)

print(spark.version)
