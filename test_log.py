from pyspark.sql import SparkSession

spark = (
    SparkSession.builder.appName("TestLog").getOrCreate()
)

log_df = spark.read.text("data/access.log")

log_df.show(truncate=False)
print("Total number of log entrie:", log_df.count())