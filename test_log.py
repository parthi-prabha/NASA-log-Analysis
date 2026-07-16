from pyspark.sql import SparkSession

spark = (
    SparkSession.builder.appName("TestLog").getOrCreate()
)

log_df = spark.read.text("data/access.log")

print(log_df.show())
print(log_df.printSchema())