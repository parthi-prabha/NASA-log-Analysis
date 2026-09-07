from pyspark.sql import SparkSession
from pyspark.sql.functions import regexp_extract, col


spark = (
    SparkSession.builder
    .appName("NASA Log Analysis")
    .getOrCreate()
)



log_df = spark.read.text("data/access.log")

print("Raw Data:")
log_df.show(5, truncate=False)


log_pattern = r'^(\S+) \S+ \S+ \[(.*?)\] "(\S+) (.*?) (\S+)" (\d{3}) (\d+)$'



parsed_df = (
    log_df
    .withColumn("host", regexp_extract(col("value"), log_pattern, 1))
    .withColumn("timestamp", regexp_extract(col("value"), log_pattern, 2))
    .withColumn("method", regexp_extract(col("value"), log_pattern, 3))
    .withColumn("url", regexp_extract(col("value"), log_pattern, 4))
    .withColumn("protocol", regexp_extract(col("value"), log_pattern, 5))
    .withColumn("status", regexp_extract(col("value"), log_pattern, 6))
    .withColumn("response_size", regexp_extract(col("value"), log_pattern, 7))
)



parsed_df = parsed_df.select(
    "host",
    "timestamp",
    "method",
    "url",
    "protocol",
    "status",
    "response_size"
)



print("Structured Data:")

parsed_df.show(10, truncate=False)


print("Schema:")

parsed_df.printSchema()

spark.stop()