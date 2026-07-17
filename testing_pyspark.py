from pyspark.sql import SparkSession 

spark = (
    SparkSession.builder.appName("Learning Pyspark").getOrCreate()
)

log_df = spark.read.text("data/access.log")

print("Schema:")
log_df.printSchema()

print("Columns:")
print(log_df.columns)

print("First 5 rows")
log_df.show(5, truncate=False)

print("Total Experiments")
print(log_df.count())

print("no of columns : ", len(log_df.columns))