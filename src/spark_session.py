from pyspark.sql import SparkSession


def create_spark_session():
    return (
        SparkSession.builder
        .appName("NASA Log Analysis")
        .getOrCreate()
    )