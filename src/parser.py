from pyspark.sql.functions import col, regexp_extract, when, to_timestamp, try_to_timestamp
from pyspark.sql.types import IntegerType, LongType


LOG_PATTERN = r'^(\S+) \S+ \S+ \[(.*?)\] "(\S+) (.*?) (\S+)" (\d{3}) (\d+)$'


def parse_logs(log_df):

    parsed_df = (
        log_df
        .withColumn(
            "host",
            regexp_extract(col("value"), LOG_PATTERN, 1)
        )
        .withColumn(
            "timestamp_raw",
            regexp_extract(col("value"), LOG_PATTERN, 2)
        )
        .withColumn(
            "method",
            regexp_extract(col("value"), LOG_PATTERN, 3)
        )
        .withColumn(
            "url",
            regexp_extract(col("value"), LOG_PATTERN, 4)
        )
        .withColumn(
            "protocol",
            regexp_extract(col("value"), LOG_PATTERN, 5)
        )
        .withColumn(
            "status_raw",
            regexp_extract(col("value"), LOG_PATTERN, 6)
        )
        .withColumn(
            "response_size_raw",
            regexp_extract(col("value"), LOG_PATTERN, 7)
        )
        .withColumn(
            "timestamp",
            to_timestamp(
                when(col("timestamp_raw") != "", col("timestamp_raw")),
                "dd/MMM/yyyy:HH:mm:ss Z"
            )
        )
        .withColumn(
            "status",
            when(
                col("status_raw") != "",
                col("status_raw").cast(IntegerType())
            )
        )
        .withColumn(
            "response_size",
            when(
                col("response_size_raw") != "",
                col("response_size_raw").cast(LongType())
            )
        )
    )

    return parsed_df.select(
        "host",
        "timestamp",
        "method",
        "url",
        "protocol",
        "status",
        "response_size"
    )