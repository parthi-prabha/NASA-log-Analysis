from pyspark.sql.functions import col, sum, hour, to_date


def run_analytics(df):

    results = {}

    results["status"] = (
        df.filter(col("status").isNotNull())
        .groupBy("status")
        .count()
        .orderBy("status")
    )

    results["top_urls"] = (
        df.filter(col("url") != "")
        .groupBy("url")
        .count()
        .orderBy(col("count").desc())
        .limit(10)
    )

    results["top_hosts"] = (
        df.filter(col("host") != "")
        .groupBy("host")
        .count()
        .orderBy(col("count").desc())
        .limit(10)
    )

    results["methods"] = (
        df.filter(col("method") != "")
        .groupBy("method")
        .count()
        .orderBy(col("count").desc())
    )

    results["hourly_traffic"] = (
        df.filter(col("timestamp").isNotNull())
        .groupBy(hour("timestamp").alias("hour"))
        .count()
        .orderBy("hour")
    )

    results["daily_traffic"] = (
        df.filter(col("timestamp").isNotNull())
        .groupBy(to_date("timestamp").alias("date"))
        .count()
        .orderBy("date")
    )

    results["errors"] = (
        df.filter(
            (col("status") >= 400) &
            col("status").isNotNull()
        )
        .groupBy("status")
        .count()
        .orderBy("status")
    )

    return results