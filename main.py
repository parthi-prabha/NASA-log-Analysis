from src.spark_session import create_spark_session
from src.parser import parse_logs
from src.analytics import run_analytics
from src.export_full_data import save_full_dataset


def main():

    spark = create_spark_session()

    print("\n========== READING NASA LOG ==========")

    log_df = spark.read.text("data/access.log")

    print("Raw log count:", log_df.count())

    print("\n========== PARSING LOG ==========")

    parsed_df = parse_logs(log_df)

    parsed_df.cache()

    print("\n========== PARSED DATA ==========")

    parsed_df.show(10, truncate=False)

    print("\n========== SCHEMA ==========")

    parsed_df.printSchema()

    print("\n========== TOTAL PARSED RECORDS ==========")

    print(parsed_df.count())

    print("\n========== SAVING FULL DATASET ==========")

    save_full_dataset(parsed_df)

    print("\n========== RUNNING ANALYTICS ==========")

    run_analytics(parsed_df)

    spark.stop()


if __name__ == "__main__":
    main()