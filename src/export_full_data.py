import os


def save_full_dataset(df, output_path="output/nasa_logs.parquet"):

    os.makedirs("output", exist_ok=True)

    pandas_df = df.toPandas()

    pandas_df.to_parquet(
        output_path,
        engine="pyarrow",
        index=False
    )

    print(f"\nFull dataset saved to: {output_path}")
    print(f"Total records exported: {len(pandas_df):,}")