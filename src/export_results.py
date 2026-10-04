import csv
import os


def save_csv(df, path):

    os.makedirs(path, exist_ok=True)

    file_path = os.path.join(path, "data.csv")

    rows = df.collect()

    with open(file_path, "w", newline="", encoding="utf-8") as file:

        writer = csv.writer(file)

        writer.writerow(df.columns)

        for row in rows:
            writer.writerow(row)

    print(f"Saved: {file_path}")