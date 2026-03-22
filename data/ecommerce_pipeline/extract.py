import os

import pandas as pd


def extract_data(file_path: str, target_fields: list):

    if not os.path.exists(file_path):
        print(f"CSV file not found: {file_path}")
        return []

    df = pd.read_csv(file_path)
    df = df[target_fields]
    data = [tuple(row) for row in df.to_numpy()]
    print(f"Extracted {len(data)} rows from CSV")
    return data
