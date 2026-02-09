import pandas as pd
from pathlib import Path


def silver_transform(**context):
    ti = context["ti"]
    bronze_file = ti.xcom_pull(key="bronze_file", task_ids="bronze_data")

    if not bronze_file:
        raise ValueError("Could not get bronze file for processing")

    output_path = Path(
        "/opt/airflow/data/market_api_etl/data/silver_data/silver_transformed_data.csv"
    )
    output_path.parent.mkdir(parents=True, exist_ok=True)

    df = pd.read_csv(bronze_file)

    df.columns = [c.split(". ", 1)[1] if ". " in c else c for c in df.columns]
    df = df.rename(columns={"index": "date"})

    if "date" not in df.columns:
        raise ValueError("Expected a 'date' column (or 'index' to rename to 'date')")

    df["date"] = pd.to_datetime(df["date"])
    df = df.sort_values("date")

    num_cols = ["open", "high", "low", "close", "volume"]
    for c in num_cols:
        if c in df.columns:
            df[c] = pd.to_numeric(df[c], errors="coerce")

    df["day"] = df["date"].dt.day
    df["month"] = df["date"].dt.month
    df["year"] = df["date"].dt.year
    df["day_name"] = df["date"].dt.day_name()
    df["month_name"] = df["date"].dt.month_name()

    df["daily_return_pct"] = (df["close"] - df["open"]) / df["open"] * 100
    df["intraday_volatility_pct"] = (df["high"] - df["low"]) / df["open"] * 100

    df.to_csv(output_path, index=False)

    ti.xcom_push(key="silver_file", value=str(output_path))
    return str(output_path)
