import requests
import os
import pandas as pd
from airflow.models import Variable


def get_data(**context):
    ti = context["ti"]
    ALPHA_API_KEY = Variable.get("ALPHA_API_KEY")

    if not ALPHA_API_KEY:
        raise ValueError("ALPHA_API_KEY environment variable is not set")

    url = (
        "https://www.alphavantage.co/query"
        "?function=TIME_SERIES_WEEKLY"
        "&symbol=IBM"
        f"&apikey={ALPHA_API_KEY}"
    )

    response = requests.get(url, timeout=10)
    response.raise_for_status()

    data = response.json()
    time_series = data.get("Weekly Time Series")

    if not time_series:
        raise ValueError(f"No 'Weekly Time Series' in API response: {data}")

    df = pd.DataFrame.from_dict(time_series, orient="index")
    df.reset_index(inplace=True)
    df.rename(columns={"index": "date"}, inplace=True)

    os.makedirs("/opt/airflow/data/market_api_etl/data/bronze_data", exist_ok=True)
    file_path = (
        "/opt/airflow/data/market_api_etl/data/bronze_data/bronze_data_weekly_data.csv"
    )
    df.to_csv(file_path, index=False)

    print(f"Data retrieved: {len(df)} rows")
    ti.xcom_push(key="bronze_file", value=file_path)

    return file_path
