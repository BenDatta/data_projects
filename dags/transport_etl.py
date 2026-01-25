from datetime import datetime, timedelta

import pandas as pd
from airflow import DAG  # type: ignore
from airflow.decorators import task  # type: ignore

default_args = {
    "owner": "data_analytics_team",
    "retries": 3,
    "retry_delay": timedelta(minutes=10),
}
with DAG(
    dag_id="transport_data_etl",
    start_date=datetime(2024, 1, 1),
    schedule="@hourly",
    catchup=False,
    default_args=default_args,
    tags=["data_analytics"],
) as dag:

    @task
    def extract_city_data() -> str:
        return "data/city.csv"

    @task
    def city_silver(file_path: str) -> str:
        df = pd.read_csv(file_path)
        output_path = "data/silver_data/city_silver.parquet"
        df.to_parquet(output_path, index=False)
        return output_path

    raw_data = extract_city_data()
    silver_city_dt = city_silver(raw_data)
