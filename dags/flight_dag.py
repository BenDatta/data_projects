from datetime import datetime, timedelta

from airflow import DAG  # type: ignore
from airflow.operators.python import PythonOperator  # type: ignore

from data.flight_etl.bronze_ingest import get_flight_data
from data.flight_etl.gold import run_gold_aggregate
from data.flight_etl.silver_transform import silver_transform

default_args = {
    "owner": "analytics_team",
    "retries": 2,
    "retry_delay": timedelta(minutes=5),
}

with DAG(
    dag_id="flight_details_etl",
    start_date=datetime(2020, 10, 1),
    schedule="@daily",
    default_args=default_args,
    catchup=False,
    tags=["engineering"],
) as dag:
    bronze_ingest = PythonOperator(
        task_id="bronze_ingest",
        python_callable=get_flight_data,
    )

    silver_transform_task = PythonOperator(
        task_id="silver_transform",
        python_callable=silver_transform,
    )

    gold_aggregate = PythonOperator(
        task_id="gold_aggregate",
        python_callable=run_gold_aggregate,
    )

    bronze_ingest >> silver_transform_task >> gold_aggregate
