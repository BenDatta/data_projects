from datetime import datetime, timedelta

from airflow import DAG
from airflow.operators.python import PythonOperator  # type: ignore

from data.market_api_etl.bronze_data import get_data
from data.market_api_etl.silver_transform import silver_transform
from data.market_api_etl.gold import gold_aggregate_table

default_args = {
    "owner": "analytics_team",
    "retries": 3,
    "retry_delay": timedelta(minutes=5),
}

with DAG(
    dag_id="market_api_etl",
    schedule="@daily",
    catchup=False,
    default_args=default_args,
    start_date=datetime(2020, 10, 10),
    tags=["engineering", "analytics"],
) as dag:
    bronze_data_ingest_task = PythonOperator(
        task_id="bronze_data", python_callable=get_data
    )

    silver_transform_task = PythonOperator(
        task_id="silver_transform", python_callable=silver_transform
    )

    gold_agg_task = PythonOperator(task_id="gold", python_callable=gold_aggregate_table)

    bronze_data_ingest_task >> silver_transform_task >> gold_agg_task
