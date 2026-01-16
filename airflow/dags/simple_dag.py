from datetime import datetime, timedelta

from airflow import DAG  # type: ignore
from airflow.decorators import task  # type: ignore

# Default DAG arguments
default_args = {
    "owner": "analytics_team",
    "retries": 2,
    "retry_delay": timedelta(minutes=5),
}

with DAG(
    dag_id="simple_dag_modern",
    start_date=datetime(2020, 10, 1),
    schedule="@daily",
    default_args=default_args,
    catchup=False,
    tags=["engineering"],
) as dag:

    @task
    def welcome_df():
        print("Hello world")
        return "Hello world"

    @task
    def goodnight():
        print("It is night time")
        return "It is night time"

    # Set dependencies using TaskFlow
    welcome_df() >> goodnight()
