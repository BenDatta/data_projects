from datetime import datetime, timedelta

from airflow.decorators import task

from airflow import DAG

# Define settings
my_settings = {
    "owner": "analytics_team",
    "retries": 2,
    "retry_delay": timedelta(minutes=5),
}

with DAG(
    dag_id="simple_dag_modern",
    start_date=datetime(2020, 10, 1),
    schedule="@daily",
    default_args=my_settings,
    catchup=False,
    tags=["engineering"],  # Must be a list []
) as dag:

    @task
    def welcome_df():
        print("Hello world")
        return "Hello world"

    @task
    def goodnight():
        print("it is night time")
        return "it is night time"

    # In TaskFlow, you just call the functions to create dependencies
    welcome_df() >> goodnight()
