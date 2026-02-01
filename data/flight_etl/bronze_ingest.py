import requests
import json
from datetime import datetime
from pathlib import Path

URL = "https://opensky-network.org/api/states/all"


def get_flight_data(**context):
    response = requests.get(URL, timeout=10)
    response.raise_for_status()

    data = response.json()

    timestamp = datetime.now().strftime("%Y%m%d%H%M%S")

    path = (
        Path("/opt/airflow/data/flight_etl/data/bronze_data")
        / f"flights_bronze_{timestamp}.json"
    )
    path.parent.mkdir(parents=True, exist_ok=True)

    with open(path, "w") as f:
        json.dump(data, f)

    context["ti"].xcom_push(key="bronze_file", value=str(path))
