from pathlib import Path

import pandas as pd


def run_gold_aggregate(**context):
    silver_file = context["ti"].xcom_pull(
        key="silver_file", task_ids="silver_transform"
    )
    if not silver_file:
        raise ValueError("Silver file path not found in XCom")

    df = pd.read_csv(silver_file)

    agg = (
        df.groupby("origin_country")
        .agg(
            total_flights=("icao24", "count"),
            avg_velocity=("velocity", "mean"),
            on_ground=("on_ground", "sum"),
        )
        .reset_index()
    )

    gold_path = Path("/opt/airflow/data/flight_etl/data/gold_data")
    gold_path.mkdir(parents=True, exist_ok=True)

    output_file = gold_path / f"flights_gold_{context['ds_nodash']}.csv"
    agg.to_csv(output_file, index=False)

    context["ti"].xcom_push(key="gold_file", value=str(output_file))
