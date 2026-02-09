import pandas as pd
from pathlib import Path


def gold_aggregate_table(**context):
    ti = context["ti"]

    silver_file = ti.xcom_pull(key="silver_file", task_ids="silver_transform")
    if not silver_file:
        raise ValueError("Could not pull silver_file from XCom (silver_transform)")

    output_dir = Path("/opt/airflow/data/market_api_etl/data/gold_table")
    output_dir.mkdir(parents=True, exist_ok=True)

    df = pd.read_csv(silver_file)

    # Gold aggregation monthly
    gold_agg_price_monthly = (
        df.groupby("month_name", observed=True)[["open", "close", "high", "low"]]
        .mean()
        .round(2)
        .rename(
            columns={
                "open": "avg_open_price",
                "high": "avg_high_price",
                "low": "avg_low_price",
                "close": "avg_close_price",
            }
        )
        .sort_values(by=["avg_open_price", "avg_high_price"], ascending=False)
    )

    # Avg intraday volatility by day of week
    gold_avg_day_volatility = (
        df.groupby("day_name", observed=True)[["intraday_volatility_pct"]]
        .mean()
        .round(2)
        .rename(columns={"intraday_volatility_pct": "avg_intraday_volatility_pct"})
        .sort_values(by="avg_intraday_volatility_pct", ascending=False)
    )

    # Avg monthly volume
    gold_avg_month_vol = (
        df.groupby("month_name", observed=True)[["volume"]]
        .mean()
        .round(0)
        .rename(columns={"volume": "avg_volume"})
        .reset_index()
        .rename(columns={"month_name": "month"})
    )

    # Write outputs
    p1 = output_dir / "gold_agg_price_monthly.csv"
    p2 = output_dir / "gold_avg_day_volatility.csv"
    p3 = output_dir / "gold_avg_month_vol.csv"

    gold_agg_price_monthly.to_csv(p1)
    gold_avg_day_volatility.to_csv(p2)
    gold_avg_month_vol.to_csv(p3, index=False)

    ti.xcom_push(key="gold_files", value=[str(p1), str(p2), str(p3)])

    print(f"✅ All tables saved to {output_dir}")
    return [str(p1), str(p2), str(p3)]
