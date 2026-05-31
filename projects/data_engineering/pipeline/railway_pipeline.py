from pathlib import Path

import pandas as pd

folder = Path(__file__).parent
csv_path = folder / "../data/railway.csv"


def bronze_load(filepath):
    return pd.read_csv(filepath)


def silver_transform(df):
    # Handle nulls
    df["Reason for Delay"] = df["Reason for Delay"].fillna("unknown")
    df["Railcard"] = df["Railcard"].fillna("unknown")

    # Rename columns
    df.columns = [col.replace(" ", "_").lower() for col in df.columns]

    # Convert dates
    date_cols = ["date_of_purchase", "date_of_journey"]
    for col in date_cols:
        df[col] = pd.to_datetime(df[col])

    return df


def gold_df(df):
    # 1. Location Summary
    location_summary = (
        df.groupby("departure_station")
        .agg({"price": "sum", "transaction_id": "count"})
        .sort_values(by="price", ascending=False)
        .rename(columns={"price": "total_revenue", "transaction_id": "ticket_volume"})
        .reset_index()
    )

    # 2. Payment Summary
    payment_summary = (
        df.groupby("payment_method")
        .agg(ticket_count=("transaction_id", "count"))
        .reset_index()
    )

    # 3. Refund Summary (Filtered for 'Yes')
    refund_summary = (
        df.groupby(["refund_request", "ticket_type"])
        .agg(ticket_count=("transaction_id", "count"))
        .reset_index()
    )
    refund_summary = refund_summary[refund_summary["refund_request"] == "Yes"].copy()

    total_refunds = refund_summary["ticket_count"].sum()
    refund_summary["percentage"] = (
        refund_summary["ticket_count"] / total_refunds * 100
    ).round(2)

    # Return all three as a tuple
    return location_summary, payment_summary, refund_summary


# 1. Load and Clean
raw_data = bronze_load(csv_path)
clean_data = silver_transform(raw_data)

# 2. Generate Gold Tables
loc_gold, pay_gold, ref_gold = gold_df(clean_data)

# 3. Save Results
clean_data.to_csv(folder / "../data/railway_silver.csv", index=False)
loc_gold.to_csv(folder / "../data/gold_location_revenue.csv", index=False)
pay_gold.to_csv(folder / "../data/gold_payment_summary.csv", index=False)
ref_gold.to_csv(folder / "../data/gold_refund_analysis.csv", index=False)

print("✅ Pipeline complete. Silver and 3 Gold files saved in ../data/")
