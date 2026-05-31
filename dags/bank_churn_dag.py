from datetime import datetime, timedelta
from pathlib import Path

import pandas as pd
from airflow import DAG  # type: ignore
from airflow.decorators import task  # type: ignore
from airflow.operators.python import get_current_context  # type: ignore

STAGING_BASE = Path("/opt/airflow/data/bank_churn_etl/staging")

default_args = {
    "owner": "analytics_team",
    "retries": 2,
    "retry_delay": timedelta(minutes=5),
}


def staging_dir() -> Path:
    run_id = get_current_context()["run_id"].replace(":", "_").replace("+", "_")
    path = STAGING_BASE / run_id
    path.mkdir(parents=True, exist_ok=True)
    return path


with DAG(
    dag_id="bank_customers_churn",
    start_date=datetime(2023, 11, 12),
    schedule="@daily",
    catchup=False,
    default_args=default_args,
    tags=["engineering"],
) as dag:

    @task
    def extract_data() -> dict[str, str]:
        file_path = "/opt/airflow/data/bank_churn_etl/Bank_Churn_all_data.xlsx"
        staging = staging_dir()

        customer = pd.read_excel(
            file_path, sheet_name="Customer_Info", engine="openpyxl"
        )
        acc_info = pd.read_excel(
            file_path, sheet_name="Account_Info", engine="openpyxl"
        )

        customer_path = staging / "customer.parquet"
        acc_info_path = staging / "acc_info.parquet"
        customer.to_parquet(customer_path, index=False)
        acc_info.to_parquet(acc_info_path, index=False)

        return {
            "customer_path": str(customer_path),
            "acc_info_path": str(acc_info_path),
        }

    @task
    def transform(data_dict: dict[str, str]) -> str:
        customer = pd.read_parquet(data_dict["customer_path"])
        acc_info = pd.read_parquet(data_dict["acc_info_path"])

        bank_churn = customer.merge(
            acc_info, how="inner", on="CustomerId"
        ).drop_duplicates()

        bank_churn["Geography"] = bank_churn["Geography"].replace(
            {"FRA": "France", "French": "France"}
        )
        bank_churn["Age"] = (
            pd.to_numeric(bank_churn["Age"], errors="coerce").fillna(0).astype(int)
        )

        bank_churn["Balance"] = (
            pd.to_numeric(
                bank_churn["Balance"].replace("€", "", regex=True),
                errors="coerce",
            )
            .fillna(0)
            .astype(float)
        )

        if "Tenure_y" in bank_churn.columns:
            bank_churn = bank_churn.rename(columns={"Tenure_y": "Tenure"})
            bank_churn.drop(columns="Tenure_x", errors="ignore", inplace=True)

        bank_churn.dropna(subset=["Surname"], inplace=True)

        out_path = staging_dir() / "bank_churn.parquet"
        bank_churn.to_parquet(out_path, index=False)
        return str(out_path)

    @task
    def dim_customer(bank_churn_path: str) -> str:
        bank_churn = pd.read_parquet(bank_churn_path)
        dim_cust = (
            bank_churn[["CustomerId", "Surname", "Age", "Gender"]]
            .copy()
            .reset_index(drop=True)
        )
        dim_cust["Customer_key"] = dim_cust.index + 1

        out_path = Path(bank_churn_path).parent / "dim_customer.parquet"
        dim_cust.to_parquet(out_path, index=False)
        return str(out_path)

    @task
    def dim_country(bank_churn_path: str) -> str:
        bank_churn = pd.read_parquet(bank_churn_path)
        dim_count = bank_churn[["Geography"]].drop_duplicates().reset_index(drop=True)
        dim_count["Country_key"] = dim_count.index + 1

        out_path = Path(bank_churn_path).parent / "dim_country.parquet"
        dim_count.to_parquet(out_path, index=False)
        return str(out_path)

    @task
    def fact_bank_transactions(
        bank_churn_path: str, dim_cust_path: str, dim_count_path: str
    ) -> str:
        bank_churn = pd.read_parquet(bank_churn_path)
        dim_cust = pd.read_parquet(dim_cust_path)
        dim_count = pd.read_parquet(dim_count_path)

        fact = bank_churn.merge(
            dim_cust[["CustomerId", "Customer_key"]], on="CustomerId"
        )
        fact = fact.merge(dim_count[["Geography", "Country_key"]], on="Geography")

        fact = fact[
            [
                "Customer_key",
                "Country_key",
                "CreditScore",
                "Balance",
                "NumOfProducts",
                "HasCrCard",
                "Tenure",
                "IsActiveMember",
                "EstimatedSalary",
                "Exited",
            ]
        ]

        out_path = Path(bank_churn_path).parent / "fact_bank_transactions.parquet"
        fact.to_parquet(out_path, index=False)
        return str(out_path)

    raw_paths = extract_data()
    clean_path = transform(raw_paths)
    cust_path = dim_customer(clean_path)
    country_path = dim_country(clean_path)
    fact_bank_transactions(clean_path, cust_path, country_path)
