from datetime import datetime, timedelta

import pandas as pd
from airflow import DAG  # type: ignore
from airflow.decorators import task  # type: ignore

# Default DAG arguments
default_args = {
    "owner": "analytics_team",
    "retries": 2,
    "retry_delay": timedelta(minutes=5),
}

with DAG(
    dag_id="bank_customers_churn",
    start_date=datetime(2023, 11, 12),
    schedule="@daily",
    catchup=False,
    default_args=default_args,
    tags=["engineering"],
) as dag:

    @task
    def extract_data():
        file_path = (
            "/Users/benjamin/Documents/GitHub/python_data_analysis/"
            "projects/data engineering/data/bank_churn/Bank_Churn_all_data.xlsx"
        )

        customer = pd.read_excel(
            file_path, sheet_name="Customer_Info", engine="openpyxl"
        )
        acc_info = pd.read_excel(
            file_path, sheet_name="Account_Info", engine="openpyxl"
        )

        # In Airflow TaskFlow, it's often easier to return dataframes directly
        # for local testing, or dictionary of dataframes.
        return {"customer": customer, "acc_info": acc_info}

    @task
    def transform(data_dict):
        # FIX: Accessing the dataframes from the dictionary returned by extract_data
        customer = data_dict["customer"]
        acc_info = data_dict["acc_info"]

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
        return bank_churn

    @task
    def dim_customer(bank_churn):
        dim_cust = (
            bank_churn[["CustomerId", "Surname", "Age", "Gender"]]
            .copy()
            .reset_index(drop=True)
        )
        dim_cust["Customer_key"] = dim_cust.index + 1
        return dim_cust

    @task
    def dim_country(bank_churn):
        dim_count = bank_churn[["Geography"]].drop_duplicates().reset_index(drop=True)
        dim_count["Country_key"] = dim_count.index + 1
        return dim_count

    @task
    def fact_bank_transactions(bank_churn, dim_cust, dim_count):
        fact = bank_churn.merge(
            dim_cust[["CustomerId", "Customer_key"]], on="CustomerId"
        )
        fact = fact.merge(dim_count[["Geography", "Country_key"]], on="Geography")

        return fact[
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

    # Pipeline Flow
    raw_data = extract_data()
    clean_df = transform(raw_data)
    cust_df = dim_customer(clean_df)
    country_df = dim_country(clean_df)
    fact_bank_transactions(clean_df, cust_df, country_df)
