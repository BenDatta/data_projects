from pathlib import Path

import pandas as pd

folder = Path(__file__).parent


def extract_data():
    file_path = folder / "data" / "bank_churn" / "Bank_Churn_all_data.xlsx"

    customer = pd.read_excel(file_path, sheet_name="Customer_Info")
    acc_info = pd.read_excel(file_path, sheet_name="Account_Info")

    return {"customer": customer, "acc_info": acc_info}


def transform(customer, acc_info):
    bank_churn = customer.merge(acc_info, how="inner", on="CustomerId")

    bank_churn = bank_churn.drop_duplicates()

    bank_churn["Geography"] = bank_churn["Geography"].replace(
        {"FRA": "France", "French": "France"}
    )

    bank_churn["Age"] = (
        pd.to_numeric(bank_churn["Age"], errors="coerce").fillna(0).astype(int)
    )

    bank_churn["Balance"] = bank_churn["Balance"].replace("€", "", regex=True)

    bank_churn["Balance"] = (
        pd.to_numeric(bank_churn["Balance"], errors="coerce").fillna(0).astype(float)
    )

    if "Tenure_y" in bank_churn.columns:
        bank_churn = bank_churn.rename(columns={"Tenure_y": "Tenure"})
        bank_churn.drop(columns="Tenure_x", errors="ignore", inplace=True)

    bank_churn.dropna(subset=["Surname"], inplace=True)

    return bank_churn


def dim_customer(bank_churn):
    dim_customer = bank_churn[["CustomerId", "Surname", "Age", "Gender"]].reset_index(
        drop=True
    )
    dim_customer["Customer_key"] = dim_customer.index + 1

    return dim_customer


def dim_country(bank_churn):
    dim_country = bank_churn[["Geography"]].reset_index(drop=True)
    dim_country["Country_key"] = dim_country.index + 1

    return dim_country


def fact_bank_transactions(bank_churn, dim_customer_df, dim_country_df):
    fact = bank_churn.merge(
        dim_customer_df[["CustomerId", "Customer_key"]], on="CustomerId"
    )
    fact = fact.merge(dim_country_df[["Geography", "Country_key"]], on="Geography")

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

    return fact
