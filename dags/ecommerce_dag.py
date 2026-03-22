from datetime import datetime

from airflow.providers.common.sql.operators.sql import SQLExecuteQueryOperator
from airflow.providers.postgres.hooks.postgres import PostgresHook
from airflow.sdk import dag, task

from data.ecommerce_pipeline.extract import extract_data


@dag(
    start_date=datetime(2026, 1, 1),
    schedule="@daily",
    tags=["ecommerce"],
    catchup=False,
)
def extract_ecommerce_data():

    create_tables = SQLExecuteQueryOperator(
        task_id="create_tables",
        conn_id="ecommerce_connection",
        sql="sql/create_tables.sql",
    )

    @task
    def get_columns(table_name: str):
        hook = PostgresHook(postgres_conn_id="ecommerce_connection")
        records = hook.get_records(
            """
            SELECT column_name
            FROM information_schema.columns
            WHERE table_name = %s
            ORDER BY ordinal_position;
        """,
            parameters=[table_name],
        )
        return [row[0] for row in records]

    @task
    def run_extract(file_path: str, target_fields: list):
        data = extract_data(file_path, target_fields)
        print("Extraction completed")
        return data

    @task
    def load_raw_data(data: list, target_fields: list, table_name: str):
        if not data:
            print("No data to insert")
            return

        hook = PostgresHook(postgres_conn_id="ecommerce_connection")
        hook.insert_rows(
            table=table_name,
            rows=data,
            target_fields=target_fields,
            replace=True,
            replace_index=target_fields[0],
        )
        print("Inserted")

    # File paths
    base_path = "/opt/airflow/data/ecommerce_pipeline/data"

    orders_cols = get_columns.override(task_id="get_columns_orders")("orders")
    order_items_cols = get_columns.override(task_id="get_columns_order_items")(
        "order_items"
    )
    refunds_cols = get_columns.override(task_id="get_columns_refunds")(
        "order_item_refunds"
    )
    products_cols = get_columns.override(task_id="get_columns_products")("products")
    page_views_cols = get_columns.override(task_id="get_columns_page_views")(
        "website_page_views"
    )
    sessions_cols = get_columns.override(task_id="get_columns_sessions")(
        "website_sessions"
    )

    orders_data = run_extract.override(task_id="run_extract_orders")(
        f"{base_path}/orders.csv", orders_cols
    )
    order_items_data = run_extract.override(task_id="run_extract_order_items")(
        f"{base_path}/order_items.csv", order_items_cols
    )
    refunds_data = run_extract.override(task_id="run_extract_refunds")(
        f"{base_path}/order_item_refunds.csv", refunds_cols
    )
    products_data = run_extract.override(task_id="run_extract_products")(
        f"{base_path}/products.csv", products_cols
    )
    page_views_data = run_extract.override(task_id="run_extract_page_views")(
        f"{base_path}/website_pageviews.csv", page_views_cols
    )
    sessions_data = run_extract.override(task_id="run_extract_sessions")(
        f"{base_path}/website_sessions.csv", sessions_cols
    )

    load_orders = load_raw_data.override(task_id="load_orders")(
        orders_data, orders_cols, "orders"
    )
    load_order_items = load_raw_data.override(task_id="load_order_items")(
        order_items_data, order_items_cols, "order_items"
    )
    load_refunds = load_raw_data.override(task_id="load_refunds")(
        refunds_data, refunds_cols, "order_item_refund"
    )
    load_products = load_raw_data.override(task_id="load_products")(
        products_data, products_cols, "products"
    )
    load_page_views = load_raw_data.override(task_id="load_page_views")(
        page_views_data, page_views_cols, "website_page_view"
    )
    load_sessions = load_raw_data.override(task_id="load_sessions")(
        sessions_data, sessions_cols, "website_sessions"
    )

    create_tables >> orders_cols >> load_orders
    create_tables >> order_items_cols >> load_order_items
    create_tables >> refunds_cols >> load_refunds
    create_tables >> products_cols >> load_products
    create_tables >> page_views_cols >> load_page_views
    create_tables >> sessions_cols >> load_sessions


dag = extract_ecommerce_data()
