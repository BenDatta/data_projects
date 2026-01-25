from pyspark import pipelines as dp
from pyspark.sql import functions as F
from pyspark.sql.functions import current_timestamp


@dp.materialized_view(
    name="data_projects.silver.city",
    comment="Cleaned and standardized products dimension with business transformations",
    table_properties={
        "quality": "silver",
        "layer": "silver",
        "delta.enableChangeDataFeed": "true",
        "delta.autoOptimize.optimizeWrite": "true",
        "delta.autoOptimize.autoCompact": "true",
    },
)
def city_silver():
    df_bronze = spark.read.table("data_projects.bronze.city")
    df_silver = df_bronze.select(
        F.col("city_id").alias("city_id"),
        F.col("city_name").alias("city_name"),
        F.col("bronze_ingesttime").alias("bronze_ingest_time"),
    )
    df_silver = df_silver.withColumn("silver_ingest_time", current_timestamp())

    return df_silver
