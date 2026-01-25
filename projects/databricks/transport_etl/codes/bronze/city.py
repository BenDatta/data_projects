from pyspark import pipelines as dp
from pyspark.sql.functions import current_timestamp


@dp.materialized_view(
    name="data_projects.bronze.city",
    comment="City data (bronze)",
    table_properties={
        "quality": "bronze",
        "delta.enableChangeDataFeed": "true",
    },
)
def city_bronze():
    return (
        spark.read.format("csv")
        .option("header", "true")
        .option("inferSchema", "true")
        .option("mode", "PERMISSIVE")
        .load("s3://transport-prod-city-etl/city.csv")
        .withColumn("bronze_ingesttime", current_timestamp())
    )
