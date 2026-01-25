import pyspark.sql.functions as F
from pyspark import pipelines as dp

SOURCE_PATH = "s3://transport-prod-city-etl/Full Load/"


@dp.table(
    name="data_projects.bronze.trips",
    comment="Streaming ingestion of raw orders data with Auto Loader",
    table_properties={
        "quality": "bronze",
        "layer": "bronze",
        "source_format": "csv",
        "delta.enableChangeDataFeed": "true",
        "delta.autoOptimize.optimizeWrite": "true",
        "delta.autoOptimize.autoCompact": "true",
    },
)
def orders_bronze():
    df = (
        spark.readStream.format("cloudFiles")
        .option("cloudFiles.format", "csv")
        .option("cloudFiles.inferColumnTypes", "true")
        .option("cloudFiles.schemaEvolutionMode", "rescue")
        .option("cloudFiles.maxFilesPerTrigger", 100)
        .load(SOURCE_PATH)
    )

    # Rename the problematic column
    df = df.withColumnRenamed("distance_travelled(km)", "distance_travelled_km")

    df = df.withColumn("file_name", F.col("_metadata.file_path")).withColumn(
        "bronze_ingest_datetime", F.current_timestamp()
    )

    return df
