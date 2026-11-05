"""Auto Loader: Parquet files in the raw zone -> bronze Delta tables in Unity Catalog (task 06)."""

import sys
from pathlib import Path

from pyspark.sql import functions as F

# Make src/ importable, so `import config` works both locally and on Databricks.
sys.path.append(str(Path(__file__).resolve().parents[1]))

import config  # noqa: E402


def bronze_table(cfg: dict, source_name: str) -> str:
    """Full name of the bronze table of one source, e.g. logs.bronze.logs_web."""
    return f"{cfg['catalog']}.bronze.logs_{source_name}"


def load_source(spark, cfg: dict, source: dict) -> None:
    """Load the new raw files of one source into its bronze table, then stop (availableNow)."""
    name = source["name"]

    stream = (
        spark.readStream.format("cloudFiles")
        .option("cloudFiles.format", "parquet")
        .option("cloudFiles.schemaLocation", config.processed_path(cfg, "schemas_folder", name))
        .option("cloudFiles.schemaEvolutionMode", "addNewColumns")
        .option("pathGlobFilter", "*.parquet")
        .load(config.raw_path(cfg, name))
        .select(
            "*",
            F.col("_metadata.file_path").alias("_source_file"),
            F.col("_metadata.file_modification_time").alias("_source_file_modified_at"),
            F.current_timestamp().alias("_ingested_at"),
        )
    )

    query = (
        stream.writeStream.option(
            "checkpointLocation", config.processed_path(cfg, "checkpoints_folder", name)
        )
        .trigger(availableNow=True)
        .toTable(bronze_table(cfg, name))
    )
    query.awaitTermination()


def main() -> None:
    spark = config.get_spark()
    cfg = config.load_config()

    for source in cfg["sources"]:
        load_source(spark, cfg, source)
        table = bronze_table(cfg, source["name"])
        print(f"[{source['name']}] {table} now has {spark.table(table).count()} rows")


if __name__ == "__main__":
    main()