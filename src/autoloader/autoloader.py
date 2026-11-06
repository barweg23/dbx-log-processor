"""Auto Loader: Parquet files in the raw zone -> bronze Delta tables in Unity Catalog (task 06)."""

import sys
from pathlib import Path

from pyspark.sql import functions as F

# Make src/ importable, so `import config` works both locally and on Databricks.
try:
    src_folder = Path(__file__).resolve().parents[1]
except NameError:
    # A Databricks job runs the file with exec(), so __file__ is not set;
    # the working directory is then the folder of the file, e.g. src/autoloader.
    src_folder = Path.cwd().parent
sys.path.append(str(src_folder))

import config  # noqa: E402


def load_source(spark, cfg, source_name):
    """Load the new raw files of one source into its bronze table, then stop (availableNow)."""
    stream = (
        spark.readStream.format("cloudFiles")
        .option("cloudFiles.format", "parquet")
        .option("cloudFiles.schemaLocation", config.schema_path(cfg, source_name))
        .option("cloudFiles.schemaEvolutionMode", "addNewColumns")
        .option("pathGlobFilter", "*.parquet")
        .load(config.raw_path(cfg, source_name))
        .select(
            "*",
            F.col("_metadata.file_path").alias("_source_file"),
            F.col("_metadata.file_modification_time").alias("_source_file_modified_at"),
            F.current_timestamp().alias("_ingested_at"),
        )
    )

    query = (
        stream.writeStream
        .option("checkpointLocation", config.checkpoint_path(cfg, source_name))
        .trigger(availableNow=True)
        .toTable(config.table_name(cfg, "bronze", source_name))
    )
    query.awaitTermination()


def main():
    spark = config.get_spark()
    cfg = config.load_config()

    for source in cfg["sources"]:
        name = source["name"]
        load_source(spark, cfg, name)

        table = config.table_name(cfg, "bronze", name)
        print(f"[{name}] {table} now has {spark.table(table).count()} rows")


if __name__ == "__main__":
    main()