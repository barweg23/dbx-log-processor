"""Incremental ingestion: Azure SQL -> raw zone in ADLS Gen2 (task 05)."""

import sys
import time
from datetime import datetime, timezone
from pathlib import Path

from pyspark.sql import functions as F

# Make src/ importable, so `import config` works both locally and on Databricks.
sys.path.append(str(Path(__file__).resolve().parents[1]))

import config  # noqa: E402

# Watermarks are passed around as text in this exact format, never as Python datetimes.
TS_FORMAT = "yyyy-MM-dd HH:mm:ss.SSSSSS"


def build_jdbc_url(cfg: dict) -> str:
    """Build the JDBC URL for Azure SQL from the sql section of config.yaml."""
    server = cfg["sql"]["server"]
    database = cfg["sql"]["database"]
    return (
        f"jdbc:sqlserver://{server}:1433;database={database};"
        "encrypt=true;trustServerCertificate=false;"
        "hostNameInCertificate=*.database.windows.net;loginTimeout=30;"
    )

def read_source(spark, cfg: dict, password: str, source: dict, watermark: str):
    """Read rows newer than the watermark from one SQL table; the filter runs inside SQL Server."""
    query = (
        f"SELECT * FROM {source['table']} "
        f"WHERE {source['watermark']} >= "
        f"DATEADD(MICROSECOND, 1, CAST('{watermark}' AS DATETIME2(7)))"
    )
    return (
        spark.read.format("jdbc")
        .option("url", build_jdbc_url(cfg))
        .option("driver", "com.microsoft.sqlserver.jdbc.SQLServerDriver")
        .option("query", query)
        .option("user", cfg["sql"]["user"])
        .option("password", password)
        .load()
    )

def with_retry(action, attempts: int = 5, wait_seconds: int = 30):
    """Run action(); while the serverless database is waking up, wait and try again."""
    for attempt in range(1, attempts + 1):
        try:
            return action()
        except Exception as error:
            if "not currently available" not in str(error) or attempt == attempts:
                raise
            print(f"Database is waking up (attempt {attempt}/{attempts}), retrying in {wait_seconds}s...")
            time.sleep(wait_seconds)

def state_table(cfg: dict) -> str:
    """Full name of the watermark control table, built from the catalog in config.yaml."""
    return f"{cfg['catalog']}.bronze._ingestion_state"


def ensure_state_table(spark, cfg: dict) -> None:
    """Create the control table on the first run; does nothing if it already exists."""
    spark.sql(
        f"""
        CREATE TABLE IF NOT EXISTS {state_table(cfg)} (
            source         STRING,
            watermark      TIMESTAMP,
            rows_ingested  BIGINT,
            updated_at     TIMESTAMP
        ) USING DELTA
        """
    )


def get_watermark(spark, cfg: dict, source_name: str) -> str:
    """Return the stored watermark for a source, or the configured start date on the first run."""
    rows = spark.sql(
        f"SELECT date_format(watermark, '{TS_FORMAT}') AS wm "
        f"FROM {state_table(cfg)} WHERE source = '{source_name}'"
    ).collect()
    if rows and rows[0]["wm"] is not None:
        return rows[0]["wm"]
    return cfg["ingestion"]["start_date"]


def save_watermark(spark, cfg: dict, source_name: str, watermark: str, rows_ingested: int) -> None:
    """Upsert the watermark of one source into the control table."""
    spark.sql(
        f"""
        MERGE INTO {state_table(cfg)} AS target
USING (
    SELECT '{source_name}'                       AS source,
           CAST('{watermark}' AS TIMESTAMP)     AS watermark,
           CAST({rows_ingested} AS BIGINT)    AS rows_ingested,
           current_timestamp()                 AS updated_at
) AS src
ON target.source = src.source
WHEN MATCHED THEN UPDATE SET *
WHEN NOT MATCHED THEN INSERT *
        """
    )

def write_raw(df, cfg: dict, source_name: str, run_id: str) -> str:
    """Append the new rows as one timestamped Parquet output in the raw zone and return its path."""
    path = f"{config.raw_path(cfg, source_name)}{run_id}.parquet"
    df.write.mode("append").parquet(path)
    return path

def ingest_source(spark, cfg: dict, password: str, source: dict, run_id: str) -> int:
    """Copy the new rows of one source to the raw zone and move its watermark forward."""
    name = source["name"]
    watermark = get_watermark(spark, cfg, name)
    df = read_source(spark, cfg, password, source, watermark)

    if with_retry(df.isEmpty):
        save_watermark(spark, cfg, name, watermark, 0)
        return 0

    path = write_raw(df, cfg, name, run_id)

    # Take the new watermark from what actually landed in raw, not from a second read of SQL.
    stats = (
        spark.read.parquet(path)
        .agg(
            F.count("*").alias("rows"),
            F.date_format(F.max(source["watermark"]), TS_FORMAT).alias("watermark"),
        )
        .first()
    )
    save_watermark(spark, cfg, name, stats["watermark"], stats["rows"])
    return stats["rows"]


def main() -> None:
    spark = config.get_spark()
    spark.conf.set("spark.sql.session.timeZone", "UTC")
    cfg = config.load_config()
    password = config.get_sql_password(cfg, spark)
    run_id = datetime.now(timezone.utc).strftime("%Y%m%d_%H%M%S")

    ensure_state_table(spark, cfg)
    for source in cfg["sources"]:
        rows = ingest_source(spark, cfg, password, source, run_id)
        print(f"[{source['name']}] ingested {rows} new rows")


if __name__ == "__main__":
    main()