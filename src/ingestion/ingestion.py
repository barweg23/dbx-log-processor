"""Incremental ingestion: Azure SQL -> raw zone in ADLS Gen2 (task 05)."""

import sys
import time
from datetime import datetime, timezone
from pathlib import Path

from pyspark.sql import functions as F

# Make src/ importable, so `import config` works both locally and on Databricks.
try:
    src_folder = Path(__file__).resolve().parents[1]
except NameError:
    # A Databricks job runs the file with exec(), so __file__ is not set;
    # the working directory is then the folder of the file, e.g. src/ingestion.
    src_folder = Path.cwd().parent
sys.path.append(str(src_folder))

import config  # noqa: E402

# Watermarks are passed around as text in this exact format, never as Python datetimes.
TS_FORMAT = "yyyy-MM-dd HH:mm:ss.SSSSSS"


# --- Reading from Azure SQL -------------------------------------------------

def build_jdbc_url(cfg):
    """JDBC address of the Azure SQL database from the sql section of config.yaml."""
    server = cfg["sql"]["server"]
    database = cfg["sql"]["database"]
    return (
        f"jdbc:sqlserver://{server}:1433;database={database};"
        "encrypt=true;trustServerCertificate=false;"
        "hostNameInCertificate=*.database.windows.net;loginTimeout=30;"
    )


def read_source(spark, cfg, password, source, watermark):
    """Read the rows newer than the watermark from one SQL table; SQL Server does the filtering."""
    # Spark keeps 6 digits after the second, SQL Server keeps 7, so the stored watermark can be
    # slightly lower than the real value. Starting 1 microsecond later skips rows already loaded.
    query = (
        f"SELECT * FROM {source['table']} "
        f"WHERE {source['watermark']} >= DATEADD(MICROSECOND, 1, CAST('{watermark}' AS DATETIME2(7)))"
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


def with_retry(action, attempts=5, wait_seconds=30):
    """Run action(); while the serverless database is waking up, wait and try again."""
    for attempt in range(1, attempts + 1):
        try:
            return action()
        except Exception as error:
            if "not currently available" not in str(error) or attempt == attempts:
                raise
            print(f"Database is waking up (attempt {attempt}/{attempts}), retrying in {wait_seconds}s...")
            time.sleep(wait_seconds)


# --- Watermark control table ------------------------------------------------

def state_table(cfg):
    return f"{cfg['catalog']}.bronze._ingestion_state"


def ensure_state_table(spark, cfg):
    """Create the control table on the first run; does nothing if it already exists."""
    spark.sql(f"""
        CREATE TABLE IF NOT EXISTS {state_table(cfg)} (
            source         STRING,
            watermark      TIMESTAMP,
            rows_ingested  BIGINT,
            updated_at     TIMESTAMP
        ) USING DELTA
    """)


def get_watermark(spark, cfg, source_name):
    """Stored watermark of a source, or the start date from config.yaml on the first run."""
    rows = spark.sql(f"""
        SELECT date_format(watermark, '{TS_FORMAT}') AS wm
        FROM {state_table(cfg)}
        WHERE source = '{source_name}'
    """).collect()

    if len(rows) == 0:
        return cfg["ingestion"]["start_date"]
    return rows[0]["wm"]


def save_watermark(spark, cfg, source_name, watermark, rows_ingested):
    """Insert or update (MERGE) the watermark of one source."""
    spark.sql(f"""
        MERGE INTO {state_table(cfg)} AS target
        USING (
            SELECT '{source_name}'                   AS source,
                   CAST('{watermark}' AS TIMESTAMP)  AS watermark,
                   CAST({rows_ingested} AS BIGINT)   AS rows_ingested,
                   current_timestamp()               AS updated_at
        ) AS src
        ON target.source = src.source
        WHEN MATCHED THEN UPDATE SET *
        WHEN NOT MATCHED THEN INSERT *
    """)


# --- Writing to the raw zone ------------------------------------------------

def write_raw(df, cfg, source_name, run_id):
    """Append the new rows as one Parquet output named after the run, and return its path."""
    path = f"{config.raw_path(cfg, source_name)}{run_id}.parquet"
    df.write.mode("append").parquet(path)
    return path


def ingest_source(spark, cfg, password, source, run_id):
    """Copy the new rows of one source to the raw zone and move its watermark forward."""
    name = source["name"]
    watermark = get_watermark(spark, cfg, name)
    df = read_source(spark, cfg, password, source, watermark)

    if with_retry(df.isEmpty):
        save_watermark(spark, cfg, name, watermark, 0)
        return 0

    path = write_raw(df, cfg, name, run_id)

    # Count rows and take the new watermark from what actually landed in raw,
    # not from a second read of SQL (a row added in between would be skipped forever).
    written = spark.read.parquet(path)
    rows = written.count()
    new_watermark = written.agg(F.date_format(F.max(source["watermark"]), TS_FORMAT)).first()[0]

    save_watermark(spark, cfg, name, new_watermark, rows)
    return rows


def main():
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