"""Write the outcome of every data quality run to a monitoring table (task 08)."""

from datetime import datetime, timezone

from pyspark.sql import functions as F

# Used both to create the table and to build the rows we append to it.
SCHEMA = (
    "run_id STRING, run_timestamp TIMESTAMP, source STRING, rule STRING, "
    "rows_checked BIGINT, rows_failed BIGINT, failure_ratio DOUBLE, "
    "threshold DOUBLE, threshold_breached BOOLEAN"
)


def monitoring_table(cfg):
    return f"{cfg['catalog']}.monitoring.quality_check_runs"


def ensure_monitoring_table(spark, cfg):
    spark.sql(f"CREATE TABLE IF NOT EXISTS {monitoring_table(cfg)} ({SCHEMA}) USING DELTA")


def count_failures_per_rule(checked):
    """Return a dictionary {rule name: number of rows that broke it}."""
    rows = checked.select(F.explode("failed_rules").alias("rule")).groupBy("rule").count().collect()
    failures = {}
    for row in rows:
        failures[row["rule"]] = row["count"]
    return failures


def track_quality_run(spark, cfg, run_id, source_name, rule_names, checked, threshold, threshold_breached):
    """Append one row per rule of one source, also for rules with zero failures."""
    rows_checked = checked.count()
    failures = count_failures_per_rule(checked)
    run_timestamp = datetime.now(timezone.utc)

    records = []
    for rule in rule_names:
        rows_failed = failures.get(rule, 0)
        ratio = rows_failed / rows_checked if rows_checked > 0 else 0.0
        records.append(
            (run_id, run_timestamp, source_name, rule, rows_checked, rows_failed,
             ratio, float(threshold), threshold_breached)
        )

    spark.createDataFrame(records, SCHEMA).write.mode("append").saveAsTable(monitoring_table(cfg))