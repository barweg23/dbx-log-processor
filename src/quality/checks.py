"""Data quality framework: config-driven rules, silver/quarantine split and run summary (task 07)."""

import sys
from datetime import datetime, timezone
from pathlib import Path

from pyspark.sql import Window
from pyspark.sql import functions as F

# Make src/ importable, so `import config` works both locally and on Databricks.
try:
    src_folder = Path(__file__).resolve().parents[1]
except NameError:
    # A Databricks job runs the file with exec(), so __file__ is not set;
    # the working directory is then the folder of the file, e.g. src/quality.
    src_folder = Path.cwd().parent
sys.path.append(str(src_folder))

import config  # noqa: E402
from quality.tracking import ensure_monitoring_table, track_quality_run  # noqa: E402


# --- Helpers ----------------------------------------------------------------

def rule_name(rule):
    """Readable rule id used in quarantine and monitoring, e.g. 'not_null:log_id,user_id'."""
    if "columns" in rule:
        columns = rule["columns"]
    else:
        columns = [rule["column"]]
    return f"{rule['rule']}:{','.join(columns)}"


def any_of(conditions):
    """Combine boolean columns with OR: TRUE when at least one condition is TRUE."""
    result = F.lit(False)
    for condition in conditions:
        result = result | condition
    return result


def parse_timestamp(column, formats):
    """Try every known format; the first one that parses wins, NULL when none does."""
    attempts = []
    for fmt in formats:
        attempts.append(F.try_to_timestamp(F.col(column), F.lit(fmt)))
    return F.coalesce(*attempts)


# --- Rules: each returns a column that is TRUE when the row FAILS the rule --

def fails_not_null(rule):
    conditions = []
    for column in rule["columns"]:
        conditions.append(F.col(column).isNull())
    return any_of(conditions)


def fails_unique(rule):
    # Number the rows that share a key; every row after the first one is a duplicate.
    window = Window.partitionBy(*rule["columns"]).orderBy("_ingested_at", "_source_file")
    return F.row_number().over(window) > 1


def fails_regex(rule):
    value = F.col(rule["column"])
    return value.isNotNull() & ~value.rlike(rule["pattern"])


def fails_range(rule):
    value = F.col(rule["column"])
    conditions = []
    if "min" in rule:
        conditions.append(value < rule["min"])
    if "max" in rule:
        conditions.append(value > rule["max"])
    return value.isNotNull() & any_of(conditions)


def fails_allowed_values(rule):
    value = F.col(rule["column"])
    return value.isNotNull() & ~value.isin(rule["values"])


def fails_timestamp(rule, formats):
    return parse_timestamp(rule["column"], formats).isNull()


def rule_fails(rule, cfg):
    """Pick the check that matches the rule type written in config.yaml."""
    rule_type = rule["rule"]
    if rule_type == "not_null":
        return fails_not_null(rule)
    if rule_type == "unique":
        return fails_unique(rule)
    if rule_type == "regex":
        return fails_regex(rule)
    if rule_type == "range":
        return fails_range(rule)
    if rule_type == "allowed_values":
        return fails_allowed_values(rule)
    if rule_type == "timestamp":
        return fails_timestamp(rule, cfg["quality"]["timestamp_formats"])
    raise ValueError(f"Unknown rule type '{rule_type}' in config.yaml")


# --- Applying the rules and writing the results -----------------------------

def apply_rules(df, rules, cfg):
    """Add a failed_rules column: the list of names of every rule the row breaks."""
    failures = []
    for rule in rules:
        failures.append(F.when(rule_fails(rule, cfg), F.lit(rule_name(rule))))
    return df.withColumn("failed_rules", F.array_compact(F.array(*failures)))


def split_rows(checked, run_id):
    """Return (valid rows, invalid rows). Invalid rows keep failed_rules and get the run id."""
    valid = checked.filter(F.size("failed_rules") == 0).drop("failed_rules")
    invalid = checked.filter(F.size("failed_rules") > 0).withColumn("run_id", F.lit(run_id))
    return valid, invalid


def write_table(df, table):
    """Replace the whole table with this DataFrame, so a rerun gives the same result."""
    df.write.mode("overwrite").option("overwriteSchema", "true").saveAsTable(table)


def main():
    spark = config.get_spark()
    cfg = config.load_config()
    threshold = cfg["quality"]["failure_threshold"]
    run_id = datetime.now(timezone.utc).strftime("%Y%m%d_%H%M%S")
    sources_over_threshold = []
    ensure_monitoring_table(spark, cfg)

    for source in cfg["sources"]:
        name = source["name"]
        bronze = spark.table(config.table_name(cfg, "bronze", name))

        checked = apply_rules(bronze, source["rules"], cfg)
        valid, invalid = split_rows(checked, run_id)
        write_table(valid, config.table_name(cfg, "silver", name))
        write_table(invalid, config.table_name(cfg, "quarantine", name))

        total = bronze.count()
        failed = invalid.count()
        passed = valid.count()
        ratio = failed / total if total > 0 else 0.0
        print(f"[{name}] read: {total}, passed: {passed}, failed: {failed}, failure ratio: {ratio:.0%}")

        breached = ratio > threshold
        rule_names = []
        for rule in source["rules"]:
            rule_names.append(rule_name(rule))
        track_quality_run(spark, cfg, run_id, name, rule_names, checked, threshold, breached)

        if breached:
            sources_over_threshold.append(name)

    # Raise only after every source is written and tracked, so a failing run still leaves a trail.
    if sources_over_threshold:
        raise RuntimeError(f"Failure ratio above {threshold:.0%} for: {', '.join(sources_over_threshold)}")


if __name__ == "__main__":
    main()