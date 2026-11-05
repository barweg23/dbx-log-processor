"""Data quality framework: config-driven rules, silver/quarantine split and run summary (task 07)."""

from functools import reduce

from pyspark.sql import Column, Window
from pyspark.sql import functions as F
import sys
from datetime import datetime, timezone
from functools import reduce
from pathlib import Path
from pyspark.sql import Column, Window
from pyspark.sql import functions as F
# Make src/ importable, so `import config` works both locally and on Databricks.
sys.path.append(str(Path(__file__).resolve().parents[1]))

import config  # noqa: E402
from quality.tracking import ensure_monitoring_table, track_quality_run  # noqa: E402


def rule_name(rule: dict) -> str:
    """Readable rule id used in quarantine and monitoring, e.g. 'not_null:log_id,user_id'."""
    columns = rule.get("columns") or [rule["column"]]
    return f"{rule['rule']}:{','.join(columns)}"


def any_of(conditions: list) -> Column:
    """OR together a list of boolean columns."""
    result = F.lit(False)
    for condition in conditions:
        result = result | condition
    return result


def parse_timestamp(column: str, formats: list) -> Column:
    """Try every known format; the first one that parses wins, NULL when none does."""
    return F.coalesce(*[F.try_to_timestamp(F.col(column), F.lit(fmt)) for fmt in formats])


# Each function returns a boolean column that is TRUE when the row FAILS the rule.

def fails_not_null(rule: dict, cfg: dict) -> Column:
    return any_of([F.col(c).isNull() for c in rule["columns"]])


def fails_unique(rule: dict, cfg: dict) -> Column:
    # Number the rows inside each key; every row after the first is a duplicate.
    window = Window.partitionBy(*rule["columns"]).orderBy("_ingested_at", "_source_file")
    return F.row_number().over(window) > 1


def fails_regex(rule: dict, cfg: dict) -> Column:
    value = F.col(rule["column"])
    return value.isNotNull() & ~value.rlike(rule["pattern"])     # value.rlike(rule["pattern"]) 


def fails_range(rule: dict, cfg: dict) -> Column:
    value = F.col(rule["column"])
    out_of_range = []
    if "min" in rule:
        out_of_range.append(value < rule["min"])
    if "max" in rule:
        out_of_range.append(value > rule["max"])
    return value.isNotNull() & any_of(out_of_range)


def fails_timestamp(rule: dict, cfg: dict) -> Column:
    return parse_timestamp(rule["column"], cfg["quality"]["timestamp_formats"]).isNull()

def fails_allowed_values(rule: dict, cfg: dict) -> Column:
    value = F.col(rule["column"])
    return value.isNotNull() & ~value.isin(rule["values"])

# Maps the rule type written in config.yaml to the function that checks it.
RULE_CHECKS = {
    "not_null": fails_not_null,
    "unique": fails_unique,
    "regex": fails_regex,
    "range": fails_range,
    "allowed_values": fails_allowed_values,
    "timestamp": fails_timestamp,
}


def apply_rules(df, rules: list, cfg: dict):
    """Add a failed_rules array column listing the name of every rule the row breaks."""
    failures = []
    for rule in rules:
        check = RULE_CHECKS.get(rule["rule"])
        if check is None:
            raise ValueError(f"Unknown rule type '{rule['rule']}' in config.yaml")
        failures.append(F.when(check(rule, cfg), F.lit(rule_name(rule))))
    return df.withColumn("failed_rules", F.array_compact(F.array(*failures)))

def split_rows(checked, run_id):
    """Return (valid rows, invalid rows). Invalid rows keep failed_rules and get the run id."""
    valid = checked.filter(F.size("failed_rules") == 0).drop("failed_rules")
    invalid = checked.filter(F.size("failed_rules") > 0).withColumn("run_id", F.lit(run_id))
    return valid, invalid

def write_table(df, table_name):
    """Replace the whole table with this DataFrame, so a rerun gives the same result."""
    df.write.mode("overwrite").option("overwriteSchema", "true").saveAsTable(table_name)

def main():
    spark = config.get_spark()
    cfg = config.load_config()
    threshold = cfg["quality"]["failure_threshold"]
    run_id = datetime.now(timezone.utc).strftime("%Y%m%d_%H%M%S")
    sources_over_threshold = []
    ensure_monitoring_table(spark, cfg)

    for source in cfg["sources"]:
        name = source["name"]
        bronze = spark.table(f"{cfg['catalog']}.bronze.logs_{name}")

        checked = apply_rules(bronze, source["rules"], cfg)
        valid, invalid = split_rows(checked, run_id)
        write_table(valid, f"{cfg['catalog']}.silver.logs_{name}")
        write_table(invalid, f"{cfg['catalog']}.quarantine.logs_{name}")

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

    if sources_over_threshold:
        raise RuntimeError(
            f"Failure ratio above {threshold:.0%} for: {', '.join(sources_over_threshold)}"
        )


if __name__ == "__main__":
    main()