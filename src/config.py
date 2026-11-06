"""Project configuration: settings come from config.yaml, secrets from a secure store."""

import os
from pathlib import Path

import yaml

CONFIG_PATH = Path(__file__).parent / "config.yaml"


def load_config(path: Path = CONFIG_PATH) -> dict:
    """Read config.yaml; values set by the Databricks Asset Bundle (task 09) take priority."""
    with open(path, encoding="utf-8") as file:
        cfg = yaml.safe_load(file)

    if "LOGS_CATALOG" in os.environ:
        cfg["catalog"] = os.environ["LOGS_CATALOG"]
    if "LOGS_STORAGE_ACCOUNT" in os.environ:
        cfg["storage"]["account"] = os.environ["LOGS_STORAGE_ACCOUNT"]

    return cfg


def is_running_on_databricks() -> bool:
    """Databricks sets DATABRICKS_RUNTIME_VERSION on every cluster; locally it does not exist."""
    return "DATABRICKS_RUNTIME_VERSION" in os.environ

def get_sql_password(config: dict, spark=None) -> str:
    """Return the SQL password from the secret scope on Databricks, or from the environment locally."""
    sql = config["sql"]

    if is_running_on_databricks():
        from pyspark.dbutils import DBUtils

        dbutils = DBUtils(spark)
        return dbutils.secrets.get(scope=sql["secret_scope"], key=sql["password_key"])

    from dotenv import load_dotenv

    load_dotenv()
    password = os.environ.get(sql["password_env"])
    if not password:
        raise RuntimeError(
            f"Environment variable {sql['password_env']} is not set. Add it to your .env file."
        )
    return password

def get_spark():
    """Return a Spark session: the cluster's own on Databricks, Databricks Connect locally."""
    if is_running_on_databricks():
        from pyspark.sql import SparkSession

        return SparkSession.builder.getOrCreate()
    from databricks.connect import DatabricksSession

    return DatabricksSession.builder.getOrCreate()


def raw_path(cfg: dict, source_name: str) -> str:
    """Folder of one source in the raw container, e.g. abfss://raw@<account>.dfs.core.windows.net/web/."""
    storage = cfg["storage"]
    return (
        f"abfss://{storage['raw_container']}@{storage['account']}.dfs.core.windows.net/"
        f"{source_name}/"
    )


def processed_path(cfg: dict, folder_key: str, source_name: str) -> str:
    """Per-source folder in the processed container, e.g. .../checkpoints/web/ for folder_key='checkpoints_folder'."""
    storage = cfg["storage"]
    return (
        f"abfss://{storage['processed_container']}@{storage['account']}.dfs.core.windows.net/"
        f"{storage[folder_key]}/{source_name}/"
    )