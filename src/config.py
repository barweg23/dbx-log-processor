"""Project configuration: settings come from config.yaml, secrets from a secure store."""

import os
from pathlib import Path

import yaml

CONFIG_PATH = Path(__file__).parent / "config.yaml"


# --- Settings ---------------------------------------------------------------

def load_config(path=CONFIG_PATH):
    """Read config.yaml; values set by the Databricks Asset Bundle (task 09) take priority."""
    with open(path, encoding="utf-8") as file:
        cfg = yaml.safe_load(file)

    if "LOGS_CATALOG" in os.environ:
        cfg["catalog"] = os.environ["LOGS_CATALOG"]
    if "LOGS_STORAGE_ACCOUNT" in os.environ:
        cfg["storage"]["account"] = os.environ["LOGS_STORAGE_ACCOUNT"]

    return cfg


# --- Runtime: where the code runs, Spark and secrets ------------------------

def is_running_on_databricks():
    """Databricks sets DATABRICKS_RUNTIME_VERSION on every cluster; locally it does not exist."""
    return "DATABRICKS_RUNTIME_VERSION" in os.environ


def get_spark():
    """Return a Spark session: the cluster's own on Databricks, Databricks Connect locally."""
    if is_running_on_databricks():
        from pyspark.sql import SparkSession
        return SparkSession.builder.getOrCreate()

    from databricks.connect import DatabricksSession
    return DatabricksSession.builder.getOrCreate()


def get_sql_password(cfg, spark):
    """Return the SQL password: from the secret scope on Databricks, from the .env file locally."""
    sql = cfg["sql"]

    if is_running_on_databricks():
        from pyspark.dbutils import DBUtils
        dbutils = DBUtils(spark)
        return dbutils.secrets.get(scope=sql["secret_scope"], key=sql["password_key"])

    from dotenv import load_dotenv
    load_dotenv()
    password = os.environ.get(sql["password_env"])
    if not password:
        raise RuntimeError(f"Environment variable {sql['password_env']} is not set. Add it to your .env file.")
    return password


# --- Paths in the data lake and table names ---------------------------------

def storage_path(cfg, container, folder):
    """abfss:// address of a folder in one container of the storage account."""
    account = cfg["storage"]["account"]
    return f"abfss://{container}@{account}.dfs.core.windows.net/{folder}/"


def raw_path(cfg, source_name):
    """Folder of one source in the raw container, e.g. .../raw/web/."""
    return storage_path(cfg, cfg["storage"]["raw_container"], source_name)


def checkpoint_path(cfg, source_name):
    """Auto Loader checkpoint of one source, e.g. .../processed/checkpoints/web/."""
    storage = cfg["storage"]
    return storage_path(cfg, storage["processed_container"], f"{storage['checkpoints_folder']}/{source_name}")


def schema_path(cfg, source_name):
    """Auto Loader schema location of one source, e.g. .../processed/schemas/web/."""
    storage = cfg["storage"]
    return storage_path(cfg, storage["processed_container"], f"{storage['schemas_folder']}/{source_name}")


def table_name(cfg, schema, source_name):
    """Full Unity Catalog table name, e.g. table_name(cfg, "bronze", "web") -> logs.bronze.logs_web."""
    return f"{cfg['catalog']}.{schema}.logs_{source_name}"