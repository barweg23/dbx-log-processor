"""Project configuration: settings come from config.yaml, secrets from a secure store."""

import os
from pathlib import Path

import yaml

CONFIG_PATH = Path(__file__).parent / "config.yaml"


def load_config(path: Path = CONFIG_PATH) -> dict:
    """Read config.yaml and return it as a dictionary."""
    with open(path, encoding="utf-8") as file:
        return yaml.safe_load(file)


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