"""SparkSession factory configured for local Delta Lake writes."""

from __future__ import annotations

import os

from delta import configure_spark_with_delta_pip
from pyspark.sql import SparkSession

DEFAULT_APP_NAME = "scrm-etl"


def create_spark_session(app_name: str = DEFAULT_APP_NAME) -> SparkSession:
    """Build a local Spark session with the Delta catalog enabled."""
    builder = (
        SparkSession.builder.appName(app_name)
        .master(os.getenv("SPARK_MASTER", "local[*]"))
        .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension")
        .config(
            "spark.sql.catalog.spark_catalog",
            "org.apache.spark.sql.delta.catalog.DeltaCatalog",
        )
        .config("spark.sql.shuffle.partitions", os.getenv("SPARK_SHUFFLE_PARTITIONS", "2"))
        .config("spark.ui.enabled", os.getenv("SPARK_UI_ENABLED", "false"))
        .config("spark.driver.memory", os.getenv("SPARK_DRIVER_MEMORY", "1g"))
        .config("spark.sql.session.timeZone", "UTC")
        .config("spark.databricks.delta.schema.autoMerge.enabled", "true")
    )
    return configure_spark_with_delta_pip(builder).getOrCreate()
