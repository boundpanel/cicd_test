from __future__ import annotations

import os

import pytest
from pyspark.sql import SparkSession

from scrm_etl.spark import create_spark_session

os.environ.setdefault("TZ", "UTC")


@pytest.fixture(scope="session")
def spark() -> SparkSession:
    session = create_spark_session(app_name="scrm-etl-tests")
    yield session
    session.stop()
