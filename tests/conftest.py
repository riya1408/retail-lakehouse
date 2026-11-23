import os
import sys

import pytest
from pyspark.sql import SparkSession


@pytest.fixture(scope="session")
def spark():
    """One small local Spark for the whole test run (started once, shared by all tests)."""
    os.environ["PYSPARK_PYTHON"] = sys.executable          # Windows: use this venv's Python
    os.environ["PYSPARK_DRIVER_PYTHON"] = sys.executable
    session = (SparkSession.builder
               .master("local[1]")
               .appName("retail-lakehouse-tests")
               .config("spark.driver.host", "127.0.0.1")         # stay on loopback (Docker/VPN safe)
               .config("spark.driver.bindAddress", "127.0.0.1")
               .config("spark.python.worker.faulthandler.enabled", "true")   # real error if a worker dies
               .config("spark.sql.execution.pyspark.udf.faulthandler.enabled", "true")
               .config("spark.sql.execution.arrow.pyspark.enabled", "true")          # test data via Arrow
               .config("spark.sql.execution.arrow.pyspark.fallback.enabled", "false")
               .config("spark.sql.session.timeZone", "UTC")
               .config("spark.sql.shuffle.partitions", "1")
               .config("spark.ui.enabled", "false")
               .getOrCreate())
    yield session
    session.stop()
