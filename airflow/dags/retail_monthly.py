"""
Retail lakehouse - monthly load, orchestrated by Airflow.

One DAG run = one month of data (Airflow's "logical date" tells the run which month it owns).
Every task is idempotent, so retries, reruns and backfills are safe.

Part A (now):   check raw file -> bronze (DuckDB, runs inside the Airflow container)
Part B (next):  upload to Databricks -> silver (replaceWhere) -> SCD2 MERGE -> dbt build
"""
import os
from datetime import datetime, timedelta

from airflow.exceptions import AirflowFailException
from airflow.providers.standard.operators.bash import BashOperator
from airflow.providers.standard.operators.empty import EmptyOperator
from airflow.sdk import dag, task

PROJECT = os.environ.get("RETAIL_PROJECT_DIR", "/opt/project")   # the repo, mounted into the container
MONTH_FILE = "{{ data_interval_start.strftime('%Y-%b') }}.csv"  # e.g. 2019-Oct.csv


@dag(
    dag_id="retail_monthly_load",
    schedule="@monthly",
    start_date=datetime(2019, 10, 1),
    end_date=datetime(2019, 11, 30),
    catchup=False,                      # months are loaded on purpose, via backfill
    max_active_runs=1,                  # one month at a time (SCD2 must go in date order)
    default_args={"retries": 2, "retry_delay": timedelta(minutes=1)},
    tags=["retail", "lakehouse"],
)
def retail_monthly_load():

    @task
    def check_raw_file(data_interval_start=None):
        """Fail fast with a clear message if this month's CSV isn't there."""
        name = data_interval_start.strftime("%Y-%b") + ".csv"
        path = os.path.join(PROJECT, "data", "raw", name)
        if not os.path.exists(path):
            raise AirflowFailException(f"Raw file missing: {path}")   # no point retrying
        size_gb = os.path.getsize(path) / 1e9
        print(f"Found {name}: {size_gb:.2f} GB")
        return name

    bronze = BashOperator(
        task_id="bronze",
        bash_command=f"python to_bronze.py data/raw/{MONTH_FILE}",
        cwd=PROJECT,
    )

    # Part B placeholders: the shape of the full pipeline, filled in next session.
    upload = EmptyOperator(task_id="upload_to_databricks")
    silver = EmptyOperator(task_id="silver_replace_where")
    scd2 = EmptyOperator(task_id="scd2_merge")
    dbt_build = EmptyOperator(task_id="dbt_build")

    check_raw_file() >> bronze >> upload >> silver >> scd2 >> dbt_build


retail_monthly_load()
