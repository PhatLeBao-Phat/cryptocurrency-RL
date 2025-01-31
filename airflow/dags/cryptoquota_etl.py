from airflow.decorators import dag, task
from airflow.operators.python import PythonVirtualenvOperator
import logging
from pendulum import datetime, duration
import os

os.chdir("/opt/")

# Airflow task logger
t_log = logging.getLogger("airflow.task")

# Define parameters
_QUERY_PATH = "queries/init_cryptoquota.sql"

# Parse requirements
with open("etl_pipelines/crypto_etl_requirements.txt") as f:
    requirements = f.read().splitlines()


# -------------- #
# DAG Definition #
# -------------- #
@dag(
    start_date=datetime(2024, 7, 1),
    schedule="@daily",
    catchup=False,
    max_consecutive_failed_dag_runs=5,
    max_active_runs=1,
    doc_md=__doc__,
    default_args={
        "owner": "Astro",
        "retries": 3,
        "retry_delay": duration(seconds=30),
    },
    tags=["example", "ETL"],
    concurrency=1,
    # is_paused_upon_creation=False,
)
def crypto_currency_quota_etl():
    # ---------------- #
    # Task Definitions #
    # ---------------- #

    @task(retries=2)
    def create_quota_table_in_postgres(query=_QUERY_PATH):
        print("Creating quota table in Postgres.")
        t_log.info(f"Executing query from: {query}")

    @task.virtualenv(
        task_id="cryptocurrency",
        requirements=requirements,
        system_site_packages=False,
    )
    def cryptocurrency_etl():
        from etl_pipelines.crypto_pipeline import (
            CryptoPipeline, 
            QuotaPipeline,
        )
        pipe = CryptoPipeline()
        pipe.run()


    # Task dependencies
    create_quota_table = create_quota_table_in_postgres()
    cryptocurrency_etl = cryptocurrency_etl()
    create_quota_table >>  cryptocurrency_etl


# Instantiate the DAG
crypto_currency_quota_etl()
