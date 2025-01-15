######################################
##### CRYPTOGRAPHY PIPELINE ##########
######################################

# Import 
from datetime import datetime, timedelta
from airflow import DAG 
from airflow.operators.python import PythonOperator
import os
import sys

# Handle import
grandparent_dir = os.path.abspath(os.path.join(os.path.dirname(__file__), '..', '..'))
sys.path.append(grandparent_dir)

# Local import
from etl_basic.crypto_pipeline import (
    CryptoPipeline, 
    # QuotaPipeline,
)
from dev.utils.logging import *

# if __name__ == "__main__":
#     # Fetching Crypto symbol data
#     logger.info("TRIGGER CRYPTO SYMBOL PIPELINE...")
#     pipe = CryptoPipeline()
#     pipe.run()
#     logger.info("FINISHED CRYPTO SYMBOL PIPELINE")

#     # Fetching quotation for cryptocurrency
#     logger.info("TRIGGER CRYPTO QUOTATOIN PIPELINE...")
#     pipe = QuotaPipeline()
#     pipe.run()
#     logger.info("FINISHED CRYPTO QUOTATON PIPELINE")

default_args = {
    "owner": "airflow",  # Default owner
    "depends_on_past": False,  # Task doesn't depend on previous runs
    "email_on_failure": False,  # Disable email notifications
    "email_on_retry": False,  # Disable email notifications on retry
    "retries": 1,  # Number of retries
    "retry_delay": timedelta(minutes=5),  # Time between retries
}

# Define the DAG
with DAG(
    dag_id="cryptography_load",  # Unique identifier for the DAG
    default_args=default_args,  # Apply default arguments
    description="A simple example DAG",  # Short description
    schedule_interval=timedelta(days=1),  # Schedule: Daily
    start_date=datetime(2023, 1, 1),  # Start date
    catchup=False,  # Don't run tasks for missed intervals
    tags=["example"],  # Tags for organizing DAGs
) as dag:
    def task():
        print("Start task")
        logger.info("TRIGGER CRYPTO SYMBOL PIPELINE...")
        pipe = CryptoPipeline()
        pipe.run()
        logger.info("FINISHED CRYPTO SYMBOL PIPELINE")
