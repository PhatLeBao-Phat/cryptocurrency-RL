from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.python import PythonOperator

# Default arguments for the DAG
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
    dag_id="example_dag",  # Unique identifier for the DAG
    default_args=default_args,  # Apply default arguments
    description="A simple example DAG",  # Short description
    schedule_interval=timedelta(days=1),  # Schedule: Daily
    start_date=datetime(2023, 1, 1),  # Start date
    catchup=False,  # Don't run tasks for missed intervals
    tags=["example"],  # Tags for organizing DAGs
) as dag:

    # Task 1: Start task
    def start_task():
        print("Starting the DAG!")

    start = PythonOperator(
        task_id="start_task",  # Unique ID for the task
        python_callable=start_task,  # Function to call
    )

    # Task 2: Process data
    def process_data():
        print("Processing data...")

    process = PythonOperator(
        task_id="process_data",  # Unique ID for the task
        python_callable=process_data,  # Function to call
    )

    # Task 3: End task
    def end_task():
        print("Ending the DAG!")

    end = PythonOperator(
        task_id="end_task",  # Unique ID for the task
        python_callable=end_task,  # Function to call
    )

    # Define the task dependencies
    start >> process >> end
