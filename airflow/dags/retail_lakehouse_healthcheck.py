from datetime import datetime

from airflow.sdk import DAG, task


with DAG(
    dag_id="retail_lakehouse_healthcheck",
    description="Validates the RetailLakehouse Airflow environment",
    start_date=datetime(2026, 1, 1),
    schedule=None,
    catchup=False,
    tags=["retail-lakehouse", "portfolio"],
):

    @task
    def environment_check():
        print("RetailLakehouse Airflow environment is running successfully.")
        return "healthy"

    environment_check()