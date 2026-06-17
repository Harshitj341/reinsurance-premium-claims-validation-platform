"""
L&H Reinsurance - Bronze Medallion Pipeline
===========================================
This DAG orchestrates the movement of raw seriatim data from the MinIO 
landing zone into the structured Bronze layer (Delta format).

Architecture:
- Event-Driven: Uses TriggerDagRunOperator to decouple the Medallion layers,
  allowing the Silver Staging pipeline to run asynchronously immediately after
  Bronze processing completes.
"""

from datetime import datetime
from airflow import DAG
from airflow.operators.bash import BashOperator
from airflow.operators.trigger_dagrun import TriggerDagRunOperator

default_args = {
    "owner": "data_engineering",
    "retries": 1
}

with DAG(
    dag_id="bronze_ingestion_pipeline",
    default_args=default_args,
    start_date=datetime(2024, 1, 1),
    schedule_interval=None,
    catchup=False,
    tags=["bronze", "delta_lake", "reinsurance"]
) as dag:

    # Execute the core PySpark worker script natively inside the Airflow container
    run_bronze_pyspark_job = BashOperator(
        task_id="run_bronze_ingestion_script",
        bash_command="python /opt/airflow/dags/scripts/bronze_ingest.py"
    )

    # Trigger the downstream Silver Staging Medallion DAG upon success
    trigger_silver_medallion = TriggerDagRunOperator(
        task_id="trigger_silver_staging_pipeline",
        trigger_dag_id="silver_staging_dag", 
        wait_for_completion=False
    )

    # Dependency Chain
    run_bronze_pyspark_job >> trigger_silver_medallion