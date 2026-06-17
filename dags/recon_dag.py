"""
DAG Name: Financial Reconciliation Orchestration Engine (recon_dag)
Domain: Reinsurance Life & Health Ledgering
Data Architecture Tier: Gold Core Business Layer

Description:
    Triggers the business rules process matching audited Silver validated elements 
    with legally verified Statement of Account (SOA) transactions. Confirmed entries 
    are promoted into the analytical Gold target layer to serve analytical query tools.
"""

import os
import sys
import logging
from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.bash import BashOperator

# ==============================================================================
# SYSTEM LOGGING LOGIC
# ==============================================================================
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
    handlers=[logging.StreamHandler(sys.stdout)]
)
logger = logging.getLogger("airflow.dag.recon_dag")

# ==============================================================================
# DECLARATIVE WORKFLOW CORE ARGUMENTS
# ==============================================================================
default_args = {
    "owner": "platform_data_engineering",
    "depends_on_past": False,
    "email_on_failure": False,
    "email_on_retry": False,
    "retries": 1,
    "retry_delay": timedelta(minutes=5),
}

# ==============================================================================
# WORKFLOW GRAPH DEFINITION
# ==============================================================================
with DAG(
    dag_id="recon_dag",
    description="Reconciles verified Silver data against approved structural SOA balances for Gold promotion.",
    default_args=default_args,
    start_date=datetime(2026, 1, 1),
    schedule_interval="@daily",
    catchup=False,
    max_active_runs=1,
    tags=["financial_reconciliation", "gold_promotion", "reinsurance_analytics"],
) as dag:

    logger.info("Building DAG structural execution nodes for recon_dag workflow context.")

    # Execution Task: Runs the core financial reconciliation processing script
    run_recon = BashOperator(
        task_id="run_reconciliation",
        bash_command="python /opt/airflow/dags/scripts/recon.py",
        execution_timeout=timedelta(hours=2),
        env={**os.environ},
        append_env=True,
    )

    # Defined Orchestration Topologies
    run_recon