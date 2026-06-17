"""
DAG Architecture: Silver Layer Validation & Orchestration Workflow
Domain: Reinsurance Life & Health Core Pipeline Platform

Description:
    Orchestrates post-staging data quality gates by executing the core 
    actuarial rules engine script, evaluating systemic data quality metrics, 
    and generating operational validation summary balance sheets[cite: 5].
    
    Acts as the programmatic gatekeeper determining whether to halt processing 
    due to data corruption or trigger downstream financial reconciliation[cite: 5].
"""

import os
import smtplib
import logging
from datetime import datetime, timedelta
from collections import defaultdict
from email.mime.text import MIMEText

from airflow import DAG
from airflow.operators.bash import BashOperator
from airflow.operators.python import PythonOperator
from airflow.providers.postgres.hooks.postgres import PostgresHook
from airflow.operators.trigger_dagrun import TriggerDagRunOperator

# ==============================================================================
# LOGGING & CORE CONFIGURATION ENVIRONMENT VARIABLES
# ==============================================================================
logger = logging.getLogger("airflow.task")

POSTGRES_CONN_ID = os.getenv("AIRFLOW_CONN_POSTGRES_DEFAULT", "postgres_default")
SMTP_HOST = os.getenv("SMTP_HOST", "smtp.gmail.com")
SMTP_PORT = int(os.getenv("SMTP_PORT", 587))

# Default Operational Directives for DAG Tasks
default_args = {
    "owner": "airflow",
    "depends_on_past": False,
    "email_on_failure": False,
    "retries": 1,
    "retry_delay": timedelta(minutes=5),
}

# ==============================================================================
# AUXILIARY UTILITY OPERATIONS
# ==============================================================================

def send_email(subject, body):
    """Dispatches formal platform operational logs to engineering teams via SMTP."""
    smtp_user = os.environ.get("AIRFLOW__SMTP__SMTP_USER")
    smtp_pass = os.environ.get("AIRFLOW__SMTP__SMTP_PASSWORD")
    
    if not smtp_user or not smtp_pass:
        raise ValueError("Critical Security Alert: Airflow SMTP host credentials are not configured.")
        
    msg = MIMEText(body, "plain", "utf-8")
    msg["Subject"] = subject
    msg["From"] = smtp_user
    msg["To"] = smtp_user
    
    with smtplib.SMTP(SMTP_HOST, SMTP_PORT) as server:
        server.starttls()
        server.login(smtp_user, smtp_pass)
        server.send_message(msg)


def send_validation_summary(**context):
    """Parses execution validation telemetry blocks and dispatches system performance metrics[cite: 5]."""
    run_id = context["dag_run"].conf.get("silver_run_id")
    if not run_id:
        logger.warning("Execution tracking context missing: 'silver_run_id' absent from execution conf. Skipping run analysis.")
        return

    hook = PostgresHook(postgres_conn_id=POSTGRES_CONN_ID)

    # Extract granular audit metadata points generated during the evaluation phase
    sql_query = """
        SELECT client_id, treaty_id, file_name, category,
               check_name, status, failed_count, total_count, message
        FROM validation_results
        WHERE run_id = %s
        ORDER BY client_id, file_name, check_name
    """
    rows = hook.get_records(sql_query, parameters=(run_id,))

    if not rows:
        logger.warning(f"No validation telemetry records recovered for active tracing sequence: {run_id}")
        return

    # Calculate system processing health indexes
    total = len(rows)
    passed = sum(1 for r in rows if r[5] == 'PASS')
    failed = sum(1 for r in rows if r[5] == 'FAIL')
    warned = sum(1 for r in rows if r[5] == 'WARN')
    skipped = sum(1 for r in rows if r[5] == 'SKIPPED')

    # Structure data profiles grouped cleanly by Client, Contract, and File context signatures
    grouped = defaultdict(list)
    for row in rows:
        client_id, treaty_id, file_name, category = row[0], row[1], row[2], row[3]
        grouped[(client_id, treaty_id, file_name, category)].append(row)

    # Format the Operational Evaluation Summary Report Body
    lines = [
        f"Run Execution ID : {run_id}",
        "",
        f"Metrics Index    : {total} Core Rules Evaluated | "
        f"PASS {passed} | FAIL {failed} | "
        f"WARN {warned} | SKIPPED {skipped}",
        "",
        "═════════════════════════════════════════════════════════════════════",
    ]

    for (client_id, treaty_id, file_name, category), checks in grouped.items():
        lines.append(f"\nContext Group: {client_id} / {treaty_id} / {file_name} [{category.upper()}]")
        
        for r in checks:
            # Explicit tuple unpacking ensures safety against column order variances
            _, _, _, _, check_name, status, failed_count, total_count, msg = r
            
            indicator = {"PASS": "✓", "FAIL": "✗", "WARN": "⚠", "SKIPPED": "—"}.get(status, "?")
            line = f"  {indicator} {check_name:<30} {status}"
            
            if failed_count:
                line += f"  ({failed_count}/{total_count} anomalous records discovered)"
            if msg and status != "PASS":
                line += f"  — Diagnostics: {msg}"
            lines.append(line)

    subject = f"[RI Platform] Automated Data Quality Validation {'FAILED' if failed > 0 else 'PASSED'} — Run {run_id}"
    send_email(subject, "\n".join(lines))
    logger.info(f"System metrics summary transmitted successfully for run track: {run_id}")

# ==============================================================================
# DAG WORKFLOW ORCHESTRATION BLOCK
# ==============================================================================
with DAG(
    dag_id="silver_validation_dag",
    description="Orchestrates data quality controls and rules verification post core staging[cite: 5]",
    default_args=default_args,
    start_date=datetime(2025, 1, 1),
    schedule_interval=None,
    catchup=False,
    max_active_runs=1,
    tags=["silver", "validation", "reinsurance"],
) as dag:

    # Task 1: Execute the analytical Spark engine to process validation rules
    run_validation_task = BashOperator(
        task_id="run_silver_validation",
        bash_command="python /opt/airflow/dags/scripts/silver_validation.py",
        env={
            "SILVER_RUN_ID": "{{ dag_run.conf.get('silver_run_id', '') }}"
        }
    )

    # Task 2: Collate diagnostic results and report back platform performance metrics
    send_summary_task = PythonOperator(
        task_id="send_validation_summary",
        python_callable=send_validation_summary,
    )
    
    # Task 3: Signal downstream notification engine workflows upon verification completion
    trigger_soa_notification = TriggerDagRunOperator(
        task_id="trigger_soa_notification",
        trigger_dag_id="soa_notification_dag",
        wait_for_completion=False,
    )

    # Pipeline Sequence Topography
    run_validation_task >> send_summary_task >> trigger_soa_notification