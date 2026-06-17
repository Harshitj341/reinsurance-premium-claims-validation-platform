"""
L&H Reinsurance - Mapping Review Reminder DAG
=============================================
Orchestrates periodic scans of the mapping_review_queue table to identify
pending schema drift issues and notifies the Data Engineering team.

Schedule: Every 3 hours.
"""

import logging
import os
import smtplib
from datetime import datetime, timedelta
from collections import defaultdict
from email.mime.text import MIMEText

from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.providers.postgres.hooks.postgres import PostgresHook
from airflow.models import Variable

# ============================================================
# CONFIGURATION
# ============================================================
POSTGRES_CONN_ID = "postgres_default"
SMTP_HOST = "smtp.gmail.com"
SMTP_PORT = 587
REVIEW_URL = Variable.get("mapping_review_url", default_var="http://localhost:5050")

logger = logging.getLogger(__name__)

default_args = {
    "owner": "airflow",
    "depends_on_past": False,
    "email_on_failure": False,
    "retries": 1,
    "retry_delay": timedelta(minutes=5),
}

# ============================================================
# EMAIL UTILITY
# ============================================================
def send_email(subject: str, body: str):
    """Sends SMTP notifications using environment-configured credentials."""
    smtp_user = os.environ.get("AIRFLOW__SMTP__SMTP_USER")
    smtp_pass = os.environ.get("AIRFLOW__SMTP__SMTP_PASSWORD")

    if not smtp_user or not smtp_pass:
        logger.error("SMTP credentials missing. Aborting email notification.")
        return

    msg = MIMEText(body)
    msg["Subject"] = subject
    msg["From"] = smtp_user
    msg["To"] = smtp_user

    try:
        with smtplib.SMTP(SMTP_HOST, SMTP_PORT) as server:
            server.starttls()
            server.login(smtp_user, smtp_pass)
            server.send_message(msg)
        logger.info(f"Notification sent successfully: {subject}")
    except Exception as e:
        logger.error(f"SMTP error: {e}")

# ============================================================
# TASK LOGIC
# ============================================================
def process_reminders():
    """Scans mapping_review_queue for pending items and summarizes by client."""
    hook = PostgresHook(postgres_conn_id=POSTGRES_CONN_ID)
    
    # Query pending mapping review items
    query = "SELECT client_id, file_name, raw_col_name FROM mapping_review_queue WHERE status = 'PENDING'"
    records = hook.get_records(query)
    
    if not records:
        logger.info("No pending mapping reviews found.")
        return

    # Group issues by client
    grouped = defaultdict(list)
    for client, file, col in records:
        grouped[client].append(f"File: {file} | Column: {col}")

    lines = ["SOA/Mapping Notification Summary", "", f"Review Dashboard: {REVIEW_URL}", "═══════════════════════════════"]
    for client, issues in grouped.items():
        lines.extend(["", f"Client: {client}"])
        lines.extend([f"  - {i}" for i in issues])

    send_email("[RI Platform] Mapping Action Required", "\n".join(lines))

# ============================================================
# DAG DEFINITION
# ============================================================
with DAG(
    dag_id="reminder_dag",
    description="Notifies mapping team of pending review items",
    default_args=default_args,
    start_date=datetime(2025, 1, 1),
    schedule_interval="0 */3 * * *",
    catchup=False,
    max_active_runs=1,
    tags=["reinsurance", "mapping", "reminders"]
) as dag:

    PythonOperator(
        task_id="send_reminders",
        python_callable=process_reminders
    )