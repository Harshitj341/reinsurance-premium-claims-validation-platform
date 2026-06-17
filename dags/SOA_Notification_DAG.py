"""
DAG Architecture: Statement of Account (SOA) Operations Alert Engine
Domain: Reinsurance Life & Health Operations Validation

Description:
    Queries daily ledger system inputs to flag periods lacking binding 
    financial control entries[cite: 6]. Isolates discrepancies such as rejected 
    records or unmapped bookkeeping layers and compiles an operational report 
    for manual ledger entry remediation[cite: 6].
"""

import os
import smtplib
import logging
from datetime import datetime, timedelta
from collections import defaultdict
from email.mime.text import MIMEText

from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.providers.postgres.hooks.postgres import PostgresHook

# ==============================================================================
# LOGGING & CORE CONFIGURATION ENVIRONMENT VARIABLES
# ==============================================================================
logger = logging.getLogger("airflow.task")

POSTGRES_CONN_ID = os.getenv("AIRFLOW_CONN_POSTGRES_DEFAULT", "postgres_default")
SMTP_HOST = os.getenv("SMTP_HOST", "smtp.gmail.com")
SMTP_PORT = int(os.getenv("SMTP_PORT", 587))
UI_BASE_URL = os.getenv("PLATFORM_UI_BASE_URL", "http://localhost:5050")

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
    """Dispatches operational action sheets via standard corporate SMTP loop.[cite: 6]"""
    smtp_user = os.environ.get("AIRFLOW__SMTP__SMTP_USER")
    smtp_pass = os.environ.get("AIRFLOW__SMTP__SMTP_PASSWORD")

    if not smtp_user or not smtp_pass:
        raise ValueError("Critical Security Alert: Airflow SMTP host credentials are not configured.[cite: 6]")

    msg = MIMEText(body, "plain", "utf-8")
    msg["Subject"] = subject
    msg["From"] = smtp_user
    msg["To"] = smtp_user

    with smtplib.SMTP(SMTP_HOST, SMTP_PORT) as server:
        server.starttls()
        server.login(smtp_user, smtp_pass)
        server.send_message(msg)


def send_soa_notifications():
    """Identifies file tracking anomalies and compiles daily operational alert summaries.[cite: 6]"""
    hook = PostgresHook(postgres_conn_id=POSTGRES_CONN_ID)

    # Recover reporting blocks tracked within the platform today
    query_today_uploads = """
        SELECT DISTINCT
            client_id,
            year,
            quarter
        FROM file_tracking
        WHERE uploaded_at >= CURRENT_DATE
          AND uploaded_at < CURRENT_DATE + INTERVAL '1 day'
        ORDER BY client_id, year, quarter
    """
    periods = hook.get_records(query_today_uploads)

    if not periods:
        logger.info("No modern business tracking uploads detected inside the past execution window.[cite: 6]")
        return

    grouped = defaultdict(list)

    # Safe row unpacking using structured field names
    for row in periods:
        client_id, year, quarter = row[0], row[1], row[2]

        latest_soa_query = """
            SELECT status
            FROM soa_entries
            WHERE client_id = %s
              AND year = %s
              AND period = %s
            ORDER BY version DESC
            LIMIT 1
        """
        latest_soa = hook.get_first(
            latest_soa_query,
            parameters=(client_id, str(year), quarter),
        )

        if latest_soa:
            status = latest_soa[0]

            # Bypass processing if the record has already reached a finalized state
            if status in ("PENDING_APPROVAL", "APPROVED", "DUPLICATE"):
                continue

            if status == "RECONCILED":
                logger.info(f"Account: {client_id} for Window: {quarter}-{year} matches validation limits. Skipping alert.[cite: 6]")
                continue

            if status == "REJECTED":
                grouped[client_id].append((year, quarter, "REJECTED - RESUBMISSION REQUIRED"))[cite: 6]
                continue
        else:
            # Explicit default fallback if no statement of account can be fetched
            grouped[client_id].append((year, quarter, "READY_FOR_SOA_ENTRY"))[cite: 6]

    actionable = {client: entries for client, entries in grouped.items() if entries}[cite: 6]

    if not actionable:
        logger.info("All data ingestion tracks balanced. No operational actions required today.[cite: 6]")
        return

    # Compile the final operational email notification body
    lines = [
        "Statement of Account (SOA) Operational Action Sheet",
        "",
        "Action Dashboard Location:",
        f"{UI_BASE_URL}/soa",
        "",
        "═══════════════════════════════════════════════════════════════",
    ]

    for client_id, entries in actionable.items():
        lines.append(f"\nAccount Identifier: {client_id}")
        for year, quarter, status in entries:
            lines.append(f"  [{quarter} / {year}]  ⟶  {status}")

    send_email(
        "[RI Platform] SOA Action Required",
        "\n".join(lines),
    )[cite: 6]

    logger.info(f"Dispatched accounting alert bundle for {len(actionable)} target client profiles.[cite: 6]")


# ==============================================================================
# DAG WORKFLOW ORCHESTRATION BLOCK
# ==============================================================================
with DAG(
    dag_id="soa_notification_dag",
    description="Alerts operational desks when missing Statement of Account entries block financial consolidation[cite: 6]",
    default_args=default_args,
    start_date=datetime(2025, 1, 1),
    schedule_interval="@daily",
    catchup=False,
    max_active_runs=1,
    tags=["soa", "notification", "reinsurance"],
) as dag:

    notify_task = PythonOperator(
        task_id="send_soa_notifications",
        python_callable=send_soa_notifications,
    )[cite: 6]