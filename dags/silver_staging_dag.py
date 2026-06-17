"""
L&H Reinsurance - Silver Staging & Mapping Audit DAG
====================================================
This DAG acts as the Data Quality and Schema Drift gateway for the Silver Medallion layer.

Key Architectural Features:
1. Memory-Safe Header Parsing: Uses PyArrow and Pandas to stream *only* file headers 
   from S3/MinIO to evaluate schema drift without loading massive datasets into memory.
2. Human-in-the-Loop (HITL): Unmapped columns dynamically pause downstream execution 
   and push records to a Flask UI review queue.
3. Dynamic Alerting: Generates summary CSVs of DQ exceptions and emails them to Ops.
"""

import os
import io
import csv
import logging
import smtplib
from collections import defaultdict
from datetime import datetime, timedelta
from email import encoders
from email.mime.base import MIMEBase
from email.mime.multipart import MIMEMultipart
from email.mime.text import MIMEText

import pandas as pd
import pyarrow.parquet as pq
from pyarrow.fs import S3FileSystem

from airflow import DAG
from airflow.models import Variable
from airflow.operators.bash import BashOperator
from airflow.operators.python import PythonOperator
from airflow.operators.trigger_dagrun import TriggerDagRunOperator
from airflow.providers.amazon.aws.hooks.s3 import S3Hook
from airflow.providers.postgres.hooks.postgres import PostgresHook

# ============================================================
# CONFIGURATION & LOGGING
# ============================================================
logger = logging.getLogger(__name__)

POSTGRES_CONN_ID = "postgres_default"
AWS_CONN_ID = "aws_default"
SMTP_HOST = "smtp.gmail.com"
SMTP_PORT = 587

# Fetch dynamic variables securely
OPS_EMAIL = Variable.get("ops_alert_email", default_var="reinsurance-ops@company.local")
REVIEW_URL = Variable.get("mapping_review_url", default_var="http://localhost:5050")

default_args = {
    "owner": "data_engineering",
    "depends_on_past": False,
    "email_on_failure": False,
    "retries": 1,
    "retry_delay": timedelta(minutes=5),
}

# ============================================================
# UTILITY FUNCTIONS
# ============================================================
def get_silver_run_id(**context) -> str:
    """Extracts the specific silver_run_id from the upstream PySpark BashOperator stdout."""
    xcom_value = context["ti"].xcom_pull(task_ids="run_silver_staging")
    if not xcom_value:
        raise ValueError("Silver staging did not return a run_id.")
    
    marker = "SILVER_RUN_ID="
    if marker not in str(xcom_value):
        raise ValueError(f"Could not locate {marker} in task output.")
        
    return str(xcom_value).split(marker, 1)[1].strip()

def send_email(subject: str, body: str, attachment: bytes = None, attachment_name: str = None):
    """Custom SMTP wrapper to handle dynamic CSV exception attachments."""
    smtp_user = os.environ.get("AIRFLOW__SMTP__SMTP_USER")
    smtp_pass = os.environ.get("AIRFLOW__SMTP__SMTP_PASSWORD")

    if not smtp_user or not smtp_pass:
        logger.warning("SMTP credentials missing. Email suppressed.")
        return

    msg = MIMEMultipart() if attachment else MIMEText(body)
    msg["Subject"] = subject
    msg["From"] = smtp_user
    msg["To"] = OPS_EMAIL

    if attachment:
        msg.attach(MIMEText(body))
        part = MIMEBase("application", "octet-stream")
        part.set_payload(attachment)
        encoders.encode_base64(part)
        part.add_header("Content-Disposition", f'attachment; filename="{attachment_name}"')
        msg.attach(part)

    with smtplib.SMTP(SMTP_HOST, SMTP_PORT) as server:
        server.starttls()
        server.login(smtp_user, smtp_pass)
        server.send_message(msg)

    logger.info("Operational email sent successfully: %s", subject)

def read_file_columns(s3_path: str) -> list:
    """
    Parses an S3 URI and streams *only* the headers to protect memory.
    Leverages Airflow's centralized connection manager for credentials.
    """
    clean_path = s3_path.replace("s3a://", "s3://")
    if not clean_path.startswith("s3://"):
        raise ValueError(f"Expected an S3 path, received: {s3_path}")
        
    bucket_name, key = clean_path.replace("s3://", "").split("/", 1)
    s3_hook = S3Hook(aws_conn_id=AWS_CONN_ID)
    creds = s3_hook.get_credentials()

    if clean_path.endswith(".csv"):
        file_obj = s3_hook.get_key(key, bucket_name)
        initial_bytes = file_obj.get()["Body"].read(50000) # Pull 50KB chunk buffer
        df = pd.read_csv(io.BytesIO(initial_bytes), nrows=0, on_bad_lines='skip')
        return [col.strip() for col in df.columns]

    elif clean_path.endswith(".parquet"):
        pyarrow_fs = S3FileSystem(
            access_key=creds.access_key,
            secret_key=creds.secret_key,
            token=creds.token,
            region=s3_hook.region_name
        )
        schema = pq.read_schema(f"{bucket_name}/{key}", filesystem=pyarrow_fs)
        return [col.strip() for col in schema.names]

    raise ValueError(f"Unsupported file format for path: {s3_path}")

# ============================================================
# TASK CALLABLES
# ============================================================
def audit_and_check_mapping(**context):
    postgres_hook = PostgresHook(postgres_conn_id=POSTGRES_CONN_ID)
    ready_batches, blocked_batches = [], []
    consolidated_missing = defaultdict(list)

    logger.info("Initializing Schema Drift & Mapping Audit...")

    files_query = """
        SELECT bil.file_hash, bil.client_id, bil.file_name, bil.year, bil.quarter, bil.category
        FROM bronze_ingestion_log bil
        LEFT JOIN silver_staging_log ssl ON bil.file_hash = ssl.file_hash
        WHERE (ssl.file_hash IS NULL OR ssl.status = 'FAILED') AND bil.status = 'SUCCESS'
    """
    files = postgres_hook.get_records(files_query)
    
    for row in files:
        file_hash, client_id, file_name, year, quarter, category = row
        landing_path = f"s3a://landing/{client_id}/{year}/{quarter}/{file_hash}/{file_name}"
        
        try:
            columns = read_file_columns(landing_path)
            
            mapping_query = """
                SELECT raw_col_name FROM column_mapping
                WHERE client_id = %s AND category = %s AND effective_to IS NULL
            """
            mapped_rows = postgres_hook.get_records(mapping_query, parameters=(client_id, category))
            mapped_columns = {r[0].strip() for r in mapped_rows}
            
            missing_mappings = sorted(list(set(columns) - mapped_columns))

            if missing_mappings:
                logger.warning(f"Schema Drift: Missing mappings for {file_name}")
                insert_review_queue_query = """
                    INSERT INTO mapping_review_queue (
                        client_id, file_name, category, raw_col_name, status, created_at
                    )
                    SELECT %s, %s, %s, %s, 'PENDING', NOW()
                    WHERE NOT EXISTS (
                        SELECT 1 FROM mapping_review_queue
                        WHERE client_id = %s AND category = %s AND raw_col_name = %s
                          AND status IN ('PENDING', 'AFFECTS_CALCULATIONS')
                    )
                """
                for missing_col in missing_mappings:
                    postgres_hook.run(
                        insert_review_queue_query,
                        parameters=(client_id, file_name, category, missing_col, client_id, category, missing_col)
                    )
                consolidated_missing[file_name].extend(missing_mappings)
                blocked_batches.append({"file_hash": file_hash, "client_id": client_id, "file_name": file_name})
            else:
                insert_queue_query =  """
                    INSERT INTO silver_run_queue (
                        file_hash, client_id, category, file_name, year, quarter, period, period_type, queued_at
                    ) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, NOW())
                    ON CONFLICT (file_hash) DO NOTHING
                """
                postgres_hook.run(
                    insert_queue_query,
                    parameters=(file_hash, client_id, category, file_name, year, quarter, quarter, 'quarterly')
                )
                ready_batches.append({"file_hash": file_hash, "client_id": client_id, "file_name": file_name})

        except Exception as e:
            logger.error(f"Error processing {file_name}: {e}")
            blocked_batches.append({"file_hash": file_hash, "file_name": file_name, "reason": str(e)})

    if consolidated_missing:
        body = ["Schema Drift Detected - Files Blocked\n", "="*35 + "\n"]
        for fname, cols in consolidated_missing.items():
            body.append(f"\n{fname}")
            for col in sorted(set(cols)): body.append(f"  - {col}")
        body.append(f"\n\nResolve mappings via HITL UI:\n{REVIEW_URL}")
        send_email(subject="[RI Platform] Schema Drift Alert", body="\n".join(body))

    context["ti"].xcom_push(key="ready_batches", value=ready_batches)

def send_summary(**context):
    postgres_hook = PostgresHook(postgres_conn_id=POSTGRES_CONN_ID)
    
    try:
        run_id = get_silver_run_id(**context)
    except ValueError:
        logger.warning("No Run ID published by staging. Skipping summary.")
        return

    summary = postgres_hook.get_first("""
        SELECT COUNT(*), 
               COUNT(*) FILTER (WHERE status = 'SUCCESS'), 
               COUNT(*) FILTER (WHERE status = 'FAILED'),
               COALESCE(SUM(rows_to_silver), 0), 
               COALESCE(SUM(rows_quarantined), 0),
               MAX(processed_at)
        FROM silver_staging_log WHERE run_id = %s
    """, parameters=(run_id,))

    body = f"""
Run ID     : {run_id}
Run Time   : {summary[5]}

Summary:
  Total Files   : {summary[0]}
  Processed     : {summary[1]}
  Failed        : {summary[2]}

Rows:
  To Silver     : {summary[3]}
  Quarantined   : {summary[4]}
"""
    attachment, attachment_name = None, None

    if summary[4] > 0:
        quarantine_rows = postgres_hook.get_records("""
            SELECT client_id, file_name, category, dq_rule, row_count
            FROM silver_quarantine_summary WHERE run_id = %s
        """, parameters=(run_id,))
        
        if quarantine_rows:
            buf = io.StringIO()
            writer = csv.writer(buf)
            writer.writerow(["client_id", "file_name", "category", "dq_rule", "row_count"])
            writer.writerows(quarantine_rows)
            attachment = buf.getvalue().encode("utf-8")
            attachment_name = f"DQ_EXCEPTIONS_{run_id}.csv"
            body += f"\nSee attached: {attachment_name}\n"

    send_email(f"Silver Staging Complete — {run_id}", body, attachment, attachment_name)

# ============================================================
# DAG DEFINITION
# ============================================================
with DAG(
    dag_id="silver_staging_dag",
    description="Silver staging pipeline with mapping validation",
    default_args=default_args,
    start_date=datetime(2025, 1, 1),
    schedule_interval="@daily",
    catchup=False,
    max_active_runs=1,
    tags=["silver", "mapping", "reinsurance"],
) as dag:

    audit_task = PythonOperator(task_id="audit_and_check_mapping", python_callable=audit_and_check_mapping)
    staging_task = BashOperator(task_id="run_silver_staging", bash_command="python /opt/airflow/dags/scripts/silver_staging.py", do_xcom_push=True)
    summary_task = PythonOperator(task_id="send_summary", python_callable=send_summary)
    run_id_task = PythonOperator(task_id="get_silver_run_id", python_callable=get_silver_run_id)
    
    trigger_validation = TriggerDagRunOperator(
        task_id="trigger_silver_validation",
        trigger_dag_id="silver_validation_dag",
        conf={"silver_run_id": "{{ ti.xcom_pull(task_ids='get_silver_run_id') }}"},
        wait_for_completion=False,
    )

    audit_task >> staging_task >> summary_task >> run_id_task >> trigger_validation