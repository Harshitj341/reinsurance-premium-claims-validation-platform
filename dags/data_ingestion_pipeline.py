"""
L&H Reinsurance - Data Ingestion & Landing Pipeline (Sensor DAG)
================================================================
This pipeline acts as the file gateway for the Medallion architecture.
It utilizes an Airflow PythonSensor to monitor the `data_receipt` volume
for incoming Life & Health seriatim data. 

Key Architectural Features:
1. Idempotency: Implements a persistent MD5 hash store via PostgresHook 
   to deduplicate files before they enter the Bronze layer.
2. Object Storage: securely routes valid files to MinIO (S3) landing buckets.
3. Alerting: Uses dynamic Airflow Variables for Ops notifications.
"""

import os
import re
import hashlib
import logging
from datetime import datetime

import boto3
from botocore.client import Config
from botocore.exceptions import ClientError

from airflow import DAG
from airflow.models import Variable
from airflow.sensors.python import PythonSensor
from airflow.operators.python import PythonOperator
from airflow.operators.email import EmailOperator
from airflow.providers.postgres.hooks.postgres import PostgresHook

# ============================================================
# CONFIGURATION & LOGGING
# ============================================================
logger = logging.getLogger(__name__)

# Fetch ops email securely from Airflow variables, avoiding PII in source code
OPS_EMAIL = Variable.get("ops_alert_email", default_var="reinsurance-ops@company.local")
LANDING_ZONE_PATH = "/opt/airflow/data_receipt"

default_args = {
    "owner": "data_engineering",
    "retries": 1,
    "email": [OPS_EMAIL],
    "email_on_failure": True
}

# ============================================================
# UTILITY FUNCTIONS
# ============================================================
def compute_file_hash(file_path: str) -> str:
    """Generates an MD5 hash for file content to guarantee pipeline idempotency."""
    hash_md5 = hashlib.md5()
    with open(file_path, "rb") as f:
        for chunk in iter(lambda: f.read(4096), b""):
            hash_md5.update(chunk)
    return hash_md5.hexdigest()

# ============================================================
# TASK CALLABLES
# ============================================================
def check_for_files(**kwargs) -> bool:
    """Sensor callable: Returns True if unprocessed CSVs exist in the landing zone."""
    if not os.path.exists(LANDING_ZONE_PATH):
        logger.warning(f"Landing zone path {LANDING_ZONE_PATH} does not exist.")
        return False
        
    files = [f for f in os.listdir(LANDING_ZONE_PATH) if f.endswith(".csv")]
    if files:
        logger.info(f"Sensor triggered: Detected {len(files)} new files.")
        return True
    return False

def detect_files(**kwargs) -> list:
    """Parses incoming file names to extract L&H domain metadata (Year, Quarter, Type)."""
    files_to_process = []
    
    for file_name in os.listdir(LANDING_ZONE_PATH):
        if not file_name.endswith(".csv"):
            continue
            
        file_path = os.path.join(LANDING_ZONE_PATH, file_name)
        
        # Regex to extract: [category]_[year]_[quarter].csv
        # Example: claims_2023_Q1.csv
        match = re.search(r"([a-zA-Z]+)_(\d{4})_(Q[1-4])", file_name)
        if match:
            category, year, quarter = match.groups()
            file_hash = compute_file_hash(file_path)
            
            files_to_process.append({
                "file_name": file_name,
                "file_path": file_path,
                "category": category.lower(),
                "year": year,
                "quarter": quarter,
                "file_hash": file_hash
            })
            logger.info(f"Parsed valid file metadata: {file_name}")
        else:
            logger.warning(f"File {file_name} failed nomenclature pattern match. Skipping.")
            
    # Push metadata to XCom for downstream tasks
    return files_to_process

def upload_to_minio(**kwargs) -> dict:
    """
    Evaluates MD5 hashes against the persistent Postgres store to block duplicates,
    then uploads valid payloads to MinIO object storage.
    """
    ti = kwargs["ti"]
    files = ti.xcom_pull(task_ids="detect_files")
    
    if not files:
        logger.info("No valid files available for upload. Terminating gracefully.")
        return {"total": 0, "uploaded": []}

    # Initialize connections using Airflow Hooks to prevent hardcoded credentials
    pg_hook = PostgresHook(postgres_conn_id="postgres_default")
    conn = pg_hook.get_conn()
    cursor = conn.cursor()
    
    # Note: Transitioning to S3Hook is recommended for prod, MinIO kwargs retained for local Docker
    s3 = boto3.client(
        "s3",
        endpoint_url="http://minio:9000",
        aws_access_key_id="minioadmin",
        aws_secret_access_key="minioadmin",
        config=Config(signature_version="s3v4")
    )

    bucket_name = "landing"
    try:
        s3.head_bucket(Bucket=bucket_name)
    except ClientError:
        s3.create_bucket(Bucket=bucket_name)
        logger.info(f"Created new MinIO bucket: {bucket_name}")

    uploaded = []

    for file in files:
        file_hash = file["file_hash"]
        
        # Idempotency Gate: Check persistent hash store
        cursor.execute("SELECT 1 FROM file_tracking WHERE file_hash = %s", (file_hash,))
        if cursor.fetchone():
            logger.warning(f"DUPLICATE DETECTED: File {file['file_name']} with hash {file_hash} already processed. Skipping.")
            continue

        # Extract mock client_id from directory structure (assuming /data_receipt/{client_id}/)
        # Fallback to 'UNKNOWN_CLIENT' if dropped directly in root
        parts = file["file_path"].split(os.sep)
        client_id = parts[-2] if len(parts) >= 2 and parts[-2] != "data_receipt" else "UNKNOWN_CLIENT"

        # Upload to Object Storage
        object_name = f"{client_id}/{file['year']}/{file['quarter']}/{file['file_name']}"
        s3.upload_file(file["file_path"], bucket_name, object_name)
        logger.info(f"Successfully uploaded {file['file_name']} to MinIO: {object_name}")

        # Commit state to Hash Store
        cursor.execute(
            """
            INSERT INTO file_tracking (file_name, year, quarter, file_hash, client_id)
            VALUES (%s, %s, %s, %s, %s)
            """,
            (file["file_name"], file["year"], file["quarter"], file_hash, client_id)
        )

        uploaded.append(f"{client_id}/{file['file_name']}")

    conn.commit()
    cursor.close()
    conn.close()

    return {
        "total": len(files),
        "uploaded": uploaded
    }

# ============================================================
# DAG DEFINITION
# ============================================================
with DAG(
    dag_id="data_ingestion_pipeline",
    start_date=datetime(2024, 1, 1),
    schedule_interval="@daily",
    catchup=False,
    default_args=default_args,
    tags=["ingestion", "landing", "reinsurance"]
) as dag:

    wait_for_files = PythonSensor(
        task_id="wait_for_files",
        python_callable=check_for_files,
        mode="reschedule",
        poke_interval=60
    )

    parse_file_metadata = PythonOperator(
        task_id="detect_files",
        python_callable=detect_files
    )

    upload_payloads = PythonOperator(
        task_id="upload_to_minio",
        python_callable=upload_to_minio
    )

    send_ops_summary = EmailOperator(
        task_id="send_summary_email",
        to=OPS_EMAIL,
        subject="[RI Platform] Ingestion Completed",
        html_content="""
        <h3>Ingestion Summary</h3>
        <p><b>Total Files Detected:</b> {{ ti.xcom_pull(task_ids='upload_to_minio')['total'] }}</p>
        <p><b>Successfully Uploaded (Deduplicated):</b> 
           {{ ti.xcom_pull(task_ids='upload_to_minio')['uploaded'] | length }}</p>
        <hr>
        <h4>Files Sent to Landing Zone:</h4>
        <ul>
        {% for file in ti.xcom_pull(task_ids='upload_to_minio')['uploaded'] %}
            <li>{{ file }}</li>
        {% endfor %}
        </ul>
        """
    )

    # Dependency Chain
    wait_for_files >> parse_file_metadata >> upload_payloads >> send_ops_summary