"""
Module: Gold Reconciliation & Financial Verification Engine
Architecture Layer: Medallion Architecture (Silver Validated -> Gold Financial Reporting)
Domain: Reinsurance Life & Health Financial Controls

Description:
    Performs automated end-of-period financial ledger balancing by validating 
    aggregate accounting rows against official statements of account (SOA).
    
    Ensures absolute ledger integrity using strict balancing criteria: 
    data sets passing financial tolerances are securely upserted into production 
    Gold Delta structures, while financial breaks trigger operational escalations[cite: 4].
"""

import os
import sys
import logging
import datetime
import traceback
import smtplib
from email.mime.text import MIMEText
from email.mime.multipart import MIMEMultipart
from minio import Minio

from pyspark.sql import SparkSession, Row
from pyspark.sql import functions as F
from delta import configure_spark_with_delta_pip
from delta.tables import DeltaTable

# ==============================================================================
# LOGGING SYSTEM SETUP
# ==============================================================================
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(name)s - %(message)s",
    handlers=[logging.StreamHandler(sys.stdout)]
)
logger = logging.getLogger("gold_reconciliation")

# ==============================================================================
# CONFIGURATION & INFRASTRUCTURE PARAMETERS
# ==============================================================================
PYTHON_BIN = os.getenv("PYTHON_BIN", "/usr/local/bin/python")
os.environ["PYSPARK_PYTHON"] = PYTHON_BIN
os.environ["PYSPARK_DRIVER_PYTHON"] = PYTHON_BIN

# Infrastructure Coordinates
MINIO_ENDPOINT = os.getenv("MINIO_ENDPOINT", "minio:9000")
MINIO_ACCESS_KEY = os.getenv("MINIO_ACCESS_KEY", "minioadmin")
MINIO_SECRET_KEY = os.getenv("MINIO_SECRET_KEY", "minioadmin")

DB_HOST = os.getenv("DB_HOST", "postgres")
DB_NAME = os.getenv("DB_NAME", "airflow")
DB_USER = os.getenv("DB_USER", "airflow")
DB_PASS = os.getenv("DB_PASS", "airflow")

JDBC_URL = f"jdbc:postgresql://{DB_HOST}:5432/{DB_NAME}"
JDBC_PROPS = {
    "user": DB_USER,
    "password": DB_PASS,
    "driver": "org.postgresql.Driver"
}

# Operational Escalation Targets
SMTP_HOST = os.getenv("SMTP_HOST", "smtp.gmail.com")
SMTP_PORT = int(os.getenv("SMTP_PORT", 587))
VARIANCE_THRESHOLD = float(os.getenv("RECON_VARIANCE_THRESHOLD", 1.0))

# ==============================================================================
# STORAGE & ALERTS AUXILIARY INITIALIZATION
# ==============================================================================
try:
    minio_client = Minio(
        MINIO_ENDPOINT, 
        access_key=MINIO_ACCESS_KEY, 
        secret_key=MINIO_SECRET_KEY, 
        secure=False
    )
    if not minio_client.bucket_exists("gold"):
        logger.info("Initializing target Gold production storage system bucket.")
        minio_client.make_bucket("gold")
except Exception as minio_init_err:
    logger.error(f"Storage boundary checks bypassed due to initialization variance: {str(minio_init_err)}")


def send_ops_email(subject, body):
    """Dispatches formal reconciliation balance sheets directly to operations."""
    smtp_user = os.environ.get("AIRFLOW__SMTP__SMTP_USER")
    smtp_pass = os.environ.get("AIRFLOW__SMTP__SMTP_PASSWORD")

    if not smtp_user or not smtp_pass:
        logger.warning("SMTP configuration keys absent from host environment. Skipping email dispatch.")
        return

    msg = MIMEMultipart()
    msg["Subject"] = subject
    msg["From"] = smtp_user
    msg["To"] = smtp_user
    msg.attach(MIMEText(body, "plain"))

    try:
        with smtplib.SMTP(SMTP_HOST, SMTP_PORT) as server:
            server.starttls()
            server.login(smtp_user, smtp_pass)
            server.send_message(msg)
        logger.info(f"Operations escalation message transmitted: {subject}")
    except Exception as smtp_err:
        logger.error(f"Failed to communicate metrics report over SMTP relay: {str(smtp_err)}")

# ==============================================================================
# PIPELINE ENTRY POINT LOGIC
# ==============================================================================

def main():
    run_id = os.environ.get("RECON_RUN_ID", "UNKNOWN_RUN")
    had_failure = False
    recon_results_pool = []

    # Initialize high-throughput analytical session structures
    builder = (
        SparkSession.builder
        .appName(f"Gold-Reconciliation-Engine_{run_id}")
        .master("local[*]")
        .config("spark.pyspark.python", PYTHON_BIN)
        .config("spark.pyspark.driver.python", PYTHON_BIN)
        .config(
            "spark.jars",
            ",".join([
                "/opt/airflow/jars/postgresql-42.7.3.jar",
                "/opt/airflow/jars/hadoop-aws-3.3.4.jar",
                "/opt/airflow/jars/aws-java-sdk-bundle-1.12.262.jar"
            ])
        )
        .config(
            "spark.driver.extraClassPath",
            "/opt/airflow/jars/postgresql-42.7.3.jar:"
            "/opt/airflow/jars/hadoop-aws-3.3.4.jar:"
            "/opt/airflow/jars/aws-java-sdk-bundle-1.12.262.jar"
        )
        .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension")
        .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog")
        .config("spark.hadoop.fs.s3a.endpoint", f"http://{MINIO_ENDPOINT}")
        .config("spark.hadoop.fs.s3a.access.key", MINIO_ACCESS_KEY)
        .config("spark.hadoop.fs.s3a.secret.key", MINIO_SECRET_KEY)
        .config("spark.hadoop.fs.s3a.path.style.access", "true")
        .config("spark.hadoop.fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem")
        .config("spark.sql.shuffle.partitions", "8")
        .config("spark.default.parallelism", "8")
        .config("spark.databricks.delta.schema.autoMerge.enabled", "true")
        .config("spark.sql.debug.maxToStringFields", "200")
    )

    spark = configure_spark_with_delta_pip(builder).getOrCreate()
    spark.sparkContext.setLogLevel("WARN")

    # Composite Key structures ensuring reliable merge updates across storage layers
    MERGE_KEYS = {
        "premium": ["policy_number", "benefit_id", "valuation_date", "policy_effective_date"],
        "claims":  ["claim_id", "policy_number", "benefit_id", "policy_effective_date"]
    }

    try:
        # 1. Recover latest active downstream validation execution path
        if run_id == "UNKNOWN_RUN":
            target_query = "(SELECT run_id FROM silver_staging_log WHERE status = 'SUCCESS' ORDER BY run_id DESC LIMIT 1) x"
            result = spark.read.jdbc(url=JDBC_URL, table=target_query, properties=JDBC_PROPS).collect()
            if not result:
                logger.info("No successful silver execution tracks discovered. Terminating cleanly.")
                spark.stop()
                sys.exit(0)
            run_id = result[0]["run_id"]

        logger.info(f"Reconciliation Engine active for Target Validation Trace ID: {run_id}")

        # 2. Extract operational groups involved in the current validation scope
        staging_log_query = f"""
            (SELECT DISTINCT client_id, category, year, period 
             FROM silver_staging_log 
             WHERE run_id = '{run_id}' AND status = 'SUCCESS') as staging_groups
        """
        groups = spark.read.jdbc(url=JDBC_URL, table=staging_log_query, properties=JDBC_PROPS).collect()

        if not groups:
            logger.info(f"No execution metadata blocks discovered for execution run: {run_id}. Terminating process loop.")
            spark.stop()
            sys.exit(0)

        # 3. Build optimized memory lookup structures for Approved Statements of Account
        soa_query = "(SELECT client_id, year, period, premium_soa, claims_soa FROM soa_entries WHERE status = 'APPROVED') as soa_block"
        soa_df = spark.read.jdbc(url=JDBC_URL, table=soa_query, properties=JDBC_PROPS).collect()
        soa_lookup = {(str(r["client_id"]), str(r["year"]), str(r["period"])): r for r in soa_df}

        # 4. Perform ledger balancing evaluations across matching financial records
        for grp in groups:
            client_id = str(grp["client_id"])
            category = str(grp["category"]).lower()
            year = str(grp["year"])
            period = str(grp["period"])

            silver_path = f"s3a://silver/validated/{client_id}/{category}/"
            gold_path = f"s3a://gold/{client_id}/{category}/"

            logger.info(f"Evaluating balance conditions for context: {client_id} | {category.upper()} | {year}-{period}")

            try:
                if not DeltaTable.isDeltaTable(spark, silver_path):
                    logger.warning(f"Storage path missing at staging node: {silver_path}")
                    recon_results_pool.append({
                        "run_id": run_id, "client_id": client_id, "category": category,
                        "year": year, "period": period, "soa_amount": 0.0, "file_amount": 0.0,
                        "variance": 0.0, "status": "NO_DATA", "checked_at": datetime.datetime.now()
                    })
                    continue

                df_silver = spark.read.format("delta").load(silver_path) \
                    .filter((F.col("year") == year) & (F.col("period") == period))

                # Aggregate financial data vectors
                if "premium" in category:
                    file_amount = df_silver.agg(F.sum("premium_amount")).collect()[0][0] or 0.0
                elif "claims" in category:
                    file_amount = df_silver.agg(F.sum("claim_amount")).collect()[0][0] or 0.0
                else:
                    logger.warning(f"Unrecognized business data entity layer encountered: {category}")
                    continue

                # Match against approved accounting control records
                soa_entry = soa_lookup.get((client_id, year, period))

                if not soa_entry:
                    status = "PENDING_SOA"
                    variance = 0.0
                    soa_amount = 0.0
                    reason = "No matching signed Statement of Account structure found."
                    had_failure = True
                else:
                    soa_amount = float(soa_entry["premium_soa"] if "premium" in category else soa_entry["claims_soa"])
                    variance = file_amount - soa_amount

                    if abs(variance) <= VARIANCE_THRESHOLD:
                        status = "RECONCILED"
                        reason = f"Variance inside tolerated boundary parameters (<= {VARIANCE_THRESHOLD})."
                    else:
                        status = "BREAK"
                        reason = f"Financial variance limit breach detected: {variance:.2f}"
                        had_failure = True

                recon_results_pool.append({
                    "run_id": run_id, "client_id": client_id, "category": category,
                    "year": year, "period": period, "soa_amount": float(soa_amount),
                    "file_amount": float(file_amount), "variance": float(variance),
                    "status": status, "checked_at": datetime.datetime.now()
                })

                # 5. Securely promote reconciled financial positions to Gold
                if status == "RECONCILED":
                    try:
                        clean_category = category.replace("_adj", "")
                        merge_keys = MERGE_KEYS.get(clean_category)
                        if not merge_keys:
                            raise KeyError(f"No production keys mapped for domain space: {clean_category}")

                        if DeltaTable.isDeltaTable(spark, gold_path):
                            target = DeltaTable.forPath(spark, gold_path)
                            merge_condition = " AND ".join([f"target.{k} = source.{k}" for k in merge_keys])
                            merge_condition += " AND target.year = source.year AND target.period = source.period"

                            target.alias("target").merge(
                                df_silver.alias("source"), merge_condition
                            ).whenMatchedUpdateAll().whenNotMatchedInsertAll().execute()
                            logger.info(f" -> Position Reconciled. Core updates written to production: {gold_path}")
                        else:
                            df_silver.write.format("delta").mode("append").partitionBy("year", "period").save(gold_path)
                            logger.info(f" -> Position Reconciled. Initialized production table context: {gold_path}")
                    except Exception as promo_err:
                        had_failure = True
                        logger.error(f" -> System failure during Gold promotion step: {str(promo_err)[:200]}")
                        recon_results_pool[-1]["status"] = "ERROR"
                else:
                    logger.warning(f" -> Promotion block active [{status}]: {reason}")

            except Exception as loop_err:
                had_failure = True
                logger.error(f" -> Group processing interrupted: {str(loop_err)[:200]}")
                recon_results_pool.append({
                    "run_id": run_id, "client_id": client_id, "category": category,
                    "year": year, "period": period, "soa_amount": 0.0, "file_amount": 0.0,
                    "variance": 0.0, "status": "ERROR", "checked_at": datetime.datetime.now()
                })

        # 6. Save audit telemetry back to tracking database
        if recon_results_pool:
            try:
                schema_fields = ["run_id", "client_id", "category", "year", "period", "soa_amount", "file_amount", "variance", "status", "checked_at"]
                rows_list = [Row(**record) for record in recon_results_pool]
                df_writeout = spark.createDataFrame(rows_list).select(*schema_fields)
                df_writeout.write.jdbc(url=JDBC_URL, table="reconciliation_results", mode="append", properties=JDBC_PROPS)
            except Exception as db_write_err:
                had_failure = True
                logger.critical(f"Audit log database synchronization failure: {str(db_write_err)[:200]}")

        # 7. Generate and route formal operations alerts
        overall_status = "FAILED/BREAKS DETECTED" if had_failure else "SUCCESS"
        subject = f"[RI Platform] Financial Recon Execution: {overall_status} (Run {run_id})"

        email_body = f"Reconciliation Sequence Summary ID: {run_id}\n"
        email_body += f"Processing Health Index: {overall_status}\n"
        email_body += "=" * 60 + "\n\n"

        for res in recon_results_pool:
            email_body += f"Client Account : {res['client_id']}\n"
            email_body += f"Domain Scope   : {res['category'].upper()}\n"
            email_body += f"Report Window  : {res['year']} - {res['period']}\n"
            email_body += f"Status Outcome : {res['status']}\n"
            email_body += "-" * 40 + "\n"
            email_body += f"Ledger Sum     : {res['file_amount']:,.2f}\n"
            email_body += f"Statement Sum  : {res['soa_amount']:,.2f}\n"
            email_body += f"Net Variance   : {res['variance']:,.2f}\n\n"

        if had_failure:
            email_body += "\nACTION REQUIRED: One or more data groups failed financial balancing or lack an approved Statement of Account. Anomalous layers have been isolated and blocked from promotion.\n"
        else:
            email_body += "\nAll data pools successfully verified against statement totals and migrated to the Gold reporting layer.\n"

        send_ops_email(subject, email_body)

    except Exception as master_exception:
        had_failure = True
        logger.critical(f"Pipeline processing halted by critical unhandled exception: {str(master_exception)}")
        
        crash_subject = f"[RI Platform] CRITICAL ENGINE FAULT (Run {run_id})"
        crash_body = f"The reconciliation pipeline crashed during execution.\n\nError Summary:\n{str(master_exception)}\n\nTraceback:\n{traceback.format_exc()}"
        send_ops_email(crash_subject, crash_body)

    finally:
        logger.info("De-allocating analytical data session structures. Core cleanup tasks complete.")
        spark.stop()
        if had_failure:
            sys.exit(1)
        sys.exit(0)


if __name__ == "__main__":
    main()