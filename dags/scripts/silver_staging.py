"""
Module: Silver Staging Pipeline
Architecture Layer: Medallion Architecture (Bronze -> Silver)
Domain: Reinsurance Life & Health

Description:
    Processes raw delta tables from the Bronze layer, applies structural column 
    normalization, enforces canonical business schema mappings, anonymizes 
    PII fields via cryptographic hashing, and validates records against 
    predefined Data Quality (DQ) rules. Non-compliant data is routed to a 
    quarantine isolation layer for audit trailing.
"""

import os
import sys
import logging
from datetime import datetime, timezone
import psycopg2
from minio import Minio
from pyspark.sql import SparkSession
from pyspark.sql.functions import (
    current_timestamp,
    lit,
    col,
    to_date,
    when,
    concat_ws,
    sha2,
    coalesce    
)
from pyspark.sql.types import (
    StructType,
    StructField,
    StringType,
    IntegerType
)
from delta import configure_spark_with_delta_pip

# ==============================================================================
# LOGGING CONFIGURATION
# ==============================================================================
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
    handlers=[logging.StreamHandler(sys.stdout)]
)
logger = logging.getLogger("silver_staging")

# ==============================================================================
# ENVIRONMENT & ENVIRONMENT VARIABLES
# ==============================================================================
PYTHON_BIN = os.getenv("PYTHON_BIN", "/usr/local/bin/python")
os.environ["PYSPARK_PYTHON"] = PYTHON_BIN
os.environ["PYSPARK_DRIVER_PYTHON"] = PYTHON_BIN

# Decoupled Credentials (Using defaults for local development environments)
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

# ==============================================================================
# MINIO CLIENT & BUCKET INITIALIZATION
# ==============================================================================
try:
    minio_client = Minio(
        MINIO_ENDPOINT,
        access_key=MINIO_ACCESS_KEY,
        secret_key=MINIO_SECRET_KEY,
        secure=False
    )
except Exception as e:
    logger.error(f"Failed to initialize MinIO Client: {str(e)}")
    sys.exit(1)


def ensure_bucket(bucket_name):
    """Verifies existence of an object bucket; creates it if absent."""
    try:
        if not minio_client.bucket_exists(bucket_name):
            logger.info(f"Creating infrastructure bucket: {bucket_name}")
            minio_client.make_bucket(bucket_name)
        else:
            logger.debug(f"Infrastructure bucket already exists: {bucket_name}")
    except Exception as e:
        logger.error(f"Error checking/creating bucket {bucket_name}: {str(e)}")
        raise


for bucket in ["bronze", "silver", "quarantine"]:
    ensure_bucket(bucket)

# ==============================================================================
# SPARK ENGINE SETUP
# ==============================================================================
builder = (
    SparkSession.builder
    .appName("Silver-Staging-Pipeline")
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

logger.info("======================================")
logger.info("Spark Distributed Session Initialized")
logger.info(f"Python Environment Executable : {PYTHON_BIN}")
logger.info(f"Runtime Environment Version   : {sys.version.split()[0]}")
logger.info("======================================")


def get_db():
    """Generates a stateful transactional connection to the relational operational store."""
    return psycopg2.connect(
        host=DB_HOST,
        database=DB_NAME,
        user=DB_USER,
        password=DB_PASS
    )

# ==============================================================================
# PIPELINE HELPER FUNCTIONS
# ==============================================================================

def normalize_col(name):
    """Transforms raw column schemas into lower snake_case standards."""
    return (
        name.lower()
            .strip()
            .replace("/", "_")
            .replace(" ", "_")
            .replace("%", "_pct")
            .replace("-", "_")
    )


def add_issue(df, condition, issue_msg):
    """Appends array strings tracking data quality rule evaluation faults."""
    return df.withColumn(
        "dq_issues",
        when(
            condition,
            when(col("dq_issues") == "", lit(issue_msg))
            .otherwise(concat_ws("|", col("dq_issues"), lit(issue_msg)))
        ).otherwise(col("dq_issues"))
    )


def apply_dq(df, category):
    """Enforces specific Reinsurance domain logic and type validations."""
    df = df.withColumn("dq_issues", lit(""))

    if category in ("premium", "premium_adj"):
        # Structural Mandatory Non-Null Fields
        for c in ["policy_number", "date_effect_policy", "dob"]:
            if c in df.columns:
                df = add_issue(df, col(c).isNull(), f"{c}:NULL")

        # Explicit Temporal Schema Hardening
        for c in ["issue_date", "date_effect_policy", "dob", "val_date"]:
            if c in df.columns:
                df = df.withColumn(c, to_date(col(c)))

        # Explicit Numerical Metrics Casts
        if "age" in df.columns:
            df = df.withColumn("age", col("age").cast("integer"))
        if "sar" in df.columns:
            df = df.withColumn("sar", col("sar").cast("double"))
        if "ri_sar" in df.columns:
            df = df.withColumn("ri_sar", col("ri_sar").cast("double"))
        if "premium_amt" in df.columns:
            df = df.withColumn("premium_amt", col("premium_amt").cast("double"))

        # Domain Logic Range Rules
        if "age" in df.columns:
            df = add_issue(
                df,
                col("age").isNotNull() & ((col("age") < 0) | (col("age") > 100)),
                "age:OUT_OF_RANGE_0_100"
            )

        # Financial Coherence Rules (Sum at Risk must exceed raw underlying premium)
        if "sar" in df.columns and "premium_amt" in df.columns:
            df = add_issue(
                df,
                col("sar").isNotNull() & col("premium_amt").isNotNull() & (col("sar") <= col("premium_amt")),
                "sar:NOT_GREATER_THAN_PREMIUM_AMT"
            )

        if "ri_sar" in df.columns and "premium_amt" in df.columns:
            df = add_issue(
                df,
                col("ri_sar").isNotNull() & col("premium_amt").isNotNull() & (col("ri_sar") <= col("premium_amt")),
                "ri_sar:NOT_GREATER_THAN_PREMIUM_AMT"
            )

        # Temporal Validity Boundaries
        if "issue_date" in df.columns and "val_date" in df.columns:
            df = add_issue(
                df,
                col("issue_date").isNotNull() & col("val_date").isNotNull() & (col("issue_date") > col("val_date")),
                "issue_date:AFTER_VAL_DATE"
            )

        if "date_effect_policy" in df.columns and "val_date" in df.columns:
            df = add_issue(
                df,
                col("date_effect_policy").isNotNull() & col("val_date").isNotNull() & (col("date_effect_policy") > col("val_date")),
                "date_effect_policy:AFTER_VAL_DATE"
            )

        if "dob" in df.columns and "val_date" in df.columns:
            df = add_issue(
                df,
                col("dob").isNotNull() & col("val_date").isNotNull() & (col("dob") > col("val_date")),
                "dob:AFTER_VAL_DATE"
            )

        if "new_renew" in df.columns:
            df = add_issue(
                df,
                col("new_renew").isNotNull() & ~col("new_renew").isin("New", "Renew"),
                "new_renew:INVALID_VALUE"
            )

    elif category in ("claims", "claims_adj"):
        # Claims Sub-domain Null Validations
        for c in ["policy_num", "loss_date"]:
            if c in df.columns:
                df = add_issue(df, col(c).isNull(), f"{c}:NULL")

        for c in ["loss_date", "reported_date"]:
            if c in df.columns:
                df = df.withColumn(c, to_date(col(c)))

        if "claim_amount" in df.columns:
            df = df.withColumn("claim_amount", col("claim_amount").cast("double"))

        if "loss_date" in df.columns and "reported_date" in df.columns:
            df = add_issue(
                df,
                col("loss_date").isNotNull() & col("reported_date").isNotNull() & (col("loss_date") > col("reported_date")),
                "loss_date:AFTER_REPORTED_DATE"
            )

    return df

# ==============================================================================
# PIPELINE EXECUTION
# ==============================================================================
run_queue_df = spark.read.jdbc(url=JDBC_URL, table="silver_run_queue", properties=JDBC_PROPS)
files = run_queue_df.collect()

if not files:
    logger.info("Silver state execution queue is empty. Terminating operational workflow run.")
    spark.stop()
    sys.exit(0)

log_schema = StructType([
    StructField("run_id", StringType(), True),
    StructField("client_id", StringType(), True),
    StructField("file_hash", StringType(), True),
    StructField("file_name", StringType(), True),
    StructField("category", StringType(), True),
    StructField("year", StringType(), True),
    StructField("quarter", StringType(), True),
    StructField("status", StringType(), True),
    StructField("rows_read", IntegerType(), True),
    StructField("rows_to_silver", IntegerType(), True),
    StructField("rows_quarantined", IntegerType(), True),
    StructField("target_path", StringType(), True),
    StructField("quarantine_path", StringType(), True),
    StructField("error_message", StringType(), True),
    StructField("period", StringType(), True),
    StructField("period_type", StringType(), True),
])

run_id = datetime.now(timezone.utc).strftime("%Y%m%d_%H%M%SZ")
logger.info(f"Initialized Pipeline Job Execution Context — Run ID: {run_id} | Total Queue Records: {len(files)}")

log_rows = []
quarantine_summary_rows = []
successful_hashes = []

for file in files:
    client_id = file["client_id"]
    category = file["category"]
    file_hash = file["file_hash"]
    file_name = file["file_name"]
    year = file["year"]
    quarter = file["quarter"]
    period = file["period"]
    period_type = file["period_type"]
    df = None

    logger.info(f"Starting Processing Loop for File: {client_id}/{file_name} [{category}]")

    try:
        # Load Raw Ingested Bronze Records
        bronze_path = f"s3a://bronze/{client_id}/{category}/"
        df = spark.read.format("delta").load(bronze_path).filter(col("file_hash") == file_hash)

        # Standardize Base Column Schemas
        df = df.toDF(*[normalize_col(c) for c in df.columns])

        # Fetch Active Declarative Metadata Mappings
        mapping_df = (
            spark.read.jdbc(url=JDBC_URL, table="column_mapping", properties=JDBC_PROPS)
            .filter((col("client_id") == client_id) & (col("category") == category) & col("effective_to").isNull())
        )
        mapping_rows = mapping_df.collect()
        mapping = {normalize_col(row["raw_col_name"]): row["canonical_name"] for row in mapping_rows}

        # Apply Canonical Mapping Schemas and Mitigate Column Space Collisions
        for raw, canonical in mapping.items():
            if raw in df.columns and raw != canonical:
                if canonical in df.columns:
                    df = df.withColumn(canonical, coalesce(col(canonical), col(raw))).drop(raw)
                else:
                    df = df.withColumnRenamed(raw, canonical)

        # Handle Compliance Masking for PII Elements via Cryptographic Salting
        pii_cols = [row["canonical_name"] for row in mapping_rows if row["is_pii"]]
        for pii_col in pii_cols:
            if pii_col in df.columns:
                df = df.withColumn(pii_col, sha2(col(pii_col).cast("string"), 256))

        # Evaluate Configured Data Quality Framework Policies
        df = apply_dq(df, category).cache()

        # Isolate Conforming Records from Out-Of-Bounds Fault Vectors
        clean_df = df.filter(col("dq_issues") == "").drop("dq_issues")
        quarantine_df = df.filter(col("dq_issues") != "")

        rows_to_silver = clean_df.count()
        rows_quarantined = quarantine_df.count()
        rows_read = rows_to_silver + rows_quarantined

        clean_df = (
            clean_df
            .withColumn("client_id", lit(client_id))
            .withColumn("category", lit(category))
            .withColumn("year", lit(year))
            .withColumn("quarter", lit(quarter))
            .withColumn("period", lit(period))
            .withColumn("period_type", lit(period_type))
        )

        # Append Validated Datasets into Optimized Silver Storage
        silver_path = f"s3a://silver/staging/{client_id}/{category}/"
        (
            clean_df
            .withColumn("silver_ingestion_time", current_timestamp())
            .write
            .format("delta")
            .mode("append")
            .option("mergeSchema", "true")
            .partitionBy("year", "quarter")
            .save(silver_path)
        )

        # Append Anomalous Datasets to Quarantine Storage For Deep Investigations
        quarantine_path = None
        if rows_quarantined > 0:
            quarantine_path = f"s3a://quarantine/{client_id}/{category}/"
            (
                quarantine_df
                .withColumn("quarantine_ingestion_time", current_timestamp())
                .write
                .format("delta")
                .mode("append")
                .option("mergeSchema", "true")
                .save(quarantine_path)
            )

            # Generate Aggregated Quality Violation Diagnostic Reports
            summary = quarantine_df.groupBy("dq_issues").count().collect()
            for row in summary:
                for rule in row["dq_issues"].split("|"):
                    if rule:
                        quarantine_summary_rows.append({
                            "run_id": run_id,
                            "client_id": client_id,
                            "file_name": file_name,
                            "category": category,
                            "dq_rule": rule,
                            "row_count": row["count"],
                        })

        log_rows.append({
            "run_id": run_id,
            "client_id": client_id,
            "file_hash": file_hash,
            "file_name": file_name,
            "category": category,
            "year": year,
            "quarter": quarter,
            "status": "SUCCESS",
            "rows_read": rows_read,
            "rows_to_silver": rows_to_silver,
            "rows_quarantined": rows_quarantined,
            "target_path": silver_path,
            "quarantine_path": quarantine_path,
            "error_message": None,
            "period": period,
            "period_type": period_type,
        })
        successful_hashes.append(file_hash)
        logger.info(f"✓ Operational success for {file_name} | Routed {rows_to_silver} rows to Silver, {rows_quarantined} to Quarantine.")

    except Exception as e:
        log_rows.append({
            "run_id": run_id,
            "client_id": client_id,
            "file_hash": file_hash,
            "file_name": file_name,
            "category": category,
            "year": year,
            "quarter": quarter,
            "status": "FAILED",
            "rows_read": 0,
            "rows_to_silver": 0,
            "rows_quarantined": 0,
            "target_path": None,
            "quarantine_path": None,
            "error_message": str(e),
            "period": period,
            "period_type": period_type,
        })
        logger.error(f"✗ Functional exception processing file metadata execution space {file_name}: {str(e)}")

    finally:
        if df is not None:
            df.unpersist()

# ==============================================================================
# WRITING AUDIT METRICS LOGS BACK TO OPERATIONAL RDBMS
# ==============================================================================
if log_rows:
    spark.createDataFrame(log_rows, schema=log_schema).write.jdbc(
        url=JDBC_URL, table="silver_staging_log", mode="append", properties=JDBC_PROPS
    )

if quarantine_summary_rows:
    quarantine_schema = StructType([
        StructField("run_id", StringType(), True),
        StructField("client_id", StringType(), True),
        StructField("file_name", StringType(), True),
        StructField("category", StringType(), True),
        StructField("dq_rule", StringType(), True),
        StructField("row_count", IntegerType(), True),
    ])
    spark.createDataFrame(quarantine_summary_rows, schema=quarantine_schema).write.jdbc(
        url=JDBC_URL, table="silver_quarantine_summary", mode="append", properties=JDBC_PROPS
    )

# Transaction-Safe Elimination of Processed Items from the Queue State
if successful_hashes:
    conn = None
    try:
        conn = get_db()
        cur = conn.cursor()
        placeholders = ",".join(["%s"] * len(successful_hashes))
        delete_query = f"DELETE FROM silver_run_queue WHERE file_hash IN ({placeholders})"
        cur.execute(delete_query, successful_hashes)
        conn.commit()
        cur.close()
    except Exception as e:
        logger.error(f"Failed to clear processed file hashes from run queue: {str(e)}")
    finally:
        if conn is not None:
            conn.close()

# Summary Calculations for Final Reports
total = len(log_rows)
success = sum(1 for r in log_rows if r["status"] == "SUCCESS")
failed = sum(1 for r in log_rows if r["status"] == "FAILED")
total_quarantine = sum(r["rows_quarantined"] for r in log_rows)

logger.info("=======================================================================")
logger.info(f"Silver Layer Job Run Sequence Complete : {run_id}")
logger.info(f"Total Discovered Stage Artifacts Checked: {total}")
logger.info(f"Operational Run Success Files          : {success}")
logger.info(f"Pipeline Operational Failure Elements  : {failed}")
logger.info(f"Quarantined Records Trapped In Layer   : {total_quarantine}")
logger.info("=======================================================================")

spark.stop()

if failed > 0:
    sys.exit(1)

print(f"SILVER_RUN_ID={run_id}")