"""
L&H Reinsurance - Bronze PySpark Worker
=======================================
Executable PySpark script designed to be orchestrated by Airflow.
Reads validated, deduplicated CSV payloads from MinIO and writes them 
to structured Delta Lake tables in the Bronze zone.

Features:
- Enterprise Logging integration for Airflow capture.
- Environment Variable credential mapping for security.
- Cross-client collision detection.
"""

import os
import sys
import logging
from datetime import datetime, timezone

from pyspark.sql import SparkSession
from pyspark.sql.functions import current_timestamp, lit, count, col
from pyspark.sql.types import (
    StructType, StructField, StringType,
    IntegerType, BooleanType, TimestampType
)
from delta import configure_spark_with_delta_pip
from delta.tables import DeltaTable

# ============================================================
# CONFIGURATION & LOGGING
# ============================================================
logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s] %(message)s")
logger = logging.getLogger(__name__)

# Pull credentials securely from environment with local Docker fallbacks
PG_USER = os.getenv("PG_USER", "airflow")
PG_PASS = os.getenv("PG_PASS", "airflow")
MINIO_ACCESS = os.getenv("MINIO_ACCESS_KEY", "minioadmin")
MINIO_SECRET = os.getenv("MINIO_SECRET_KEY", "minioadmin")

JDBC_URL = "jdbc:postgresql://postgres:5432/airflow"
JDBC_PROPERTIES = {"user": PG_USER, "password": PG_PASS, "driver": "org.postgresql.Driver"}

SCHEMA_REGISTRY = {
    "premium":     {"policy_num", "premium", "val_date"},
    "premium_adj": {"policy_num", "premium", "val_date"},
    "claims":      {"claim_id", "claim_amount", "reported_date"},
    "claims_adj":  {"claim_id", "claim_amount", "reported_date"}
}

MAX_RETRIES = 3

def classify_file(file_name: str) -> str:
    """Categorizes incoming data based on nomenclature patterns."""
    name = file_name.lower()
    if "prem" in name and "adj" in name: return "premium_adj"
    if "prem" in name: return "premium"
    if "claim" in name and "adj" in name: return "claims_adj"
    if "claim" in name: return "claims"
    return "others"

def main():
    logger.info("Initializing PySpark Session for Bronze Ingestion...")
    builder = SparkSession.builder \
        .appName("Bronze Ingestion") \
        .config("spark.master", "local[*]") \
        .config("spark.jars", ",".join([
            "/opt/airflow/jars/postgresql-42.7.3.jar",
            "/opt/airflow/jars/hadoop-aws-3.3.4.jar",
            "/opt/airflow/jars/aws-java-sdk-bundle-1.12.262.jar"
        ])) \
        .config("spark.driver.extraClassPath",
            "/opt/airflow/jars/postgresql-42.7.3.jar:"
            "/opt/airflow/jars/hadoop-aws-3.3.4.jar:"
            "/opt/airflow/jars/aws-java-sdk-bundle-1.12.262.jar") \
        .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension") \
        .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog") \
        .config("spark.hadoop.fs.s3a.endpoint", "http://minio:9000") \
        .config("spark.hadoop.fs.s3a.access.key", MINIO_ACCESS) \
        .config("spark.hadoop.fs.s3a.secret.key", MINIO_SECRET) \
        .config("spark.hadoop.fs.s3a.path.style.access", "true") \
        .config("spark.hadoop.fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem")

    spark = configure_spark_with_delta_pip(builder).getOrCreate()
    spark.conf.set("spark.databricks.delta.schema.autoMerge.enabled", "true")

    logger.info("Fetching File Tracking metadata from Postgres...")
    file_tracking_df = spark.read.jdbc(url=JDBC_URL, table="file_tracking", properties=JDBC_PROPERTIES)

    try:
        log_df = spark.read.jdbc(url=JDBC_URL, table="bronze_ingestion_log", properties=JDBC_PROPERTIES)
        written_pairs = log_df.filter(col("status").isin("SUCCESS", "DUPLICATE", "CROSS_CLIENT_DUP", "UNKNOWN_CATEGORY")) \
                              .select("client_id", "file_hash").distinct()
        
        exhausted_pairs = (log_df.filter("status = 'FAILED'")
                           .groupBy("client_id", "file_hash")
                           .agg(count("*").alias("fail_count"))
                           .filter(col("fail_count") >= MAX_RETRIES)
                           .select("client_id", "file_hash"))
        
        skip_pairs = written_pairs.union(exhausted_pairs).distinct()
    except Exception:
        logger.warning("Bronze ingestion log table empty or missing. Proceeding without skips.")
        skip_pairs = spark.createDataFrame([], "client_id string, file_hash string")

    new_files_df = file_tracking_df.join(skip_pairs, on=["client_id", "file_hash"], how="left_anti")
    files = new_files_df.collect()

    if not files:
        logger.info("No new files to process. Terminating successfully.")
        spark.stop()
        sys.exit(0)

    # Cross-client collision detection mapping
    file_tracking_pd = file_tracking_df.select("client_id", "file_name", "file_hash").toPandas()
    dup_map = {}
    for file_hash, group in file_tracking_pd.groupby("file_hash"):
        file_list = group["file_name"].tolist()
        dup_map[file_hash] = {"count": len(file_list), "files": file_list}

    hash_to_clients = file_tracking_pd.groupby("file_hash")["client_id"].apply(lambda x: list(x.unique())).to_dict()
    cross_client_hashes = {h: clients for h, clients in hash_to_clients.items() if len(clients) > 1}
    
    if cross_client_hashes:
        logger.warning(f"Cross-client duplicates detected for {len(cross_client_hashes)} hash(es).")

    run_id = datetime.now(timezone.utc).strftime("%Y%m%d_%H%M%SZ")
    log_rows, schema_drift_rows = [], []
    logger.info(f"Initiating Run ID: {run_id} | Files to process: {len(files)}")

    for file in files:
        file_name, file_hash, year, quarter, client_id = file["file_name"], file["file_hash"], file["year"], file["quarter"], file["client_id"]
        category = classify_file(file_name)
        path = f"s3a://landing/{client_id}/{year}/{quarter}/{file_hash}/{file_name}"

        logger.info(f"Processing Payload: {client_id}/{file_name} [{category}]")

        try:
            df = spark.read.option("header", True).csv(path)
            df = df.toDF(*[c.lower().strip() for c in df.columns])
            incoming_cols = set(df.columns)

            if category != "others":
                expected_cols = SCHEMA_REGISTRY.get(category, set())
                for col_name in (incoming_cols - expected_cols):
                    schema_drift_rows.append({"run_id": run_id, "client_id": client_id, "file_name": file_name, "file_hash": file_hash, "category": category, "issue_type": "NEW_COLUMN", "column_name": col_name, "detected_at": datetime.now(timezone.utc)})
                for col_name in (expected_cols - incoming_cols):
                    schema_drift_rows.append({"run_id": run_id, "client_id": client_id, "file_name": file_name, "file_hash": file_hash, "category": category, "issue_type": "MISSING_COLUMN", "column_name": col_name, "detected_at": datetime.now(timezone.utc)})

            rows_read = df.count()

            # Determine pipeline state routing
            if file_hash in cross_client_hashes:
                status, is_duplicate, duplicate_count = "CROSS_CLIENT_DUP", True, len(cross_client_hashes[file_hash])
                duplicate_files = ",".join(cross_client_hashes[file_hash])
                error_message = f"Same file_hash found under clients: {cross_client_hashes[file_hash]}"
            elif dup_map.get(file_hash, {}).get("count", 1) > 1:
                dup_info = dup_map[file_hash]
                status, is_duplicate, duplicate_count = "DUPLICATE", True, dup_info["count"]
                duplicate_files = ",".join(dup_info["files"])
                error_message = f"File uploaded multiple times. Found in: {duplicate_files}"
            elif category == "others":
                status, is_duplicate, duplicate_count, duplicate_files = "UNKNOWN_CATEGORY", False, 1, file_name
                error_message = "Category could not be determined. Manual classification required."
            else:
                status, is_duplicate, duplicate_count, duplicate_files, error_message = "SUCCESS", False, 1, file_name, None

            # Enrich DataFrame with audit metadata
            df = df.withColumn("client_id", lit(client_id)).withColumn("file_name", lit(file_name)).withColumn("file_hash", lit(file_hash)) \
                   .withColumn("category", lit(category)).withColumn("year", lit(year)).withColumn("quarter", lit(quarter)) \
                   .withColumn("run_id", lit(run_id)).withColumn("source_path", lit(path)).withColumn("ingestion_time", current_timestamp())

            target_path = f"s3a://bronze/{client_id}/{category}/"
            
            # Execute Delta MERGE Upsert
            df.write.format("delta").mode("append").option("mergeSchema", "true").save(target_path)

            rows_written = int(DeltaTable.forPath(spark, target_path).history(1).select("operationMetrics").collect()[0][0].get("numOutputRows", 0))

            log_rows.append({"run_id": run_id, "client_id": client_id, "file_name": file_name, "file_hash": file_hash, "year": year, "quarter": quarter, "category": category, "status": status, "rows_read": rows_read, "rows_written": rows_written, "target_path": target_path, "error_message": error_message, "is_duplicate": is_duplicate, "duplicate_count": duplicate_count, "duplicate_files": duplicate_files})
            logger.info(f"SUCCESS: {client_id}/{file_name} -> {rows_written} rows written [{status}]")

        except Exception as e:
            log_rows.append({"run_id": run_id, "client_id": client_id, "file_name": file_name, "file_hash": file_hash, "year": year, "quarter": quarter, "category": category, "status": "FAILED", "rows_read": 0, "rows_written": 0, "target_path": None, "error_message": str(e), "is_duplicate": False, "duplicate_count": 1, "duplicate_files": file_name})
            logger.error(f"FAILED: {client_id}/{file_name} -> {str(e)[:120]}")

    logger.info("Committing Run Logs to Postgres...")
    
    # Schemas built dynamically inline for Postgres push
    log_schema = StructType([
        StructField("run_id", StringType(), True), StructField("client_id", StringType(), True), StructField("file_name", StringType(), True), StructField("file_hash", StringType(), True), StructField("year", StringType(), True), StructField("quarter", StringType(), True), StructField("category", StringType(), True), StructField("status", StringType(), True), StructField("rows_read", IntegerType(), True), StructField("rows_written", IntegerType(), True), StructField("target_path", StringType(), True), StructField("error_message", StringType(), True), StructField("is_duplicate", BooleanType(), True), StructField("duplicate_count", IntegerType(), True), StructField("duplicate_files", StringType(), True)
    ])
    if log_rows: spark.createDataFrame(log_rows, schema=log_schema).write.jdbc(url=JDBC_URL, table="bronze_ingestion_log", mode="append", properties=JDBC_PROPERTIES)

    drift_schema = StructType([
        StructField("run_id", StringType(), True), StructField("client_id", StringType(), True), StructField("file_name", StringType(), True), StructField("file_hash", StringType(), True), StructField("category", StringType(), True), StructField("issue_type", StringType(), True), StructField("column_name", StringType(), True), StructField("detected_at", TimestampType(), True)
    ])
    if schema_drift_rows: spark.createDataFrame(schema_drift_rows, schema=drift_schema).write.jdbc(url=JDBC_URL, table="schema_drift_log", mode="append", properties=JDBC_PROPERTIES)

    success = sum(1 for r in log_rows if r["status"] == "SUCCESS")
    failed = sum(1 for r in log_rows if r["status"] == "FAILED")
    
    logger.info(f"Bronze Ingestion Complete | Total: {len(log_rows)} | Success: {success} | Failed: {failed}")
    spark.stop()
    if failed > 0: sys.exit(1)

if __name__ == "__main__":
    main()