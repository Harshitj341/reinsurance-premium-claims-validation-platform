"""
Module: Silver Validation & Promotion Engine
Architecture Layer: Medallion Architecture (Silver Staging -> Silver Validated)
Domain: Reinsurance Life & Health Risk Underwriting

Description:
    Processes stage records by enforcing contract treaty mappings, calculating
    historical duplicate hashes across premium/claims layers, and calculating
    complex actuarial business parameters (Sum At Risk rules, CAL limits). 
    
    Includes a 30% tolerance threshold rule: files exceeding 30% data quality 
    failures are completely rejected, isolating anomalous source systems and 
    alerting operations via operational mail notifications.
"""

import os
import sys
import logging
import datetime
import traceback
import smtplib
from email.mime.text import MIMEText
from email.mime.multipart import MIMEMultipart
from email.mime.base import MIMEBase
from email import encoders

from pyspark.sql import SparkSession, Row
from pyspark.sql import functions as F
from pyspark.sql.window import Window
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
logger = logging.getLogger("silver_validation")

# ==============================================================================
# CONFIGURATION & RECOVERY PARAMETERS
# ==============================================================================
PYTHON_BIN = os.getenv("PYTHON_BIN", "/usr/local/bin/python")
os.environ["PYSPARK_PYTHON"] = PYTHON_BIN
os.environ["PYSPARK_DRIVER_PYTHON"] = PYTHON_BIN

# Infrastructure Credentials
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

# ==============================================================================
# SYSTEM RECOVERY HELPER UTILITIES
# ==============================================================================

def send_ops_email(subject, body, attachment_path=None):
    """Dispatches precise metrics payloads detailing pipeline quarantine breaches."""
    smtp_user = os.environ.get("AIRFLOW__SMTP__SMTP_USER")
    smtp_pass = os.environ.get("AIRFLOW__SMTP__SMTP_PASSWORD")

    if not smtp_user or not smtp_pass:
        logger.warning("SMTP environment variables are absent. Skipping validation alert routing.")
        return

    msg = MIMEMultipart()
    msg["Subject"] = subject
    msg["From"] = smtp_user
    msg["To"] = smtp_user
    msg.attach(MIMEText(body, "plain"))

    if attachment_path and os.path.exists(attachment_path):
        try:
            with open(attachment_path, "rb") as f:
                part = MIMEBase("application", "octet-stream")
                part.set_payload(f.read())
            encoders.encode_base64(part)
            part.add_header("Content-Disposition", f'attachment; filename="{os.path.basename(attachment_path)}"')
            msg.attach(part)
        except Exception as file_err:
            logger.error(f"Failed to attach diagnostic quarantine file: {str(file_err)}")

    try:
        with smtplib.SMTP(SMTP_HOST, SMTP_PORT) as server:
            server.starttls()
            server.login(smtp_user, smtp_pass)
            server.send_message(msg)
        logger.info(f"Operations escalation notification delivered successfully: {subject}")
    except Exception as smtp_err:
        logger.error(f"Failed to transmit operational escalation alert via SMTP host: {str(smtp_err)}")


def flag_critical(df, condition, rule_name):
    """Enforces fine-grained conditional pipeline quarantine row tagging."""
    return df.withColumn(
        "dq_reasons",
        F.when(condition,
            F.when(F.col("dq_reasons") == "", F.lit(rule_name))
             .otherwise(F.concat_ws("|", F.col("dq_reasons"), F.lit(rule_name)))
        ).otherwise(F.col("dq_reasons"))
    )

# ==============================================================================
# PIPELINE APPLICATION RUN LOGIC
# ==============================================================================

def main():
    run_id = os.environ.get("SILVER_RUN_ID")
    
    # Session Parameter Builders
    builder = (
        SparkSession.builder
        .appName(f"Silver-Validation-Engine_{run_id if run_id else 'AUTO_DETECTED'}")
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
    )

    spark = configure_spark_with_delta_pip(builder).getOrCreate()
    spark.sparkContext.setLogLevel("WARN")
    spark.conf.set("spark.databricks.delta.schema.autoMerge.enabled", "true")

    # Fallback discovery logic when explicit run ids are omitted from execution contexts
    if not run_id:
        try:
            fallback_query = "(SELECT run_id FROM silver_staging_log WHERE status = 'SUCCESS' ORDER BY run_id DESC LIMIT 1) x"
            result = spark.read.jdbc(url=JDBC_URL, table=fallback_query, properties=JDBC_PROPS).collect()
            if not result:
                raise RuntimeError("No active successful history entries found inside database log tracks.")
            run_id = result[0]["run_id"]
            logger.info(f"Auto-discovered last active pipeline sequence id to parse: {run_id}")
        except Exception as err:
            logger.error(f"Execution context lookup failed completely: {str(err)}")
            spark.stop()
            sys.exit(1)

    validation_results_pool = []

    # Composite Keys tracking upstream relational structures uniquely
    MERGE_KEYS = {
        "premium":     ["policy_number", "benefit_id", "valuation_date", "policy_effective_date"],
        "premium_adj": ["policy_number", "benefit_id", "valuation_date", "policy_effective_date"],
        "claims":      ["claim_id", "policy_number", "benefit_id", "policy_effective_date"],
        "claims_adj":  ["claim_id", "policy_number", "benefit_id", "policy_effective_date"],
    }

    try:
        staging_log_query = f"(SELECT file_hash, client_id, file_name, category, year, period, period_type FROM silver_staging_log WHERE run_id = '{run_id}' AND status = 'SUCCESS') as staging_block"
        active_files = spark.read.jdbc(url=JDBC_URL, table=staging_log_query, properties=JDBC_PROPS).collect()

        if not active_files:
            logger.info("No successful active entries discovered within current execution window. Terminating stage.")
            spark.stop()
            sys.exit(0)

        def load_and_broadcast_table(table_name):
            return F.broadcast(spark.read.jdbc(url=JDBC_URL, table=table_name, properties=JDBC_PROPS))

        # Core Referential Target Dimensions Broadcast Optimization
        df_treaty_config = load_and_broadcast_table("treaty_benefit_config")
        df_rate_tables = load_and_broadcast_table("rate_tables")

        groups = {}
        for row in active_files:
            groups.setdefault((row["client_id"], row["category"]), []).append(row)

        # Prioritize Premium processing before running dependent Claim assertions
        sorted_groups = sorted(groups.items(), key=lambda x: 0 if x[0][1].upper() == 'PREMIUM' else 1)

        for (client_id, category), metadata_rows in sorted_groups:
            hashes_in_group = [r["file_hash"] for r in metadata_rows]
            silver_path = f"s3a://silver/staging/{client_id}/{category}/"
            
            try:
                df_silver_raw = spark.read.format("delta").load(silver_path)
            except Exception as load_err:
                logger.error(f"Failed to mount storage context delta file systems: {str(load_err)}")
                for meta in metadata_rows:
                    validation_results_pool.append(create_error_record(run_id, meta, "LOAD_DELTA", f"Path load error: {str(load_err)[:200]}"))
                continue

            df_current_run = df_silver_raw.filter(F.col("file_hash").isin(hashes_in_group))
            df_treaty_client = df_treaty_config.filter(F.col("client_id") == client_id)

            df_joined = (
                df_current_run.alias("src").join(
                    df_treaty_client.alias("cfg"),
                    on=["client_id", "benefit_id", "plan_code"],
                    how="left"
                )
                .select(
                    "src.*",
                    F.col("cfg.treaty_id").alias("treaty_id"),
                    F.col("cfg.product_id").alias("product_id"),
                    F.col("cfg.quota_share").alias("quota_share"),
                    F.col("cfg.surplus_multiple").alias("surplus_multiple"),
                    F.col("cfg.retention_limit").alias("retention_limit"),
                    F.col("cfg.cal_limit").alias("cal_limit"),
                    F.col("cfg.cal_level").alias("cal_level"),
                    F.col("cfg.effective_date").alias("effective_date"),
                    F.col("cfg.age_min").alias("age_min"),
                    F.col("cfg.age_max").alias("age_max"),
                    F.col("cfg.is_inforce_block").alias("is_inforce_block")
                )
            ).persist()

            for meta in metadata_rows:
                f_hash = meta["file_hash"]
                df_file = df_joined.filter(F.col("file_hash") == f_hash)
                
                total_records = df_file.count()
                if total_records == 0:
                    continue

                df_valid_records = df_file.withColumn("dq_reasons", F.lit(""))
                result_meta = meta.asDict() if hasattr(meta, "asDict") else dict(meta)
                result_meta["treaty_id"] = "MULTIPLE"

                # =========================================================================
                # GLOBAL STRUCTURAL SANITY CHECK RULES
                # =========================================================================
                df_valid_records = flag_critical(df_valid_records, F.col("treaty_id").isNull(), "TREATY_MAPPING")
                unmapped_count = df_valid_records.filter(F.col("dq_reasons").contains("TREATY_MAPPING")).count()
                status = "FAIL" if unmapped_count > 0 else "PASS"
                validation_results_pool.append(create_record(run_id, result_meta, "TREATY_MAPPING", status, unmapped_count, total_records, f"Unmapped contracts discovered: {unmapped_count} rows."))

                # =========================================================================
                # PREMIUM VALIDATION RULES
                # =========================================================================
                if category.upper() == "PREMIUM":
                    
                    # Deduplication Assertion Checking Layers
                    try:
                        w_dup = Window.partitionBy("policy_number", "benefit_id", "valuation_date").orderBy(F.lit(1))
                        df_valid_records = df_valid_records.withColumn("_row_num", F.row_number().over(w_dup))
                        df_valid_records = flag_critical(df_valid_records, F.col("_row_num") > 1, "DUPLICATE_PREMIUM")
                        
                        try:
                            hash_query_premium = f"(SELECT policy_number, benefit_id, valuation_date FROM persistent_hash_store WHERE client_id = '{client_id}' AND category = 'PREMIUM') AS known_hashes"
                            df_hist_hashes = spark.read.jdbc(url=JDBC_URL, table=hash_query_premium, properties=JDBC_PROPS).withColumn("_is_hist_dup", F.lit(True))
                            df_valid_records = df_valid_records.join(df_hist_hashes, on=["policy_number", "benefit_id", "valuation_date"], how="left")
                            df_valid_records = flag_critical(df_valid_records, F.col("_is_hist_dup").isNotNull(), "DUPLICATE_PREMIUM")
                        except Exception as inner_db_err:
                            logger.warning(f"Unable to cross-reference hash infrastructure indexes: {str(inner_db_err)}")

                        dup_count = df_valid_records.filter(F.col("dq_reasons").contains("DUPLICATE_PREMIUM")).count()
                        status = "FAIL" if dup_count > 0 else "PASS"
                        validation_results_pool.append(create_record(run_id, result_meta, "DUPLICATE_PREMIUM", status, dup_count, total_records, f"Duplicate premium bounds tracked: {dup_count}"))
                    except Exception as ex:
                        validation_results_pool.append(create_error_record(run_id, result_meta, "DUPLICATE_PREMIUM", ex))

                    # Operational Flag Analysis (Warnings Only)
                    try:
                        nb_count = df_valid_records.filter(F.datediff(F.col("valuation_date"), F.col("policy_effective_date")) < 365).count()
                        validation_results_pool.append(create_record(run_id, result_meta, "NEW_BUSINESS_FLAG", "WARN", nb_count, total_records, f"Identified {nb_count} current New Business records."))
                    except Exception:
                        pass

                    # Reinsured Sum At Risk Math Formula Approximations
                    try:
                        df_risar = df_valid_records.withColumn(
                            "computed_risar",
                            F.when(
                                F.col("quota_share").isNotNull() & F.col("retention_limit").isNotNull(),
                                F.when(F.col("sum_at_risk") > F.col("retention_limit"), (F.col("sum_at_risk") - F.col("retention_limit")) * F.col("quota_share"))
                                 .otherwise(F.col("sum_at_risk") * F.col("quota_share"))
                            )
                            .when(F.col("quota_share").isNotNull(), F.col("sum_at_risk") * F.col("quota_share"))
                            .when(F.col("surplus_multiple").isNotNull(), F.least(F.col("sum_at_risk") - F.col("retention_limit"), F.col("surplus_multiple") * F.col("retention_limit")))
                            .otherwise(F.col("reinsured_sum_at_risk"))
                        )
                        failed_risar_cnt = df_risar.filter(F.abs(F.col("computed_risar") - F.col("reinsured_sum_at_risk")) > 0.01).count()
                        status = "WARN" if failed_risar_cnt > 0 else "PASS"
                        validation_results_pool.append(create_record(run_id, result_meta, "RISAR_VALIDATION", status, failed_risar_cnt, total_records, f"Failed financial sum-at-risk tolerances: {failed_risar_cnt}"))
                    except Exception:
                        pass

                    try:
                        df_to_check = df_valid_records.filter((F.col("is_inforce_block") == False) | (F.col("is_inforce_block").isNull()))
                        failed_eff_cnt = df_to_check.filter(F.col("policy_effective_date") < F.col("effective_date")).count()
                        status = "WARN" if failed_eff_cnt > 0 else "PASS"
                        validation_results_pool.append(create_record(run_id, result_meta, "EFFECTIVE_DATE", status, failed_eff_cnt, total_records, f"Backdated effective violations: {failed_eff_cnt}"))
                    except Exception:
                        pass

                    try:
                        df_age = df_valid_records.withColumn("current_age", F.floor(F.datediff(F.col("valuation_date"), F.col("date_of_birth")) / 365.25))
                        failed_age_cnt = df_age.filter((F.col("current_age") < F.col("age_min")) | (F.col("current_age") > F.col("age_max"))).count()
                        status = "WARN" if failed_age_cnt > 0 else "PASS"
                        validation_results_pool.append(create_record(run_id, result_meta, "AGE_RANGE", status, failed_age_cnt, total_records, f"Demographic age target failures: {failed_age_cnt}"))
                    except Exception:
                        pass

                    # Premium Table Rate Calculation Variances
                    try:
                        if "policy_renewal_freq" in df_valid_records.columns:
                            df_rate_prep = df_valid_records.withColumn(
                                "frequency_multiplier",
                                F.when(F.lower(F.col("policy_renewal_freq")) == "annual", 1)
                                 .when(F.lower(F.col("policy_renewal_freq")) == "quarterly", 4)
                                 .when(F.lower(F.col("policy_renewal_freq")) == "monthly", 12)
                                 .otherwise(1)
                            )
                        else:
                            df_rate_prep = df_valid_records.withColumn("frequency_multiplier", F.lit(1))

                        join_cond = [
                            df_rate_prep.client_id == df_rate_tables.client_id,
                            df_rate_prep.treaty_id == df_rate_tables.treaty_id,
                            F.trim(df_rate_prep.benefit_id) == F.trim(df_rate_tables.benefit_id),
                            df_rate_prep.current_age.cast("int") == df_rate_tables.age.cast("int"),
                            F.upper(F.trim(df_rate_prep.gender)) == F.upper(F.trim(df_rate_tables.gender))
                        ]

                        df_rate_joined = df_rate_prep.join(df_rate_tables, on=join_cond, how="left")

                        missing_rates_cnt = df_rate_joined.filter(F.col("rate").isNull()).count()
                        if missing_rates_cnt > 0:
                            validation_results_pool.append(create_record(run_id, result_meta, "RATE_TABLE_MISSING_MATCH", "WARN", missing_rates_cnt, total_records, f"Missing mathematical rate schema definitions: {missing_rates_cnt}"))

                        df_calc = df_rate_joined.filter(F.col("rate").isNotNull()).withColumn(
                            "expected_premium", (F.col("reinsured_sum_at_risk") / 1000) * F.col("rate") * F.col("frequency_multiplier")
                        )
                        failed_rate_variance_cnt = df_calc.filter(F.abs(F.col("expected_premium") - F.col("premium_amount")) > 0.01).count()
                        status = "WARN" if failed_rate_variance_cnt > 0 else "PASS"
                        validation_results_pool.append(create_record(run_id, result_meta, "RATE_TABLE", status, failed_rate_variance_cnt, total_records, f"Premium table rate calculations variance: {failed_rate_variance_cnt}"))
                    except Exception:
                        pass

                # =========================================================================
                # CLAIMS VALIDATION RULES
                # =========================================================================
                elif category.upper() == "CLAIMS":
                    premium_history_path = f"s3a://silver/validated/{client_id}/premium/"
                    claims_history_path = f"s3a://silver/validated/{client_id}/claims/"

                    try:
                        df_premium_history = spark.read.format("delta").load(premium_history_path).filter(F.col("client_id") == client_id).cache()
                        has_premium_history = True
                    except Exception as load_warn:
                        logger.warning(f"Premium historical ledger targets missing or unreachable: {str(load_warn)}")
                        has_premium_history = False

                    # Claim Deduplication Routines
                    try:
                        w_dup = Window.partitionBy("claim_id", "policy_number", "benefit_id").orderBy(F.lit(1))
                        df_valid_records = df_valid_records.withColumn("_row_num", F.row_number().over(w_dup))
                        df_valid_records = flag_critical(df_valid_records, F.col("_row_num") > 1, "DUPLICATE_CLAIMS")
                        
                        try:
                            hash_query_claims = f"(SELECT claim_id, policy_number, benefit_id FROM persistent_hash_store WHERE client_id = '{client_id}' AND category = 'CLAIMS') AS known_hashes"
                            df_hist_claims = spark.read.jdbc(url=JDBC_URL, table=hash_query_claims, properties=JDBC_PROPS).withColumn("_is_hist_dup", F.lit(True))
                            df_valid_records = df_valid_records.join(df_hist_claims, on=["claim_id", "policy_number", "benefit_id"], how="left")
                            df_valid_records = flag_critical(df_valid_records, F.col("_is_hist_dup").isNotNull(), "DUPLICATE_CLAIMS")
                        except Exception:
                            pass
                            
                        total_dups = df_valid_records.filter(F.col("dq_reasons").contains("DUPLICATE_CLAIMS")).count()
                        status = "FAIL" if total_dups > 0 else "PASS"
                        validation_results_pool.append(create_record(run_id, result_meta, "DUPLICATE_CLAIMS", status, total_dups, total_records, f"Identified historical duplicate matches: {total_dups}"))
                    except Exception as ex:
                        validation_results_pool.append(create_error_record(run_id, result_meta, "DUPLICATE_CLAIMS", ex))

                    # Premium Coverage Verifications
                    try:
                        if not has_premium_history:
                            df_valid_records = flag_critical(df_valid_records, F.lit(True), "PREMIUM_EXISTS")
                            unmatched_premium_cnt = total_records
                        else:
                            df_p_keys = df_premium_history.select("treaty_id", "policy_number", "benefit_id").distinct().withColumn("_has_prem", F.lit(True))
                            df_valid_records = df_valid_records.join(df_p_keys, on=["treaty_id", "policy_number", "benefit_id"], how="left")
                            df_valid_records = flag_critical(df_valid_records, F.col("_has_prem").isNull(), "PREMIUM_EXISTS")
                            unmatched_premium_cnt = df_valid_records.filter(F.col("dq_reasons").contains("PREMIUM_EXISTS")).count()
                            
                        status = "FAIL" if unmatched_premium_cnt > 0 else "PASS"
                        validation_results_pool.append(create_record(run_id, result_meta, "PREMIUM_EXISTS", status, unmatched_premium_cnt, total_records, f"Unregistered claim components: {unmatched_premium_cnt}"))
                    except Exception as ex:
                        validation_results_pool.append(create_error_record(run_id, result_meta, "PREMIUM_EXISTS", ex))

                    # Claim Ceiling Boundary Analysis
                    try:
                        if "reinsured_sum_at_risk" in df_valid_records.columns:
                            df_valid_records = flag_critical(df_valid_records, F.col("reinsured_sum_at_risk") < F.col("claim_amount"), "RISAR_GTE_CLAIM")
                        elif has_premium_history:
                            df_max_risar = df_premium_history.groupBy("treaty_id", "policy_number", "benefit_id").agg(F.max("reinsured_sum_at_risk").alias("_max_risar"))
                            df_valid_records = df_valid_records.join(df_max_risar, on=["treaty_id", "policy_number", "benefit_id"], how="left")
                            df_valid_records = flag_critical(df_valid_records, F.col("_max_risar").isNull() | (F.col("_max_risar") < F.col("claim_amount")), "RISAR_GTE_CLAIM")
                        else:
                            df_valid_records = flag_critical(df_valid_records, F.lit(True), "RISAR_GTE_CLAIM")

                        failed_risar_claim_cnt = df_valid_records.filter(F.col("dq_reasons").contains("RISAR_GTE_CLAIM")).count()
                        status = "FAIL" if failed_risar_claim_cnt > 0 else "PASS"
                        validation_results_pool.append(create_record(run_id, result_meta, "RISAR_GTE_CLAIM", status, failed_risar_claim_cnt, total_records, f"Claims exceeding contractual liabilities: {failed_risar_claim_cnt}"))
                    except Exception as ex:
                        validation_results_pool.append(create_error_record(run_id, result_meta, "RISAR_GTE_CLAIM", ex))

                    # Claim Limit (CAL) Testing Layers
                    try:
                        df_valid_records = flag_critical(df_valid_records, (F.col("cal_level") == "policy") & (F.col("claim_amount") > F.col("cal_limit")), "CAL_CHECK")

                        df_cal_life = df_valid_records.filter(F.col("cal_level") == "life")
                        if df_cal_life.count() > 0:
                            try:
                                df_hist_claims_all = spark.read.format("delta").load(claims_history_path).filter((F.col("client_id") == client_id) & (F.col("file_hash") != f_hash))
                                df_life_combined = df_cal_life.select("insured_id", "treaty_id", "benefit_id", "claim_amount", "cal_limit").unionByName(
                                    df_hist_claims_all.join(df_cal_life.select("insured_id", "treaty_id", "benefit_id", "cal_limit").distinct(), on=["insured_id", "treaty_id", "benefit_id"], how="inner").select("insured_id", "treaty_id", "benefit_id", "claim_amount", "cal_limit")
                                )
                                df_life_agg = df_life_combined.groupBy("insured_id").agg(F.sum("claim_amount").alias("total_life_claims"), F.first("cal_limit").alias("cal_limit"))
                                df_life_bad = df_life_agg.filter(F.col("total_life_claims") > F.col("cal_limit")).select("insured_id").withColumn("_cal_life_breach", F.lit(True))
                                df_valid_records = df_valid_records.join(df_life_bad, on="insured_id", how="left")
                                df_valid_records = flag_critical(df_valid_records, F.col("_cal_life_breach").isNotNull() & (F.col("cal_level") == "life"), "CAL_CHECK")
                            except Exception as inner_ex:
                                logger.warning(f"Cumulative tracking checks for active life records skipped: {str(inner_ex)}")

                        total_cal_failures = df_valid_records.filter(F.col("dq_reasons").contains("CAL_CHECK")).count()
                        status = "FAIL" if total_cal_failures > 0 else "PASS"
                        validation_results_pool.append(create_record(run_id, result_meta, "CAL_CHECK", status, total_cal_failures, total_records, f"Contractual CAL balance violations recorded: {total_cal_failures}"))
                    except Exception as ex:
                        validation_results_pool.append(create_error_record(run_id, result_meta, "CAL_CHECK", ex))

                    # Financial Health Validations (Negative Check / Chronological Lapses)
                    try:
                        failed_neg_cnt = df_valid_records.filter(F.col("claim_amount") <= 0).count()
                        status = "WARN" if failed_neg_cnt > 0 else "PASS"
                        validation_results_pool.append(create_record(run_id, result_meta, "NO_ZERO_NEGATIVE", status, failed_neg_cnt, total_records, f"Zero value or negative claims detected: {failed_neg_cnt}"))
                    except Exception:
                        pass

                    try:
                        if has_premium_history:
                            df_prem_dates = df_premium_history.groupBy("treaty_id", "policy_number", "benefit_id").agg(
                                F.max("valuation_date").alias("last_paid_to_date"),
                                F.min("policy_effective_date").alias("prem_policy_effective_date")
                            )
                            df_dates_joined = df_valid_records.join(df_prem_dates, on=["treaty_id", "policy_number", "benefit_id"], how="left")
                            
                            failed_dol = df_dates_joined.filter(
                                (F.col("loss_event_date") > F.date_add(F.col("last_paid_to_date"), 365)) |
                                (F.col("loss_event_date") < F.col("prem_policy_effective_date")) |
                                (F.col("loss_event_date").isNull())
                            ).count()
                            status_dol = "WARN" if failed_dol > 0 else "PASS"
                            validation_results_pool.append(create_record(run_id, result_meta, "DOL_COVERAGE", status_dol, failed_dol, total_records, f"Chronological coverage exceptions tracked: {failed_dol}"))

                            failed_lapse = df_dates_joined.filter((F.datediff(F.col("loss_event_date"), F.col("last_paid_to_date")) > 30) | (F.col("last_paid_to_date").isNull())).count()
                            status_lapse = "WARN" if failed_lapse > 0 else "PASS"
                            validation_results_pool.append(create_record(run_id, result_meta, "LAPSE_CHECK", status_lapse, failed_lapse, total_records, f"Grace parameters expired: {failed_lapse}"))
                    except Exception:
                        pass

                # =========================================================================
                # THRESHOLD METRICS & PROMOTION LOGIC
                # =========================================================================
                row_integrity_failed = False
                post_join_count = df_valid_records.count()
                
                if post_join_count != total_records:
                    row_integrity_failed = True
                    validation_results_pool.append(create_record(
                        run_id, result_meta, "ROW_INTEGRITY_CHECK", "FAIL",
                        abs(post_join_count - total_records), total_records,
                        f"Join fan-out detected: expected {total_records}, got {post_join_count}. Forcing full quarantine."
                    ))
                    df_valid_records = df_valid_records.withColumn(
                        "dq_reasons",
                        F.when(F.col("dq_reasons") == "", F.lit("ROW_INTEGRITY_CHECK"))
                         .otherwise(F.concat_ws("|", F.col("dq_reasons"), F.lit("ROW_INTEGRITY_CHECK")))
                    )

                df_quarantine = df_valid_records.filter(F.col("dq_reasons") != "")
                df_clean = df_valid_records.filter(F.col("dq_reasons") == "")

                quarantined_rows = df_quarantine.count()
                quarantine_ratio = 1.0 if row_integrity_failed else (quarantined_rows / total_records if total_records > 0 else 0)
                quarantine_csv_path = None

                if quarantined_rows > 0:
                    quarantine_dir = "/opt/airflow/quarantine"
                    os.makedirs(quarantine_dir, exist_ok=True)
                    q_filename = f"{meta['file_name'].replace('.csv', '')}_quarantine.csv"
                    quarantine_csv_path = os.path.join(quarantine_dir, q_filename)
                    
                    df_q_pd = df_quarantine.withColumn("validation_run_id", F.lit(run_id)).withColumn("quarantine_timestamp", F.current_timestamp()).toPandas()
                    
                    rule_counts = {}
                    for reasons in df_q_pd['dq_reasons']:
                        for r in reasons.split('|'):
                            if r:
                                rule_counts[r] = rule_counts.get(r, 0) + 1
                    rule_breakdown = "\n".join([f"  - {k}: {v} rows" for k, v in rule_counts.items()])
                    
                    cols_to_drop = [c for c in df_q_pd.columns if c.startswith("_")]
                    df_q_pd.drop(columns=cols_to_drop, inplace=True, errors="ignore")
                    df_q_pd.to_csv(quarantine_csv_path, index=False)

                    subject = f"[RI Platform] Automated Quarantine Alert - {meta['file_name']}"
                    
                    # Core 30% Quarantine Fail-Safe Trigger Cutoff
                    if quarantine_ratio >= 0.30:
                        body = f"""File {meta['file_name']} CRITICALLY REJECTED.

Quarantine Threshold Breach: {quarantine_ratio:.2%} (Configured Upper Bound 30%)
Total Records Handled     : {total_records}
Quarantined Fault Rows    : {quarantined_rows}

Categorical Rule Breakdowns:
{rule_breakdown}

This payload batch data has been fully rejected. No structures were promoted downstream to Silver.
"""
                        send_ops_email(subject, body, quarantine_csv_path)
                        validation_results_pool.append(create_record(run_id, result_meta, "FILE_REJECTED", "FAIL", quarantined_rows, total_records, "Quarantine tolerance limit exceeded >= 30%"))
                    else:
                        body = f"""File {meta['file_name']} processed under threshold limits. Partial quarantine routing applied.

Quarantine Ratio Checked  : {quarantine_ratio:.2%}
Total Input Rows Checked  : {total_records}
Quarantined Target Rows   : {quarantined_rows}

Categorical Rule Breakdowns:
{rule_breakdown}

Conforming datasets successfully migrated into active target layers. Anomalies attached.
"""
                        send_ops_email(subject, body, quarantine_csv_path)

                # Execute Downstream Promotional Merges only if under the 30% threshold
                if quarantine_ratio < 0.30:
                    try:
                        cols_to_drop = [c for c in df_clean.columns if c.startswith("_") or c == "dq_reasons"]
                        df_clean = df_clean.drop(*cols_to_drop)
                        
                        df_clean = df_clean \
                            .withColumn("validation_run_id", F.lit(run_id)) \
                            .withColumn("period_type", F.lit(meta["period_type"])) \
                            .withColumn("year", F.lit(str(meta["year"]))) \
                            .withColumn("period", F.lit(str(meta["period"])))

                        validated_path = f"s3a://silver/validated/{client_id}/{category}/"
                        merge_keys = MERGE_KEYS.get(category)

                        if merge_keys and DeltaTable.isDeltaTable(spark, validated_path):
                            logger.info(f"Executing Delta Upsert Merge Operations for: {validated_path}")
                            target = DeltaTable.forPath(spark, validated_path)
                            merge_condition = ("target.year = source.year AND target.period = source.period AND " + " AND ".join([f"target.{k} = source.{k}" for k in merge_keys]))
                            target.alias("target").merge(df_clean.alias("source"), merge_condition).whenMatchedUpdateAll().whenNotMatchedInsertAll().execute()
                        else:
                            logger.info(f"Initializing or appending Delta partitions natively inside: {validated_path}")
                            df_clean.write.format("delta").mode("append").partitionBy("year", "period").option("mergeSchema", "true").save(validated_path)
                    except Exception as promo_err:
                        logger.error(f"WARNING: Pipeline promotion phase aborted unexpectedly for hash context {f_hash}: {str(promo_err)}")

                df_valid_records.unpersist()
            df_joined.unpersist()

        # Write Pipeline Run Analysis Back to Postgres RDBMS metadata spaces
        if validation_results_pool:
            schema_fields = ["run_id", "client_id", "treaty_id", "file_hash", "file_name", "category", "year", "period", "check_name", "status", "failed_count", "total_count", "message", "checked_at"]
            rows_list = []
            
            for item in validation_results_pool:
                rows_list.append(Row(
                    run_id=str(item["run_id"]), client_id=str(item["client_id"]),
                    treaty_id=str(item["treaty_id"]) if item["treaty_id"] else "UNRESOLVED",
                    file_hash=str(item["file_hash"]), file_name=str(item["file_name"]), category=str(item["category"]),
                    year=str(item["year"]), period=str(item["period"]), check_name=str(item["check_name"]),
                    status=str(item["status"]), failed_count=int(item["failed_count"]), total_count=int(item["total_count"]),
                    message=str(item["message"]), checked_at=item["checked_at"]
                ))
            
            df_writeout = spark.createDataFrame(rows_list).select(*schema_fields)
            df_writeout.write.jdbc(url=JDBC_URL, table="validation_results", mode="append", properties=JDBC_PROPS)

        # Output Summary Metrics Logging Reports
        file_summaries = {}
        global_counters = {"PASS": 0, "FAIL": 0, "WARN": 0, "SKIPPED": 0}
        
        for item in validation_results_pool:
            file_summaries.setdefault(item["file_name"], []).append(item)
            if item["status"] in global_counters:
                global_counters[item["status"]] += 1

        for file_name, checks in file_summaries.items():
            logger.info(f"Summary Analysis Report Metrics for File Trace: {file_name}")
            for c in checks:
                logger.info(f" -> Policy Assertion Check: {c['check_name']:<30} | Status Core: {c['status']:<8} | Violation Count: {c['failed_count']}/{c['total_count']}")
        
        logger.info("=======================================================================")
        logger.info(f"METRICS POOL LIFECYCLE SUMMARY: PASS={global_counters['PASS']} | FAIL={global_counters['FAIL']} | WARN={global_counters['WARN']} | SKIPPED={global_counters['SKIPPED']}")
        logger.info("=======================================================================")

    except Exception as master_exception:
        logger.critical(f"FATAL RECOVERY BLOCK TRIGGERED: UNHANDLED ROOT EXCEPTION: {str(master_exception)}")
        traceback.print_exc()
    finally:
        logger.info("De-allocating analytical data session pipelines. Core teardown clean steps running.")
        spark.stop()
        sys.exit(0)

# ==============================================================================
# AUDITING SCHEMA LOG STRUCT BUILDERS
# ==============================================================================

def create_record(run_id, meta, check_name, status, failed_count, total_count, message):
    meta_dict = meta.asDict() if hasattr(meta, "asDict") else dict(meta)
    return {
        "run_id": run_id, "client_id": meta_dict["client_id"], "treaty_id": meta_dict.get("treaty_id", "UNRESOLVED"),
        "file_hash": meta_dict["file_hash"], "file_name": meta_dict["file_name"], "category": meta_dict["category"],
        "year": str(meta_dict["year"]), "period": str(meta_dict["period"]), "check_name": check_name,
        "status": status, "failed_count": int(failed_count), "total_count": int(total_count),
        "message": message[:200], "checked_at": datetime.datetime.now()
    }

def create_error_record(run_id, meta, check_name, exception):
    meta_dict = meta.asDict() if hasattr(meta, "asDict") else dict(meta)
    return {
        "run_id": run_id, "client_id": meta_dict["client_id"], "treaty_id": meta_dict.get("treaty_id", "UNRESOLVED"),
        "file_hash": meta_dict["file_hash"], "file_name": meta_dict["file_name"], "category": meta_dict["category"],
        "year": str(meta_dict["year"]), "period": str(meta_dict["period"]), "check_name": check_name,
        "status": "FAIL", "failed_count": 0, "total_count": 0,
        "message": f"PIPELINE_RUNTIME_CRITICAL_ERROR: {str(exception)[:180]}", "checked_at": datetime.datetime.now()
    }

if __name__ == "__main__":
    main()