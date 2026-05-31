# Databricks notebook source
# Generic file-ingest-from-S3 template.
#
# Config block at top is what you change. Engine below it is reusable
# across CSV / Parquet / JSON file ingest jobs.
#
# For production schema-detection and type-inference, replace the body
# of this notebook with an AGI ingest_to_evidence call. See README.md.

# COMMAND ----------

# === Configuration (CHANGE THIS for your file source) =====================

S3_BUCKET_AND_PREFIX = None  # set via job parameter, e.g. "s3://my-bucket/vendor-drops/customers/"
FILE_PATTERN = None          # set via job parameter, e.g. "*.csv"
TARGET_TABLE = None          # set via job parameter, e.g. "analytics.vendor_drops.customers"
FILE_FORMAT = "csv"          # csv | parquet | json | delta

# CSV-specific options. Ignored for Parquet/JSON.
CSV_HEADER = True
CSV_INFER_SCHEMA = True      # set False for production; type drift breaks ingest
CSV_DELIMITER = ","
CSV_NULL_VALUE = ""

# Set MAX_FILES to a small integer to limit ingest for smoke testing.
# None means ingest everything matched by S3_BUCKET_AND_PREFIX + FILE_PATTERN.
MAX_FILES = None

# === Read job parameters ===================================================

try:
    S3_BUCKET_AND_PREFIX = dbutils.widgets.get("s3_path") or S3_BUCKET_AND_PREFIX
except Exception:
    pass
try:
    FILE_PATTERN = dbutils.widgets.get("file_pattern") or FILE_PATTERN
except Exception:
    pass
try:
    TARGET_TABLE = dbutils.widgets.get("target_table") or TARGET_TABLE
except Exception:
    pass
try:
    _mf = dbutils.widgets.get("max_files")
    MAX_FILES = int(_mf) if _mf else MAX_FILES
except Exception:
    pass

assert S3_BUCKET_AND_PREFIX, "s3_path must be set (job parameter or hardcoded)"
assert FILE_PATTERN, "file_pattern must be set"
assert TARGET_TABLE, "target_table must be set"

# === Configure S3 auth =====================================================

# Reads AWS creds from Databricks workspace secret scope `s3_creds`.
# The Swamp workflow creates that scope via secret_scope + secret.put steps.
# If you use an instance profile or UC external location instead, remove
# this block and remove the secret_* steps from workflow.yaml.

import os
try:
    aws_access_key = dbutils.secrets.get("s3_creds", "aws_access_key_id")
    aws_secret_key = dbutils.secrets.get("s3_creds", "aws_secret_access_key")
    spark.conf.set("fs.s3a.access.key", aws_access_key)
    spark.conf.set("fs.s3a.secret.key", aws_secret_key)
    print("S3 credentials configured from secret scope")
except Exception as e:
    print(f"No secret scope s3_creds found; relying on workspace S3 access ({e})")

# === Universal read + write engine =========================================

from pyspark.sql import functions as F

read_path = f"{S3_BUCKET_AND_PREFIX.rstrip('/')}/{FILE_PATTERN}"
print(f"Reading {FILE_FORMAT} from {read_path}")

reader = spark.read.format(FILE_FORMAT)
if FILE_FORMAT == "csv":
    reader = (
        reader
        .option("header", str(CSV_HEADER).lower())
        .option("inferSchema", str(CSV_INFER_SCHEMA).lower())
        .option("sep", CSV_DELIMITER)
        .option("nullValue", CSV_NULL_VALUE)
    )

raw_df = reader.load(read_path)
raw_df = raw_df.withColumn("source_file", F.input_file_name())
raw_df = raw_df.withColumn("ingested_at", F.current_timestamp())

if MAX_FILES is not None and MAX_FILES > 0:
    distinct_files = (
        raw_df.select("source_file").distinct().limit(MAX_FILES)
    )
    raw_df = raw_df.join(
        F.broadcast(distinct_files), on="source_file", how="inner"
    )
    print(f"MAX_FILES={MAX_FILES} applied; limiting to {MAX_FILES} files")

# Ensure target schema exists. Catalog must exist already.
target_catalog, target_schema, target_table = TARGET_TABLE.split(".")
spark.sql(f"CREATE SCHEMA IF NOT EXISTS {target_catalog}.{target_schema}")

# Write to Bronze.
raw_df.write.mode("overwrite").saveAsTable(TARGET_TABLE)

row_count = spark.table(TARGET_TABLE).count()
print(f"Wrote {row_count} rows to {TARGET_TABLE}")

import json
dbutils.notebook.exit(json.dumps({
    "rows": int(row_count),
    "table": TARGET_TABLE,
    "source": read_path,
}))
