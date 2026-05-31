# Databricks notebook source
# Stripe customers -> Bronze ingest.
#
# Authenticated, cursor-paginated, rate-limited HTTP API ingest. Same
# universal engine as the Met Museum reference, with the auth +
# pagination story plugged in.
#
# To adapt for another provider:
# - Update API_BASE, LIST_ENDPOINT, RATE_PER_SEC in the config block
# - Update build_headers() for your auth scheme
# - Update parse_list_response() + list-fetch loop for your pagination

# COMMAND ----------

# === Configuration (Stripe customers) =====================================

API_NAME = "stripe"
API_BASE = "https://api.stripe.com"
LIST_ENDPOINT = "/v1/customers"
PAGE_SIZE = 100  # Stripe max per page
RATE_PER_SEC = 100  # Stripe's default rate limit
NUM_PARTITIONS = 10  # per_partition_rate = 100 / 10 = 10 req/sec
REQUEST_TIMEOUT_SEC = 30
RETRY_MAX_ATTEMPTS = 3

TARGET_CATALOG = "workspace"
TARGET_SCHEMA = "stripe"
TARGET_BRONZE_TABLE = "bronze_customers"

try:
    TARGET_CATALOG = dbutils.widgets.get("catalog") or TARGET_CATALOG
except Exception:
    pass
try:
    _mr = dbutils.widgets.get("max_records")
    MAX_RECORDS = int(_mr) if _mr else None
except Exception:
    MAX_RECORDS = None


def build_headers():
    """Bearer token auth from Databricks secret scope."""
    api_key = dbutils.secrets.get("stripe", "api_key")
    return {
        "Authorization": f"Bearer {api_key}",
        # Optionally pin an API version for stability:
        # "Stripe-Version": "2023-10-16",
    }


def parse_list_response(response_json):
    """Stripe returns {data: [...], has_more: bool}. We return list of IDs."""
    return [obj["id"] for obj in response_json.get("data", [])]


# === Universal engine =====================================================

import json
import time
from typing import Iterator

import pandas as pd
import requests
from pyspark.sql import functions as F
from pyspark.sql.types import (
    IntegerType,
    LongType,
    StringType,
    StructField,
    StructType,
)

# Phase 1: stream the list endpoint, collect all IDs with cursor pagination.
session = requests.Session()
session.headers.update(build_headers())

all_ids = []
cursor = None
page = 0
while True:
    params = {"limit": PAGE_SIZE}
    if cursor:
        params["starting_after"] = cursor
    response = session.get(
        API_BASE + LIST_ENDPOINT, params=params, timeout=REQUEST_TIMEOUT_SEC
    )
    response.raise_for_status()
    body = response.json()
    page_ids = parse_list_response(body)
    if not page_ids:
        break
    all_ids.extend(page_ids)
    page += 1
    print(f"{API_NAME}: list page {page}, got {len(page_ids)} IDs (total {len(all_ids)})")
    if not body.get("has_more"):
        break
    cursor = page_ids[-1]
    if MAX_RECORDS is not None and len(all_ids) >= MAX_RECORDS:
        all_ids = all_ids[:MAX_RECORDS]
        print(f"{API_NAME}: MAX_RECORDS={MAX_RECORDS} reached")
        break

print(f"{API_NAME}: total {len(all_ids)} IDs to fetch")

# COMMAND ----------

# Phase 2: rate-limited per-row detail fetch via mapInPandas.

per_partition_rate = max(RATE_PER_SEC / NUM_PARTITIONS, 0.1)
DETAIL_URL_TEMPLATE = API_BASE + LIST_ENDPOINT + "/{id}"


def fetch_partition(iterator: Iterator[pd.DataFrame]) -> Iterator[pd.DataFrame]:
    sess = requests.Session()
    sess.headers.update(build_headers())
    last_refill = time.monotonic()
    tokens = per_partition_rate

    for pdf in iterator:
        rows_out = []
        for object_id in pdf["id"]:
            now = time.monotonic()
            elapsed = now - last_refill
            tokens = min(
                per_partition_rate, tokens + elapsed * per_partition_rate
            )
            last_refill = now
            if tokens < 1:
                time.sleep((1 - tokens) / per_partition_rate)
                tokens = 0
            else:
                tokens -= 1

            url = DETAIL_URL_TEMPLATE.format(id=object_id)
            attempt = 0
            while True:
                try:
                    resp = sess.get(url, timeout=REQUEST_TIMEOUT_SEC)
                    status = resp.status_code
                    if status == 200:
                        rows_out.append((str(object_id), status, resp.text, None))
                        break
                    if status == 404:
                        rows_out.append((str(object_id), status, None, "not_found"))
                        break
                    if status in (429, 500, 502, 503, 504):
                        attempt += 1
                        if attempt >= RETRY_MAX_ATTEMPTS:
                            rows_out.append(
                                (str(object_id), status, None, f"retries_exhausted ({status})")
                            )
                            break
                        time.sleep(2 ** attempt)
                        continue
                    rows_out.append(
                        (str(object_id), status, None, f"unexpected_status ({status})")
                    )
                    break
                except requests.exceptions.RequestException as exc:
                    attempt += 1
                    if attempt >= RETRY_MAX_ATTEMPTS:
                        rows_out.append((str(object_id), 0, None, f"network_error: {exc}"))
                        break
                    time.sleep(2 ** attempt)
        yield pd.DataFrame(
            rows_out,
            columns=["object_id", "http_status", "raw_json", "error_message"],
        )


result_schema = StructType([
    StructField("object_id", StringType(), True),
    StructField("http_status", IntegerType(), True),
    StructField("raw_json", StringType(), True),
    StructField("error_message", StringType(), True),
])

if not all_ids:
    print(f"{API_NAME}: no IDs to fetch, exiting")
    dbutils.notebook.exit(json.dumps({"rows": 0, "skipped": True}))

id_df = spark.createDataFrame([(str(i),) for i in all_ids], ["id"])
results_df = (
    id_df.repartition(NUM_PARTITIONS)
    .mapInPandas(fetch_partition, schema=result_schema)
    .withColumn("ingested_at", F.current_timestamp())
)

# COMMAND ----------

spark.sql(f"CREATE SCHEMA IF NOT EXISTS {TARGET_CATALOG}.{TARGET_SCHEMA}")
full_table = f"{TARGET_CATALOG}.{TARGET_SCHEMA}.{TARGET_BRONZE_TABLE}"
# WRITE MODE: this template defaults to "overwrite" because it does a full
# pull every run. If you change the list-fetch loop to use an incremental
# filter (e.g. Stripe's created[gte]=<last_run>), switch this to
# mode("append") AND add a MERGE INTO step downstream to dedupe on
# object_id. Failing to change the mode will silently wipe yesterday's
# data on every run.
results_df.write.mode("overwrite").saveAsTable(full_table)

row_count = spark.table(full_table).count()
success_count = spark.sql(
    f"SELECT COUNT(*) AS n FROM {full_table} WHERE http_status = 200"
).collect()[0]["n"]
failure_count = row_count - success_count
print(
    f"{API_NAME}: wrote {row_count} rows ({success_count} success, {failure_count} failure) "
    f"to {full_table}"
)

dbutils.notebook.exit(json.dumps({
    "rows": int(row_count),
    "success": int(success_count),
    "failure": int(failure_count),
    "table": full_table,
}))
