# Databricks notebook source
# Met Museum -> Bronze ingest
#
# Reference implementation of the universal API-ingest pattern documented
# at examples/api-ingest/README.md. The configuration block below is the
# only part that changes when adapting this notebook to a different API.
# Everything under "# === Universal engine ===" is API-agnostic.

# COMMAND ----------

# === Configuration (CHANGE THIS for other APIs) =========================

API_NAME = "met_museum"
LIST_URL = "https://collectionapi.metmuseum.org/public/collection/v1/objects"
DETAIL_URL_TEMPLATE = (
    "https://collectionapi.metmuseum.org/public/collection/v1/objects/{id}"
)
RATE_PER_SEC = 80          # provider's rate cap (req/sec across the whole job)
NUM_PARTITIONS = 8         # Spark parallelism; per-partition rate = RATE / N
REQUEST_TIMEOUT_SEC = 30
RETRY_MAX_ATTEMPTS = 3
TARGET_SCHEMA = "met_museum"
TARGET_BRONZE_TABLE = "bronze_objects"

# Job parameters override these defaults if set. Wrapped in try/except so the
# notebook also runs interactively without widgets configured.
try:
    TARGET_CATALOG = dbutils.widgets.get("catalog") or "workspace"
except Exception:
    TARGET_CATALOG = "workspace"

# Set MAX_OBJECTS to None for a full ~470K backfill (~100 min wall time on Met).
# Set to a small integer (e.g. 10, 100, 1000) for fast testing.
try:
    _max = dbutils.widgets.get("max_objects")
    MAX_OBJECTS = int(_max) if _max else None
except Exception:
    MAX_OBJECTS = None


def build_headers():
    """Return HTTP headers for every request. Met is public, so empty.

    For authenticated APIs:
        return {"Authorization": f"Bearer {dbutils.secrets.get('scope', 'key')}"}
    """
    return {}


def parse_list_response(response_json):
    """Extract the list of object IDs from the LIST_URL response.

    Met returns {"total": N, "objectIDs": [int, int, ...]}. Other APIs
    will use different shapes (Stripe: response["data"]; GitHub: array
    at root). Override per API.
    """
    return response_json.get("objectIDs", []) or []


# === Universal engine (do NOT modify per API) ===========================

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

# COMMAND ----------

# Fetch the list of IDs from LIST_URL.
session = requests.Session()
session.headers.update(build_headers())
list_response = session.get(LIST_URL, timeout=REQUEST_TIMEOUT_SEC)
list_response.raise_for_status()
all_ids = parse_list_response(list_response.json())
print(f"{API_NAME}: list endpoint returned {len(all_ids)} IDs")

if MAX_OBJECTS is not None and MAX_OBJECTS > 0:
    all_ids = all_ids[:MAX_OBJECTS]
    print(f"{API_NAME}: limited to MAX_OBJECTS={MAX_OBJECTS}")

# COMMAND ----------

# Rate-limited fan-out: mapPartitions with one Session + one token bucket
# per partition. Total throughput = NUM_PARTITIONS * per_partition_rate.

per_partition_rate = max(RATE_PER_SEC / NUM_PARTITIONS, 0.1)


def fetch_partition(iterator: Iterator[pd.DataFrame]) -> Iterator[pd.DataFrame]:
    """Run inside each Spark partition. Maintains one Session and one
    token bucket for the partition's lifetime.

    Uses mapInPandas signature (required on Databricks serverless, which
    rejects RDD operations). Each yielded DataFrame matches result_schema.
    """
    sess = requests.Session()
    sess.headers.update(build_headers())
    last_refill = time.monotonic()
    tokens = per_partition_rate

    for pdf in iterator:
        rows_out = []
        for object_id in pdf["id"]:
            now = time.monotonic()
            elapsed = now - last_refill
            tokens = min(per_partition_rate, tokens + elapsed * per_partition_rate)
            last_refill = now
            if tokens < 1:
                time.sleep((1 - tokens) / per_partition_rate)
                tokens = 0
            else:
                tokens -= 1

            url = DETAIL_URL_TEMPLATE.format(id=int(object_id))
            attempt = 0
            while True:
                try:
                    resp = sess.get(url, timeout=REQUEST_TIMEOUT_SEC)
                    status = resp.status_code
                    if status == 200:
                        rows_out.append((int(object_id), status, resp.text, None))
                        break
                    if status == 404:
                        rows_out.append((int(object_id), status, None, "not_found"))
                        break
                    if status in (429, 500, 502, 503, 504):
                        attempt += 1
                        if attempt >= RETRY_MAX_ATTEMPTS:
                            rows_out.append(
                                (int(object_id), status, None, f"retries_exhausted ({status})")
                            )
                            break
                        time.sleep(2 ** attempt)
                        continue
                    rows_out.append(
                        (int(object_id), status, None, f"unexpected_status ({status})")
                    )
                    break
                except requests.exceptions.RequestException as exc:
                    attempt += 1
                    if attempt >= RETRY_MAX_ATTEMPTS:
                        rows_out.append((int(object_id), 0, None, f"network_error: {exc}"))
                        break
                    time.sleep(2 ** attempt)
        yield pd.DataFrame(
            rows_out,
            columns=["object_id", "http_status", "raw_json", "error_message"],
        )


# COMMAND ----------

# Drive the fan-out: build a DataFrame of IDs, repartition for parallelism,
# mapPartitions through fetch_partition, write to Bronze.

result_schema = StructType(
    [
        StructField("object_id", LongType(), True),
        StructField("http_status", IntegerType(), True),
        StructField("raw_json", StringType(), True),
        StructField("error_message", StringType(), True),
    ]
)

id_df = spark.createDataFrame([(int(i),) for i in all_ids], ["id"])
results_df = (
    id_df.repartition(NUM_PARTITIONS)
    .mapInPandas(fetch_partition, schema=result_schema)
    .withColumn("ingested_at", F.current_timestamp())
)

# COMMAND ----------

# Ensure the target schema exists (idempotent), then write Bronze.
spark.sql(f"CREATE SCHEMA IF NOT EXISTS {TARGET_CATALOG}.{TARGET_SCHEMA}")
full_table = f"{TARGET_CATALOG}.{TARGET_SCHEMA}.{TARGET_BRONZE_TABLE}"
results_df.write.mode("overwrite").saveAsTable(full_table)

# COMMAND ----------

# Summary.
row_count = spark.table(full_table).count()
success_count = spark.sql(
    f"SELECT COUNT(*) AS n FROM {full_table} WHERE http_status = 200"
).collect()[0]["n"]
failure_count = row_count - success_count
print(
    f"{API_NAME}: wrote {row_count} rows to {full_table} "
    f"({success_count} 200, {failure_count} non-200)"
)

dbutils.notebook.exit(
    json.dumps(
        {
            "rows": int(row_count),
            "success": int(success_count),
            "failure": int(failure_count),
            "table": full_table,
        }
    )
)
