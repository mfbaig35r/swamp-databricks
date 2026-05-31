# API ingest pattern

A reusable Swamp + Databricks pattern for pulling data from external HTTP
APIs into a Delta Lake table, with a typed Silver view on top.

Reference implementation: [met-museum](./met-museum). The Met Museum API
needs no authentication, which makes it the easiest "hello world" for this
pattern. Adapt to authenticated APIs (Stripe, GitHub, Salesforce, etc.) by
changing the configuration block at the top of the bronze notebook.

## Architecture

```
┌──────────────────┐
│   External API   │  e.g. metmuseum.github.io
└─────────┬────────┘
          │ HTTP, rate-limited
          ▼
┌──────────────────┐
│ Bronze (raw)     │  one row per API response, raw_json STRING
│ <schema>.bronze  │  http_status, error_message, ingested_at
└─────────┬────────┘
          │ SQL extraction (idempotent)
          ▼
┌──────────────────┐
│ Silver (typed)   │  typed columns extracted from raw_json
│ <schema>.silver  │  used by analytics, ML, dashboards
└──────────────────┘
```

## What's universal

Every API ingestion has the same Spark fan-out engine. The variables that
change per API are encapsulated in a **configuration block** at the top of
the bronze notebook:

| Variable | What it controls | Example |
|---|---|---|
| `API_NAME` | Used in table/schema names | `met_museum`, `stripe`, `github` |
| `LIST_URL` | Endpoint that returns all IDs (or cursor-paginated list) | Met `/objects`; Stripe `/v1/customers?limit=100` |
| `DETAIL_URL_TEMPLATE` | Per-row fetch URL with `{id}` placeholder | Met `/objects/{id}`; GitHub `/repos/{id}` |
| `RATE_PER_SEC` | Provider's rate limit | Met 80; Stripe 100; GitHub 1.4 (5000/hr) |
| `NUM_PARTITIONS` | Spark parallelism | 8 by default; per-partition rate = `RATE_PER_SEC / NUM_PARTITIONS` |
| `build_headers()` | Auth header factory | `{}` for public; `{"Authorization": f"Bearer {token}"}` for OAuth |
| `parse_list_response()` | Extract list of IDs from the list endpoint response | API-specific shape |
| `extract_key()` | The unique key on each detail response | Often just `objectID` or `id` |

The rest of the notebook (Spark fan-out, token bucket, retries, Session
reuse, bronze write) is reusable as-is.

## What's API-specific

Two things change beyond the config block:

1. **Pagination of the list endpoint.** Some APIs return all IDs in one
   array (Met). Most use cursor pagination (Stripe `starting_after`),
   offset pagination (Salesforce SOQL), or `Link` header pagination
   (GitHub). Replace the list-fetch step accordingly. The detail fetch
   loop stays the same.
2. **Silver field extraction.** Each API has its own JSON shape, so each
   gets its own Silver `CREATE OR REPLACE TABLE ... AS SELECT raw_json:...`
   SQL. The Bronze layer (raw_json + metadata columns) is identical across
   all APIs.

## Why Bronze stays raw

Schema drift is the reason APIs cost so much to maintain in warehouses.
Vendors add and remove fields; field types change; nested shapes vary by
record. If you parse JSON to typed columns at Bronze, every schema change
breaks ingest.

Bronze in this pattern is:

```sql
CREATE TABLE bronze.<table> (
  object_id      BIGINT,
  http_status    INT,
  raw_json       STRING,
  error_message  STRING,
  ingested_at    TIMESTAMP
)
```

Five columns. Cannot drift. Captures everything including errors. Silver
extracts what you care about today; tomorrow's new field is already
captured in raw_json, you just add a Silver column when you need it.

## Rate-limited Spark fan-out

The load-bearing trick: **`mapInPandas`** with one Session and one token
bucket per partition. Math:

```
RATE_PER_SEC = 80          # provider's cap
NUM_PARTITIONS = 8         # Spark parallelism
per_partition_rate = 10    # = RATE_PER_SEC / NUM_PARTITIONS
```

Each partition opens a `requests.Session` (connection reuse), holds a
token bucket that refills at `per_partition_rate`, and acquires a token
before each request. Across 8 partitions you get exactly 80 req/sec global
throughput, no central coordination needed.

Retries: 429 and 5xx only, exponential backoff. 404 means "this ID doesn't
exist" and gets recorded as `error_message: not_found` rather than
retried.

**Why `mapInPandas` and not `rdd.mapPartitions`?** Databricks serverless
compute rejects RDD operations with `NOT_IMPLEMENTED`. `mapInPandas`
provides the same per-partition execution model with a pandas DataFrame
interface and works on both serverless and classic clusters. The
serverless restriction was confirmed end-to-end against Databricks Free
on the reference Met implementation.

## Adapt to another API

Approximate effort to fork this pattern for a new API:

| Task | Time |
|---|---|
| Update config block (URLs, rate, auth) | 5 min |
| Replace list-fetch if it's cursor-paginated | 15-30 min |
| Write Silver SQL for the new schema | 15-30 min |
| Update workflow.yaml to point at new table/schema names | 5 min |

For most public REST APIs, **45-90 minutes from "I want this data in
Databricks" to a running scheduled workflow.**

## Variations not shown in the reference

- **Restart safety via UC Volume staging.** Write raw responses to
  `/Volumes/<catalog>/<schema>/raw/{id}.json` first, then `COPY INTO`
  Bronze. Lets you resume a failed long-running pull without re-fetching.
- **Incremental refresh.** Diff `LIST_URL` response against existing
  `bronze.<table>.object_id` and only fetch new IDs. Compresses repeat
  runs from hours to seconds.
- **Multi-source pull.** Several APIs into one workspace? Same pattern,
  different config blocks. Consider a shared `_lib/api_ingest.py` notebook
  imported via `%run`.
