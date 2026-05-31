# Template: file ingest from S3

A reusable Swamp + Databricks template for landing files from S3 (or any
cloud storage path Databricks can read) into a UC Bronze table, with an
optional Silver typed view.

**Use when**: a vendor drops CSVs in S3, a data lake delivers Parquet
files, or any "file lands in object storage, I want it queryable as a
table" workflow.

**Don't use this for**: external API ingest (use
[`../../api-ingest/`](../../api-ingest/)), streaming sources (use
DLT directly), or files already in a UC volume managed by Databricks
(write SQL `COPY INTO` instead).

## What this template does

1. Ensures the target UC schema exists
2. Configures S3 credentials in workspace secrets (idempotent)
3. Uploads a Spark notebook that reads from a configurable S3 path
4. Creates an idempotent job that runs the notebook
5. Runs the job and waits for terminal state
6. Builds a Silver typed view via SQL warehouse
7. Snapshots the result via `uc_table.read`

The Bronze layer captures every row from the source files with metadata
(`source_file`, `ingested_at`); the Silver layer does typed extraction.

## Where AGI fits in

For nontrivial file ingest (schema detection, type inference, date format
parsing, currency/parens/decimal handling, schema drift detection), the
right move is to call [AGI](https://github.com/mfbaig/artificial-general-ingestion)
from inside the notebook via its MCP surface or Python wrapper. AGI handles
all of the above as battle-tested algorithms; do not regenerate them in
every notebook.

This template shows the **simple case** (CSV with header, Spark infers
schema) that works without AGI. For production file ingest, swap the
notebook body for a single `ingest_to_evidence` call.

## Customize before running

In `workflow.yaml`, change:

- `inputs.s3_path` default: point at your actual S3 prefix
- `inputs.file_pattern` default: e.g. `*.csv` or `customer-*.csv`
- `inputs.target_table`: e.g. `analytics.vendor_drops.customers`
- The Silver SQL: extract whichever columns matter for your use case

In the notebook content (inlined in `workflow.yaml`):

- `FILE_FORMAT`: `csv`, `parquet`, `json`, etc.
- Spark read options: header, delimiter, schema inference behavior

## Auth setup

Default flow uses AWS credentials in a Databricks secret scope:

```sh
swamp vault set s3_creds aws_access_key_id     # your local Swamp vault
swamp vault set s3_creds aws_secret_access_key  # your local Swamp vault
```

The workflow's `secret_scope` + `secret.put` steps read from your Swamp
vault via CEL `${{ vault.get(...) }}` and write to a Databricks workspace
secret scope named `s3_creds`. The notebook reads them via
`dbutils.secrets.get("s3_creds", "...")`.

Alternative: use an instance profile (workspace-level config) or UC
external locations with storage credentials; remove the `secret_scope`
and `secret.put` steps from the workflow if you go that route.

## Standalone notebook source

For clean review and editing, the standalone notebook lives at
[`notebook.py`](./notebook.py) on GitHub (the `.py` extension is not
allowed in the Swamp registry archive, so it ships only via GitHub).

The same Python is inlined in `workflow.yaml` under
`notebook.upload`'s `content:` field, so the workflow runs directly
without needing the standalone file.

## Smoke test before scheduling

Run with `max_files=1` (or a small number) to validate the full chain
without ingesting your whole vendor drop:

```sh
swamp workflow run workflow.yaml --input max_files=1
```

If that succeeds, drop the `max_files` input and add a `schedule:` field
for production.
