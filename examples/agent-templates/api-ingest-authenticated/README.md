# Template: authenticated API ingest

Extends the [Met Museum API ingest pattern](../../api-ingest/met-museum/)
with authentication (API key, Bearer token, OAuth client credentials) and
cursor pagination. Stripe is the worked example because it's well‑known
and uses both: Bearer token auth + cursor pagination.

**Use when**: ingesting from any rate‑limited HTTP API that needs auth
and pagination. Stripe (customers, charges, subscriptions). GitHub
(orgs, repos, issues). Salesforce REST. Shopify. Internal company APIs.

**Don't use this for**: file ingest (use
[`../file-ingest-from-s3/`](../file-ingest-from-s3/)), streaming
sources (use Kafka or DLT), or APIs returning all data in one shot
(use the Met pattern directly).

## What this template does

1. Ensures the target UC schema exists
2. Creates a `secret_scope` and stores the Stripe API key
3. Uploads a Spark notebook that fan‑outs across customer fetches,
   respecting Stripe's 100 req/sec rate limit
4. Creates an idempotent job that runs the notebook
5. Runs the job and waits
6. Builds a Silver typed view via SQL warehouse
7. Snapshots the result

The same engine works for any cursor‑paginated REST API. Per‑API,
change only the config block + Silver SQL.

## Customize before running

In `workflow.yaml`:

- Replace `stripe` references with your provider name
- `target_table`: e.g. `analytics.stripe.customers_bronze`
- The Silver SQL: extract the fields your reporting needs

In the notebook content (inlined in `workflow.yaml`):

- `API_BASE`: provider base URL (e.g. `https://api.stripe.com`)
- `LIST_ENDPOINT`: list endpoint path
- `RATE_PER_SEC`: provider's published rate limit
- `build_headers()`: change to your auth scheme
- `parse_list_response()`: change to your list response shape

## Auth setup

For Stripe specifically:

```sh
# In your local Swamp vault
swamp vault set stripe api_key sk_live_xxxxxxxxxxxx  # your secret key
```

The workflow's `secret.put` step reads from your Swamp vault and writes
to a Databricks workspace secret scope named `stripe`. The notebook
reads via `dbutils.secrets.get("stripe", "api_key")` and includes it as
a Bearer token in every request.

For OAuth APIs (Salesforce, Microsoft Graph): the
[`references/api-ingest-patterns.md`](../../../.claude/skills/swamp-databricks-author/references/api-ingest-patterns.md)
file in the skill documents the OAuth client‑credentials refresh pattern.
Replace `build_headers()` with the OAuth version; everything else stays.

## Pagination

Stripe uses cursor pagination: each response has a `data` array and a
`has_more` boolean, and you pass `starting_after=<last_id>` to get the
next page. The list‑fetch loop in the notebook handles this; the
detail‑fetch loop after is the same as Met.

For other paginations (Link header, offset, all‑at‑once), see
[`references/api-ingest-patterns.md`](../../../.claude/skills/swamp-databricks-author/references/api-ingest-patterns.md).

## Standalone notebook source

[`notebook.py`](./notebook.py) on GitHub (the `.py` extension is not
allowed in the Swamp registry archive). Same Python is inlined in
`workflow.yaml`.

## Smoke test before scheduling

Run with `max_records=10`:

```sh
swamp workflow run workflow.yaml --input max_records=10
```

Stripe lets you use test mode (`sk_test_...`) for safe smoke runs; the
template defaults to that.

Production: drop `max_records`, add `schedule:` for incremental refresh.
The current template does a full pull; for production you'd want to
filter `created[gte]=<last_run_time>` in the list endpoint and only
fetch new records.
