# Agent templates

Starting points the `swamp-databricks-author` skill points at when
generating a new workflow. Each template is a fork‑and‑adapt
implementation of a canonical pattern.

## When to use these

If you're authoring with Claude Code and the skill activated, it'll
pick the right template and adapt it for your specific case. If you're
hand‑authoring (no agent), pick the template that matches your
pattern, copy the directory, and customize the config block + Silver
SQL.

## The four canonical patterns

| Template | Use when |
|---|---|
| [`file-ingest-from-s3/`](./file-ingest-from-s3/) | A vendor drops files in S3 and you want them queryable as a UC table |
| [`api-ingest-authenticated/`](./api-ingest-authenticated/) | A SaaS API needs auth + pagination; you want the response data in a UC table |
| [`dbt-run/`](./dbt-run/) | You have a dbt project in Git and want it scheduled on a Databricks SQL warehouse |
| [`ml-training/`](./ml-training/) | You have training data in UC and want a model trained on a cadence, with MLflow logging |

## What's universal across templates

Every template follows the same shape:

1. **Auth setup** (secret_scope + secret.put steps) where credentials
   are needed
2. **Resource pre‑creation** (uc_schema.create_or_update, repo
   attachment, etc.) so the workflow is idempotent across reruns
3. **Notebook upload** (or repo pull) for the imperative core
4. **Job define + run + wait** for the actual work
5. **Optional Silver build** via sql_warehouse.run_query for typed
   extraction from Bronze
6. **Optional snapshot** via uc_table.read so downstream Swamp steps
   can chain off the result

Every template includes a `max_*` or `MAX_*` input for smoke testing
with a bounded workload before going to production.

## What changes per pattern

| Variable | Where it lives |
|---|---|
| Source (S3 path / API URL / Git repo / UC table) | Workflow inputs |
| Auth (none / API key / OAuth / workspace identity) | secret_scope + secret.put steps; build_headers() in the notebook |
| Pagination (none / cursor / Link / offset / streaming) | Notebook's list‑fetch loop |
| Target table shape | Silver SQL |
| Schedule | The `schedule:` field on the workflow |

For the engine pieces (rate limiting via mapInPandas, retries on
429/5xx only, raw‑JSON Bronze, error capture columns), the patterns
are reusable as‑is. See
[`../api-ingest/README.md`](../api-ingest/README.md) for the universal
ingest pattern documentation.

## Generating with the skill

If you're working in Claude Code with the
`@mfbaig35r/databricks` pack installed, prompts like these activate
the `swamp-databricks-author` skill:

- "create a workflow that pulls Stripe customers into a UC table" →
  forks `api-ingest-authenticated/`
- "build a daily dbt run from this Git repo" → forks `dbt-run/`
- "schedule a weekly training job on this UC table" → forks
  `ml-training/`
- "ingest CSVs from S3 into a Bronze table" → forks
  `file-ingest-from-s3/`

The skill asks the necessary questions (auth, schedule, target),
generates a customized workflow.yaml and notebook, and outputs a
review checklist before you commit.

## Standalone notebook source files

Each template ships a `notebook.py` for clean review and editing. Per
the Swamp registry's safety analyzer (`.py` not in the file‑extension
allowlist), those `.py` files don't ship in the pulled archive. They
live on GitHub at the link in each template's README. The same Python
is inlined in `workflow.yaml` under `notebook.upload`'s `content:`
field, so the workflow runs directly from the pulled archive without
the standalone file.
