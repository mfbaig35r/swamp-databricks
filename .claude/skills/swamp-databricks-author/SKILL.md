---
name: swamp-databricks-author
description: Generate a runnable Databricks pipeline as a Swamp workflow plus an agent-written notebook. Triggers on "create a Databricks notebook", "build a Swamp workflow that ingests", "add a Databricks pipeline", "pull <data> into a Databricks table", "ingest <source> into UC", "write a pipeline that pulls <api> into Databricks", "ingest <files> into Delta", "schedule a Databricks job for". Use when the user wants to author a new Databricks data pipeline that ships as a swamp-databricks workflow plus a committed notebook. Covers file ingest, API ingest, dbt runs, ML training, and custom transforms. Always produces a notebook the user can review before committing.
---

# Authoring a Databricks pipeline with swamp-databricks

You are generating two things together: a Databricks notebook (Python or SQL)
and a Swamp workflow YAML that uploads, runs, and orchestrates it. The user
will review and commit both before scheduling.

This skill targets the `@mfbaig35r/databricks` extension. Confirm the pack is
installed (`swamp extension list | grep databricks`) before generating; if
absent, instruct the user to run
`swamp extension pull @mfbaig35r/databricks` first.

## Activation

Triggers when the user says any of:

- "create a Databricks notebook that..."
- "build a Swamp workflow that ingests..."
- "add a Databricks pipeline..."
- "pull <source> into a Databricks table"
- "ingest <files|api|db> into UC"
- "schedule a Databricks job for..."
- "write a pipeline that pulls <api> into Databricks"

Do NOT activate for: pure Databricks SQL ad-hoc queries (use the SQL editor),
infrastructure-only changes (use the Terraform provider or the existing
swamp-databricks `uc_*` models directly), or Databricks Asset Bundles
authoring (different toolchain).

## Required information to gather before writing code

Always ask, do not assume:

1. **Source.** Where does the data come from? File on S3/Azure/GCS? HTTP API
   (public or authenticated)? Existing UC table? Streaming source?
2. **Target.** Catalog, schema, table name. Bronze + Silver, or single table?
3. **Auth.** None (public API), Databricks workspace secret (use
   `secret_scope` + `secret` models), or workspace identity?
4. **Schedule.** Manual run only, daily, hourly, on-event? Affects whether the
   workflow needs a `schedule:` field and what value.
5. **Scale.** Rough record count. Affects partition count, rate limit math,
   and whether to write to UC Volume staging for restart safety.
6. **Downstream consumers.** Who reads the resulting table? Drives whether to
   add a `uc_permissions.update` step and/or a notification step.

If the user can't answer auth or schedule yet, generate a sensible default
(no auth, manual trigger) and flag it clearly so they fill in later.

## Patterns this skill knows

Reference the templates under
[`examples/agent-templates/`](../../../examples/agent-templates/). Fork the
closest one, adapt the config block + Silver SQL; do not regenerate the
universal engine.

### File ingest (S3/Azure/GCS → UC table)

Template: [`examples/agent-templates/file-ingest-from-s3/`](../../../examples/agent-templates/file-ingest-from-s3/).
Simple Spark `inferSchema` for the default case. For production schema
detection and type inference, replace the notebook body with an
[AGI](https://github.com/mfbaig/artificial-general-ingestion)
`ingest_to_evidence` MCP call. Auth via secret_scope + secret.put when
reading from S3; remove those steps if the workspace uses an instance
profile or UC external location.

### API ingest, public (no auth)

Reference: [`examples/api-ingest/met-museum/`](../../../examples/api-ingest/met-museum/).
Met Museum API as the worked example: public, all-IDs-at-once list
endpoint, ~80 req/sec rate limit. Universal engine in
[`examples/api-ingest/README.md`](../../../examples/api-ingest/README.md).

### API ingest, authenticated + paginated

Template: [`examples/agent-templates/api-ingest-authenticated/`](../../../examples/agent-templates/api-ingest-authenticated/).
Stripe customers as the worked example: Bearer auth + cursor pagination
+ secret_scope. Per-provider, change `API_BASE`, `LIST_ENDPOINT`,
`RATE_PER_SEC`, `build_headers()`, `parse_list_response()`, and the
Silver SQL. Read
[references/api-ingest-patterns.md](references/api-ingest-patterns.md)
for cursor, Link header, offset, and OAuth refresh patterns.

### dbt run

Template: [`examples/agent-templates/dbt-run/`](../../../examples/agent-templates/dbt-run/).
Uses the `repo` model to attach the dbt project + the `job` model with
`dbt_task`. No notebook. Read
[references/dbt-task.md](references/dbt-task.md) for the task schema
and common failure modes.

### ML training

Template: [`examples/agent-templates/ml-training/`](../../../examples/agent-templates/ml-training/).
Reads training data from a UC table, trains with scikit-learn
(placeholder; swap for your model), logs to MLflow via `autolog`.
Optionally writes predictions to a UC table.

As of v0.18, the template uses explicit Swamp steps for the MLflow
lifecycle around the training:

- `mlflow_experiment.create_or_update` ensures the experiment exists
  before the notebook calls `mlflow.start_run()`.
- `registered_model.create_or_update` + `model_version.create` (both
  commented out by default) register the trained model to UC Model
  Registry once the notebook surfaces a `run_id` via
  `dbutils.notebook.exit`.
- `model_version.update_alias` (commented out) sets `production` /
  `staging` / `champion` aliases for stage transitions.
- `model_serving_endpoint.create` (commented out, **paid Databricks
  only**) deploys a version to a real-time endpoint.

The training itself still lives in the notebook (mlflow.autolog
captures params/metrics/model). Swamp manages everything around it.

### Custom transform / orchestration

For anything not covered above, write the notebook for the specific
case. Compose with the standard workflow shape: ensure schema, upload
notebook, create_or_update job, run, wait_run, optionally grant and
notify. The templates above show the standard shape; adapt the notebook
body.

## What to generate

Produce, in order:

1. **The notebook file** at a path the user picks (default
   `examples/<task-name>/notebook.py` in the user's repo, or directly into
   their dbt project). Always begins with a clear config block at the top.
   Always uses `mapInPandas` not `rdd.mapPartitions` for fan-out (Databricks
   serverless rejects RDD operations).
2. **The workflow YAML** at `examples/<task-name>/workflow.yaml` wiring
   together: schema setup, notebook upload (or `repo` if from Git), job
   create_or_update, run, wait_run, plus any required `secret_scope`/`secret`,
   `uc_permissions.update`, and downstream notification steps.
3. **A smoke test plan** as a comment block in the workflow YAML's header,
   describing how to validate end-to-end with a bounded input (e.g.,
   `MAX_OBJECTS=10` or a single file).
4. **Required setup notes** in a brief markdown summary explaining: what
   prerequisites need to be installed (`@mfbaig35r/databricks`, AGI if used),
   which secrets need to be in the Swamp vault before running, and what to
   review before committing.

## Non-negotiable disciplines

These are not optional. Generated workflows that skip them will fail in
production.

1. **Smoke test with a bounded input before scheduling.** Always include a
   `max_*` or `limit` parameter the user can set to 10 or 100 for the first
   run. The Met example uses `MAX_OBJECTS`. The first end-to-end run should
   be small enough to complete in under 5 minutes.
2. **Review the generated notebook before committing.** Output an explicit
   "REVIEW CHECKLIST" comment block with the items the user should verify:
   schema columns, secret references, error handling, rate limit, output
   table name. Do not encourage `git commit -A`-style staging.
3. **Use `create_or_update`, not `create`, for resource-defining steps.**
   Workflows should be idempotent across reruns. `job.create` errors on the
   second run; `job.create_or_update` reconciles. Same for `uc_schema`,
   `uc_catalog`, `uc_volume`, `secret_scope`, `dlt_pipeline`, `sql_warehouse`.
4. **Capture errors in Bronze.** For external sources, write an
   `http_status` and `error_message` column alongside the raw payload so
   failed rows are recoverable, not dropped silently.
5. **Set `wait_run` timeouts realistically.** Default 7200 (2 hours) is
   right for most ingests. Long-running jobs (full Met backfill, multi-hour
   ML training) need higher. Short test runs benefit from 600 (10 min) so
   failed smoke tests fail fast.

## Anti-patterns to avoid

- **Regenerating the notebook on every workflow run.** The notebook lives
  in a Git repo (committed file) and the workflow uploads from there or via
  `repo.pull`. The agent generates v1, the human reviews and commits, then
  the workflow runs the committed version. Do not suggest fetching code
  from a prompt at runtime.
- **Inlining secrets in the workflow YAML.** Always use `${{ vault.get(...) }}`
  CEL expressions for credentials. Workflow YAMLs end up in version control;
  raw secrets do not belong there.
- **Skipping the `uc_schema.create_or_update` step.** Even if the catalog
  is the default `workspace`, schemas are not auto-created. The workflow
  must ensure the target schema exists before the notebook writes to it.
- **Using `rdd.mapPartitions` for HTTP fan-out.** Databricks serverless
  rejects this with `[NOT_IMPLEMENTED]`. Use `mapInPandas` with the same
  partition+token-bucket pattern documented in
  `examples/api-ingest/README.md`.
- **Hand-writing CSV/Excel schema inference.** Use AGI's
  `ingest_to_evidence` MCP call if available; do not regenerate that
  algorithm in every notebook.

## After generation

1. Tell the user to run the smoke test (bounded input) before committing.
2. Tell the user to review the notebook's REVIEW CHECKLIST block.
3. Once smoke test passes and review is complete, the workflow can be
   scheduled by adding the `schedule:` field with a cron expression.

## When to delegate to other skills

- If the user wants to publish their workflow as a reusable Swamp extension,
  delegate to `swamp-extension-publish`.
- If the user wants to add a Databricks resource type that this pack does
  not yet cover (e.g., MLflow registered model, cluster policy), delegate
  to `swamp-extension-model` to author a new model first, then return here
  to wire it into a workflow.
- If the user wants to author a workflow that uses multiple extensions
  (Databricks + S3 + Postgres + Slack), this skill handles the Databricks
  steps; the user composes the rest from their installed extensions.
