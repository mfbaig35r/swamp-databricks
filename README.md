# @mfbaig35r/databricks

Databricks Jobs, DLT pipelines, and Unity Catalog resources as Swamp models.
Compose Databricks pipelines with non-Databricks resources (S3 datastores,
Postgres tables, secrets vaults, Cloudflare modules) in a single Swamp workflow.

This first release ships the `@mfbaig35r/databricks/job` model, covering the
Databricks Jobs API 2.2 surface: create, read, update (full reset), delete,
trigger a run, wait for terminal state, and cancel a run.

## Why this exists

Databricks already has two ways to manage jobs: the Terraform provider for
infra lifecycle, and Databricks Asset Bundles (DAB) for in-Databricks pipeline
definition. Neither composes with non-Databricks resources in a single
declarative graph. That is the gap this extension fills inside Swamp's
automation framework.

## Install

```sh
swamp extension pull @mfbaig35r/databricks
```

Then reference it from a workflow or model in your Swamp repo. Configure the
target workspace via `globalArguments`:

```yaml
globalArguments:
  workspace_url: "https://adb-xxx.azuredatabricks.net"
  auth:
    kind: pat
    token_secret: databricks/pat
```

The `token_secret` value is a vault key, not the raw token. Store the actual
PAT in your Swamp vault first:

```sh
swamp vault set databricks/pat
```

## Methods

| Method       | API call                              | Notes |
|--------------|---------------------------------------|-------|
| `create`     | `POST /api/2.2/jobs/create`           | Writes a `job` resource keyed by the user-supplied name. |
| `read`       | `GET /api/2.2/jobs/get`               | Live read; does not mutate resources. |
| `update`     | `POST /api/2.2/jobs/reset`            | Full replace. Partial patch is intentionally out of scope. |
| `delete`     | `POST /api/2.2/jobs/delete`           | Removes the job from the workspace. |
| `run`        | `POST /api/2.2/jobs/run-now`          | Fire and forget; writes a `last_run` resource. |
| `wait_run`   | `GET /api/2.2/jobs/runs/get` (poll)   | Polls until terminal state (TERMINATED, SKIPPED, INTERNAL_ERROR). |
| `cancel_run` | `POST /api/2.2/jobs/runs/cancel`      | Cancels an in-flight run by `run_id`. |

## Example workflow

```yaml
name: nightly-ingest
steps:
  - id: define_job
    model: "@mfbaig35r/databricks/job"
    method: create
    arguments:
      name: nightly-ingest
      tasks:
        - task_key: ingest
          notebook_task:
            notebook_path: /Repos/team/etl/ingest
          job_cluster_key: small
      job_clusters:
        - job_cluster_key: small
          new_cluster:
            spark_version: 15.4.x-scala2.12
            node_type_id: i3.xlarge
            num_workers: 2
  - id: trigger
    model: "@mfbaig35r/databricks/job"
    method: run
    arguments:
      job_ref: nightly-ingest
  - id: wait
    model: "@mfbaig35r/databricks/job"
    method: wait_run
    arguments:
      run_id: ${{ steps.trigger.outputs.run_id }}
```

## Task type coverage in v1

Validated via Zod discriminated union:

- `notebook_task`
- `sql_task`
- `pipeline_task` (DLT)

Other Databricks task types (`spark_python_task`, `python_wheel_task`,
`dbt_task`, `run_job_task`, `for_each_task`, `condition_task`,
`spark_jar_task`) are not yet schema-validated. They will be added in
subsequent releases as their schemas stabilize.

## Auth strategies

- `pat`: personal access token resolved from a vault key.
- `oauth_m2m`: client credentials grant via `/oidc/v1/token`. Client secret
  resolved from a vault key.
- `azure_msi`: stubbed; not yet implemented.

## Roadmap

- `@mfbaig35r/databricks/dlt_pipeline`: Delta Live Tables pipelines as a first-class
  model.
- `@mfbaig35r/databricks/uc_schema`, `uc_table`, `uc_volume`: Unity Catalog
  resources.
- `@mfbaig35r/databricks/secret_scope`, `secret`: Secrets API.
- `@mfbaig35r/databricks/sql_warehouse`: SQL Warehouses for DBSQL tasks.
- Expand task discriminated union to the full Jobs API surface.

## License

Apache-2.0. See `LICENSE`.
