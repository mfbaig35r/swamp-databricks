# @mfbaig35r/databricks

Databricks Jobs, DLT pipelines, and workspace notebooks as Swamp models.
Compose Databricks pipelines with non-Databricks resources (S3 datastores,
Postgres tables, secrets vaults, Cloudflare modules) in a single Swamp workflow.

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

Store a Databricks PAT in a Swamp vault:

```sh
swamp vault create local_encryption databricks
swamp vault put databricks pat
```

Reference the vault from each model's `globalArguments`:

```yaml
globalArguments:
  workspace_url: "https://dbc-xxxx.cloud.databricks.com"
  auth_kind: pat
  token: ${{ vault.get("databricks", "pat") }}
```

## Models

### `@mfbaig35r/databricks/job`

Databricks Jobs API 2.2 lifecycle.

| Method       | API call                              |
|--------------|---------------------------------------|
| `create`     | `POST /api/2.2/jobs/create`           |
| `read`       | `GET /api/2.2/jobs/get`               |
| `update`     | `POST /api/2.2/jobs/reset`            |
| `delete`     | `POST /api/2.2/jobs/delete`           |
| `run`        | `POST /api/2.2/jobs/run-now`          |
| `wait_run`   | polls `GET /api/2.2/jobs/runs/get`    |
| `cancel_run` | `POST /api/2.2/jobs/runs/cancel`      |

Task types validated by Zod in v0.2: `notebook_task`, `sql_task`, `pipeline_task`.

### `@mfbaig35r/databricks/notebook`

Workspace notebook lifecycle. Resources keyed by absolute path.

| Method   | API call                              |
|----------|---------------------------------------|
| `upload` | `POST /api/2.0/workspace/import`      |
| `read`   | `GET /api/2.0/workspace/export`       |
| `delete` | `POST /api/2.0/workspace/delete`      |

### `@mfbaig35r/databricks/dlt_pipeline`

Delta Live Tables pipeline lifecycle. DLT calls runs "updates".

| Method         | API call                                          |
|----------------|---------------------------------------------------|
| `create`       | `POST /api/2.0/pipelines`                         |
| `read`         | `GET /api/2.0/pipelines/{id}`                     |
| `update`       | `PUT /api/2.0/pipelines/{id}`                     |
| `delete`       | `DELETE /api/2.0/pipelines/{id}`                  |
| `start_update` | `POST /api/2.0/pipelines/{id}/updates`            |
| `wait_update`  | polls `GET /api/2.0/pipelines/{id}/updates/{uid}` |
| `stop`         | `POST /api/2.0/pipelines/{id}/stop`               |

On Databricks Free Edition, set `serverless: true` in the `create` args. The
DLT model is shipped as preview in v0.2; end-to-end validation against a Free
workspace is pending.

## Example workflow

```yaml
name: nightly-ingest
steps:
  - id: upload_etl_nb
    model: "@mfbaig35r/databricks/notebook"
    method: upload
    arguments:
      path: /Shared/etl/ingest
      language: PYTHON
      overwrite: true
      content: |
        # Databricks notebook source
        spark.read.format("csv").load("s3://my-bucket/data/").write.saveAsTable("bronze.events")

  - id: define_job
    model: "@mfbaig35r/databricks/job"
    method: create
    arguments:
      name: nightly-ingest
      tasks:
        - task_key: ingest
          notebook_task:
            notebook_path: /Shared/etl/ingest

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

## Auth strategies

- `pat` (default): personal access token, resolved via CEL `vault.get("...", "...")`.
- `oauth_m2m`: client credentials grant via `/oidc/v1/token`. Client secret
  resolved via CEL.
- `azure_msi`: stubbed; not yet implemented.

## Roadmap

- Expand Job task-type discriminated union (`spark_python_task`,
  `python_wheel_task`, `dbt_task`, `run_job_task`, `for_each_task`,
  `condition_task`, `spark_jar_task`).
- Unity Catalog: `@mfbaig35r/databricks/uc_schema`, `uc_table`, `uc_volume`,
  with grants.
- Secrets: `@mfbaig35r/databricks/secret_scope`, `secret`.
- SQL Warehouses: `@mfbaig35r/databricks/sql_warehouse`.
- Azure MSI auth.

## License

Apache-2.0. See `LICENSE.txt`.
