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

Task types validated by Zod in v0.5: `notebook_task`, `sql_task` (with
query / file / dashboard / alert variants), `pipeline_task`,
`spark_python_task`, `spark_jar_task`, `python_wheel_task`, `dbt_task`,
`run_job_task`, `condition_task`, `for_each_task` (recursive). End-to-end
workspace validation covers `notebook_task`; others ship as schema-only
because they need compute or dependencies the smoke test environment
does not have.

### `@mfbaig35r/databricks/notebook`

Workspace notebook lifecycle. Resources keyed by absolute path.

| Method   | API call                              |
|----------|---------------------------------------|
| `upload` | `POST /api/2.0/workspace/import`      |
| `read`   | `GET /api/2.0/workspace/export`       |
| `delete` | `POST /api/2.0/workspace/delete`      |

### `@mfbaig35r/databricks/sql_warehouse`

SQL Warehouse lifecycle plus statement execution via the SQL Statement Execution API.

| Method             | API call                                              |
|--------------------|-------------------------------------------------------|
| `create`           | `POST /api/2.0/sql/warehouses`                        |
| `adopt`            | `GET /api/2.0/sql/warehouses/{id}` (register existing) |
| `read`             | `GET /api/2.0/sql/warehouses/{id}`                    |
| `update`           | `POST /api/2.0/sql/warehouses/{id}/edit`              |
| `delete`           | `DELETE /api/2.0/sql/warehouses/{id}`                 |
| `start`            | `POST /api/2.0/sql/warehouses/{id}/start`             |
| `stop`             | `POST /api/2.0/sql/warehouses/{id}/stop`              |
| `run_query`        | `POST /api/2.0/sql/statements` (sync up to 50s)       |
| `wait_statement`   | polls `GET /api/2.0/sql/statements/{id}`              |
| `cancel_statement` | `POST /api/2.0/sql/statements/{id}/cancel`            |

On Databricks Free Edition, warehouse quotas are small (often 1-2). Use
`adopt` to register the auto-provisioned Starter Warehouse instead of
creating new ones. `enable_serverless_compute: true` is the default (the
only option on Free).

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

On Databricks Free Edition, set `serverless: true` in the `create` args. When
`serverless: true`, `catalog` is required (the Databricks API rejects without
it); on Free, the default UC catalog is `workspace`.

**Heads up on cleanup:** `DELETE /api/2.0/pipelines/{id}` removes the pipeline
definition but does NOT drop the Delta tables the pipeline materialized in its
target schema. Once a table is written, it is an independent UC object. To
fully clean up after deleting a pipeline, run
`DROP TABLE <catalog>.<target>.<name>` in a SQL warehouse or notebook. A future
`@mfbaig35r/databricks/sql_warehouse` model will make this a Swamp-native step.

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
