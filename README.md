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

Task types validated by Zod in v0.6: `notebook_task`, `sql_task` (with
query / file / dashboard / alert variants), `pipeline_task`,
`spark_python_task`, `spark_jar_task`, `python_wheel_task`, `dbt_task`,
`run_job_task`, `condition_task`, `for_each_task` (recursive). End-to-end
workspace validation covers `notebook_task` and `sql_task` (via the
`workspace_file` model documented below). Others ship as schema-only
because they need compute or dependencies the smoke test environment
does not have.

### `@mfbaig35r/databricks/notebook`

Workspace notebook lifecycle. Resources keyed by absolute path.

| Method   | API call                              |
|----------|---------------------------------------|
| `upload` | `POST /api/2.0/workspace/import`      |
| `read`   | `GET /api/2.0/workspace/export`       |
| `delete` | `POST /api/2.0/workspace/delete`      |

### `@mfbaig35r/databricks/secret_scope` + `@mfbaig35r/databricks/secret`

Workspace secrets (distinct from Swamp vault). Secret values pass through
to the Databricks Secrets API and are never persisted in Swamp's data
layer; pass them via CEL `${{ vault.get(...) }}` if you want them sourced
from a Swamp vault.

| Model + method                | API call                                 |
|-------------------------------|------------------------------------------|
| `secret_scope.create`         | `POST /api/2.0/secrets/scopes/create`    |
| `secret_scope.list`           | `GET /api/2.0/secrets/scopes/list`       |
| `secret_scope.delete`         | `POST /api/2.0/secrets/scopes/delete`    |
| `secret.put`                  | `POST /api/2.0/secrets/put`              |
| `secret.delete`               | `POST /api/2.0/secrets/delete`           |
| `secret.list` (keys only)     | `GET /api/2.0/secrets/list?scope=...`    |

### `@mfbaig35r/databricks/uc_catalog` + `uc_schema` + `uc_table` + `uc_volume`

Unity Catalog object lifecycle. Pair with DLT, jobs, or SQL workloads
that read/write into UC.

| Model + method            | API call                                        |
|---------------------------|-------------------------------------------------|
| `uc_catalog.create`/`read`/`update`/`delete`/`list`/`create_or_update` | `/api/2.1/unity-catalog/catalogs` |
| `uc_schema.create`        | `POST /api/2.1/unity-catalog/schemas`           |
| `uc_schema.read`          | `GET /api/2.1/unity-catalog/schemas/{full_name}`|
| `uc_schema.update`        | `PATCH /api/2.1/unity-catalog/schemas/{full_name}` |
| `uc_schema.delete`        | `DELETE /api/2.1/unity-catalog/schemas/{full_name}` |
| `uc_schema.list`          | `GET /api/2.1/unity-catalog/schemas?catalog_name=...` |
| `uc_table.read`           | `GET /api/2.1/unity-catalog/tables/{full_name}` |
| `uc_table.delete`         | `DELETE /api/2.1/unity-catalog/tables/{full_name}` |
| `uc_table.list`           | `GET /api/2.1/unity-catalog/tables?...`         |
| `uc_volume.create`        | `POST /api/2.1/unity-catalog/volumes`           |
| `uc_volume.read`/`update`/`delete`/`list` | the rest of the volumes surface |

`uc_table` does NOT create tables (no such API endpoint). Use
`sql_warehouse.run_query` or a job notebook task with
`CREATE TABLE` SQL, then `uc_table.read` captures the table snapshot.

### `@mfbaig35r/databricks/query`

DBSQL saved queries. The `query_id` returned here is the same identifier
the `job` model's `sql_task.query.query_id` field references.

| Method   | API call                                |
|----------|-----------------------------------------|
| `create` | `POST /api/2.0/sql/queries`             |
| `read`   | `GET /api/2.0/sql/queries/{query_id}`   |
| `update` | `POST /api/2.0/sql/queries/{query_id}`  |
| `delete` | `DELETE /api/2.0/sql/queries/{query_id}` |
| `list`   | `GET /api/2.0/sql/queries`              |

### `@mfbaig35r/databricks/repo`

Databricks Git Repos. Real jobs typically reference notebooks via repo
paths (`notebook_task.notebook_path: /Repos/me/my-project/etl`) rather
than uploading to `/Shared/`.

| Method   | API call                                              |
|----------|-------------------------------------------------------|
| `create` | `POST /api/2.0/repos`                                 |
| `read`   | `GET /api/2.0/repos/{repo_id}`                        |
| `update` | `PATCH /api/2.0/repos/{repo_id}` (switch branch/tag)  |
| `pull`   | `PATCH /api/2.0/repos/{repo_id}` (re-sends current branch) |
| `delete` | `DELETE /api/2.0/repos/{repo_id}`                     |
| `list`   | `GET /api/2.0/repos`                                  |

Private repos need a workspace‑level Git credential configured (Settings
→ User Settings → Linked Accounts). Public repos work without setup.

### `@mfbaig35r/databricks/workspace_permissions` + `uc_permissions`

Workspace and Unity Catalog ACLs as Swamp models. Workspace permissions
use a full‑replace (`set`) and additive (`update`) PATCH model on
`(object_type, object_id)` pairs. UC permissions use a changes‑style
PATCH (add/remove privileges per principal) on
`(securable_type, full_name)` pairs.

| Model + method                       | API call                                                  |
|--------------------------------------|-----------------------------------------------------------|
| `workspace_permissions.get`          | `GET /api/2.0/permissions/{type}/{id}`                    |
| `workspace_permissions.set`          | `PUT /api/2.0/permissions/{type}/{id}` (full replace ACL) |
| `workspace_permissions.update`       | `PATCH /api/2.0/permissions/{type}/{id}` (additive)       |
| `workspace_permissions.list_levels`  | `GET /api/2.0/permissions/{type}/{id}/permissionLevels`   |
| `uc_permissions.get`                 | `GET /api/2.1/unity-catalog/permissions/{type}/{full_name}` |
| `uc_permissions.get_effective`       | `GET /api/2.1/unity-catalog/effective-permissions/{type}/{full_name}` (direct + inherited) |
| `uc_permissions.update`              | `PATCH /api/2.1/unity-catalog/permissions/{type}/{full_name}` |

Workspace `object_type` enum: `jobs`, `pipelines`, `sql/warehouses`,
`clusters`, `cluster-policies`, `instance-pools`, `notebooks`,
`directories`, `serving-endpoints`, `experiments`, `registered-models`,
`tokens`, `passwords`, `repos`, `dashboards`, `queries`, `alerts`,
`genie`, `dbsql-dashboards`, `apps`, `vector-search-endpoints`.

UC `securable_type` enum: `catalog`, `schema`, `table`, `volume`,
`function`, `external_location`, `storage_credential`, `metastore`,
`connection`, `provider`, `share`, `recipient`, `clean_room`, `model`,
`service_credential`.

`set` and `update` write a `permissions` (or `uc_permissions`) resource
recording what was last applied. `get` does not.

### Idempotent `create_or_update`

Most resource-managing models expose a `create_or_update` method
alongside `create` and `update`. The semantics: if a Swamp resource
keyed by `args.name` exists, take the update path; otherwise create.
Available on: `job`, `dlt_pipeline`, `sql_warehouse`, `secret_scope`,
`uc_catalog`, `uc_schema`, `uc_volume`.

As of v0.11, `create_or_update` reconciles against the **workspace**,
not Swamp's data layer. Each method does a workspace GET (or list, for
secret scopes) before deciding which path to take, so delete‑then‑
`create_or_update` correctly takes the create path. Also handles out‑
of‑band workspace deletes (UI delete → Swamp `create_or_update`
correctly recreates).

### `@mfbaig35r/databricks/workspace_file`

Workspace file (FILE object type) lifecycle. Use this when a downstream
task references a plain source file at a workspace path: `sql_task.file`,
`spark_python_task.python_file`, dbt project files, etc. Distinct from
the `notebook` model, which manages NOTEBOOK objects.

| Method   | API call                                  |
|----------|-------------------------------------------|
| `upload` | `POST /api/2.0/workspace/import` (`format: AUTO`, no language) |
| `read`   | `GET /api/2.0/workspace/export` (`format: AUTO`) |
| `delete` | `POST /api/2.0/workspace/delete`          |

Upload also calls `/api/2.0/workspace/get-status` and records the
resulting `object_type` on the resource. In modern Databricks workspaces
this is `FILE`. Older workspaces may produce a `NOTEBOOK` for certain
content; the resource reflects what actually got created.

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
