# Running dbt projects via swamp-databricks

The `job` model supports `dbt_task` as a first-class task type. Pair it
with the `repo` model to manage the dbt project's source.

## Workflow shape

```yaml
name: dbt-nightly
schedule: "0 3 * * *"

steps:
  # 1. Ensure the dbt project repo is attached to the workspace
  - id: project
    model: "@mfbaig35r/databricks/repo"
    method: create_or_update
    arguments:
      name: dbt-analytics
      url: https://github.com/myco/dbt-analytics
      provider: gitHub
      path: /Repos/dbt/analytics
      branch: main

  # 2. Pull latest before running
  - id: pull
    model: "@mfbaig35r/databricks/repo"
    method: pull
    arguments:
      repo_ref: dbt-analytics

  # 3. Define the dbt job (idempotent across reruns)
  - id: job
    model: "@mfbaig35r/databricks/job"
    method: create_or_update
    arguments:
      name: dbt-nightly
      tasks:
        - task_key: deps_run_test
          dbt_task:
            commands:
              - "dbt deps"
              - "dbt run --target prod"
              - "dbt test --target prod"
            warehouse_id: ${{ inputs.warehouse_id }}
            project_directory: /Repos/dbt/analytics
            catalog: analytics
            schema: prod
            source: WORKSPACE

  # 4. Trigger and wait
  - id: trigger
    model: "@mfbaig35r/databricks/job"
    method: run
    arguments:
      job_ref: dbt-nightly

  - id: wait
    model: "@mfbaig35r/databricks/job"
    method: wait_run
    arguments:
      run_id: ${{ steps.trigger.outputs.run_id }}
      timeout_seconds: 3600
```

## dbt_task schema fields

| Field | Type | Notes |
|---|---|---|
| `commands` | string[] | 1-10 dbt CLI commands run sequentially. First failure stops the chain. |
| `warehouse_id` | string | SQL warehouse the dbt CLI connects through. Required. |
| `project_directory` | string | Optional. Workspace path to the dbt project. Defaults to repo root. |
| `profiles_directory` | string | Optional. Where to find `profiles.yml`. Defaults to standard locations. |
| `schema` | string | Optional. Override the schema dbt writes to. |
| `catalog` | string | Optional. Override the UC catalog. |
| `source` | enum | `WORKSPACE` or `GIT`. Use `WORKSPACE` when the project is in `/Repos/`. |

## Common command patterns

| Use case | Commands |
|---|---|
| Standard build | `dbt deps`, `dbt run`, `dbt test` |
| Full refresh | `dbt deps`, `dbt run --full-refresh`, `dbt test` |
| Specific selector | `dbt deps`, `dbt run --select tag:nightly`, `dbt test --select tag:nightly` |
| Snapshots only | `dbt deps`, `dbt snapshot` |
| Build artifacts for downstream | `dbt deps`, `dbt run`, `dbt test`, `dbt docs generate` |

## Auth model

The dbt CLI authenticates to Databricks via the SQL warehouse's standard
auth. No separate dbt credentials needed in the workflow. The user running
the job needs `CAN_USE` on the warehouse and write access to the target
catalog/schema.

If the dbt project needs source-system credentials (e.g., for `dbt-snowflake`
in a multi-warehouse setup), store them in a Databricks secret scope and
reference via `dbt_task` environment variables (Databricks runtime exposes
them). Set them up with the `secret_scope` and `secret` models in steps
before the `job.create_or_update`.

## Common failure modes

1. **`profiles.yml` not found.** Set `profiles_directory` explicitly or
   include a `profiles.yml` at the dbt project root that references
   `target: databricks`.
2. **`warehouse_id` is stopped.** dbt task does not auto-start warehouses.
   Add a `sql_warehouse.start` step before the dbt job, or set the
   warehouse's `auto_stop_mins` low and `min_num_clusters: 1` so it
   warms up on demand.
3. **Schema does not exist.** dbt errors trying to write to a non-existent
   schema. Add a `uc_schema.create_or_update` step before the dbt job.
4. **Repo not pulled.** If you skip the `repo.pull` step, dbt runs against
   stale code. Always pull before run unless deliberately pinning a
   revision via the `tag` field on `repo.update`.

## Smoke test pattern

For a dbt project's first scheduled run, validate with a single model:

```yaml
- id: job
  model: "@mfbaig35r/databricks/job"
  method: create_or_update
  arguments:
    name: dbt-smoke
    tasks:
      - task_key: smoke
        dbt_task:
          commands:
            - "dbt deps"
            - "dbt run --select tag:smoke"
          warehouse_id: ${{ inputs.warehouse_id }}
          project_directory: /Repos/dbt/analytics
```

Tag a single, fast model with `smoke` in the dbt project. The smoke run
exercises the full chain (repo pull, dbt deps, warehouse connection, UC
write) with a 30-second model instead of a 30-minute one.
