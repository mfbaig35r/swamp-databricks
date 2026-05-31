# Template: scheduled dbt run

Pull a dbt project from a Git repo, run it on a SQL warehouse, on a
schedule. No custom notebook required; the `dbt_task` task type in the
job model handles the full dbt CLI surface.

**Use when**: you have a dbt project in Git and want it scheduled on a
Databricks SQL warehouse, with the repo attachment and pull lifecycle
managed by Swamp.

**Don't use this for**: ad‑hoc dbt runs (use the dbt CLI locally) or
dbt projects targeting non‑Databricks warehouses (use dbt Cloud or
your own orchestrator).

## What this template does

1. Attaches the dbt project Git repo to the workspace (idempotent)
2. Pulls the latest commit on the configured branch before every run
3. Ensures the SQL warehouse is registered as a Swamp resource
4. Defines an idempotent job with a `dbt_task` task type running
   `dbt deps && dbt run && dbt test`
5. Triggers and waits for terminal state

## Customize before running

In `workflow.yaml`:

- `inputs.repo_url`: your dbt project Git URL
- `inputs.warehouse_id`: workspace warehouse ID (use `sql_warehouse.adopt`
  to register it first)
- `inputs.catalog` / `inputs.target_schema`: where dbt writes
- The `commands:` list: your actual dbt command chain
- `schedule:` field: cron expression for production

## Auth setup

The dbt CLI authenticates to Databricks through the SQL warehouse's
standard workspace auth. No separate dbt credentials needed.

If your dbt project's Git repo is private, you need a workspace‑level
Git credential configured (Settings → User Settings → Linked
Accounts). Public repos work without setup.

If your dbt project needs source‑system credentials (e.g., a separate
data source Postgres connection), set them up via `secret_scope` +
`secret.put` steps and reference them in dbt's `profiles.yml` or as
job task environment variables.

## Smoke test before scheduling

Tag a single fast model in your dbt project with `smoke`, then run:

```sh
swamp workflow run workflow.yaml --input commands_mode=smoke
```

The template's `commands_mode=smoke` option swaps the production
`dbt run` for `dbt run --select tag:smoke`, exercising the full chain
(repo pull, dbt deps, warehouse connection, UC write) in seconds
instead of the full project's minutes.

Once smoke passes, drop `commands_mode` (or set to `production`) and
add `schedule:` for production.

## Common failure modes

See [`references/dbt-task.md`](../../../.claude/skills/swamp-databricks-author/references/dbt-task.md)
in the skill for the full list. The frequent ones:

1. `profiles.yml` not found → set `profiles_directory` explicitly
2. Warehouse is stopped → add a `sql_warehouse.start` step before the job
3. Target schema doesn't exist → add a `uc_schema.create_or_update` step
4. Repo not pulled → keep the `repo.pull` step in the workflow
