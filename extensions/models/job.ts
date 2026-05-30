import { z } from "npm:zod@4";
import {
  dbxFetch,
  GlobalArgs,
  GlobalArgsSchema,
  Logger,
  ReadResource,
  sha256,
  WriteResource,
} from "./_lib/databricks.ts";

// ---------------------------------------------------------------------------
// Task schemas (v0.5: full Jobs API 2.2 task-body coverage minus
// spark_submit_task and clean_rooms_notebook_task).
// Schema accepted by Databricks for all 10 types; end-to-end smoke validation
// in this repo covers notebook_task only. Workflows using other task types
// will validate against the Zod schema before reaching the API.
// ---------------------------------------------------------------------------

const NotebookTask = z.object({
  notebook_path: z.string(),
  source: z.enum(["WORKSPACE", "GIT"]).optional(),
  base_parameters: z.record(z.string(), z.string()).optional(),
  warehouse_id: z.string().optional(),
});

const SqlQueryTask = z.object({
  warehouse_id: z.string(),
  query: z.object({ query_id: z.string() }).optional(),
  file: z.object({
    path: z.string(),
    source: z.enum(["WORKSPACE", "GIT"]).optional(),
  }).optional(),
  dashboard: z.object({
    dashboard_id: z.string(),
    subscriptions: z.array(z.object({
      user_name: z.string().optional(),
      destination_id: z.string().optional(),
    })).optional(),
    custom_subject: z.string().optional(),
    pause_subscriptions: z.boolean().optional(),
  }).optional(),
  alert: z.object({
    alert_id: z.string(),
    subscriptions: z.array(z.object({
      user_name: z.string().optional(),
      destination_id: z.string().optional(),
    })).optional(),
    pause_subscriptions: z.boolean().optional(),
  }).optional(),
  parameters: z.record(z.string(), z.string()).optional(),
});

const PipelineTask = z.object({
  pipeline_id: z.string(),
  full_refresh: z.boolean().optional(),
});

const SparkPythonTask = z.object({
  python_file: z.string().describe(
    "Path to .py file (workspace path, dbfs URI, S3 URI, or git repo file)",
  ),
  parameters: z.array(z.string()).optional().describe("Command-line arguments"),
  source: z.enum(["WORKSPACE", "GIT"]).optional(),
});

const SparkJarTask = z.object({
  main_class_name: z.string(),
  parameters: z.array(z.string()).optional(),
  run_as_repl: z.boolean().optional(),
});

const PythonWheelTask = z.object({
  package_name: z.string(),
  entry_point: z.string(),
  parameters: z.array(z.string()).optional().describe("Positional arguments"),
  named_parameters: z.record(z.string(), z.string()).optional().describe(
    "Keyword arguments",
  ),
});

const DbtTask = z.object({
  commands: z.array(z.string()).min(1).max(10).describe(
    "Up to 10 dbt CLI commands run sequentially (e.g. ['dbt deps', 'dbt run'])",
  ),
  warehouse_id: z.string().describe(
    "SQL Warehouse the dbt CLI connects through",
  ),
  project_directory: z.string().optional(),
  profiles_directory: z.string().optional(),
  schema: z.string().optional(),
  catalog: z.string().optional(),
  source: z.enum(["WORKSPACE", "GIT"]).optional(),
});

const RunJobTask = z.object({
  job_id: z.number().int(),
  job_parameters: z.record(z.string(), z.string()).optional(),
});

const ConditionTask = z.object({
  op: z.enum([
    "EQUAL_TO",
    "GREATER_THAN",
    "GREATER_THAN_OR_EQUAL",
    "LESS_THAN",
    "LESS_THAN_OR_EQUAL",
    "NOT_EQUAL",
  ]),
  left: z.string(),
  right: z.string(),
});

const TASK_BODY_KEYS = [
  "notebook_task",
  "sql_task",
  "pipeline_task",
  "spark_python_task",
  "spark_jar_task",
  "python_wheel_task",
  "dbt_task",
  "run_job_task",
  "condition_task",
  "for_each_task",
] as const;

// `for_each_task` nests a Task (the task to run for each input). Zod's
// recursive types use z.lazy. We declare the type explicitly so TS can resolve
// the cycle.
type TaskShape = {
  task_key: string;
  description?: string;
  depends_on?: Array<{ task_key: string }>;
  job_cluster_key?: string;
  existing_cluster_id?: string;
  timeout_seconds?: number;
  max_retries?: number;
  notebook_task?: z.infer<typeof NotebookTask>;
  sql_task?: z.infer<typeof SqlQueryTask>;
  pipeline_task?: z.infer<typeof PipelineTask>;
  spark_python_task?: z.infer<typeof SparkPythonTask>;
  spark_jar_task?: z.infer<typeof SparkJarTask>;
  python_wheel_task?: z.infer<typeof PythonWheelTask>;
  dbt_task?: z.infer<typeof DbtTask>;
  run_job_task?: z.infer<typeof RunJobTask>;
  condition_task?: z.infer<typeof ConditionTask>;
  for_each_task?: { inputs: string; concurrency?: number; task: TaskShape };
};

const Task: z.ZodType<TaskShape> = z.lazy(() =>
  z.object({
    task_key: z.string().regex(/^[a-zA-Z0-9_-]+$/),
    description: z.string().optional(),
    depends_on: z.array(z.object({ task_key: z.string() })).optional(),
    job_cluster_key: z.string().optional(),
    existing_cluster_id: z.string().optional(),
    timeout_seconds: z.number().int().nonnegative().optional(),
    max_retries: z.number().int().nonnegative().optional(),
    notebook_task: NotebookTask.optional(),
    sql_task: SqlQueryTask.optional(),
    pipeline_task: PipelineTask.optional(),
    spark_python_task: SparkPythonTask.optional(),
    spark_jar_task: SparkJarTask.optional(),
    python_wheel_task: PythonWheelTask.optional(),
    dbt_task: DbtTask.optional(),
    run_job_task: RunJobTask.optional(),
    condition_task: ConditionTask.optional(),
    for_each_task: z.object({
      inputs: z.string().describe(
        "JSON array literal OR JSONPath into a task output",
      ),
      concurrency: z.number().int().positive().optional(),
      task: Task,
    }).optional(),
  }).refine(
    (t) => TASK_BODY_KEYS.filter((k) => t[k] !== undefined).length === 1,
    {
      message: "exactly one task body required: " + TASK_BODY_KEYS.join(", "),
    },
  )
);

const JobCluster = z.object({
  job_cluster_key: z.string(),
  new_cluster: z.object({
    spark_version: z.string(),
    node_type_id: z.string(),
    num_workers: z.number().int().nonnegative().optional(),
    autoscale: z.object({
      min_workers: z.number().int().nonnegative(),
      max_workers: z.number().int().nonnegative(),
    }).optional(),
    data_security_mode: z.enum([
      "SINGLE_USER",
      "USER_ISOLATION",
      "NONE",
    ]).optional(),
    custom_tags: z.record(z.string(), z.string()).optional(),
  }),
});

const JobSettings = z.object({
  name: z.string().min(1).max(4096),
  tasks: z.array(Task).min(1).max(100),
  job_clusters: z.array(JobCluster).max(100).optional(),
  schedule: z.object({
    quartz_cron_expression: z.string(),
    timezone_id: z.string(),
    pause_status: z.enum(["PAUSED", "UNPAUSED"]).optional(),
  }).optional(),
  tags: z.record(z.string(), z.string()).optional(),
  timeout_seconds: z.number().int().nonnegative().optional(),
  max_concurrent_runs: z.number().int().positive().optional(),
  queue: z.object({ enabled: z.boolean() }).optional(),
});

// ---------------------------------------------------------------------------
// Resource schemas
// ---------------------------------------------------------------------------

const JobResourceSchema = z.object({
  job_id: z.number().int(),
  name: z.string(),
  creator_user_name: z.string().optional(),
  created_time_ms: z.number().int(),
  settings_hash: z.string(),
  workspace_url: z.string().url(),
});

const LastRunResourceSchema = z.object({
  job_id: z.number().int(),
  run_id: z.number().int(),
  run_page_url: z.string().url().optional(),
  life_cycle_state: z.string(),
  result_state: z.string().optional(),
  start_time_ms: z.number().int().optional(),
  end_time_ms: z.number().int().optional(),
});

// ---------------------------------------------------------------------------
// Model
// ---------------------------------------------------------------------------

/**
 * `@mfbaig35r/databricks/job`: Databricks Jobs API 2.2 lifecycle as a Swamp model.
 *
 * Methods cover create, read, update (full reset via `/jobs/reset`), delete,
 * `run` (fire-and-forget `/jobs/run-now`), `wait_run` (poll until terminal),
 * and `cancel_run`. Resources track the workspace-assigned `job_id` and the
 * most recent run, so subsequent calls look up state by the user-supplied
 * `name` rather than re-creating.
 *
 * Auth is configured via `globalArguments`: `pat` (resolved via CEL
 * `vault.get`), `oauth_m2m` (client credentials), or `azure_msi` (stubbed).
 *
 * Notebook management is in the sibling `@mfbaig35r/databricks/notebook` model
 * as of v0.2; this model only references notebooks by path through
 * `notebook_task.notebook_path`.
 *
 * @see https://docs.databricks.com/api/workspace/jobs
 */
export const model = {
  type: "@mfbaig35r/databricks/job",
  version: "2026.05.30.10",
  globalArguments: GlobalArgsSchema,

  resources: {
    "job": {
      description: "A Databricks Jobs API 2.2 job, keyed by user-supplied name",
      schema: JobResourceSchema,
      lifetime: "infinite" as const,
      garbageCollection: 5,
    },
    "last_run": {
      description: "Most recent run triggered through this model",
      schema: LastRunResourceSchema,
      lifetime: "workflow" as const,
      garbageCollection: 10,
    },
  },

  methods: {
    create: {
      description: "Create a Databricks job via POST /api/2.2/jobs/create. " +
        "Writes a 'job' resource keyed by name; subsequent methods look it up.",
      arguments: JobSettings,
      execute: async (
        args: z.infer<typeof JobSettings>,
        context: {
          globalArgs: GlobalArgs;
          writeResource: WriteResource;
          logger: Logger;
        },
      ) => {
        const out = await dbxFetch(
          context.globalArgs,
          "/api/2.2/jobs/create",
          { method: "POST", body: JSON.stringify(args) },
        );
        const jobId = out.job_id as number;
        context.logger.info(
          "Created Databricks job {name} -> {job_id}",
          { name: args.name, job_id: jobId },
        );
        const handle = await context.writeResource("job", args.name, {
          job_id: jobId,
          name: args.name,
          created_time_ms: Date.now(),
          settings_hash: await sha256(JSON.stringify(args)),
          workspace_url: context.globalArgs.workspace_url,
        });
        return { dataHandles: [handle] };
      },
    },

    read: {
      description:
        "Fetch current settings from the workspace via GET /api/2.2/jobs/get.",
      arguments: z.object({ job_ref: z.string() }),
      execute: async (
        args: { job_ref: string },
        context: {
          globalArgs: GlobalArgs;
          readResource: ReadResource;
          logger: Logger;
        },
      ) => {
        const prior = await context.readResource(args.job_ref);
        if (!prior) {
          throw new Error(
            `No stored 'job' resource named '${args.job_ref}'. Call 'create' first.`,
          );
        }
        const jobId = prior.job_id as number;
        const live = await dbxFetch(
          context.globalArgs,
          `/api/2.2/jobs/get?job_id=${jobId}`,
        );
        context.logger.info("Read job {job_id}", { job_id: jobId });
        return { dataHandles: [], outputs: { live } };
      },
    },

    update: {
      description: "Full replace via POST /api/2.2/jobs/reset. " +
        "Spec passed becomes spec on the workspace; partial patch not supported.",
      arguments: z.object({
        job_ref: z.string(),
        settings: JobSettings,
      }),
      execute: async (
        args: { job_ref: string; settings: z.infer<typeof JobSettings> },
        context: {
          globalArgs: GlobalArgs;
          readResource: ReadResource;
          writeResource: WriteResource;
          logger: Logger;
        },
      ) => {
        const prior = await context.readResource(args.job_ref);
        if (!prior) {
          throw new Error(
            `No stored 'job' resource named '${args.job_ref}'. Call 'create' first.`,
          );
        }
        const jobId = prior.job_id as number;
        await dbxFetch(
          context.globalArgs,
          "/api/2.2/jobs/reset",
          {
            method: "POST",
            body: JSON.stringify({
              job_id: jobId,
              new_settings: args.settings,
            }),
          },
        );
        context.logger.info(
          "Reset job {job_id} to new settings",
          { job_id: jobId },
        );
        const handle = await context.writeResource("job", args.settings.name, {
          ...prior,
          name: args.settings.name,
          settings_hash: await sha256(JSON.stringify(args.settings)),
        });
        return { dataHandles: [handle] };
      },
    },

    delete: {
      description: "Delete the job via POST /api/2.2/jobs/delete.",
      arguments: z.object({ job_ref: z.string() }),
      execute: async (
        args: { job_ref: string },
        context: {
          globalArgs: GlobalArgs;
          readResource: ReadResource;
          logger: Logger;
        },
      ) => {
        const prior = await context.readResource(args.job_ref);
        if (!prior) {
          throw new Error(
            `No stored 'job' resource named '${args.job_ref}'.`,
          );
        }
        const jobId = prior.job_id as number;
        await dbxFetch(
          context.globalArgs,
          "/api/2.2/jobs/delete",
          { method: "POST", body: JSON.stringify({ job_id: jobId }) },
        );
        context.logger.info("Deleted job {job_id}", { job_id: jobId });
        return { dataHandles: [] };
      },
    },

    run: {
      description: "Trigger a one-off run via POST /api/2.2/jobs/run-now. " +
        "Fire-and-forget; call 'wait_run' separately to poll for terminal state.",
      arguments: z.object({
        job_ref: z.string(),
        job_parameters: z.record(z.string(), z.string()).optional(),
        notebook_params: z.record(z.string(), z.string()).optional(),
        idempotency_token: z.string().optional(),
      }),
      execute: async (
        args: {
          job_ref: string;
          job_parameters?: Record<string, string>;
          notebook_params?: Record<string, string>;
          idempotency_token?: string;
        },
        context: {
          globalArgs: GlobalArgs;
          readResource: ReadResource;
          writeResource: WriteResource;
          logger: Logger;
        },
      ) => {
        const prior = await context.readResource(args.job_ref);
        if (!prior) {
          throw new Error(
            `No stored 'job' resource named '${args.job_ref}'. Call 'create' first.`,
          );
        }
        const jobId = prior.job_id as number;
        const out = await dbxFetch(
          context.globalArgs,
          "/api/2.2/jobs/run-now",
          {
            method: "POST",
            body: JSON.stringify({
              job_id: jobId,
              job_parameters: args.job_parameters,
              notebook_params: args.notebook_params,
              idempotency_token: args.idempotency_token,
            }),
          },
        );
        const runId = out.run_id as number;
        const runPageUrl = out.run_page_url as string | undefined;
        context.logger.info(
          "Triggered run {run_id} for job {job_id}",
          { run_id: runId, job_id: jobId },
        );
        const handle = await context.writeResource(
          "last_run",
          `${jobId}:${runId}`,
          {
            job_id: jobId,
            run_id: runId,
            run_page_url: runPageUrl,
            life_cycle_state: "PENDING",
          },
        );
        return { dataHandles: [handle] };
      },
    },

    wait_run: {
      description:
        "Poll GET /api/2.2/jobs/runs/get until life_cycle_state is terminal " +
        "(TERMINATED, SKIPPED, INTERNAL_ERROR). Updates last_run resource.",
      arguments: z.object({
        run_id: z.number().int(),
        poll_seconds: z.number().int().positive().default(10),
        timeout_seconds: z.number().int().positive().default(7200),
      }),
      execute: async (
        args: { run_id: number; poll_seconds: number; timeout_seconds: number },
        context: {
          globalArgs: GlobalArgs;
          writeResource: WriteResource;
          logger: Logger;
        },
      ) => {
        const terminal = new Set([
          "TERMINATED",
          "SKIPPED",
          "INTERNAL_ERROR",
        ]);
        const deadline = Date.now() + args.timeout_seconds * 1000;
        let last: Record<string, unknown> | null = null;
        while (Date.now() < deadline) {
          const res = await dbxFetch(
            context.globalArgs,
            `/api/2.2/jobs/runs/get?run_id=${args.run_id}`,
          );
          last = res;
          const state = res.state as {
            life_cycle_state: string;
            result_state?: string;
          };
          context.logger.info(
            "Run {run_id} state {state}",
            { run_id: args.run_id, state: state.life_cycle_state },
          );
          if (terminal.has(state.life_cycle_state)) break;
          await new Promise((r) => setTimeout(r, args.poll_seconds * 1000));
        }
        if (!last) throw new Error(`No response for run ${args.run_id}`);
        const state = last.state as {
          life_cycle_state: string;
          result_state?: string;
        };
        const jobId = (last.job_id ?? 0) as number;
        const handle = await context.writeResource(
          "last_run",
          `${jobId}:${args.run_id}`,
          {
            job_id: jobId,
            run_id: args.run_id,
            run_page_url: last.run_page_url as string | undefined,
            life_cycle_state: state.life_cycle_state,
            result_state: state.result_state,
            start_time_ms: last.start_time as number | undefined,
            end_time_ms: last.end_time as number | undefined,
          },
        );
        return { dataHandles: [handle] };
      },
    },

    cancel_run: {
      description: "POST /api/2.2/jobs/runs/cancel for a given run_id.",
      arguments: z.object({ run_id: z.number().int() }),
      execute: async (
        args: { run_id: number },
        context: {
          globalArgs: GlobalArgs;
          logger: Logger;
        },
      ) => {
        await dbxFetch(
          context.globalArgs,
          "/api/2.2/jobs/runs/cancel",
          { method: "POST", body: JSON.stringify({ run_id: args.run_id }) },
        );
        context.logger.info("Cancelled run {run_id}", { run_id: args.run_id });
        return { dataHandles: [] };
      },
    },

    create_or_update: {
      description:
        "Reconcile: if a 'job' resource named args.name exists, call " +
        "POST /api/2.2/jobs/reset (full replace). Otherwise POST /jobs/create.",
      arguments: JobSettings,
      execute: async (
        args: z.infer<typeof JobSettings>,
        context: {
          globalArgs: GlobalArgs;
          readResource: ReadResource;
          writeResource: WriteResource;
          logger: Logger;
        },
      ) => {
        const prior = await context.readResource(args.name);
        if (prior) {
          const jobId = prior.job_id as number;
          await dbxFetch(
            context.globalArgs,
            "/api/2.2/jobs/reset",
            {
              method: "POST",
              body: JSON.stringify({ job_id: jobId, new_settings: args }),
            },
          );
          context.logger.info(
            "create_or_update: reset existing job {job_id}",
            { job_id: jobId },
          );
          const handle = await context.writeResource("job", args.name, {
            ...prior,
            name: args.name,
            settings_hash: await sha256(JSON.stringify(args)),
          });
          return { dataHandles: [handle] };
        }
        const out = await dbxFetch(
          context.globalArgs,
          "/api/2.2/jobs/create",
          { method: "POST", body: JSON.stringify(args) },
        );
        const jobId = out.job_id as number;
        context.logger.info(
          "create_or_update: created new job {job_id}",
          { job_id: jobId },
        );
        const handle = await context.writeResource("job", args.name, {
          job_id: jobId,
          name: args.name,
          created_time_ms: Date.now(),
          settings_hash: await sha256(JSON.stringify(args)),
          workspace_url: context.globalArgs.workspace_url,
        });
        return { dataHandles: [handle] };
      },
    },
  },
};
