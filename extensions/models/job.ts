import { z } from "npm:zod@4";

// ---------------------------------------------------------------------------
// Task schemas (v1: notebook, sql, pipeline; others passthrough)
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
    source: z.string().optional(),
  }).optional(),
  parameters: z.record(z.string(), z.string()).optional(),
});

const PipelineTask = z.object({
  pipeline_id: z.string(),
  full_refresh: z.boolean().optional(),
});

const Task = z.object({
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
}).refine(
  (t) =>
    [t.notebook_task, t.sql_task, t.pipeline_task].filter(Boolean).length === 1,
  {
    message:
      "exactly one task body required (notebook_task, sql_task, or pipeline_task)",
  },
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
// Global args: workspace URL + auth strategy. Secrets resolved at call time.
// ---------------------------------------------------------------------------

const GlobalArgsSchema = z.object({
  workspace_url: z.string().url(),
  auth_kind: z.enum(["pat", "oauth_m2m", "azure_msi"]).default("pat"),
  token: z.string().optional().describe(
    'Resolved PAT value. Pass via CEL: ${{ vault.get("databricks", "pat") }}',
  ),
  oauth_client_id: z.string().optional(),
  oauth_client_secret: z.string().optional().describe(
    'Resolved OAuth M2M client secret. Pass via CEL: ${{ vault.get("...", "...") }}',
  ),
  azure_msi_client_id: z.string().optional(),
});

type GlobalArgs = z.infer<typeof GlobalArgsSchema>;

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

const NotebookResourceSchema = z.object({
  path: z.string(),
  language: z.string(),
  uploaded_time_ms: z.number().int(),
  workspace_url: z.string().url(),
});

const UploadNotebookArgs = z.object({
  path: z.string().regex(/^\/.+/, "path must be absolute (start with /)"),
  content: z.string().min(1).describe("Raw notebook source text (NOT base64)"),
  language: z.enum(["PYTHON", "SCALA", "SQL", "R"]).default("PYTHON"),
  overwrite: z.boolean().default(false),
});

const DeleteNotebookArgs = z.object({
  path: z.string().regex(/^\/.+/),
  recursive: z.boolean().default(false),
});

/** UTF-8 safe base64 encode for the workspace import body. */
function b64encode(s: string): string {
  const bytes = new TextEncoder().encode(s);
  let bin = "";
  for (let i = 0; i < bytes.length; i++) bin += String.fromCharCode(bytes[i]);
  return btoa(bin);
}

/**
 * Encode a workspace path into a Swamp-safe resource name.
 * Swamp rejects '/', '\\', '..', and null bytes in resource names.
 * "/Shared/foo/bar" -> "Shared:foo:bar"
 */
function pathToResourceName(path: string): string {
  return path.replace(/^\//, "").replace(/\//g, ":");
}

// ---------------------------------------------------------------------------
// Helpers (Web Crypto, Bearer fetch)
// ---------------------------------------------------------------------------

async function sha256(input: string): Promise<string> {
  const buf = await crypto.subtle.digest(
    "SHA-256",
    new TextEncoder().encode(input),
  );
  return Array.from(new Uint8Array(buf))
    .map((b) => b.toString(16).padStart(2, "0"))
    .join("");
}

async function resolveToken(globalArgs: GlobalArgs): Promise<string> {
  switch (globalArgs.auth_kind) {
    case "pat": {
      if (!globalArgs.token) {
        throw new Error(
          "auth_kind=pat requires globalArgs.token (resolved PAT value, " +
            'pass via CEL: ${{ vault.get("databricks", "pat") }})',
        );
      }
      return globalArgs.token;
    }
    case "oauth_m2m": {
      if (!globalArgs.oauth_client_id || !globalArgs.oauth_client_secret) {
        throw new Error(
          "auth_kind=oauth_m2m requires globalArgs.oauth_client_id and oauth_client_secret",
        );
      }
      const tokenUrl = `${globalArgs.workspace_url}/oidc/v1/token`;
      const body = new URLSearchParams({
        grant_type: "client_credentials",
        scope: "all-apis",
      });
      const auth = btoa(
        `${globalArgs.oauth_client_id}:${globalArgs.oauth_client_secret}`,
      );
      const res = await fetch(tokenUrl, {
        method: "POST",
        headers: {
          "Authorization": `Basic ${auth}`,
          "Content-Type": "application/x-www-form-urlencoded",
        },
        body: body.toString(),
      });
      if (!res.ok) {
        throw new Error(
          `oauth_m2m token mint failed: ${res.status} ${await res.text()}`,
        );
      }
      const json = await res.json() as { access_token: string };
      return json.access_token;
    }
    case "azure_msi":
      throw new Error(
        "azure_msi auth is not implemented in @mfbaig35r/databricks/job v1",
      );
  }
}

async function dbxFetch(
  globalArgs: GlobalArgs,
  path: string,
  init: RequestInit = {},
): Promise<Record<string, unknown>> {
  const token = await resolveToken(globalArgs);
  const res = await fetch(`${globalArgs.workspace_url}${path}`, {
    ...init,
    headers: {
      "Authorization": `Bearer ${token}`,
      "Content-Type": "application/json",
      ...(init.headers ?? {}),
    },
  });
  if (!res.ok) {
    throw new Error(
      `databricks ${path} ${res.status}: ${await res.text()}`,
    );
  }
  return await res.json() as Record<string, unknown>;
}

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
 * Auth is configured via `globalArguments`: `pat` (vault-resolved token),
 * `oauth_m2m` (client credentials grant), or `azure_msi` (stubbed).
 *
 * @see https://docs.databricks.com/api/workspace/jobs
 */
export const model = {
  type: "@mfbaig35r/databricks/job",
  version: "2026.05.30.1",
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
    "notebook": {
      description:
        "A workspace notebook uploaded via this model, keyed by absolute path. " +
        "Convenience surface for end-to-end smoke testing; will split into " +
        "@mfbaig35r/databricks/notebook in a future release.",
      schema: NotebookResourceSchema,
      lifetime: "infinite" as const,
      garbageCollection: 5,
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

          writeResource: (
            specName: string,
            instanceName: string,
            data: Record<string, unknown>,
          ) => Promise<{ name: string }>;
          logger: {
            info: (msg: string, props: Record<string, unknown>) => void;
          };
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
        "Fetch current settings from the workspace via GET /api/2.2/jobs/get. " +
        "Returns live settings without modifying any stored resource.",
      arguments: z.object({ job_ref: z.string() }),
      execute: async (
        args: { job_ref: string },
        context: {
          globalArgs: GlobalArgs;

          readResource: (
            instanceName: string,
          ) => Promise<Record<string, unknown> | null>;
          logger: {
            info: (msg: string, props: Record<string, unknown>) => void;
          };
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
        "Spec passed becomes the spec on the workspace; partial patching not supported in v1.",
      arguments: z.object({
        job_ref: z.string(),
        settings: JobSettings,
      }),
      execute: async (
        args: { job_ref: string; settings: z.infer<typeof JobSettings> },
        context: {
          globalArgs: GlobalArgs;

          readResource: (
            instanceName: string,
          ) => Promise<Record<string, unknown> | null>;
          writeResource: (
            specName: string,
            instanceName: string,
            data: Record<string, unknown>,
          ) => Promise<{ name: string }>;
          logger: {
            info: (msg: string, props: Record<string, unknown>) => void;
          };
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

          readResource: (
            instanceName: string,
          ) => Promise<Record<string, unknown> | null>;
          logger: {
            info: (msg: string, props: Record<string, unknown>) => void;
          };
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

          readResource: (
            instanceName: string,
          ) => Promise<Record<string, unknown> | null>;
          writeResource: (
            specName: string,
            instanceName: string,
            data: Record<string, unknown>,
          ) => Promise<{ name: string }>;
          logger: {
            info: (msg: string, props: Record<string, unknown>) => void;
          };
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

          writeResource: (
            specName: string,
            instanceName: string,
            data: Record<string, unknown>,
          ) => Promise<{ name: string }>;
          logger: {
            info: (msg: string, props: Record<string, unknown>) => void;
          };
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
          logger: {
            info: (msg: string, props: Record<string, unknown>) => void;
          };
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

    upload_notebook: {
      description: "Import a notebook source to the workspace via " +
        "POST /api/2.0/workspace/import. Writes a 'notebook' resource keyed " +
        "by absolute path. Content is raw source text; the model base64-encodes.",
      arguments: UploadNotebookArgs,
      execute: async (
        args: z.infer<typeof UploadNotebookArgs>,
        context: {
          globalArgs: GlobalArgs;
          writeResource: (
            specName: string,
            instanceName: string,
            data: Record<string, unknown>,
          ) => Promise<{ name: string }>;
          logger: {
            info: (msg: string, props: Record<string, unknown>) => void;
          };
        },
      ) => {
        await dbxFetch(
          context.globalArgs,
          "/api/2.0/workspace/import",
          {
            method: "POST",
            body: JSON.stringify({
              path: args.path,
              content: b64encode(args.content),
              format: "SOURCE",
              language: args.language,
              overwrite: args.overwrite,
            }),
          },
        );
        context.logger.info(
          "Uploaded {language} notebook to {path}",
          { language: args.language, path: args.path },
        );
        const handle = await context.writeResource(
          "notebook",
          pathToResourceName(args.path),
          {
            path: args.path,
            language: args.language,
            uploaded_time_ms: Date.now(),
            workspace_url: context.globalArgs.workspace_url,
          },
        );
        return { dataHandles: [handle] };
      },
    },

    delete_notebook: {
      description:
        "Delete a notebook from the workspace via POST /api/2.0/workspace/delete.",
      arguments: DeleteNotebookArgs,
      execute: async (
        args: z.infer<typeof DeleteNotebookArgs>,
        context: {
          globalArgs: GlobalArgs;
          logger: {
            info: (msg: string, props: Record<string, unknown>) => void;
          };
        },
      ) => {
        await dbxFetch(
          context.globalArgs,
          "/api/2.0/workspace/delete",
          {
            method: "POST",
            body: JSON.stringify({
              path: args.path,
              recursive: args.recursive,
            }),
          },
        );
        context.logger.info("Deleted notebook {path}", { path: args.path });
        return { dataHandles: [] };
      },
    },
  },
};
