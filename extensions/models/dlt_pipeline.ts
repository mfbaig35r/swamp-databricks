import { z } from "npm:zod@4";
import {
  dbxFetch,
  existsOnWorkspace,
  GlobalArgs,
  GlobalArgsSchema,
  Logger,
  ReadResource,
  sha256,
  WriteResource,
} from "./_lib/databricks.ts";

// ---------------------------------------------------------------------------
// Library schemas (what runs inside the pipeline)
// ---------------------------------------------------------------------------

const NotebookLibrary = z.object({
  notebook: z.object({
    path: z.string().regex(/^\/.+/, "notebook path must be absolute"),
  }),
});

const FileLibrary = z.object({
  file: z.object({
    path: z.string().regex(/^\/.+/, "file path must be absolute"),
  }),
});

const Library = z.union([NotebookLibrary, FileLibrary]);

const PipelineCluster = z.object({
  label: z.enum(["default", "maintenance"]).optional(),
  num_workers: z.number().int().nonnegative().optional(),
  autoscale: z.object({
    min_workers: z.number().int().nonnegative(),
    max_workers: z.number().int().nonnegative(),
    mode: z.enum(["ENHANCED", "LEGACY"]).optional(),
  }).optional(),
  node_type_id: z.string().optional(),
  driver_node_type_id: z.string().optional(),
  spark_conf: z.record(z.string(), z.string()).optional(),
  custom_tags: z.record(z.string(), z.string()).optional(),
});

// ---------------------------------------------------------------------------
// Pipeline settings (subset; expand per release as users hit edges)
// ---------------------------------------------------------------------------

const PipelineSettings = z.object({
  name: z.string().min(1).max(1024),
  storage: z.string().optional().describe(
    "DBFS/UC volume path for pipeline storage. Optional on Free/serverless.",
  ),
  configuration: z.record(z.string(), z.string()).optional().describe(
    "Spark conf passed into the pipeline runtime",
  ),
  catalog: z.string().optional().describe(
    "Unity Catalog target catalog (use with target schema)",
  ),
  target: z.string().optional().describe(
    "Default schema/database for pipeline outputs",
  ),
  libraries: z.array(Library).min(1).describe(
    "Notebooks or files that define the pipeline",
  ),
  clusters: z.array(PipelineCluster).optional(),
  continuous: z.boolean().optional().describe(
    "true = streaming, false = triggered (default false)",
  ),
  development: z.boolean().optional().describe(
    "true = dev mode (no auto-restart on failure)",
  ),
  photon: z.boolean().optional(),
  edition: z.enum(["CORE", "PRO", "ADVANCED"]).optional().describe(
    "DLT pricing edition; ignored on Free/serverless",
  ),
  channel: z.enum(["CURRENT", "PREVIEW"]).optional(),
  serverless: z.boolean().optional().describe(
    "Use serverless compute (required on Databricks Free)",
  ),
}).refine(
  (s) => !s.serverless || !!s.catalog,
  {
    message:
      "catalog is required when serverless=true (Databricks API constraint; " +
      "on Free Edition the default UC catalog is 'workspace')",
    path: ["catalog"],
  },
);

// ---------------------------------------------------------------------------
// Resource schemas
// ---------------------------------------------------------------------------

const PipelineResourceSchema = z.object({
  pipeline_id: z.string(),
  name: z.string(),
  created_time_ms: z.number().int(),
  settings_hash: z.string(),
  workspace_url: z.string().url(),
});

const LastUpdateResourceSchema = z.object({
  pipeline_id: z.string(),
  update_id: z.string(),
  state: z.string(),
  cause: z.string().optional(),
  creation_time_ms: z.number().int().optional(),
});

// ---------------------------------------------------------------------------
// Model
// ---------------------------------------------------------------------------

/**
 * `@mfbaig35r/databricks/dlt_pipeline`: Delta Live Tables pipeline lifecycle.
 *
 * DLT calls runs "updates". Methods mirror the job model: create, read,
 * update (full replace via PUT), delete, start_update (triggers a pipeline
 * update), wait_update (poll until terminal), stop (cancel + halt).
 *
 * On Databricks Free Edition, set `serverless: true` in the create args. On
 * paid workspaces you can specify `clusters` with explicit node types instead.
 *
 * @see https://docs.databricks.com/api/workspace/pipelines
 */
export const model = {
  type: "@mfbaig35r/databricks/dlt_pipeline",
  version: "2026.05.30.16",
  globalArguments: GlobalArgsSchema,

  resources: {
    "pipeline": {
      description: "A DLT pipeline, keyed by user-supplied name",
      schema: PipelineResourceSchema,
      lifetime: "infinite" as const,
      garbageCollection: 5,
    },
    "last_update": {
      description: "Most recent update triggered through this model",
      schema: LastUpdateResourceSchema,
      lifetime: "workflow" as const,
      garbageCollection: 10,
    },
  },

  methods: {
    create: {
      description: "Create a DLT pipeline via POST /api/2.0/pipelines. " +
        "Writes a 'pipeline' resource keyed by name.",
      arguments: PipelineSettings,
      execute: async (
        args: z.infer<typeof PipelineSettings>,
        context: {
          globalArgs: GlobalArgs;
          writeResource: WriteResource;
          logger: Logger;
        },
      ) => {
        const out = await dbxFetch(
          context.globalArgs,
          "/api/2.0/pipelines",
          { method: "POST", body: JSON.stringify(args) },
        );
        const pipelineId = out.pipeline_id as string;
        context.logger.info(
          "Created DLT pipeline {name} -> {pipeline_id}",
          { name: args.name, pipeline_id: pipelineId },
        );
        const handle = await context.writeResource("pipeline", args.name, {
          pipeline_id: pipelineId,
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
        "Fetch live pipeline state via GET /api/2.0/pipelines/{pipeline_id}.",
      arguments: z.object({ pipeline_ref: z.string() }),
      execute: async (
        args: { pipeline_ref: string },
        context: {
          globalArgs: GlobalArgs;
          readResource: ReadResource;
          logger: Logger;
        },
      ) => {
        const prior = await context.readResource(args.pipeline_ref);
        if (!prior) {
          throw new Error(
            `No stored 'pipeline' resource named '${args.pipeline_ref}'. Call 'create' first.`,
          );
        }
        const pipelineId = prior.pipeline_id as string;
        const live = await dbxFetch(
          context.globalArgs,
          `/api/2.0/pipelines/${pipelineId}`,
        );
        context.logger.info(
          "Read pipeline {pipeline_id}",
          { pipeline_id: pipelineId },
        );
        return { dataHandles: [], outputs: { live } };
      },
    },

    update: {
      description:
        "Full replace pipeline settings via PUT /api/2.0/pipelines/{pipeline_id}.",
      arguments: z.object({
        pipeline_ref: z.string(),
        settings: PipelineSettings,
      }),
      execute: async (
        args: {
          pipeline_ref: string;
          settings: z.infer<typeof PipelineSettings>;
        },
        context: {
          globalArgs: GlobalArgs;
          readResource: ReadResource;
          writeResource: WriteResource;
          logger: Logger;
        },
      ) => {
        const prior = await context.readResource(args.pipeline_ref);
        if (!prior) {
          throw new Error(
            `No stored 'pipeline' resource named '${args.pipeline_ref}'.`,
          );
        }
        const pipelineId = prior.pipeline_id as string;
        await dbxFetch(
          context.globalArgs,
          `/api/2.0/pipelines/${pipelineId}`,
          {
            method: "PUT",
            body: JSON.stringify({ ...args.settings, id: pipelineId }),
          },
        );
        context.logger.info(
          "Updated pipeline {pipeline_id}",
          { pipeline_id: pipelineId },
        );
        const handle = await context.writeResource(
          "pipeline",
          args.settings.name,
          {
            ...prior,
            name: args.settings.name,
            settings_hash: await sha256(JSON.stringify(args.settings)),
          },
        );
        return { dataHandles: [handle] };
      },
    },

    delete: {
      description:
        "Delete the pipeline via DELETE /api/2.0/pipelines/{pipeline_id}.",
      arguments: z.object({ pipeline_ref: z.string() }),
      execute: async (
        args: { pipeline_ref: string },
        context: {
          globalArgs: GlobalArgs;
          readResource: ReadResource;
          logger: Logger;
        },
      ) => {
        const prior = await context.readResource(args.pipeline_ref);
        if (!prior) {
          throw new Error(
            `No stored 'pipeline' resource named '${args.pipeline_ref}'.`,
          );
        }
        const pipelineId = prior.pipeline_id as string;
        await dbxFetch(
          context.globalArgs,
          `/api/2.0/pipelines/${pipelineId}`,
          { method: "DELETE" },
        );
        context.logger.info(
          "Deleted pipeline {pipeline_id}",
          { pipeline_id: pipelineId },
        );
        return { dataHandles: [] };
      },
    },

    start_update: {
      description:
        "Trigger a pipeline update via POST /api/2.0/pipelines/{pipeline_id}/updates. " +
        "Returns update_id. Fire-and-forget; call 'wait_update' to poll.",
      arguments: z.object({
        pipeline_ref: z.string(),
        full_refresh: z.boolean().default(false),
        full_refresh_selection: z.array(z.string()).optional().describe(
          "Subset of tables to fully refresh",
        ),
        refresh_selection: z.array(z.string()).optional().describe(
          "Subset of tables to incrementally refresh",
        ),
        cause: z.string().optional(),
      }),
      execute: async (
        args: {
          pipeline_ref: string;
          full_refresh: boolean;
          full_refresh_selection?: string[];
          refresh_selection?: string[];
          cause?: string;
        },
        context: {
          globalArgs: GlobalArgs;
          readResource: ReadResource;
          writeResource: WriteResource;
          logger: Logger;
        },
      ) => {
        const prior = await context.readResource(args.pipeline_ref);
        if (!prior) {
          throw new Error(
            `No stored 'pipeline' resource named '${args.pipeline_ref}'.`,
          );
        }
        const pipelineId = prior.pipeline_id as string;
        const body: Record<string, unknown> = {
          full_refresh: args.full_refresh,
        };
        if (args.full_refresh_selection) {
          body.full_refresh_selection = args.full_refresh_selection;
        }
        if (args.refresh_selection) {
          body.refresh_selection = args.refresh_selection;
        }
        if (args.cause) body.cause = args.cause;
        const out = await dbxFetch(
          context.globalArgs,
          `/api/2.0/pipelines/${pipelineId}/updates`,
          { method: "POST", body: JSON.stringify(body) },
        );
        const updateId = out.update_id as string;
        context.logger.info(
          "Started update {update_id} on pipeline {pipeline_id}",
          { update_id: updateId, pipeline_id: pipelineId },
        );
        const handle = await context.writeResource(
          "last_update",
          `${pipelineId}:${updateId}`,
          {
            pipeline_id: pipelineId,
            update_id: updateId,
            state: "QUEUED",
            cause: args.cause,
            creation_time_ms: Date.now(),
          },
        );
        return { dataHandles: [handle] };
      },
    },

    wait_update: {
      description:
        "Poll GET /api/2.0/pipelines/{pipeline_id}/updates/{update_id} until " +
        "state is terminal (COMPLETED, FAILED, CANCELED). Updates last_update resource.",
      arguments: z.object({
        pipeline_ref: z.string(),
        update_id: z.string(),
        poll_seconds: z.number().int().positive().default(15),
        timeout_seconds: z.number().int().positive().default(7200),
      }),
      execute: async (
        args: {
          pipeline_ref: string;
          update_id: string;
          poll_seconds: number;
          timeout_seconds: number;
        },
        context: {
          globalArgs: GlobalArgs;
          readResource: ReadResource;
          writeResource: WriteResource;
          logger: Logger;
        },
      ) => {
        const prior = await context.readResource(args.pipeline_ref);
        if (!prior) {
          throw new Error(
            `No stored 'pipeline' resource named '${args.pipeline_ref}'.`,
          );
        }
        const pipelineId = prior.pipeline_id as string;
        const terminal = new Set(["COMPLETED", "FAILED", "CANCELED"]);
        const deadline = Date.now() + args.timeout_seconds * 1000;
        let last: Record<string, unknown> | null = null;
        while (Date.now() < deadline) {
          const res = await dbxFetch(
            context.globalArgs,
            `/api/2.0/pipelines/${pipelineId}/updates/${args.update_id}`,
          );
          last = res;
          const update = res.update as { state: string };
          context.logger.info(
            "Update {update_id} state {state}",
            { update_id: args.update_id, state: update.state },
          );
          if (terminal.has(update.state)) break;
          await new Promise((r) => setTimeout(r, args.poll_seconds * 1000));
        }
        if (!last) {
          throw new Error(`No response for update ${args.update_id}`);
        }
        const update = last.update as {
          state: string;
          cause?: string;
          creation_time?: number;
        };
        const handle = await context.writeResource(
          "last_update",
          `${pipelineId}:${args.update_id}`,
          {
            pipeline_id: pipelineId,
            update_id: args.update_id,
            state: update.state,
            cause: update.cause,
            creation_time_ms: update.creation_time,
          },
        );
        return { dataHandles: [handle] };
      },
    },

    stop: {
      description:
        "Stop the pipeline via POST /api/2.0/pipelines/{pipeline_id}/stop. " +
        "Cancels any in-flight update.",
      arguments: z.object({ pipeline_ref: z.string() }),
      execute: async (
        args: { pipeline_ref: string },
        context: {
          globalArgs: GlobalArgs;
          readResource: ReadResource;
          logger: Logger;
        },
      ) => {
        const prior = await context.readResource(args.pipeline_ref);
        if (!prior) {
          throw new Error(
            `No stored 'pipeline' resource named '${args.pipeline_ref}'.`,
          );
        }
        const pipelineId = prior.pipeline_id as string;
        await dbxFetch(
          context.globalArgs,
          `/api/2.0/pipelines/${pipelineId}/stop`,
          { method: "POST", body: "{}" },
        );
        context.logger.info(
          "Stopped pipeline {pipeline_id}",
          { pipeline_id: pipelineId },
        );
        return { dataHandles: [] };
      },
    },

    create_or_update: {
      description:
        "Reconcile via Swamp data + workspace check: if a 'pipeline' " +
        "resource exists for this name AND the workspace still has that " +
        "pipeline_id, call PUT /api/2.0/pipelines/{id} (full replace). " +
        "Otherwise create. Safe across Swamp tombstones.",
      arguments: PipelineSettings,
      execute: async (
        args: z.infer<typeof PipelineSettings>,
        context: {
          globalArgs: GlobalArgs;
          readResource: ReadResource;
          writeResource: WriteResource;
          logger: Logger;
        },
      ) => {
        const prior = await context.readResource(args.name);
        if (prior) {
          const pipelineId = prior.pipeline_id as string;
          const stillExists = await existsOnWorkspace(
            context.globalArgs,
            `/api/2.0/pipelines/${pipelineId}`,
          );
          if (stillExists) {
            await dbxFetch(
              context.globalArgs,
              `/api/2.0/pipelines/${pipelineId}`,
              {
                method: "PUT",
                body: JSON.stringify({ ...args, id: pipelineId }),
              },
            );
            context.logger.info(
              "create_or_update: updated existing pipeline {pipeline_id}",
              { pipeline_id: pipelineId },
            );
            const handle = await context.writeResource(
              "pipeline",
              args.name,
              {
                ...prior,
                name: args.name,
                settings_hash: await sha256(JSON.stringify(args)),
              },
            );
            return { dataHandles: [handle] };
          }
        }
        const out = await dbxFetch(
          context.globalArgs,
          "/api/2.0/pipelines",
          { method: "POST", body: JSON.stringify(args) },
        );
        const pipelineId = out.pipeline_id as string;
        context.logger.info(
          "create_or_update: created new pipeline {pipeline_id}",
          { pipeline_id: pipelineId },
        );
        const handle = await context.writeResource("pipeline", args.name, {
          pipeline_id: pipelineId,
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
