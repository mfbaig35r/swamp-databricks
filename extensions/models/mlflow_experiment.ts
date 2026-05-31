import { z } from "npm:zod@4";
import {
  dbxFetch,
  existsOnWorkspace,
  GlobalArgs,
  GlobalArgsSchema,
  Logger,
  ReadResource,
  WriteResource,
} from "./_lib/databricks.ts";

const CreateArgs = z.object({
  name: z.string().min(1).max(256).describe(
    "Workspace path, e.g. '/Shared/experiments/churn-model'",
  ),
  artifact_location: z.string().optional().describe(
    "Where to store run artifacts. Defaults to workspace-managed location.",
  ),
  tags: z.array(z.object({
    key: z.string(),
    value: z.string(),
  })).optional(),
});

const UpdateArgs = z.object({
  experiment_ref: z.string(),
  new_name: z.string().optional(),
});

const SetTagArgs = z.object({
  experiment_ref: z.string(),
  key: z.string(),
  value: z.string(),
});

const ExperimentResourceSchema = z.object({
  experiment_id: z.string(),
  name: z.string(),
  artifact_location: z.string().optional(),
  lifecycle_stage: z.string(),
  created_time_ms: z.number().int(),
  workspace_url: z.string().url(),
});

function pathToResourceName(path: string): string {
  return path.replace(/^\//, "").replace(/\//g, ":");
}

/**
 * `@mfbaig35r/databricks/mlflow_experiment`: workspace MLflow experiment
 * lifecycle. Pair with `registered_model` and `model_version` for the
 * full UC Model Registry training-to-registry path.
 *
 * Experiment names are workspace paths (e.g. `/Shared/experiments/churn`).
 * Resource names encode `/` as `:` for Swamp compatibility.
 *
 * @see https://docs.databricks.com/api/workspace/experiments
 */
export const model = {
  type: "@mfbaig35r/databricks/mlflow_experiment",
  version: "2026.05.30.18",
  globalArguments: GlobalArgsSchema,

  resources: {
    "experiment": {
      description: "A workspace MLflow experiment, keyed by workspace path " +
        "(with `/` encoded as `:` in the resource name).",
      schema: ExperimentResourceSchema,
      lifetime: "infinite" as const,
      garbageCollection: 5,
    },
  },

  methods: {
    create: {
      description: "POST /api/2.0/mlflow/experiments/create.",
      arguments: CreateArgs,
      execute: async (
        args: z.infer<typeof CreateArgs>,
        context: {
          globalArgs: GlobalArgs;
          writeResource: WriteResource;
          logger: Logger;
        },
      ) => {
        const out = await dbxFetch(
          context.globalArgs,
          "/api/2.0/mlflow/experiments/create",
          { method: "POST", body: JSON.stringify(args) },
        );
        const experimentId = out.experiment_id as string;
        context.logger.info(
          "Created MLflow experiment {name} -> {experiment_id}",
          { name: args.name, experiment_id: experimentId },
        );
        const handle = await context.writeResource(
          "experiment",
          pathToResourceName(args.name),
          {
            experiment_id: experimentId,
            name: args.name,
            artifact_location: args.artifact_location,
            lifecycle_stage: "active",
            created_time_ms: Date.now(),
            workspace_url: context.globalArgs.workspace_url,
          },
        );
        return { dataHandles: [handle] };
      },
    },

    read: {
      description:
        "GET /api/2.0/mlflow/experiments/get-by-name?experiment_name=...",
      arguments: z.object({ experiment_ref: z.string() }),
      execute: async (
        args: { experiment_ref: string },
        context: {
          globalArgs: GlobalArgs;
          readResource: ReadResource;
          logger: Logger;
        },
      ) => {
        const prior = await context.readResource(args.experiment_ref);
        if (!prior) {
          throw new Error(
            `No stored 'experiment' resource named '${args.experiment_ref}'.`,
          );
        }
        const expName = prior.name as string;
        const live = await dbxFetch(
          context.globalArgs,
          `/api/2.0/mlflow/experiments/get-by-name?experiment_name=${
            encodeURIComponent(expName)
          }`,
        );
        context.logger.info(
          "Read MLflow experiment {name}",
          { name: expName },
        );
        return { dataHandles: [], outputs: { live } };
      },
    },

    update: {
      description:
        "POST /api/2.0/mlflow/experiments/update. Only `new_name` is " +
        "supported by the API.",
      arguments: UpdateArgs,
      execute: async (
        args: z.infer<typeof UpdateArgs>,
        context: {
          globalArgs: GlobalArgs;
          readResource: ReadResource;
          writeResource: WriteResource;
          logger: Logger;
        },
      ) => {
        const prior = await context.readResource(args.experiment_ref);
        if (!prior) {
          throw new Error(
            `No stored 'experiment' resource named '${args.experiment_ref}'.`,
          );
        }
        const body: Record<string, unknown> = {
          experiment_id: prior.experiment_id,
        };
        if (args.new_name) body.new_name = args.new_name;
        await dbxFetch(
          context.globalArgs,
          "/api/2.0/mlflow/experiments/update",
          { method: "POST", body: JSON.stringify(body) },
        );
        context.logger.info(
          "Updated MLflow experiment {experiment_id}",
          { experiment_id: prior.experiment_id },
        );
        const handle = await context.writeResource(
          "experiment",
          args.experiment_ref,
          {
            ...prior,
            name: (args.new_name ?? prior.name) as string,
          },
        );
        return { dataHandles: [handle] };
      },
    },

    set_tag: {
      description:
        "POST /api/2.0/mlflow/experiments/set-experiment-tag. Adds or " +
        "overwrites a single tag.",
      arguments: SetTagArgs,
      execute: async (
        args: z.infer<typeof SetTagArgs>,
        context: {
          globalArgs: GlobalArgs;
          readResource: ReadResource;
          logger: Logger;
        },
      ) => {
        const prior = await context.readResource(args.experiment_ref);
        if (!prior) {
          throw new Error(
            `No stored 'experiment' resource named '${args.experiment_ref}'.`,
          );
        }
        await dbxFetch(
          context.globalArgs,
          "/api/2.0/mlflow/experiments/set-experiment-tag",
          {
            method: "POST",
            body: JSON.stringify({
              experiment_id: prior.experiment_id,
              key: args.key,
              value: args.value,
            }),
          },
        );
        context.logger.info(
          "Set tag {key} on experiment {experiment_id}",
          { key: args.key, experiment_id: prior.experiment_id },
        );
        return { dataHandles: [] };
      },
    },

    delete: {
      description:
        "POST /api/2.0/mlflow/experiments/delete. Marks the experiment " +
        "as deleted; can be restored within 30 days.",
      arguments: z.object({ experiment_ref: z.string() }),
      execute: async (
        args: { experiment_ref: string },
        context: {
          globalArgs: GlobalArgs;
          readResource: ReadResource;
          logger: Logger;
        },
      ) => {
        const prior = await context.readResource(args.experiment_ref);
        if (!prior) {
          throw new Error(
            `No stored 'experiment' resource named '${args.experiment_ref}'.`,
          );
        }
        await dbxFetch(
          context.globalArgs,
          "/api/2.0/mlflow/experiments/delete",
          {
            method: "POST",
            body: JSON.stringify({
              experiment_id: prior.experiment_id,
            }),
          },
        );
        context.logger.info(
          "Deleted MLflow experiment {experiment_id}",
          { experiment_id: prior.experiment_id },
        );
        return { dataHandles: [] };
      },
    },

    list: {
      description:
        "POST /api/2.0/mlflow/experiments/search with optional filter. " +
        "Returns up to max_results experiments.",
      arguments: z.object({
        filter: z.string().optional().describe(
          "MLflow filter syntax, e.g. \"tags.team = 'churn'\"",
        ),
        max_results: z.number().int().positive().max(50000).default(100),
        view_type: z.enum(["ACTIVE_ONLY", "DELETED_ONLY", "ALL"]).default(
          "ACTIVE_ONLY",
        ),
      }),
      execute: async (
        args: {
          filter?: string;
          max_results: number;
          view_type: "ACTIVE_ONLY" | "DELETED_ONLY" | "ALL";
        },
        context: { globalArgs: GlobalArgs; logger: Logger },
      ) => {
        const body: Record<string, unknown> = {
          max_results: args.max_results,
          view_type: args.view_type,
        };
        if (args.filter) body.filter = args.filter;
        const res = await dbxFetch(
          context.globalArgs,
          "/api/2.0/mlflow/experiments/search",
          { method: "POST", body: JSON.stringify(body) },
        );
        const experiments = (res.experiments ?? []) as Array<{
          experiment_id: string;
          name: string;
          lifecycle_stage: string;
        }>;
        context.logger.info(
          "Listed {count} MLflow experiments",
          { count: experiments.length },
        );
        return { dataHandles: [], outputs: { experiments } };
      },
    },

    create_or_update: {
      description:
        "Reconcile against the workspace: if an experiment with this " +
        "name exists, store the existing experiment_id locally (no-op " +
        "on the workspace since MLflow only supports renames). Otherwise " +
        "create. Safe across Swamp tombstones.",
      arguments: CreateArgs,
      execute: async (
        args: z.infer<typeof CreateArgs>,
        context: {
          globalArgs: GlobalArgs;
          writeResource: WriteResource;
          logger: Logger;
        },
      ) => {
        const exists = await existsOnWorkspace(
          context.globalArgs,
          `/api/2.0/mlflow/experiments/get-by-name?experiment_name=${
            encodeURIComponent(args.name)
          }`,
        );
        if (exists) {
          const live = await dbxFetch(
            context.globalArgs,
            `/api/2.0/mlflow/experiments/get-by-name?experiment_name=${
              encodeURIComponent(args.name)
            }`,
          );
          const exp = live.experiment as {
            experiment_id: string;
            artifact_location?: string;
            lifecycle_stage: string;
            creation_time?: number;
          };
          context.logger.info(
            "create_or_update: adopted existing experiment {name}",
            { name: args.name },
          );
          const handle = await context.writeResource(
            "experiment",
            pathToResourceName(args.name),
            {
              experiment_id: exp.experiment_id,
              name: args.name,
              artifact_location: exp.artifact_location,
              lifecycle_stage: exp.lifecycle_stage,
              created_time_ms: exp.creation_time ?? Date.now(),
              workspace_url: context.globalArgs.workspace_url,
            },
          );
          return { dataHandles: [handle] };
        }
        const out = await dbxFetch(
          context.globalArgs,
          "/api/2.0/mlflow/experiments/create",
          { method: "POST", body: JSON.stringify(args) },
        );
        const experimentId = out.experiment_id as string;
        context.logger.info(
          "create_or_update: created new experiment {name} -> {experiment_id}",
          { name: args.name, experiment_id: experimentId },
        );
        const handle = await context.writeResource(
          "experiment",
          pathToResourceName(args.name),
          {
            experiment_id: experimentId,
            name: args.name,
            artifact_location: args.artifact_location,
            lifecycle_stage: "active",
            created_time_ms: Date.now(),
            workspace_url: context.globalArgs.workspace_url,
          },
        );
        return { dataHandles: [handle] };
      },
    },
  },
};
