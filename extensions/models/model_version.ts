import { z } from "npm:zod@4";
import {
  dbxFetch,
  GlobalArgs,
  GlobalArgsSchema,
  Logger,
  ReadResource,
  WriteResource,
} from "./_lib/databricks.ts";

const CreateArgs = z.object({
  model_ref: z.string().describe(
    "Name of the registered_model Swamp resource (uses its full_name)",
  ),
  source: z.string().min(1).describe(
    "URI of the MLflow run's model artifact, " +
      "e.g. 'runs:/<run_id>/model' or a UC volume path",
  ),
  run_id: z.string().optional().describe(
    "Originating MLflow run ID. Recommended for lineage.",
  ),
  comment: z.string().optional(),
});

const UpdateAliasArgs = z.object({
  model_ref: z.string(),
  version: z.number().int().positive(),
  alias: z.string().min(1).max(255).describe(
    "Alias to set, e.g. 'production', 'staging', 'champion'",
  ),
});

const VersionResourceSchema = z.object({
  full_name: z.string(),
  version_number: z.number().int(),
  status: z.string(),
  run_id: z.string().optional(),
  source: z.string(),
  created_time_ms: z.number().int(),
  workspace_url: z.string().url(),
});

/**
 * `@mfbaig35r/databricks/model_version`: UC Model Registry version
 * lifecycle. Pairs with `registered_model` (the parent) and
 * `model_serving_endpoint` (deployment target).
 *
 * Versions are created from MLflow run artifacts (source URI). Use
 * aliases (`production`, `staging`, `champion`, etc.) for stage
 * transitions; UC model versions don't have the old workspace registry's
 * `stage` field.
 *
 * @see https://docs.databricks.com/api/workspace/modelversions
 */
export const model = {
  type: "@mfbaig35r/databricks/model_version",
  version: "2026.05.30.18",
  globalArguments: GlobalArgsSchema,

  resources: {
    "version": {
      description: "A registered_model version. Resource name is " +
        "'<catalog>:<schema>:<model>:<version>' for Swamp safety.",
      schema: VersionResourceSchema,
      lifetime: "infinite" as const,
      garbageCollection: 20,
    },
  },

  methods: {
    create: {
      description: "POST /api/2.1/unity-catalog/models/{full_name}/versions. " +
        "Registers a new version from an MLflow run's model artifact.",
      arguments: CreateArgs,
      execute: async (
        args: z.infer<typeof CreateArgs>,
        context: {
          globalArgs: GlobalArgs;
          readResource: ReadResource;
          writeResource: WriteResource;
          logger: Logger;
        },
      ) => {
        const modelRecord = await context.readResource(args.model_ref);
        if (!modelRecord) {
          throw new Error(
            `No stored 'model' resource named '${args.model_ref}'. ` +
              `Call registered_model.create or create_or_update first.`,
          );
        }
        const fullName = modelRecord.full_name as string;
        const body: Record<string, unknown> = {
          source: args.source,
        };
        if (args.run_id) body.run_id = args.run_id;
        if (args.comment) body.comment = args.comment;
        const out = await dbxFetch(
          context.globalArgs,
          `/api/2.1/unity-catalog/models/${fullName}/versions`,
          { method: "POST", body: JSON.stringify(body) },
        );
        const versionNumber = out.version as number;
        const status = out.status as string;
        context.logger.info(
          "Registered version {version} of model {full_name}",
          { version: versionNumber, full_name: fullName },
        );
        const handle = await context.writeResource(
          "version",
          `${fullName.replace(/\./g, ":")}:${versionNumber}`,
          {
            full_name: fullName,
            version_number: versionNumber,
            status,
            run_id: args.run_id,
            source: args.source,
            created_time_ms: Date.now(),
            workspace_url: context.globalArgs.workspace_url,
          },
        );
        return { dataHandles: [handle] };
      },
    },

    read: {
      description:
        "GET /api/2.1/unity-catalog/models/{full_name}/versions/{version}.",
      arguments: z.object({
        model_ref: z.string(),
        version: z.number().int().positive(),
      }),
      execute: async (
        args: { model_ref: string; version: number },
        context: {
          globalArgs: GlobalArgs;
          readResource: ReadResource;
          logger: Logger;
        },
      ) => {
        const modelRecord = await context.readResource(args.model_ref);
        if (!modelRecord) {
          throw new Error(
            `No stored 'model' resource named '${args.model_ref}'.`,
          );
        }
        const fullName = modelRecord.full_name as string;
        const live = await dbxFetch(
          context.globalArgs,
          `/api/2.1/unity-catalog/models/${fullName}/versions/${args.version}`,
        );
        context.logger.info(
          "Read model version {full_name}@{version}",
          { full_name: fullName, version: args.version },
        );
        return { dataHandles: [], outputs: { live } };
      },
    },

    update_alias: {
      description:
        "PUT /api/2.1/unity-catalog/models/{full_name}/aliases/{alias}. " +
        "Points an alias to a version. Use for stage transitions " +
        "(production, staging, champion, challenger).",
      arguments: UpdateAliasArgs,
      execute: async (
        args: z.infer<typeof UpdateAliasArgs>,
        context: {
          globalArgs: GlobalArgs;
          readResource: ReadResource;
          logger: Logger;
        },
      ) => {
        const modelRecord = await context.readResource(args.model_ref);
        if (!modelRecord) {
          throw new Error(
            `No stored 'model' resource named '${args.model_ref}'.`,
          );
        }
        const fullName = modelRecord.full_name as string;
        await dbxFetch(
          context.globalArgs,
          `/api/2.1/unity-catalog/models/${fullName}/aliases/${args.alias}`,
          {
            method: "PUT",
            body: JSON.stringify({ version_num: args.version }),
          },
        );
        context.logger.info(
          "Set alias {alias} on {full_name}@{version}",
          {
            alias: args.alias,
            full_name: fullName,
            version: args.version,
          },
        );
        return { dataHandles: [] };
      },
    },

    delete: {
      description:
        "DELETE /api/2.1/unity-catalog/models/{full_name}/versions/{version}.",
      arguments: z.object({
        model_ref: z.string(),
        version: z.number().int().positive(),
      }),
      execute: async (
        args: { model_ref: string; version: number },
        context: {
          globalArgs: GlobalArgs;
          readResource: ReadResource;
          logger: Logger;
        },
      ) => {
        const modelRecord = await context.readResource(args.model_ref);
        if (!modelRecord) {
          throw new Error(
            `No stored 'model' resource named '${args.model_ref}'.`,
          );
        }
        const fullName = modelRecord.full_name as string;
        await dbxFetch(
          context.globalArgs,
          `/api/2.1/unity-catalog/models/${fullName}/versions/${args.version}`,
          { method: "DELETE" },
        );
        context.logger.info(
          "Deleted model version {full_name}@{version}",
          { full_name: fullName, version: args.version },
        );
        return { dataHandles: [] };
      },
    },

    list: {
      description: "List all versions of a registered model via " +
        "GET /api/2.1/unity-catalog/models/{full_name}/versions.",
      arguments: z.object({
        model_ref: z.string(),
        max_results: z.number().int().positive().max(1000).optional(),
      }),
      execute: async (
        args: { model_ref: string; max_results?: number },
        context: {
          globalArgs: GlobalArgs;
          readResource: ReadResource;
          logger: Logger;
        },
      ) => {
        const modelRecord = await context.readResource(args.model_ref);
        if (!modelRecord) {
          throw new Error(
            `No stored 'model' resource named '${args.model_ref}'.`,
          );
        }
        const fullName = modelRecord.full_name as string;
        const qs = new URLSearchParams();
        if (args.max_results) qs.set("max_results", String(args.max_results));
        const res = await dbxFetch(
          context.globalArgs,
          `/api/2.1/unity-catalog/models/${fullName}/versions${
            qs.toString() ? "?" + qs : ""
          }`,
        );
        const versions = (res.model_versions ?? []) as Array<{
          version: number;
          status: string;
          run_id?: string;
        }>;
        context.logger.info(
          "Listed {count} versions of {full_name}",
          { count: versions.length, full_name: fullName },
        );
        return { dataHandles: [], outputs: { versions } };
      },
    },
  },
};
