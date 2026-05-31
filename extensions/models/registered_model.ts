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
  name: z.string().min(1).max(255).describe("Model name (NOT full_name)"),
  catalog_name: z.string(),
  schema_name: z.string(),
  comment: z.string().optional(),
  storage_location: z.string().optional().describe(
    "External storage URI for the model versions. UC-managed if omitted.",
  ),
});

const UpdateArgs = z.object({
  model_ref: z.string(),
  comment: z.string().optional(),
  owner: z.string().optional(),
});

const ModelResourceSchema = z.object({
  full_name: z.string().describe("<catalog>.<schema>.<name>"),
  name: z.string(),
  catalog_name: z.string(),
  schema_name: z.string(),
  owner: z.string().optional(),
  created_time_ms: z.number().int(),
  workspace_url: z.string().url(),
});

/**
 * `@mfbaig35r/databricks/registered_model`: Unity Catalog registered model
 * lifecycle. Pairs with `model_version` for versioning and
 * `model_serving_endpoint` for deployment.
 *
 * UC Model Registry is the recommended successor to the legacy workspace
 * Model Registry. Use this model, not the workspace registry's API.
 *
 * @see https://docs.databricks.com/api/workspace/registeredmodels
 */
export const model = {
  type: "@mfbaig35r/databricks/registered_model",
  version: "2026.05.30.18",
  globalArguments: GlobalArgsSchema,

  resources: {
    "model": {
      description: "A UC registered model, keyed by user-supplied name.",
      schema: ModelResourceSchema,
      lifetime: "infinite" as const,
      garbageCollection: 5,
    },
  },

  methods: {
    create: {
      description: "POST /api/2.1/unity-catalog/models.",
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
          "/api/2.1/unity-catalog/models",
          { method: "POST", body: JSON.stringify(args) },
        );
        const fullName = out.full_name as string;
        context.logger.info(
          "Created registered model {full_name}",
          { full_name: fullName },
        );
        const handle = await context.writeResource("model", args.name, {
          full_name: fullName,
          name: args.name,
          catalog_name: args.catalog_name,
          schema_name: args.schema_name,
          owner: out.owner as string | undefined,
          created_time_ms: Date.now(),
          workspace_url: context.globalArgs.workspace_url,
        });
        return { dataHandles: [handle] };
      },
    },

    read: {
      description: "GET /api/2.1/unity-catalog/models/{full_name}.",
      arguments: z.object({ model_ref: z.string() }),
      execute: async (
        args: { model_ref: string },
        context: {
          globalArgs: GlobalArgs;
          readResource: ReadResource;
          logger: Logger;
        },
      ) => {
        const prior = await context.readResource(args.model_ref);
        if (!prior) {
          throw new Error(
            `No stored 'model' resource named '${args.model_ref}'.`,
          );
        }
        const live = await dbxFetch(
          context.globalArgs,
          `/api/2.1/unity-catalog/models/${prior.full_name}`,
        );
        context.logger.info(
          "Read registered model {full_name}",
          { full_name: prior.full_name },
        );
        return { dataHandles: [], outputs: { live } };
      },
    },

    update: {
      description: "PATCH /api/2.1/unity-catalog/models/{full_name}.",
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
        const prior = await context.readResource(args.model_ref);
        if (!prior) {
          throw new Error(
            `No stored 'model' resource named '${args.model_ref}'.`,
          );
        }
        const patch: Record<string, unknown> = {};
        if (args.comment !== undefined) patch.comment = args.comment;
        if (args.owner) patch.owner = args.owner;
        await dbxFetch(
          context.globalArgs,
          `/api/2.1/unity-catalog/models/${prior.full_name}`,
          { method: "PATCH", body: JSON.stringify(patch) },
        );
        context.logger.info(
          "Updated registered model {full_name}",
          { full_name: prior.full_name },
        );
        const handle = await context.writeResource(
          "model",
          args.model_ref,
          {
            ...prior,
            owner: (args.owner ?? prior.owner) as string | undefined,
          },
        );
        return { dataHandles: [handle] };
      },
    },

    delete: {
      description: "DELETE /api/2.1/unity-catalog/models/{full_name}.",
      arguments: z.object({ model_ref: z.string() }),
      execute: async (
        args: { model_ref: string },
        context: {
          globalArgs: GlobalArgs;
          readResource: ReadResource;
          logger: Logger;
        },
      ) => {
        const prior = await context.readResource(args.model_ref);
        if (!prior) {
          throw new Error(
            `No stored 'model' resource named '${args.model_ref}'.`,
          );
        }
        await dbxFetch(
          context.globalArgs,
          `/api/2.1/unity-catalog/models/${prior.full_name}`,
          { method: "DELETE" },
        );
        context.logger.info(
          "Deleted registered model {full_name}",
          { full_name: prior.full_name },
        );
        return { dataHandles: [] };
      },
    },

    list: {
      description: "List registered models in a catalog/schema via " +
        "GET /api/2.1/unity-catalog/models.",
      arguments: z.object({
        catalog_name: z.string(),
        schema_name: z.string(),
        max_results: z.number().int().positive().max(1000).optional(),
      }),
      execute: async (
        args: {
          catalog_name: string;
          schema_name: string;
          max_results?: number;
        },
        context: { globalArgs: GlobalArgs; logger: Logger },
      ) => {
        const qs = new URLSearchParams({
          catalog_name: args.catalog_name,
          schema_name: args.schema_name,
        });
        if (args.max_results) qs.set("max_results", String(args.max_results));
        const res = await dbxFetch(
          context.globalArgs,
          `/api/2.1/unity-catalog/models?${qs}`,
        );
        const models = (res.registered_models ?? []) as Array<{
          full_name: string;
          name: string;
        }>;
        context.logger.info(
          "Listed {count} registered models in {catalog}.{schema}",
          {
            count: models.length,
            catalog: args.catalog_name,
            schema: args.schema_name,
          },
        );
        return { dataHandles: [], outputs: { models } };
      },
    },

    create_or_update: {
      description:
        "Reconcile against the workspace: GET first; if exists call " +
        "PATCH (comment only), otherwise POST. Safe across tombstones.",
      arguments: CreateArgs,
      execute: async (
        args: z.infer<typeof CreateArgs>,
        context: {
          globalArgs: GlobalArgs;
          writeResource: WriteResource;
          logger: Logger;
        },
      ) => {
        const fullName =
          `${args.catalog_name}.${args.schema_name}.${args.name}`;
        const exists = await existsOnWorkspace(
          context.globalArgs,
          `/api/2.1/unity-catalog/models/${fullName}`,
        );
        if (exists) {
          if (args.comment !== undefined) {
            await dbxFetch(
              context.globalArgs,
              `/api/2.1/unity-catalog/models/${fullName}`,
              {
                method: "PATCH",
                body: JSON.stringify({ comment: args.comment }),
              },
            );
          }
          context.logger.info(
            "create_or_update: adopted existing model {full_name}",
            { full_name: fullName },
          );
          const handle = await context.writeResource("model", args.name, {
            full_name: fullName,
            name: args.name,
            catalog_name: args.catalog_name,
            schema_name: args.schema_name,
            created_time_ms: Date.now(),
            workspace_url: context.globalArgs.workspace_url,
          });
          return { dataHandles: [handle] };
        }
        const out = await dbxFetch(
          context.globalArgs,
          "/api/2.1/unity-catalog/models",
          { method: "POST", body: JSON.stringify(args) },
        );
        const createdFullName = out.full_name as string;
        context.logger.info(
          "create_or_update: created new model {full_name}",
          { full_name: createdFullName },
        );
        const handle = await context.writeResource("model", args.name, {
          full_name: createdFullName,
          name: args.name,
          catalog_name: args.catalog_name,
          schema_name: args.schema_name,
          owner: out.owner as string | undefined,
          created_time_ms: Date.now(),
          workspace_url: context.globalArgs.workspace_url,
        });
        return { dataHandles: [handle] };
      },
    },
  },
};
