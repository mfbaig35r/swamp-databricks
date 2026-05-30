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
  name: z.string().min(1).max(255),
  comment: z.string().optional(),
  properties: z.record(z.string(), z.string()).optional(),
  storage_root: z.string().optional().describe(
    "Managed-storage URI for the catalog (s3://, abfss://, gs://).",
  ),
});

const UpdateArgs = z.object({
  catalog_ref: z.string(),
  new_name: z.string().optional(),
  comment: z.string().optional(),
  owner: z.string().optional(),
  properties: z.record(z.string(), z.string()).optional(),
});

const CatalogResourceSchema = z.object({
  name: z.string(),
  comment: z.string().optional(),
  owner: z.string().optional(),
  created_time_ms: z.number().int(),
  workspace_url: z.string().url(),
});

/**
 * `@mfbaig35r/databricks/uc_catalog`: Unity Catalog catalog lifecycle.
 * Top-level UC container. Pair with `uc_schema` / `uc_table` / `uc_volume`
 * to manage a full UC tree from Swamp.
 *
 * On Databricks Free Edition the workspace ships with a default `workspace`
 * catalog. Quota for additional catalogs on Free is small (often 1).
 *
 * @see https://docs.databricks.com/api/workspace/catalogs
 */
export const model = {
  type: "@mfbaig35r/databricks/uc_catalog",
  version: "2026.05.30.10",
  globalArguments: GlobalArgsSchema,

  resources: {
    "catalog": {
      description: "A Unity Catalog catalog, keyed by name.",
      schema: CatalogResourceSchema,
      lifetime: "infinite" as const,
      garbageCollection: 5,
    },
  },

  methods: {
    create: {
      description: "Create a catalog via POST /api/2.1/unity-catalog/catalogs.",
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
          "/api/2.1/unity-catalog/catalogs",
          { method: "POST", body: JSON.stringify(args) },
        );
        context.logger.info("Created catalog {name}", { name: args.name });
        const handle = await context.writeResource("catalog", args.name, {
          name: args.name,
          comment: args.comment,
          owner: out.owner as string | undefined,
          created_time_ms: Date.now(),
          workspace_url: context.globalArgs.workspace_url,
        });
        return { dataHandles: [handle] };
      },
    },

    read: {
      description: "GET /api/2.1/unity-catalog/catalogs/{name}.",
      arguments: z.object({ catalog_ref: z.string() }),
      execute: async (
        args: { catalog_ref: string },
        context: {
          globalArgs: GlobalArgs;
          readResource: ReadResource;
          logger: Logger;
        },
      ) => {
        const prior = await context.readResource(args.catalog_ref);
        if (!prior) {
          throw new Error(
            `No stored 'catalog' resource named '${args.catalog_ref}'.`,
          );
        }
        const live = await dbxFetch(
          context.globalArgs,
          `/api/2.1/unity-catalog/catalogs/${prior.name}`,
        );
        context.logger.info("Read catalog {name}", { name: prior.name });
        return { dataHandles: [], outputs: { live } };
      },
    },

    update: {
      description: "PATCH /api/2.1/unity-catalog/catalogs/{name}.",
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
        const prior = await context.readResource(args.catalog_ref);
        if (!prior) {
          throw new Error(
            `No stored 'catalog' resource named '${args.catalog_ref}'.`,
          );
        }
        const patch: Record<string, unknown> = {};
        if (args.new_name) patch.new_name = args.new_name;
        if (args.comment !== undefined) patch.comment = args.comment;
        if (args.owner) patch.owner = args.owner;
        if (args.properties) patch.properties = args.properties;
        await dbxFetch(
          context.globalArgs,
          `/api/2.1/unity-catalog/catalogs/${prior.name}`,
          { method: "PATCH", body: JSON.stringify(patch) },
        );
        context.logger.info("Updated catalog {name}", { name: prior.name });
        const handle = await context.writeResource(
          "catalog",
          (args.new_name ?? prior.name) as string,
          {
            ...prior,
            name: (args.new_name ?? prior.name) as string,
            comment: args.comment !== undefined
              ? args.comment
              : prior.comment as string | undefined,
            owner: (args.owner ?? prior.owner) as string | undefined,
          },
        );
        return { dataHandles: [handle] };
      },
    },

    delete: {
      description: "DELETE /api/2.1/unity-catalog/catalogs/{name}. " +
        "Catalog must be empty (no schemas) unless force=true.",
      arguments: z.object({
        catalog_ref: z.string(),
        force: z.boolean().default(false),
      }),
      execute: async (
        args: { catalog_ref: string; force: boolean },
        context: {
          globalArgs: GlobalArgs;
          readResource: ReadResource;
          logger: Logger;
        },
      ) => {
        const prior = await context.readResource(args.catalog_ref);
        if (!prior) {
          throw new Error(
            `No stored 'catalog' resource named '${args.catalog_ref}'.`,
          );
        }
        const qs = args.force ? "?force=true" : "";
        await dbxFetch(
          context.globalArgs,
          `/api/2.1/unity-catalog/catalogs/${prior.name}${qs}`,
          { method: "DELETE" },
        );
        context.logger.info("Deleted catalog {name}", { name: prior.name });
        return { dataHandles: [] };
      },
    },

    list: {
      description: "List all catalogs via GET /api/2.1/unity-catalog/catalogs.",
      arguments: z.object({}),
      execute: async (
        _args: Record<string, never>,
        context: { globalArgs: GlobalArgs; logger: Logger },
      ) => {
        const res = await dbxFetch(
          context.globalArgs,
          "/api/2.1/unity-catalog/catalogs",
        );
        const catalogs = (res.catalogs ?? []) as Array<{
          name: string;
          comment?: string;
        }>;
        context.logger.info(
          "Listed {count} catalogs",
          { count: catalogs.length },
        );
        return { dataHandles: [], outputs: { catalogs } };
      },
    },

    create_or_update: {
      description:
        "Reconcile: if a 'catalog' resource named args.name exists in " +
        "Swamp's data layer, call PATCH; otherwise call POST. Returns the " +
        "same resource shape as create.",
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
        const prior = await context.readResource(args.name);
        if (prior) {
          const patch: Record<string, unknown> = {};
          if (args.comment !== undefined) patch.comment = args.comment;
          if (args.properties) patch.properties = args.properties;
          await dbxFetch(
            context.globalArgs,
            `/api/2.1/unity-catalog/catalogs/${prior.name}`,
            { method: "PATCH", body: JSON.stringify(patch) },
          );
          context.logger.info(
            "create_or_update: patched existing catalog {name}",
            { name: args.name },
          );
          const handle = await context.writeResource("catalog", args.name, {
            ...prior,
            comment: args.comment !== undefined
              ? args.comment
              : prior.comment as string | undefined,
          });
          return { dataHandles: [handle] };
        }
        const out = await dbxFetch(
          context.globalArgs,
          "/api/2.1/unity-catalog/catalogs",
          { method: "POST", body: JSON.stringify(args) },
        );
        context.logger.info(
          "create_or_update: created new catalog {name}",
          { name: args.name },
        );
        const handle = await context.writeResource("catalog", args.name, {
          name: args.name,
          comment: args.comment,
          owner: out.owner as string | undefined,
          created_time_ms: Date.now(),
          workspace_url: context.globalArgs.workspace_url,
        });
        return { dataHandles: [handle] };
      },
    },
  },
};
