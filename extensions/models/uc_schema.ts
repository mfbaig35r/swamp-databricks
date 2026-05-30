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
  name: z.string().min(1).max(255).describe("Schema name (NOT full_name)"),
  catalog_name: z.string().describe(
    "Parent catalog (e.g. 'workspace' on Free)",
  ),
  comment: z.string().optional(),
  properties: z.record(z.string(), z.string()).optional(),
  storage_root: z.string().optional().describe(
    "External storage root (managed-storage UC volumes path)",
  ),
});

const UpdateArgs = z.object({
  schema_ref: z.string(),
  new_name: z.string().optional(),
  comment: z.string().optional(),
  owner: z.string().optional(),
  properties: z.record(z.string(), z.string()).optional(),
});

const SchemaResourceSchema = z.object({
  full_name: z.string().describe("<catalog>.<schema>"),
  name: z.string(),
  catalog_name: z.string(),
  owner: z.string().optional(),
  created_time_ms: z.number().int(),
  workspace_url: z.string().url(),
});

/**
 * `@mfbaig35r/databricks/uc_schema`: Unity Catalog schema (database) lifecycle.
 * Methods cover create, read, update, delete, list (by catalog).
 *
 * On Databricks Free Edition the default catalog is `workspace`; pair this
 * model with workflows that ingest into a target schema you control.
 *
 * @see https://docs.databricks.com/api/workspace/schemas
 */
export const model = {
  type: "@mfbaig35r/databricks/uc_schema",
  version: "2026.05.30.11",
  globalArguments: GlobalArgsSchema,

  resources: {
    "schema": {
      description: "A Unity Catalog schema, keyed by user-supplied name.",
      schema: SchemaResourceSchema,
      lifetime: "infinite" as const,
      garbageCollection: 5,
    },
  },

  methods: {
    create: {
      description: "Create a schema via POST /api/2.1/unity-catalog/schemas.",
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
          "/api/2.1/unity-catalog/schemas",
          { method: "POST", body: JSON.stringify(args) },
        );
        const fullName = out.full_name as string;
        context.logger.info(
          "Created schema {full_name}",
          { full_name: fullName },
        );
        const handle = await context.writeResource("schema", args.name, {
          full_name: fullName,
          name: args.name,
          catalog_name: args.catalog_name,
          owner: out.owner as string | undefined,
          created_time_ms: Date.now(),
          workspace_url: context.globalArgs.workspace_url,
        });
        return { dataHandles: [handle] };
      },
    },

    read: {
      description: "GET /api/2.1/unity-catalog/schemas/{full_name}.",
      arguments: z.object({ schema_ref: z.string() }),
      execute: async (
        args: { schema_ref: string },
        context: {
          globalArgs: GlobalArgs;
          readResource: ReadResource;
          logger: Logger;
        },
      ) => {
        const prior = await context.readResource(args.schema_ref);
        if (!prior) {
          throw new Error(
            `No stored 'schema' resource named '${args.schema_ref}'.`,
          );
        }
        const live = await dbxFetch(
          context.globalArgs,
          `/api/2.1/unity-catalog/schemas/${prior.full_name}`,
        );
        context.logger.info(
          "Read schema {full_name}",
          { full_name: prior.full_name },
        );
        return { dataHandles: [], outputs: { live } };
      },
    },

    update: {
      description:
        "Update schema via PATCH /api/2.1/unity-catalog/schemas/{full_name}.",
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
        const prior = await context.readResource(args.schema_ref);
        if (!prior) {
          throw new Error(
            `No stored 'schema' resource named '${args.schema_ref}'.`,
          );
        }
        const patch: Record<string, unknown> = {};
        if (args.new_name) patch.new_name = args.new_name;
        if (args.comment !== undefined) patch.comment = args.comment;
        if (args.owner) patch.owner = args.owner;
        if (args.properties) patch.properties = args.properties;
        const out = await dbxFetch(
          context.globalArgs,
          `/api/2.1/unity-catalog/schemas/${prior.full_name}`,
          { method: "PATCH", body: JSON.stringify(patch) },
        );
        const fullName = (out.full_name ?? prior.full_name) as string;
        context.logger.info(
          "Updated schema {full_name}",
          { full_name: fullName },
        );
        const handle = await context.writeResource(
          "schema",
          (args.new_name ?? prior.name) as string,
          {
            ...prior,
            full_name: fullName,
            name: (args.new_name ?? prior.name) as string,
            owner: (args.owner ?? prior.owner) as string | undefined,
          },
        );
        return { dataHandles: [handle] };
      },
    },

    delete: {
      description: "DELETE /api/2.1/unity-catalog/schemas/{full_name}. " +
        "Schema must be empty (no tables/volumes) unless force=true.",
      arguments: z.object({
        schema_ref: z.string(),
        force: z.boolean().default(false),
      }),
      execute: async (
        args: { schema_ref: string; force: boolean },
        context: {
          globalArgs: GlobalArgs;
          readResource: ReadResource;
          logger: Logger;
        },
      ) => {
        const prior = await context.readResource(args.schema_ref);
        if (!prior) {
          throw new Error(
            `No stored 'schema' resource named '${args.schema_ref}'.`,
          );
        }
        const qs = args.force ? "?force=true" : "";
        await dbxFetch(
          context.globalArgs,
          `/api/2.1/unity-catalog/schemas/${prior.full_name}${qs}`,
          { method: "DELETE" },
        );
        context.logger.info(
          "Deleted schema {full_name}",
          { full_name: prior.full_name },
        );
        return { dataHandles: [] };
      },
    },

    list: {
      description:
        "List schemas in a catalog via GET /api/2.1/unity-catalog/schemas?catalog_name=...",
      arguments: z.object({ catalog_name: z.string() }),
      execute: async (
        args: { catalog_name: string },
        context: { globalArgs: GlobalArgs; logger: Logger },
      ) => {
        const res = await dbxFetch(
          context.globalArgs,
          `/api/2.1/unity-catalog/schemas?catalog_name=${
            encodeURIComponent(args.catalog_name)
          }`,
        );
        const schemas = (res.schemas ?? []) as Array<{
          full_name: string;
          name: string;
        }>;
        context.logger.info(
          "Listed {count} schemas in catalog {catalog}",
          { count: schemas.length, catalog: args.catalog_name },
        );
        return { dataHandles: [], outputs: { schemas } };
      },
    },

    create_or_update: {
      description:
        "Reconcile against the workspace: GET the schema first; if it " +
        "exists call PATCH, otherwise POST. Safe across Swamp tombstones " +
        "(delete + create_or_update with the same name will correctly " +
        "create rather than try to PATCH a missing schema).",
      arguments: CreateArgs,
      execute: async (
        args: z.infer<typeof CreateArgs>,
        context: {
          globalArgs: GlobalArgs;
          writeResource: WriteResource;
          logger: Logger;
        },
      ) => {
        const fullName = `${args.catalog_name}.${args.name}`;
        const exists = await existsOnWorkspace(
          context.globalArgs,
          `/api/2.1/unity-catalog/schemas/${fullName}`,
        );
        if (exists) {
          const patch: Record<string, unknown> = {};
          if (args.comment !== undefined) patch.comment = args.comment;
          if (args.properties) patch.properties = args.properties;
          await dbxFetch(
            context.globalArgs,
            `/api/2.1/unity-catalog/schemas/${fullName}`,
            { method: "PATCH", body: JSON.stringify(patch) },
          );
          context.logger.info(
            "create_or_update: patched existing schema {full_name}",
            { full_name: fullName },
          );
          const handle = await context.writeResource("schema", args.name, {
            full_name: fullName,
            name: args.name,
            catalog_name: args.catalog_name,
            created_time_ms: Date.now(),
            workspace_url: context.globalArgs.workspace_url,
          });
          return { dataHandles: [handle] };
        }
        const out = await dbxFetch(
          context.globalArgs,
          "/api/2.1/unity-catalog/schemas",
          { method: "POST", body: JSON.stringify(args) },
        );
        const createdFullName = out.full_name as string;
        context.logger.info(
          "create_or_update: created new schema {full_name}",
          { full_name: createdFullName },
        );
        const handle = await context.writeResource("schema", args.name, {
          full_name: createdFullName,
          name: args.name,
          catalog_name: args.catalog_name,
          owner: out.owner as string | undefined,
          created_time_ms: Date.now(),
          workspace_url: context.globalArgs.workspace_url,
        });
        return { dataHandles: [handle] };
      },
    },
  },
};
