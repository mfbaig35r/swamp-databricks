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

const VolumeType = z.enum(["MANAGED", "EXTERNAL"]);

const CreateArgs = z.object({
  name: z.string().min(1).max(255),
  catalog_name: z.string(),
  schema_name: z.string(),
  volume_type: VolumeType.default("MANAGED"),
  comment: z.string().optional(),
  storage_location: z.string().optional().describe(
    "Required for EXTERNAL volumes (cloud URI).",
  ),
}).refine(
  (v) => v.volume_type !== "EXTERNAL" || !!v.storage_location,
  {
    message: "storage_location is required when volume_type=EXTERNAL",
    path: ["storage_location"],
  },
);

const UpdateArgs = z.object({
  volume_ref: z.string(),
  new_name: z.string().optional(),
  comment: z.string().optional(),
  owner: z.string().optional(),
});

const VolumeResourceSchema = z.object({
  full_name: z.string().describe("<catalog>.<schema>.<volume>"),
  name: z.string(),
  catalog_name: z.string(),
  schema_name: z.string(),
  volume_type: z.string(),
  owner: z.string().optional(),
  created_time_ms: z.number().int(),
  workspace_url: z.string().url(),
});

/**
 * `@mfbaig35r/databricks/uc_volume`: Unity Catalog volume lifecycle.
 * Volumes are UC-governed locations for arbitrary files (CSVs, images,
 * config, etc.). Methods: create, read, update, delete, list.
 *
 * `MANAGED` volumes live in UC-managed storage. `EXTERNAL` volumes
 * point at user-controlled cloud storage via `storage_location`.
 *
 * @see https://docs.databricks.com/api/workspace/volumes
 */
export const model = {
  type: "@mfbaig35r/databricks/uc_volume",
  version: "2026.05.30.16",
  globalArguments: GlobalArgsSchema,

  resources: {
    "volume": {
      description: "A Unity Catalog volume, keyed by user-supplied name.",
      schema: VolumeResourceSchema,
      lifetime: "infinite" as const,
      garbageCollection: 5,
    },
  },

  methods: {
    create: {
      description: "Create a volume via POST /api/2.1/unity-catalog/volumes.",
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
          "/api/2.1/unity-catalog/volumes",
          { method: "POST", body: JSON.stringify(args) },
        );
        const fullName = out.full_name as string;
        context.logger.info(
          "Created volume {full_name}",
          { full_name: fullName },
        );
        const handle = await context.writeResource("volume", args.name, {
          full_name: fullName,
          name: args.name,
          catalog_name: args.catalog_name,
          schema_name: args.schema_name,
          volume_type: args.volume_type,
          owner: out.owner as string | undefined,
          created_time_ms: Date.now(),
          workspace_url: context.globalArgs.workspace_url,
        });
        return { dataHandles: [handle] };
      },
    },

    read: {
      description: "GET /api/2.1/unity-catalog/volumes/{full_name}.",
      arguments: z.object({ volume_ref: z.string() }),
      execute: async (
        args: { volume_ref: string },
        context: {
          globalArgs: GlobalArgs;
          readResource: ReadResource;
          logger: Logger;
        },
      ) => {
        const prior = await context.readResource(args.volume_ref);
        if (!prior) {
          throw new Error(
            `No stored 'volume' resource named '${args.volume_ref}'.`,
          );
        }
        const live = await dbxFetch(
          context.globalArgs,
          `/api/2.1/unity-catalog/volumes/${prior.full_name}`,
        );
        context.logger.info(
          "Read volume {full_name}",
          { full_name: prior.full_name },
        );
        return { dataHandles: [], outputs: { live } };
      },
    },

    update: {
      description: "PATCH /api/2.1/unity-catalog/volumes/{full_name}.",
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
        const prior = await context.readResource(args.volume_ref);
        if (!prior) {
          throw new Error(
            `No stored 'volume' resource named '${args.volume_ref}'.`,
          );
        }
        const patch: Record<string, unknown> = {};
        if (args.new_name) patch.new_name = args.new_name;
        if (args.comment !== undefined) patch.comment = args.comment;
        if (args.owner) patch.owner = args.owner;
        const out = await dbxFetch(
          context.globalArgs,
          `/api/2.1/unity-catalog/volumes/${prior.full_name}`,
          { method: "PATCH", body: JSON.stringify(patch) },
        );
        const fullName = (out.full_name ?? prior.full_name) as string;
        context.logger.info(
          "Updated volume {full_name}",
          { full_name: fullName },
        );
        const handle = await context.writeResource(
          "volume",
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
      description: "DELETE /api/2.1/unity-catalog/volumes/{full_name}.",
      arguments: z.object({ volume_ref: z.string() }),
      execute: async (
        args: { volume_ref: string },
        context: {
          globalArgs: GlobalArgs;
          readResource: ReadResource;
          logger: Logger;
        },
      ) => {
        const prior = await context.readResource(args.volume_ref);
        if (!prior) {
          throw new Error(
            `No stored 'volume' resource named '${args.volume_ref}'.`,
          );
        }
        await dbxFetch(
          context.globalArgs,
          `/api/2.1/unity-catalog/volumes/${prior.full_name}`,
          { method: "DELETE" },
        );
        context.logger.info(
          "Deleted volume {full_name}",
          { full_name: prior.full_name },
        );
        return { dataHandles: [] };
      },
    },

    list: {
      description: "List volumes via GET /api/2.1/unity-catalog/volumes.",
      arguments: z.object({
        catalog_name: z.string(),
        schema_name: z.string(),
        max_results: z.number().int().positive().max(50).optional(),
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
          `/api/2.1/unity-catalog/volumes?${qs}`,
        );
        const volumes = (res.volumes ?? []) as Array<{
          full_name: string;
          name: string;
        }>;
        context.logger.info(
          "Listed {count} volumes in {catalog}.{schema}",
          {
            count: volumes.length,
            catalog: args.catalog_name,
            schema: args.schema_name,
          },
        );
        return { dataHandles: [], outputs: { volumes } };
      },
    },

    create_or_update: {
      description:
        "Reconcile against the workspace: GET the volume first; if it " +
        "exists call PATCH (comment only - UC volume PATCH does not accept " +
        "volume_type / storage_location changes), otherwise POST. Safe " +
        "across Swamp tombstones.",
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
          `/api/2.1/unity-catalog/volumes/${fullName}`,
        );
        if (exists) {
          const patch: Record<string, unknown> = {};
          if (args.comment !== undefined) patch.comment = args.comment;
          await dbxFetch(
            context.globalArgs,
            `/api/2.1/unity-catalog/volumes/${fullName}`,
            { method: "PATCH", body: JSON.stringify(patch) },
          );
          context.logger.info(
            "create_or_update: patched existing volume {full_name}",
            { full_name: fullName },
          );
          const handle = await context.writeResource("volume", args.name, {
            full_name: fullName,
            name: args.name,
            catalog_name: args.catalog_name,
            schema_name: args.schema_name,
            volume_type: args.volume_type,
            created_time_ms: Date.now(),
            workspace_url: context.globalArgs.workspace_url,
          });
          return { dataHandles: [handle] };
        }
        const out = await dbxFetch(
          context.globalArgs,
          "/api/2.1/unity-catalog/volumes",
          { method: "POST", body: JSON.stringify(args) },
        );
        const createdFullName = out.full_name as string;
        context.logger.info(
          "create_or_update: created new volume {full_name}",
          { full_name: createdFullName },
        );
        const handle = await context.writeResource("volume", args.name, {
          full_name: createdFullName,
          name: args.name,
          catalog_name: args.catalog_name,
          schema_name: args.schema_name,
          volume_type: args.volume_type,
          owner: out.owner as string | undefined,
          created_time_ms: Date.now(),
          workspace_url: context.globalArgs.workspace_url,
        });
        return { dataHandles: [handle] };
      },
    },
  },
};
