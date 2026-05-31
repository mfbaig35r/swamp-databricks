import { z } from "npm:zod@4";
import {
  dbxFetch,
  GlobalArgs,
  GlobalArgsSchema,
  Logger,
  WriteResource,
} from "./_lib/databricks.ts";

// ---------------------------------------------------------------------------
// Schemas
// ---------------------------------------------------------------------------

const ObjectType = z.enum([
  "jobs",
  "pipelines",
  "sql/warehouses",
  "clusters",
  "cluster-policies",
  "instance-pools",
  "notebooks",
  "directories",
  "serving-endpoints",
  "experiments",
  "registered-models",
  "tokens",
  "passwords",
  "repos",
  "dashboards",
  "queries",
  "alerts",
  "genie",
  "dbsql-dashboards",
  "apps",
  "vector-search-endpoints",
]);

const AccessControlEntry = z.object({
  user_name: z.string().optional(),
  group_name: z.string().optional(),
  service_principal_name: z.string().optional(),
  permission_level: z.string(),
}).refine(
  (e) =>
    [e.user_name, e.group_name, e.service_principal_name].filter(Boolean)
      .length === 1,
  {
    message:
      "exactly one principal field required (user_name, group_name, or service_principal_name)",
  },
);

const GetArgs = z.object({
  object_type: ObjectType,
  object_id: z.string(),
});

const SetArgs = z.object({
  object_type: ObjectType,
  object_id: z.string(),
  access_control_list: z.array(AccessControlEntry).min(1),
});

const ListLevelsArgs = z.object({
  object_type: ObjectType,
  object_id: z.string().describe(
    "Sample object id; needed because levels are returned per-object",
  ),
});

const PermissionsResourceSchema = z.object({
  object_type: z.string(),
  object_id: z.string(),
  access_control_list: z.array(z.object({}).passthrough()).describe(
    "Last access_control_list applied via set or update",
  ),
  applied_time_ms: z.number().int(),
  workspace_url: z.string().url(),
});

function resourceName(objectType: string, objectId: string): string {
  return `${objectType.replace(/\//g, "-")}:${objectId}`;
}

/**
 * `@mfbaig35r/databricks/workspace_permissions`: workspace-level permissions
 * for jobs, pipelines, warehouses, notebooks, repos, dashboards, queries,
 * alerts, experiments, registered models, serving endpoints, clusters,
 * cluster policies, and instance pools.
 *
 * UC objects (catalogs/schemas/tables/volumes) use a different grant model;
 * see `@mfbaig35r/databricks/uc_permissions`.
 *
 * @see https://docs.databricks.com/api/workspace/permissions
 */
export const model = {
  type: "@mfbaig35r/databricks/workspace_permissions",
  version: "2026.05.30.18",
  globalArguments: GlobalArgsSchema,

  resources: {
    "permissions": {
      description:
        "Last access_control_list applied to a (object_type, object_id) " +
        "pair. set/update write this; get does not.",
      schema: PermissionsResourceSchema,
      lifetime: "infinite" as const,
      garbageCollection: 10,
    },
  },

  methods: {
    get: {
      description:
        "GET /api/2.0/permissions/{object_type}/{object_id}. Returns the " +
        "live ACL in outputs; does not write a resource.",
      arguments: GetArgs,
      execute: async (
        args: z.infer<typeof GetArgs>,
        context: { globalArgs: GlobalArgs; logger: Logger },
      ) => {
        const live = await dbxFetch(
          context.globalArgs,
          `/api/2.0/permissions/${args.object_type}/${args.object_id}`,
        );
        context.logger.info(
          "Read permissions on {object_type}/{object_id}",
          { object_type: args.object_type, object_id: args.object_id },
        );
        return { dataHandles: [], outputs: { live } };
      },
    },

    set: {
      description:
        "PUT /api/2.0/permissions/{object_type}/{object_id}. Full replace " +
        "of the ACL. Writes a permissions resource recording what was applied.",
      arguments: SetArgs,
      execute: async (
        args: z.infer<typeof SetArgs>,
        context: {
          globalArgs: GlobalArgs;
          writeResource: WriteResource;
          logger: Logger;
        },
      ) => {
        await dbxFetch(
          context.globalArgs,
          `/api/2.0/permissions/${args.object_type}/${args.object_id}`,
          {
            method: "PUT",
            body: JSON.stringify({
              access_control_list: args.access_control_list,
            }),
          },
        );
        context.logger.info(
          "Set permissions on {object_type}/{object_id} ({count} entries)",
          {
            object_type: args.object_type,
            object_id: args.object_id,
            count: args.access_control_list.length,
          },
        );
        const handle = await context.writeResource(
          "permissions",
          resourceName(args.object_type, args.object_id),
          {
            object_type: args.object_type,
            object_id: args.object_id,
            access_control_list: args.access_control_list,
            applied_time_ms: Date.now(),
            workspace_url: context.globalArgs.workspace_url,
          },
        );
        return { dataHandles: [handle] };
      },
    },

    update: {
      description:
        "PATCH /api/2.0/permissions/{object_type}/{object_id}. Additive " +
        "update; existing grants not in access_control_list are preserved. " +
        "Writes a permissions resource recording the patch payload.",
      arguments: SetArgs,
      execute: async (
        args: z.infer<typeof SetArgs>,
        context: {
          globalArgs: GlobalArgs;
          writeResource: WriteResource;
          logger: Logger;
        },
      ) => {
        await dbxFetch(
          context.globalArgs,
          `/api/2.0/permissions/${args.object_type}/${args.object_id}`,
          {
            method: "PATCH",
            body: JSON.stringify({
              access_control_list: args.access_control_list,
            }),
          },
        );
        context.logger.info(
          "Patched permissions on {object_type}/{object_id} (+{count} entries)",
          {
            object_type: args.object_type,
            object_id: args.object_id,
            count: args.access_control_list.length,
          },
        );
        const handle = await context.writeResource(
          "permissions",
          resourceName(args.object_type, args.object_id),
          {
            object_type: args.object_type,
            object_id: args.object_id,
            access_control_list: args.access_control_list,
            applied_time_ms: Date.now(),
            workspace_url: context.globalArgs.workspace_url,
          },
        );
        return { dataHandles: [handle] };
      },
    },

    list_levels: {
      description:
        "GET /api/2.0/permissions/{object_type}/{object_id}/permissionLevels. " +
        "Returns the permission levels valid for this object type.",
      arguments: ListLevelsArgs,
      execute: async (
        args: z.infer<typeof ListLevelsArgs>,
        context: { globalArgs: GlobalArgs; logger: Logger },
      ) => {
        const res = await dbxFetch(
          context.globalArgs,
          `/api/2.0/permissions/${args.object_type}/${args.object_id}/permissionLevels`,
        );
        const levels = (res.permission_levels ?? []) as Array<{
          permission_level: string;
          description: string;
        }>;
        context.logger.info(
          "Listed {count} permission levels for {object_type}",
          { count: levels.length, object_type: args.object_type },
        );
        return { dataHandles: [], outputs: { permission_levels: levels } };
      },
    },
  },
};
