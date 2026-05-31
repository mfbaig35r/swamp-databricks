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

const SecurableType = z.enum([
  "catalog",
  "schema",
  "table",
  "volume",
  "function",
  "external_location",
  "storage_credential",
  "metastore",
  "connection",
  "provider",
  "share",
  "recipient",
  "clean_room",
  "model",
  "service_credential",
]);

const Change = z.object({
  principal: z.string().describe(
    "User email, group name (e.g. 'account users'), or service principal app_id",
  ),
  add: z.array(z.string()).optional().describe(
    "Privileges to grant (e.g. SELECT, MODIFY, USE_CATALOG, CREATE_TABLE)",
  ),
  remove: z.array(z.string()).optional().describe(
    "Privileges to revoke",
  ),
}).refine(
  (c) => (c.add?.length ?? 0) + (c.remove?.length ?? 0) > 0,
  { message: "each change must include at least one add or remove privilege" },
);

const GetArgs = z.object({
  securable_type: SecurableType,
  full_name: z.string().describe(
    "Securable identifier: '<catalog>' for catalog, '<catalog>.<schema>' for schema, '<catalog>.<schema>.<table>' for table/volume/function",
  ),
});

const UpdateArgs = z.object({
  securable_type: SecurableType,
  full_name: z.string(),
  changes: z.array(Change).min(1),
});

const PermissionsResourceSchema = z.object({
  securable_type: z.string(),
  full_name: z.string(),
  changes_applied: z.array(z.object({}).passthrough()),
  applied_time_ms: z.number().int(),
  workspace_url: z.string().url(),
});

function resourceName(securableType: string, fullName: string): string {
  return `${securableType}:${fullName.replace(/\./g, ":")}`;
}

/**
 * `@mfbaig35r/databricks/uc_permissions`: Unity Catalog grants on catalogs,
 * schemas, tables, volumes, functions, external locations, storage
 * credentials, models, and related securables.
 *
 * UC permissions use a changes-style PATCH (add/remove privileges per
 * principal) rather than the full-replace ACL model used for workspace
 * permissions. Use `@mfbaig35r/databricks/workspace_permissions` for
 * jobs, pipelines, warehouses, notebooks, etc.
 *
 * @see https://docs.databricks.com/api/workspace/grants
 */
export const model = {
  type: "@mfbaig35r/databricks/uc_permissions",
  version: "2026.05.30.12",
  globalArguments: GlobalArgsSchema,

  resources: {
    "uc_permissions": {
      description:
        "Last set of changes applied to a UC securable. update writes this; " +
        "get does not.",
      schema: PermissionsResourceSchema,
      lifetime: "infinite" as const,
      garbageCollection: 10,
    },
  },

  methods: {
    get: {
      description:
        "GET /api/2.1/unity-catalog/permissions/{securable_type}/{full_name}. " +
        "Returns direct grants on the securable; does not write a resource.",
      arguments: GetArgs,
      execute: async (
        args: z.infer<typeof GetArgs>,
        context: { globalArgs: GlobalArgs; logger: Logger },
      ) => {
        const live = await dbxFetch(
          context.globalArgs,
          `/api/2.1/unity-catalog/permissions/${args.securable_type}/${args.full_name}`,
        );
        context.logger.info(
          "Read UC permissions on {securable_type} {full_name}",
          {
            securable_type: args.securable_type,
            full_name: args.full_name,
          },
        );
        return { dataHandles: [], outputs: { live } };
      },
    },

    get_effective: {
      description:
        "GET /api/2.1/unity-catalog/effective-permissions/{securable_type}/{full_name}. " +
        "Returns effective grants (direct + inherited from parent securables).",
      arguments: GetArgs,
      execute: async (
        args: z.infer<typeof GetArgs>,
        context: { globalArgs: GlobalArgs; logger: Logger },
      ) => {
        const live = await dbxFetch(
          context.globalArgs,
          `/api/2.1/unity-catalog/effective-permissions/${args.securable_type}/${args.full_name}`,
        );
        context.logger.info(
          "Read effective UC permissions on {securable_type} {full_name}",
          {
            securable_type: args.securable_type,
            full_name: args.full_name,
          },
        );
        return { dataHandles: [], outputs: { live } };
      },
    },

    update: {
      description:
        "PATCH /api/2.1/unity-catalog/permissions/{securable_type}/{full_name}. " +
        "Apply changes (add/remove privileges per principal). Writes a " +
        "uc_permissions resource recording what was applied.",
      arguments: UpdateArgs,
      execute: async (
        args: z.infer<typeof UpdateArgs>,
        context: {
          globalArgs: GlobalArgs;
          writeResource: WriteResource;
          logger: Logger;
        },
      ) => {
        await dbxFetch(
          context.globalArgs,
          `/api/2.1/unity-catalog/permissions/${args.securable_type}/${args.full_name}`,
          {
            method: "PATCH",
            body: JSON.stringify({ changes: args.changes }),
          },
        );
        context.logger.info(
          "Applied {count} changes to UC permissions on {securable_type} {full_name}",
          {
            count: args.changes.length,
            securable_type: args.securable_type,
            full_name: args.full_name,
          },
        );
        const handle = await context.writeResource(
          "uc_permissions",
          resourceName(args.securable_type, args.full_name),
          {
            securable_type: args.securable_type,
            full_name: args.full_name,
            changes_applied: args.changes,
            applied_time_ms: Date.now(),
            workspace_url: context.globalArgs.workspace_url,
          },
        );
        return { dataHandles: [handle] };
      },
    },
  },
};
