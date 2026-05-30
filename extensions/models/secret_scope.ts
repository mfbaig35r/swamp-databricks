import { z } from "npm:zod@4";
import {
  dbxFetch,
  GlobalArgs,
  GlobalArgsSchema,
  Logger,
  ReadResource,
  WriteResource,
} from "./_lib/databricks.ts";

const ScopeSettings = z.object({
  scope: z.string().min(1).max(128).regex(
    /^[a-zA-Z0-9_-]+$/,
    "scope name must match [a-zA-Z0-9_-]+",
  ),
  initial_manage_principal: z.string().optional().describe(
    "Principal granted MANAGE on the scope (e.g. 'users'). " +
      "Omit for the calling user only.",
  ),
  scope_backend_type: z.enum(["DATABRICKS", "AZURE_KEYVAULT"]).optional()
    .describe(
      "DATABRICKS = workspace-managed (default). AZURE_KEYVAULT only on Azure.",
    ),
});

const ScopeResourceSchema = z.object({
  scope: z.string(),
  backend_type: z.string(),
  created_time_ms: z.number().int(),
  workspace_url: z.string().url(),
});

/**
 * `@mfbaig35r/databricks/secret_scope`: workspace secret scopes (Databricks
 * Secrets API, not the same as Swamp vaults). Use Swamp vaults for secrets
 * the Swamp models themselves consume (PATs, etc.); use this model for
 * secrets that Databricks runtime code reads via `dbutils.secrets.get()`.
 *
 * @see https://docs.databricks.com/api/workspace/secrets
 */
export const model = {
  type: "@mfbaig35r/databricks/secret_scope",
  version: "2026.05.30.10",
  globalArguments: GlobalArgsSchema,

  resources: {
    "scope": {
      description: "A workspace secret scope, keyed by scope name.",
      schema: ScopeResourceSchema,
      lifetime: "infinite" as const,
      garbageCollection: 5,
    },
  },

  methods: {
    create: {
      description:
        "Create a secret scope via POST /api/2.0/secrets/scopes/create.",
      arguments: ScopeSettings,
      execute: async (
        args: z.infer<typeof ScopeSettings>,
        context: {
          globalArgs: GlobalArgs;
          writeResource: WriteResource;
          logger: Logger;
        },
      ) => {
        await dbxFetch(
          context.globalArgs,
          "/api/2.0/secrets/scopes/create",
          { method: "POST", body: JSON.stringify(args) },
        );
        context.logger.info("Created secret scope {scope}", {
          scope: args.scope,
        });
        const handle = await context.writeResource("scope", args.scope, {
          scope: args.scope,
          backend_type: args.scope_backend_type ?? "DATABRICKS",
          created_time_ms: Date.now(),
          workspace_url: context.globalArgs.workspace_url,
        });
        return { dataHandles: [handle] };
      },
    },

    list: {
      description: "List all secret scopes on the workspace via " +
        "GET /api/2.0/secrets/scopes/list.",
      arguments: z.object({}),
      execute: async (
        _args: Record<string, never>,
        context: { globalArgs: GlobalArgs; logger: Logger },
      ) => {
        const res = await dbxFetch(
          context.globalArgs,
          "/api/2.0/secrets/scopes/list",
        );
        const scopes = (res.scopes ?? []) as Array<{
          name: string;
          backend_type: string;
        }>;
        context.logger.info(
          "Listed {count} secret scopes",
          { count: scopes.length },
        );
        return { dataHandles: [], outputs: { scopes } };
      },
    },

    create_or_update: {
      description:
        "Reconcile: if a 'scope' resource named args.scope exists in " +
        "Swamp's data layer, no-op (Databricks scopes do not support edit). " +
        "Otherwise create. Useful for idempotent workflows.",
      arguments: ScopeSettings,
      execute: async (
        args: z.infer<typeof ScopeSettings>,
        context: {
          globalArgs: GlobalArgs;
          readResource: ReadResource;
          writeResource: WriteResource;
          logger: Logger;
        },
      ) => {
        const prior = await context.readResource(args.scope);
        if (prior) {
          context.logger.info(
            "create_or_update: secret scope {scope} already exists, no-op",
            { scope: args.scope },
          );
          const handle = await context.writeResource("scope", args.scope, {
            ...prior,
          });
          return { dataHandles: [handle] };
        }
        await dbxFetch(
          context.globalArgs,
          "/api/2.0/secrets/scopes/create",
          { method: "POST", body: JSON.stringify(args) },
        );
        context.logger.info(
          "create_or_update: created new secret scope {scope}",
          { scope: args.scope },
        );
        const handle = await context.writeResource("scope", args.scope, {
          scope: args.scope,
          backend_type: args.scope_backend_type ?? "DATABRICKS",
          created_time_ms: Date.now(),
          workspace_url: context.globalArgs.workspace_url,
        });
        return { dataHandles: [handle] };
      },
    },

    delete: {
      description:
        "Delete a secret scope via POST /api/2.0/secrets/scopes/delete. " +
        "All secrets in the scope are also deleted.",
      arguments: z.object({ scope_ref: z.string() }),
      execute: async (
        args: { scope_ref: string },
        context: {
          globalArgs: GlobalArgs;
          readResource: ReadResource;
          logger: Logger;
        },
      ) => {
        const prior = await context.readResource(args.scope_ref);
        if (!prior) {
          throw new Error(
            `No stored 'scope' resource named '${args.scope_ref}'.`,
          );
        }
        const scopeName = prior.scope as string;
        await dbxFetch(
          context.globalArgs,
          "/api/2.0/secrets/scopes/delete",
          { method: "POST", body: JSON.stringify({ scope: scopeName }) },
        );
        context.logger.info(
          "Deleted secret scope {scope}",
          { scope: scopeName },
        );
        return { dataHandles: [] };
      },
    },
  },
};
