import { z } from "npm:zod@4";
import {
  dbxFetch,
  GlobalArgs,
  GlobalArgsSchema,
  Logger,
  WriteResource,
} from "./_lib/databricks.ts";

const PutArgs = z.object({
  scope: z.string(),
  key: z.string().min(1).max(128),
  string_value: z.string().min(1).describe(
    "The secret value. Pass via CEL vault.get to avoid surfacing the literal: " +
      '${{ vault.get("local-vault", "my-key") }}',
  ),
});

const DeleteArgs = z.object({
  scope: z.string(),
  key: z.string(),
});

const ListArgs = z.object({
  scope: z.string(),
});

const SecretMetaResourceSchema = z.object({
  scope: z.string(),
  key: z.string(),
  last_updated_ms: z.number().int(),
  workspace_url: z.string().url(),
});

/**
 * `@mfbaig35r/databricks/secret`: put/delete/list keys inside a Databricks
 * workspace secret scope. The Secrets API has no read-value endpoint by
 * design; values are only read by runtime code via `dbutils.secrets.get()`.
 *
 * **Values are NEVER persisted in Swamp's data layer.** The model writes
 * a metadata-only resource (scope + key + last_updated_ms) on put. The
 * actual value passes through to Databricks and is forgotten by Swamp.
 *
 * @see https://docs.databricks.com/api/workspace/secrets
 */
export const model = {
  type: "@mfbaig35r/databricks/secret",
  version: "2026.05.30.10",
  globalArguments: GlobalArgsSchema,

  resources: {
    "secret_meta": {
      description: "Metadata for a workspace secret (scope + key only). " +
        "The value itself is NEVER stored here.",
      schema: SecretMetaResourceSchema,
      lifetime: "infinite" as const,
      garbageCollection: 5,
    },
  },

  methods: {
    put: {
      description:
        "Put a secret via POST /api/2.0/secrets/put. Writes scope+key " +
        "metadata to Swamp; the value passes through to Databricks and is " +
        "not retained in Swamp's data layer.",
      arguments: PutArgs,
      execute: async (
        args: z.infer<typeof PutArgs>,
        context: {
          globalArgs: GlobalArgs;
          writeResource: WriteResource;
          logger: Logger;
        },
      ) => {
        await dbxFetch(
          context.globalArgs,
          "/api/2.0/secrets/put",
          {
            method: "POST",
            body: JSON.stringify({
              scope: args.scope,
              key: args.key,
              string_value: args.string_value,
            }),
          },
        );
        context.logger.info(
          "Put secret {scope}/{key} (value not logged)",
          { scope: args.scope, key: args.key },
        );
        const handle = await context.writeResource(
          "secret_meta",
          `${args.scope}:${args.key}`,
          {
            scope: args.scope,
            key: args.key,
            last_updated_ms: Date.now(),
            workspace_url: context.globalArgs.workspace_url,
          },
        );
        return { dataHandles: [handle] };
      },
    },

    delete: {
      description: "Delete a secret via POST /api/2.0/secrets/delete.",
      arguments: DeleteArgs,
      execute: async (
        args: z.infer<typeof DeleteArgs>,
        context: { globalArgs: GlobalArgs; logger: Logger },
      ) => {
        await dbxFetch(
          context.globalArgs,
          "/api/2.0/secrets/delete",
          { method: "POST", body: JSON.stringify(args) },
        );
        context.logger.info(
          "Deleted secret {scope}/{key}",
          { scope: args.scope, key: args.key },
        );
        return { dataHandles: [] };
      },
    },

    list: {
      description:
        "List all keys in a scope via GET /api/2.0/secrets/list?scope=... " +
        "Returns key names and last_updated_timestamp only, no values.",
      arguments: ListArgs,
      execute: async (
        args: z.infer<typeof ListArgs>,
        context: { globalArgs: GlobalArgs; logger: Logger },
      ) => {
        const res = await dbxFetch(
          context.globalArgs,
          `/api/2.0/secrets/list?scope=${encodeURIComponent(args.scope)}`,
        );
        const secrets = (res.secrets ?? []) as Array<{
          key: string;
          last_updated_timestamp: number;
        }>;
        context.logger.info(
          "Listed {count} secrets in scope {scope}",
          { count: secrets.length, scope: args.scope },
        );
        return { dataHandles: [], outputs: { secrets } };
      },
    },
  },
};
