import { z } from "npm:zod@4";
import {
  dbxFetch,
  GlobalArgs,
  GlobalArgsSchema,
  Logger,
  WriteResource,
} from "./_lib/databricks.ts";

const TableResourceSchema = z.object({
  full_name: z.string().describe("<catalog>.<schema>.<table>"),
  name: z.string(),
  catalog_name: z.string(),
  schema_name: z.string(),
  table_type: z.string(),
  data_source_format: z.string().optional(),
  owner: z.string().optional(),
  observed_time_ms: z.number().int(),
  workspace_url: z.string().url(),
});

const ReadArgs = z.object({
  full_name: z.string().regex(
    /^[^.]+\.[^.]+\.[^.]+$/,
    "must be <catalog>.<schema>.<table>",
  ),
});

const DeleteArgs = z.object({
  full_name: z.string().regex(
    /^[^.]+\.[^.]+\.[^.]+$/,
    "must be <catalog>.<schema>.<table>",
  ),
});

const ListArgs = z.object({
  catalog_name: z.string(),
  schema_name: z.string(),
  max_results: z.number().int().positive().max(50).optional(),
});

/**
 * `@mfbaig35r/databricks/uc_table`: Unity Catalog table governance.
 *
 * Tables are NOT created via this API surface; they come from SQL
 * `CREATE TABLE` (use `sql_warehouse.run_query`) or Spark writes
 * (use `job` with a notebook task). This model covers governance: read
 * metadata, delete, list.
 *
 * `read` persists an observed snapshot as a 'table' resource so other
 * Swamp models can chain off the table's existence.
 *
 * @see https://docs.databricks.com/api/workspace/tables
 */
export const model = {
  type: "@mfbaig35r/databricks/uc_table",
  version: "2026.05.30.9",
  globalArguments: GlobalArgsSchema,

  resources: {
    "table": {
      description:
        "An observed UC table snapshot. observed_time_ms records when " +
        "this snapshot was taken; call read again to refresh.",
      schema: TableResourceSchema,
      lifetime: "infinite" as const,
      garbageCollection: 5,
    },
  },

  methods: {
    read: {
      description:
        "Fetch table metadata via GET /api/2.1/unity-catalog/tables/{full_name}. " +
        "Writes a 'table' resource keyed by full_name.",
      arguments: ReadArgs,
      execute: async (
        args: z.infer<typeof ReadArgs>,
        context: {
          globalArgs: GlobalArgs;
          writeResource: WriteResource;
          logger: Logger;
        },
      ) => {
        const live = await dbxFetch(
          context.globalArgs,
          `/api/2.1/unity-catalog/tables/${args.full_name}`,
        );
        context.logger.info(
          "Read table {full_name}",
          { full_name: args.full_name },
        );
        const handle = await context.writeResource(
          "table",
          args.full_name.replace(/\./g, ":"),
          {
            full_name: live.full_name as string,
            name: live.name as string,
            catalog_name: live.catalog_name as string,
            schema_name: live.schema_name as string,
            table_type: live.table_type as string,
            data_source_format: live.data_source_format as string | undefined,
            owner: live.owner as string | undefined,
            observed_time_ms: Date.now(),
            workspace_url: context.globalArgs.workspace_url,
          },
        );
        return { dataHandles: [handle], outputs: { live } };
      },
    },

    delete: {
      description: "DELETE /api/2.1/unity-catalog/tables/{full_name}. " +
        "Drops the table including its data.",
      arguments: DeleteArgs,
      execute: async (
        args: z.infer<typeof DeleteArgs>,
        context: { globalArgs: GlobalArgs; logger: Logger },
      ) => {
        await dbxFetch(
          context.globalArgs,
          `/api/2.1/unity-catalog/tables/${args.full_name}`,
          { method: "DELETE" },
        );
        context.logger.info(
          "Deleted table {full_name}",
          { full_name: args.full_name },
        );
        return { dataHandles: [] };
      },
    },

    list: {
      description:
        "List tables in a schema via GET /api/2.1/unity-catalog/tables.",
      arguments: ListArgs,
      execute: async (
        args: z.infer<typeof ListArgs>,
        context: { globalArgs: GlobalArgs; logger: Logger },
      ) => {
        const qs = new URLSearchParams({
          catalog_name: args.catalog_name,
          schema_name: args.schema_name,
        });
        if (args.max_results) qs.set("max_results", String(args.max_results));
        const res = await dbxFetch(
          context.globalArgs,
          `/api/2.1/unity-catalog/tables?${qs}`,
        );
        const tables = (res.tables ?? []) as Array<{
          full_name: string;
          name: string;
        }>;
        context.logger.info(
          "Listed {count} tables in {catalog}.{schema}",
          {
            count: tables.length,
            catalog: args.catalog_name,
            schema: args.schema_name,
          },
        );
        return { dataHandles: [], outputs: { tables } };
      },
    },
  },
};
