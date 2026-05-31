import { z } from "npm:zod@4";
import {
  dbxFetch,
  GlobalArgs,
  GlobalArgsSchema,
  Logger,
  ReadResource,
  WriteResource,
} from "./_lib/databricks.ts";

// ---------------------------------------------------------------------------
// Schemas
// ---------------------------------------------------------------------------

const CreateArgs = z.object({
  name: z.string().min(1).max(255),
  query: z.string().min(1).describe("SQL text"),
  warehouse_id: z.string().describe(
    "SQL warehouse the query runs against (NOT the data_source_id)",
  ),
  description: z.string().optional(),
  parent: z.string().optional().describe(
    "Folder path in the workspace, e.g. 'folders/<id>'",
  ),
  run_as_role: z.enum(["viewer", "owner"]).optional(),
  tags: z.array(z.string()).optional(),
});

const UpdateArgs = z.object({
  query_ref: z.string(),
  name: z.string().optional(),
  query: z.string().optional(),
  warehouse_id: z.string().optional(),
  description: z.string().optional(),
  run_as_role: z.enum(["viewer", "owner"]).optional(),
  tags: z.array(z.string()).optional(),
});

const QueryResourceSchema = z.object({
  query_id: z.string(),
  name: z.string(),
  warehouse_id: z.string(),
  created_time_ms: z.number().int(),
  workspace_url: z.string().url(),
});

/**
 * `@mfbaig35r/databricks/query`: DBSQL saved queries.
 *
 * Saved queries are what `sql_task.query.query_id` references in a Job
 * task. Use this model to manage them as code: create / update SQL text,
 * delete, list. The model owns the query_id mapping by user-supplied name.
 *
 * v0.10 surface: standard saved queries. Does not cover query snippets,
 * favorites, alerts (see workspace_permissions on `alerts` object type
 * for those).
 *
 * @see https://docs.databricks.com/api/workspace/queries
 */
export const model = {
  type: "@mfbaig35r/databricks/query",
  version: "2026.05.30.13",
  globalArguments: GlobalArgsSchema,

  resources: {
    "query": {
      description: "A saved DBSQL query, keyed by user-supplied name.",
      schema: QueryResourceSchema,
      lifetime: "infinite" as const,
      garbageCollection: 5,
    },
  },

  methods: {
    create: {
      description: "Create a saved query via POST /api/2.0/sql/queries.",
      arguments: CreateArgs,
      execute: async (
        args: z.infer<typeof CreateArgs>,
        context: {
          globalArgs: GlobalArgs;
          writeResource: WriteResource;
          logger: Logger;
        },
      ) => {
        const body: Record<string, unknown> = {
          query: {
            display_name: args.name,
            query_text: args.query,
            warehouse_id: args.warehouse_id,
          },
        };
        const queryBody = body.query as Record<string, unknown>;
        if (args.description) queryBody.description = args.description;
        if (args.parent) queryBody.parent_path = args.parent;
        if (args.run_as_role) queryBody.run_as_mode = args.run_as_role;
        if (args.tags) queryBody.tags = args.tags;
        const out = await dbxFetch(
          context.globalArgs,
          "/api/2.0/sql/queries",
          { method: "POST", body: JSON.stringify(body) },
        );
        const query = (out.query ?? out) as Record<string, unknown>;
        const queryId = query.id as string;
        context.logger.info(
          "Created query {name} -> {query_id}",
          { name: args.name, query_id: queryId },
        );
        const handle = await context.writeResource("query", args.name, {
          query_id: queryId,
          name: args.name,
          warehouse_id: args.warehouse_id,
          created_time_ms: Date.now(),
          workspace_url: context.globalArgs.workspace_url,
        });
        return { dataHandles: [handle] };
      },
    },

    read: {
      description: "GET /api/2.0/sql/queries/{query_id}.",
      arguments: z.object({ query_ref: z.string() }),
      execute: async (
        args: { query_ref: string },
        context: {
          globalArgs: GlobalArgs;
          readResource: ReadResource;
          logger: Logger;
        },
      ) => {
        const prior = await context.readResource(args.query_ref);
        if (!prior) {
          throw new Error(
            `No stored 'query' resource named '${args.query_ref}'.`,
          );
        }
        const live = await dbxFetch(
          context.globalArgs,
          `/api/2.0/sql/queries/${prior.query_id}`,
        );
        context.logger.info(
          "Read query {query_id}",
          { query_id: prior.query_id },
        );
        return { dataHandles: [], outputs: { live } };
      },
    },

    update: {
      description:
        "Update a saved query via POST /api/2.0/sql/queries/{query_id}.",
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
        const prior = await context.readResource(args.query_ref);
        if (!prior) {
          throw new Error(
            `No stored 'query' resource named '${args.query_ref}'.`,
          );
        }
        const update: Record<string, unknown> = {};
        if (args.name) update.display_name = args.name;
        if (args.query) update.query_text = args.query;
        if (args.warehouse_id) update.warehouse_id = args.warehouse_id;
        if (args.description !== undefined) {
          update.description = args.description;
        }
        if (args.run_as_role) update.run_as_mode = args.run_as_role;
        if (args.tags) update.tags = args.tags;
        await dbxFetch(
          context.globalArgs,
          `/api/2.0/sql/queries/${prior.query_id}`,
          {
            method: "POST",
            body: JSON.stringify({
              query_id: prior.query_id,
              query: update,
              update_mask: Object.keys(update).join(","),
            }),
          },
        );
        context.logger.info(
          "Updated query {query_id}",
          { query_id: prior.query_id },
        );
        const handle = await context.writeResource(
          "query",
          (args.name ?? prior.name) as string,
          {
            ...prior,
            name: (args.name ?? prior.name) as string,
            warehouse_id: (args.warehouse_id ?? prior.warehouse_id) as string,
          },
        );
        return { dataHandles: [handle] };
      },
    },

    delete: {
      description: "DELETE /api/2.0/sql/queries/{query_id}.",
      arguments: z.object({ query_ref: z.string() }),
      execute: async (
        args: { query_ref: string },
        context: {
          globalArgs: GlobalArgs;
          readResource: ReadResource;
          logger: Logger;
        },
      ) => {
        const prior = await context.readResource(args.query_ref);
        if (!prior) {
          throw new Error(
            `No stored 'query' resource named '${args.query_ref}'.`,
          );
        }
        await dbxFetch(
          context.globalArgs,
          `/api/2.0/sql/queries/${prior.query_id}`,
          { method: "DELETE" },
        );
        context.logger.info(
          "Deleted query {query_id}",
          { query_id: prior.query_id },
        );
        return { dataHandles: [] };
      },
    },

    list: {
      description: "GET /api/2.0/sql/queries (list all queries).",
      arguments: z.object({
        page_size: z.number().int().positive().max(100).optional(),
      }),
      execute: async (
        args: { page_size?: number },
        context: { globalArgs: GlobalArgs; logger: Logger },
      ) => {
        const qs = new URLSearchParams();
        if (args.page_size) qs.set("page_size", String(args.page_size));
        const res = await dbxFetch(
          context.globalArgs,
          `/api/2.0/sql/queries${qs.toString() ? "?" + qs : ""}`,
        );
        const results = (res.results ?? []) as Array<{
          id: string;
          display_name: string;
        }>;
        context.logger.info(
          "Listed {count} queries",
          { count: results.length },
        );
        return { dataHandles: [], outputs: { results } };
      },
    },
  },
};
