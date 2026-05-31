import { z } from "npm:zod@4";
import {
  dbxFetch,
  existsOnWorkspace,
  GlobalArgs,
  GlobalArgsSchema,
  Logger,
  ReadResource,
  sha256,
  WriteResource,
} from "./_lib/databricks.ts";

// ---------------------------------------------------------------------------
// Schemas
// ---------------------------------------------------------------------------

const ClusterSize = z.enum([
  "2X-Small",
  "X-Small",
  "Small",
  "Medium",
  "Large",
  "X-Large",
  "2X-Large",
  "3X-Large",
  "4X-Large",
]);

const WarehouseSettings = z.object({
  name: z.string().min(1).max(100),
  cluster_size: ClusterSize.default("X-Small"),
  min_num_clusters: z.number().int().positive().default(1),
  max_num_clusters: z.number().int().positive().default(1),
  auto_stop_mins: z.number().int().nonnegative().default(10).describe(
    "Minutes idle before auto-stop. 0 disables auto-stop.",
  ),
  enable_photon: z.boolean().optional(),
  enable_serverless_compute: z.boolean().default(true).describe(
    "Required on Databricks Free (serverless-only).",
  ),
  warehouse_type: z.enum(["CLASSIC", "PRO"]).default("PRO"),
  spot_instance_policy: z.enum([
    "COST_OPTIMIZED",
    "RELIABILITY_OPTIMIZED",
  ]).optional(),
  channel: z.object({
    name: z.enum([
      "CHANNEL_NAME_CURRENT",
      "CHANNEL_NAME_PREVIEW",
    ]),
  }).optional(),
  tags: z.object({
    custom_tags: z.array(z.object({
      key: z.string(),
      value: z.string(),
    })),
  }).optional(),
});

const RunQueryArgs = z.object({
  warehouse_ref: z.string(),
  statement: z.string().min(1),
  catalog: z.string().optional(),
  schema: z.string().optional(),
  wait_timeout_seconds: z.number().int().min(0).max(50).default(10).describe(
    "0 = async (returns statement_id, poll with wait_statement). 5-50 = sync wait.",
  ),
  on_wait_timeout: z.enum(["CONTINUE", "CANCEL"]).default("CONTINUE"),
  row_limit: z.number().int().positive().optional(),
});

// ---------------------------------------------------------------------------
// Resource schemas
// ---------------------------------------------------------------------------

const WarehouseResourceSchema = z.object({
  warehouse_id: z.string(),
  name: z.string(),
  state: z.string(),
  cluster_size: z.string(),
  enable_serverless_compute: z.boolean(),
  warehouse_type: z.string(),
  created_time_ms: z.number().int(),
  settings_hash: z.string(),
  workspace_url: z.string().url(),
});

const LastStatementResourceSchema = z.object({
  statement_id: z.string(),
  warehouse_id: z.string(),
  state: z.string(),
  sql_preview: z.string(),
  submitted_time_ms: z.number().int(),
});

// ---------------------------------------------------------------------------
// Model
// ---------------------------------------------------------------------------

/**
 * `@mfbaig35r/databricks/sql_warehouse`: Databricks SQL Warehouse lifecycle
 * plus statement execution via the SQL Statement Execution API.
 *
 * Methods cover create, read, update (full replace via /edit), delete, start,
 * stop, run_query, wait_statement, and cancel_statement.
 *
 * `run_query` defaults to a 10-second synchronous wait. If the statement
 * completes within that window the result is returned in outputs; otherwise
 * a `last_statement` resource is written and `wait_statement` polls until
 * terminal. Use this to run a DROP TABLE for DLT pipeline cleanup, or to
 * validate SQL tasks before wiring them into a job.
 *
 * On Databricks Free Edition, serverless is the only option: leave
 * `enable_serverless_compute: true` (the default). Warehouse quotas are
 * small (usually 1-2), so prefer referencing an existing auto-provisioned
 * Starter Warehouse over creating new ones.
 *
 * @see https://docs.databricks.com/api/workspace/warehouses
 * @see https://docs.databricks.com/api/workspace/statementexecution
 */
export const model = {
  type: "@mfbaig35r/databricks/sql_warehouse",
  version: "2026.05.30.18",
  globalArguments: GlobalArgsSchema,

  resources: {
    "warehouse": {
      description:
        "A SQL warehouse, keyed by user-supplied name. State refreshed on read.",
      schema: WarehouseResourceSchema,
      lifetime: "infinite" as const,
      garbageCollection: 5,
    },
    "last_statement": {
      description: "Most recent statement submitted through this model. " +
        "sql_preview is the first 200 chars of the SQL.",
      schema: LastStatementResourceSchema,
      lifetime: "workflow" as const,
      garbageCollection: 20,
    },
  },

  methods: {
    create: {
      description: "Create a SQL warehouse via POST /api/2.0/sql/warehouses. " +
        "Writes a 'warehouse' resource keyed by name.",
      arguments: WarehouseSettings,
      execute: async (
        args: z.infer<typeof WarehouseSettings>,
        context: {
          globalArgs: GlobalArgs;
          writeResource: WriteResource;
          logger: Logger;
        },
      ) => {
        const out = await dbxFetch(
          context.globalArgs,
          "/api/2.0/sql/warehouses",
          { method: "POST", body: JSON.stringify(args) },
        );
        const warehouseId = out.id as string;
        context.logger.info(
          "Created SQL warehouse {name} -> {warehouse_id}",
          { name: args.name, warehouse_id: warehouseId },
        );
        const handle = await context.writeResource("warehouse", args.name, {
          warehouse_id: warehouseId,
          name: args.name,
          state: (out.state as string) ?? "STARTING",
          cluster_size: args.cluster_size,
          enable_serverless_compute: args.enable_serverless_compute,
          warehouse_type: args.warehouse_type,
          created_time_ms: Date.now(),
          settings_hash: await sha256(JSON.stringify(args)),
          workspace_url: context.globalArgs.workspace_url,
        });
        return { dataHandles: [handle] };
      },
    },

    read: {
      description:
        "Fetch live warehouse state via GET /api/2.0/sql/warehouses/{id}.",
      arguments: z.object({ warehouse_ref: z.string() }),
      execute: async (
        args: { warehouse_ref: string },
        context: {
          globalArgs: GlobalArgs;
          readResource: ReadResource;
          logger: Logger;
        },
      ) => {
        const prior = await context.readResource(args.warehouse_ref);
        if (!prior) {
          throw new Error(
            `No stored 'warehouse' resource named '${args.warehouse_ref}'. ` +
              `Call 'create' first or use 'adopt' to register an existing warehouse.`,
          );
        }
        const warehouseId = prior.warehouse_id as string;
        const live = await dbxFetch(
          context.globalArgs,
          `/api/2.0/sql/warehouses/${warehouseId}`,
        );
        context.logger.info(
          "Read warehouse {warehouse_id} state {state}",
          { warehouse_id: warehouseId, state: live.state },
        );
        return { dataHandles: [], outputs: { live } };
      },
    },

    list: {
      description:
        "List all warehouses on the workspace via GET /api/2.0/sql/warehouses. " +
        "Returns the full list in outputs; does not write a resource. Useful " +
        "to discover warehouse_ids for adopt.",
      arguments: z.object({}),
      execute: async (
        _args: Record<string, never>,
        context: {
          globalArgs: GlobalArgs;
          logger: Logger;
        },
      ) => {
        const res = await dbxFetch(
          context.globalArgs,
          "/api/2.0/sql/warehouses",
        );
        const warehouses = (res.warehouses ?? []) as Array<{
          id: string;
          name: string;
          state: string;
        }>;
        context.logger.info(
          "Listed {count} warehouses on workspace",
          { count: warehouses.length },
        );
        return { dataHandles: [], outputs: { warehouses } };
      },
    },

    adopt: {
      description:
        "Register an existing workspace warehouse as a Swamp 'warehouse' " +
        "resource without creating a new one. Useful on Free Edition where " +
        "warehouse quotas are tight and the workspace ships with a Starter " +
        "Warehouse already.",
      arguments: z.object({
        name: z.string(),
        warehouse_id: z.string(),
      }),
      execute: async (
        args: { name: string; warehouse_id: string },
        context: {
          globalArgs: GlobalArgs;
          writeResource: WriteResource;
          logger: Logger;
        },
      ) => {
        const live = await dbxFetch(
          context.globalArgs,
          `/api/2.0/sql/warehouses/${args.warehouse_id}`,
        );
        context.logger.info(
          "Adopted warehouse {warehouse_id} as {name}",
          { warehouse_id: args.warehouse_id, name: args.name },
        );
        const handle = await context.writeResource("warehouse", args.name, {
          warehouse_id: args.warehouse_id,
          name: args.name,
          state: (live.state as string) ?? "UNKNOWN",
          cluster_size: (live.cluster_size as string) ?? "Unknown",
          enable_serverless_compute:
            (live.enable_serverless_compute as boolean) ?? false,
          warehouse_type: (live.warehouse_type as string) ?? "UNKNOWN",
          created_time_ms: Date.now(),
          settings_hash: "adopted",
          workspace_url: context.globalArgs.workspace_url,
        });
        return { dataHandles: [handle] };
      },
    },

    update: {
      description:
        "Full replace warehouse settings via POST /api/2.0/sql/warehouses/{id}/edit.",
      arguments: z.object({
        warehouse_ref: z.string(),
        settings: WarehouseSettings,
      }),
      execute: async (
        args: {
          warehouse_ref: string;
          settings: z.infer<typeof WarehouseSettings>;
        },
        context: {
          globalArgs: GlobalArgs;
          readResource: ReadResource;
          writeResource: WriteResource;
          logger: Logger;
        },
      ) => {
        const prior = await context.readResource(args.warehouse_ref);
        if (!prior) {
          throw new Error(
            `No stored 'warehouse' resource named '${args.warehouse_ref}'.`,
          );
        }
        const warehouseId = prior.warehouse_id as string;
        await dbxFetch(
          context.globalArgs,
          `/api/2.0/sql/warehouses/${warehouseId}/edit`,
          { method: "POST", body: JSON.stringify(args.settings) },
        );
        context.logger.info(
          "Updated warehouse {warehouse_id}",
          { warehouse_id: warehouseId },
        );
        const handle = await context.writeResource(
          "warehouse",
          args.settings.name,
          {
            ...prior,
            name: args.settings.name,
            cluster_size: args.settings.cluster_size,
            enable_serverless_compute: args.settings.enable_serverless_compute,
            warehouse_type: args.settings.warehouse_type,
            settings_hash: await sha256(JSON.stringify(args.settings)),
          },
        );
        return { dataHandles: [handle] };
      },
    },

    delete: {
      description:
        "Delete the warehouse via DELETE /api/2.0/sql/warehouses/{id}.",
      arguments: z.object({ warehouse_ref: z.string() }),
      execute: async (
        args: { warehouse_ref: string },
        context: {
          globalArgs: GlobalArgs;
          readResource: ReadResource;
          logger: Logger;
        },
      ) => {
        const prior = await context.readResource(args.warehouse_ref);
        if (!prior) {
          throw new Error(
            `No stored 'warehouse' resource named '${args.warehouse_ref}'.`,
          );
        }
        const warehouseId = prior.warehouse_id as string;
        await dbxFetch(
          context.globalArgs,
          `/api/2.0/sql/warehouses/${warehouseId}`,
          { method: "DELETE" },
        );
        context.logger.info(
          "Deleted warehouse {warehouse_id}",
          { warehouse_id: warehouseId },
        );
        return { dataHandles: [] };
      },
    },

    start: {
      description:
        "Start the warehouse via POST /api/2.0/sql/warehouses/{id}/start.",
      arguments: z.object({ warehouse_ref: z.string() }),
      execute: async (
        args: { warehouse_ref: string },
        context: {
          globalArgs: GlobalArgs;
          readResource: ReadResource;
          logger: Logger;
        },
      ) => {
        const prior = await context.readResource(args.warehouse_ref);
        if (!prior) {
          throw new Error(
            `No stored 'warehouse' resource named '${args.warehouse_ref}'.`,
          );
        }
        const warehouseId = prior.warehouse_id as string;
        await dbxFetch(
          context.globalArgs,
          `/api/2.0/sql/warehouses/${warehouseId}/start`,
          { method: "POST", body: "{}" },
        );
        context.logger.info(
          "Started warehouse {warehouse_id}",
          { warehouse_id: warehouseId },
        );
        return { dataHandles: [] };
      },
    },

    stop: {
      description:
        "Stop the warehouse via POST /api/2.0/sql/warehouses/{id}/stop.",
      arguments: z.object({ warehouse_ref: z.string() }),
      execute: async (
        args: { warehouse_ref: string },
        context: {
          globalArgs: GlobalArgs;
          readResource: ReadResource;
          logger: Logger;
        },
      ) => {
        const prior = await context.readResource(args.warehouse_ref);
        if (!prior) {
          throw new Error(
            `No stored 'warehouse' resource named '${args.warehouse_ref}'.`,
          );
        }
        const warehouseId = prior.warehouse_id as string;
        await dbxFetch(
          context.globalArgs,
          `/api/2.0/sql/warehouses/${warehouseId}/stop`,
          { method: "POST", body: "{}" },
        );
        context.logger.info(
          "Stopped warehouse {warehouse_id}",
          { warehouse_id: warehouseId },
        );
        return { dataHandles: [] };
      },
    },

    run_query: {
      description: "Submit a SQL statement via POST /api/2.0/sql/statements. " +
        "Synchronous up to wait_timeout_seconds (default 10s). On terminal " +
        "completion within that window, result is returned in outputs and no " +
        "last_statement resource is persisted; otherwise a last_statement is " +
        "written so wait_statement can take over.",
      arguments: RunQueryArgs,
      execute: async (
        args: z.infer<typeof RunQueryArgs>,
        context: {
          globalArgs: GlobalArgs;
          readResource: ReadResource;
          writeResource: WriteResource;
          logger: Logger;
        },
      ) => {
        const prior = await context.readResource(args.warehouse_ref);
        if (!prior) {
          throw new Error(
            `No stored 'warehouse' resource named '${args.warehouse_ref}'.`,
          );
        }
        const warehouseId = prior.warehouse_id as string;
        const body: Record<string, unknown> = {
          warehouse_id: warehouseId,
          statement: args.statement,
          wait_timeout: `${args.wait_timeout_seconds}s`,
          on_wait_timeout: args.on_wait_timeout,
        };
        if (args.catalog) body.catalog = args.catalog;
        if (args.schema) body.schema = args.schema;
        if (args.row_limit) body.row_limit = args.row_limit;
        const out = await dbxFetch(
          context.globalArgs,
          "/api/2.0/sql/statements",
          { method: "POST", body: JSON.stringify(body) },
        );
        const statementId = out.statement_id as string;
        const status = out.status as { state: string };
        context.logger.info(
          "Submitted statement {statement_id} on warehouse {warehouse_id}, state {state}",
          {
            statement_id: statementId,
            warehouse_id: warehouseId,
            state: status.state,
          },
        );
        const terminal = new Set([
          "SUCCEEDED",
          "FAILED",
          "CANCELED",
          "CLOSED",
        ]);
        if (terminal.has(status.state)) {
          return {
            dataHandles: [],
            outputs: { result: out },
          };
        }
        const handle = await context.writeResource(
          "last_statement",
          statementId,
          {
            statement_id: statementId,
            warehouse_id: warehouseId,
            state: status.state,
            sql_preview: args.statement.slice(0, 200),
            submitted_time_ms: Date.now(),
          },
        );
        return { dataHandles: [handle], outputs: { partial: out } };
      },
    },

    wait_statement: {
      description:
        "Poll GET /api/2.0/sql/statements/{statement_id} until state is " +
        "terminal (SUCCEEDED, FAILED, CANCELED, CLOSED). Updates last_statement.",
      arguments: z.object({
        statement_id: z.string(),
        poll_seconds: z.number().int().positive().default(5),
        timeout_seconds: z.number().int().positive().default(900),
      }),
      execute: async (
        args: {
          statement_id: string;
          poll_seconds: number;
          timeout_seconds: number;
        },
        context: {
          globalArgs: GlobalArgs;
          writeResource: WriteResource;
          logger: Logger;
        },
      ) => {
        const terminal = new Set([
          "SUCCEEDED",
          "FAILED",
          "CANCELED",
          "CLOSED",
        ]);
        const deadline = Date.now() + args.timeout_seconds * 1000;
        let last: Record<string, unknown> | null = null;
        while (Date.now() < deadline) {
          const res = await dbxFetch(
            context.globalArgs,
            `/api/2.0/sql/statements/${args.statement_id}`,
          );
          last = res;
          const status = res.status as { state: string };
          context.logger.info(
            "Statement {statement_id} state {state}",
            { statement_id: args.statement_id, state: status.state },
          );
          if (terminal.has(status.state)) break;
          await new Promise((r) => setTimeout(r, args.poll_seconds * 1000));
        }
        if (!last) {
          throw new Error(`No response for statement ${args.statement_id}`);
        }
        const status = last.status as { state: string };
        const warehouseId = (last.warehouse_id ?? "") as string;
        const handle = await context.writeResource(
          "last_statement",
          args.statement_id,
          {
            statement_id: args.statement_id,
            warehouse_id: warehouseId,
            state: status.state,
            sql_preview: "(see prior submission)",
            submitted_time_ms: Date.now(),
          },
        );
        return { dataHandles: [handle], outputs: { result: last } };
      },
    },

    cancel_statement: {
      description:
        "Cancel a running statement via POST /api/2.0/sql/statements/{id}/cancel.",
      arguments: z.object({ statement_id: z.string() }),
      execute: async (
        args: { statement_id: string },
        context: {
          globalArgs: GlobalArgs;
          logger: Logger;
        },
      ) => {
        await dbxFetch(
          context.globalArgs,
          `/api/2.0/sql/statements/${args.statement_id}/cancel`,
          { method: "POST", body: "{}" },
        );
        context.logger.info(
          "Cancelled statement {statement_id}",
          { statement_id: args.statement_id },
        );
        return { dataHandles: [] };
      },
    },

    create_or_update: {
      description:
        "Reconcile via Swamp data + workspace check: if a 'warehouse' " +
        "resource exists for this name AND the workspace still has that " +
        "warehouse_id, call /edit. Otherwise create. Safe across Swamp " +
        "tombstones.",
      arguments: WarehouseSettings,
      execute: async (
        args: z.infer<typeof WarehouseSettings>,
        context: {
          globalArgs: GlobalArgs;
          readResource: ReadResource;
          writeResource: WriteResource;
          logger: Logger;
        },
      ) => {
        const prior = await context.readResource(args.name);
        if (prior) {
          const warehouseId = prior.warehouse_id as string;
          const stillExists = await existsOnWorkspace(
            context.globalArgs,
            `/api/2.0/sql/warehouses/${warehouseId}`,
          );
          if (stillExists) {
            await dbxFetch(
              context.globalArgs,
              `/api/2.0/sql/warehouses/${warehouseId}/edit`,
              { method: "POST", body: JSON.stringify(args) },
            );
            context.logger.info(
              "create_or_update: edited existing warehouse {warehouse_id}",
              { warehouse_id: warehouseId },
            );
            const handle = await context.writeResource(
              "warehouse",
              args.name,
              {
                ...prior,
                name: args.name,
                cluster_size: args.cluster_size,
                enable_serverless_compute: args.enable_serverless_compute,
                warehouse_type: args.warehouse_type,
                settings_hash: await sha256(JSON.stringify(args)),
              },
            );
            return { dataHandles: [handle] };
          }
        }
        const out = await dbxFetch(
          context.globalArgs,
          "/api/2.0/sql/warehouses",
          { method: "POST", body: JSON.stringify(args) },
        );
        const warehouseId = out.id as string;
        context.logger.info(
          "create_or_update: created new warehouse {warehouse_id}",
          { warehouse_id: warehouseId },
        );
        const handle = await context.writeResource("warehouse", args.name, {
          warehouse_id: warehouseId,
          name: args.name,
          state: (out.state as string) ?? "STARTING",
          cluster_size: args.cluster_size,
          enable_serverless_compute: args.enable_serverless_compute,
          warehouse_type: args.warehouse_type,
          created_time_ms: Date.now(),
          settings_hash: await sha256(JSON.stringify(args)),
          workspace_url: context.globalArgs.workspace_url,
        });
        return { dataHandles: [handle] };
      },
    },
  },
};
