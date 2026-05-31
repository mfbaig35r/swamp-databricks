import { z } from "npm:zod@4";
import {
  dbxFetch,
  GlobalArgs,
  GlobalArgsSchema,
  Logger,
  ReadResource,
  WriteResource,
} from "./_lib/databricks.ts";

const ServedEntity = z.object({
  name: z.string().optional(),
  entity_name: z.string().describe(
    "Full UC name of the registered_model, e.g. '<catalog>.<schema>.<model>'",
  ),
  entity_version: z.string().describe("Model version number as a string"),
  workload_size: z.enum(["Small", "Medium", "Large"]).optional(),
  workload_type: z.enum([
    "CPU",
    "GPU_SMALL",
    "GPU_MEDIUM",
    "GPU_LARGE",
    "MULTIGPU_MEDIUM",
  ]).optional(),
  scale_to_zero_enabled: z.boolean().optional(),
  environment_vars: z.record(z.string(), z.string()).optional(),
});

const TrafficConfig = z.object({
  routes: z.array(z.object({
    served_model_name: z.string().optional(),
    served_entity_name: z.string().optional(),
    traffic_percentage: z.number().int().min(0).max(100),
  })),
});

const CreateArgs = z.object({
  name: z.string().min(1).max(63).regex(/^[a-zA-Z0-9_-]+$/),
  config: z.object({
    served_entities: z.array(ServedEntity).min(1),
    traffic_config: TrafficConfig.optional(),
  }),
  tags: z.array(z.object({
    key: z.string(),
    value: z.string().optional(),
  })).optional(),
});

const InvokeArgs = z.object({
  endpoint_ref: z.string(),
  dataframe_records: z.array(z.record(z.string(), z.unknown())).optional(),
  dataframe_split: z.object({
    columns: z.array(z.string()),
    data: z.array(z.array(z.unknown())),
  }).optional(),
  instances: z.array(z.unknown()).optional(),
  inputs: z.unknown().optional(),
}).refine(
  (a) =>
    [a.dataframe_records, a.dataframe_split, a.instances, a.inputs].filter(
      Boolean,
    ).length === 1,
  {
    message:
      "exactly one input shape required: dataframe_records | dataframe_split | instances | inputs",
  },
);

const EndpointResourceSchema = z.object({
  name: z.string(),
  endpoint_id: z.string().optional(),
  state: z.string().optional(),
  served_entity_name: z.string(),
  served_entity_version: z.string(),
  created_time_ms: z.number().int(),
  workspace_url: z.string().url(),
});

/**
 * `@mfbaig35r/databricks/model_serving_endpoint`: real-time model serving.
 *
 * **Not supported on Databricks Free.** Model Serving requires a paid
 * Databricks workspace. This model ships schema-validated but
 * end-to-end smoke validation is pending until a paid workspace is
 * available. The schema follows the public API docs for AWS Databricks
 * as of v0.18; Azure and GCP variants may have minor differences in
 * `workload_type` and traffic_config shape.
 *
 * Pairs with `registered_model` and `model_version`: serving endpoints
 * deploy specific versions of registered UC models.
 *
 * @see https://docs.databricks.com/api/workspace/servingendpoints
 */
export const model = {
  type: "@mfbaig35r/databricks/model_serving_endpoint",
  version: "2026.05.30.18",
  globalArguments: GlobalArgsSchema,

  resources: {
    "endpoint": {
      description:
        "A serving endpoint. Schema-only on Databricks Free; end-to-end " +
        "validation pending until a paid workspace surface is available.",
      schema: EndpointResourceSchema,
      lifetime: "infinite" as const,
      garbageCollection: 5,
    },
  },

  methods: {
    create: {
      description: "POST /api/2.0/serving-endpoints. Paid Databricks only.",
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
          "/api/2.0/serving-endpoints",
          { method: "POST", body: JSON.stringify(args) },
        );
        const endpointId = out.id as string | undefined;
        const firstEntity = args.config.served_entities[0];
        context.logger.info(
          "Created serving endpoint {name}",
          { name: args.name },
        );
        const handle = await context.writeResource("endpoint", args.name, {
          name: args.name,
          endpoint_id: endpointId,
          state: out.state as string | undefined,
          served_entity_name: firstEntity.entity_name,
          served_entity_version: firstEntity.entity_version,
          created_time_ms: Date.now(),
          workspace_url: context.globalArgs.workspace_url,
        });
        return { dataHandles: [handle] };
      },
    },

    read: {
      description: "GET /api/2.0/serving-endpoints/{name}.",
      arguments: z.object({ endpoint_ref: z.string() }),
      execute: async (
        args: { endpoint_ref: string },
        context: {
          globalArgs: GlobalArgs;
          readResource: ReadResource;
          logger: Logger;
        },
      ) => {
        const prior = await context.readResource(args.endpoint_ref);
        if (!prior) {
          throw new Error(
            `No stored 'endpoint' resource named '${args.endpoint_ref}'.`,
          );
        }
        const live = await dbxFetch(
          context.globalArgs,
          `/api/2.0/serving-endpoints/${prior.name}`,
        );
        context.logger.info(
          "Read serving endpoint {name}",
          { name: prior.name },
        );
        return { dataHandles: [], outputs: { live } };
      },
    },

    update_config: {
      description:
        "PUT /api/2.0/serving-endpoints/{name}/config. Full replace of " +
        "served_entities and traffic_config.",
      arguments: z.object({
        endpoint_ref: z.string(),
        config: z.object({
          served_entities: z.array(ServedEntity).min(1),
          traffic_config: TrafficConfig.optional(),
        }),
      }),
      execute: async (
        args: {
          endpoint_ref: string;
          config: {
            served_entities: z.infer<typeof ServedEntity>[];
            traffic_config?: z.infer<typeof TrafficConfig>;
          };
        },
        context: {
          globalArgs: GlobalArgs;
          readResource: ReadResource;
          writeResource: WriteResource;
          logger: Logger;
        },
      ) => {
        const prior = await context.readResource(args.endpoint_ref);
        if (!prior) {
          throw new Error(
            `No stored 'endpoint' resource named '${args.endpoint_ref}'.`,
          );
        }
        await dbxFetch(
          context.globalArgs,
          `/api/2.0/serving-endpoints/${prior.name}/config`,
          { method: "PUT", body: JSON.stringify(args.config) },
        );
        context.logger.info(
          "Updated config on serving endpoint {name}",
          { name: prior.name },
        );
        const firstEntity = args.config.served_entities[0];
        const handle = await context.writeResource(
          "endpoint",
          args.endpoint_ref,
          {
            ...prior,
            served_entity_name: firstEntity.entity_name,
            served_entity_version: firstEntity.entity_version,
          },
        );
        return { dataHandles: [handle] };
      },
    },

    delete: {
      description: "DELETE /api/2.0/serving-endpoints/{name}.",
      arguments: z.object({ endpoint_ref: z.string() }),
      execute: async (
        args: { endpoint_ref: string },
        context: {
          globalArgs: GlobalArgs;
          readResource: ReadResource;
          logger: Logger;
        },
      ) => {
        const prior = await context.readResource(args.endpoint_ref);
        if (!prior) {
          throw new Error(
            `No stored 'endpoint' resource named '${args.endpoint_ref}'.`,
          );
        }
        await dbxFetch(
          context.globalArgs,
          `/api/2.0/serving-endpoints/${prior.name}`,
          { method: "DELETE" },
        );
        context.logger.info(
          "Deleted serving endpoint {name}",
          { name: prior.name },
        );
        return { dataHandles: [] };
      },
    },

    list: {
      description: "GET /api/2.0/serving-endpoints (list all endpoints).",
      arguments: z.object({}),
      execute: async (
        _args: Record<string, never>,
        context: { globalArgs: GlobalArgs; logger: Logger },
      ) => {
        const res = await dbxFetch(
          context.globalArgs,
          "/api/2.0/serving-endpoints",
        );
        const endpoints = (res.endpoints ?? []) as Array<{
          name: string;
          state: { ready: string };
        }>;
        context.logger.info(
          "Listed {count} serving endpoints",
          { count: endpoints.length },
        );
        return { dataHandles: [], outputs: { endpoints } };
      },
    },

    invoke: {
      description:
        "POST /api/2.0/serving-endpoints/{name}/invocations. Sends an " +
        "inference request. Use one of dataframe_records, " +
        "dataframe_split, instances, or inputs (matched to the model's " +
        "input signature).",
      arguments: InvokeArgs,
      execute: async (
        args: z.infer<typeof InvokeArgs>,
        context: {
          globalArgs: GlobalArgs;
          readResource: ReadResource;
          logger: Logger;
        },
      ) => {
        const prior = await context.readResource(args.endpoint_ref);
        if (!prior) {
          throw new Error(
            `No stored 'endpoint' resource named '${args.endpoint_ref}'.`,
          );
        }
        const body: Record<string, unknown> = {};
        if (args.dataframe_records) {
          body.dataframe_records = args.dataframe_records;
        }
        if (args.dataframe_split) body.dataframe_split = args.dataframe_split;
        if (args.instances) body.instances = args.instances;
        if (args.inputs !== undefined) body.inputs = args.inputs;
        const res = await dbxFetch(
          context.globalArgs,
          `/serving-endpoints/${prior.name}/invocations`,
          { method: "POST", body: JSON.stringify(body) },
        );
        context.logger.info(
          "Invoked serving endpoint {name}",
          { name: prior.name },
        );
        return { dataHandles: [], outputs: { predictions: res } };
      },
    },
  },
};
