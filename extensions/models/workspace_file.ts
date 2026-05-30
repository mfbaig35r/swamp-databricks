import { z } from "npm:zod@4";
import {
  b64encode,
  dbxFetch,
  GlobalArgs,
  GlobalArgsSchema,
  Logger,
  pathToResourceName,
  ReadResource,
  WriteResource,
} from "./_lib/databricks.ts";

// ---------------------------------------------------------------------------
// Schemas
// ---------------------------------------------------------------------------

const FileResourceSchema = z.object({
  path: z.string(),
  object_type: z.string(),
  uploaded_time_ms: z.number().int(),
  workspace_url: z.string().url(),
});

const UploadArgs = z.object({
  path: z.string().regex(/^\/.+/, "path must be absolute (start with /)"),
  content: z.string().min(1).describe(
    "Raw file content (NOT base64). UTF-8 text only in v0.6.",
  ),
  overwrite: z.boolean().default(false),
});

const DeleteArgs = z.object({
  path: z.string().regex(/^\/.+/),
});

const ReadArgs = z.object({
  path: z.string().regex(/^\/.+/),
});

// ---------------------------------------------------------------------------
// Model
// ---------------------------------------------------------------------------

/**
 * `@mfbaig35r/databricks/workspace_file`: workspace files (FILE object type)
 * lifecycle. Distinct from `@mfbaig35r/databricks/notebook`, which manages
 * NOTEBOOK objects. Use this model when your downstream task needs a plain
 * source file at a workspace path, e.g. `sql_task.file.path`,
 * `spark_python_task.python_file`, or a dbt project's `profiles.yml`.
 *
 * Upload uses `POST /api/2.0/workspace/import` with `format: AUTO` and no
 * `language`, which Databricks interprets as "create a workspace file"
 * rather than a notebook. The resulting workspace object has
 * `object_type: FILE`.
 *
 * The model verifies the resulting object_type via a `/api/2.0/workspace/get-status`
 * call right after import. If Databricks ends up creating a NOTEBOOK instead
 * (older workspaces sometimes do this for certain content), the upload still
 * succeeds but the object_type field on the resource records what was
 * actually created.
 *
 * @see https://docs.databricks.com/api/workspace/workspace
 */
export const model = {
  type: "@mfbaig35r/databricks/workspace_file",
  version: "2026.05.30.10",
  globalArguments: GlobalArgsSchema,

  resources: {
    "file": {
      description:
        "A workspace file (or, in some workspaces, a notebook), keyed by " +
        "absolute path. The object_type field records what Databricks " +
        "actually created.",
      schema: FileResourceSchema,
      lifetime: "infinite" as const,
      garbageCollection: 5,
    },
  },

  methods: {
    upload: {
      description: "Import a file to the workspace via " +
        "POST /api/2.0/workspace/import with format=AUTO. " +
        "Verifies the resulting object_type via GET /api/2.0/workspace/get-status.",
      arguments: UploadArgs,
      execute: async (
        args: z.infer<typeof UploadArgs>,
        context: {
          globalArgs: GlobalArgs;
          writeResource: WriteResource;
          logger: Logger;
        },
      ) => {
        await dbxFetch(
          context.globalArgs,
          "/api/2.0/workspace/import",
          {
            method: "POST",
            body: JSON.stringify({
              path: args.path,
              content: b64encode(args.content),
              format: "AUTO",
              overwrite: args.overwrite,
            }),
          },
        );
        const status = await dbxFetch(
          context.globalArgs,
          `/api/2.0/workspace/get-status?path=${encodeURIComponent(args.path)}`,
        );
        const objectType = (status.object_type as string) ?? "UNKNOWN";
        context.logger.info(
          "Uploaded workspace file to {path}, object_type {object_type}",
          { path: args.path, object_type: objectType },
        );
        const handle = await context.writeResource(
          "file",
          pathToResourceName(args.path),
          {
            path: args.path,
            object_type: objectType,
            uploaded_time_ms: Date.now(),
            workspace_url: context.globalArgs.workspace_url,
          },
        );
        return { dataHandles: [handle] };
      },
    },

    read: {
      description:
        "Export a workspace file via GET /api/2.0/workspace/export?format=AUTO. " +
        "Returns decoded content (UTF-8) in outputs.",
      arguments: ReadArgs,
      execute: async (
        args: z.infer<typeof ReadArgs>,
        context: {
          globalArgs: GlobalArgs;
          logger: Logger;
        },
      ) => {
        const res = await dbxFetch(
          context.globalArgs,
          `/api/2.0/workspace/export?path=${
            encodeURIComponent(args.path)
          }&format=AUTO`,
        );
        const b64 = res.content as string;
        const bin = atob(b64);
        const bytes = new Uint8Array(bin.length);
        for (let i = 0; i < bin.length; i++) bytes[i] = bin.charCodeAt(i);
        const text = new TextDecoder().decode(bytes);
        context.logger.info(
          "Exported workspace file {path} ({bytes} bytes)",
          { path: args.path, bytes: text.length },
        );
        return { dataHandles: [], outputs: { content: text } };
      },
    },

    delete: {
      description:
        "Delete the workspace file via POST /api/2.0/workspace/delete.",
      arguments: DeleteArgs,
      execute: async (
        args: z.infer<typeof DeleteArgs>,
        context: {
          globalArgs: GlobalArgs;
          readResource: ReadResource;
          logger: Logger;
        },
      ) => {
        await dbxFetch(
          context.globalArgs,
          "/api/2.0/workspace/delete",
          {
            method: "POST",
            body: JSON.stringify({ path: args.path }),
          },
        );
        context.logger.info(
          "Deleted workspace file {path}",
          { path: args.path },
        );
        return { dataHandles: [] };
      },
    },
  },
};
