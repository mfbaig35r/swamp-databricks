import { z } from "npm:zod@4";
import {
  b64encode,
  dbxFetch,
  GlobalArgs,
  GlobalArgsSchema,
  Logger,
  pathToResourceName,
  WriteResource,
} from "./_lib/databricks.ts";

// ---------------------------------------------------------------------------
// Schemas
// ---------------------------------------------------------------------------

const NotebookResourceSchema = z.object({
  path: z.string(),
  language: z.string(),
  uploaded_time_ms: z.number().int(),
  workspace_url: z.string().url(),
});

const UploadArgs = z.object({
  path: z.string().regex(/^\/.+/, "path must be absolute (start with /)"),
  content: z.string().min(1).describe("Raw notebook source text (NOT base64)"),
  language: z.enum(["PYTHON", "SCALA", "SQL", "R"]).default("PYTHON"),
  overwrite: z.boolean().default(false),
});

const DeleteArgs = z.object({
  path: z.string().regex(/^\/.+/),
  recursive: z.boolean().default(false),
});

const ExportArgs = z.object({
  path: z.string().regex(/^\/.+/),
  format: z.enum(["SOURCE", "HTML", "JUPYTER", "DBC"]).default("SOURCE"),
});

// ---------------------------------------------------------------------------
// Model
// ---------------------------------------------------------------------------

/**
 * `@mfbaig35r/databricks/notebook`: Databricks workspace notebook lifecycle
 * via the Workspace API. Methods cover import (upload), export (read content),
 * and delete. Resources track the absolute path; subsequent `delete` and
 * `read` look up by the same path used at upload time.
 *
 * @see https://docs.databricks.com/api/workspace/workspace
 */
export const model = {
  type: "@mfbaig35r/databricks/notebook",
  version: "2026.05.30.8",
  globalArguments: GlobalArgsSchema,

  resources: {
    "notebook": {
      description: "A workspace notebook, keyed by absolute path. " +
        "Resource names encode '/' as ':' for Swamp compatibility.",
      schema: NotebookResourceSchema,
      lifetime: "infinite" as const,
      garbageCollection: 5,
    },
  },

  methods: {
    upload: {
      description: "Import a notebook source to the workspace via " +
        "POST /api/2.0/workspace/import. Content is raw source text; the " +
        "model base64-encodes for transport.",
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
              format: "SOURCE",
              language: args.language,
              overwrite: args.overwrite,
            }),
          },
        );
        context.logger.info(
          "Uploaded {language} notebook to {path}",
          { language: args.language, path: args.path },
        );
        const handle = await context.writeResource(
          "notebook",
          pathToResourceName(args.path),
          {
            path: args.path,
            language: args.language,
            uploaded_time_ms: Date.now(),
            workspace_url: context.globalArgs.workspace_url,
          },
        );
        return { dataHandles: [handle] };
      },
    },

    read: {
      description: "Export a notebook from the workspace via " +
        "GET /api/2.0/workspace/export?path=...&format=SOURCE. " +
        "Returns decoded source text in outputs.",
      arguments: ExportArgs,
      execute: async (
        args: z.infer<typeof ExportArgs>,
        context: {
          globalArgs: GlobalArgs;
          logger: Logger;
        },
      ) => {
        const qs = new URLSearchParams({
          path: args.path,
          format: args.format,
        }).toString();
        const res = await dbxFetch(
          context.globalArgs,
          `/api/2.0/workspace/export?${qs}`,
        );
        const b64 = res.content as string;
        const bin = atob(b64);
        const bytes = new Uint8Array(bin.length);
        for (let i = 0; i < bin.length; i++) bytes[i] = bin.charCodeAt(i);
        const text = new TextDecoder().decode(bytes);
        context.logger.info(
          "Exported notebook {path} ({format}, {bytes} bytes)",
          { path: args.path, format: args.format, bytes: text.length },
        );
        return { dataHandles: [], outputs: { content: text } };
      },
    },

    delete: {
      description: "Delete a notebook from the workspace via " +
        "POST /api/2.0/workspace/delete.",
      arguments: DeleteArgs,
      execute: async (
        args: z.infer<typeof DeleteArgs>,
        context: {
          globalArgs: GlobalArgs;
          logger: Logger;
        },
      ) => {
        await dbxFetch(
          context.globalArgs,
          "/api/2.0/workspace/delete",
          {
            method: "POST",
            body: JSON.stringify({
              path: args.path,
              recursive: args.recursive,
            }),
          },
        );
        context.logger.info("Deleted notebook {path}", { path: args.path });
        return { dataHandles: [] };
      },
    },
  },
};
