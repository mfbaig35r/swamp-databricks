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

const GitProvider = z.enum([
  "gitHub",
  "bitbucketCloud",
  "gitLab",
  "azureDevOpsServices",
  "gitHubEnterprise",
  "bitbucketServer",
  "gitLabEnterpriseEdition",
  "awsCodeCommit",
]);

const SparseCheckout = z.object({
  patterns: z.array(z.string()).optional(),
});

const CreateArgs = z.object({
  name: z.string().min(1).max(255).describe(
    "Swamp-side handle. Workspace path becomes /Repos/<user>/<name> unless `path` is set.",
  ),
  url: z.string().url().describe("Git repository URL"),
  provider: GitProvider,
  path: z.string().regex(/^\/.+/).optional().describe(
    "Absolute workspace path. Defaults to /Repos/<current-user>/<name>.",
  ),
  branch: z.string().optional().describe(
    "Initial branch to check out. Defaults to remote default.",
  ),
  sparse_checkout: SparseCheckout.optional(),
});

const UpdateArgs = z.object({
  repo_ref: z.string(),
  branch: z.string().optional().describe(
    "New branch to check out. Triggers a pull. Mutually exclusive with `tag`.",
  ),
  tag: z.string().optional().describe(
    "Tag to check out. Mutually exclusive with `branch`.",
  ),
  sparse_checkout: SparseCheckout.optional(),
}).refine(
  (a) => !(a.branch && a.tag),
  { message: "branch and tag are mutually exclusive" },
);

const PullArgs = z.object({
  repo_ref: z.string(),
});

const RepoResourceSchema = z.object({
  repo_id: z.number().int(),
  name: z.string(),
  url: z.string().url(),
  provider: z.string(),
  path: z.string(),
  branch: z.string().optional(),
  head_commit_id: z.string().optional(),
  created_time_ms: z.number().int(),
  workspace_url: z.string().url(),
});

/**
 * `@mfbaig35r/databricks/repo`: Databricks Git Repos lifecycle.
 *
 * Real Databricks jobs typically reference notebooks via a Git Repo path
 * rather than uploading them to `/Shared/`. This model creates the repo at
 * a workspace path, switches branches, pulls updates, and deletes when done.
 *
 * Private repos require a workspace-level Git credential to be configured
 * (Settings -> User Settings -> Linked Accounts). Public repos work without
 * additional setup.
 *
 * @see https://docs.databricks.com/api/workspace/repos
 */
export const model = {
  type: "@mfbaig35r/databricks/repo",
  version: "2026.05.30.18",
  globalArguments: GlobalArgsSchema,

  resources: {
    "repo": {
      description: "A Databricks Git Repo, keyed by user-supplied name.",
      schema: RepoResourceSchema,
      lifetime: "infinite" as const,
      garbageCollection: 5,
    },
  },

  methods: {
    create: {
      description:
        "Create a Git Repo via POST /api/2.0/repos. Clones the repository " +
        "into the workspace at the resolved path.",
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
          url: args.url,
          provider: args.provider,
        };
        if (args.path) body.path = args.path;
        if (args.branch) body.branch = args.branch;
        if (args.sparse_checkout) body.sparse_checkout = args.sparse_checkout;
        const out = await dbxFetch(
          context.globalArgs,
          "/api/2.0/repos",
          { method: "POST", body: JSON.stringify(body) },
        );
        const repoId = out.id as number;
        context.logger.info(
          "Created repo {name} -> {repo_id}",
          { name: args.name, repo_id: repoId },
        );
        const handle = await context.writeResource("repo", args.name, {
          repo_id: repoId,
          name: args.name,
          url: args.url,
          provider: args.provider,
          path: (out.path as string) ?? args.path ?? "",
          branch: (out.branch as string | undefined) ?? args.branch,
          head_commit_id: out.head_commit_id as string | undefined,
          created_time_ms: Date.now(),
          workspace_url: context.globalArgs.workspace_url,
        });
        return { dataHandles: [handle] };
      },
    },

    read: {
      description: "GET /api/2.0/repos/{repo_id}.",
      arguments: z.object({ repo_ref: z.string() }),
      execute: async (
        args: { repo_ref: string },
        context: {
          globalArgs: GlobalArgs;
          readResource: ReadResource;
          logger: Logger;
        },
      ) => {
        const prior = await context.readResource(args.repo_ref);
        if (!prior) {
          throw new Error(
            `No stored 'repo' resource named '${args.repo_ref}'.`,
          );
        }
        const live = await dbxFetch(
          context.globalArgs,
          `/api/2.0/repos/${prior.repo_id}`,
        );
        context.logger.info(
          "Read repo {repo_id}",
          { repo_id: prior.repo_id },
        );
        return { dataHandles: [], outputs: { live } };
      },
    },

    update: {
      description:
        "PATCH /api/2.0/repos/{repo_id}. Switch branch/tag (triggers a pull) " +
        "or update sparse checkout.",
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
        const prior = await context.readResource(args.repo_ref);
        if (!prior) {
          throw new Error(
            `No stored 'repo' resource named '${args.repo_ref}'.`,
          );
        }
        const patch: Record<string, unknown> = {};
        if (args.branch) patch.branch = args.branch;
        if (args.tag) patch.tag = args.tag;
        if (args.sparse_checkout) patch.sparse_checkout = args.sparse_checkout;
        const out = await dbxFetch(
          context.globalArgs,
          `/api/2.0/repos/${prior.repo_id}`,
          { method: "PATCH", body: JSON.stringify(patch) },
        );
        context.logger.info(
          "Updated repo {repo_id}",
          { repo_id: prior.repo_id },
        );
        const handle = await context.writeResource(
          "repo",
          args.repo_ref,
          {
            ...prior,
            branch: (out.branch as string | undefined) ??
              args.branch ??
              prior.branch as string | undefined,
            head_commit_id: out.head_commit_id as string | undefined,
          },
        );
        return { dataHandles: [handle] };
      },
    },

    pull: {
      description:
        "PATCH /api/2.0/repos/{repo_id} with the current branch. Pulls the " +
        "current branch to the latest commit on remote. (Databricks requires " +
        "branch or tag in the PATCH body; this method re-sends the stored " +
        "branch from the resource. If branch is unknown, read the live state " +
        "first via GET .../{repo_id}.)",
      arguments: PullArgs,
      execute: async (
        args: z.infer<typeof PullArgs>,
        context: {
          globalArgs: GlobalArgs;
          readResource: ReadResource;
          writeResource: WriteResource;
          logger: Logger;
        },
      ) => {
        const prior = await context.readResource(args.repo_ref);
        if (!prior) {
          throw new Error(
            `No stored 'repo' resource named '${args.repo_ref}'.`,
          );
        }
        let branch = prior.branch as string | undefined;
        if (!branch) {
          const live = await dbxFetch(
            context.globalArgs,
            `/api/2.0/repos/${prior.repo_id}`,
          );
          branch = live.branch as string | undefined;
        }
        if (!branch) {
          throw new Error(
            `Cannot pull repo ${prior.repo_id}: current branch unknown. ` +
              `Use 'update' with an explicit branch instead.`,
          );
        }
        const out = await dbxFetch(
          context.globalArgs,
          `/api/2.0/repos/${prior.repo_id}`,
          { method: "PATCH", body: JSON.stringify({ branch }) },
        );
        context.logger.info(
          "Pulled repo {repo_id}",
          { repo_id: prior.repo_id },
        );
        const handle = await context.writeResource(
          "repo",
          args.repo_ref,
          {
            ...prior,
            head_commit_id: out.head_commit_id as string | undefined,
          },
        );
        return { dataHandles: [handle] };
      },
    },

    delete: {
      description: "DELETE /api/2.0/repos/{repo_id}.",
      arguments: z.object({ repo_ref: z.string() }),
      execute: async (
        args: { repo_ref: string },
        context: {
          globalArgs: GlobalArgs;
          readResource: ReadResource;
          logger: Logger;
        },
      ) => {
        const prior = await context.readResource(args.repo_ref);
        if (!prior) {
          throw new Error(
            `No stored 'repo' resource named '${args.repo_ref}'.`,
          );
        }
        await dbxFetch(
          context.globalArgs,
          `/api/2.0/repos/${prior.repo_id}`,
          { method: "DELETE" },
        );
        context.logger.info(
          "Deleted repo {repo_id}",
          { repo_id: prior.repo_id },
        );
        return { dataHandles: [] };
      },
    },

    list: {
      description: "GET /api/2.0/repos (list all repos).",
      arguments: z.object({
        path_prefix: z.string().optional().describe(
          "Filter by workspace path prefix (e.g. /Repos/me)",
        ),
        next_page_token: z.string().optional(),
      }),
      execute: async (
        args: { path_prefix?: string; next_page_token?: string },
        context: { globalArgs: GlobalArgs; logger: Logger },
      ) => {
        const qs = new URLSearchParams();
        if (args.path_prefix) qs.set("path_prefix", args.path_prefix);
        if (args.next_page_token) {
          qs.set("next_page_token", args.next_page_token);
        }
        const res = await dbxFetch(
          context.globalArgs,
          `/api/2.0/repos${qs.toString() ? "?" + qs : ""}`,
        );
        const repos = (res.repos ?? []) as Array<{
          id: number;
          path: string;
          url: string;
          branch?: string;
        }>;
        context.logger.info("Listed {count} repos", { count: repos.length });
        return { dataHandles: [], outputs: { repos } };
      },
    },
  },
};
