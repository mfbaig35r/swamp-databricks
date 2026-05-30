// Shared helpers for @mfbaig35r/databricks models.
// Keep model files thin: this owns workspace auth, the bearer fetch, and
// schema definitions that every model needs.

import { z } from "npm:zod@4";

// ---------------------------------------------------------------------------
// Global args
// ---------------------------------------------------------------------------

export const GlobalArgsSchema = z.object({
  workspace_url: z.string().url(),
  auth_kind: z.enum(["pat", "oauth_m2m", "azure_msi"]).default("pat"),
  token: z.string().optional().describe(
    'Resolved PAT value. Pass via CEL: ${{ vault.get("databricks", "pat") }}',
  ),
  oauth_client_id: z.string().optional(),
  oauth_client_secret: z.string().optional().describe(
    'Resolved OAuth M2M client secret. Pass via CEL: ${{ vault.get("...", "...") }}',
  ),
  azure_msi_client_id: z.string().optional(),
});

export type GlobalArgs = z.infer<typeof GlobalArgsSchema>;

// ---------------------------------------------------------------------------
// Auth + fetch
// ---------------------------------------------------------------------------

export async function resolveToken(globalArgs: GlobalArgs): Promise<string> {
  switch (globalArgs.auth_kind) {
    case "pat": {
      if (!globalArgs.token) {
        throw new Error(
          "auth_kind=pat requires globalArgs.token (resolved PAT value, " +
            'pass via CEL: ${{ vault.get("databricks", "pat") }})',
        );
      }
      return globalArgs.token;
    }
    case "oauth_m2m": {
      if (!globalArgs.oauth_client_id || !globalArgs.oauth_client_secret) {
        throw new Error(
          "auth_kind=oauth_m2m requires globalArgs.oauth_client_id and oauth_client_secret",
        );
      }
      const tokenUrl = `${globalArgs.workspace_url}/oidc/v1/token`;
      const body = new URLSearchParams({
        grant_type: "client_credentials",
        scope: "all-apis",
      });
      const auth = btoa(
        `${globalArgs.oauth_client_id}:${globalArgs.oauth_client_secret}`,
      );
      const res = await fetch(tokenUrl, {
        method: "POST",
        headers: {
          "Authorization": `Basic ${auth}`,
          "Content-Type": "application/x-www-form-urlencoded",
        },
        body: body.toString(),
      });
      if (!res.ok) {
        throw new Error(
          `oauth_m2m token mint failed: ${res.status} ${await res.text()}`,
        );
      }
      const json = await res.json() as { access_token: string };
      return json.access_token;
    }
    case "azure_msi":
      throw new Error(
        "azure_msi auth is not implemented in @mfbaig35r/databricks v0.2",
      );
  }
}

export async function dbxFetch(
  globalArgs: GlobalArgs,
  path: string,
  init: RequestInit = {},
): Promise<Record<string, unknown>> {
  const token = await resolveToken(globalArgs);
  const res = await fetch(`${globalArgs.workspace_url}${path}`, {
    ...init,
    headers: {
      "Authorization": `Bearer ${token}`,
      "Content-Type": "application/json",
      ...(init.headers ?? {}),
    },
  });
  if (!res.ok) {
    throw new Error(
      `databricks ${path} ${res.status}: ${await res.text()}`,
    );
  }
  return await res.json() as Record<string, unknown>;
}

// ---------------------------------------------------------------------------
// Small helpers used by multiple models
// ---------------------------------------------------------------------------

export async function sha256(input: string): Promise<string> {
  const buf = await crypto.subtle.digest(
    "SHA-256",
    new TextEncoder().encode(input),
  );
  return Array.from(new Uint8Array(buf))
    .map((b) => b.toString(16).padStart(2, "0"))
    .join("");
}

/** UTF-8 safe base64 encode (Databricks workspace import body). */
export function b64encode(s: string): string {
  const bytes = new TextEncoder().encode(s);
  let bin = "";
  for (let i = 0; i < bytes.length; i++) bin += String.fromCharCode(bytes[i]);
  return btoa(bin);
}

/**
 * Encode a workspace path into a Swamp-safe resource name.
 * Swamp rejects '/', '\\', '..', and null bytes in resource names.
 * "/Shared/foo/bar" -> "Shared:foo:bar"
 */
export function pathToResourceName(path: string): string {
  return path.replace(/^\//, "").replace(/\//g, ":");
}

// ---------------------------------------------------------------------------
// Shared context types for method execute signatures
// ---------------------------------------------------------------------------

export type Logger = {
  info: (msg: string, props: Record<string, unknown>) => void;
};

export type WriteResource = (
  specName: string,
  instanceName: string,
  data: Record<string, unknown>,
) => Promise<{ name: string }>;

export type ReadResource = (
  instanceName: string,
) => Promise<Record<string, unknown> | null>;
