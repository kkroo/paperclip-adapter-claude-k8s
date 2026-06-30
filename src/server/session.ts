import type { AdapterSessionCodec } from "@paperclipai/adapter-utils";

function readNonEmptyString(value: unknown): string | null {
  return typeof value === "string" && value.trim().length > 0 ? value.trim() : null;
}

function extractSessionFields(record: Record<string, unknown>) {
  const sessionId = readNonEmptyString(record.sessionId) ?? readNonEmptyString(record.session_id);
  const cwd =
    readNonEmptyString(record.cwd) ??
    readNonEmptyString(record.workdir) ??
    readNonEmptyString(record.folder);
  const workspaceId = readNonEmptyString(record.workspaceId) ?? readNonEmptyString(record.workspace_id);
  const repoUrl = readNonEmptyString(record.repoUrl) ?? readNonEmptyString(record.repo_url);
  const repoRef = readNonEmptyString(record.repoRef) ?? readNonEmptyString(record.repo_ref);
  const promptBundleKey =
    readNonEmptyString(record.promptBundleKey) ?? readNonEmptyString(record.prompt_bundle_key);
  const model = readNonEmptyString(record.model);
  return { sessionId, cwd, workspaceId, repoUrl, repoRef, promptBundleKey, model };
}

export const sessionCodec: AdapterSessionCodec = {
  deserialize(raw: unknown) {
    if (typeof raw !== "object" || raw === null || Array.isArray(raw)) return null;
    const { sessionId, cwd, workspaceId, repoUrl, repoRef, promptBundleKey, model } =
      extractSessionFields(raw as Record<string, unknown>);
    if (!sessionId) return null;
    return {
      sessionId,
      ...(cwd ? { cwd } : {}),
      ...(workspaceId ? { workspaceId } : {}),
      ...(repoUrl ? { repoUrl } : {}),
      ...(repoRef ? { repoRef } : {}),
      ...(promptBundleKey ? { promptBundleKey } : {}),
      ...(model ? { model } : {}),
    };
  },
  serialize(params: Record<string, unknown> | null) {
    if (!params) return null;
    const { sessionId, cwd, workspaceId, repoUrl, repoRef, promptBundleKey, model } =
      extractSessionFields(params);
    if (!sessionId) return null;
    return {
      sessionId,
      ...(cwd ? { cwd } : {}),
      ...(workspaceId ? { workspaceId } : {}),
      ...(repoUrl ? { repoUrl } : {}),
      ...(repoRef ? { repoRef } : {}),
      ...(promptBundleKey ? { promptBundleKey } : {}),
      ...(model ? { model } : {}),
    };
  },
  getDisplayId(params: Record<string, unknown> | null) {
    if (!params) return null;
    return readNonEmptyString(params.sessionId) ?? readNonEmptyString(params.session_id);
  },
};
