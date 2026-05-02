import type * as k8s from "@kubernetes/client-node";
import type { AdapterExecutionContext } from "@paperclipai/adapter-utils";
import {
  asString,
  asNumber,
  asBoolean,
  asStringArray,
  parseObject,
  buildPaperclipEnv,
  renderTemplate,
} from "@paperclipai/adapter-utils/server-utils";
import { createHash } from "node:crypto";
import { readFileSync } from "node:fs";
import type { ClaudePromptBundle } from "./prompt-cache.js";

/**
 * Path to the project-scope .mcp.json that paperclip's helm-chart seed-init
 * writes on every pod start. The adapter runs inside the paperclip
 * StatefulSet pod, which mounts the same /paperclip PVC the Job pods will
 * mount, so reading this path here gives us the exact baseline the Job
 * pod would otherwise inherit. Read lazily (only when an agent actually
 * supplies adapterConfig.mcpServers) so the adapter does not require the
 * file to exist for normal operation — and so unit tests don't blow up.
 */
const SHARED_MCP_BASELINE_PATH = "/paperclip/.mcp.json";

function loadSharedMcpBaseline(): Record<string, unknown> {
  try {
    const raw = readFileSync(SHARED_MCP_BASELINE_PATH, "utf8");
    const parsed = JSON.parse(raw) as { mcpServers?: unknown };
    if (parsed && typeof parsed === "object" && parsed.mcpServers && typeof parsed.mcpServers === "object") {
      return parsed.mcpServers as Record<string, unknown>;
    }
  } catch {
    // Missing / unreadable / malformed baseline → start from empty.
    // Per-agent overrides alone are still a valid mcp.json.
  }
  return {};
}

function assertSafePathComponent(field: string, value: string): void {
  if (!/^[a-zA-Z0-9-]+$/.test(value)) {
    throw new Error(`Invalid ${field} for log path: ${value}`);
  }
}

function sanitizeForK8sPath(value: string): string {
  return value.replace(/[^a-zA-Z0-9-]/g, "");
}

export function buildPodLogPath(companyId: string, agentId: string, runId: string): string {
  return `/paperclip/instances/default/data/run-logs/${companyId}/${agentId}/${runId}.pod.ndjson`;
}

/** Prompts above this size (bytes) are staged via a Secret instead of an
 *  init container env var, protecting against the ~1 MiB PodSpec limit. */
const LARGE_PROMPT_THRESHOLD_BYTES = 256 * 1024;

// Inline prompt assembly — these functions are not yet in the published adapter-utils
function joinPromptSections(sections: string[], separator = "\n\n"): string {
  return sections.filter((s) => s.trim().length > 0).join(separator);
}

function stringifyPaperclipWakePayload(wake: unknown): string | null {
  if (!wake || typeof wake !== "object") return null;
  try {
    const json = JSON.stringify(wake);
    return json === "{}" ? null : json;
  } catch {
    return null;
  }
}

function renderPaperclipWakePrompt(wake: unknown, _opts?: { resumedSession?: boolean }): string {
  if (!wake || typeof wake !== "object") return "";
  const w = wake as Record<string, unknown>;
  const reason = typeof w.reason === "string" ? w.reason.trim() : "";
  const comments = Array.isArray(w.comments) ? w.comments : [];
  if (!reason && comments.length === 0) return "";
  const parts: string[] = [];
  if (reason) parts.push(`Wake reason: ${reason}`);
  for (const c of comments) {
    if (typeof c === "object" && c !== null) {
      const comment = c as Record<string, unknown>;
      const body = typeof comment.body === "string" ? comment.body.trim() : "";
      if (body) parts.push(`Comment: ${body}`);
    }
  }
  return parts.join("\n\n");
}
import type { SelfPodInfo } from "./k8s-client.js";

/**
 * Parse a config value that may be either a JSON object or multiline
 * `key=value` text (one pair per line).  This fixes the config-hint
 * parity issue where textarea hints promise `key=value` per line but
 * `parseObject` only handles JSON.
 */
function parseKeyValueConfig(raw: unknown): Record<string, string> {
  if (typeof raw === "object" && raw !== null && !Array.isArray(raw)) {
    // Already an object (JSON was parsed upstream)
    const result: Record<string, string> = {};
    for (const [k, v] of Object.entries(raw as Record<string, unknown>)) {
      if (typeof v === "string") result[k] = v;
    }
    return result;
  }
  if (typeof raw !== "string" || !raw.trim()) return {};
  // Try JSON parse first
  try {
    const parsed = JSON.parse(raw);
    if (typeof parsed === "object" && parsed !== null && !Array.isArray(parsed)) {
      const result: Record<string, string> = {};
      for (const [k, v] of Object.entries(parsed as Record<string, unknown>)) {
        if (typeof v === "string") result[k] = v;
      }
      return result;
    }
  } catch {
    // Not JSON — fall through to key=value parsing
  }
  // Parse key=value lines
  const result: Record<string, string> = {};
  for (const line of raw.split(/\r?\n/)) {
    const trimmed = line.trim();
    if (!trimmed || trimmed.startsWith("#")) continue;
    const eqIdx = trimmed.indexOf("=");
    if (eqIdx <= 0) continue;
    const key = trimmed.slice(0, eqIdx).trim();
    const value = trimmed.slice(eqIdx + 1).trim();
    if (key) result[key] = value;
  }
  return result;
}

export interface JobBuildInput {
  ctx: AdapterExecutionContext;
  selfPod: SelfPodInfo;
  /** Prepared prompt bundle (skills + instructions). When provided, --add-dir and --append-system-prompt-file use bundle paths. */
  promptBundle?: ClaudePromptBundle | null;
}

/** When the prompt exceeds the env-var size limit, the manifest uses a
 *  Secret-backed volume instead of the init container's PROMPT_CONTENT env.
 *  The caller must create this Secret before the Job and clean it up after. */
export interface PromptSecret {
  name: string;
  namespace: string;
  data: Record<string, string>;
}

export interface JobBuildResult {
  job: k8s.V1Job;
  jobName: string;
  namespace: string;
  prompt: string;
  claudeArgs: string[];
  promptMetrics: Record<string, number>;
  /** Non-null when the prompt is too large for an env var and must be
   *  staged as a K8s Secret before creating the Job. */
  promptSecret: PromptSecret | null;
  /** User-supplied extra labels that were dropped because they used a reserved prefix. */
  skippedLabels: string[];
  /** Path to the pod log file on the shared PVC. */
  podLogPath: string;
}

function sanitizeForK8sName(value: string, maxLen = 16): string {
  // Trim trailing hyphens after slicing so names don't end with `-` when
  // truncation lands on a hyphen boundary (finding #16, FAR-15).
  return value.toLowerCase().replace(/[^a-z0-9-]/g, "").slice(0, maxLen).replace(/-+$/, "");
}

/**
 * Sanitize a string for use as a Kubernetes label value (RFC 1123 subset:
 * `[a-zA-Z0-9]([-_.a-zA-Z0-9]*[a-zA-Z0-9])?`, max 63 chars).  Returns `null`
 * when no usable characters remain — the caller should omit the label.
 */
export function sanitizeLabelValue(value: string, maxLen = 63): string | null {
  const cleaned = value.replace(/[^a-zA-Z0-9._-]/g, "").slice(0, maxLen);
  const trimmed = cleaned.replace(/^[^a-zA-Z0-9]+/, "").replace(/[^a-zA-Z0-9]+$/, "");
  return trimmed.length > 0 ? trimmed : null;
}

/**
 * Build a short deterministic hash suffix from the raw inputs to avoid
 * collisions when sanitized slugs happen to be identical.
 */
function shortHash(input: string, len = 6): string {
  return createHash("sha256").update(input).digest("hex").slice(0, len);
}

function buildEnvVars(
  ctx: AdapterExecutionContext,
  selfPod: SelfPodInfo,
  config: Record<string, unknown>,
): k8s.V1EnvVar[] {
  const { runId, agent, context } = ctx;
  const envConfig = parseObject(config.env);

  // Layer 1: PAPERCLIP_* base vars
  const paperclipEnv = buildPaperclipEnv(agent);

  // Layer 2: Context vars (run, wake, workspace — same as claude_local)
  paperclipEnv.PAPERCLIP_RUN_ID = runId;

  const setIfPresent = (envKey: string, value: unknown) => {
    if (typeof value === "string" && value.trim().length > 0) {
      paperclipEnv[envKey] = value.trim();
    }
  };

  setIfPresent("PAPERCLIP_TASK_ID", context.taskId ?? context.issueId);
  setIfPresent("PAPERCLIP_WAKE_REASON", context.wakeReason);
  setIfPresent("PAPERCLIP_WAKE_COMMENT_ID", context.wakeCommentId ?? context.commentId);
  setIfPresent("PAPERCLIP_APPROVAL_ID", context.approvalId);
  setIfPresent("PAPERCLIP_APPROVAL_STATUS", context.approvalStatus);

  const wakePayloadJson = stringifyPaperclipWakePayload(context.paperclipWake);
  if (wakePayloadJson) {
    paperclipEnv.PAPERCLIP_WAKE_PAYLOAD_JSON = wakePayloadJson;
  }

  const workspaceContext = parseObject(context.paperclipWorkspace);
  setIfPresent("PAPERCLIP_WORKSPACE_CWD", workspaceContext.cwd);
  setIfPresent("PAPERCLIP_WORKSPACE_SOURCE", workspaceContext.source);
  setIfPresent("PAPERCLIP_WORKSPACE_STRATEGY", workspaceContext.strategy);
  setIfPresent("PAPERCLIP_WORKSPACE_ID", workspaceContext.workspaceId);
  setIfPresent("PAPERCLIP_WORKSPACE_REPO_URL", workspaceContext.repoUrl);
  setIfPresent("PAPERCLIP_WORKSPACE_REPO_REF", workspaceContext.repoRef);
  setIfPresent("PAPERCLIP_WORKSPACE_BRANCH", workspaceContext.branchName);
  setIfPresent("PAPERCLIP_WORKSPACE_WORKTREE_PATH", workspaceContext.worktreePath);
  setIfPresent("AGENT_HOME", workspaceContext.agentHome);

  const linkedIssueIds = Array.isArray(context.issueIds)
    ? context.issueIds.filter((v): v is string => typeof v === "string" && v.trim().length > 0)
    : [];
  if (linkedIssueIds.length > 0) {
    paperclipEnv.PAPERCLIP_LINKED_ISSUE_IDS = linkedIssueIds.join(",");
  }

  if (Array.isArray(context.paperclipWorkspaces) && context.paperclipWorkspaces.length > 0) {
    paperclipEnv.PAPERCLIP_WORKSPACES_JSON = JSON.stringify(context.paperclipWorkspaces);
  }
  if (Array.isArray(context.paperclipRuntimeServiceIntents) && context.paperclipRuntimeServiceIntents.length > 0) {
    paperclipEnv.PAPERCLIP_RUNTIME_SERVICE_INTENTS_JSON = JSON.stringify(context.paperclipRuntimeServiceIntents);
  }
  if (Array.isArray(context.paperclipRuntimeServices) && context.paperclipRuntimeServices.length > 0) {
    paperclipEnv.PAPERCLIP_RUNTIME_SERVICES_JSON = JSON.stringify(context.paperclipRuntimeServices);
  }
  setIfPresent("PAPERCLIP_RUNTIME_PRIMARY_URL", context.paperclipRuntimePrimaryUrl);

  // Auth token for agent callback to Paperclip API
  if (ctx.authToken) {
    paperclipEnv.PAPERCLIP_API_KEY = ctx.authToken;
  }

  // PAPERCLIP_API_URL is inherited from the Deployment env via selfPod.inheritedEnv.
  // buildPaperclipEnv() sets a localhost value which is wrong for Job pods —
  // the inherited value (set in the infra repo) points to the in-cluster service.
  if (selfPod.inheritedEnv.PAPERCLIP_API_URL) {
    paperclipEnv.PAPERCLIP_API_URL = selfPod.inheritedEnv.PAPERCLIP_API_URL;
  }
  // PAPERCLIP_AGENT_ID and PAPERCLIP_COMPANY_ID come from buildPaperclipEnv.
  // PAPERCLIP_RUN_ID is set above. Together they form the runContext that
  // the bundled paperclip-mcp-server stdio bridge uses to authenticate
  // /plugins/tools/execute calls when an agent invokes a plugin tool via MCP.

  // Layer 3: Inherited from Deployment (Bedrock, API keys, etc.)
  const merged: Record<string, string> = {
    ...selfPod.inheritedEnv,
    ...paperclipEnv,
  };

  // Layer 4: User-defined overrides from adapterConfig.env (wins over everything)
  for (const [key, value] of Object.entries(envConfig)) {
    if (typeof value === "string") merged[key] = value;
  }

  // HOME must be /paperclip to match PVC mount and enable session resume
  merged.HOME = "/paperclip";

  // Convert literal env to V1EnvVar array
  const envVars: k8s.V1EnvVar[] = Object.entries(merged).map(([name, value]) => ({
    name,
    value,
  }));

  // Append valueFrom entries from the Deployment container (secretKeyRef,
  // configMapKeyRef, fieldRef, etc.).  Skip any whose name was already set
  // by a literal value — the literal value wins (same precedence as above).
  const literalNames = new Set(Object.keys(merged));
  for (const entry of selfPod.inheritedEnvValueFrom) {
    if (!literalNames.has(entry.name)) {
      envVars.push(entry);
    }
  }

  return envVars;
}

export function buildJobManifest(input: JobBuildInput): JobBuildResult {
  const { ctx, selfPod, promptBundle } = input;
  const { runId, agent, runtime, config: rawConfig, context } = ctx;
  const config = parseObject(rawConfig);

  // Resolve config values
  const namespace = asString(config.namespace, "") || selfPod.namespace;
  const image = asString(config.image, "") || selfPod.image;
  const model = asString(config.model, "");
  const effort = asString(config.effort, "");
  const maxTurns = asNumber(config.maxTurnsPerRun, 0);
  // K8s Job pods are always unattended — no one to approve permission prompts
  const dangerouslySkipPermissions = asBoolean(config.dangerouslySkipPermissions, true);
  const extraArgs = asStringArray(config.extraArgs);
  const timeoutSec = asNumber(config.timeoutSec, 0);
  const ttlSeconds = asNumber(config.ttlSecondsAfterFinished, 300);
  const hasConfigKey = (key: string) => Object.prototype.hasOwnProperty.call(config, key);
  const configuredNodeSelector = parseKeyValueConfig(config.nodeSelector);
  const nodeSelector = hasConfigKey("nodeSelector") ? configuredNodeSelector : selfPod.nodeSelector;
  const configuredTolerations = Array.isArray(config.tolerations) ? config.tolerations : [];
  const tolerations = hasConfigKey("tolerations") ? configuredTolerations : selfPod.tolerations;
  const extraLabels = parseKeyValueConfig(config.labels);

  // Resolve working directory — use workspace cwd, fall back to /paperclip
  const workspaceContext = parseObject(context.paperclipWorkspace);
  const workspaceCwd = asString(workspaceContext.cwd, "");
  const configuredCwd = asString(config.cwd, "");
  const workingDir = workspaceCwd || configuredCwd || "/paperclip";

  // Build a deterministic, collision-resistant job name within the 63-char
  // DNS label limit.  Layout: "ac-{agentSlug}-{runSlug}-{hash}" where the
  // hash is derived from the raw (un-truncated) agent+run IDs.
  const agentSlug = sanitizeForK8sName(agent.id, 16);
  const runSlug = sanitizeForK8sName(runId, 16);
  const hash = shortHash(`${agent.id}:${runId}`);
  const jobName = `ac-${agentSlug}-${runSlug}-${hash}`;

  // Build prompt (same logic as claude_local)
  const promptTemplate = asString(
    config.promptTemplate,
    "You are agent {{agent.id}} ({{agent.name}}). Continue your Paperclip work.",
  );
  const bootstrapPromptTemplate = asString(config.bootstrapPromptTemplate, "");
  const runtimeSessionParams = parseObject(runtime.sessionParams);
  const runtimeSessionId = asString(runtimeSessionParams.sessionId, runtime.sessionId ?? "");
  const templateData = {
    agentId: agent.id,
    companyId: agent.companyId,
    runId,
    company: { id: agent.companyId },
    agent,
    run: { id: runId, source: "on_demand" },
    context,
  };
  const renderedBootstrapPrompt =
    !runtimeSessionId && bootstrapPromptTemplate.trim().length > 0
      ? renderTemplate(bootstrapPromptTemplate, templateData).trim()
      : "";
  const wakePrompt = renderPaperclipWakePrompt(context.paperclipWake, { resumedSession: Boolean(runtimeSessionId) });
  const shouldUseResumeDeltaPrompt = Boolean(runtimeSessionId) && wakePrompt.length > 0;
  const renderedPrompt = shouldUseResumeDeltaPrompt ? "" : renderTemplate(promptTemplate, templateData);
  const sessionHandoffNote = asString(context.paperclipSessionHandoffMarkdown, "").trim();
  const prompt = joinPromptSections([
    renderedBootstrapPrompt,
    wakePrompt,
    sessionHandoffNote,
    renderedPrompt,
  ]);
  const promptMetrics = {
    promptChars: prompt.length,
    bootstrapPromptChars: renderedBootstrapPrompt.length,
    wakePromptChars: wakePrompt.length,
    sessionHandoffChars: sessionHandoffNote.length,
    heartbeatPromptChars: renderedPrompt.length,
  };

  // Per-agent MCP layering — adapterConfig.mcpServers is a map of
  // server-name → MCP server spec ({command, args, env, ...} for stdio
  // or {type: "http"|"sse", url} for transport-typed entries).
  // When set, we merge with the shared baseline at /paperclip/.mcp.json
  // (paperclip + prometheus + tempo + kubernetes-readonly + github,
  // written by the helm chart's seed-init) and ship the result with
  // claude --mcp-config + --strict-mcp-config so the agent gets exactly
  // the merged set with no surprise reads from disk.
  // Spread semantics: per-agent entries override baseline by name; new
  // entries are added. To swap kubernetes-readonly for ns-rw or admin,
  // override the "kubernetes" key. To add figma, set a new "figma" key.
  const perAgentMcpServers = parseObject(config.mcpServers);
  const hasPerAgentMcp = Object.keys(perAgentMcpServers).length > 0;
  let mergedMcpJson: string | null = null;
  if (hasPerAgentMcp) {
    const baseline = loadSharedMcpBaseline();
    const merged = { ...baseline, ...perAgentMcpServers };
    mergedMcpJson = JSON.stringify({ mcpServers: merged });
  }

  // Build Claude CLI args
  // Prefer the bundle's materialized instructions file over the raw config path.
  // Never inject --append-system-prompt-file on session resumes — the instructions
  // are already in the session cache and re-injecting wastes tokens.
  const rawInstructionsFilePath = asString(config.instructionsFilePath, "").trim();
  const effectiveInstructionsFilePath =
    promptBundle?.instructionsFilePath ?? (rawInstructionsFilePath || null);
  const claudeArgs = ["--print", "-", "--output-format", "stream-json", "--verbose"];
  if (runtimeSessionId) claudeArgs.push("--resume", runtimeSessionId);
  if (dangerouslySkipPermissions) claudeArgs.push("--dangerously-skip-permissions");
  if (model) claudeArgs.push("--model", model);
  if (effort) claudeArgs.push("--effort", effort);
  if (maxTurns > 0) claudeArgs.push("--max-turns", String(maxTurns));
  if (effectiveInstructionsFilePath && !runtimeSessionId) {
    claudeArgs.push("--append-system-prompt-file", effectiveInstructionsFilePath);
  }
  if (promptBundle) claudeArgs.push("--add-dir", promptBundle.addDir);
  if (mergedMcpJson) {
    // --strict-mcp-config makes claude ignore the project-scope file at
    // workingDir/.mcp.json and use ONLY the file we materialize below.
    // Without it, claude would still read /paperclip/.mcp.json and merge
    // it on top of ours — losing per-agent overrides like kubernetes
    // ns-rw replacing readonly, since the project-scope file would win.
    claudeArgs.push("--mcp-config", "/tmp/prompt/mcp.json", "--strict-mcp-config");
  }
  if (extraArgs.length > 0) claudeArgs.push(...extraArgs);

  // Build env vars
  const envVars = buildEnvVars(ctx, selfPod, config);

  // Resource defaults — UI stores dotted keys (e.g. "resources.requests.cpu")
  // as flat config entries, so read them directly from config with the dotted key.
  const containerResources: k8s.V1ResourceRequirements = {
    requests: {
      cpu: asString(config["resources.requests.cpu"], "1000m"),
      memory: asString(config["resources.requests.memory"], "2Gi"),
    },
    limits: {
      cpu: asString(config["resources.limits.cpu"], "4000m"),
      memory: asString(config["resources.limits.memory"], "8Gi"),
    },
  };

  // Labels — system identifiers must pass RFC 1123 label value format.
  const sanitizedAgentId = sanitizeLabelValue(agent.id);
  const sanitizedRunId = sanitizeLabelValue(runId);
  const sanitizedCompanyId = sanitizeLabelValue(agent.companyId);
  const skippedLabels: string[] = [];
  if (!sanitizedRunId) skippedLabels.push("paperclip.io/run-id");
  if (!sanitizedCompanyId) skippedLabels.push("paperclip.io/company-id");
  const labels: Record<string, string> = {
    "app.kubernetes.io/managed-by": "paperclip",
    "app.kubernetes.io/component": "agent-job",
    // sanitizedAgentId null-check is enforced in execute.ts before Job creation
    "paperclip.io/agent-id": sanitizedAgentId ?? agent.id,
    "paperclip.io/adapter-type": "claude_k8s",
  };
  if (sanitizedRunId) labels["paperclip.io/run-id"] = sanitizedRunId;
  if (sanitizedCompanyId) labels["paperclip.io/company-id"] = sanitizedCompanyId;
  // Reattach-target labels: let a future execute() identify this Job as the
  // continuation of the same logical unit of work (same task + same resume
  // session) so it can attach to the running pod across a Paperclip restart
  // instead of deleting it and starting over (FAR-124).
  const taskIdRaw = asString(context.taskId, "") || asString(context.issueId, "");
  const taskLabel = taskIdRaw ? sanitizeLabelValue(taskIdRaw) : null;
  if (taskLabel) labels["paperclip.io/task-id"] = taskLabel;
  const sessionLabel = runtimeSessionId ? sanitizeLabelValue(runtimeSessionId) : null;
  if (sessionLabel) labels["paperclip.io/session-id"] = sessionLabel;
  for (const [key, value] of Object.entries(extraLabels)) {
    if (key.startsWith("paperclip.io/") || key.startsWith("app.kubernetes.io/")) {
      skippedLabels.push(key);
    } else {
      labels[key] = value;
    }
  }

  // Volumes
  const volumes: k8s.V1Volume[] = [
    {
      name: "prompt",
      emptyDir: {},
    },
  ];
  const volumeMounts: k8s.V1VolumeMount[] = [
    {
      name: "prompt",
      mountPath: "/tmp/prompt",
    },
  ];

  // Mount shared PVC for /paperclip (session state, workspaces, data).
  //
  // Phase E.1 — when paperclip's k8s execution target supplies
  // `workspaceVolumeClaim` / `workspaceMountPath`, those win over the
  // adapter defaults derived from selfPod.  When unset, retain the
  // selfPod-derived PVC and the conventional /paperclip mount path.
  // `effectiveConfig` (built in execute.ts) lands these env-supplied
  // values into `config.workspaceVolumeClaim` / `config.workspaceMountPath`.
  const envWorkspaceClaim = asString(config.workspaceVolumeClaim, "").trim();
  const envWorkspaceMountPath = asString(config.workspaceMountPath, "").trim();
  const dataClaimName = envWorkspaceClaim || selfPod.pvcClaimName || "";
  const dataMountPath = envWorkspaceMountPath || "/paperclip";
  if (dataClaimName) {
    volumes.push({
      name: "data",
      persistentVolumeClaim: { claimName: dataClaimName },
    });
    volumeMounts.push({
      name: "data",
      mountPath: dataMountPath,
    });
  }

  // Mount secret volumes inherited from the Deployment pod
  for (const sv of selfPod.secretVolumes) {
    volumes.push({
      name: sv.volumeName,
      secret: { secretName: sv.secretName, defaultMode: sv.defaultMode, optional: true },
    });
    volumeMounts.push({
      name: sv.volumeName,
      mountPath: sv.mountPath,
      readOnly: true,
    });
  }

  // Security context matching the main Deployment
  const securityContext: k8s.V1SecurityContext = {
    capabilities: { drop: ["ALL"] },
    readOnlyRootFilesystem: false,
    runAsNonRoot: true,
    runAsUser: 1000,
    allowPrivilegeEscalation: false,
  };

  const podSecurityContext: k8s.V1PodSecurityContext = {
    runAsNonRoot: true,
    runAsUser: 1000,
    runAsGroup: 1000,
    fsGroup: 1000,
    fsGroupChangePolicy: "OnRootMismatch",
  };

  // Build the claude command string for the main container
  const claudeArgsEscaped = claudeArgs.map((a) => `'${a.replace(/'/g, "'\\''")}'`).join(" ");
  const logPathCompanyId = sanitizeForK8sPath(agent.companyId);
  const logPathAgentId = sanitizeForK8sPath(agent.id);
  const logPathRunId = sanitizeForK8sPath(runId);
  assertSafePathComponent("companyId", logPathCompanyId);
  assertSafePathComponent("agentId", logPathAgentId);
  assertSafePathComponent("runId", logPathRunId);
  const podLogPath = buildPodLogPath(logPathCompanyId, logPathAgentId, logPathRunId);
  // Refresh OAuth credentials via ccrotate before invoking claude. The shared
  // /paperclip/.claude/.credentials.json on the RWX PVC may contain an expired
  // access token (claude OAuth tokens last ~30-60 min and the paperclip pod
  // doesn't refresh them automatically — that's ccrotate's job). Without this,
  // claude in the Job pod fails with `401 Invalid authentication credentials`
  // whenever the cached token is older than its expiresAt.
  //
  // Just `next --yes` — no pre-snap. Claude-code's installed Stop hook
  // already snaps the active account's just-refreshed tokens at session
  // end, so the previous Job's exit handles the normal save path. Doing
  // an extra `snap --force` here under multiple-concurrent-Jobs raced
  // with another Job's `next` mid-write of the active config files
  // (ccrotate.js writeClaudeFiles writes credentials and config in two
  // steps); a pre-snap reading partial state then committed mismatched
  // creds into a profile labeled with the previous account's email,
  // clobbering tokens across unrelated profiles. Edge case lost: if a
  // prior agent crashed without firing its Stop hook, its just-refreshed
  // access token isn't saved — recoverable on the next switchTo via the
  // refresh-token, costing one extra OAuth refresh.
  // `--yes` is still required because Job pods have no stdin, so without
  // it ccrotate prompts and hangs/exits when all accounts are at extra
  // usage. Failure is non-fatal: if ccrotate isn't on PATH or all
  // accounts are exhausted, we still try claude with whatever
  // credentials are on disk so the operator gets a meaningful
  // 401-from-claude instead of an opaque init failure.
  const ccrotateRefresh = `(command -v ccrotate >/dev/null 2>&1 && ccrotate next --yes --target claude >/dev/null 2>&1) || true`;
  // `set -o pipefail` so a claude binary crash (OOM, segfault, missing-bin)
  // surfaces as a non-zero shell exit code instead of being masked by tee's
  // exit code. Without pipefail the pod marks Succeeded even when claude
  // never emits any stream-json — paperclip-server's parser only catches
  // type:error events from inside the JSON stream, not pre-stream crashes.
  const claudeInvocation = `set -o pipefail; ${ccrotateRefresh}; cat /tmp/prompt/prompt.txt | claude ${claudeArgsEscaped} | tee ${podLogPath}`;
  const mainCommand = claudeInvocation;

  // Decide prompt delivery strategy: env var (small) or Secret volume (large).
  const promptBytes = Buffer.byteLength(prompt, "utf-8");
  const useLargePromptPath = promptBytes > LARGE_PROMPT_THRESHOLD_BYTES;
  let promptSecret: PromptSecret | null = null;
  const promptSecretName = `${jobName}-prompt`;

  if (useLargePromptPath) {
    // Stage prompt as a Secret; the init container copies from the mounted
    // secret volume to the emptyDir so the main container reads it the
    // same way regardless of prompt size.
    promptSecret = {
      name: promptSecretName,
      namespace,
      data: { "prompt.txt": prompt },
    };
    volumes.push({
      name: "prompt-secret",
      secret: { secretName: promptSecretName, optional: false },
    });
  }

  // Build the init container — writes the prompt (always) and, when an
  // agent supplied adapterConfig.mcpServers, the merged mcp.json next to
  // it in the same prompt emptyDir. mcp.json is small (~few kB) so the
  // env-var path is fine even when the prompt itself goes the
  // Secret-volume route for size.
  const initCommandParts = useLargePromptPath
    ? ["cp /tmp/prompt-secret/prompt.txt /tmp/prompt/prompt.txt"]
    : [`printf '%s' "$PROMPT_CONTENT" > /tmp/prompt/prompt.txt`];
  const initEnv: k8s.V1EnvVar[] = useLargePromptPath
    ? []
    : [{ name: "PROMPT_CONTENT", value: prompt }];
  if (mergedMcpJson) {
    initCommandParts.push(`printf '%s' "$MCP_CONFIG" > /tmp/prompt/mcp.json`);
    initEnv.push({ name: "MCP_CONFIG", value: mergedMcpJson });
  }
  const initVolumeMounts: k8s.V1VolumeMount[] = [
    { name: "data", mountPath: dataMountPath },
    { name: "prompt", mountPath: "/tmp/prompt" },
  ];
  if (useLargePromptPath) {
    initVolumeMounts.push({
      name: "prompt-secret",
      mountPath: "/tmp/prompt-secret",
      readOnly: true,
    });
  }
  const initContainer: k8s.V1Container = {
    name: "write-prompt",
    image: "busybox:1.36",
    imagePullPolicy: "IfNotPresent",
    command: ["sh", "-c", initCommandParts.join("; ")],
    ...(initEnv.length > 0 ? { env: initEnv } : {}),
    volumeMounts: initVolumeMounts,
    securityContext,
    resources: {
      requests: { cpu: "10m", memory: "16Mi" },
      limits: { cpu: "100m", memory: "64Mi" },
    },
  };

  const job: k8s.V1Job = {
    apiVersion: "batch/v1",
    kind: "Job",
    metadata: {
      name: jobName,
      namespace,
      labels,
      annotations: {
        "paperclip.io/adapter-type": "claude_k8s",
        "paperclip.io/agent-name": agent.name,
      },
    },
    spec: {
      backoffLimit: 0,
      ...(timeoutSec > 0 ? { activeDeadlineSeconds: timeoutSec } : {}),
      ttlSecondsAfterFinished: ttlSeconds,
      template: {
        metadata: { labels },
        spec: {
          restartPolicy: "Never",
          serviceAccountName: asString(config.serviceAccountName, "") || undefined,
          securityContext: podSecurityContext,
          ...(selfPod.imagePullSecrets.length > 0 ? { imagePullSecrets: selfPod.imagePullSecrets } : {}),
          ...(selfPod.dnsConfig ? { dnsConfig: selfPod.dnsConfig } : {}),
          ...(Object.keys(nodeSelector).length > 0 ? { nodeSelector } : {}),
          ...(tolerations.length > 0 ? { tolerations: tolerations as k8s.V1Toleration[] } : {}),
          initContainers: [initContainer],
          containers: [
            {
              name: "claude",
              image,
              imagePullPolicy: asString(config.imagePullPolicy, "IfNotPresent"),
              workingDir,
              command: ["sh", "-c", mainCommand],
              env: envVars,
              ...(selfPod.inheritedEnvFrom.length > 0 ? { envFrom: selfPod.inheritedEnvFrom } : {}),
              volumeMounts,
              securityContext,
              resources: containerResources,
            },
          ],
          volumes,
        },
      },
    },
  };

  return { job, jobName, namespace, prompt, claudeArgs, promptMetrics, promptSecret, skippedLabels, podLogPath };
}
