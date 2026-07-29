import type { AdapterExecutionContext, AdapterExecutionResult } from "@paperclipai/adapter-utils";
import {
  asString,
  asNumber,
  asBoolean,
  parseObject,
  readPaperclipRuntimeSkillEntries,
  resolvePaperclipDesiredSkillNames,
} from "@paperclipai/adapter-utils/server-utils";
import fs from "node:fs/promises";
import path from "node:path";
import { prepareClaudePromptBundle } from "./prompt-cache.js";
import {
  parseClaudeStreamJson,
  describeClaudeFailure,
  isClaudeMaxTurnsResult,
  isClaudeUnknownSessionError,
  isClaudeImmutableThinkingBlockError,
  classifyClaudeUpstreamFailure,
  extractClaudeRetryNotBefore,
} from "./parse.js";
import { getSelfPodInfo, getBatchApi, getCoreApi } from "./k8s-client.js";
import {
  buildJobManifest,
  buildPodLogPath,
  resolveJobIsolation,
  sanitizeLabelValue,
  type JobIsolation,
} from "./job-manifest.js";
import type * as k8s from "@kubernetes/client-node";

const POLL_INTERVAL_MS = 2000;
const KEEPALIVE_INTERVAL_MS = 15_000;
const K8S_CONCURRENCY_GUARD_TIMEOUT_MS = 15_000;
const RUN_ID_LABEL = "paperclip.io/run-id";
const ISOLATION_MODE_LABEL = "paperclip.io/isolation-mode";
const ISOLATION_KEY_LABEL = "paperclip.io/isolation-key";
const TASK_KEY_LABEL = "paperclip.io/task-key";
const TASK_ID_LABEL = "paperclip.io/task-id";
const SESSION_ID_LABEL = "paperclip.io/session-id";
const CONCURRENT_RUN_BLOCKED_PATH = "/api/metrics/claude-k8s/concurrent-run-blocked";
const ISOLATED_RUN_STARTED_PATH = "/api/metrics/claude-k8s/isolated-run-started";
const TERMINAL_RUN_STATUSES = new Set(["succeeded", "failed", "cancelled", "timeout", "timed_out"]);

async function withK8sConcurrencyGuardTimeout<T>(operation: Promise<T>): Promise<T> {
  let timeout: ReturnType<typeof setTimeout> | undefined;
  const deadline = new Promise<never>((_resolve, reject) => {
    timeout = setTimeout(() => {
      reject(new Error(
        `Kubernetes Job concurrency guard timed out after ${K8S_CONCURRENCY_GUARD_TIMEOUT_MS / 1000}s`,
      ));
    }, K8S_CONCURRENCY_GUARD_TIMEOUT_MS);
  });
  try {
    return await Promise.race([operation, deadline]);
  } finally {
    if (timeout) clearTimeout(timeout);
  }
}

type IsolationMode = JobIsolation["mode"];

type GuardIdentity = {
  isolationMode: IsolationMode | null;
  isolationKey: string | null;
  taskKey: string | null;
  sessionId: string | null;
  validIsolationMetadata: boolean;
};

type ConcurrentRunBlockedReason =
  | "live_job_for_active_run"
  | "live_job_for_unknown_run"
  | "shared_mode_serialized"
  | "unknown_isolation_blocked";

type HeartbeatRunSnapshot = {
  id: string;
  status?: string | null;
  completedAt?: string | null;
  finishedAt?: string | null;
};

function readHeartbeatRunSnapshot(raw: unknown): HeartbeatRunSnapshot | null {
  if (!raw || typeof raw !== "object") return null;
  const obj = raw as Record<string, unknown>;
  const id = asString(obj.id, "").trim();
  if (!id) return null;
  return {
    id,
    status: asString(obj.status, "") || null,
    completedAt: asString(obj.completedAt, "") || null,
    finishedAt: asString(obj.finishedAt, "") || null,
  };
}

function isActiveHeartbeatRun(run: HeartbeatRunSnapshot): boolean {
  const status = (run.status ?? "").toLowerCase();
  if (TERMINAL_RUN_STATUSES.has(status)) return false;
  if (run.completedAt || run.finishedAt) return false;
  return status === "running";
}

async function fetchHeartbeatRunSnapshot(ctx: AdapterExecutionContext, runId: string): Promise<HeartbeatRunSnapshot | null> {
  const apiUrl = process.env.PAPERCLIP_API_URL?.replace(/\/+$/, "");
  if (!apiUrl) throw new Error("PAPERCLIP_API_URL is required for k8s run-table concurrency checks");

  const resp = await fetch(`${apiUrl}/api/heartbeat-runs/${encodeURIComponent(runId)}`, {
    headers: { Authorization: `Bearer ${ctx.authToken ?? ""}` },
  });
  if (resp.status === 404) return null;
  if (!resp.ok) throw new Error(`run lookup failed for ${runId}: HTTP ${resp.status}`);
  return readHeartbeatRunSnapshot(await resp.json());
}

function readOptionalString(value: unknown): string | null {
  const parsed = asString(value, "").trim();
  return parsed || null;
}

function readJobGuardIdentity(job: k8s.V1Job): GuardIdentity {
  const labels = job.metadata?.labels ?? {};
  const rawMode = readOptionalString(labels[ISOLATION_MODE_LABEL]);
  const isolationMode = rawMode === "isolated"
    ? "workspace"
    : rawMode === "shared" || rawMode === "run" || rawMode === "workspace"
      ? rawMode
      : null;
  const isolationKey = readOptionalString(labels[ISOLATION_KEY_LABEL]);

  return {
    isolationMode,
    isolationKey,
    taskKey: readOptionalString(labels[TASK_KEY_LABEL]) ?? readOptionalString(labels[TASK_ID_LABEL]),
    sessionId: readOptionalString(labels[SESSION_ID_LABEL]),
    validIsolationMetadata: isolationMode !== null && isolationKey !== null,
  };
}

function readCurrentGuardIdentity(ctx: AdapterExecutionContext, isolation: JobIsolation): GuardIdentity {
  const runtimeIsolation = parseObject(
    (ctx.runtime as AdapterExecutionContext["runtime"] & { isolation?: unknown }).isolation,
  );
  const sessionScope = parseObject(runtimeIsolation.sessionScope);
  const sessionParams = parseObject(ctx.runtime.sessionParams);
  const isolationKey = readOptionalString(runtimeIsolation.isolationKey) ?? readOptionalString(isolation.key);

  return {
    isolationMode: isolation.mode,
    isolationKey,
    taskKey:
      readOptionalString(sessionScope.taskKey)
      ?? readOptionalString(ctx.runtime.taskKey)
      ?? readOptionalString(ctx.context.taskId)
      ?? readOptionalString(ctx.context.issueId),
    sessionId: readOptionalString(sessionParams.sessionId) ?? readOptionalString(ctx.runtime.sessionId),
    validIsolationMetadata: isolation.mode === "shared" || isolationKey !== null,
  };
}

function classifyConcurrentBlock(
  current: GuardIdentity,
  conflicting: GuardIdentity,
  conflictingRunId: string | null,
): ConcurrentRunBlockedReason {
  if (current.isolationMode === "shared") return "shared_mode_serialized";
  if (!conflicting.validIsolationMetadata) return "unknown_isolation_blocked";
  if (!conflictingRunId) return "live_job_for_unknown_run";
  return "live_job_for_active_run";
}

async function reportGuardEvent(
  ctx: AdapterExecutionContext,
  path: typeof CONCURRENT_RUN_BLOCKED_PATH | typeof ISOLATED_RUN_STARTED_PATH,
  body: Record<string, unknown>,
): Promise<void> {
  const event = path === CONCURRENT_RUN_BLOCKED_PATH
    ? "k8s_concurrent_run_blocked"
    : "k8s_isolated_run_started";
  const logBestEffort = async (stream: "stdout" | "stderr", message: string) => {
    try {
      await ctx.onLog(stream, message);
    } catch {
      // Telemetry must never change the adapter's allow/block decision.
    }
  };
  await logBestEffort("stdout", `[paperclip] ${event}: ${JSON.stringify(body)}\n`);

  const apiUrl = process.env.PAPERCLIP_API_URL?.replace(/\/+$/, "");
  if (!apiUrl || !ctx.authToken) {
    await logBestEffort(
      "stderr",
      `[paperclip] Warning: cannot report ${event}; PAPERCLIP_API_URL or adapter auth token is missing\n`,
    );
    return;
  }

  try {
    const response = await fetch(`${apiUrl}${path}`, {
      method: "POST",
      headers: {
        Authorization: `Bearer ${ctx.authToken}`,
        "Content-Type": "application/json",
        "X-Paperclip-Run-Id": ctx.runId,
      },
      body: JSON.stringify(body),
      signal: AbortSignal.timeout(2_000),
    });
    if (!response.ok) {
      await logBestEffort("stderr", `[paperclip] Warning: ${event} report returned HTTP ${response.status}\n`);
    }
  } catch (err) {
    const message = err instanceof Error ? err.message : String(err);
    await logBestEffort("stderr", `[paperclip] Warning: failed to report ${event}: ${message}\n`);
  }
}

async function reportConcurrentRunBlocked(
  ctx: AdapterExecutionContext,
  current: GuardIdentity,
  conflicting: GuardIdentity,
  conflictingRunId: string | null,
): Promise<void> {
  await reportGuardEvent(ctx, CONCURRENT_RUN_BLOCKED_PATH, {
    agentId: ctx.agent.id,
    companyId: ctx.agent.companyId,
    reason: classifyConcurrentBlock(current, conflicting, conflictingRunId),
    isolationMode: current.isolationMode ?? "shared",
    isolationKey: conflicting.isolationKey ?? current.isolationKey,
    taskKey: conflicting.taskKey ?? current.taskKey,
    sessionId: conflicting.sessionId ?? current.sessionId,
  });
}

async function reportIsolatedRunStarted(
  ctx: AdapterExecutionContext,
  current: GuardIdentity,
): Promise<void> {
  await reportGuardEvent(ctx, ISOLATED_RUN_STARTED_PATH, {
    agentId: ctx.agent.id,
    companyId: ctx.agent.companyId,
    isolationMode: current.isolationMode,
    isolationKey: current.isolationKey,
    taskKey: current.taskKey,
    sessionId: current.sessionId,
  });
}

// Module-level tracking of active Jobs for SIGTERM best-effort cleanup.
interface ActiveJobRef {
  namespace: string;
  jobName: string;
  promptSecretName?: string;
  promptSecretNamespace?: string;
  kubeconfigPath?: string;
}
const activeJobs = new Set<ActiveJobRef>();
// Per-agent serialization lock: prevents the TOCTOU race (FAR-29) where two
// concurrent execute() calls for the same agent both pass the list-then-create
// guard and create K8s Jobs simultaneously on the shared PVC.
const agentCreationMutex = new Map<string, Promise<void>>();
let sigtermHandlerRegistered = false;

function resolveIsolationMutexKey(
  ctx: Pick<AdapterExecutionContext, "runtime" | "agent">,
  config: Record<string, unknown>,
): { mutexKey: string; isolationKey: string | null; isolation: JobIsolation } {
  const isolation = resolveJobIsolation(ctx, config);
  if (!isolation.enabled) return { mutexKey: ctx.agent.id, isolationKey: null, isolation };
  return {
    mutexKey: `${ctx.agent.id}:${isolation.key}`,
    isolationKey: isolation.key,
    isolation,
  };
}

function sanitizePathComponent(value: string): string {
  return value.replace(/[^a-zA-Z0-9-]/g, "");
}

function safePathComponent(value: string): string {
  return sanitizePathComponent(value) || "unknown";
}

function resolvePromptCacheRoot(
  config: Record<string, unknown>,
  ctx: AdapterExecutionContext,
  isolation: JobIsolation,
): string | null {
  const configured = asString(config.promptCacheRoot, "").trim();
  if (configured) return configured;
  if (!isolation.enabled) return null;
  if (isolation.promptCacheRoot) return isolation.promptCacheRoot;
  const root = `/paperclip/instances/default/data/k8s-isolation/${safePathComponent(ctx.agent.companyId)}/${safePathComponent(ctx.agent.id)}/${isolation.key}`;
  return `${root}/prompt-cache`;
}

function ensureSigtermHandler(): void {
  if (sigtermHandlerRegistered) return;
  sigtermHandlerRegistered = true;
  process.once("SIGTERM", () => {
    // Do NOT delete active K8s Jobs on SIGTERM (FAR-107).  Paperclip itself
    // receives SIGTERM during rolling deploys, evictions, scale-down, etc.
    // Deleting the Jobs we own there causes the in-flight heartbeat to surface
    // a false-positive `k8s_job_deleted_externally` error and tears down work
    // the user expected to keep running.
    //
    // The correct behaviour with `reattachOrphanedJobs=true` (default) is to
    // leave the Jobs alive: the next paperclip process discovers them via the
    // orphan-classification path and reattaches their log streams.  When
    // `reattachOrphanedJobs=false` the operator explicitly opted into manual
    // cleanup and should not have us auto-deleting either.  The owning Job's
    // ownerReference (FAR-15) keeps the prompt Secret tied to the Job, so
    // both survive together and TTL cleans them up after natural completion.
    process.kill(process.pid, "SIGTERM");
  });
}

async function listClaudeSessionJsonlFiles(root: string, sessionId: string): Promise<string[]> {
  const files: string[] = [];
  const stack = [root];
  const target = `${sessionId}.jsonl`;

  while (stack.length > 0) {
    const dir = stack.pop()!;
    let entries: import("node:fs").Dirent[];
    try {
      entries = await fs.readdir(dir, { withFileTypes: true });
    } catch {
      continue;
    }

    for (const entry of entries) {
      const fullPath = path.join(dir, entry.name);
      if (entry.isDirectory()) {
        stack.push(fullPath);
      } else if (entry.isFile() && entry.name === target) {
        files.push(fullPath);
      }
    }
  }

  return files;
}

async function moveIfExists(source: string, dest: string): Promise<boolean> {
  try {
    await fs.stat(source);
  } catch {
    return false;
  }
  await fs.mkdir(path.dirname(dest), { recursive: true });
  await fs.rename(source, dest);
  return true;
}

function timestampForPath(date = new Date()): string {
  return date.toISOString().replace(/[-:]/g, "").replace(/\.\d{3}Z$/, "Z");
}

async function quarantineClaudeSessionArtifacts(input: {
  sessionId: string | null;
  onLog: AdapterExecutionContext["onLog"];
}): Promise<void> {
  const sessionId = input.sessionId?.trim() ?? "";
  if (!/^[0-9a-fA-F-]{16,}$/.test(sessionId)) return;

  const home = process.env.HOME || "/paperclip";
  const projectsRoot = path.join(home, ".claude", "projects");
  const quarantineRoot = path.join(home, ".claude", "quarantine", `immutable-thinking-${timestampForPath()}`);
  const files = await listClaudeSessionJsonlFiles(projectsRoot, sessionId);
  let moved = 0;

  for (const file of files) {
    const relative = path.relative(projectsRoot, file);
    if (relative.startsWith("..")) continue;

    if (await moveIfExists(file, path.join(quarantineRoot, "projects", relative))) {
      moved++;
    }

    const sessionDir = path.join(path.dirname(file), sessionId);
    const relativeSessionDir = path.relative(projectsRoot, sessionDir);
    if (!relativeSessionDir.startsWith("..")) {
      if (await moveIfExists(sessionDir, path.join(quarantineRoot, "projects", relativeSessionDir))) {
        moved++;
      }
    }
  }

  if (moved > 0) {
    await input.onLog(
      "stdout",
      `[paperclip] Quarantined ${moved} Claude session artifact(s) for immutable thinking failure: ${sessionId}\n`,
    );
  }
}

/**
 * Tail a pod log file from the shared PVC, emitting complete lines via onLog.
 * Uses adaptive polling: 250ms when the file is actively growing, backed off
 * to 1000ms after 5 consecutive polls with no growth.
 */
interface TailOptions {
  onLog: AdapterExecutionContext["onLog"];
  stopSignal: { stopped: boolean };
}

async function tailPodLogFile(
  filePath: string,
  opts: TailOptions,
): Promise<string> {
  const { onLog, stopSignal } = opts;
  const accumulator: string[] = [];
  let pendingLine = "";
  let consecutiveIdlePolls = 0;
  let pollInterval = 250;

  // Wait up to 30s for the file to appear
  const deadline = Date.now() + 30_000;
  while (Date.now() < deadline) {
    try {
      await fs.stat(filePath);
      break;
    } catch {
      if (stopSignal.stopped) throw new Error("Stop signal received before log file appeared");
      await new Promise((resolve) => setTimeout(resolve, 250));
    }
  }
  // Final check after the wait loop
  let handle: fs.FileHandle;
  try {
    handle = await fs.open(filePath, "r");
  } catch {
    throw new Error(`Pod log file never appeared at ${filePath}`);
  }

  let offset = 0;
  try {
    while (!stopSignal.stopped) {
      const stat = await fs.stat(filePath);
      const size = stat.size;
      if (size > offset) {
        const buf = Buffer.alloc(size - offset);
        const { bytesRead } = await handle.read(buf, 0, buf.length, offset);
        offset += bytesRead;
        consecutiveIdlePolls = 0;
        pollInterval = 250;

        const combined = pendingLine + buf.toString("utf-8", 0, bytesRead);
        const lines = combined.split("\n");
        pendingLine = lines.pop() ?? "";
        for (const line of lines) {
          accumulator.push(line);
          await onLog("stdout", line + "\n");
        }
      } else {
        consecutiveIdlePolls++;
        if (consecutiveIdlePolls >= 5) pollInterval = 1000;
      }
      if (!stopSignal.stopped) await new Promise((resolve) => setTimeout(resolve, pollInterval));
    }

    // Final drain.  When the pod's `tee` flushes the trailing JSON result
    // line moments before the Job is reported Complete, distributed
    // filesystems (e.g. cephfs RWX where pod and reader run on different
    // nodes) can lag on metadata propagation by a beat or two — a single
    // size+read pass can miss those last lines and the parser then fails
    // with "Failed to parse Claude JSON output" even though claude exited
    // 0.  Poll size up to ~5 s until it goes stable for two consecutive
    // checks before declaring drain complete.
    const DRAIN_MAX_MS = 5_000;
    const DRAIN_POLL_MS = 250;
    const drainDeadline = Date.now() + DRAIN_MAX_MS;
    let stableSize = -1;
    while (true) {
      const size = (await fs.stat(filePath)).size;
      if (size > offset) {
        const buf = Buffer.alloc(size - offset);
        const { bytesRead } = await handle.read(buf, 0, buf.length, offset);
        offset += bytesRead;
        const combined = pendingLine + buf.toString("utf-8", 0, bytesRead);
        const lines = combined.split("\n");
        pendingLine = lines.pop() ?? "";
        for (const line of lines) {
          accumulator.push(line);
          await onLog("stdout", line + "\n");
        }
        stableSize = -1; // reset stability counter — we just read more
      } else if (size === stableSize) {
        break; // size held steady across two polls — drain done
      } else {
        stableSize = size;
      }
      if (Date.now() >= drainDeadline) break;
      await new Promise((resolve) => setTimeout(resolve, DRAIN_POLL_MS));
    }
    if (pendingLine) {
      accumulator.push(pendingLine);
      await onLog("stdout", pendingLine + "\n");
      pendingLine = "";
    }
  } finally {
    await handle.close();
  }

  return accumulator.join("\n");
}

export function mergeEnvironmentConfig(
  adapterConfig: Record<string, unknown>,
  envConfig: Record<string, unknown> | null | undefined,
): Record<string, unknown> {
  const merged = { ...adapterConfig };
  if (!envConfig) return merged;
  for (const [key, value] of Object.entries(envConfig)) {
    if (value !== null && value !== undefined) merged[key] = value;
  }
  return merged;
}

/**
 * Detect a Kubernetes 404 (Not Found) error from @kubernetes/client-node.
 * Works for both v0.x (response.statusCode) and v1.0+ (response.status, message).
 * Exported for unit tests.
 */
export function isK8s404(err: unknown): boolean {
  if (!(err instanceof Error)) return false;
  const e = err as unknown as Record<string, unknown>;
  const resp = e.response as Record<string, unknown> | undefined;
  if (resp?.statusCode === 404 || resp?.status === 404) return true;
  if (e.statusCode === 404) return true;
  return /HTTP-Code:\s*404\b/.test(err.message);
}

/**
 * Returns true when the heartbeat-run status indicates the run was explicitly
 * cancelled and the K8s Job must be torn down.
 *
 * Only `cancelled` / `cancelling` qualify.  Treating any non-`running` status
 * as cancellation (the previous behaviour) produced spurious
 * k8s_job_deleted_externally errors for in-flight runs whenever the API
 * briefly reported a transient or stale status — Nancy's runs at
 * Privileged Escalation hit this without anyone actually cancelling them
 * (FAR-107).  Other terminal statuses (`succeeded`/`failed`/`completed`)
 * are unreachable in practice while the adapter is still executing
 * (the adapter's own return is what flips them) and even if observed,
 * they do not warrant our deleting a Job that may still be doing work.
 * Exported for unit tests.
 */
export function shouldAbortForCancellation(runStatus: string | undefined): boolean {
  if (!runStatus) return false;
  return runStatus === "cancelled" || runStatus === "cancelling";
}

/**
 * Linear scan for a `{"type":"result"}` line in raw stdout. Used as a recovery
 * path when `parseClaudeStreamJson` returns null even though stdout contains
 * a parseable result event — the streaming parser is sensitive to interleaved
 * non-JSON lines (e.g. paperclip keepalive prefixes), but the result event
 * itself is a complete single line we can pluck out and parse standalone.
 */
function scanForResultEvent(stdout: string): Record<string, unknown> | null {
  if (!stdout) return null;
  for (const rawLine of stdout.split(/\r?\n/)) {
    const line = rawLine.trim();
    if (!line || line[0] !== "{") continue;
    if (!line.includes('"type":"result"')) continue;
    try {
      const obj = JSON.parse(line) as unknown;
      if (obj && typeof obj === "object" && (obj as { type?: unknown }).type === "result") {
        return obj as Record<string, unknown>;
      }
    } catch {
      // keep looking — may be a truncated result line followed by a complete one
    }
  }
  return null;
}

/**
 * Returns the first non-JSON/plain-text line in stdout, treating JSON objects
 * with a "type" field as protocol artefacts and skipping them.
 * Used by buildPartialRunError to detect init-only runs.
 */
function firstContentLine(stdout: string): string {
  return stdout.split(/\r?\n/)
    .map((l) => l.trim())
    .find((l) => {
      if (!l) return false;
      try {
        const obj = JSON.parse(l);
        if (typeof obj === "object" && obj !== null) {
          const t = (obj as Record<string, unknown>).type;
          if (typeof t === "string" && t) return false;
        }
      } catch {
        // not JSON — treat as content
      }
      return true;
    }) ?? "";
}

function truncateMiddle(value: string, max = 800): string {
  if (value.length <= max) return value;
  const head = Math.max(0, Math.floor((max - 15) / 2));
  const tail = Math.max(0, max - 15 - head);
  return `${value.slice(0, head)} ...[truncated]... ${value.slice(value.length - tail)}`;
}

function redactDiagnosticLogLine(value: string): string {
  return value
    .replace(/\bBearer\s+[A-Za-z0-9._:=/-]+/gi, "Bearer <redacted>")
    .replace(/\beyJ[A-Za-z0-9._-]+/g, "<jwt-redacted>")
    .replace(/\bfigd_[A-Za-z0-9._-]+/g, "figd_<redacted>")
    .replace(/\bgbrain_at_[A-Za-z0-9._-]+/g, "gbrain_at_<redacted>")
    .replace(/\bGOCSPX-[A-Za-z0-9_-]+/g, "GOCSPX-<redacted>")
    .replace(/\bsk-[A-Za-z0-9_-]+/g, "sk-<redacted>")
    .replace(/\b[A-Za-z0-9_-]{20,}:[A-Za-z0-9_-]{10,}:[A-Za-z0-9_-]{20,}\b/g, "<compound-token-redacted>")
    .replace(/\b(token|secret|password|api[_-]?key)=\S+/gi, "$1=<redacted>");
}

export function extractContainerLogDiagnostic(raw: string): string | null {
  const lines = raw
    .split(/\r?\n/)
    .map((line) => redactDiagnosticLogLine(line.trim()))
    .filter((line) => line.length > 0);
  if (lines.length === 0) return null;
  return truncateMiddle(lines.slice(-5).join(" | "));
}

function normalizePodLogResponse(value: unknown): string {
  if (typeof value === "string") return value;
  if (value && typeof value === "object") {
    const body = (value as { body?: unknown }).body;
    if (typeof body === "string") return body;
  }
  return "";
}

async function readPodContainerLogTail(input: {
  namespace: string;
  podName: string;
  kubeconfigPath?: string;
}): Promise<string | null> {
  const coreApi = getCoreApi(input.kubeconfigPath);
  try {
    const raw = await coreApi.readNamespacedPodLog({
      name: input.podName,
      namespace: input.namespace,
      container: "claude",
      tailLines: 120,
    });
    return extractContainerLogDiagnostic(normalizePodLogResponse(raw));
  } catch (err) {
    if (isK8s404(err)) return null;
    return `failed to read pod log: ${redactDiagnosticLogLine(err instanceof Error ? err.message : String(err))}`;
  }
}

/**
 * Returns true when stdout contains only init/system/assistant events from the
 * given model with no human-readable content lines.  Used to detect init-only
 * non-zero-exit runs that should be classified as claude_init_failed rather than
 * the generic "Claude exited with code N" message.
 */
function isInitOnlyRun(model: string, stdout: string): boolean {
  if (!stdout.trim() || !model) return false;
  const content = firstContentLine(stdout);
  if (content) return false;
  // Check that at least the init event for this model was seen
  const hasModelInit = stdout.includes(`"model":"${model}"`) || stdout.includes(`"model":"${model.replace(/-/g, "_")}"`);
  return hasModelInit;
}

/**
 * Append the pod's terminated-state detail (reason/message/signal) to a
 * partial-run error message when available.  Exit code is already in the
 * caller-supplied message, so we only append fields that add new signal —
 * specifically reason (e.g. OOMKilled, Error, ContainerCannotRun), message
 * (kubelet diagnostic text), and signal.  Saves the operator a kubectl trip.
 */
function appendPodCause(message: string, state: PodTerminatedState | null, containerLogTail: string | null = null): string {
  const parts: string[] = [];
  if (state?.reason) parts.push(`reason=${state.reason}`);
  if (state?.message) parts.push(`message=${state.message}`);
  if (state?.signal !== null && state?.signal !== undefined) parts.push(`signal=${state.signal}`);
  if (state?.exitCode === 137) parts.push("SIGKILL (commonly OOMKilled)");
  if (containerLogTail) parts.push(`container_log=${containerLogTail}`);
  if (parts.length === 0) return message;
  return `${message} [pod: ${parts.join(", ")}]`;
}

/**
 * Build the error message when Claude's stdout contains no result event.
 * Skips system/init event lines so the UI doesn't display the raw init JSON.
 * When `podState` is provided, appends the K8s container terminated reason/
 * message so failures self-explain without requiring `kubectl`.
 * Exported for unit tests.
 */
export function buildPartialRunError(
  exitCode: number | null,
  model: string,
  stdout: string,
  podState: PodTerminatedState | null = null,
  containerLogTail: string | null = null,
): string {
  if (exitCode === 0) return "Failed to parse Claude JSON output";

  // If the stream contained only structured events with no plain-text output,
  // surface the model name so the operator can diagnose missing credentials
  // or unsupported/misconfigured model.
  const contentLine = firstContentLine(stdout);
  if (contentLine) {
    return appendPodCause(`Claude exited with code ${exitCode ?? -1}: ${contentLine}`, podState, containerLogTail);
  }

  if (isInitOnlyRun(model, stdout) && (exitCode ?? 0) !== 0) {
    const modelHint = model ? ` (model: ${model})` : "";
    return appendPodCause(
      `Claude exited immediately after init${modelHint} (exit code ${exitCode ?? -1}) — the model may be unsupported or the session may have been rejected before producing output`,
      podState,
      containerLogTail,
    );
  }

  const initOnlyOutput = stdout.trim() !== "" && model !== "";
  if (initOnlyOutput) {
    const modelHint = model ? ` (model: ${model})` : "";
    return appendPodCause(
      `Claude started but did not produce a result${modelHint} — check API credentials, model support, and adapter config`,
      podState,
      containerLogTail,
    );
  }

  return appendPodCause(`Claude exited with code ${exitCode ?? -1}`, podState, containerLogTail);
}

export type OrphanClassification =
  | "reattach"
  | "block_session_mismatch"
  | "block_task_mismatch"
  | "block_task_unknown";

/**
 * Classify a non-terminal orphaned K8s Job (one whose `paperclip.io/run-id`
 * label does not match the current runId but does belong to this agent) as a
 * reattach candidate or a block reason.
 *
 * Decision matrix:
 *   - taskId mismatch (both present, different values)         → block_task_mismatch
 *   - taskId missing on either side                            → block_task_unknown
 *   - taskId match + both have sessionId + sessionIds differ   → block_session_mismatch
 *   - taskId match + one or both sides missing sessionId       → reattach (reconcile)
 *   - taskId match + both have sessionId + sessionIds match    → reattach (happy path)
 *
 * Exported for unit tests.
 */
export function classifyOrphan(
  job: k8s.V1Job,
  expected: { taskId: string | null; sessionId: string | null },
): OrphanClassification {
  const labels = job.metadata?.labels ?? {};
  const jobTaskId = labels["paperclip.io/task-id"] ?? null;
  const jobSessionId = labels["paperclip.io/session-id"] ?? null;

  // taskId missing on either side
  if (!expected.taskId || !jobTaskId) return "block_task_unknown";

  // taskId mismatch
  if (expected.taskId !== jobTaskId) return "block_task_mismatch";

  // taskId matches — check sessionId
  if (expected.sessionId && jobSessionId && expected.sessionId !== jobSessionId) {
    return "block_session_mismatch";
  }

  return "reattach";
}

/**
 * Build an error message for a pod that reached phase=Failed before or
 * instead of streaming logs. Includes the claude container's terminated exit
 * code and reason when available so operators can diagnose crashes without
 * needing kubectl.  Exported for unit tests.
 */
export function describePodTerminatedError(
  podName: string,
  phase: string,
  containerStatuses: k8s.V1ContainerStatus[],
): string {
  const mainCs = containerStatuses.find((cs) => cs.name === "claude");
  const terminated = mainCs?.state?.terminated;
  if (terminated) {
    const code = terminated.exitCode ?? "unknown";
    const reason = terminated.reason ?? terminated.message ?? "no reason";
    return `Pod ${podName} reached phase=${phase}: claude exited ${code} (${reason})`;
  }
  return `Pod ${podName} reached phase=${phase}`;
}

/**
 * Wait for the Job's pod to reach a terminal or running state.
 * Returns the pod name once logs can be streamed, or throws on failure.
 */
async function waitForPod(
  namespace: string,
  jobName: string,
  scheduleTimeoutMs: number,
  startTimeoutMs: number,
  onLog: AdapterExecutionContext["onLog"],
  kubeconfigPath?: string,
): Promise<string> {
  const coreApi = getCoreApi(kubeconfigPath);
  const scheduleDeadline = Date.now() + scheduleTimeoutMs;
  const labelSelector = `job-name=${jobName}`;

  await onLog("stdout", `[paperclip] Waiting for pod to be scheduled (job: ${jobName})...\n`);

  let lastStatus = "";
  let lastStatusDetails = "no pod observed yet";
  let startDeadline = 0;
  while (true) {
    const podList = await coreApi.listNamespacedPod({
      namespace,
      labelSelector,
    });
    const pod = podList.items[0];

    if (!pod) {
      if (Date.now() >= scheduleDeadline) {
        throw new Error(`Timed out waiting for pod to be scheduled (${Math.round(scheduleTimeoutMs / 1000)}s)`);
      }
      if (lastStatus !== "no-pod") {
        await onLog("stdout", `[paperclip] Waiting for Job controller to create pod...\n`);
        lastStatus = "no-pod";
      }
      await new Promise((resolve) => setTimeout(resolve, POLL_INTERVAL_MS));
      continue;
    }

    const podName = pod.metadata?.name ?? "unknown";
    const phase = pod.status?.phase ?? "Unknown";
    const conditions = pod.status?.conditions ?? [];
    const scheduledCondition = conditions.find((c) => c.type === "PodScheduled");
    const isScheduled = Boolean(pod.spec?.nodeName) || scheduledCondition?.status === "True";
    const initStatuses = pod.status?.initContainerStatuses ?? [];
    const containerStatuses = pod.status?.containerStatuses ?? [];

    // Log phase transitions
    const statusKey = `${phase}:${initStatuses.map((s) => s.state?.waiting?.reason ?? s.state?.terminated?.reason ?? "ok").join(",")}:${containerStatuses.map((s) => s.state?.waiting?.reason ?? (s.state?.running ? "running" : "waiting")).join(",")}`;
    if (statusKey !== lastStatus) {
      const details: string[] = [`phase=${phase}`];
      for (const init of initStatuses) {
        if (init.state?.waiting) details.push(`init/${init.name}: waiting (${init.state.waiting.reason ?? "unknown"})`);
        else if (init.state?.running) details.push(`init/${init.name}: running`);
        else if (init.state?.terminated) details.push(`init/${init.name}: done (exit ${init.state.terminated.exitCode})`);
      }
      for (const cs of containerStatuses) {
        if (cs.state?.waiting) details.push(`${cs.name}: waiting (${cs.state.waiting.reason ?? "unknown"})`);
        else if (cs.state?.running) details.push(`${cs.name}: running`);
        else if (cs.state?.terminated) details.push(`${cs.name}: terminated (exit ${cs.state.terminated.exitCode ?? "?"}, ${cs.state.terminated.reason ?? "no reason"})`);
      }
      await onLog("stdout", `[paperclip] Pod ${podName}: ${details.join(", ")}\n`);
      lastStatus = statusKey;
      lastStatusDetails = details.join(", ");
    }

    if (isScheduled && startDeadline === 0) {
      startDeadline = Date.now() + startTimeoutMs;
      await onLog("stdout", `[paperclip] Pod ${podName} scheduled; waiting for containers to start...\n`);
    }

    // Ready to stream logs
    if (phase === "Running" || phase === "Succeeded") {
      return podName;
    }
    // phase=Failed means the pod crashed before we could stream logs.
    // Throwing here routes the caller into the error path with a structured
    // message instead of entering the log-streaming path with a dead pod.
    if (phase === "Failed") {
      throw new Error(describePodTerminatedError(podName, phase, containerStatuses));
    }

    // Init containers done + main running (phase may still say Pending briefly)
    const allInitsDone = initStatuses.length > 0 && initStatuses.every(
      (s) => s.state?.terminated?.exitCode === 0,
    );
    const mainRunning = containerStatuses.some((s) => s.state?.running);
    if (allInitsDone && mainRunning) {
      return podName;
    }

    // Check for init container failures
    for (const init of initStatuses) {
      const terminated = init.state?.terminated;
      if (terminated && (terminated.exitCode ?? 0) !== 0) {
        throw new Error(`Init container "${init.name}" failed with exit code ${terminated.exitCode}: ${terminated.reason ?? terminated.message ?? "unknown"}`);
      }
      const waiting = init.state?.waiting;
      if (waiting?.reason === "ErrImagePull" || waiting?.reason === "ImagePullBackOff") {
        throw new Error(`Init container "${init.name}" image pull failed: ${waiting.message ?? waiting.reason}`);
      }
      if (waiting?.reason === "CrashLoopBackOff") {
        throw new Error(`Init container "${init.name}" crash loop: ${waiting.message ?? waiting.reason}`);
      }
    }

    // Check for unrecoverable scheduling failures
    const unschedulable = conditions.find(
      (c) => c.type === "PodScheduled" && c.status === "False" && c.reason === "Unschedulable",
    );
    if (unschedulable) {
      throw new Error(`Pod unschedulable: ${unschedulable.message ?? "insufficient resources"}`);
    }

    if (!isScheduled && Date.now() >= scheduleDeadline) {
      throw new Error(`Timed out waiting for pod to be scheduled (${Math.round(scheduleTimeoutMs / 1000)}s)`);
    }
    if (isScheduled && startDeadline > 0 && Date.now() >= startDeadline) {
      throw new Error(`Timed out waiting for pod containers to start (${Math.round(startTimeoutMs / 1000)}s): ${lastStatusDetails}`);
    }

    // Check for main container image pull errors
    for (const cs of containerStatuses) {
      const waiting = cs.state?.waiting;
      if (waiting?.reason === "ErrImagePull" || waiting?.reason === "ImagePullBackOff") {
        throw new Error(`Image pull failed for "${cs.name}": ${waiting.message ?? waiting.reason}`);
      }
      if (waiting?.reason === "CrashLoopBackOff") {
        throw new Error(`Container "${cs.name}" crash loop: ${waiting.message ?? waiting.reason}`);
      }
    }

    await new Promise((resolve) => setTimeout(resolve, POLL_INTERVAL_MS));
  }
}

/**
 * Wait for the Job to reach a terminal state (Complete or Failed).
 * Returns the Job's final status.  A 404 (job deleted by TTL or externally)
 * is treated as a soft terminal: succeeded=false, timedOut=false, jobGone=true.
 * The caller should log this and fall through to stdout parsing.
 */
type JobConditionSnapshot = { type?: string; status?: string; reason?: string; message?: string };

async function waitForJobCompletion(
  namespace: string,
  jobName: string,
  timeoutMs: number,
  kubeconfigPath?: string,
  observer?: { lastConditions: JobConditionSnapshot[] | null; pollCount: number },
): Promise<{ succeeded: boolean; timedOut: boolean; jobGone?: boolean }> {
  const batchApi = getBatchApi(kubeconfigPath);
  const deadline = timeoutMs > 0 ? Date.now() + timeoutMs : 0;

  while (deadline === 0 || Date.now() < deadline) {
    let job;
    try {
      job = await batchApi.readNamespacedJob({ name: jobName, namespace });
    } catch (err: unknown) {
      if (isK8s404(err)) {
        // Job was deleted (TTL garbage collection or external deletion) before
        // we detected its terminal condition.  The container must have already
        // exited for TTL to fire, so log streaming will have captured the output.
        return { succeeded: false, timedOut: false, jobGone: true };
      }
      throw err;
    }
    const conditions = job.status?.conditions ?? [];
    if (observer) {
      observer.pollCount += 1;
      observer.lastConditions = conditions.map((c) => ({
        type: c.type, status: c.status, reason: c.reason, message: c.message,
      }));
    }

    const complete = conditions.find((c) => c.type === "Complete" && c.status === "True");
    if (complete) return { succeeded: true, timedOut: false };

    const failed = conditions.find((c) => c.type === "Failed" && c.status === "True");
    if (failed) {
      const isDeadlineExceeded = failed.reason === "DeadlineExceeded";
      return { succeeded: false, timedOut: isDeadlineExceeded };
    }

    await new Promise((resolve) => setTimeout(resolve, POLL_INTERVAL_MS));
  }

  return { succeeded: false, timedOut: true };
}

/**
 * Get the exit code from the Job's pod.
 */
async function getPodExitCode(namespace: string, jobName: string, kubeconfigPath?: string): Promise<number | null> {
  const state = await getPodTerminatedState(namespace, jobName, kubeconfigPath);
  return state?.exitCode ?? null;
}

/**
 * Get the claude container's terminated state (exit code, reason, message,
 * signal) from the Job's pod. Returns null if the pod or container is gone.
 * Used by the no-result error path to explain *why* a run was truncated.
 */
export interface PodTerminatedState {
  exitCode: number | null;
  reason: string | null;
  message: string | null;
  signal: number | null;
}

/**
 * Result of a pod-state lookup.  `state` is the terminated state when available;
 * `phase` and `podMissing` give the caller enough context to render an honest
 * truncation-cause message instead of guessing "likely deleted" (FAR-107).
 */
export interface PodLookupResult {
  state: PodTerminatedState | null;
  phase: string | null;
  podMissing: boolean;
}

async function lookupPodState(
  namespace: string,
  jobName: string,
  kubeconfigPath?: string,
): Promise<PodLookupResult> {
  const coreApi = getCoreApi(kubeconfigPath);
  const podList = await coreApi.listNamespacedPod({
    namespace,
    labelSelector: `job-name=${jobName}`,
  });
  const pod = podList.items[0];
  if (!pod) return { state: null, phase: null, podMissing: true };

  const phase = pod.status?.phase ?? null;
  const containerStatus = pod.status?.containerStatuses?.find((s) => s.name === "claude");
  const terminated = containerStatus?.state?.terminated;
  if (!terminated) return { state: null, phase, podMissing: false };
  return {
    state: {
      exitCode: terminated.exitCode ?? null,
      reason: terminated.reason ?? null,
      message: (terminated.message ?? "").trim() || null,
      signal: terminated.signal ?? null,
    },
    phase,
    podMissing: false,
  };
}

/**
 * Read the claude container's terminated state, retrying briefly when the pod
 * exists in a terminal phase but kubelet has not yet propagated the
 * containerStatuses[].state.terminated field.  Without this retry, fast
 * truncated-stream exits surface as "pod state unavailable" (FAR-107) and
 * mask the real exit code / OOMKilled / SIGTERM cause.
 */
async function getPodLookupWithRetry(
  namespace: string,
  jobName: string,
  kubeconfigPath?: string,
  attempts = 4,
  delayMs = 500,
): Promise<PodLookupResult> {
  let last: PodLookupResult = { state: null, phase: null, podMissing: true };
  for (let i = 0; i < attempts; i++) {
    last = await lookupPodState(namespace, jobName, kubeconfigPath);
    if (last.state) return last;
    if (last.podMissing) return last;
    // Pod exists but no terminated state.  If it is in a terminal phase the
    // containerStatuses update is in flight — wait briefly and retry.  If it
    // is still Running/Pending, retrying is unlikely to help, so bail.
    if (last.phase !== "Succeeded" && last.phase !== "Failed") return last;
    if (i < attempts - 1) await new Promise((r) => setTimeout(r, delayMs));
  }
  return last;
}

async function getPodTerminatedState(
  namespace: string,
  jobName: string,
  kubeconfigPath?: string,
): Promise<PodTerminatedState | null> {
  return (await lookupPodState(namespace, jobName, kubeconfigPath)).state;
}

/**
 * Format a human-readable explanation for a truncated run, including the
 * pod's claude-container terminated state when available. Exit code 137
 * is annotated as SIGKILL/OOM since that is the most common cause.
 * Exported for unit tests.
 */
export function describeTruncationCause(
  state: PodTerminatedState | null,
  lookup?: PodLookupResult,
): string {
  if (!state) {
    if (lookup?.podMissing) {
      return "pod is gone — Job pod was removed (eviction, preemption, or external delete) before exit could be read";
    }
    if (lookup && !lookup.podMissing) {
      const phaseHint = lookup.phase ? `pod phase=${lookup.phase}` : "pod present";
      return `container terminated state not yet observable (${phaseHint}) — kubelet status update did not land within retry window; exit cause unknown`;
    }
    return "pod state unavailable — exit cause unknown";
  }
  const parts: string[] = [];
  if (state.exitCode !== null) {
    parts.push(`exit code ${state.exitCode}`);
    if (state.exitCode === 137) parts.push("SIGKILL (commonly OOMKilled)");
    else if (state.exitCode === 143) parts.push("SIGTERM");
  } else {
    parts.push("no exit code");
  }
  if (state.signal !== null) parts.push(`signal ${state.signal}`);
  if (state.reason) parts.push(`reason=${state.reason}`);
  if (state.message) parts.push(`message=${state.message}`);
  return parts.join(", ");
}

/**
 * Delete Job and its pods. Best-effort — failures are logged but not thrown.
 */
async function cleanupJob(
  namespace: string,
  jobName: string,
  onLog: AdapterExecutionContext["onLog"],
  kubeconfigPath?: string,
  podLogPath?: string,
): Promise<void> {
  try {
    const batchApi = getBatchApi(kubeconfigPath);
    await batchApi.deleteNamespacedJob({
      name: jobName,
      namespace,
      body: { propagationPolicy: "Background" },
    });
    if (podLogPath) {
      try { await fs.unlink(podLogPath); } catch { /* non-fatal */ }
    }
  } catch (err) {
    const msg = err instanceof Error ? err.message : String(err);
    await onLog("stderr", `[paperclip] Warning: failed to cleanup job ${jobName}: ${msg}\n`);
  }
}

export async function execute(ctx: AdapterExecutionContext): Promise<AdapterExecutionResult> {
  const { runId, runtime, config: rawConfig, onLog, onMeta } = ctx;
  const onExternalRuntimeLaunched = (ctx as AdapterExecutionContext & {
    onExternalRuntimeLaunched?: (identity: { jobName: string; jobUid: string }) => Promise<void>;
  }).onExternalRuntimeLaunched;
  // Phase E.1 — when paperclip dispatches a heartbeat with a remote/k8s
  // executionTarget, merge `executionTarget.config` (the env's K8sRemoteSpec)
  // over the agent's adapter_config.  Environment fields win, but keys whose
  // env value is null/undefined are skipped so a partially filled K8sRemoteSpec
  // doesn't clobber adapter defaults.  Any other transport (ssh, sandbox) and
  // local targets fall through to the agent's adapter_config unchanged.
  // TODO(env-config): plumb cross-namespace secret resolution.  `secretsNamespace`
  //  is merged through here so it lands in `effectiveConfig`, but downstream
  //  Secret reads currently always use the Job's own namespace; a future patch
  //  needs to pass `secretsNamespace` into the Secret-resolution path.
  const target = (ctx as unknown as { executionTarget?: { kind?: string; transport?: string; config?: Record<string, unknown> } }).executionTarget;
  const isK8sRemoteTarget = target?.kind === "remote" && target?.transport === "k8s";
  const adapterConfigObj = parseObject(rawConfig);
  const effectiveConfig = isK8sRemoteTarget
    ? mergeEnvironmentConfig(adapterConfigObj, target?.config ?? null)
    : adapterConfigObj;
  // Build a derived context whose `config` is the merged result so every
  // downstream call (incl. buildJobManifest) sees the env-supplied fields.
  const effectiveCtx: AdapterExecutionContext = isK8sRemoteTarget
    ? ({ ...ctx, config: effectiveConfig } as AdapterExecutionContext)
    : ctx;
  const config = effectiveConfig;
  const timeoutSec = asNumber(config.timeoutSec, 0);
  const graceSec = asNumber(config.graceSec, 60);
  const retainJobs = asBoolean(config.retainJobs, false);
  let isolationKey: string | null = null;
  let jobIsolation!: JobIsolation;
  let creationMutexKey = "";
  try {
    const isolation = resolveIsolationMutexKey(effectiveCtx, config);
    isolationKey = isolation.isolationKey;
    jobIsolation = isolation.isolation;
    creationMutexKey = isolation.mutexKey;
  } catch (err) {
    const msg = err instanceof Error ? err.message : String(err);
    await onLog("stderr", `[paperclip] Cannot create K8s Job: ${msg}\n`);
    return {
      exitCode: null,
      signal: null,
      timedOut: false,
      errorMessage: msg,
      errorCode: "k8s_isolation_key_invalid",
    };
  }
  const currentGuardIdentity = readCurrentGuardIdentity(effectiveCtx, jobIsolation);
  // K8sRemoteSpec.kubeconfig is *content* (resolved by the env driver), but
  // the adapter's existing config interprets `kubeconfig` as a filesystem
  // path. When the env supplies null we fall through to in-cluster auth as
  // today (mergeEnvironmentConfig skips null values, so adapter's
  // "no kubeconfig" wins → undefined → loadFromCluster).
  // TODO(env-config): when env supplies non-null kubeconfig content, write
  //  it to a tmp file and load that path through getKubeConfig() so the
  //  client uses the env-supplied credential. Out of scope for E.1.
  const kubeconfigPath = asString(config.kubeconfig, "") || undefined;
  const paperclipApiUrl = process.env.PAPERCLIP_API_URL ?? "";
  if (!paperclipApiUrl) {
    await onLog("stderr", "[paperclip] Warning: PAPERCLIP_API_URL not set — cancel polling disabled\n");
  }

  // Guard: claude_k8s must not run concurrently for the same agent (shared PVC/session).
  // After a server restart, orphaned K8s Jobs from previous (now-failed) runs may
  // still be running.  We detect those by comparing the Job's run-id label against
  // the current runId.  When reattachOrphanedJobs is enabled and the orphan matches
  // the current agent+task+session, we attach to it instead of deleting it (FAR-124).
  const agentId = ctx.agent.id;
  const sanitizedAgentId = sanitizeLabelValue(agentId);
  if (!sanitizedAgentId) {
    await onLog("stderr", `[paperclip] Cannot create K8s Job: agent.id "${agentId}" produces no valid RFC 1123 label characters\n`);
    return {
      exitCode: null,
      signal: null,
      timedOut: false,
      errorMessage: `Agent ID "${agentId}" cannot be sanitized to a valid Kubernetes label`,
      errorCode: "k8s_agent_id_invalid",
    };
  }
  // FAR-29: serialize guard+create per agent within this process to prevent the
  // TOCTOU race where two concurrent execute() calls both pass the list-then-create
  // guard and create K8s Jobs simultaneously on the shared PVC.
  const _prevCreation = agentCreationMutex.get(creationMutexKey) ?? Promise.resolve();
  let _releaseMutex: () => void = () => {};
  const _mutexSlot = new Promise<void>((resolve) => { _releaseMutex = resolve; });
  // Chain: next caller for this agent waits on _mutexSlot, which resolves in finally.
  agentCreationMutex.set(creationMutexKey, _prevCreation.then(() => _mutexSlot, () => _mutexSlot));
  // Wait for any prior execute() call to finish its guard+create phase.
  await _prevCreation.catch(() => {});

  // Hoist declarations used in both the guard+create phase and the log-streaming
  // section so the mutex try/finally can be added without a large re-indent.
  // eslint-disable-next-line prefer-const
  let jobName!: string;
  // eslint-disable-next-line prefer-const
  let namespace!: string;
  // eslint-disable-next-line prefer-const
  let podLogPath!: string;
  let promptSecret: { name: string; namespace: string; data: Record<string, string> } | null = null;
  // runtimeSessionParams and currentSessionIdRaw are also used after the
  // try block (in the result-parsing section) so hoist them here.
  const runtimeSessionParams = parseObject(runtime.sessionParams);
  const currentSessionIdRaw = asString(runtimeSessionParams.sessionId, runtime.sessionId ?? "");
  const coreApi = getCoreApi(kubeconfigPath);
  const batchApi = getBatchApi(kubeconfigPath);

  try {
  const selfPod = await getSelfPodInfo(kubeconfigPath);
  const guardNamespace = asString(config.namespace, "") || selfPod.namespace;
  try {
    const existing = await withK8sConcurrencyGuardTimeout(
      batchApi.listNamespacedJob({
        namespace: guardNamespace,
        labelSelector: `paperclip.io/agent-id=${sanitizedAgentId},paperclip.io/adapter-type=claude_k8s`,
      }),
    );
    const running = existing.items.filter(
      (j) =>
        !j.metadata?.deletionTimestamp &&
        !j.status?.conditions?.some((c) => (c.type === "Complete" || c.type === "Failed") && c.status === "True"),
    );
    if (running.length > 0) {
      const activeRunning: typeof running = [];
      for (const job of running) {
        const jobRunId = job.metadata?.labels?.[RUN_ID_LABEL] ?? "";
        const jobName = job.metadata?.name ?? "unknown";
        const jobNamespace = job.metadata?.namespace ?? guardNamespace;

        const conflictingIdentity = readJobGuardIdentity(job);

        if (!jobRunId) {
          await onLog("stderr", `[paperclip] Blocked: running Job ${jobName} has no ${RUN_ID_LABEL} provenance label\n`);
          await reportConcurrentRunBlocked(effectiveCtx, currentGuardIdentity, conflictingIdentity, null);
          return {
            exitCode: null,
            signal: null,
            timedOut: false,
            errorMessage: `Concurrent run blocked: orphaned Job ${jobName} has unknown run provenance`,
            errorCode: "k8s_orphan_task_unknown",
          };
        }

        if (jobRunId === runId) {
          activeRunning.push(job);
          continue;
        }

        const priorRun = await fetchHeartbeatRunSnapshot(ctx, jobRunId);
        if (!priorRun || !isActiveHeartbeatRun(priorRun)) {
          await cleanupJob(jobNamespace, jobName, onLog, kubeconfigPath);
          await onLog("stdout", `[paperclip] Ignoring stale Job ${jobName}: run ${jobRunId} is terminal or missing in Paperclip.\n`);
          continue;
        }

        if (
          isolationKey
          && conflictingIdentity.validIsolationMetadata
          && conflictingIdentity.isolationMode !== "shared"
          && conflictingIdentity.isolationKey !== isolationKey
        ) {
          await onLog("stdout", `[paperclip] Allowing concurrent Job ${jobName}: isolation-key ${conflictingIdentity.isolationKey} differs from ${isolationKey}.\n`);
          continue;
        }

        activeRunning.push(job);
      }

      // Separate orphaned jobs (from a previous server-side run) from truly
      // concurrent jobs (same runId — shouldn't happen but guard defensively).
      const orphaned = activeRunning.filter(
        (j) => (j.metadata?.labels?.[RUN_ID_LABEL] ?? "") !== runId,
      );
      const samRun = activeRunning.filter(
        (j) => (j.metadata?.labels?.[RUN_ID_LABEL] ?? "") === runId,
      );

      if (orphaned.length > 0) {
        const names = orphaned.map((j) => j.metadata?.name).join(", ");
        await onLog("stderr", `[paperclip] Concurrent run blocked: active run-backed Job(s) still running for this agent: ${names}\n`);
        const conflictingJob = orphaned[0]!;
        await reportConcurrentRunBlocked(
          effectiveCtx,
          currentGuardIdentity,
          readJobGuardIdentity(conflictingJob),
          readOptionalString(conflictingJob.metadata?.labels?.[RUN_ID_LABEL]),
        );
        return {
          exitCode: null,
          signal: null,
          timedOut: false,
          errorMessage: `Concurrent run blocked: active run-backed Job ${names} is still running for this agent`,
          errorCode: "k8s_concurrent_run_blocked",
        };
      }

      // If there are still running Jobs that belong to THIS run (shouldn't happen
      // since we haven't created the Job yet), block execution.
      if (samRun.length > 0) {
        const names = samRun.map((j) => j.metadata?.name).join(", ");
        await onLog("stderr", `[paperclip] Concurrent run blocked: existing Job(s) still running for this run: ${names}\n`);
        const conflictingJob = samRun[0]!;
        await reportConcurrentRunBlocked(
          effectiveCtx,
          currentGuardIdentity,
          readJobGuardIdentity(conflictingJob),
          readOptionalString(conflictingJob.metadata?.labels?.[RUN_ID_LABEL]),
        );
        return {
          exitCode: null,
          signal: null,
          timedOut: false,
          errorMessage: `Concurrent run blocked: Job ${names} is still running for this agent`,
          errorCode: "k8s_concurrent_run_blocked",
        };
      }
    }
  } catch (err: unknown) {
    // If we can't list jobs, fail closed — the K8s concurrency guard is the
    // only thing preventing zombie Jobs on a shared PVC from corrupting
    // sessions.  404 (namespace not found) is treated as a hard failure;
    // other errors (5xx, network) are also surfaced.
    const msg = err instanceof Error ? err.message : String(err);
    await onLog("stderr", `[paperclip] Concurrency guard failed: unable to list jobs: ${msg}\n`);
    return {
      exitCode: null,
      signal: null,
      timedOut: false,
      errorMessage: `Concurrency guard unreachable: ${msg}`,
      errorCode: "k8s_concurrency_guard_unreachable",
    };
  }

  // Prepare the prompt bundle (skills + instructions) on the server filesystem.
  // The K8s Job pod mounts the same PVC at /paperclip, so bundle paths written
  // here are accessible inside the pod at the identical absolute path.
  const skillEntries = await readPaperclipRuntimeSkillEntries(config, import.meta.dirname ?? __dirname);
  const desiredSkillNames = new Set(resolvePaperclipDesiredSkillNames(config, skillEntries));
  const desiredSkills = skillEntries.filter((e) => desiredSkillNames.has(e.key));
  const skillSummary = desiredSkills.length > 0 ? desiredSkills.map((s) => s.runtimeName ?? s.key).join(", ") : "none";
  await onLog("stdout", `[paperclip] Skills bundled (${desiredSkills.length}): ${skillSummary}\n`);
  const instructionsFilePath = asString(config.instructionsFilePath, "").trim();
  const instructionsFileDir = instructionsFilePath ? `${path.dirname(instructionsFilePath)}/` : "";
  let instructionsContents: string | null = null;
  if (instructionsFilePath) {
    try {
      const raw = await fs.readFile(instructionsFilePath, "utf-8");
      const pathDirective =
        `\nThe above agent instructions were loaded from ${instructionsFilePath}. ` +
        `Resolve any relative file references from ${instructionsFileDir}. ` +
        `This base directory is authoritative for sibling instruction files such as ` +
        `./HEARTBEAT.md, ./SOUL.md, and ./TOOLS.md; do not resolve those from the parent agent directory.`;
      instructionsContents = raw + pathDirective;
    } catch (err) {
      await onLog(
        "stderr",
        `[paperclip] Warning: could not read agent instructions file "${instructionsFilePath}": ${err instanceof Error ? err.message : String(err)}\n`,
      );
    }
  }
  const promptBundle = await prepareClaudePromptBundle({
    companyId: ctx.agent.companyId,
    skills: desiredSkills,
    instructionsContents,
    rootDir: resolvePromptCacheRoot(config, effectiveCtx, jobIsolation),
    onLog,
  });

  // Build Job manifest
    const built = buildJobManifest({ ctx: effectiveCtx, selfPod, promptBundle });
    const job = built.job;
    jobName = built.jobName;
    namespace = built.namespace;
    const prompt = built.prompt;
    const claudeArgs = built.claudeArgs;
    const promptMetrics = built.promptMetrics;
    promptSecret = built.promptSecret;
    podLogPath = built.podLogPath;
    if (built.skippedLabels.length > 0) {
      await onLog("stderr", `[paperclip] Warning: skipped ${built.skippedLabels.length} extra label(s) with reserved prefix: ${built.skippedLabels.join(", ")}\n`);
    }

    // Report invocation metadata
    if (onMeta) {
      await onMeta({
        adapterType: "claude_k8s",
        command: `kubectl job/${jobName}`,
        cwd: namespace,
        commandArgs: claudeArgs,
        commandNotes: [
          `Image: ${job.spec?.template.spec?.containers[0]?.image ?? "unknown"}`,
          `Namespace: ${namespace}`,
          `Timeout: ${timeoutSec}s`,
          `Skills (${desiredSkills.length}): ${skillSummary}`,
        ],
        prompt,
        ...(promptMetrics ? { promptMetrics } : {}),
        context: ctx.context,
      } as Parameters<typeof onMeta>[0]);
    }

    // If the prompt is large, create a Secret to hold it (avoids the ~1 MiB
    // PodSpec limit).  The Secret is cleaned up in the finally block.
    if (promptSecret) {
      try {
        await coreApi.createNamespacedSecret({
          namespace: promptSecret.namespace,
          body: {
            apiVersion: "v1",
            kind: "Secret",
            metadata: {
              name: promptSecret.name,
              namespace: promptSecret.namespace,
              labels: {
                "app.kubernetes.io/managed-by": "paperclip",
                "paperclip.io/adapter-type": "claude_k8s",
                "paperclip.io/run-id": runId,
              },
            },
            stringData: promptSecret.data,
          },
        });
        await onLog("stdout", `[paperclip] Created prompt Secret: ${promptSecret.name} (${Math.round(Buffer.byteLength(prompt, "utf-8") / 1024)} KiB)\n`);
      } catch (err) {
        const msg = err instanceof Error ? err.message : String(err);
        await onLog("stderr", `[paperclip] Failed to create prompt Secret: ${msg}\n`);
        return {
          exitCode: null,
          signal: null,
          timedOut: false,
          errorMessage: `Failed to create prompt Secret: ${msg}`,
          errorCode: "k8s_prompt_secret_create_failed",
        };
      }
    }

    // Create the Job
    let createdJobUid: string | undefined;
    try {
      const created = await batchApi.createNamespacedJob({ namespace, body: job });
      createdJobUid = created.metadata?.uid;
    } catch (err) {
      const msg = err instanceof Error ? err.message : String(err);
      await onLog("stderr", `[paperclip] Failed to create K8s Job: ${msg}\n`);
      if (promptSecret) {
        try {
          await coreApi.deleteNamespacedSecret({ name: promptSecret.name, namespace: promptSecret.namespace });
        } catch { /* best-effort */ }
      }
      return {
        exitCode: null,
        signal: null,
        timedOut: false,
        errorMessage: `Failed to create Kubernetes Job: ${msg}`,
        errorCode: "k8s_job_create_failed",
      };
    }
    if (!createdJobUid || !onExternalRuntimeLaunched) {
      await cleanupJob(namespace, jobName, onLog, kubeconfigPath, podLogPath);
      if (promptSecret) {
        await coreApi.deleteNamespacedSecret({
          name: promptSecret.name,
          namespace: promptSecret.namespace,
        }).catch(() => undefined);
      }
      return {
        exitCode: null,
        signal: null,
        timedOut: false,
        errorMessage: !createdJobUid
          ? "Created Kubernetes Job did not return a UID"
          : "Paperclip did not provide an external-runtime launch acknowledgment",
        errorCode: "k8s_job_identity_unacknowledged",
      };
    }
    try {
      await onExternalRuntimeLaunched({ jobName, jobUid: createdJobUid });
    } catch (err) {
      await cleanupJob(namespace, jobName, onLog, kubeconfigPath, podLogPath);
      if (promptSecret) {
        await coreApi.deleteNamespacedSecret({
          name: promptSecret.name,
          namespace: promptSecret.namespace,
        }).catch(() => undefined);
      }
      return {
        exitCode: null,
        signal: null,
        timedOut: false,
        errorMessage: `External runtime launch acknowledgment failed: ${err instanceof Error ? err.message : String(err)}`,
        errorCode: "k8s_job_identity_unacknowledged",
      };
    }

    if (jobIsolation.enabled) {
      await reportIsolatedRunStarted(effectiveCtx, currentGuardIdentity);
    }

    // Attach ownerReference so K8s GC cleans up the Secret if the process
    // crashes before the finally block runs.
    if (promptSecret && createdJobUid) {
      try {
        await coreApi.patchNamespacedSecret({
          name: promptSecret.name,
          namespace: promptSecret.namespace,
          body: [
            {
              op: "add",
              path: "/metadata/ownerReferences",
              value: [
                {
                  apiVersion: "batch/v1",
                  kind: "Job",
                  name: jobName,
                  uid: createdJobUid,
                  blockOwnerDeletion: false,
                },
              ],
            },
          ],
        });
      } catch (err) {
        const msg = err instanceof Error ? err.message : String(err);
        await onLog("stderr", `[paperclip] Warning: failed to set ownerReference on prompt Secret: ${msg}\n`);
      }
    }

    await onLog("stdout", `[paperclip] Created K8s Job: ${jobName} in namespace ${namespace} (deadline: ${timeoutSec > 0 ? `${timeoutSec}s` : "none"})\n`);
  } finally {
    // Release the per-agent creation mutex so the next queued execute() call
    // can proceed with its guard+create phase (FAR-29).
    _releaseMutex();
  }

  // eslint-disable-next-line @typescript-eslint/no-shadow
  let stdout = "";
  let exitCode: number | null = null;
  let podTerminatedState: PodTerminatedState | null = null;
  let jobTimedOut = false;
  let keepaliveTimer: ReturnType<typeof setInterval> | null = null;
  // Set when we return a mismatch error so the finally block knows not to
  // delete a job that is still alive and the UI is waiting on.
  let skipCleanup = false;
  // Set when the job disappeared (404) or grace-timer fired before we saw a
  // terminal condition — used to emit a clearer error when stdout parsing fails.
  let jobDeletedExternally = false;
  // Forensics for k8s_job_deleted_externally — captures which of the three
  // detection paths observed the 404, the last successful Job-condition read
  // before deletion, and timing.  Surfaced in the error message so the next
  // occurrence is self-diagnosing instead of opaque (FAR-107).
  let jobGoneDetectionPath: string | null = null;
  let jobGoneAt: number | null = null;
  const jobObserver: { lastConditions: JobConditionSnapshot[] | null; pollCount: number } = {
    lastConditions: null,
    pollCount: 0,
  };
  let podRunningAt: number | null = null;
  let podName: string | null = null;
  let containerLogTail: string | null = null;

  const activeJobRef: ActiveJobRef = {
    namespace,
    jobName,
    ...(promptSecret ? { promptSecretName: promptSecret.name, promptSecretNamespace: promptSecret.namespace } : {}),
    kubeconfigPath,
  };
  activeJobs.add(activeJobRef);
  ensureSigtermHandler();

  try {
    // Wait for pod to be ready for log streaming
    const scheduleTimeoutMs = Math.max(0, asNumber(config.podScheduleTimeoutSec, 120)) * 1000;
    const startTimeoutMs = Math.max(0, asNumber(config.podStartTimeoutSec, 600)) * 1000;
    try {
      podName = await waitForPod(namespace, jobName, scheduleTimeoutMs, startTimeoutMs, onLog, kubeconfigPath);
      await onLog("stdout", `[paperclip] Pod running: ${podName}\n`);
      podRunningAt = Date.now();

    } catch (err) {
      const msg = err instanceof Error ? err.message : String(err);
      const startupFailure = msg.includes("pod containers to start");
      const failureLabel = startupFailure ? "Pod startup failed" : "Pod scheduling failed";
      await onLog("stderr", `[paperclip] ${failureLabel}: ${msg}\n`);
      return {
        exitCode: null,
        signal: null,
        timedOut: false,
        errorMessage: `${failureLabel}: ${msg}`,
        errorCode: "k8s_pod_schedule_failed",
      };
    }

    // Stream logs and wait for completion concurrently.
    // The log stream will end when the container exits.
    // We also poll the Job status to detect deadline exceeded.
    // 0 = no timeout (run indefinitely, matching claude_local behavior)
    const completionTimeoutMs = timeoutSec > 0 ? (timeoutSec + graceSec) * 1000 : 0;

    // Keepalive: periodically send a status line via onLog so the
    // Paperclip server knows the adapter is still alive even when the
    // pod produces no output (e.g. Claude is in a long thinking phase).
    let lastLogAt = Date.now();
    let keepaliveJobTerminal = false;
    let consecutiveTerminalReadings = 0;
    // Shared signal: when job completion resolves, tell the log streamer to
    // stop.  Declared before keepaliveTimer so the cancel path inside the
    // timer can set it without temporal dead zone issues.
    const logStopSignal = { stopped: false };
    // Set when the run is externally cancelled (cancel-poll path).
    let cancelled = false;

    keepaliveTimer = setInterval(() => {
      // Fire-and-forget the async work; setInterval callbacks must be
      // synchronous or the timer will drift.
      void (async () => {
        if (keepaliveJobTerminal || cancelled) return;

        // Verify the Job is still alive before announcing or refreshing.
        // Require two consecutive terminal readings before latching to
        // guard against a stale K8s API cache returning a false terminal
        // status on a single read (finding #5, FAR-15).
        try {
          const job = await batchApi.readNamespacedJob({ name: jobName, namespace });
          const terminal = job.status?.conditions?.some(
            (c) => (c.type === "Complete" || c.type === "Failed") && c.status === "True",
          );
          if (terminal) {
            consecutiveTerminalReadings++;
            if (consecutiveTerminalReadings >= 2) {
              keepaliveJobTerminal = true;
            }
            return;
          }
          consecutiveTerminalReadings = 0;
        } catch (err: unknown) {
          // Only treat 404 (Job deleted) as terminal.  Transient 5xx or
          // connection resets should NOT permanently disable the keepalive —
          // the next tick will re-check and the reaper uses the staleness
          // window as a safety net.
          if (isK8s404(err)) {
            keepaliveJobTerminal = true;
            return;
          }
          // Log transient errors but leave keepaliveJobTerminal false so
          // the next tick retries.
          const msg = err instanceof Error ? err.message : String(err);
          void onLog("stderr", `[paperclip] keepalive: transient error checking job status: ${msg}\n`).catch(() => {});
          return;
        }

        // Cancel-polling: check if the Paperclip run was cancelled externally.
        // HTTP non-2xx is treated as transient — never interpret a 5xx as cancel.
        if (paperclipApiUrl && ctx.authToken) {
          try {
            const resp = await fetch(`${paperclipApiUrl}/api/heartbeat-runs/${runId}`, {
              headers: { Authorization: `Bearer ${ctx.authToken}` },
            });
            if (resp.ok) {
              const data = await resp.json() as { status?: string };
              if (shouldAbortForCancellation(data.status)) {
                void onLog("stdout", `[paperclip] Run cancelled externally — deleting Job ${jobName}\n`).catch(() => {});
                cancelled = true;
                logStopSignal.stopped = true;
                try {
                  await batchApi.deleteNamespacedJob({
                    name: jobName,
                    namespace,
                    body: { propagationPolicy: "Background" },
                  });
                } catch { /* best-effort — completion watcher will see 404 and settle */ }
                return;
              }
            } else if (resp.status >= 500) {
              void onLog("stderr", `[paperclip] keepalive: cancel poll returned HTTP ${resp.status} — transient, ignoring\n`).catch(() => {});
            }
          } catch {
            // network error — transient, skip this tick
          }
        }

        const silenceSec = Math.round((Date.now() - lastLogAt) / 1000);
        void onLog("stdout", `[paperclip] keepalive — job ${jobName} running (${silenceSec}s since last output)\n`).catch(() => {});
      })();
    }, KEEPALIVE_INTERVAL_MS);
    const wrappedOnLog: typeof onLog = async (stream, chunk) => {
      lastLogAt = Date.now();
      return onLog(stream, chunk);
    };

    const [tailResult, completionResult] = await Promise.allSettled([
      tailPodLogFile(podLogPath, { onLog: wrappedOnLog, stopSignal: logStopSignal }),
      waitForJobCompletion(namespace, jobName, completionTimeoutMs, kubeconfigPath, jobObserver).then(r => { logStopSignal.stopped = true; return r; }),
    ]);

    stdout = tailResult.status === "fulfilled" ? tailResult.value : "";

    // Belt-and-braces: tailPodLogFile's drain can still miss the trailing
    // `result` line on cephfs RWX when metadata propagation lags Job
    // completion.  Read the full file from disk while it still exists —
    // the cleanupJob path in `finally` will delete it before the parser
    // runs further down.  Prefer the longer of (tail, file).
    if (podLogPath) {
      try {
        const fsp = await import("node:fs/promises");
        const onDisk = await fsp.readFile(podLogPath, "utf-8");
        if (onDisk.length > stdout.length) {
          await onLog("stderr", `[paperclip] tail accumulator (${stdout.length}b) < on-disk pod log (${onDisk.length}b); using on-disk for parse.\n`);
          stdout = onDisk;
        }
      } catch {
        // already cleaned up or unreadable — fall through with tail content
      }
    }

    if (completionResult.status === "fulfilled") {
      jobTimedOut = completionResult.value.timedOut;
      if (completionResult.value.jobGone) {
        // Job was deleted by TTL or externally before we observed the Complete/Failed
        // condition.  The container must have exited first (TTL only fires after
        // completion), so log streaming has captured the full output — continue
        // to stdout parsing rather than returning an error.
        jobDeletedExternally = true;
        if (!jobGoneDetectionPath) {
          jobGoneDetectionPath = "completion-poll-404";
          jobGoneAt = Date.now();
        }
        await onLog("stdout", `[paperclip] Job ${jobName} was deleted before terminal condition was observed (TTL or external deletion) — proceeding with captured output.\n`);
      }
    } else {
      // waitForJobCompletion threw an unexpected error — re-check job state to
      // avoid returning while the job is still running.  Use a bounded timeout
      // (60s) so we don't hang the heartbeat indefinitely if the K8s API is degraded.
      jobTimedOut = false;
      const RECHECK_TIMEOUT_MS = 60_000;
      const actualState = await waitForJobCompletion(namespace, jobName, RECHECK_TIMEOUT_MS, kubeconfigPath, jobObserver);
      if (actualState.timedOut) {
        // Re-check itself timed out — the job may still be running.
        // Return an error so the UI knows the run is not done.
        jobTimedOut = true;
      } else if (actualState.jobGone) {
        // Job was deleted before we could confirm terminal state — same as the
        // fulfilled+jobGone case above: proceed with captured output.
        jobDeletedExternally = true;
        if (!jobGoneDetectionPath) {
          jobGoneDetectionPath = "recheck-poll-404";
          jobGoneAt = Date.now();
        }
        await onLog("stdout", `[paperclip] Job ${jobName} was deleted before terminal condition was observed (TTL or external deletion) — proceeding with captured output.\n`);
      } else if (!actualState.succeeded) {
        // Job still not terminal — the completion error was likely transient.
        // Return an error so the UI knows the run is not done, rather than
        // returning with parsed (potentially incomplete) stdout.
        await onLog("stderr", `[paperclip] Job ${jobName} still not terminal after log/completion mismatch — returning error to keep UI in sync.\n`);
        skipCleanup = true;
        return {
          exitCode,
          signal: null,
          timedOut: false,
          errorMessage: `Job ${jobName} did not complete cleanly (log stream ended before job reached terminal state)`,
          errorCode: "k8s_job_state_mismatch",
        };
      }
    }

    podTerminatedState = await getPodTerminatedState(namespace, jobName, kubeconfigPath);
    exitCode = podTerminatedState?.exitCode ?? null;
    if ((exitCode ?? 0) !== 0 && podName) {
      containerLogTail = await readPodContainerLogTail({ namespace, podName, kubeconfigPath });
      if (containerLogTail) {
        await onLog("stderr", `[paperclip] failed pod container log tail: ${containerLogTail}\n`);
      }
    }
  } finally {
    if (keepaliveTimer) clearInterval(keepaliveTimer);
    activeJobs.delete(activeJobRef);
    if (skipCleanup) {
      await onLog("stdout", `[paperclip] Retaining job ${jobName} (state mismatch — UI is waiting on it)\n`);
    } else if (!retainJobs) {
      await cleanupJob(namespace, jobName, onLog, kubeconfigPath, podLogPath);
    } else {
      await onLog("stdout", `[paperclip] Retaining job ${jobName} for debugging (retainJobs=true)\n`);
    }
    // Clean up prompt Secret if one was created
    if (promptSecret) {
      try {
        await coreApi.deleteNamespacedSecret({ name: promptSecret.name, namespace: promptSecret.namespace });
      } catch {
        // Best-effort cleanup — TTL or manual deletion will catch stragglers
      }
    }
  }

  // Parse Claude output (reuse claude_local parsing)
  if (jobTimedOut) {
    return {
      exitCode,
      signal: null,
      timedOut: true,
      errorMessage: `Timed out after ${timeoutSec}s`,
      errorCode: "timeout",
    };
  }

  const parsedStream = parseClaudeStreamJson(stdout);
  let parsed = parsedStream.resultJson;

  // Recovery path for malformed or partially duplicated streams: if a complete
  // result event is present as its own line, preserve the successful run rather
  // than reporting a parser failure.
  if (!parsed) {
    const recovered = scanForResultEvent(stdout);
    if (recovered) {
      await onLog(
        "stderr",
        `[paperclip] parseClaudeStreamJson returned null but stdout contains a result event ` +
          `(subtype=${String((recovered as { subtype?: unknown }).subtype ?? "")}, ` +
          `is_error=${(recovered as { is_error?: unknown }).is_error === true}); recovering directly.\n`,
      );
      parsed = recovered;
    }
  }

  // If the session was stale, clear it so the next heartbeat starts fresh.
  // Claude can reject resumed sessions when immutable thinking blocks in the
  // saved transcript no longer match the latest assistant turn; that has the
  // same recovery path as an unknown/missing session.
  if (
    parsed &&
    (exitCode ?? 0) !== 0 &&
    (isClaudeUnknownSessionError(parsed) || isClaudeImmutableThinkingBlockError(parsed))
  ) {
    if (isClaudeImmutableThinkingBlockError(parsed)) {
      const parsedSessionId = asString(parsed.session_id as string, "").trim();
      await quarantineClaudeSessionArtifacts({
        sessionId: parsedSessionId || currentSessionIdRaw || null,
        onLog,
      });
    }
    await onLog("stdout", `[paperclip] Claude session is unavailable; clearing for next run.\n`);
    return {
      exitCode,
      signal: null,
      timedOut: false,
      errorMessage: describeClaudeFailure(parsed) ?? "Session unavailable",
      errorCode: "session_unavailable",
      clearSession: true,
      resultJson: parsed,
    };
  }

  if (!parsed) {
    if (jobDeletedExternally && exitCode === null) {
      // Forensic context (FAR-107): users sometimes see this error when nothing
      // actually deleted the Job manually.  Surface enough state in the message
      // to distinguish self-delete (SIGTERM/cancel), TTL-after-completion, and
      // genuine external deletion without needing cluster shell access.
      const detailParts: string[] = [];
      if (jobGoneDetectionPath) detailParts.push(`detected_via=${jobGoneDetectionPath}`);
      detailParts.push(`job=${jobName}`);
      detailParts.push(`ns=${namespace}`);
      if (podRunningAt !== null && jobGoneAt !== null) {
        detailParts.push(`elapsed_since_pod_running=${Math.round((jobGoneAt - podRunningAt) / 1000)}s`);
      }
      detailParts.push(`completion_polls=${jobObserver.pollCount}`);
      const lastConds = jobObserver.lastConditions;
      if (lastConds && lastConds.length > 0) {
        const summary = lastConds
          .map((c) => `${c.type}=${c.status}${c.reason ? `(${c.reason})` : ""}`)
          .join(",");
        detailParts.push(`last_job_conditions=[${summary}]`);
      } else {
        detailParts.push("last_job_conditions=none_observed");
      }
      detailParts.push(`stdout_bytes=${stdout.length}`);
      const stdoutLines = stdout.split("\n").filter((l) => l.trim()).length;
      detailParts.push(`stdout_nonempty_lines=${stdoutLines}`);
      return {
        exitCode,
        signal: null,
        timedOut: false,
        errorMessage: `K8s Job was deleted externally before Claude could complete [${detailParts.join(", ")}]`,
        errorCode: "k8s_job_deleted_externally",
        resultJson: { stdout },
      };
    }
    if (parsedStream.llmApiEmptyResponse) {
      return {
        exitCode,
        signal: null,
        timedOut: false,
        errorMessage: "LLM API returned an empty response (stop_reason: null, output_tokens: 0) — the upstream model API may be degraded or misconfigured",
        errorCode: "llm_api_error",
        resultJson: { stdout },
      };
    }
    if (parsedStream.truncatedMidStream) {
      // Re-query pod state with retry — the initial single-shot read can lose
      // to kubelet propagation lag and surface a useless "pod state unavailable"
      // message that hides the real exit cause (OOMKilled, SIGTERM, etc).  The
      // retry distinguishes pod-genuinely-gone from terminated-state-lag and
      // gives the operator the actual exit code/reason where possible (FAR-107).
      let lookup: PodLookupResult | undefined;
      let refreshedState = podTerminatedState;
      try {
        lookup = await getPodLookupWithRetry(namespace, jobName, kubeconfigPath);
        refreshedState = lookup.state;
        if (refreshedState && refreshedState.exitCode !== null) {
          exitCode = refreshedState.exitCode;
        }
      } catch (err) {
        await onLog("stderr", `[paperclip] truncation diagnostic: pod re-query failed (${err instanceof Error ? err.message : String(err)})\n`).catch(() => {});
      }
      const cause = describeTruncationCause(refreshedState, lookup);
      const modelHint = parsedStream.model ? ` (model: ${parsedStream.model})` : "";
      return {
        exitCode,
        signal: null,
        timedOut: false,
        errorMessage: `Claude run was truncated mid-stream${modelHint} — assistant produced content but no result event arrived; ${cause}`,
        errorCode: "claude_truncated",
        resultJson: { stdout },
      };
    }
    return {
      exitCode,
      signal: null,
      timedOut: false,
      errorMessage: buildPartialRunError(exitCode, parsedStream.model, stdout, podTerminatedState, containerLogTail),
      resultJson: { stdout },
    };
  }

  const usage = parsedStream.usage ?? (() => {
    const usageObj = parseObject(parsed.usage as Record<string, unknown>);
    return {
      inputTokens: asNumber(usageObj.input_tokens, 0),
      cachedInputTokens: asNumber(usageObj.cache_read_input_tokens, 0),
      outputTokens: asNumber(usageObj.output_tokens, 0),
    };
  })();

  const fallbackSessionId = currentSessionIdRaw;
  const resolvedSessionId = parsedStream.sessionId
    ?? (asString(parsed.session_id as string, fallbackSessionId) || fallbackSessionId);
  const model = asString(config.model, "");
  const workspaceContext = parseObject(ctx.context.paperclipWorkspace);
  const workspaceId = asString(workspaceContext.workspaceId, "") || null;
  const workspaceRepoUrl = asString(workspaceContext.repoUrl, "") || null;
  const workspaceRepoRef = asString(workspaceContext.repoRef, "") || null;
  const cwd = asString(workspaceContext.cwd, "");

  const resolvedSessionParams = resolvedSessionId
    ? {
        sessionId: resolvedSessionId,
        ...(model ? { model } : {}),
        ...(cwd ? { cwd } : {}),
        ...(workspaceId ? { workspaceId } : {}),
        ...(workspaceRepoUrl ? { repoUrl: workspaceRepoUrl } : {}),
        ...(workspaceRepoRef ? { repoRef: workspaceRepoRef } : {}),
      } as Record<string, unknown>
    : null;

  const clearSessionForMaxTurns = isClaudeMaxTurnsResult(parsed);
  const failed = (exitCode ?? 0) !== 0;
  const baseErrorMessage = failed
    ? describeClaudeFailure(parsed) ?? `Claude exited with code ${exitCode ?? -1}`
    : null;
  // "Before any token usage": the run consumed no input/cached/output tokens.
  const zeroTokenProgress =
    usage.inputTokens === 0 && usage.cachedInputTokens === 0 && usage.outputTokens === 0;
  const upstream = classifyClaudeUpstreamFailure({
    failed,
    zeroTokenProgress,
    parsed,
    stdout,
    errorMessage: baseErrorMessage,
  });
  const capacityExhausted = upstream.family === "upstream_capacity_exhausted";
  const transientUpstream = upstream.family === "transient_upstream";
  const requestedModel =
    parsedStream.model || asString(parsed.model as string, "") || model || "(unknown)";
  // A no-capacity tier fails identically forever, so surface it clearly instead
  // of reporting `transient_upstream` (which feeds core's tight-reschedule loop).
  // Retries then come at the agent's normal heartbeat cadence — paced + visible.
  const errorMessage = capacityExhausted
    ? `Provider capacity exhausted for model "${requestedModel}" before any token usage (penstock: ${upstream.capacityCode}); all subscriptions for this tier are exhausted, so the run made no progress. Not retried as a transient throttle — add capacity for this model to the vault pool or reassign the agent to a healthy tier.`
    : baseErrorMessage;
  // retryNotBefore stays informative for both throttle families (a capacity 429
  // carries a Retry-After); on its own it does not re-arm the transient loop.
  const retryNotBefore = (transientUpstream || capacityExhausted)
    ? extractClaudeRetryNotBefore({ parsed, stdout, errorMessage: baseErrorMessage })
    : null;
  const resultJson = upstream.family
    ? {
        ...parsed,
        errorFamily: upstream.family,
        ...(upstream.capacityCode ? { upstreamCapacityCode: upstream.capacityCode } : {}),
        ...(retryNotBefore ? { retryNotBefore } : {}),
      }
    : parsed;

  return {
    exitCode,
    signal: null,
    timedOut: false,
    errorMessage,
    errorCode: upstream.errorCode,
    // errorFamily is the strict shared union (only "transient_upstream"). A
    // capacity-exhausted run is deliberately NOT tagged transient — that is what
    // stops core's tight-reschedule loop. Its structured signal rides errorMeta.
    errorFamily: transientUpstream ? "transient_upstream" : null,
    ...(capacityExhausted
      ? {
          errorMeta: {
            errorFamily: "upstream_capacity_exhausted",
            upstreamCapacityCode: upstream.capacityCode,
          } as Record<string, unknown>,
        }
      : {}),
    retryNotBefore,
    usage,
    sessionId: resolvedSessionId || null,
    sessionParams: resolvedSessionParams,
    sessionDisplayId: resolvedSessionId || null,
    provider: "anthropic",
    model: parsedStream.model || asString(parsed.model as string, model),
    billingType: "api",
    costUsd: parsedStream.costUsd ?? asNumber(parsed.total_cost_usd, 0),
    resultJson,
    summary: parsedStream.summary || asString(parsed.result as string, ""),
    clearSession: clearSessionForMaxTurns,
  } as AdapterExecutionResult;
}
