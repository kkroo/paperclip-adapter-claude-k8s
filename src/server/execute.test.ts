import { describe, it, expect, vi, beforeEach, afterEach } from "vitest";
import type * as k8s from "@kubernetes/client-node";
import type { Writable } from "node:stream";
import { readFile } from "node:fs/promises";
import type { AdapterExecutionContext } from "@paperclipai/adapter-utils";

// All K8s API mock functions — declared before vi.mock() so the factory can
// reference them.  The mock's logApi.log default is a never-resolving promise,
// simulating the FAR-10 hang where K8s API drops the connection indefinitely.
const mockLogFn = vi.fn();
const mockGetSelfPodInfo = vi.fn();
const mockBatchListJobs = vi.fn();
const mockBatchCreateJob = vi.fn();
const mockBatchReadJob = vi.fn();
const mockBatchDeleteJob = vi.fn();
const mockBatchPatchJob = vi.fn();
const mockCoreListPods = vi.fn();
const mockCoreReadPodLog = vi.fn();
const mockCoreCreateSecret = vi.fn();
const mockCorePatchSecret = vi.fn();
const mockCoreDeleteSecret = vi.fn();
// vi.hoisted ensures a single vi.fn() instance shared between the mock factory
// (which runs at hoist time) and the test body (which calls mockResolvedValue).
// A plain const would be re-assigned at its original position, leaving the
// factory with a stale reference to a different vi.fn() instance.
const mockReadSkillEntries = vi.hoisted(() => vi.fn());

// Module-level state for fs mock - kept for future use if mock is needed
const mockFsContent = new Map<string, string>();

vi.mock("./k8s-client.js", () => ({
  getLogApi: () => ({ log: mockLogFn }),
  getBatchApi: () => ({
    listNamespacedJob: mockBatchListJobs,
    createNamespacedJob: mockBatchCreateJob,
    readNamespacedJob: mockBatchReadJob,
    deleteNamespacedJob: mockBatchDeleteJob,
    patchNamespacedJob: mockBatchPatchJob,
  }),
  getCoreApi: () => ({
    listNamespacedPod: mockCoreListPods,
    readNamespacedPodLog: mockCoreReadPodLog,
    createNamespacedSecret: mockCoreCreateSecret,
    patchNamespacedSecret: mockCorePatchSecret,
    deleteNamespacedSecret: mockCoreDeleteSecret,
  }),
  getAuthzApi: () => ({}),
  getSelfPodInfo: mockGetSelfPodInfo,
  resetCache: vi.fn(),
}));

const mockPrepareBundle = vi.fn();
vi.mock("./prompt-cache.js", () => ({
  prepareClaudePromptBundle: mockPrepareBundle,
}));

vi.mock("@paperclipai/adapter-utils/server-utils", async (importOriginal) => {
  const original = await importOriginal<typeof import("@paperclipai/adapter-utils/server-utils")>();
  // Enumerate all original exports so transitive deps (job-manifest.ts, parse.ts,
  // prompt-cache.ts, etc.) keep working.  Only readPaperclipRuntimeSkillEntries
  // is replaced so tests run without real fs.stat I/O under fake timers.
  return Object.assign(Object.create(null), original, {
    readPaperclipRuntimeSkillEntries: mockReadSkillEntries,
  });
});

const {
  isK8s404,
  buildPartialRunError,
  classifyOrphan,
  describePodTerminatedError,
  describeTruncationCause,
  extractContainerLogDiagnostic,
  shouldAbortForCancellation,
  execute,
} = await import("./execute.js");

function makeJob(opts: {
  runId?: string;
  agentId?: string;
  taskId?: string;
  sessionId?: string;
  isolationMode?: "isolated" | "shared" | "run" | "workspace";
  isolationKey?: string;
  adapterType?: string;
  terminal?: boolean;
}): k8s.V1Job {
  const labels: Record<string, string> = {
    "paperclip.io/adapter-type": opts.adapterType ?? "claude_k8s",
  };
  if (opts.agentId) labels["paperclip.io/agent-id"] = opts.agentId;
  if (opts.runId) labels["paperclip.io/run-id"] = opts.runId;
  if (opts.taskId) labels["paperclip.io/task-id"] = opts.taskId;
  if (opts.sessionId) labels["paperclip.io/session-id"] = opts.sessionId;
  if (opts.isolationMode) labels["paperclip.io/isolation-mode"] = opts.isolationMode;
  if (opts.isolationKey) labels["paperclip.io/isolation-key"] = opts.isolationKey;
  return {
    metadata: { name: "ac-job", namespace: "paperclip", labels },
    status: opts.terminal
      ? { conditions: [{ type: "Complete", status: "True" }] }
      : { conditions: [] },
  } as k8s.V1Job;
}

describe("isK8s404", () => {
  it("returns false for non-Error values", () => {
    expect(isK8s404(null)).toBe(false);
    expect(isK8s404(undefined)).toBe(false);
    expect(isK8s404("string error")).toBe(false);
    expect(isK8s404(404)).toBe(false);
  });

  it("returns false for unrelated errors", () => {
    expect(isK8s404(new Error("something went wrong"))).toBe(false);
    expect(isK8s404(new Error("HTTP-Code: 500 Message: Internal Server Error"))).toBe(false);
  });

  it("detects 404 from v1.0+ message format", () => {
    const err = new Error("HTTP-Code: 404 Message: Unknown API Status Code! Body: ...");
    expect(isK8s404(err)).toBe(true);
  });

  it("detects 404 from v0.x response.statusCode", () => {
    const err = Object.assign(new Error("Not Found"), {
      response: { statusCode: 404 },
    });
    expect(isK8s404(err)).toBe(true);
  });

  it("detects 404 from v1.0+ response.status", () => {
    const err = Object.assign(new Error("Not Found"), {
      response: { status: 404 },
    });
    expect(isK8s404(err)).toBe(true);
  });

  it("detects 404 from direct statusCode property", () => {
    const err = Object.assign(new Error("Not Found"), { statusCode: 404 });
    expect(isK8s404(err)).toBe(true);
  });

  it("does not match non-404 status codes on response", () => {
    const err = Object.assign(new Error("Forbidden"), {
      response: { statusCode: 403 },
    });
    expect(isK8s404(err)).toBe(false);
  });
});

describe("buildPartialRunError", () => {
  const initLine = JSON.stringify({
    type: "system",
    subtype: "init",
    model: "claude-sonnet-4-6",
    session_id: "sess_abc",
  });

  it("returns parse-failure message when exitCode is 0", () => {
    expect(buildPartialRunError(0, "", "")).toBe("Failed to parse Claude JSON output");
    expect(buildPartialRunError(0, "claude-sonnet-4-6", initLine)).toBe(
      "Failed to parse Claude JSON output",
    );
  });

  it("returns generic exit message when stdout is empty", () => {
    expect(buildPartialRunError(1, "", "")).toBe("Claude exited with code 1");
    expect(buildPartialRunError(null, "", "")).toBe("Claude exited with code -1");
  });

  it("returns init-only message when stdout is init-only with non-zero exit code (FAR-101)", () => {
    const msg = buildPartialRunError(1, "claude-sonnet-4-6", initLine);
    expect(msg).toBe(
      "Claude exited immediately after init (model: claude-sonnet-4-6) (exit code 1) — the model may be unsupported or the session may have been rejected before producing output",
    );
  });

  it("includes model from parsedStream when stdout is init-only", () => {
    const msg = buildPartialRunError(null, "MiniMax-M2.7", initLine);
    expect(msg).toContain("MiniMax-M2.7");
    expect(msg).not.toContain("type");
    expect(msg).not.toContain("system");
  });

  it("uses first non-system line as content when present", () => {
    const stdout = [initLine, "Error: no API key configured"].join("\n");
    const msg = buildPartialRunError(1, "claude-sonnet-4-6", stdout);
    expect(msg).toBe("Claude exited with code 1: Error: no API key configured");
  });

  it("returns init-only message when stdout has init + result event but no plain content (structured artefact, not surfaced verbatim)", () => {
    // In production, buildPartialRunError is only called when parseClaudeStreamJson
    // returns null (no result event).  If somehow a result event appears here, the
    // raw JSON blob must not be shown — the init-only message is cleaner and avoids
    // leaking protocol internals to the UI.
    const resultLike = JSON.stringify({ type: "result", subtype: "error", result: "rate limit" });
    const stdout = [initLine, resultLike].join("\n");
    const msg = buildPartialRunError(2, "claude-sonnet-4-6", stdout);
    expect(msg).toContain("Claude exited immediately after init");
    expect(msg).toContain("claude-sonnet-4-6");
    expect(msg).not.toMatch(/\{.*type.*result/);
  });

  it("skips rate_limit_event and surfaces model hint (FAR-32 Anthropic/Nancy repro)", () => {
    // Reproduces the second variant from FAR-32: init event + rate_limit_event +
    // assistant event (thinking only, no result).  The rate_limit_event JSON blob
    // must not appear verbatim in the error message.
    const rateLimitEvent = JSON.stringify({
      type: "rate_limit_event",
      rate_limit_info: { status: "allowed", resetsAt: 1777056000, rateLimitType: "five_hour" },
      uuid: "3ab8f9eb-b9d6-4bf6-9c39-4608427717fc",
      session_id: "ad5f3e11-3c0c-4144-b53d-d4b959e57cee",
    });
    const stdout = [initLine, rateLimitEvent].join("\n");
    const msg = buildPartialRunError(null, "claude-opus-4-7", stdout);
    expect(msg).toContain("claude-opus-4-7");
    expect(msg).toContain("did not produce a result");
    expect(msg).not.toContain("rate_limit_event");
    expect(msg).not.toContain("rateLimitType");
  });

  it("skips assistant events and surfaces model hint (FAR-32: MiniMax-M2.7 output_tokens=0)", () => {
    // Reproduces the exact failure: init event + assistant event with only a
    // thinking block and output_tokens=0, no result event.  The assistant JSON
    // blob must not be surfaced verbatim as the error message.
    const assistantEvent = JSON.stringify({
      type: "assistant",
      message: {
        id: "063ad6038e4c889faa7c95168e007d73",
        type: "message",
        role: "assistant",
        content: [{ type: "thinking", thinking: "Let me start…", signature: "abc123" }],
        model: "MiniMax-M2.7",
        stop_reason: null,
        stop_sequence: null,
        usage: { input_tokens: 11013, output_tokens: 0, cache_creation_input_tokens: 0, cache_read_input_tokens: 0 },
      },
    });
    const stdout = [initLine, assistantEvent].join("\n");
    const msg = buildPartialRunError(null, "MiniMax-M2.7", stdout);
    expect(msg).toContain("MiniMax-M2.7");
    expect(msg).toContain("did not produce a result");
    expect(msg).not.toContain("063ad6038e4c889faa7c95168e007d73");
    expect(msg).not.toContain("output_tokens");
    expect(msg).not.toContain("thinking");
  });

  it("skips user events alongside system events", () => {
    const userEvent = JSON.stringify({ type: "user", message: { role: "user", content: [] } });
    const stdout = [initLine, userEvent, "Error: API quota exceeded"].join("\n");
    const msg = buildPartialRunError(1, "claude-sonnet-4-6", stdout);
    expect(msg).toBe("Claude exited with code 1: Error: API quota exceeded");
  });

  it("null exitCode renders as -1 in message", () => {
    const msg = buildPartialRunError(null, "", "Some plain error text");
    expect(msg).toBe("Claude exited with code -1: Some plain error text");
  });

  it("skips multiple consecutive system events", () => {
    const anotherSystem = JSON.stringify({ type: "system", subtype: "other" });
    const stdout = [initLine, anotherSystem, "real error line"].join("\n");
    const msg = buildPartialRunError(1, "model-x", stdout);
    expect(msg).toBe("Claude exited with code 1: real error line");
  });

  it("appends pod terminated reason/message when state is provided (FAR-100)", () => {
    const msg = buildPartialRunError(1, "claude-sonnet-4-6", initLine, {
      exitCode: 1,
      reason: "Error",
      message: "model not supported",
      signal: null,
    });
    expect(msg).toContain("Claude exited immediately after init");
    expect(msg).toContain("claude-sonnet-4-6");
    expect(msg).toContain("[pod: reason=Error, message=model not supported]");
  });

  it("flags exit 137 as OOMKilled in pod cause", () => {
    const msg = buildPartialRunError(137, "claude-sonnet-4-6", initLine, {
      exitCode: 137,
      reason: "OOMKilled",
      message: null,
      signal: null,
    });
    expect(msg).toContain("[pod: reason=OOMKilled, SIGKILL (commonly OOMKilled)]");
  });

  it("appends pod cause to content-line message", () => {
    const stdout = [initLine, "Error: bad request"].join("\n");
    const msg = buildPartialRunError(1, "claude-sonnet-4-6", stdout, {
      exitCode: 1,
      reason: "Error",
      message: null,
      signal: null,
    });
    expect(msg).toBe("Claude exited with code 1: Error: bad request [pod: reason=Error]");
  });

  it("does not append anything when podState is null (back-compat)", () => {
    const msg = buildPartialRunError(1, "claude-sonnet-4-6", initLine, null);
    expect(msg).not.toContain("[pod:");
  });

  it("appends a container log tail for empty stdout failures", () => {
    const msg = buildPartialRunError(128, "claude-opus-5[1m]", "", {
      exitCode: 128,
      reason: "Error",
      message: null,
      signal: null,
    }, "OAuth failed: Bearer <redacted> and token=<redacted>");

    expect(msg).toContain("Claude exited with code 128");
    expect(msg).toContain("reason=Error");
    expect(msg).toContain("container_log=OAuth failed: Bearer <redacted>");
    expect(msg).toContain("token=<redacted>");
  });

  it("redacts sensitive tokens in container log diagnostics", () => {
    const diagnostic = extractContainerLogDiagnostic([
      "ignored",
      "OAuth failed: Bearer eyJhbGciOiFake.token and token=secret-value",
      "figma=figd_secret gbrain=gbrain_at_secret api_key=abc123",
    ].join("\n"));

    expect(diagnostic).toContain("OAuth failed: Bearer <redacted>");
    expect(diagnostic).toContain("token=<redacted>");
    expect(diagnostic).toContain("figd_<redacted>");
    expect(diagnostic).toContain("gbrain_at_<redacted>");
    expect(diagnostic).toContain("api_key=<redacted>");
    expect(diagnostic).not.toContain("eyJhbGciOiFake");
    expect(diagnostic).not.toContain("secret-value");
  });
});

describe("classifyOrphan", () => {
  const taskId = "task-xyz";
  const sessionId = "sess-123";

  // --- Happy path: reattach ---
  it("returns reattach when taskId matches and both sessionIds match", () => {
    const job = makeJob({ taskId, sessionId });
    expect(classifyOrphan(job, { taskId, sessionId })).toBe("reattach");
  });

  it("returns reattach when taskId matches and expected sessionId is null (missing on current side)", () => {
    const job = makeJob({ taskId, sessionId });
    expect(classifyOrphan(job, { taskId, sessionId: null })).toBe("reattach");
  });

  it("returns reattach when taskId matches and job has no session-id label (missing on job side)", () => {
    const job = makeJob({ taskId });
    expect(classifyOrphan(job, { taskId, sessionId })).toBe("reattach");
  });

  it("returns reattach when taskId matches and neither side has a sessionId", () => {
    const job = makeJob({ taskId });
    expect(classifyOrphan(job, { taskId, sessionId: null })).toBe("reattach");
  });

  // --- Block: task unknown ---
  it("returns block_task_unknown when expected taskId is null", () => {
    const job = makeJob({ taskId, sessionId });
    expect(classifyOrphan(job, { taskId: null, sessionId })).toBe("block_task_unknown");
  });

  it("returns block_task_unknown when job has no task-id label", () => {
    const job = makeJob({ sessionId });
    expect(classifyOrphan(job, { taskId, sessionId })).toBe("block_task_unknown");
  });

  // --- Block: task mismatch ---
  it("returns block_task_mismatch when both sides have taskId but they differ", () => {
    const job = makeJob({ taskId: "task-other", sessionId });
    expect(classifyOrphan(job, { taskId, sessionId })).toBe("block_task_mismatch");
  });

  // --- Block: session mismatch ---
  it("returns block_session_mismatch when taskId matches but sessionIds differ", () => {
    const job = makeJob({ taskId, sessionId: "sess-other" });
    expect(classifyOrphan(job, { taskId, sessionId })).toBe("block_session_mismatch");
  });

  // --- Terminal orphan (caller filters these before classifyOrphan) ---
  it("returns reattach for terminal job (caller is responsible for filtering terminals)", () => {
    const job = makeJob({ taskId, sessionId, terminal: true });
    // classifyOrphan does not check terminal status — that is the caller's job
    expect(classifyOrphan(job, { taskId, sessionId })).toBe("reattach");
  });
});

// Regression: FAR-10 — waitForPod must throw on phase=Failed, not return the pod name.
// These tests cover describePodTerminatedError, the helper that waitForPod uses to build
// the error message before throwing.  Verifies that phase=Failed with no claude logs
// produces a structured, actionable error instead of silently entering the log-stream path.
describe("describePodTerminatedError", () => {
  it("includes exit code and reason when claude container status is available", () => {
    const cs = [
      {
        name: "claude",
        state: { terminated: { exitCode: 137, reason: "OOMKilled" } },
      },
    ] as k8s.V1ContainerStatus[];
    const msg = describePodTerminatedError("mypod", "Failed", cs);
    expect(msg).toContain("137");
    expect(msg).toContain("OOMKilled");
    expect(msg).toContain("phase=Failed");
  });

  it("falls back to message field when reason is absent", () => {
    const cs = [
      {
        name: "claude",
        state: { terminated: { exitCode: 1, message: "signal: killed" } },
      },
    ] as k8s.V1ContainerStatus[];
    const msg = describePodTerminatedError("mypod", "Failed", cs);
    expect(msg).toContain("signal: killed");
    expect(msg).toContain("1");
  });

  it("returns generic message when no claude container status is present", () => {
    const msg = describePodTerminatedError("mypod", "Failed", []);
    expect(msg).toBe("Pod mypod reached phase=Failed");
  });

  it("ignores non-claude containers", () => {
    const cs = [
      {
        name: "sidecar",
        state: { terminated: { exitCode: 0, reason: "Completed" } },
      },
    ] as k8s.V1ContainerStatus[];
    const msg = describePodTerminatedError("mypod", "Failed", cs);
    expect(msg).toBe("Pod mypod reached phase=Failed");
  });

  it("handles null exitCode gracefully", () => {
    const cs = [
      {
        name: "claude",
        state: { terminated: { exitCode: null, reason: "Error" } },
      },
    ] as unknown as k8s.V1ContainerStatus[];
    const msg = describePodTerminatedError("mypod", "Failed", cs);
    expect(msg).toContain("unknown");
    expect(msg).toContain("Error");
  });
});

describe("describeTruncationCause", () => {
  it("annotates exit code 137 as SIGKILL/OOM", () => {
    const msg = describeTruncationCause({ exitCode: 137, reason: "OOMKilled", message: "Memory cgroup out of memory", signal: null });
    expect(msg).toContain("exit code 137");
    expect(msg).toContain("SIGKILL");
    expect(msg).toContain("OOMKilled");
    expect(msg).toContain("Memory cgroup out of memory");
  });

  it("annotates exit code 143 as SIGTERM", () => {
    const msg = describeTruncationCause({ exitCode: 143, reason: null, message: null, signal: null });
    expect(msg).toContain("exit code 143");
    expect(msg).toContain("SIGTERM");
  });

  it("falls back to 'pod state unavailable' when state is null", () => {
    const msg = describeTruncationCause(null);
    expect(msg).toContain("pod state unavailable");
  });

  it("emits 'no exit code' when exitCode is null but state exists", () => {
    const msg = describeTruncationCause({ exitCode: null, reason: "Error", message: null, signal: null });
    expect(msg).toContain("no exit code");
    expect(msg).toContain("reason=Error");
  });
});

describe("execute: all-invalid agent.id (N4)", () => {
  it("returns hard error without creating a Job when agent.id sanitizes to null", async () => {
    const logs: string[] = [];
    const result = await execute({
      runId: "run-001",
      agent: { id: "@@@", companyId: "co1", name: "Bad Agent", adapterType: "claude_k8s", adapterConfig: {} },
      runtime: { sessionId: null, sessionParams: null, sessionDisplayId: null, taskKey: null },
      config: {},
      context: {},
      onLog: async (_stream, msg) => { logs.push(msg); },
    });
    expect(result.errorCode).toBe("k8s_agent_id_invalid");
    expect(result.errorMessage).toContain("@@@");
    // getSelfPodInfo must NOT have been called (early return before K8s calls)
    const { getSelfPodInfo } = await import("./k8s-client.js");
    expect(getSelfPodInfo).not.toHaveBeenCalled();
  });
});


// ─── Helpers shared across execute() integration tests ───────────────────────

function makeCtx(overrides: Partial<AdapterExecutionContext> = {}): AdapterExecutionContext {
  return {
    runId: "run-test-001",
    agent: {
      id: "agent-abc",
      companyId: "co1",
      name: "Test Agent",
      adapterType: "claude_k8s",
      adapterConfig: {},
    },
    runtime: { sessionId: null, sessionParams: null, sessionDisplayId: null, taskKey: null },
    config: {},
    context: {},
    onLog: vi.fn().mockResolvedValue(undefined),
    onExternalRuntimeLaunched: vi.fn().mockResolvedValue(undefined),
    ...overrides,
  } as unknown as AdapterExecutionContext;
}

function makeIsolatedRuntime(isolationKey: string): AdapterExecutionContext["runtime"] {
  return {
    sessionId: null,
    sessionParams: null,
    sessionDisplayId: null,
    taskKey: null,
    isolation: {
      isolationMode: "workspace",
      isolationKey,
      workspaceRoot: "/paperclip/workspaces/workspace-1",
      homeRoot: "/paperclip/k8s-isolation/workspace-1/home",
      sessionRoot: "/paperclip/k8s-isolation/workspace-1/session",
      cacheRoot: "/runtime-cache/paperclip-workspaces/workspace-1/cache",
      tmpRoot: "/runtime-cache/paperclip-workspaces/workspace-1/tmp",
      storage: {
        workspace: "persistent",
        home: "persistent",
        session: "persistent",
        cache: "ephemeral",
      },
      sessionScope: { taskKey: "task-current", isolationKey },
    } as NonNullable<AdapterExecutionContext["runtime"]["isolation"]>,
  };
}

function makeSelfPodResult() {
  return {
    namespace: "paperclip",
    image: "paperclipai/paperclip:latest",
    imagePullSecrets: [],
    dnsConfig: undefined,
    nodeSelector: {},
    tolerations: [],
    pvcClaimName: "paperclip-data",
    secretVolumes: [],
    inheritedEnv: {},
    inheritedEnvValueFrom: [],
    inheritedEnvFrom: [],
  };
}

function makeBundle() {
  return {
    bundleKey: "test-bundle",
    rootDir: "/tmp/test-bundle",
    addDir: "/tmp/test-bundle",
    instructionsFilePath: null,
  };
}

// Valid minimal Claude stream-json output used in happy-path tests.
const CLAUDE_HAPPY_OUTPUT = [
  JSON.stringify({ type: "system", subtype: "init", model: "claude-sonnet-4-6", session_id: "sess_test123" }),
  JSON.stringify({
    type: "result",
    subtype: "success",
    result: "Done.",
    session_id: "sess_test123",
    usage: { input_tokens: 100, output_tokens: 50, cache_read_input_tokens: 10 },
    total_cost_usd: 0.001,
  }),
].join("\n") + "\n";

describe("execute: stdout accumulator regression", () => {
  it("assigns the captured pod log to the outer stdout used by the parser", async () => {
    const source = await readFile(new URL("./execute.ts", import.meta.url), "utf-8");
    expect(source).toMatch(/\n\s*stdout = tailResult\.status === "fulfilled" \? tailResult\.value : "";/);
    expect(source).not.toMatch(/\n\s*let stdout = tailResult\.status === "fulfilled" \? tailResult\.value : "";/);
  });
});

// ─── execute: concurrency guard paths ────────────────────────────────────────

describe("execute: concurrency guard", () => {
  beforeEach(() => {
    vi.clearAllMocks();
    vi.unstubAllGlobals();
    process.env.PAPERCLIP_API_URL = "https://paperclip.test";
    vi.stubGlobal("fetch", vi.fn().mockResolvedValue({
      ok: true,
      status: 200,
      json: async () => ({ id: "prior-run", status: "running", startedAt: new Date().toISOString() }),
    }));
    mockReadSkillEntries.mockResolvedValue([]);
    mockGetSelfPodInfo.mockResolvedValue(makeSelfPodResult());
  });

  afterEach(() => {
    vi.unstubAllGlobals();
    delete process.env.PAPERCLIP_API_URL;
  });

  it("returns k8s_concurrency_guard_unreachable when listNamespacedJob throws", async () => {
    mockBatchListJobs.mockRejectedValue(new Error("K8s API unavailable"));
    const result = await execute(makeCtx());
    expect(result.errorCode).toBe("k8s_concurrency_guard_unreachable");
    expect(result.errorMessage).toContain("K8s API unavailable");
  });

  it("fails closed when listNamespacedJob never settles", async () => {
    vi.useFakeTimers();
    try {
      mockBatchListJobs.mockImplementation(() => new Promise(() => {}));
      const resultPromise = execute(makeCtx());

      await vi.advanceTimersByTimeAsync(15_000);
      const result = await resultPromise;

      expect(result.errorCode).toBe("k8s_concurrency_guard_unreachable");
      expect(result.errorMessage).toContain("timed out after 15s");
      expect(mockBatchCreateJob).not.toHaveBeenCalled();
    } finally {
      vi.useRealTimers();
    }
  });

  it("returns k8s_concurrent_run_blocked when reattach disabled and orphan is running", async () => {
    const orphan = makeJob({ runId: "prior-run", agentId: "agent-abc", terminal: false });
    mockBatchListJobs.mockResolvedValue({ items: [orphan] });
    const result = await execute(makeCtx({ config: { reattachOrphanedJobs: false } } as Partial<AdapterExecutionContext>));
    expect(result.errorCode).toBe("k8s_concurrent_run_blocked");
    expect(result.errorMessage).toContain("active run");
  });

  it("deletes stale orphan and proceeds when the run-id lookup is terminal", async () => {
    process.env.PAPERCLIP_API_URL = "https://paperclip.test";
    const orphan = makeJob({ runId: "prior-run", agentId: "agent-abc", taskId: "task-current" });
    mockBatchListJobs.mockResolvedValue({ items: [orphan] });
    mockBatchDeleteJob.mockResolvedValue({});
    mockBatchCreateJob.mockRejectedValue(new Error("create reached"));
    mockPrepareBundle.mockResolvedValue(makeBundle());
    const fetchMock = vi.fn().mockResolvedValue({
      ok: true,
      status: 200,
      json: async () => ({ id: "prior-run", status: "failed", finishedAt: "2026-05-25T00:00:00.000Z" }),
    });
    vi.stubGlobal("fetch", fetchMock);

    const result = await execute(makeCtx({ context: { taskId: "task-current" }, authToken: "token-1" } as Partial<AdapterExecutionContext>));

    expect(fetchMock).toHaveBeenCalledWith("https://paperclip.test/api/heartbeat-runs/prior-run", {
      headers: { Authorization: "Bearer token-1" },
    });
    expect(mockBatchDeleteJob).toHaveBeenCalledWith({
      name: "ac-job",
      namespace: "paperclip",
      body: { propagationPolicy: "Background" },
    });
    expect(result.errorCode).toBe("k8s_job_create_failed");
    expect(result.errorMessage).toContain("create reached");
  });

  it("deletes stale orphan and proceeds when the run-id lookup is missing", async () => {
    process.env.PAPERCLIP_API_URL = "https://paperclip.test";
    const orphan = makeJob({ runId: "missing-run", agentId: "agent-abc", taskId: "task-current" });
    mockBatchListJobs.mockResolvedValue({ items: [orphan] });
    mockBatchDeleteJob.mockResolvedValue({});
    mockBatchCreateJob.mockRejectedValue(new Error("create reached"));
    mockPrepareBundle.mockResolvedValue(makeBundle());
    vi.stubGlobal("fetch", vi.fn().mockResolvedValue({ ok: false, status: 404 }));

    const result = await execute(makeCtx({ context: { taskId: "task-current" } } as Partial<AdapterExecutionContext>));

    expect(mockBatchDeleteJob).toHaveBeenCalledWith({
      name: "ac-job",
      namespace: "paperclip",
      body: { propagationPolicy: "Background" },
    });
    expect(result.errorCode).toBe("k8s_job_create_failed");
  });

  it("blocks when the run-id lookup is still active", async () => {
    process.env.PAPERCLIP_API_URL = "https://paperclip.test";
    const orphan = makeJob({ runId: "active-run", agentId: "agent-abc", taskId: "task-current" });
    mockBatchListJobs.mockResolvedValue({ items: [orphan] });
    vi.stubGlobal("fetch", vi.fn().mockResolvedValue({
      ok: true,
      status: 200,
      json: async () => ({ id: "active-run", status: "running", startedAt: new Date().toISOString() }),
    }));

    const result = await execute(makeCtx({ context: { taskId: "task-current" } } as Partial<AdapterExecutionContext>));

    expect(mockBatchDeleteJob).not.toHaveBeenCalled();
    expect(result.errorCode).toBe("k8s_concurrent_run_blocked");
    expect(result.errorMessage).toContain("active run");
  });

  it("allows active jobs with a different isolation key", async () => {
    process.env.PAPERCLIP_API_URL = "https://paperclip.test";
    const other = makeJob({
      runId: "active-run",
      agentId: "agent-abc",
      taskId: "task-other",
      isolationMode: "workspace",
      isolationKey: "other-key",
    });
    mockBatchListJobs.mockResolvedValue({ items: [other] });
    mockBatchCreateJob.mockRejectedValue(new Error("create reached"));
    mockPrepareBundle.mockResolvedValue(makeBundle());
    vi.stubGlobal("fetch", vi.fn().mockResolvedValue({
      ok: true,
      status: 200,
      json: async () => ({ id: "active-run", status: "running", startedAt: new Date().toISOString() }),
    }));

    const result = await execute(makeCtx({
      config: { isolationMode: "isolated", isolationKey: "ignored-config-key" },
      runtime: makeIsolatedRuntime("current-key"),
    }));

    expect(result.errorCode).toBe("k8s_job_create_failed");
    expect(result.errorMessage).toContain("create reached");
    expect(mockPrepareBundle).toHaveBeenCalledWith(expect.objectContaining({
      rootDir: expect.stringContaining("current-key/prompt-cache"),
    }));
  });

  it("recognizes legacy isolated labels when allowing a different isolation key", async () => {
    const other = makeJob({
      runId: "active-run",
      agentId: "agent-abc",
      taskId: "task-other",
      isolationMode: "isolated",
      isolationKey: "legacy-other-key",
    });
    mockBatchListJobs.mockResolvedValue({ items: [other] });
    mockBatchCreateJob.mockRejectedValue(new Error("create reached"));
    mockPrepareBundle.mockResolvedValue(makeBundle());
    vi.stubGlobal("fetch", vi.fn().mockResolvedValue({
      ok: true,
      status: 200,
      json: async () => ({ id: "active-run", status: "running", startedAt: new Date().toISOString() }),
    }));

    const result = await execute(makeCtx({ runtime: makeIsolatedRuntime("current-key") }));

    expect(result.errorCode).toBe("k8s_job_create_failed");
    expect(result.errorMessage).toContain("create reached");
  });

  it("blocks active jobs with the same isolation key", async () => {
    process.env.PAPERCLIP_API_URL = "https://paperclip.test";
    const same = makeJob({
      runId: "active-run",
      agentId: "agent-abc",
      taskId: "task-current",
      isolationMode: "workspace",
      isolationKey: "same-key",
    });
    mockBatchListJobs.mockResolvedValue({ items: [same] });
    vi.stubGlobal("fetch", vi.fn().mockResolvedValue({
      ok: true,
      status: 200,
      json: async () => ({ id: "active-run", status: "running", startedAt: new Date().toISOString() }),
    }));

    const result = await execute(makeCtx({ runtime: makeIsolatedRuntime("same-key") }));

    expect(mockBatchCreateJob).not.toHaveBeenCalled();
    expect(result.errorCode).toBe("k8s_concurrent_run_blocked");
  });

  it("reports the conflicting isolation key, task, and session for an isolated block", async () => {
    const same = makeJob({
      runId: "active-run",
      agentId: "agent-abc",
      taskId: "task-conflict",
      sessionId: "session-conflict",
      isolationMode: "workspace",
      isolationKey: "same-key",
    });
    mockBatchListJobs.mockResolvedValue({ items: [same] });
    const fetchMock = vi.fn().mockImplementation(async (_url: string, init?: RequestInit) => {
      if (init?.method === "POST") return { ok: true, status: 202 };
      return {
        ok: true,
        status: 200,
        json: async () => ({ id: "active-run", status: "running", startedAt: new Date().toISOString() }),
      };
    });
    vi.stubGlobal("fetch", fetchMock);

    const result = await execute(makeCtx({
      authToken: "run-token",
      runtime: makeIsolatedRuntime("same-key"),
    }));

    expect(result.errorCode).toBe("k8s_concurrent_run_blocked");
    const reportCall = fetchMock.mock.calls.find(([url, init]) =>
      String(url).endsWith("/api/metrics/claude-k8s/concurrent-run-blocked")
      && (init as RequestInit | undefined)?.method === "POST"
    );
    expect(reportCall).toBeDefined();
    expect(JSON.parse((reportCall![1] as RequestInit).body as string)).toEqual({
      agentId: "agent-abc",
      companyId: "co1",
      reason: "live_job_for_active_run",
      isolationMode: "workspace",
      isolationKey: "same-key",
      taskKey: "task-conflict",
      sessionId: "session-conflict",
    });
  });

  it("fails closed and reports unknown isolation metadata for an unlabeled live Job", async () => {
    const unlabeled = makeJob({
      runId: "active-run",
      agentId: "agent-abc",
      taskId: "task-conflict",
    });
    mockBatchListJobs.mockResolvedValue({ items: [unlabeled] });
    const fetchMock = vi.fn().mockImplementation(async (_url: string, init?: RequestInit) => {
      if (init?.method === "POST") return { ok: true, status: 202 };
      return {
        ok: true,
        status: 200,
        json: async () => ({ id: "active-run", status: "running", startedAt: new Date().toISOString() }),
      };
    });
    vi.stubGlobal("fetch", fetchMock);

    const result = await execute(makeCtx({
      authToken: "run-token",
      runtime: makeIsolatedRuntime("current-key"),
    }));

    expect(result.errorCode).toBe("k8s_concurrent_run_blocked");
    const reportCall = fetchMock.mock.calls.find(([url, init]) =>
      String(url).endsWith("/api/metrics/claude-k8s/concurrent-run-blocked")
      && (init as RequestInit | undefined)?.method === "POST"
    );
    expect(reportCall).toBeDefined();
    expect(JSON.parse((reportCall![1] as RequestInit).body as string)).toMatchObject({
      reason: "unknown_isolation_blocked",
      isolationMode: "workspace",
      isolationKey: "current-key",
      taskKey: "task-conflict",
    });
  });

  it("reports shared-mode serialization without changing the fail-closed result", async () => {
    const active = makeJob({
      runId: "active-run",
      agentId: "agent-abc",
      taskId: "task-conflict",
      isolationMode: "shared",
      isolationKey: "agent-sharedagent-abc",
    });
    mockBatchListJobs.mockResolvedValue({ items: [active] });
    const fetchMock = vi.fn().mockImplementation(async (_url: string, init?: RequestInit) => {
      if (init?.method === "POST") return { ok: true, status: 202 };
      return {
        ok: true,
        status: 200,
        json: async () => ({ id: "active-run", status: "running", startedAt: new Date().toISOString() }),
      };
    });
    vi.stubGlobal("fetch", fetchMock);

    const result = await execute(makeCtx({ authToken: "run-token" }));

    expect(result.errorCode).toBe("k8s_concurrent_run_blocked");
    const reportCall = fetchMock.mock.calls.find(([url, init]) =>
      String(url).endsWith("/api/metrics/claude-k8s/concurrent-run-blocked")
      && (init as RequestInit | undefined)?.method === "POST"
    );
    expect(reportCall).toBeDefined();
    expect(JSON.parse((reportCall![1] as RequestInit).body as string)).toMatchObject({
      reason: "shared_mode_serialized",
      isolationMode: "shared",
      isolationKey: "agent-sharedagent-abc",
      taskKey: "task-conflict",
    });
  });

  it("still blocks unlabeled jobs as unknown orphans without consulting the run table", async () => {
    process.env.PAPERCLIP_API_URL = "https://paperclip.test";
    const orphan = makeJob({ agentId: "agent-abc", taskId: "task-current" });
    mockBatchListJobs.mockResolvedValue({ items: [orphan] });
    const fetchMock = vi.fn();
    vi.stubGlobal("fetch", fetchMock);

    const result = await execute(makeCtx({ context: { taskId: "task-current" } } as Partial<AdapterExecutionContext>));

    expect(fetchMock).not.toHaveBeenCalled();
    expect(result.errorCode).toBe("k8s_orphan_task_unknown");
  });

  it("returns k8s_concurrent_run_blocked when active run orphan has no task label", async () => {
    const orphan = makeJob({ runId: "prior-run", agentId: "agent-abc" }); // no taskId label
    mockBatchListJobs.mockResolvedValue({ items: [orphan] });
    const result = await execute(makeCtx());
    expect(result.errorCode).toBe("k8s_concurrent_run_blocked");
    expect(result.errorMessage).toContain("active run");
  });

  it("returns k8s_concurrent_run_blocked when orphan task-id mismatches current task", async () => {
    const orphan = makeJob({ runId: "prior-run", agentId: "agent-abc", taskId: "task-other" });
    mockBatchListJobs.mockResolvedValue({ items: [orphan] });
    const result = await execute(
      makeCtx({ context: { taskId: "task-current" } } as Partial<AdapterExecutionContext>),
    );
    expect(result.errorCode).toBe("k8s_concurrent_run_blocked");
    expect(result.errorMessage).toContain("active run");
  });

  it("returns k8s_concurrent_run_blocked when active run task matches but session differs", async () => {
    const orphan = makeJob({
      runId: "prior-run",
      agentId: "agent-abc",
      taskId: "task-match",
      sessionId: "sess-other",
    });
    mockBatchListJobs.mockResolvedValue({ items: [orphan] });
    const result = await execute(
      makeCtx({
        context: { taskId: "task-match" },
        runtime: { sessionId: "sess-current", sessionParams: null, sessionDisplayId: null, taskKey: null },
      } as Partial<AdapterExecutionContext>),
    );
    expect(result.errorCode).toBe("k8s_concurrent_run_blocked");
    expect(result.errorMessage).toContain("active run");
  });

  it("returns k8s_concurrent_run_blocked when same-run job is still running", async () => {
    // runId matches → samRun.length > 0 → blocked
    const sameRunJob = makeJob({ runId: "run-test-001", agentId: "agent-abc", terminal: false });
    mockBatchListJobs.mockResolvedValue({ items: [sameRunJob] });
    const result = await execute(makeCtx());
    expect(result.errorCode).toBe("k8s_concurrent_run_blocked");
    expect(result.errorMessage).toContain("still running for this agent");
  });

  it("ignores terminating jobs (deletionTimestamp set) and proceeds past the concurrency guard", async () => {
    // A job being force-deleted has deletionTimestamp set but no Complete/Failed condition.
    // The guard must treat it as terminal so subsequent runs are not blocked.
    const terminating: k8s.V1Job = {
      metadata: {
        name: "terminating-job",
        namespace: "paperclip",
        labels: { "paperclip.io/agent-id": "agent-abc", "paperclip.io/adapter-type": "claude_k8s" },
        deletionTimestamp: new Date(),
      },
      status: { conditions: [] },
    };
    mockBatchListJobs.mockResolvedValue({ items: [terminating] });
    // Guard passes → next failure is job creation (no further mocks set up)
    mockBatchCreateJob.mockRejectedValue(new Error("quota exceeded"));
    mockPrepareBundle.mockResolvedValue(makeBundle());
    const result = await execute(makeCtx());
    // Must NOT be a concurrency error — the guard let us through
    expect(result.errorCode).not.toBe("k8s_concurrent_run_blocked");
    expect(result.errorCode).toBe("k8s_job_create_failed");
  });

  it("blocks a matching active-run orphan instead of reattaching", async () => {
    const orphan = makeJob({
      runId: "prior-run",
      agentId: "agent-abc",
      taskId: "task-match",
      sessionId: "sess-match",
    });
    mockBatchListJobs.mockResolvedValue({ items: [orphan] });
    const result = await execute(
      makeCtx({
        context: { taskId: "task-match" },
        runtime: { sessionId: "sess-match", sessionParams: null, sessionDisplayId: null, taskKey: null },
      } as Partial<AdapterExecutionContext>),
    );

    expect(mockBatchPatchJob).not.toHaveBeenCalled();
    expect(result.errorCode).toBe("k8s_concurrent_run_blocked");
    expect(result.errorMessage).toContain("active run");
  });
});

// ─── execute: job creation paths ─────────────────────────────────────────────

describe("execute: job creation", () => {
  beforeEach(() => {
    vi.resetAllMocks();
    mockReadSkillEntries.mockResolvedValue([]);
    mockGetSelfPodInfo.mockResolvedValue(makeSelfPodResult());
    mockBatchListJobs.mockResolvedValue({ items: [] }); // no concurrent jobs
    mockPrepareBundle.mockResolvedValue(makeBundle());
    mockBatchCreateJob.mockResolvedValue({ metadata: { uid: "job-uid-1" } });
    mockBatchDeleteJob.mockResolvedValue({});
  });

  it("returns k8s_job_create_failed when createNamespacedJob throws", async () => {
    mockBatchCreateJob.mockRejectedValue(new Error("quota exceeded"));
    const result = await execute(makeCtx());
    expect(result.errorCode).toBe("k8s_job_create_failed");
    expect(result.errorMessage).toContain("quota exceeded");
  });

  it("acknowledges the created Job identity before continuing", async () => {
    const onExternalRuntimeLaunched = vi.fn().mockResolvedValue(undefined);

    await execute(makeCtx({ onExternalRuntimeLaunched } as Partial<AdapterExecutionContext>));

    expect(onExternalRuntimeLaunched).toHaveBeenCalledWith({
      jobName: expect.any(String),
      jobUid: "job-uid-1",
    });
  });

  it("reports an isolated start after the Job identity is acknowledged", async () => {
    process.env.PAPERCLIP_API_URL = "https://paperclip.test";
    const fetchMock = vi.fn().mockResolvedValue({ ok: true, status: 202 });
    vi.stubGlobal("fetch", fetchMock);
    mockCoreListPods.mockResolvedValue({ items: [] });
    const onExternalRuntimeLaunched = vi.fn().mockResolvedValue(undefined);

    try {
      await execute(makeCtx({
        authToken: "run-token",
        config: { podScheduleTimeoutSec: 0 },
        context: { taskId: "task-current" },
        runtime: {
          ...makeIsolatedRuntime("current-key"),
          sessionId: "session-current",
        },
        onExternalRuntimeLaunched,
      } as Partial<AdapterExecutionContext>));

      expect(onExternalRuntimeLaunched).toHaveBeenCalledBefore(fetchMock);
      const reportCall = fetchMock.mock.calls.find(([url, init]) =>
        String(url).endsWith("/api/metrics/claude-k8s/isolated-run-started")
        && (init as RequestInit | undefined)?.method === "POST"
      );
      expect(reportCall).toBeDefined();
      expect(JSON.parse((reportCall![1] as RequestInit).body as string)).toEqual({
        agentId: "agent-abc",
        companyId: "co1",
        isolationMode: "workspace",
        isolationKey: "current-key",
        taskKey: "task-current",
        sessionId: "session-current",
      });
    } finally {
      vi.unstubAllGlobals();
      delete process.env.PAPERCLIP_API_URL;
    }
  });

  it("does not fail an isolated run when telemetry logging and reporting fail", async () => {
    process.env.PAPERCLIP_API_URL = "https://paperclip.test";
    vi.stubGlobal("fetch", vi.fn().mockRejectedValue(new Error("metrics route unavailable")));
    mockCoreListPods.mockResolvedValue({ items: [] });
    const onExternalRuntimeLaunched = vi.fn().mockResolvedValue(undefined);
    const onLog = vi.fn().mockImplementation(async (_stream: string, message: string) => {
      if (message.includes("k8s_isolated_run_started")) throw new Error("log transport unavailable");
    });

    try {
      const result = await execute(makeCtx({
        authToken: "run-token",
        config: { podScheduleTimeoutSec: 0 },
        runtime: makeIsolatedRuntime("current-key"),
        onExternalRuntimeLaunched,
        onLog,
      } as Partial<AdapterExecutionContext>));

      expect(onExternalRuntimeLaunched).toHaveBeenCalledOnce();
      expect(result.errorCode).toBe("k8s_pod_schedule_failed");
      expect(result.errorMessage).not.toContain("metrics route unavailable");
      expect(result.errorMessage).not.toContain("log transport unavailable");
    } finally {
      vi.unstubAllGlobals();
      delete process.env.PAPERCLIP_API_URL;
    }
  });

  it("deletes the Job when launch identity cannot be acknowledged", async () => {
    const result = await execute(makeCtx({
      onExternalRuntimeLaunched: vi.fn().mockRejectedValue(new Error("reservation released")),
    } as Partial<AdapterExecutionContext>));

    expect(result.errorCode).toBe("k8s_job_identity_unacknowledged");
    expect(mockBatchDeleteJob).toHaveBeenCalled();
  });

  it("returns k8s_pod_schedule_failed when pod scheduling times out", async () => {
    mockBatchCreateJob.mockResolvedValue({ metadata: { uid: "uid-1" } });
    mockBatchDeleteJob.mockResolvedValue({});
    // Pod never appears → waitForPod eventually times out.
    // Provide a config with very short timeout to avoid a real 2-minute wait.
    // Instead, make listNamespacedPod return an unschedulable condition immediately.
    mockCoreListPods.mockResolvedValue({
      items: [
        {
          metadata: { name: "pod-xyz" },
          status: {
            phase: "Pending",
            conditions: [
              { type: "PodScheduled", status: "False", reason: "Unschedulable", message: "no nodes available" },
            ],
            containerStatuses: [],
            initContainerStatuses: [],
          },
        },
      ],
    });

    const result = await execute(makeCtx());

    expect(result.errorCode).toBe("k8s_pod_schedule_failed");
    expect(result.errorMessage).toContain("unschedulable");
  });
});

// ─── execute: waitForPod edge cases ──────────────────────────────────────────

describe("execute: waitForPod edge cases", () => {
  beforeEach(() => {
    vi.resetAllMocks();
    mockReadSkillEntries.mockResolvedValue([]);
    mockGetSelfPodInfo.mockResolvedValue(makeSelfPodResult());
    mockBatchListJobs.mockResolvedValue({ items: [] });
    mockPrepareBundle.mockResolvedValue(makeBundle());
    mockBatchCreateJob.mockResolvedValue({ metadata: { uid: "uid-1" } });
    mockBatchDeleteJob.mockResolvedValue({});
  });

  it("throws k8s_pod_schedule_failed when pod reaches phase=Failed immediately", async () => {
    mockCoreListPods.mockResolvedValue({
      items: [{
        metadata: { name: "pod-fail" },
        status: {
          phase: "Failed",
          containerStatuses: [{ name: "claude", state: { terminated: { exitCode: 137, reason: "OOMKilled" } } }],
          initContainerStatuses: [],
        },
      }],
    });

    const result = await execute(makeCtx());

    expect(result.errorCode).toBe("k8s_pod_schedule_failed");
    expect(result.errorMessage).toContain("OOMKilled");
  });

  it("uses the startup timeout after the pod is already scheduled", async () => {
    mockCoreListPods.mockResolvedValue({
      items: [{
        metadata: { name: "pod-starting" },
        spec: { nodeName: "k8s-paperclip-1" },
        status: {
          phase: "Pending",
          conditions: [{ type: "PodScheduled", status: "True" }],
          initContainerStatuses: [{
            name: "write-prompt",
            state: { terminated: { exitCode: 0, reason: "Completed" } },
          }],
          containerStatuses: [{
            name: "claude",
            state: { waiting: { reason: "PodInitializing" } },
          }],
        },
      }],
    });

    const ctx = makeCtx({ config: { podStartTimeoutSec: 0 } } as Partial<AdapterExecutionContext>);
    const result = await execute(ctx);

    expect(result.errorCode).toBe("k8s_pod_schedule_failed");
    expect(result.errorMessage).toContain("Pod startup failed");
    expect(result.errorMessage).toContain("containers to start");
    expect(result.errorMessage).not.toContain("Timed out waiting for pod to be scheduled");
    expect(ctx.onLog).toHaveBeenCalledWith(
      "stdout",
      expect.stringContaining("scheduled; waiting for containers to start"),
    );
  });

  it("throws k8s_pod_schedule_failed when init container exits non-zero", async () => {
    mockCoreListPods.mockResolvedValue({
      items: [{
        metadata: { name: "pod-x" },
        status: {
          phase: "Pending",
          initContainerStatuses: [{
            name: "write-prompt",
            state: { terminated: { exitCode: 1, reason: "Error" } },
          }],
          containerStatuses: [],
        },
      }],
    });

    const result = await execute(makeCtx());

    expect(result.errorCode).toBe("k8s_pod_schedule_failed");
    expect(result.errorMessage).toContain("write-prompt");
  });

  it("throws k8s_pod_schedule_failed when init container has ImagePullBackOff", async () => {
    mockCoreListPods.mockResolvedValue({
      items: [{
        metadata: { name: "pod-x" },
        status: {
          phase: "Pending",
          initContainerStatuses: [{
            name: "write-prompt",
            state: { waiting: { reason: "ImagePullBackOff", message: "pull failed" } },
          }],
          containerStatuses: [],
        },
      }],
    });

    const result = await execute(makeCtx());

    expect(result.errorCode).toBe("k8s_pod_schedule_failed");
    expect(result.errorMessage).toContain("image pull");
  });

  it("throws k8s_pod_schedule_failed when main container has CrashLoopBackOff", async () => {
    mockCoreListPods.mockResolvedValue({
      items: [{
        metadata: { name: "pod-x" },
        status: {
          phase: "Pending",
          initContainerStatuses: [],
          containerStatuses: [{
            name: "claude",
            state: { waiting: { reason: "CrashLoopBackOff" } },
          }],
        },
      }],
    });

    const result = await execute(makeCtx());

    expect(result.errorCode).toBe("k8s_pod_schedule_failed");
    expect(result.errorMessage).toContain("crash loop");
  });
});

// ─── execute: grace-period fallback (FAR-23) ─────────────────────────────────


// ─── execute: concurrency guard — multiple orphan sorting ────────────────────

describe("execute: concurrency guard — multiple orphans", () => {
  beforeEach(() => {
    vi.clearAllMocks();
    vi.unstubAllGlobals();
    process.env.PAPERCLIP_API_URL = "https://paperclip.test";
    vi.stubGlobal("fetch", vi.fn().mockResolvedValue({
      ok: true,
      status: 200,
      json: async () => ({ id: "prior-run", status: "running", startedAt: new Date().toISOString() }),
    }));
    mockGetSelfPodInfo.mockResolvedValue(makeSelfPodResult());
  });

  afterEach(() => {
    vi.unstubAllGlobals();
    delete process.env.PAPERCLIP_API_URL;
  });

  it("blocks when any run-table-active orphan remains after stale jobs are filtered", async () => {
    const orphanOld = makeJob({ runId: "prior-1", agentId: "agent-abc", taskId: "task-match" });
    orphanOld.metadata!.creationTimestamp = new Date("2024-01-01T00:00:00Z") as unknown as Date;
    const orphanNew = makeJob({ runId: "prior-2", agentId: "agent-abc", taskId: "task-other" });
    orphanNew.metadata!.creationTimestamp = new Date("2024-01-02T00:00:00Z") as unknown as Date;

    mockBatchListJobs.mockResolvedValue({ items: [orphanOld, orphanNew] });
    const result = await execute(
      makeCtx({ context: { taskId: "task-match" } } as Partial<AdapterExecutionContext>),
    );

    expect(result.errorCode).toBe("k8s_concurrent_run_blocked");
    expect(result.errorMessage).toContain("active run");
  });
});

// ─── shouldAbortForCancellation ──────────────────────────────────────────────

describe("shouldAbortForCancellation", () => {
  it("returns false for undefined", () => {
    expect(shouldAbortForCancellation(undefined)).toBe(false);
  });

  it("returns false for empty string", () => {
    expect(shouldAbortForCancellation("")).toBe(false);
  });

  it("returns false when status is 'running'", () => {
    expect(shouldAbortForCancellation("running")).toBe(false);
  });

  it("returns true when status is 'cancelled'", () => {
    expect(shouldAbortForCancellation("cancelled")).toBe(true);
  });

  it("returns true when status is 'cancelling'", () => {
    expect(shouldAbortForCancellation("cancelling")).toBe(true);
  });

  // FAR-107: terminal-but-not-cancelled statuses MUST NOT trigger Job deletion.
  // The previous "anything but running" guard caused k8s_job_deleted_externally
  // false positives for in-flight runs whenever the API briefly reported a
  // transient/stale status.
  it("returns false for non-cancellation terminal statuses (FAR-107)", () => {
    expect(shouldAbortForCancellation("succeeded")).toBe(false);
    expect(shouldAbortForCancellation("failed")).toBe(false);
    expect(shouldAbortForCancellation("completed")).toBe(false);
  });

  it("returns false for unknown statuses (FAR-107)", () => {
    expect(shouldAbortForCancellation("unknown")).toBe(false);
    expect(shouldAbortForCancellation("queued")).toBe(false);
    expect(shouldAbortForCancellation("pending")).toBe(false);
  });
});


// ─── execute: per-agent creation mutex (FAR-29 TOCTOU fix) ───────────────────
//
// Verifies that two concurrent execute() calls for the same agent cannot both
// enter the listNamespacedJob → createNamespacedJob sequence simultaneously.
// Without the per-agent mutex, both would pass the concurrency guard before
// either job appears in the other's list query.

describe("execute: per-agent creation mutex prevents TOCTOU race", () => {
  beforeEach(() => {
    vi.resetAllMocks();
    mockReadSkillEntries.mockResolvedValue([]);
    mockGetSelfPodInfo.mockResolvedValue(makeSelfPodResult());
    mockPrepareBundle.mockResolvedValue(makeBundle());
    // Make job creation fail so the guard+create phase exits quickly and
    // releases the mutex without needing to mock the full streaming path.
    mockBatchCreateJob.mockRejectedValue(new Error("mock: create not configured"));
    mockBatchDeleteJob.mockResolvedValue({});
    mockCoreDeleteSecret.mockResolvedValue({});
  });

  it("serializes guard phases for the same agent: call-2 waits until call-1 exits guard+create", async () => {
    const listCalls: string[] = [];
    let resolveFirstList!: (v: { items: [] }) => void;

    mockBatchListJobs
      .mockImplementationOnce(() => {
        listCalls.push("call-1");
        return new Promise<{ items: [] }>((resolve) => { resolveFirstList = resolve; });
      })
      .mockImplementation(() => {
        listCalls.push("call-2");
        return Promise.resolve({ items: [] });
      });

    const p1 = execute(makeCtx({ runId: "run-1" }));
    const p2 = execute(makeCtx({ runId: "run-2" }));

    // Drain microtasks: call-1 should be suspended in listNamespacedJob while
    // call-2 waits behind the per-agent mutex, not yet calling list.
    for (let i = 0; i < 20; i++) await Promise.resolve();
    expect(listCalls).toEqual(["call-1"]);

    // Let call-1's guard resolve (no running jobs). It will proceed to job
    // creation, fail (mock rejects), and release the mutex in finally.
    resolveFirstList({ items: [] });
    await Promise.allSettled([p1, p2]);

    // call-2 must have listed, and only AFTER call-1's guard resolved.
    // The exact order: call-1 listed → call-1 list resolved → call-2 listed.
    expect(listCalls).toEqual(["call-1", "call-2"]);
  });

  it("does not serialize guard phases for different agents", async () => {
    const listCalls: string[] = [];
    let resolveAgentAList!: (v: { items: [] }) => void;

    // Agent A's list is artificially slow. Agent B (different id) should
    // proceed immediately without waiting — the mutex is keyed by agent id.
    mockBatchListJobs
      .mockImplementationOnce(() => {
        listCalls.push("A");
        return new Promise<{ items: [] }>((resolve) => { resolveAgentAList = resolve; });
      })
      .mockImplementation(() => {
        listCalls.push("B");
        return Promise.resolve({ items: [] });
      });

    const ctxA = makeCtx({ runId: "run-A" });
    const ctxB = makeCtx({
      runId: "run-B",
      agent: { id: "agent-other", companyId: "co1", name: "Other Agent", adapterType: "claude_k8s", adapterConfig: {} },
    } as Partial<AdapterExecutionContext>);

    const pA = execute(ctxA);
    const pB = execute(ctxB);

    // Drain microtasks — B should have called list even though A is still
    // suspended, because they use separate mutex slots.
    for (let i = 0; i < 20; i++) await Promise.resolve();
    expect(listCalls).toContain("B");

    // Let A complete so the promises settle cleanly.
    resolveAgentAList({ items: [] });
    await Promise.allSettled([pA, pB]);
  });
});
