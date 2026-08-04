import { spawnSync } from "node:child_process";
import { mkdirSync, mkdtempSync, rmSync, writeFileSync } from "node:fs";
import { tmpdir } from "node:os";
import { join } from "node:path";
import { describe, it, expect, beforeEach, afterEach } from "vitest";
import type { AdapterExecutionContext } from "@paperclipai/adapter-utils";
import {
  buildJobManifest,
  buildPodLogPath,
  sanitizeLabelValue,
  isSensitiveEnvName,
  findLiteralSensitiveEnvVars,
  findLiteralSensitiveEnvVarsInPodSpec,
} from "./job-manifest.js";
import type { SelfPodInfo } from "./k8s-client.js";

function makeCtx(overrides: Partial<AdapterExecutionContext> = {}): AdapterExecutionContext {
  return {
    runId: "run-abc12345",
    agent: { id: "agent-abc", companyId: "co1", name: "Test Agent", adapterType: "claude_k8s", adapterConfig: {} },
    runtime: { sessionId: null, sessionParams: null, sessionDisplayId: null, taskKey: null },
    config: {},
    context: {},
    onLog: async () => {},
    ...overrides,
  };
}

function makeSelfPod(overrides: Partial<SelfPodInfo> = {}): SelfPodInfo {
  return {
    namespace: "paperclip",
    image: "paperclipai/paperclip:latest",
    imagePullSecrets: [{ name: "regcred" }],
    dnsConfig: undefined,
    nodeSelector: {},
    tolerations: [],
    pvcClaimName: "paperclip-data",
    secretVolumes: [],
    inheritedEnv: {},
    inheritedEnvValueFrom: [],
    inheritedEnvFrom: [],
    ...overrides,
  };
}

function setRuntimeIsolation(ctx: AdapterExecutionContext, isolation: Record<string, unknown>) {
  ctx.runtime = {
    ...ctx.runtime,
    isolation: isolation as NonNullable<AdapterExecutionContext["runtime"]["isolation"]>,
  };
}

function isolatedStorage(workspace: "ephemeral" | "persistent" = "persistent") {
  return {
    workspace,
    home: workspace,
    session: workspace,
    cache: "ephemeral",
  };
}

function encodeClaudeCwd(cwd: string): string {
  return cwd.replace(/[^a-zA-Z0-9-]/g, "-");
}

function createClaudeConfigDirWithSession(sessionId: string, workingDir = "/paperclip"): string {
  const configDir = mkdtempSync(join(tmpdir(), "claude-k8s-session-"));
  const projectDir = join(configDir, "projects", encodeClaudeCwd(workingDir));
  mkdirSync(projectDir, { recursive: true });
  writeFileSync(join(projectDir, `${sessionId}.jsonl`), "{}\n");
  return configDir;
}

describe("buildJobManifest", () => {
  let ctx: AdapterExecutionContext;
  let selfPod: SelfPodInfo;
  let tempDirs: string[];

  beforeEach(() => {
    ctx = makeCtx();
    selfPod = makeSelfPod();
    tempDirs = [];
    process.env.PAPERCLIP_SHARED_MCP_BASELINE_PATH = "";
  });

  afterEach(() => {
    delete process.env.PAPERCLIP_SHARED_MCP_BASELINE_PATH;
    for (const dir of tempDirs) {
      rmSync(dir, { recursive: true, force: true });
    }
  });

  describe("job naming", () => {
    it("uses ac- prefix", () => {
      const { jobName } = buildJobManifest({ ctx, selfPod });
      expect(jobName).toMatch(/^ac-/);
    });

    it("includes sanitized agent id slug (up to 16 chars)", () => {
      ctx.agent.id = "Agent-ABC!@#";
      const { jobName } = buildJobManifest({ ctx, selfPod });
      // sanitizeForK8sName: lowercase, strip non-alphanumeric (not dashes), slice 0-16
      expect(jobName).toContain("agent-abc");
    });

    it("includes sanitized run id slug (up to 16 chars)", () => {
      ctx.runId = "RUN-ABC-12345";
      const { jobName } = buildJobManifest({ ctx, selfPod });
      expect(jobName).toContain("run-abc-12345");
    });

    it("includes a deterministic hash suffix", () => {
      const result1 = buildJobManifest({ ctx, selfPod });
      const result2 = buildJobManifest({ ctx, selfPod });
      expect(result1.jobName).toBe(result2.jobName);
      // Hash suffix is 6 hex chars at the end
      expect(result1.jobName).toMatch(/-[0-9a-f]{6}$/);
    });

    it("different agent+run pairs produce different names", () => {
      const result1 = buildJobManifest({ ctx, selfPod });
      ctx.runId = "run-different";
      const result2 = buildJobManifest({ ctx, selfPod });
      expect(result1.jobName).not.toBe(result2.jobName);
    });

    it("stays within 63-char DNS label limit", () => {
      ctx.agent.id = "a".repeat(100);
      ctx.runId = "r".repeat(100);
      const { jobName } = buildJobManifest({ ctx, selfPod });
      expect(jobName.length).toBeLessThanOrEqual(63);
    });
  });

  describe("job spec", () => {
    it("sets backoffLimit to 0 for fail-fast", () => {
      const { job } = buildJobManifest({ ctx, selfPod });
      expect(job.spec?.backoffLimit).toBe(0);
    });

    it("sets activeDeadlineSeconds when timeoutSec > 0", () => {
      ctx.config = { timeoutSec: 300 };
      const { job } = buildJobManifest({ ctx, selfPod });
      expect(job.spec?.activeDeadlineSeconds).toBe(300);
    });

    it("omits activeDeadlineSeconds when timeoutSec is 0", () => {
      ctx.config = { timeoutSec: 0 };
      const { job } = buildJobManifest({ ctx, selfPod });
      expect(job.spec?.activeDeadlineSeconds).toBeUndefined();
    });

    it("sets ttlSecondsAfterFinished default 300", () => {
      const { job } = buildJobManifest({ ctx, selfPod });
      expect(job.spec?.ttlSecondsAfterFinished).toBe(300);
    });

    it("uses configured ttlSecondsAfterFinished", () => {
      ctx.config = { ttlSecondsAfterFinished: 600 };
      const { job } = buildJobManifest({ ctx, selfPod });
      expect(job.spec?.ttlSecondsAfterFinished).toBe(600);
    });
  });

  describe("labels", () => {
    it("includes required paperclip labels", () => {
      const { job } = buildJobManifest({ ctx, selfPod });
      const labels = job.metadata?.labels ?? {};
      expect(labels["app.kubernetes.io/managed-by"]).toBe("paperclip");
      expect(labels["app.kubernetes.io/component"]).toBe("agent-job");
      expect(labels["paperclip.io/agent-id"]).toBe("agent-abc");
      expect(labels["paperclip.io/run-id"]).toBe("run-abc12345");
      expect(labels["paperclip.io/company-id"]).toBe("co1");
      expect(labels["paperclip.io/adapter-type"]).toBe("claude_k8s");
    });

    it("includes extra labels from config", () => {
      ctx.config = { labels: { "env": "prod", "team": "platform" } };
      const { job } = buildJobManifest({ ctx, selfPod });
      expect(job.metadata?.labels?.env).toBe("prod");
      expect(job.metadata?.labels?.team).toBe("platform");
    });

    it("merges extra labels with required ones", () => {
      ctx.config = { labels: { "env": "prod" } };
      const { job } = buildJobManifest({ ctx, selfPod });
      expect(job.metadata?.labels?.env).toBe("prod");
      expect(job.metadata?.labels?.["paperclip.io/adapter-type"]).toBe("claude_k8s");
    });

    it("adds task-id label when context provides taskId", () => {
      ctx.context = { taskId: "task-xyz-789" };
      const { job } = buildJobManifest({ ctx, selfPod });
      expect(job.metadata?.labels?.["paperclip.io/task-id"]).toBe("task-xyz-789");
    });

    it("falls back to issueId when taskId absent", () => {
      ctx.context = { issueId: "issue-42" };
      const { job } = buildJobManifest({ ctx, selfPod });
      expect(job.metadata?.labels?.["paperclip.io/task-id"]).toBe("issue-42");
    });

    it("adds session-id label when runtime provides sessionId", () => {
      ctx.runtime = { ...ctx.runtime, sessionId: "sess-abc-1234" };
      const { job } = buildJobManifest({ ctx, selfPod });
      expect(job.metadata?.labels?.["paperclip.io/session-id"]).toBe("sess-abc-1234");
    });

    it("adds isolation labels in isolated mode", () => {
      ctx.config = { isolationMode: "isolated", isolationKey: "pr-review-123" };
      const { job } = buildJobManifest({ ctx, selfPod });
      expect(job.metadata?.labels?.["paperclip.io/isolation-mode"]).toBe("workspace");
      expect(job.metadata?.labels?.["paperclip.io/isolation-key"]).toBe("pr-review-123");
    });

    it("labels runtime isolation with its typed mode", () => {
      setRuntimeIsolation(ctx, {
        isolationMode: "run",
        isolationKey: "run:run-abc12345",
        workspaceRoot: "/runtime-cache/paperclip-runs/run-abc12345/workspace",
        homeRoot: "/runtime-cache/paperclip-runs/run-abc12345/home",
        sessionRoot: "/runtime-cache/paperclip-runs/run-abc12345/session",
        cacheRoot: "/runtime-cache/paperclip-runs/run-abc12345/cache",
        tmpRoot: "/runtime-cache/paperclip-runs/run-abc12345/tmp",
        storage: isolatedStorage("ephemeral"),
      });

      const { job } = buildJobManifest({ ctx, selfPod });
      expect(job.metadata?.labels?.["paperclip.io/isolation-mode"]).toBe("run");
      expect(job.metadata?.labels?.["paperclip.io/isolation-key"]).toBe("runrun-abc12345");
    });

    it("reads sessionId from runtime.sessionParams when sessionId prop missing", () => {
      ctx.runtime = { ...ctx.runtime, sessionParams: { sessionId: "sess-from-params" } };
      const { job } = buildJobManifest({ ctx, selfPod });
      expect(job.metadata?.labels?.["paperclip.io/session-id"]).toBe("sess-from-params");
    });

    it("omits task-id and session-id labels when neither is provided", () => {
      const { job } = buildJobManifest({ ctx, selfPod });
      expect(job.metadata?.labels?.["paperclip.io/task-id"]).toBeUndefined();
      expect(job.metadata?.labels?.["paperclip.io/session-id"]).toBeUndefined();
    });

    it("drops user label with paperclip.io/ prefix", () => {
      ctx.config = { labels: { "paperclip.io/run-id": "hijacked" } };
      const { job, skippedLabels } = buildJobManifest({ ctx, selfPod });
      expect(job.metadata?.labels?.["paperclip.io/run-id"]).not.toBe("hijacked");
      expect(skippedLabels).toContain("paperclip.io/run-id");
    });

    it("drops user label with app.kubernetes.io/ prefix", () => {
      ctx.config = { labels: { "app.kubernetes.io/managed-by": "attacker" } };
      const { job, skippedLabels } = buildJobManifest({ ctx, selfPod });
      expect(job.metadata?.labels?.["app.kubernetes.io/managed-by"]).toBe("paperclip");
      expect(skippedLabels).toContain("app.kubernetes.io/managed-by");
    });

    it("passes through user label without reserved prefix", () => {
      ctx.config = { labels: { "custom.io/team": "platform" } };
      const { job, skippedLabels } = buildJobManifest({ ctx, selfPod });
      expect(job.metadata?.labels?.["custom.io/team"]).toBe("platform");
      expect(skippedLabels).not.toContain("custom.io/team");
    });

    it("populates skippedLabels with all dropped keys", () => {
      ctx.config = {
        labels: {
          "paperclip.io/agent-id": "x",
          "app.kubernetes.io/component": "y",
          "safe": "z",
        },
      };
      const { skippedLabels } = buildJobManifest({ ctx, selfPod });
      expect(skippedLabels).toHaveLength(2);
      expect(skippedLabels).toContain("paperclip.io/agent-id");
      expect(skippedLabels).toContain("app.kubernetes.io/component");
    });
  });

  describe("system label sanitization (N4)", () => {
    it("sanitizes agent.id with @ to a valid RFC 1123 label", () => {
      ctx.agent.id = "user@example.com";
      const { job } = buildJobManifest({ ctx, selfPod });
      const label = job.metadata?.labels?.["paperclip.io/agent-id"];
      expect(label).toMatch(/^[a-zA-Z0-9]([a-zA-Z0-9._-]*[a-zA-Z0-9])?$/);
      expect(label).not.toContain("@");
    });

    it("sanitizes agent.id with spaces to a valid RFC 1123 label", () => {
      ctx.agent.id = "my agent id";
      const { job } = buildJobManifest({ ctx, selfPod });
      const label = job.metadata?.labels?.["paperclip.io/agent-id"];
      expect(label).toMatch(/^[a-zA-Z0-9]([a-zA-Z0-9._-]*[a-zA-Z0-9])?$/);
    });

    it("omits paperclip.io/run-id when sanitized value is null (all-invalid runId)", () => {
      // inject an all-special-chars runId via context override — buildJobManifest
      // uses ctx.runId directly. Use characters that are path-valid but label-invalid.
      const badCtx = makeCtx({ runId: "@@@" });
      expect(() => buildJobManifest({ ctx: badCtx, selfPod })).toThrow("Invalid runId");
    });

    it("selector matches sanitized agent-id label", () => {
      ctx.agent.id = "Agent@Test";
      const { job } = buildJobManifest({ ctx, selfPod });
      const agentLabel = job.metadata?.labels?.["paperclip.io/agent-id"];
      // the label should equal what sanitizeLabelValue produces
      expect(agentLabel).toBe("AgentTest");
    });
  });

  describe("annotations", () => {
    it("includes adapter type and agent name annotations", () => {
      const { job } = buildJobManifest({ ctx, selfPod });
      expect(job.metadata?.annotations?.["paperclip.io/adapter-type"]).toBe("claude_k8s");
      expect(job.metadata?.annotations?.["paperclip.io/agent-name"]).toBe("Test Agent");
    });
  });

  describe("pod spec", () => {
    it("sets restartPolicy to Never", () => {
      const { job } = buildJobManifest({ ctx, selfPod });
      expect(job.spec?.template?.spec?.restartPolicy).toBe("Never");
    });

    it("sets the non-root uid and primary gid without requesting volume ownership changes", () => {
      const { job } = buildJobManifest({ ctx, selfPod });
      const sc = job.spec?.template?.spec?.securityContext;
      expect(sc?.runAsNonRoot).toBe(true);
      expect(sc?.runAsUser).toBe(1000);
      expect(sc?.runAsGroup).toBe(1000);
      expect(sc?.fsGroup).toBeUndefined();
      expect(sc?.fsGroupChangePolicy).toBeUndefined();
    });

    it("includes imagePullSecrets from selfPod", () => {
      const { job } = buildJobManifest({ ctx, selfPod });
      expect(job.spec?.template?.spec?.imagePullSecrets).toEqual([{ name: "regcred" }]);
    });

    it("omits imagePullSecrets when empty", () => {
      selfPod.imagePullSecrets = [];
      const { job } = buildJobManifest({ ctx, selfPod });
      expect(job.spec?.template?.spec?.imagePullSecrets).toBeUndefined();
    });

    it("includes dnsConfig from selfPod when present", () => {
      selfPod.dnsConfig = { nameservers: ["8.8.8.8"], searches: ["svc.cluster.local"] };
      const { job } = buildJobManifest({ ctx, selfPod });
      expect(job.spec?.template?.spec?.dnsConfig).toEqual({ nameservers: ["8.8.8.8"], searches: ["svc.cluster.local"] });
    });

    it("omits dnsConfig when not present", () => {
      selfPod.dnsConfig = undefined;
      const { job } = buildJobManifest({ ctx, selfPod });
      expect(job.spec?.template?.spec?.dnsConfig).toBeUndefined();
    });
  });

  describe("init containers", () => {
    it("has write-prompt init container with busybox image", () => {
      const { job } = buildJobManifest({ ctx, selfPod });
      const init = job.spec?.template?.spec?.initContainers?.[0];
      expect(init?.name).toBe("write-prompt");
      expect(init?.image).toBe("busybox:1.36");
      expect(init?.imagePullPolicy).toBe("IfNotPresent");
    });

    it("write-prompt writes PROMPT_CONTENT to /tmp/prompt/prompt.txt", () => {
      const { job } = buildJobManifest({ ctx, selfPod });
      const init = job.spec?.template?.spec?.initContainers?.[0];
      expect(init?.command?.[0]).toBe("sh");
      expect(init?.command?.[1]).toBe("-c");
      expect(init?.command?.[2]).toContain("printf '%s' \"$PROMPT_CONTENT\" > /tmp/prompt/prompt.txt");
    });

    it("write-prompt redirects Chrome BrowserMetrics to ephemeral runtime-cache (BLO-10699)", () => {
      // The agent-browser designer tool launches Chrome with the default
      // /paperclip/.config/google-chrome profile; its BrowserMetrics *.pma
      // spool leaked 42GiB onto the shared CephFS HOME and walled the fleet
      // with EDQUOT. Only BrowserMetrics is redirected (profile auth stays
      // persistent), idempotently, to a per-pod path that dies with the pod.
      const { job } = buildJobManifest({ ctx, selfPod });
      const init = job.spec?.template?.spec?.initContainers?.[0];
      const cmd = init?.command?.[2] ?? "";
      expect(cmd).toContain("[ -L /paperclip/.config/google-chrome/BrowserMetrics ]");
      expect(cmd).toContain(
        "ln -sfn /runtime-cache/chrome-browser-metrics /paperclip/.config/google-chrome/BrowserMetrics",
      );
    });

    it("write-prompt mounts the runtime-cache emptyDir so the BrowserMetrics symlink target resolves", () => {
      const { job } = buildJobManifest({ ctx, selfPod });
      const init = job.spec?.template?.spec?.initContainers?.[0];
      expect(init?.volumeMounts).toContainEqual({ name: "runtime-cache", mountPath: "/runtime-cache" });
    });

    it("write-prompt mounts prompt volume", () => {
      const { job } = buildJobManifest({ ctx, selfPod });
      const init = job.spec?.template?.spec?.initContainers?.[0];
      expect(init?.volumeMounts).toContainEqual({ name: "prompt", mountPath: "/tmp/prompt" });
    });

    it("write-prompt mounts the data PVC at /paperclip so mkdir of run-logs succeeds as runAsUser:1000", () => {
      // Without this mount, the init container's `mkdir -p /paperclip/instances/...`
      // fails with EACCES because uid 1000 cannot write to the container image's
      // root filesystem. The data volume is the shared RWX PVC where run logs and
      // session state live.
      const { job } = buildJobManifest({ ctx, selfPod });
      const init = job.spec?.template?.spec?.initContainers?.[0];
      expect(init?.volumeMounts).toContainEqual({ name: "data", mountPath: "/paperclip" });
    });

    it("prompt env var contains rendered prompt text", () => {
      const { job, prompt } = buildJobManifest({ ctx, selfPod });
      const init = job.spec?.template?.spec?.initContainers?.[0];
      const promptEnv = init?.env?.find((e: { name: string }) => e.name === "PROMPT_CONTENT");
      expect(promptEnv?.value).toBe(prompt);
    });
  });

  describe("claude container", () => {
    it("names container 'claude'", () => {
      const { job } = buildJobManifest({ ctx, selfPod });
      expect(job.spec?.template?.spec?.containers[0]?.name).toBe("claude");
    });

    it("uses selfPod image by default", () => {
      const { job } = buildJobManifest({ ctx, selfPod });
      expect(job.spec?.template?.spec?.containers[0]?.image).toBe("paperclipai/paperclip:latest");
    });

    it("uses configured image override", () => {
      ctx.config = { image: "my-image:v2" };
      const { job } = buildJobManifest({ ctx, selfPod });
      expect(job.spec?.template?.spec?.containers[0]?.image).toBe("my-image:v2");
    });

    it("sets imagePullPolicy from config", () => {
      ctx.config = { imagePullPolicy: "Always" };
      const { job } = buildJobManifest({ ctx, selfPod });
      expect(job.spec?.template?.spec?.containers[0]?.imagePullPolicy).toBe("Always");
    });

    it("defaults imagePullPolicy to IfNotPresent", () => {
      const { job } = buildJobManifest({ ctx, selfPod });
      expect(job.spec?.template?.spec?.containers[0]?.imagePullPolicy).toBe("IfNotPresent");
    });

    it("sets workingDir to /paperclip by default", () => {
      const { job } = buildJobManifest({ ctx, selfPod });
      expect(job.spec?.template?.spec?.containers[0]?.workingDir).toBe("/paperclip");
    });

    it("uses workspace cwd when available", () => {
      ctx.context = { paperclipWorkspace: { cwd: "/workspace/myproject" } };
      const { job } = buildJobManifest({ ctx, selfPod });
      expect(job.spec?.template?.spec?.containers[0]?.workingDir).toBe("/workspace/myproject");
    });

    it("prefers workspace cwd over configured cwd", () => {
      ctx.config = { cwd: "/custom/path" };
      ctx.context = { paperclipWorkspace: { cwd: "/workspace/myproject" } };
      const { job } = buildJobManifest({ ctx, selfPod });
      expect(job.spec?.template?.spec?.containers[0]?.workingDir).toBe("/workspace/myproject");
    });
  });

  describe("volumes", () => {
    it("creates prompt emptyDir volume", () => {
      const { job } = buildJobManifest({ ctx, selfPod });
      const promptVol = job.spec?.template?.spec?.volumes?.find((v) => v.name === "prompt");
      expect(promptVol?.emptyDir).toEqual({});
    });

    it("mounts runtime cache emptyDir outside /paperclip", () => {
      const { job } = buildJobManifest({ ctx, selfPod });
      const cacheVol = job.spec?.template?.spec?.volumes?.find((v) => v.name === "runtime-cache");
      expect(cacheVol?.emptyDir?.sizeLimit).toBe("20Gi");
      const cacheMount = job.spec?.template?.spec?.containers[0]?.volumeMounts?.find((vm) => vm.name === "runtime-cache");
      expect(cacheMount?.mountPath).toBe("/runtime-cache");
    });

    it("mounts data PVC at /paperclip when pvcClaimName is set", () => {
      const { job } = buildJobManifest({ ctx, selfPod });
      const dataVol = job.spec?.template?.spec?.volumes?.find((v) => v.name === "data");
      expect(dataVol?.persistentVolumeClaim?.claimName).toBe("paperclip-data");
      const dataMount = job.spec?.template?.spec?.containers[0]?.volumeMounts?.find((vm) => vm.mountPath === "/paperclip");
      expect(dataMount?.name).toBe("data");
      const securityContext = job.spec?.template?.spec?.securityContext;
      expect(securityContext?.fsGroup).toBeUndefined();
      expect(securityContext?.fsGroupChangePolicy).toBeUndefined();
    });

    it("omits data volume when no PVC", () => {
      selfPod.pvcClaimName = null;
      const { job } = buildJobManifest({ ctx, selfPod });
      expect(job.spec?.template?.spec?.volumes?.find((v) => v.name === "data")).toBeUndefined();
    });

    it("mounts secret volumes", () => {
      selfPod.secretVolumes = [{
        volumeName: "my-secret",
        secretName: "app-secret",
        mountPath: "/secrets/app",
        defaultMode: 420,
      }];
      const { job } = buildJobManifest({ ctx, selfPod });
      const secretVol = job.spec?.template?.spec?.volumes?.find((v) => v.name === "my-secret");
      expect(secretVol?.secret?.secretName).toBe("app-secret");
      const secretMount = job.spec?.template?.spec?.containers[0]?.volumeMounts?.find((vm) => vm.mountPath === "/secrets/app");
      expect(secretMount?.readOnly).toBe(true);
    });
  });

  describe("environment variables", () => {
    it("sets HOME to /paperclip", () => {
      const { job } = buildJobManifest({ ctx, selfPod });
      const home = job.spec?.template?.spec?.containers[0]?.env?.find((e) => e.name === "HOME");
      expect(home?.value).toBe("/paperclip");
    });

    it("scopes HOME, Claude config, caches, cwd, and logs to the isolation key", () => {
      ctx.config = { isolationMode: "isolated", isolationKey: "pr-review-123" };
      const { job, podLogPath, envSecret } = buildJobManifest({ ctx, selfPod });
      const container = job.spec?.template?.spec?.containers[0];
      const env = new Map(container?.env?.map((e) => [e.name, e.value]));
      const command = container?.command?.join(" ") ?? "";
      const root = "/paperclip/instances/default/data/k8s-isolation/co1/agent-abc/pr-review-123";
      expect(container?.workingDir).toBe(`${root}/workspace`);
      expect(env.get("HOME")).toBe(`${root}/home`);
      expect(env.get("CLAUDE_CONFIG_DIR")).toBe(`${root}/home/.claude`);
      expect(env.get("XDG_CACHE_HOME")).toBe(`${root}/cache/xdg`);
      expect(env.get("TMPDIR")).toBe(`${root}/tmp`);
      expect(env.get("TMP")).toBe(`${root}/tmp`);
      expect(env.get("TEMP")).toBe(`${root}/tmp`);
      // PAPERCLIP_K8S_ISOLATION_KEY matches the sensitive-name pattern (contains
      // "KEY") even though it's a path-scoping identifier, not a credential —
      // an accepted false positive (BLO-17980): it still routes through
      // secretKeyRef instead of a literal value.
      const isolationKeyEntry = container?.env?.find((e) => e.name === "PAPERCLIP_K8S_ISOLATION_KEY");
      expect(isolationKeyEntry?.value).toBeUndefined();
      expect(isolationKeyEntry?.valueFrom?.secretKeyRef?.name).toBe(envSecret?.name);
      expect(envSecret?.data.PAPERCLIP_K8S_ISOLATION_KEY).toBe("pr-review-123");
      expect(podLogPath).toBe("/paperclip/instances/default/data/run-logs/co1/agent-abc/isolated/pr-review-123/run-abc12345.pod.ndjson");
      expect(command).toContain(
        "mkdir -p '/paperclip/instances/default/data/run-logs/co1/agent-abc/isolated/pr-review-123'",
      );
    });

    it("prefers run-scoped runtime roots over manual config and clones an independent workspace", () => {
      ctx.config = {
        isolationMode: "isolated",
        isolationKey: "config-key",
        workspaceRoot: "/paperclip/config-workspace",
      };
      ctx.context = { paperclipWorkspace: { cwd: "/paperclip/source-worktree" } };
      setRuntimeIsolation(ctx, {
        isolationMode: "run",
        isolationKey: "run:run-abc12345",
        workspaceRoot: "/runtime-cache/paperclip-runs/run-abc12345/workspace",
        homeRoot: "/runtime-cache/paperclip-runs/run-abc12345/home",
        sessionRoot: "/runtime-cache/paperclip-runs/run-abc12345/session",
        cacheRoot: "/runtime-cache/paperclip-runs/run-abc12345/cache",
        tmpRoot: "/runtime-cache/paperclip-runs/run-abc12345/tmp",
        storage: isolatedStorage("ephemeral"),
      });

      const { job } = buildJobManifest({ ctx, selfPod });
      const container = job.spec?.template?.spec?.containers[0];
      const env = new Map(container?.env?.map((entry) => [entry.name, entry.value]));
      const command = container?.command?.join(" ") ?? "";
      expect(container?.workingDir).toBe("/runtime-cache/paperclip-runs/run-abc12345");
      expect(env.get("HOME")).toBe("/runtime-cache/paperclip-runs/run-abc12345/home");
      expect(env.get("CLAUDE_CONFIG_DIR")).toBe("/runtime-cache/paperclip-runs/run-abc12345/session/.claude");
      expect(env.get("XDG_CACHE_HOME")).toBe("/runtime-cache/paperclip-runs/run-abc12345/cache/xdg");
      expect(env.get("TMPDIR")).toBe("/runtime-cache/paperclip-runs/run-abc12345/tmp");
      expect(env.get("TMP")).toBe("/runtime-cache/paperclip-runs/run-abc12345/tmp");
      expect(env.get("TEMP")).toBe("/runtime-cache/paperclip-runs/run-abc12345/tmp");
      expect(env.get("PAPERCLIP_WORKSPACE_CWD")).toBe("/runtime-cache/paperclip-runs/run-abc12345/workspace");
      expect(command).toContain("if git -C '/paperclip/source-worktree' rev-parse --verify HEAD");
      expect(command).toContain("git clone --shared --no-checkout -- '/paperclip/source-worktree' '/runtime-cache/paperclip-runs/run-abc12345/workspace'");
      expect(command).toContain("checkout --detach \"$source_head\"");
      expect(command).toContain("else rm -rf '/runtime-cache/paperclip-runs/run-abc12345/workspace' && mkdir -p '/runtime-cache/paperclip-runs/run-abc12345/workspace'");
      expect(command).toContain("fi && cd '/runtime-cache/paperclip-runs/run-abc12345/workspace' || exit $?");
      const syntaxCheck = spawnSync("/bin/sh", ["-n", "-c", command], { encoding: "utf8" });
      expect(syntaxCheck.stderr).toBe("");
      expect(syntaxCheck.status).toBe(0);
      expect(command).not.toContain("/paperclip/config-workspace");
    });

    it("gives two concurrent stateless runs distinct, non-colliding TMPDIR/TMP/TEMP values", () => {
      const buildForRun = (runId: string) => {
        const runCtx = makeCtx({ runId });
        setRuntimeIsolation(runCtx, {
          isolationMode: "run",
          isolationKey: `run:${runId}`,
          workspaceRoot: `/runtime-cache/paperclip-runs/${runId}/workspace`,
          homeRoot: `/runtime-cache/paperclip-runs/${runId}/home`,
          sessionRoot: `/runtime-cache/paperclip-runs/${runId}/session`,
          cacheRoot: `/runtime-cache/paperclip-runs/${runId}/cache`,
          tmpRoot: `/runtime-cache/paperclip-runs/${runId}/tmp`,
          storage: isolatedStorage("ephemeral"),
        });
        const { job } = buildJobManifest({ ctx: runCtx, selfPod });
        const env = new Map(job.spec?.template?.spec?.containers[0]?.env?.map((e) => [e.name, e.value]));
        return { TMPDIR: env.get("TMPDIR"), TMP: env.get("TMP"), TEMP: env.get("TEMP") };
      };

      const first = buildForRun("run-11111111");
      const second = buildForRun("run-22222222");

      expect(first.TMPDIR).toBe("/runtime-cache/paperclip-runs/run-11111111/tmp");
      expect(second.TMPDIR).toBe("/runtime-cache/paperclip-runs/run-22222222/tmp");
      expect(first.TMPDIR).not.toBe(second.TMPDIR);
      expect(first.TMP).toBe(first.TMPDIR);
      expect(first.TEMP).toBe(first.TMPDIR);
      expect(second.TMP).toBe(second.TMPDIR);
      expect(second.TEMP).toBe(second.TMPDIR);
    });

    it("keeps durable workspace sessions persistent while caches remain ephemeral", () => {
      ctx.context = { paperclipWorkspace: { cwd: "/paperclip/workspaces/workspace-1" } };
      setRuntimeIsolation(ctx, {
        isolationMode: "workspace",
        isolationKey: "workspace:workspace-1",
        workspaceRoot: "/paperclip/workspaces/workspace-1",
        homeRoot: "/paperclip/k8s-isolation/workspace-1/home",
        sessionRoot: "/paperclip/k8s-isolation/workspace-1/session",
        cacheRoot: "/runtime-cache/paperclip-workspaces/workspace-1/cache",
        tmpRoot: "/runtime-cache/paperclip-workspaces/workspace-1/tmp",
        storage: isolatedStorage(),
      });

      const { job } = buildJobManifest({ ctx, selfPod });
      const container = job.spec?.template?.spec?.containers[0];
      const env = new Map(container?.env?.map((entry) => [entry.name, entry.value]));
      expect(container?.workingDir).toBe("/paperclip/workspaces/workspace-1");
      expect(env.get("HOME")).toBe("/paperclip/k8s-isolation/workspace-1/home");
      expect(env.get("CLAUDE_CONFIG_DIR")).toBe("/paperclip/k8s-isolation/workspace-1/session/.claude");
      expect(env.get("TMPDIR")).toBe("/runtime-cache/paperclip-workspaces/workspace-1/tmp");
      expect(env.get("XDG_CACHE_HOME")).toBe("/runtime-cache/paperclip-workspaces/workspace-1/cache/xdg");
      expect(container?.command?.join(" ")).not.toContain("git clone --shared");
    });

    it("lets a runtime shared descriptor override legacy isolated config", () => {
      ctx.config = { isolationMode: "isolated", isolationKey: "config-key" };
      ctx.context = { paperclipWorkspace: { cwd: "/paperclip/shared-workspace" } };
      setRuntimeIsolation(ctx, {
        isolationMode: "shared",
        isolationKey: "agent-shared:agent-abc",
      });

      const { job } = buildJobManifest({ ctx, selfPod });
      const container = job.spec?.template?.spec?.containers[0];
      const env = new Map(container?.env?.map((entry) => [entry.name, entry.value]));
      expect(container?.workingDir).toBe("/paperclip/shared-workspace");
      expect(env.get("HOME")).toBe("/paperclip");
      expect(env.get("TMPDIR")).toBeUndefined();
      expect(job.metadata?.labels?.["paperclip.io/isolation-mode"]).toBe("shared");
      expect(job.metadata?.labels?.["paperclip.io/isolation-key"]).toBe("agent-sharedagent-abc");
    });

    it("defaults build and package caches to runtime-cache emptyDir", () => {
      const { job } = buildJobManifest({ ctx, selfPod });
      const env = new Map(job.spec?.template?.spec?.containers[0]?.env?.map((e) => [e.name, e.value]));
      expect(env.get("XDG_CACHE_HOME")).toBe("/runtime-cache/xdg");
      expect(env.get("GOCACHE")).toBe("/runtime-cache/go-build");
      expect(env.get("GOMODCACHE")).toBe("/runtime-cache/gomod");
      expect(env.get("npm_config_cache")).toBe("/runtime-cache/npm");
      expect(env.get("BUN_INSTALL_CACHE")).toBe("/runtime-cache/bun");
      expect(env.get("PIP_CACHE_DIR")).toBe("/runtime-cache/pip");
      expect(env.get("PLAYWRIGHT_BROWSERS_PATH")).toBe("/runtime-cache/ms-playwright");
    });

    it("overrides inherited cache paths with the job-local runtime-cache mount", () => {
      selfPod.inheritedEnv = { XDG_CACHE_HOME: "/paperclip/.cache", GOCACHE: "/paperclip/.cache/go-build" };
      const { job } = buildJobManifest({ ctx, selfPod });
      const env = new Map(job.spec?.template?.spec?.containers[0]?.env?.map((e) => [e.name, e.value]));
      expect(env.get("XDG_CACHE_HOME")).toBe("/runtime-cache/xdg");
      expect(env.get("GOCACHE")).toBe("/runtime-cache/go-build");
    });

    it("preserves explicit adapter cache env overrides", () => {
      ctx.config = { env: { XDG_CACHE_HOME: "/custom-cache", GOCACHE: "/custom-go-cache" } };
      const { job } = buildJobManifest({ ctx, selfPod });
      const env = new Map(job.spec?.template?.spec?.containers[0]?.env?.map((e) => [e.name, e.value]));
      expect(env.get("XDG_CACHE_HOME")).toBe("/custom-cache");
      expect(env.get("GOCACHE")).toBe("/custom-go-cache");
    });

    it("inherits env vars from selfPod, routing the credential-shaped one through a Secret", () => {
      selfPod.inheritedEnv = { ANTHROPIC_API_KEY: "sk-abc", AWS_REGION: "us-east-1" };
      const { job, envSecret } = buildJobManifest({ ctx, selfPod });
      const env = job.spec?.template?.spec?.containers[0]?.env ?? [];
      const envNames = env.map((e) => e.name);
      expect(envNames).toContain("ANTHROPIC_API_KEY");
      expect(envNames).toContain("AWS_REGION");
      const apiKeyEntry = env.find((e) => e.name === "ANTHROPIC_API_KEY");
      expect(apiKeyEntry?.value).toBeUndefined();
      expect(apiKeyEntry?.valueFrom?.secretKeyRef?.name).toBe(envSecret?.name);
      expect(envSecret?.data.ANTHROPIC_API_KEY).toBe("sk-abc");
      const regionEntry = env.find((e) => e.name === "AWS_REGION");
      expect(regionEntry?.value).toBe("us-east-1");
    });

    it("inherits ANTHROPIC_AUTH_TOKEN from selfPod for API auth via secretKeyRef, not a literal value", () => {
      selfPod.inheritedEnv = { ANTHROPIC_AUTH_TOKEN: "sk-test" };
      const { job, envSecret } = buildJobManifest({ ctx, selfPod });
      const authEntry = job.spec?.template?.spec?.containers[0]?.env?.find((e) => e.name === "ANTHROPIC_AUTH_TOKEN");
      expect(authEntry?.value).toBeUndefined();
      expect(authEntry?.valueFrom?.secretKeyRef?.name).toBe(envSecret?.name);
      expect(envSecret?.data.ANTHROPIC_AUTH_TOKEN).toBe("sk-test");
    });


    it("user env config overrides inherited env", () => {
      selfPod.inheritedEnv = { AWS_REGION: "us-east-1" };
      ctx.config = { env: { AWS_REGION: "us-west-2" } };
      const { job } = buildJobManifest({ ctx, selfPod });
      const awsRegion = job.spec?.template?.spec?.containers[0]?.env?.find((e) => e.name === "AWS_REGION");
      expect(awsRegion?.value).toBe("us-west-2");
    });

    it("sets PAPERCLIP_RUN_ID", () => {
      const { job } = buildJobManifest({ ctx, selfPod });
      const runId = job.spec?.template?.spec?.containers[0]?.env?.find((e) => e.name === "PAPERCLIP_RUN_ID");
      expect(runId?.value).toBe("run-abc12345");
    });

    it("routes PAPERCLIP_API_KEY (from authToken) through a Secret instead of a literal value (BLO-17980)", () => {
      ctx.authToken = "pk_abc123";
      const { job, envSecret } = buildJobManifest({ ctx, selfPod });
      const apiKey = job.spec?.template?.spec?.containers[0]?.env?.find((e) => e.name === "PAPERCLIP_API_KEY");
      expect(apiKey?.value).toBeUndefined();
      expect(apiKey?.valueFrom?.secretKeyRef?.key).toBe("PAPERCLIP_API_KEY");
      expect(apiKey?.valueFrom?.secretKeyRef?.name).toBe(envSecret?.name);
      expect(envSecret?.data.PAPERCLIP_API_KEY).toBe("pk_abc123");
    });

    it("inherited PAPERCLIP_API_URL from selfPod takes precedence", () => {
      ctx.authToken = "pk_abc";
      selfPod.inheritedEnv = { PAPERCLIP_API_URL: "http://paperclip:8080" };
      const { job } = buildJobManifest({ ctx, selfPod });
      const apiUrl = job.spec?.template?.spec?.containers[0]?.env?.find((e) => e.name === "PAPERCLIP_API_URL");
      expect(apiUrl?.value).toBe("http://paperclip:8080");
    });

    it("includes valueFrom env vars from selfPod", () => {
      selfPod.inheritedEnvValueFrom = [
        { name: "ANTHROPIC_API_KEY", valueFrom: { secretKeyRef: { name: "api-keys", key: "anthropic" } } },
      ];
      const { job } = buildJobManifest({ ctx, selfPod });
      const envList = job.spec?.template?.spec?.containers[0]?.env ?? [];
      const apiKeyEntry = envList.find((e) => e.name === "ANTHROPIC_API_KEY");
      expect(apiKeyEntry?.valueFrom?.secretKeyRef?.name).toBe("api-keys");
      expect(apiKeyEntry?.valueFrom?.secretKeyRef?.key).toBe("anthropic");
      expect(apiKeyEntry?.value).toBeUndefined();
    });

    it("stamps x-penstock-session: agent:<name> into ANTHROPIC_CUSTOM_HEADERS", () => {
      const { job } = buildJobManifest({ ctx, selfPod });
      const envList = job.spec?.template?.spec?.containers[0]?.env ?? [];
      const headers = envList.find((e) => e.name === "ANTHROPIC_CUSTOM_HEADERS");
      expect(headers?.value).toContain("x-penstock-session: agent:");
    });

    it("appends the session header to an existing ANTHROPIC_CUSTOM_HEADERS and respects a manual override", () => {
      const withExisting = {
        ...ctx,
        config: { ...(ctx.config as Record<string, unknown>), env: { ANTHROPIC_CUSTOM_HEADERS: "X-Custom: 1" } },
      };
      const r1 = buildJobManifest({ ctx: withExisting, selfPod });
      const h1 = (r1.job.spec?.template?.spec?.containers[0]?.env ?? []).find(
        (e) => e.name === "ANTHROPIC_CUSTOM_HEADERS",
      );
      expect(h1?.value).toContain("X-Custom: 1");
      expect(h1?.value).toContain("x-penstock-session: agent:");

      const withOverride = {
        ...ctx,
        config: {
          ...(ctx.config as Record<string, unknown>),
          env: { ANTHROPIC_CUSTOM_HEADERS: "x-penstock-session: manual-pin" },
        },
      };
      const r2 = buildJobManifest({ ctx: withOverride, selfPod });
      const h2 = (r2.job.spec?.template?.spec?.containers[0]?.env ?? []).find(
        (e) => e.name === "ANTHROPIC_CUSTOM_HEADERS",
      );
      expect(h2?.value).toBe("x-penstock-session: manual-pin");
    });

    it("literal env overrides valueFrom with the same name", () => {
      selfPod.inheritedEnv = { MY_VAR: "literal-value" };
      selfPod.inheritedEnvValueFrom = [
        { name: "MY_VAR", valueFrom: { secretKeyRef: { name: "sec", key: "k" } } },
      ];
      const { job } = buildJobManifest({ ctx, selfPod });
      const envList = job.spec?.template?.spec?.containers[0]?.env ?? [];
      const myVar = envList.filter((e) => e.name === "MY_VAR");
      expect(myVar).toHaveLength(1);
      expect(myVar[0]?.value).toBe("literal-value");
      expect(myVar[0]?.valueFrom).toBeUndefined();
    });

    it("includes envFrom sources from selfPod on the container", () => {
      selfPod.inheritedEnvFrom = [
        { secretRef: { name: "api-secrets" } },
        { configMapRef: { name: "app-config" } },
      ];
      const { job } = buildJobManifest({ ctx, selfPod });
      const container = job.spec?.template?.spec?.containers[0];
      expect(container?.envFrom).toHaveLength(2);
      expect(container?.envFrom?.[0]?.secretRef?.name).toBe("api-secrets");
      expect(container?.envFrom?.[1]?.configMapRef?.name).toBe("app-config");
    });

    it("omits envFrom when selfPod has none", () => {
      selfPod.inheritedEnvFrom = [];
      const { job } = buildJobManifest({ ctx, selfPod });
      const container = job.spec?.template?.spec?.containers[0];
      expect(container?.envFrom).toBeUndefined();
    });
  });

  describe("resources", () => {
    it("sets default resource requests and limits", () => {
      const { job } = buildJobManifest({ ctx, selfPod });
      const resources = job.spec?.template?.spec?.containers[0]?.resources;
      expect(resources?.requests).toEqual({ cpu: "1000m", memory: "2Gi" });
      expect(resources?.limits).toEqual({ cpu: "4000m", memory: "8Gi" });
    });

    it("uses configured resource overrides", () => {
      ctx.config = {
        "resources.requests.cpu": "500m",
        "resources.requests.memory": "1Gi",
        "resources.limits.cpu": "2000m",
        "resources.limits.memory": "4Gi",
      };
      const { job } = buildJobManifest({ ctx, selfPod });
      const resources = job.spec?.template?.spec?.containers[0]?.resources;
      expect(resources?.requests).toEqual({ cpu: "500m", memory: "1Gi" });
      expect(resources?.limits).toEqual({ cpu: "2000m", memory: "4Gi" });
    });
  });

  describe("nodeSelector and tolerations", () => {
    it("applies nodeSelector from config", () => {
      ctx.config = { nodeSelector: { "topology.kubernetes.io/zone": "us-east-1a" } };
      const { job } = buildJobManifest({ ctx, selfPod });
      expect(job.spec?.template?.spec?.nodeSelector).toEqual({ "topology.kubernetes.io/zone": "us-east-1a" });
    });

    it("applies tolerations from config", () => {
      ctx.config = { tolerations: [{ key: "disk", operator: "Equal", value: "ssd", effect: "NoSchedule" }] };
      const { job } = buildJobManifest({ ctx, selfPod });
      expect(job.spec?.template?.spec?.tolerations).toHaveLength(1);
    });

    it("omits nodeSelector when empty", () => {
      const { job } = buildJobManifest({ ctx, selfPod });
      expect(job.spec?.template?.spec?.nodeSelector).toBeUndefined();
    });

    it("omits tolerations when empty", () => {
      const { job } = buildJobManifest({ ctx, selfPod });
      expect(job.spec?.template?.spec?.tolerations).toBeUndefined();
    });

    it("inherits nodeSelector from the paperclip pod by default", () => {
      selfPod = makeSelfPod({ nodeSelector: { workload: "paperclip" } });
      const { job } = buildJobManifest({ ctx, selfPod });
      expect(job.spec?.template?.spec?.nodeSelector).toEqual({ workload: "paperclip" });
    });

    it("inherits tolerations from the paperclip pod by default", () => {
      const inherited = [{ key: "dedicated", operator: "Equal", value: "paperclip", effect: "NoSchedule" }];
      selfPod = makeSelfPod({ tolerations: inherited });
      const { job } = buildJobManifest({ ctx, selfPod });
      expect(job.spec?.template?.spec?.tolerations).toEqual(inherited);
    });

    it("allows explicit empty scheduling config to opt out of inherited scheduling", () => {
      selfPod = makeSelfPod({
        nodeSelector: { workload: "paperclip" },
        tolerations: [{ key: "dedicated", operator: "Equal", value: "paperclip", effect: "NoSchedule" }],
      });
      ctx.config = { nodeSelector: "", tolerations: [] };
      const { job } = buildJobManifest({ ctx, selfPod });
      expect(job.spec?.template?.spec?.nodeSelector).toBeUndefined();
      expect(job.spec?.template?.spec?.tolerations).toBeUndefined();
    });
  });

  describe("claude args", () => {
    it("builds --print - - --output-format stream-json --verbose", () => {
      const { claudeArgs } = buildJobManifest({ ctx, selfPod });
      expect(claudeArgs).toContain("--print");
      expect(claudeArgs).toContain("-");
      expect(claudeArgs).toContain("--output-format");
      expect(claudeArgs).toContain("stream-json");
      expect(claudeArgs).toContain("--verbose");
    });

    it("adds --model when configured", () => {
      ctx.config = { model: "claude-opus-4-6" };
      const { claudeArgs } = buildJobManifest({ ctx, selfPod });
      expect(claudeArgs).toContain("--model");
      expect(claudeArgs).toContain("claude-opus-4-6");
    });

    it("adds --effort when configured", () => {
      ctx.config = { effort: "high" };
      const { claudeArgs } = buildJobManifest({ ctx, selfPod });
      expect(claudeArgs).toContain("--effort");
      expect(claudeArgs).toContain("high");
    });

    it("adds --max-turns when configured", () => {
      ctx.config = { maxTurnsPerRun: 10 };
      const { claudeArgs } = buildJobManifest({ ctx, selfPod });
      expect(claudeArgs).toContain("--max-turns");
      expect(claudeArgs).toContain("10");
    });

    it("adds --resume when matching Claude session file exists", () => {
      const configDir = createClaudeConfigDirWithSession("sess_abc");
      tempDirs.push(configDir);
      ctx.config = { env: { CLAUDE_CONFIG_DIR: configDir } };
      ctx.runtime.sessionId = "sess_abc";
      const { claudeArgs } = buildJobManifest({ ctx, selfPod });
      expect(claudeArgs).toContain("--resume");
      expect(claudeArgs).toContain("sess_abc");
    });

    it("adds --resume when configured model matches session model", () => {
      const configDir = createClaudeConfigDirWithSession("sess_abc");
      tempDirs.push(configDir);
      ctx.config = {
        model: "claude-sonnet-4-6[1m]",
        env: { CLAUDE_CONFIG_DIR: configDir },
      };
      ctx.runtime.sessionParams = {
        sessionId: "sess_abc",
        model: "claude-sonnet-4-6[1m]",
      };
      const { claudeArgs } = buildJobManifest({ ctx, selfPod });
      expect(claudeArgs).toContain("--resume");
      expect(claudeArgs).toContain("sess_abc");
    });

    it("starts a fresh Claude session when configured model differs from session model", () => {
      const configDir = createClaudeConfigDirWithSession("sess_abc");
      tempDirs.push(configDir);
      ctx.config = {
        model: "claude-sonnet-4-6[1m]",
        instructionsFilePath: "/paperclip/instructions.md",
        env: { CLAUDE_CONFIG_DIR: configDir },
      };
      ctx.runtime.sessionParams = {
        sessionId: "sess_abc",
        model: "claude-sonnet-4-5",
      };
      const { claudeArgs } = buildJobManifest({ ctx, selfPod });
      expect(claudeArgs).not.toContain("--resume");
      expect(claudeArgs).not.toContain("sess_abc");
      expect(claudeArgs).toContain("--append-system-prompt-file");
    });

    it("starts a fresh Claude session when configured model has no recorded session model", () => {
      const configDir = createClaudeConfigDirWithSession("sess_abc");
      tempDirs.push(configDir);
      ctx.config = {
        model: "claude-sonnet-4-6[1m]",
        instructionsFilePath: "/paperclip/instructions.md",
        env: { CLAUDE_CONFIG_DIR: configDir },
      };
      ctx.runtime.sessionParams = { sessionId: "sess_abc" };
      const { claudeArgs } = buildJobManifest({ ctx, selfPod });
      expect(claudeArgs).not.toContain("--resume");
      expect(claudeArgs).not.toContain("sess_abc");
      expect(claudeArgs).toContain("--append-system-prompt-file");
    });

    it("starts a fresh Claude session when runtime sessionId has no local Claude session file", () => {
      const configDir = mkdtempSync(join(tmpdir(), "claude-k8s-session-missing-"));
      tempDirs.push(configDir);
      ctx.config = {
        instructionsFilePath: "/paperclip/instructions.md",
        env: { CLAUDE_CONFIG_DIR: configDir },
      };
      ctx.runtime.sessionId = "a24fcff7-99a3-43ad-b0d0-1e145827369c";
      const { claudeArgs } = buildJobManifest({ ctx, selfPod });
      expect(claudeArgs).not.toContain("--resume");
      expect(claudeArgs).not.toContain("a24fcff7-99a3-43ad-b0d0-1e145827369c");
      expect(claudeArgs).toContain("--append-system-prompt-file");
      expect(claudeArgs).toContain("/paperclip/instructions.md");
    });

    it("adds --dangerously-skip-permissions by default", () => {
      const { claudeArgs } = buildJobManifest({ ctx, selfPod });
      expect(claudeArgs).toContain("--dangerously-skip-permissions");
    });

    it("adds --append-system-prompt-file (config fallback) when instructionsFilePath set and no session", () => {
      ctx.config = { instructionsFilePath: "/paperclip/instructions.md" };
      const { claudeArgs } = buildJobManifest({ ctx, selfPod });
      expect(claudeArgs).toContain("--append-system-prompt-file");
      expect(claudeArgs).toContain("/paperclip/instructions.md");
    });

    it("omits --append-system-prompt-file on session resume (avoids token waste)", () => {
      const configDir = createClaudeConfigDirWithSession("sess_existing");
      tempDirs.push(configDir);
      ctx.config = {
        instructionsFilePath: "/paperclip/instructions.md",
        env: { CLAUDE_CONFIG_DIR: configDir },
      };
      ctx.runtime.sessionId = "sess_existing";
      const { claudeArgs } = buildJobManifest({ ctx, selfPod });
      expect(claudeArgs).not.toContain("--append-system-prompt-file");
    });

    it("adds --add-dir when promptBundle is provided", () => {
      const promptBundle = {
        bundleKey: "abc123",
        rootDir: "/paperclip/instances/default/companies/co1/claude-prompt-cache/abc123",
        addDir: "/paperclip/instances/default/companies/co1/claude-prompt-cache/abc123",
        instructionsFilePath: null,
      };
      const { claudeArgs } = buildJobManifest({ ctx, selfPod, promptBundle });
      expect(claudeArgs).toContain("--add-dir");
      expect(claudeArgs).toContain(promptBundle.addDir);
    });

    it("uses bundle instructionsFilePath for --append-system-prompt-file when promptBundle provided", () => {
      const promptBundle = {
        bundleKey: "abc123",
        rootDir: "/paperclip/instances/default/companies/co1/claude-prompt-cache/abc123",
        addDir: "/paperclip/instances/default/companies/co1/claude-prompt-cache/abc123",
        instructionsFilePath: "/paperclip/instances/default/companies/co1/claude-prompt-cache/abc123/agent-instructions.md",
      };
      ctx.config = { instructionsFilePath: "/raw/path/AGENTS.md" };
      const { claudeArgs } = buildJobManifest({ ctx, selfPod, promptBundle });
      expect(claudeArgs).toContain("--append-system-prompt-file");
      const idx = claudeArgs.indexOf("--append-system-prompt-file");
      expect(claudeArgs[idx + 1]).toBe(promptBundle.instructionsFilePath);
      expect(claudeArgs).not.toContain("/raw/path/AGENTS.md");
    });

    it("omits --append-system-prompt-file from bundle on session resume", () => {
      const configDir = createClaudeConfigDirWithSession("sess_existing");
      tempDirs.push(configDir);
      const promptBundle = {
        bundleKey: "abc123",
        rootDir: "/paperclip/instances/default/companies/co1/claude-prompt-cache/abc123",
        addDir: "/paperclip/instances/default/companies/co1/claude-prompt-cache/abc123",
        instructionsFilePath: "/paperclip/instances/default/companies/co1/claude-prompt-cache/abc123/agent-instructions.md",
      };
      ctx.config = { env: { CLAUDE_CONFIG_DIR: configDir } };
      ctx.runtime.sessionId = "sess_existing";
      const { claudeArgs } = buildJobManifest({ ctx, selfPod, promptBundle });
      expect(claudeArgs).not.toContain("--append-system-prompt-file");
      // --add-dir must still be present even on resume
      expect(claudeArgs).toContain("--add-dir");
    });

    it("omits --add-dir when no promptBundle", () => {
      const { claudeArgs } = buildJobManifest({ ctx, selfPod });
      expect(claudeArgs).not.toContain("--add-dir");
    });

    it("appends extraArgs when configured", () => {
      ctx.config = { extraArgs: ["--no-input", "--verbose"] };
      const { claudeArgs } = buildJobManifest({ ctx, selfPod });
      expect(claudeArgs).toContain("--no-input");
      expect(claudeArgs).toContain("--verbose");
    });
  });

  describe("prompt rendering", () => {
    it("includes agent name in default prompt template", () => {
      const { prompt } = buildJobManifest({ ctx, selfPod });
      expect(prompt).toContain("Test Agent");
    });

    it("uses custom promptTemplate when set", () => {
      ctx.config = { promptTemplate: "You are a helpful assistant." };
      const { prompt } = buildJobManifest({ ctx, selfPod });
      expect(prompt).toBe("You are a helpful assistant.");
    });

    it("includes workspace context in prompt when available", () => {
      ctx.context = {
        paperclipWorkspace: {
          cwd: "/project",
          strategy: "read-only",
          workspaceId: "ws1",
          repoUrl: "https://github.com/org/repo",
          branchName: "main",
        },
      };
      const { prompt } = buildJobManifest({ ctx, selfPod });
      expect(prompt).toContain("Test Agent");
    });

    it("returns promptMetrics with char counts", () => {
      const { promptMetrics } = buildJobManifest({ ctx, selfPod });
      expect(promptMetrics.promptChars).toBeGreaterThan(0);
      expect(typeof promptMetrics.promptChars).toBe("number");
    });
  });

  describe("serviceAccountName", () => {
    it("sets custom serviceAccountName when configured", () => {
      ctx.config = { serviceAccountName: "paperclip-agent" };
      const { job } = buildJobManifest({ ctx, selfPod });
      expect(job.spec?.template?.spec?.serviceAccountName).toBe("paperclip-agent");
    });

    it("omits serviceAccountName when not configured", () => {
      const { job } = buildJobManifest({ ctx, selfPod });
      expect(job.spec?.template?.spec?.serviceAccountName).toBeUndefined();
    });
  });

  describe("namespace", () => {
    it("uses selfPod namespace by default", () => {
      const { namespace } = buildJobManifest({ ctx, selfPod });
      expect(namespace).toBe("paperclip");
    });

    it("uses configured namespace override", () => {
      ctx.config = { namespace: "agents" };
      const { namespace, job } = buildJobManifest({ ctx, selfPod });
      expect(namespace).toBe("agents");
      expect(job.metadata?.namespace).toBe("agents");
    });
  });

  describe("return value", () => {
    it("returns job, jobName, namespace, prompt, claudeArgs, promptMetrics, promptSecret", () => {
      const result = buildJobManifest({ ctx, selfPod });
      expect(result.job).toBeDefined();
      expect(result.jobName).toBeDefined();
      expect(result.namespace).toBeDefined();
      expect(result.prompt).toBeDefined();
      expect(result.claudeArgs).toBeDefined();
      expect(result.promptMetrics).toBeDefined();
      expect(result.promptSecret).toBeNull();
    });
  });

  describe("nodeSelector key=value parsing", () => {
    it("parses key=value multiline text", () => {
      ctx.config = { nodeSelector: "disktype=ssd\ntopology.kubernetes.io/zone=us-east-1a" };
      const { job } = buildJobManifest({ ctx, selfPod });
      expect(job.spec?.template?.spec?.nodeSelector).toEqual({
        disktype: "ssd",
        "topology.kubernetes.io/zone": "us-east-1a",
      });
    });

    it("still accepts JSON objects", () => {
      ctx.config = { nodeSelector: { disktype: "ssd" } };
      const { job } = buildJobManifest({ ctx, selfPod });
      expect(job.spec?.template?.spec?.nodeSelector).toEqual({ disktype: "ssd" });
    });

    it("parses JSON string format", () => {
      ctx.config = { nodeSelector: '{"disktype":"ssd"}' };
      const { job } = buildJobManifest({ ctx, selfPod });
      expect(job.spec?.template?.spec?.nodeSelector).toEqual({ disktype: "ssd" });
    });

    it("skips comment lines and blank lines", () => {
      ctx.config = { nodeSelector: "# comment\n\ndisktype=ssd\n" };
      const { job } = buildJobManifest({ ctx, selfPod });
      expect(job.spec?.template?.spec?.nodeSelector).toEqual({ disktype: "ssd" });
    });
  });

  describe("labels key=value parsing", () => {
    it("parses key=value multiline text for extra labels", () => {
      ctx.config = { labels: "env=prod\nteam=platform" };
      const { job } = buildJobManifest({ ctx, selfPod });
      expect(job.metadata?.labels?.env).toBe("prod");
      expect(job.metadata?.labels?.team).toBe("platform");
    });
  });

  describe("large prompt Secret fallback", () => {
    it("returns null promptSecret for small prompts", () => {
      const { promptSecret } = buildJobManifest({ ctx, selfPod });
      expect(promptSecret).toBeNull();
    });

    it("returns promptSecret for prompts >256 KiB", () => {
      // Build a prompt >256 KiB via a custom template
      const largePrompt = "x".repeat(300 * 1024);
      ctx.config = { promptTemplate: largePrompt };
      const { promptSecret, job } = buildJobManifest({ ctx, selfPod });
      expect(promptSecret).not.toBeNull();
      expect(promptSecret!.data["prompt.txt"]).toBe(largePrompt);
      // Init container should copy from secret volume, not use PROMPT_CONTENT env
      const init = job.spec?.template?.spec?.initContainers?.[0];
      expect(init?.command).toContainEqual(expect.stringContaining("cp"));
      expect(init?.env).toBeUndefined();
      // Should have prompt-secret volume
      const secretVol = job.spec?.template?.spec?.volumes?.find((v) => v.name === "prompt-secret");
      expect(secretVol?.secret?.secretName).toBe(promptSecret!.name);
    });

    it("uses env var init container for small prompts", () => {
      const { job } = buildJobManifest({ ctx, selfPod });
      const init = job.spec?.template?.spec?.initContainers?.[0];
      expect(init?.env?.[0]?.name).toBe("PROMPT_CONTENT");
    });
  });

  describe("pod log file tailing", () => {
    it("adds ccrotate preflight but does not add rtk when enableRtk is false (default)", () => {
      const { job } = buildJobManifest({ ctx, selfPod });
      const cmd = job.spec?.template?.spec?.containers[0]?.command;
      // Command should refresh Claude auth via `next` only (no pre-snap;
      // claude-code's Stop hook handles end-of-session snap and pre-snap
      // raced with another concurrent Job's `next` mid-write — see
      // ccrotateRefresh comment). Then `cat ... | claude ... | tee ... |
      // <fail-fast awk> > /dev/null` so a terminal rate-limit event
      // unwinds the pipeline non-zero (RCA 2026-05-06). The PEN-1305 env-guard
      // setup is installed first (after `set -o pipefail`, before the ccrotate
      // preflight) so the PreToolUse hook is in place before Claude launches.
      const command = cmd?.[2] ?? "";
      expect(command).toMatch(/^set -o pipefail;/);
      expect(command).toMatch(/\(command -v ccrotate .*ccrotate next --yes --target claude.*\) \|\| true/);
      expect(command).toMatch(/cat \/tmp\/prompt\/prompt\.txt \| claude .* \| tee .* \| awk .* > \/dev\/null$/);
      expect(command.indexOf("paperclip-env-guard.mjs")).toBeLessThan(command.indexOf("ccrotate next"));
      expect(command.indexOf("ccrotate next")).toBeLessThan(command.indexOf("mkdir -p '/paperclip/instances/default/data/run-logs"));
      expect(command.indexOf("mkdir -p '/paperclip/instances/default/data/run-logs")).toBeLessThan(command.indexOf("cat /tmp/prompt/prompt.txt"));
      expect(command).not.toContain("ccrotate snap");
      expect(command).not.toContain("rtk-filter");
    });

    it("includes fail-fast awk for `out_of_credits` overage rejection (RCA 2026-05-06)", () => {
      const { job } = buildJobManifest({ ctx, selfPod });
      const cmd = job.spec?.template?.spec?.containers[0]?.command?.[2] ?? "";
      // Both substring matches must be present in the awk pattern so
      // we exit only on the specific terminal combination, not on
      // every `rate_limit_event` (most of which are informational
      // "allowed" status events with overage available).
      expect(cmd).toContain('"overageStatus":"rejected"');
      expect(cmd).toContain('"overageDisabledReason":"out_of_credits"');
      expect(cmd).toContain("[wrapper] terminal rate-limit");
      expect(cmd).toContain("exit 1");
      // Ordering matters — awk must run after `tee` so the trigger
      // event is persisted to the pod log before pipefail unwinds.
      expect(cmd.indexOf("tee ")).toBeLessThan(cmd.indexOf("awk "));
    });

    it("appends --accounts <csv> to ccrotate next when providers.anthropic.accounts is populated", () => {
      ctx.config = {
        providers: {
          anthropic: {
            accounts: ["a@b.net", "c@d.net"],
          },
        },
      };
      const { job } = buildJobManifest({ ctx, selfPod });
      const cmd = job.spec?.template?.spec?.containers[0]?.command?.[2] ?? "";
      expect(cmd).toContain("ccrotate next --yes --target claude --accounts a@b.net,c@d.net");
    });

    it("does not add --accounts when providers is undefined (global rotation path)", () => {
      const { job } = buildJobManifest({ ctx, selfPod });
      const cmd = job.spec?.template?.spec?.containers[0]?.command?.[2] ?? "";
      expect(cmd).toContain("ccrotate next --yes --target claude");
      expect(cmd).not.toContain("--accounts");
    });

    it("does not add --accounts when providers has only openai (wrong key for claude)", () => {
      ctx.config = {
        providers: {
          openai: {
            accounts: ["x@y.net"],
          },
        },
      };
      const { job } = buildJobManifest({ ctx, selfPod });
      const cmd = job.spec?.template?.spec?.containers[0]?.command?.[2] ?? "";
      expect(cmd).toContain("ccrotate next --yes --target claude");
      expect(cmd).not.toContain("--accounts");
    });

    it("command includes tee to pod log path", () => {
      const { job } = buildJobManifest({ ctx, selfPod });
      const cmd = job.spec?.template?.spec?.containers[0]?.command?.[2] ?? "";
      expect(cmd).toContain("| tee");
      expect(cmd).toContain("/paperclip/instances/default/data/run-logs/");
    });

    it("podLogPath is returned from buildJobManifest", () => {
      const result = buildJobManifest({ ctx, selfPod });
      expect(result.podLogPath).toBe(
        "/paperclip/instances/default/data/run-logs/co1/agent-abc/run-abc12345.pod.ndjson",
      );
    });

    it("buildPodLogPath returns correctly formatted path", () => {
      expect(buildPodLogPath("co1", "agent-abc", "run-abc12345")).toBe(
        "/paperclip/instances/default/data/run-logs/co1/agent-abc/run-abc12345.pod.ndjson",
      );
    });

    it("main container creates the pod log directory before tee", () => {
      const { job } = buildJobManifest({ ctx, selfPod });
      const command = job.spec?.template?.spec?.containers[0]?.command?.[2] ?? "";
      const mkdir = "mkdir -p '/paperclip/instances/default/data/run-logs/co1/agent-abc'";
      const tee = "tee '/paperclip/instances/default/data/run-logs/co1/agent-abc/run-abc12345.pod.ndjson'";
      expect(command).toContain(mkdir);
      expect(command).toContain(tee);
      expect(command.indexOf(mkdir)).toBeLessThan(command.indexOf(tee));
    });

    it("sanitizes companyId with / to valid path component for log path", () => {
      const badCtx = {
        ...ctx,
        agent: { ...ctx.agent, companyId: "co/1" },
      };
      const { podLogPath } = buildJobManifest({ ctx: badCtx as typeof ctx, selfPod });
      // / is stripped by sanitizeForK8sPath
      expect(podLogPath).toContain("co1/");
    });

    it("sanitizes agentId with @ to valid path component for log path", () => {
      const badCtx = {
        ...ctx,
        agent: { ...ctx.agent, id: "agent@123" },
      };
      const { podLogPath } = buildJobManifest({ ctx: badCtx as typeof ctx, selfPod });
      // @ is stripped by sanitizeForK8sPath
      expect(podLogPath).toContain("/agent123/");
    });

    it("sanitizes runId with underscore to valid path component for log path", () => {
      const badCtx = {
        ...ctx,
        runId: "run_123",
      };
      const { podLogPath } = buildJobManifest({ ctx: badCtx as typeof ctx, selfPod });
      // _ is stripped by sanitizeForK8sPath
      expect(podLogPath).toContain("/run123.pod.ndjson");
    });
  });
});

describe("sanitizeLabelValue", () => {
  it("passes through already-valid UUIDs and slugs", () => {
    expect(sanitizeLabelValue("abc-123-def")).toBe("abc-123-def");
    expect(sanitizeLabelValue("0d8b4472-c42c-4052-aab1-e32897909afa")).toBe("0d8b4472-c42c-4052-aab1-e32897909afa");
  });

  it("strips characters outside [a-zA-Z0-9._-]", () => {
    expect(sanitizeLabelValue("task:xyz/123")).toBe("taskxyz123");
    expect(sanitizeLabelValue("abc 123")).toBe("abc123");
  });

  it("trims leading/trailing non-alphanumeric characters", () => {
    expect(sanitizeLabelValue("--abc--")).toBe("abc");
    expect(sanitizeLabelValue("...123...")).toBe("123");
  });

  it("truncates to the configured maxLen", () => {
    const long = "a".repeat(200);
    const out = sanitizeLabelValue(long, 63);
    expect(out?.length).toBe(63);
  });

  it("returns null when no alphanumeric characters remain", () => {
    expect(sanitizeLabelValue("---")).toBeNull();
    expect(sanitizeLabelValue("")).toBeNull();
    expect(sanitizeLabelValue("   ")).toBeNull();
  });
});

describe("per-agent mcp.json layering", () => {
  let ctx: AdapterExecutionContext;
  let selfPod: SelfPodInfo;

  beforeEach(() => {
    ctx = makeCtx();
    selfPod = makeSelfPod();
    process.env.PAPERCLIP_SHARED_MCP_BASELINE_PATH = "";
  });

  it("does not inject --mcp-config when adapterConfig.mcpServers is empty", () => {
    const { claudeArgs, job, mcpConfigSecret } = buildJobManifest({ ctx, selfPod });
    expect(claudeArgs).not.toContain("--mcp-config");
    expect(claudeArgs).not.toContain("--strict-mcp-config");
    const init = job.spec!.template.spec!.initContainers![0];
    const initEnvNames = (init.env ?? []).map((e) => e.name);
    expect(initEnvNames).not.toContain("MCP_CONFIG");
    expect(mcpConfigSecret).toBeNull();
  });

  it("ships the shared baseline even when adapterConfig.mcpServers is empty, via a Secret-backed volume rather than a literal env var (BLO-17980)", () => {
    const dir = mkdtempSync(join(tmpdir(), "claude-k8s-mcp-"));
    const baselinePath = join(dir, ".mcp.json");
    try {
      writeFileSync(
        baselinePath,
        JSON.stringify({
          mcpServers: {
            paperclip: {
              command: "node",
              args: ["/app/packages/mcp-server/dist/stdio.js"],
            },
          },
        }),
      );
      process.env.PAPERCLIP_SHARED_MCP_BASELINE_PATH = baselinePath;

      const { claudeArgs, job, mcpConfigSecret } = buildJobManifest({ ctx, selfPod });
      expect(claudeArgs).toContain("--mcp-config");
      expect(claudeArgs).toContain("/tmp/prompt/mcp.json");
      expect(claudeArgs).toContain("--strict-mcp-config");

      const init = job.spec!.template.spec!.initContainers![0];
      const initEnvNames = (init.env ?? []).map((e) => e.name);
      expect(initEnvNames).not.toContain("MCP_CONFIG");

      expect(mcpConfigSecret).not.toBeNull();
      const parsed = JSON.parse(mcpConfigSecret!.data["mcp.json"]) as {
        mcpServers: Record<string, { command?: string; args?: string[] }>;
      };
      expect(parsed.mcpServers.paperclip).toEqual({
        command: "node",
        args: ["/app/packages/mcp-server/dist/stdio.js"],
      });

      // The Secret is mounted read-only and copied into the shared prompt emptyDir
      const volumes = job.spec!.template.spec!.volumes ?? [];
      const secretVolume = volumes.find((v) => v.name === "mcp-config-secret");
      expect(secretVolume?.secret?.secretName).toBe(mcpConfigSecret!.name);
      const initMount = (init.volumeMounts ?? []).find((m) => m.name === "mcp-config-secret");
      expect(initMount?.mountPath).toBe("/tmp/mcp-secret");
      expect(initMount?.readOnly).toBe(true);
      const initCmd = (init.command ?? []).join(" ");
      expect(initCmd).toContain("cp /tmp/mcp-secret/mcp.json /tmp/prompt/mcp.json");
    } finally {
      rmSync(dir, { recursive: true, force: true });
    }
  });

  it("merges per-agent overrides on top of the shared baseline and ships --mcp-config + --strict-mcp-config, staging mcp.json as a Secret instead of a literal env var (BLO-17980)", () => {
    ctx = makeCtx({
      config: {
        mcpServers: {
          kubernetes: {
            type: "sse",
            url: "http://kubernetes-mcp-server-admin.paperclip.svc.cluster.local:8080/sse",
          },
          figma: {
            type: "http",
            url: "http://figma-mcp-server.paperclip.svc.cluster.local:8080/mcp",
          },
        },
      },
    });
    const { claudeArgs, job, mcpConfigSecret } = buildJobManifest({ ctx, selfPod });
    expect(claudeArgs).toContain("--mcp-config");
    expect(claudeArgs).toContain("/tmp/prompt/mcp.json");
    expect(claudeArgs).toContain("--strict-mcp-config");

    const init = job.spec!.template.spec!.initContainers![0];
    const initEnvNames = (init.env ?? []).map((e) => e.name);
    expect(initEnvNames).not.toContain("MCP_CONFIG");

    expect(mcpConfigSecret).not.toBeNull();
    const parsed = JSON.parse(mcpConfigSecret!.data["mcp.json"]) as {
      mcpServers: Record<string, { type?: string; url?: string }>;
    };
    // Per-agent overrides land verbatim
    expect(parsed.mcpServers.kubernetes).toEqual({
      type: "sse",
      url: "http://kubernetes-mcp-server-admin.paperclip.svc.cluster.local:8080/sse",
    });
    expect(parsed.mcpServers.figma).toEqual({
      type: "http",
      url: "http://figma-mcp-server.paperclip.svc.cluster.local:8080/mcp",
    });
    // The init shell command copies the file from the mounted Secret volume, never printf's a literal value
    const initCmd = (init.command ?? []).join(" ");
    expect(initCmd).toContain("cp /tmp/mcp-secret/mcp.json /tmp/prompt/mcp.json");
    expect(initCmd).not.toContain("MCP_CONFIG");
  });

  it("never leaks an mcpServers Authorization header into any literal env value on any container (BLO-17980/BLO-17973 regression)", () => {
    ctx = makeCtx({
      config: {
        mcpServers: {
          gbrain: {
            url: "http://gbrain-mcp-admin.paperclip.svc.cluster.local:3130/mcp",
            type: "http",
            headers: { Authorization: "Bearer gbrain_at_test-token-should-never-leak" },
          },
        },
      },
    });
    const { job, mcpConfigSecret } = buildJobManifest({ ctx, selfPod });
    const allContainers = [
      ...(job.spec!.template.spec!.initContainers ?? []),
      ...job.spec!.template.spec!.containers,
    ];
    for (const c of allContainers) {
      for (const e of c.env ?? []) {
        expect(e.value ?? "").not.toContain("gbrain_at_test-token-should-never-leak");
      }
    }
    // The header only lives in the Secret payload, never inline in the Job spec.
    expect(mcpConfigSecret!.data["mcp.json"]).toContain("gbrain_at_test-token-should-never-leak");
  });
});

describe("paperclipTaskMarkdown surfacing", () => {
  let tempDirs: string[];

  beforeEach(() => {
    tempDirs = [];
  });

  afterEach(() => {
    for (const dir of tempDirs) {
      rmSync(dir, { recursive: true, force: true });
    }
  });

  // Server-side heartbeat composes context.paperclipTaskMarkdown for wakes
  // that carry first-class task context (notably PR-review wakes via the
  // github webhook handler, which set contextSnapshot.githubPrNumber +
  // githubRepoFullName but never produce a paperclipWake because there's
  // no issue tied to the PR). Without this prompt slot, the PR review
  // agent reaches the pod with NO information about which PR to review.
  //
  // See:
  //   - server/services/heartbeat.ts buildPaperclipTaskMarkdown
  //   - server/routes/github-webhook.ts (the wake call that sets
  //     contextSnapshot.githubPrNumber + reviewKind)
  it("includes context.paperclipTaskMarkdown in the assembled prompt", () => {
    const taskMd = [
      "Paperclip task context:",
      "- PR: \"Blockcast/paperclip#59\"",
      "- Wake reason: \"github_pr_opened\"",
      "",
      "GitHub PR review directive:",
      "A GitHub webhook woke you to review this pull request.",
    ].join("\n");
    const ctx = makeCtx({ context: { paperclipTaskMarkdown: taskMd } });
    const result = buildJobManifest({ ctx, selfPod: makeSelfPod() });
    expect(result.prompt).toContain("Blockcast/paperclip#59");
    expect(result.prompt).toContain("github_pr_opened");
    expect(result.prompt).toContain("GitHub PR review directive");
    expect(result.promptMetrics.taskMarkdownChars).toBe(taskMd.length);
  });

  it("does NOT inject anything when paperclipTaskMarkdown is absent (no spurious newlines)", () => {
    const result = buildJobManifest({ ctx: makeCtx(), selfPod: makeSelfPod() });
    expect(result.promptMetrics.taskMarkdownChars).toBe(0);
  });

  it("trims surrounding whitespace from paperclipTaskMarkdown before inclusion", () => {
    const taskMd = "\n\n  GitHub PR review directive:\n  ...\n\n";
    const ctx = makeCtx({ context: { paperclipTaskMarkdown: taskMd } });
    const result = buildJobManifest({ ctx, selfPod: makeSelfPod() });
    expect(result.promptMetrics.taskMarkdownChars).toBe(taskMd.trim().length);
    expect(result.prompt).toContain("GitHub PR review directive");
  });

  // The whole point of inserting `taskMarkdown` at a *specific* position
  // is so the agent reads task context (what to work on) after wake
  // context (why it woke) but before the session handoff narrative
  // (which may reference the task). A position-blind .toContain check
  // would silently accept a reorder; this test pins the contract.
  it("places taskMarkdown after wakePrompt and before sessionHandoffNote", () => {
    // claude-k8s has a local minimal renderPaperclipWakePrompt that only
    // emits "Wake reason: <reason>" lines (no issue identifier), so the
    // wake sentinel must live in the `reason` field rather than
    // `issue.identifier`.
    const ctx = makeCtx({
      context: {
        paperclipWake: {
          reason: "WAKE_SENTINEL_REASON",
          issue: { id: "x", identifier: "BLO-X", title: "t" },
        },
        paperclipTaskMarkdown: "TASK-SENTINEL paperclipTaskMarkdown body",
        paperclipSessionHandoffMarkdown: "HANDOFF-SENTINEL paperclipSessionHandoffMarkdown body",
      },
    });
    const result = buildJobManifest({ ctx, selfPod: makeSelfPod() });
    const wakeIdx = result.prompt.indexOf("WAKE_SENTINEL_REASON");
    const taskIdx = result.prompt.indexOf("TASK-SENTINEL");
    const handoffIdx = result.prompt.indexOf("HANDOFF-SENTINEL");
    expect(wakeIdx).toBeGreaterThan(-1);
    expect(taskIdx).toBeGreaterThan(wakeIdx);
    expect(handoffIdx).toBeGreaterThan(taskIdx);
  });

  // PR-review wakes overwhelmingly arrive WITH a resumed session: the
  // reviewer agent keeps a long-running claude session across wakes. The
  // resume-delta gate `Boolean(runtimeSessionId) && wakePrompt.length > 0`
  // evaluates to `false` for that shape (paperclipWake is null when
  // there's no issue tied to the PR), so `renderedPrompt` is NOT
  // suppressed — the agent gets the full bootstrap + the PR directive.
  // This test pins that behavior so a future refactor (e.g. gating
  // resume-delta on `taskMarkdown.length > 0`) doesn't silently land.
  it("does not gate resume-delta on taskMarkdown (PR-review wake shape: resumed session + no paperclipWake)", () => {
    const configDir = createClaudeConfigDirWithSession("ses_pr_review");
    tempDirs.push(configDir);
    const ctx = makeCtx({
      config: { env: { CLAUDE_CONFIG_DIR: configDir } },
      runtime: {
        sessionId: "ses_pr_review",
        sessionParams: { sessionId: "ses_pr_review" },
        sessionDisplayId: "ses_pr_review",
        taskKey: null,
      },
      context: {
        paperclipTaskMarkdown: "GitHub PR review directive: review PR #59",
      },
    });
    const result = buildJobManifest({ ctx, selfPod: makeSelfPod() });
    expect(result.prompt).toContain("GitHub PR review directive");
    expect(result.promptMetrics.taskMarkdownChars).toBeGreaterThan(0);
    // wakePrompt is empty (no paperclipWake) → resume-delta gate is OFF
    // → heartbeat prompt template still renders.
    expect(result.promptMetrics.wakePromptChars).toBe(0);
    expect(result.promptMetrics.heartbeatPromptChars).toBeGreaterThan(0);
  });

  // The complementary shape: issue-wake with both paperclipWake AND
  // paperclipTaskMarkdown set, on a resumed session. Resume-delta DOES
  // engage (wakePrompt > 0), so `renderedPrompt` IS suppressed — but
  // taskMarkdown must survive the suppression.
  it("preserves taskMarkdown even when resume-delta suppresses the heartbeat prompt", () => {
    const configDir = createClaudeConfigDirWithSession("ses_issue_wake");
    tempDirs.push(configDir);
    const ctx = makeCtx({
      config: { env: { CLAUDE_CONFIG_DIR: configDir } },
      runtime: {
        sessionId: "ses_issue_wake",
        sessionParams: { sessionId: "ses_issue_wake" },
        sessionDisplayId: "ses_issue_wake",
        taskKey: null,
      },
      context: {
        paperclipWake: {
          reason: "issue_assigned",
          issue: { id: "iw", identifier: "BLO-1234", title: "t" },
        },
        paperclipTaskMarkdown: "Paperclip task context:\n- Issue: BLO-1234",
      },
    });
    const result = buildJobManifest({ ctx, selfPod: makeSelfPod() });
    expect(result.prompt).toContain("Paperclip task context");
    expect(result.promptMetrics.taskMarkdownChars).toBeGreaterThan(0);
    expect(result.promptMetrics.wakePromptChars).toBeGreaterThan(0);
    expect(result.promptMetrics.heartbeatPromptChars).toBe(0);
  });
});

describe("fail-closed sensitive-env guard covers every container on the pod", () => {
  // BLO-21593 review finding: the guard used to enumerate the envVars/initEnv
  // locals, so the DinD sidecar — whose env is built inside buildDindSidecar()
  // and never flows through those locals — silently bypassed it. These pin the
  // check to the assembled pod spec instead.

  it("flags a sensitive literal in a sidecar container, not just the main one", () => {
    const podSpec = {
      initContainers: [
        { name: "write-prompt", env: [{ name: "PROMPT_FILE", value: "/tmp/p" }] },
        { name: "dind", env: [{ name: "DOCKER_TLS_CERTDIR", value: "" }, { name: "REGISTRY_TOKEN", value: "leaked" }] },
      ],
      containers: [{ name: "claude", env: [{ name: "AWS_REGION", value: "us-east-1" }] }],
    } as unknown as Parameters<typeof findLiteralSensitiveEnvVarsInPodSpec>[0];

    // The old per-array check, applied to the main container's env only, sees nothing.
    expect(findLiteralSensitiveEnvVars(podSpec.containers![0].env ?? [])).toEqual([]);
    // The pod-spec-wide check catches it, and names the offending container.
    expect(findLiteralSensitiveEnvVarsInPodSpec(podSpec)).toEqual(["dind/REGISTRY_TOKEN"]);
  });

  it("ignores the sidecar's non-sensitive literals", () => {
    const podSpec = {
      initContainers: [{ name: "dind", env: [{ name: "DOCKER_TLS_CERTDIR", value: "" }] }],
      containers: [{ name: "claude", env: [] }],
    } as unknown as Parameters<typeof findLiteralSensitiveEnvVarsInPodSpec>[0];
    expect(findLiteralSensitiveEnvVarsInPodSpec(podSpec)).toEqual([]);
  });

  it("guards the DinD sidecar that buildJobManifest actually assembles", () => {
    const ctx = makeCtx({ config: { enableDocker: true } });
    const { job } = buildJobManifest({ ctx, selfPod: makeSelfPod() });
    const podSpec = job.spec!.template.spec!;
    // The sidecar is on the pod...
    expect(podSpec.initContainers!.map((c) => c.name)).toContain("dind");
    // ...and is inside the guard's field of view (it returns clean today).
    expect(findLiteralSensitiveEnvVarsInPodSpec(podSpec)).toEqual([]);
  });
});
