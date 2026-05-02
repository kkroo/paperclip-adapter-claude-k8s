/**
 * Phase E.1 — environment.config (k8s remote target) merge wiring.
 *
 * Verifies that when paperclip dispatches a heartbeat with
 * `executionTarget.kind === "remote" && transport === "k8s"`, the adapter
 * merges `executionTarget.config` over `ctx.config` with environment fields
 * winning, and that the resulting fields flow through to the V1Job manifest.
 */
import { describe, it, expect, vi, beforeEach } from "vitest";
import type { AdapterExecutionContext } from "@paperclipai/adapter-utils";

// Reuse the same K8s mock shape as execute.test.ts so we can drive a happy
// path up to (and including) `createNamespacedJob` and inspect the body.
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
const mockReadSkillEntries = vi.hoisted(() => vi.fn());

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
  return Object.assign(Object.create(null), original, {
    readPaperclipRuntimeSkillEntries: mockReadSkillEntries,
  });
});

const { execute, mergeEnvironmentConfig } = await import("./execute.js");

function makeSelfPodResult() {
  return {
    namespace: "paperclip",
    image: "paperclipai/paperclip:latest",
    imagePullSecrets: [],
    dnsConfig: undefined,
    nodeSelector: { workload: "paperclip" },
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

function makeCtx(overrides: Partial<AdapterExecutionContext> = {}): AdapterExecutionContext {
  return {
    runId: "run-test-env-001",
    agent: {
      id: "agent-env",
      companyId: "co1",
      name: "Env Test Agent",
      adapterType: "claude_k8s",
      adapterConfig: {},
    },
    runtime: { sessionId: null, sessionParams: null, sessionDisplayId: null, taskKey: null },
    config: {},
    context: {},
    onLog: vi.fn().mockResolvedValue(undefined),
    ...overrides,
  } as unknown as AdapterExecutionContext;
}

describe("mergeEnvironmentConfig", () => {
  it("returns adapterConfig unchanged when environmentConfig is undefined", () => {
    const adapter = { namespace: "default", nodeSelector: { foo: "bar" } };
    expect(mergeEnvironmentConfig(adapter, undefined)).toEqual(adapter);
  });

  it("returns adapterConfig unchanged when environmentConfig is null", () => {
    const adapter = { namespace: "default" };
    expect(mergeEnvironmentConfig(adapter, null)).toEqual(adapter);
  });

  it("environmentConfig wins for fields set on both sides", () => {
    const merged = mergeEnvironmentConfig(
      { namespace: "default", nodeSelector: { foo: "bar" } },
      { namespace: "paperclip", workspaceVolumeClaim: "paperclip-data" },
    );
    expect(merged).toEqual({
      namespace: "paperclip",
      nodeSelector: { foo: "bar" },
      workspaceVolumeClaim: "paperclip-data",
    });
  });

  it("nested object fields override at top level (no deep merge)", () => {
    const merged = mergeEnvironmentConfig(
      { nodeSelector: { workload: "paperclip", arch: "amd64" } },
      { nodeSelector: { workload: "agents" } },
    );
    expect(merged.nodeSelector).toEqual({ workload: "agents" });
  });

  it("ignores environmentConfig keys whose values are null/undefined", () => {
    const merged = mergeEnvironmentConfig(
      { namespace: "default" },
      { namespace: null, workspaceMountPath: "/paperclip", kubeconfig: undefined },
    );
    expect(merged.namespace).toBe("default");
    expect(merged.workspaceMountPath).toBe("/paperclip");
    expect("kubeconfig" in merged).toBe(false);
  });
});

describe("execute: environment.config merging into V1Job manifest", () => {
  beforeEach(() => {
    vi.resetAllMocks();
    mockReadSkillEntries.mockResolvedValue([]);
    mockGetSelfPodInfo.mockResolvedValue(makeSelfPodResult());
    mockBatchListJobs.mockResolvedValue({ items: [] });
    mockPrepareBundle.mockResolvedValue(makeBundle());
    // Fail at job creation so we don't get into the wait-for-pod loop, but
    // still have a captured Job manifest body to assert against.
    mockBatchCreateJob.mockRejectedValue(new Error("intentional stop after manifest build"));
  });

  it("nodeSelector from environment overrides adapter_config", async () => {
    await execute(
      makeCtx({
        config: { nodeSelector: { workload: "agents-default" } },
        // executionTarget is not yet declared on AdapterExecutionContext in
        // the published adapter-utils types — pass via cast.
        executionTarget: {
          kind: "remote",
          transport: "k8s",
          remoteCwd: "/paperclip",
          config: {
            kubeconfig: null,
            namespace: null,
            workspaceVolumeClaim: null,
            workspaceMountPath: null,
            secretsNamespace: null,
            nodeSelector: { workload: "agents-from-env" },
            tolerations: [],
            labels: {},
            serviceAccountName: null,
            imagePullPolicy: null,
            resources: null,
          },
        },
      } as unknown as Partial<AdapterExecutionContext>),
    );

    expect(mockBatchCreateJob).toHaveBeenCalledTimes(1);
    const callArg = mockBatchCreateJob.mock.calls[0]?.[0] as { body: { spec: { template: { spec: { nodeSelector?: Record<string, string> } } } } };
    const ns = callArg.body.spec.template.spec.nodeSelector;
    expect(ns).toEqual({ workload: "agents-from-env" });
  });

  it("workspaceVolumeClaim from environment overrides selfPod PVC", async () => {
    await execute(
      makeCtx({
        config: {},
        executionTarget: {
          kind: "remote",
          transport: "k8s",
          remoteCwd: "/paperclip",
          config: {
            kubeconfig: null,
            namespace: null,
            workspaceVolumeClaim: "env-supplied-pvc",
            workspaceMountPath: null,
            secretsNamespace: null,
            nodeSelector: {},
            tolerations: [],
            labels: {},
            serviceAccountName: null,
            imagePullPolicy: null,
            resources: null,
          },
        },
      } as unknown as Partial<AdapterExecutionContext>),
    );

    expect(mockBatchCreateJob).toHaveBeenCalledTimes(1);
    const callArg = mockBatchCreateJob.mock.calls[0]?.[0] as { body: { spec: { template: { spec: { volumes?: Array<{ name: string; persistentVolumeClaim?: { claimName: string } }> } } } } };
    const dataVol = callArg.body.spec.template.spec.volumes?.find((v) => v.name === "data");
    expect(dataVol?.persistentVolumeClaim?.claimName).toBe("env-supplied-pvc");
  });

  it("workspaceMountPath from environment overrides default /paperclip mountPath", async () => {
    await execute(
      makeCtx({
        config: {},
        executionTarget: {
          kind: "remote",
          transport: "k8s",
          remoteCwd: "/workspace",
          config: {
            kubeconfig: null,
            namespace: null,
            workspaceVolumeClaim: null,
            workspaceMountPath: "/workspace",
            secretsNamespace: null,
            nodeSelector: {},
            tolerations: [],
            labels: {},
            serviceAccountName: null,
            imagePullPolicy: null,
            resources: null,
          },
        },
      } as unknown as Partial<AdapterExecutionContext>),
    );

    expect(mockBatchCreateJob).toHaveBeenCalledTimes(1);
    const callArg = mockBatchCreateJob.mock.calls[0]?.[0] as { body: { spec: { template: { spec: { containers: Array<{ volumeMounts?: Array<{ name: string; mountPath: string }> }> } } } } };
    const dataMount = callArg.body.spec.template.spec.containers[0]?.volumeMounts?.find((vm) => vm.name === "data");
    expect(dataMount?.mountPath).toBe("/workspace");
  });

  it("falls through to adapter_config defaults when no executionTarget is provided", async () => {
    await execute(
      makeCtx({
        config: { nodeSelector: { workload: "adapter-default" } },
      }),
    );

    expect(mockBatchCreateJob).toHaveBeenCalledTimes(1);
    const callArg = mockBatchCreateJob.mock.calls[0]?.[0] as { body: { spec: { template: { spec: { nodeSelector?: Record<string, string>; volumes?: Array<{ name: string; persistentVolumeClaim?: { claimName: string } }>; containers: Array<{ volumeMounts?: Array<{ name: string; mountPath: string }> }> } } } } };
    expect(callArg.body.spec.template.spec.nodeSelector).toEqual({ workload: "adapter-default" });
    const dataVol = callArg.body.spec.template.spec.volumes?.find((v) => v.name === "data");
    expect(dataVol?.persistentVolumeClaim?.claimName).toBe("paperclip-data");
    const dataMount = callArg.body.spec.template.spec.containers[0]?.volumeMounts?.find((vm) => vm.name === "data");
    expect(dataMount?.mountPath).toBe("/paperclip");
  });

  it("falls through to adapter_config when executionTarget.kind is local", async () => {
    await execute(
      makeCtx({
        config: { nodeSelector: { workload: "adapter-default" } },
        executionTarget: {
          kind: "local",
          environmentId: null,
          leaseId: null,
        },
      } as unknown as Partial<AdapterExecutionContext>),
    );

    expect(mockBatchCreateJob).toHaveBeenCalledTimes(1);
    const callArg = mockBatchCreateJob.mock.calls[0]?.[0] as { body: { spec: { template: { spec: { nodeSelector?: Record<string, string> } } } } };
    expect(callArg.body.spec.template.spec.nodeSelector).toEqual({ workload: "adapter-default" });
  });

  it("ignores executionTarget for non-k8s transports (e.g. ssh)", async () => {
    await execute(
      makeCtx({
        config: { nodeSelector: { workload: "adapter-default" } },
        executionTarget: {
          kind: "remote",
          transport: "ssh",
          remoteCwd: "/home/user",
          spec: { host: "example.com", port: 22, username: "user" },
        },
      } as unknown as Partial<AdapterExecutionContext>),
    );

    expect(mockBatchCreateJob).toHaveBeenCalledTimes(1);
    const callArg = mockBatchCreateJob.mock.calls[0]?.[0] as { body: { spec: { template: { spec: { nodeSelector?: Record<string, string> } } } } };
    expect(callArg.body.spec.template.spec.nodeSelector).toEqual({ workload: "adapter-default" });
  });
});
