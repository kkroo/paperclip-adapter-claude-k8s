import type * as k8s from "@kubernetes/client-node";
import type { AdapterExecutionContext } from "@paperclipai/adapter-utils";
import type { ClaudePromptBundle } from "./prompt-cache.js";
export declare function buildPodLogPath(companyId: string, agentId: string, runId: string): string;
import type { SelfPodInfo } from "./k8s-client.js";
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
/**
 * Sanitize a string for use as a Kubernetes label value (RFC 1123 subset:
 * `[a-zA-Z0-9]([-_.a-zA-Z0-9]*[a-zA-Z0-9])?`, max 63 chars).  Returns `null`
 * when no usable characters remain — the caller should omit the label.
 */
export declare function sanitizeLabelValue(value: string, maxLen?: number): string | null;
export declare function buildJobManifest(input: JobBuildInput): JobBuildResult;
//# sourceMappingURL=job-manifest.d.ts.map