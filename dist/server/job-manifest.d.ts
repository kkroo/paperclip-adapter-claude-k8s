import type * as k8s from "@kubernetes/client-node";
import type { AdapterExecutionContext } from "@paperclipai/adapter-utils";
import type { SelfPodInfo } from "./k8s-client.js";
export interface JobBuildInput {
    ctx: AdapterExecutionContext;
    selfPod: SelfPodInfo;
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
}
export declare function buildJobManifest(input: JobBuildInput): JobBuildResult;
//# sourceMappingURL=job-manifest.d.ts.map