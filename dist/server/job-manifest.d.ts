import type * as k8s from "@kubernetes/client-node";
import type { AdapterExecutionContext } from "@paperclipai/adapter-utils";
import type { SelfPodInfo } from "./k8s-client.js";
export interface JobBuildInput {
    ctx: AdapterExecutionContext;
    selfPod: SelfPodInfo;
}
export interface JobBuildResult {
    job: k8s.V1Job;
    jobName: string;
    namespace: string;
    prompt: string;
    claudeArgs: string[];
    promptMetrics: Record<string, number>;
}
export declare function buildJobManifest(input: JobBuildInput): JobBuildResult;
//# sourceMappingURL=job-manifest.d.ts.map