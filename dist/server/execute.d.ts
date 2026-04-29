import type { AdapterExecutionContext, AdapterExecutionResult } from "@paperclipai/adapter-utils";
import type * as k8s from "@kubernetes/client-node";
/**
 * Detect a Kubernetes 404 (Not Found) error from @kubernetes/client-node.
 * Works for both v0.x (response.statusCode) and v1.0+ (response.status, message).
 * Exported for unit tests.
 */
export declare function isK8s404(err: unknown): boolean;
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
export declare function shouldAbortForCancellation(runStatus: string | undefined): boolean;
/**
 * Build the error message when Claude's stdout contains no result event.
 * Skips system/init event lines so the UI doesn't display the raw init JSON.
 * When `podState` is provided, appends the K8s container terminated reason/
 * message so failures self-explain without requiring `kubectl`.
 * Exported for unit tests.
 */
export declare function buildPartialRunError(exitCode: number | null, model: string, stdout: string, podState?: PodTerminatedState | null): string;
export type OrphanClassification = "reattach" | "block_session_mismatch" | "block_task_mismatch" | "block_task_unknown";
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
export declare function classifyOrphan(job: k8s.V1Job, expected: {
    taskId: string | null;
    sessionId: string | null;
}): OrphanClassification;
/**
 * Build an error message for a pod that reached phase=Failed before or
 * instead of streaming logs. Includes the claude container's terminated exit
 * code and reason when available so operators can diagnose crashes without
 * needing kubectl.  Exported for unit tests.
 */
export declare function describePodTerminatedError(podName: string, phase: string, containerStatuses: k8s.V1ContainerStatus[]): string;
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
/**
 * Format a human-readable explanation for a truncated run, including the
 * pod's claude-container terminated state when available. Exit code 137
 * is annotated as SIGKILL/OOM since that is the most common cause.
 * Exported for unit tests.
 */
export declare function describeTruncationCause(state: PodTerminatedState | null, lookup?: PodLookupResult): string;
export declare function execute(ctx: AdapterExecutionContext): Promise<AdapterExecutionResult>;
//# sourceMappingURL=execute.d.ts.map