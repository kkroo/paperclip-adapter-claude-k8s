import * as k8s from "@kubernetes/client-node";
/**
 * Cached self-pod introspection result. Queried once on first execute(),
 * then reused for all subsequent Job builds so every Job inherits the
 * Deployment's image, imagePullSecrets, DNS config, PVC claim, and scheduling.
 */
export interface SelfPodSecretVolume {
    volumeName: string;
    secretName: string;
    mountPath: string;
    defaultMode: number | undefined;
}
export interface SelfPodInfo {
    namespace: string;
    image: string;
    imagePullSecrets: Array<{
        name: string;
    }>;
    dnsConfig: k8s.V1PodDNSConfig | undefined;
    nodeSelector: Record<string, string>;
    tolerations: k8s.V1Toleration[];
    pvcClaimName: string | null;
    secretVolumes: SelfPodSecretVolume[];
    /** Env vars inherited from the Deployment container (literal name/value pairs). */
    inheritedEnv: Record<string, string>;
    /** Env vars with valueFrom (secretKeyRef, configMapKeyRef, etc.) from the Deployment container. */
    inheritedEnvValueFrom: k8s.V1EnvVar[];
    /** envFrom sources (secretRef, configMapRef) from the Deployment container. */
    inheritedEnvFrom: k8s.V1EnvFromSource[];
}
export declare function getBatchApi(kubeconfigPath?: string): k8s.BatchV1Api;
export declare function getCoreApi(kubeconfigPath?: string): k8s.CoreV1Api;
export declare function getAuthzApi(kubeconfigPath?: string): k8s.AuthorizationV1Api;
export declare function getLogApi(kubeconfigPath?: string): k8s.Log;
/**
 * Query the K8s API for our own pod spec and cache the result.
 * Extracts image, imagePullSecrets, dnsConfig, scheduling, PVC claim name,
 * and environment variables to forward to Job pods.
 */
export declare function getSelfPodInfo(kubeconfigPath?: string): Promise<SelfPodInfo>;
/** Reset cached state — useful for tests. */
export declare function resetCache(): void;
//# sourceMappingURL=k8s-client.d.ts.map