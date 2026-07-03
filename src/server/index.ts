import type { ServerAdapterModule, AdapterSessionManagement } from "@paperclipai/adapter-utils";
import { type, models, agentConfigurationDoc } from "../index.js";
import { execute } from "./execute.js";
import { testEnvironment } from "./test.js";
import { sessionCodec } from "./session.js";
import { getConfigSchema } from "./config-schema.js";
import { listK8sSkills, syncK8sSkills } from "./skills.js";
import { listK8sModels } from "./models.js";

// Hoisted (not inline in `sessionManagement`) so the extra field passes the
// excess-property check when compiled against published adapter-utils
// releases whose SessionCompactionPolicy predates maxConsecutiveFailedResumes;
// Paperclip's vendored adapter-utils requires it (BLO-10889).
const defaultSessionCompaction = {
  enabled: true,
  maxSessionRuns: 0,
  maxRawInputTokens: 0,
  maxSessionAgeHours: 0,
  // 3 matches Paperclip's operative K8S_AGENT_SESSION_POLICY for claude_k8s:
  // the poisoned-session self-heal (BLO-10866) was observed on the k8s
  // external-lifecycle adapters, so if this module-declared policy is ever
  // honored over the host's static table, the self-heal must stay enabled.
  maxConsecutiveFailedResumes: 3,
};

const sessionManagement: AdapterSessionManagement = {
  supportsSessionResume: true,
  nativeContextManagement: "confirmed",
  defaultSessionCompaction,
};

export function createServerAdapter(): ServerAdapterModule {
  return {
    type,
    execute,
    testEnvironment,
    sessionCodec,
    sessionManagement,
    models,
    listModels: listK8sModels,
    listSkills: listK8sSkills,
    syncSkills: syncK8sSkills,
    supportsLocalAgentJwt: true,
    supportsInstructionsBundle: true,
    instructionsPathKey: "instructionsFilePath",
    requiresMaterializedRuntimeSkills: false,
    // Tells the reaper to skip local PID checks and use the staleness-based
    // liveness window instead (adapter spawns K8s Jobs in separate pods).
    // Cast required: adapter-utils ServerAdapterModule type predates this field.
    hasOutOfProcessLiveness: true,
    agentConfigurationDoc,
    getConfigSchema,
  } as ServerAdapterModule;
}

export { execute, testEnvironment, sessionCodec };
