import type { AdapterExecutionContext } from "@paperclipai/adapter-utils";
import { type PaperclipSkillEntry } from "@paperclipai/adapter-utils/server-utils";
export interface ClaudePromptBundle {
    bundleKey: string;
    /** Absolute path to the bundle root directory (contains .claude/skills/ and agent-instructions.md). */
    rootDir: string;
    /** Value to pass as --add-dir to the Claude CLI. */
    addDir: string;
    /** Path to the materialized instructions file, or null if no instructions were provided. */
    instructionsFilePath: string | null;
}
export declare function prepareClaudePromptBundle(input: {
    companyId: string;
    skills: PaperclipSkillEntry[];
    instructionsContents: string | null;
    onLog: AdapterExecutionContext["onLog"];
}): Promise<ClaudePromptBundle>;
//# sourceMappingURL=prompt-cache.d.ts.map