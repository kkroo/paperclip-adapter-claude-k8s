/**
 * Self-contained stdout parser for Claude stream-json output.
 * Zero external imports — required by the Paperclip adapter plugin UI parser contract.
 */
type TranscriptEntry = {
    kind: "assistant";
    ts: string;
    text: string;
} | {
    kind: "thinking";
    ts: string;
    text: string;
} | {
    kind: "user";
    ts: string;
    text: string;
} | {
    kind: "tool_call";
    ts: string;
    name: string;
    input: unknown;
    toolUseId?: string;
} | {
    kind: "tool_result";
    ts: string;
    toolUseId: string;
    content: string;
    isError: boolean;
} | {
    kind: "init";
    ts: string;
    model: string;
    sessionId: string;
} | {
    kind: "result";
    ts: string;
    text: string;
    inputTokens: number;
    outputTokens: number;
    cachedTokens: number;
    costUsd: number;
    subtype: string;
    isError: boolean;
    errors: string[];
} | {
    kind: "stderr";
    ts: string;
    text: string;
} | {
    kind: "system";
    ts: string;
    text: string;
} | {
    kind: "stdout";
    ts: string;
    text: string;
};
export declare function parseStdoutLine(line: string, ts: string): TranscriptEntry[];
export {};
//# sourceMappingURL=ui-parser.d.ts.map