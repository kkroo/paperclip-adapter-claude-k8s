/**
 * Format a single raw Claude stream-json line into a plain-text human-readable
 * string (no ANSI colour codes) suitable for forwarding to the Paperclip server
 * via onLog.  Returns null for lines that should be suppressed (empty,
 * assistant events with no printable content, etc.).  Non-JSON lines are
 * returned as-is so plain-text adapter status messages pass through unchanged.
 *
 * Mirrors the event coverage of printClaudeStreamEvent so the K8s server
 * streaming path and the CLI display path produce consistent output.
 */
export declare function formatClaudeStreamLine(raw: string): string | null;
export declare function printClaudeStreamEvent(raw: string, debug: boolean): void;
//# sourceMappingURL=format-event.d.ts.map