import { describe, it, expect } from "vitest";
import {
  CLAUDE_CODE_PACKAGE,
  DEFAULT_CLAUDE_CODE_VERSION,
  buildClaudeCodeRuntimeShell,
  claudeCodeRuntimeDir,
  resolveClaudeCodeVersion,
} from "./runtime-pin.js";

describe("resolveClaudeCodeVersion", () => {
  it("defaults to the adapter pin when unset or blank", () => {
    expect(resolveClaudeCodeVersion(undefined)).toBe(DEFAULT_CLAUDE_CODE_VERSION);
    expect(resolveClaudeCodeVersion("")).toBe(DEFAULT_CLAUDE_CODE_VERSION);
    expect(resolveClaudeCodeVersion("   ")).toBe(DEFAULT_CLAUDE_CODE_VERSION);
    expect(resolveClaudeCodeVersion(42)).toBe(DEFAULT_CLAUDE_CODE_VERSION);
  });

  it('returns "" for the "image" sentinel (use the bundled CLI)', () => {
    expect(resolveClaudeCodeVersion("image")).toBe("");
    expect(resolveClaudeCodeVersion(" image ")).toBe("");
  });

  it("accepts exact versions, including prereleases", () => {
    expect(resolveClaudeCodeVersion("2.1.280")).toBe("2.1.280");
    expect(resolveClaudeCodeVersion(" 3.0.0-beta.1 ")).toBe("3.0.0-beta.1");
  });

  it("rejects ranges, tags and shell metacharacters — the value is shell-interpolated", () => {
    for (const bad of ["latest", "^2.1.0", "2.1", "2.1.292; rm -rf /", "2.1.292$(id)", "v2.1.292", "2.1.292 "]) {
      if (bad === "2.1.292 ") continue; // trimmed → valid
      expect(() => resolveClaudeCodeVersion(bad), bad).toThrow(/claudeCodeVersion must be an exact version/);
    }
  });

  it("the default pin satisfies the Opus 5.5 floor (2.1.280)", () => {
    const [major, minor, patch] = DEFAULT_CLAUDE_CODE_VERSION.split(".").map(Number);
    expect(major).toBeGreaterThanOrEqual(2);
    expect(major > 2 || minor > 1 || (minor === 1 && patch >= 280)).toBe(true);
  });
});

describe("claudeCodeRuntimeDir", () => {
  it("lives under the data mount's shared runtimes root", () => {
    expect(claudeCodeRuntimeDir("/paperclip", "2.1.292")).toBe(
      "/paperclip/.local/lib/paperclip-k8s-runtimes/claude-code/2.1.292",
    );
    expect(claudeCodeRuntimeDir("/data/", "2.1.292")).toBe("/data/.local/lib/paperclip-k8s-runtimes/claude-code/2.1.292");
  });
});

describe("buildClaudeCodeRuntimeShell", () => {
  const shell = buildClaudeCodeRuntimeShell({ version: "2.1.292", dataMountPath: "/paperclip" });

  it("installs the exact pinned package into a versioned prefix on the data PVC", () => {
    expect(shell).toContain("__pcver='2.1.292'");
    expect(shell).toContain("__pcroot='/paperclip/.local/lib/paperclip-k8s-runtimes/claude-code'");
    expect(shell).toContain(
      `npm install --prefix "$__pctmp" --omit=dev --no-audit --no-fund --no-package-lock --loglevel=error "${CLAUDE_CODE_PACKAGE}@$__pcver"`,
    );
  });

  it("serializes concurrent installs with an atomic mkdir lock and waits for the winner", () => {
    expect(shell).toContain('if mkdir "$__pclock" 2>/dev/null; then');
    expect(shell).toMatch(/while \[ ! -f "\$__pcdir\/\.complete" \] && \[ -d "\$__pclock" \] && \[ "\$__pci" -lt 300 \]; do sleep 1/);
    // A lock left behind by a crashed installer is reclaimed after 20 minutes.
    expect(shell).toContain('find "$__pclock" -maxdepth 0 -mmin +20');
  });

  it("only publishes a runtime whose binary answers --version, via rename + marker", () => {
    const verifyIdx = shell.indexOf('"$__pctmp/node_modules/.bin/claude" --version');
    const publishIdx = shell.indexOf('mv "$__pctmp" "$__pcdir" && : > "$__pcdir/.complete"');
    expect(verifyIdx).toBeGreaterThan(-1);
    expect(publishIdx).toBeGreaterThan(verifyIdx);
    expect(shell).toContain('rm -rf "$__pctmp"; fi');
  });

  it("puts the managed CLI first on PATH and pins it against self-update, else falls back to the image CLI", () => {
    expect(shell).toContain('export PATH="$__pcdir/node_modules/.bin:$PATH"');
    expect(shell).toContain('[ -n "${DISABLE_AUTOUPDATER+x}" ] || export DISABLE_AUTOUPDATER=1');
    expect(shell).toContain("falling back to the image claude");
    expect(shell).not.toMatch(/exit \d/);
  });

  it("refuses an unvalidated version", () => {
    expect(() => buildClaudeCodeRuntimeShell({ version: "latest", dataMountPath: "/paperclip" })).toThrow(/invalid claude-code version/);
  });

  it("quotes a data mount path with a single quote safely", () => {
    const quoted = buildClaudeCodeRuntimeShell({ version: "2.1.292", dataMountPath: "/mnt/it's" });
    expect(quoted).toContain("__pcroot='/mnt/it'\\''s/.local/lib/paperclip-k8s-runtimes/claude-code'");
  });
});
