import { spawnSync } from "node:child_process";
import { mkdtempSync, readFileSync, rmSync, writeFileSync } from "node:fs";
import { tmpdir } from "node:os";
import path from "node:path";
import { describe, expect, it } from "vitest";

import {
  ENV_GUARD_SCRIPT,
  SAFE_ENV_INSPECT_SCRIPT,
  buildEnvGuardSetupShell,
  classifyAgentShellCommand,
} from "./env-guard.js";

describe("classifyAgentShellCommand", () => {
  const blocked = [
    "env",
    "printenv",
    "set",
    "export -p",
    "declare -x",
    "cat /proc/self/environ",
    "cat /proc/1/environ",
    "/usr/bin/env",
    "command env",
    "env; ls -la",
    "ls && printenv",
    'sh -lc "env"',
    "bash -c 'printenv'",
  ];
  const allowed = [
    // Legitimate env USE (set-and-run) must not be blocked.
    "env FOO=bar node script.js",
    "printenv PATH",
    // set with flags is ubiquitous in agent shells.
    "set -euo pipefail",
    "set -e",
    // Ordinary commands.
    "ls -la",
    "git status",
    'echo "hello"',
    "grep -r env .",
    // The allowlisted names-only helper.
    "node ~/.claude/safe-env-inspect.mjs",
    "./scripts/safe-env-inspect.mjs",
    "paperclip-safe-env",
    "",
  ];

  for (const cmd of blocked) {
    it(`blocks: ${JSON.stringify(cmd)}`, () => {
      const d = classifyAgentShellCommand(cmd);
      expect(d.action).toBe("block");
      expect(d.reason).toBe("full_environment_dump");
    });
  }

  for (const cmd of allowed) {
    it(`allows: ${JSON.stringify(cmd)}`, () => {
      expect(classifyAgentShellCommand(cmd).action).toBe("allow");
    });
  }
});

/**
 * Execute the literal embedded guard artifact as a real Node process, feeding
 * it a PreToolUse event on stdin — this validates the exact file the pod runs,
 * not a TS re-implementation.
 */
function runGuardScript(event: unknown): { status: number | null; stderr: string } {
  const dir = mkdtempSync(path.join(tmpdir(), "pc-guard-"));
  try {
    const file = path.join(dir, "guard.mjs");
    writeFileSync(file, ENV_GUARD_SCRIPT);
    const res = spawnSync(process.execPath, [file], {
      input: JSON.stringify(event),
      encoding: "utf8",
    });
    return { status: res.status, stderr: res.stderr ?? "" };
  } finally {
    rmSync(dir, { recursive: true, force: true });
  }
}

describe("embedded guard script (real node process)", () => {
  it("exits 2 and explains on a Bash env dump", () => {
    const { status, stderr } = runGuardScript({ tool_name: "Bash", tool_input: { command: "env" } });
    expect(status).toBe(2);
    expect(stderr).toContain("PEN-1305");
    expect(stderr).toContain("safe-env-inspect.mjs");
  });

  it("exits 0 for a benign Bash command", () => {
    expect(runGuardScript({ tool_name: "Bash", tool_input: { command: "ls -la" } }).status).toBe(0);
  });

  it("exits 0 for the allowlisted helper", () => {
    const evt = { tool_name: "Bash", tool_input: { command: "node ~/.claude/safe-env-inspect.mjs" } };
    expect(runGuardScript(evt).status).toBe(0);
  });

  it("exits 0 for a non-Bash tool even if the payload looks like a dump", () => {
    expect(runGuardScript({ tool_name: "Read", tool_input: { command: "env" } }).status).toBe(0);
  });

  it("fails open (exit 0) on malformed input", () => {
    const dir = mkdtempSync(path.join(tmpdir(), "pc-guard-"));
    try {
      const file = path.join(dir, "guard.mjs");
      writeFileSync(file, ENV_GUARD_SCRIPT);
      const res = spawnSync(process.execPath, [file], { input: "not json", encoding: "utf8" });
      expect(res.status).toBe(0);
    } finally {
      rmSync(dir, { recursive: true, force: true });
    }
  });
});

describe("safe-env-inspect helper prints names only", () => {
  it("emits variable names, never values", () => {
    const dir = mkdtempSync(path.join(tmpdir(), "pc-safe-"));
    try {
      const file = path.join(dir, "safe.mjs");
      writeFileSync(file, SAFE_ENV_INSPECT_SCRIPT);
      const res = spawnSync(process.execPath, [file], {
        encoding: "utf8",
        env: { ...process.env, PC_TEST_SECRET: "super-secret-value-xyz" },
      });
      expect(res.status).toBe(0);
      expect(res.stdout).toContain("PC_TEST_SECRET");
      expect(res.stdout).not.toContain("super-secret-value-xyz");
    } finally {
      rmSync(dir, { recursive: true, force: true });
    }
  });
});

describe("buildEnvGuardSetupShell", () => {
  // Recover the three base64 blobs the shell fragment installs.
  function decodedBlobs(shell: string): string[] {
    return [...shell.matchAll(/printf %s '([A-Za-z0-9+/=]+)'/g)].map((m) =>
      Buffer.from(m[1]!, "base64").toString("utf8"),
    );
  }

  it("round-trips the guard + helper scripts through base64", () => {
    const blobs = decodedBlobs(buildEnvGuardSetupShell());
    expect(blobs).toContain(ENV_GUARD_SCRIPT);
    expect(blobs).toContain(SAFE_ENV_INSPECT_SCRIPT);
  });

  it("merges the PreToolUse hook idempotently, preserving existing hooks", () => {
    const shell = buildEnvGuardSetupShell();
    // The merge blob is the 3rd base64 (guard, helper, merge).
    const mergeScript = decodedBlobs(shell)[2]!;
    const dir = mkdtempSync(path.join(tmpdir(), "pc-settings-"));
    try {
      // Seed an existing Stop hook to prove it is preserved.
      writeFileSync(
        path.join(dir, "settings.json"),
        JSON.stringify({ hooks: { Stop: [{ hooks: [{ type: "command", command: "echo stop" }] }] } }),
      );
      const env = { ...process.env, CLAUDE_CONFIG_DIR: dir };
      const run = () => spawnSync(process.execPath, ["-"], { input: mergeScript, encoding: "utf8", env });
      expect(run().status).toBe(0);
      expect(run().status).toBe(0); // run twice → must stay idempotent

      const settings = JSON.parse(readFileSync(path.join(dir, "settings.json"), "utf8"));
      const pre = settings.hooks.PreToolUse;
      expect(Array.isArray(pre)).toBe(true);
      const guardEntries = pre.filter(
        (g: { matcher?: string }) => g.matcher === "Bash",
      );
      expect(guardEntries).toHaveLength(1);
      expect(guardEntries[0].hooks[0].command).toContain("paperclip-env-guard.mjs");
      // Existing Stop hook survived.
      expect(settings.hooks.Stop[0].hooks[0].command).toBe("echo stop");
    } finally {
      rmSync(dir, { recursive: true, force: true });
    }
  });
});
