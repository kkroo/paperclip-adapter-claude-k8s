/**
 * Adapter-managed Claude Code runtime.
 *
 * Why this exists: Job pods inherit the paperclip image, whose Dockerfile
 * installs `@anthropic-ai/claude-code@latest` into a root-owned, layer-cached
 * `/usr/local/lib/node_modules`. The fleet therefore runs whatever version that
 * cached layer froze — 2.1.210 on 2026-10-06, four months behind — until the
 * image is rebuilt, and `claude update` cannot fix it because Job pods run as
 * uid 1000 without write access to /usr/local. The Anthropic API refuses old
 * clients for new models ("Claude Code 2.1.210 does not support this model;
 * version 2.1.280 or newer is required"), so a stale bundled CLI blocks every
 * model launch for the whole fleet.
 *
 * This module pins the CLI version in adapter config instead of in the image.
 * The Job's main command installs the pinned `@anthropic-ai/claude-code` once
 * into a shared, versioned directory on the data PVC and prepends its bin dir
 * to PATH. Installs are serialized with a mkdir lock, staged into a temp dir
 * and renamed into place only after the binary answers `--version`, so
 * concurrent Jobs never execute a half-written runtime. When the install cannot
 * be completed (registry unreachable, quota) the run falls back to the image's
 * bundled CLI with a loud stderr line rather than failing outright — a model
 * the old CLI supports keeps working, and a model it does not support fails
 * with the same API error it fails with today.
 */

/** npm package that ships the Claude Code CLI. */
export const CLAUDE_CODE_PACKAGE = "@anthropic-ai/claude-code";

/**
 * Default pinned CLI version. Bump this when a model launch needs a newer
 * client (the API error names the minimum). Operators can override per agent
 * with adapterConfig.claudeCodeVersion.
 */
export const DEFAULT_CLAUDE_CODE_VERSION = "2.1.292";

/** Config sentinel: run the CLI bundled in the container image, no bootstrap. */
export const CLAUDE_CODE_RUNTIME_FROM_IMAGE = "image";

/** Shared runtimes root, relative to the data PVC mount (HOME for Job pods). */
export const RUNTIMES_DIR_RELATIVE = ".local/lib/paperclip-k8s-runtimes";

/** Exact npm versions only — the value is interpolated into a shell command. */
const EXACT_VERSION_RE = /^\d+\.\d+\.\d+(?:-[0-9A-Za-z.-]+)?$/;

/**
 * Resolve adapterConfig.claudeCodeVersion.
 *
 * - unset / blank  → DEFAULT_CLAUDE_CODE_VERSION (adapter-managed)
 * - "image"        → "" (use the image's bundled CLI, legacy behaviour)
 * - "x.y.z"        → that exact version (adapter-managed)
 *
 * Anything else throws: the value is shell-interpolated, so a range, tag or
 * stray character must never reach the Job command.
 */
export function resolveClaudeCodeVersion(raw: unknown): string {
  const value = typeof raw === "string" ? raw.trim() : "";
  if (!value) return DEFAULT_CLAUDE_CODE_VERSION;
  if (value === CLAUDE_CODE_RUNTIME_FROM_IMAGE) return "";
  if (!EXACT_VERSION_RE.test(value)) {
    throw new Error(
      `claudeCodeVersion must be an exact version such as ${DEFAULT_CLAUDE_CODE_VERSION}, or "${CLAUDE_CODE_RUNTIME_FROM_IMAGE}" to use the container image's CLI; got ${JSON.stringify(value)}`,
    );
  }
  return value;
}

/** Directory holding one installed CLI version on the shared data PVC. */
export function claudeCodeRuntimeDir(dataMountPath: string, version: string): string {
  return `${dataMountPath.replace(/\/+$/, "")}/${RUNTIMES_DIR_RELATIVE}/claude-code/${version}`;
}

function shellSingleQuote(value: string): string {
  return `'${value.replace(/'/g, "'\\''")}'`;
}

/**
 * POSIX sh snippet (no trailing separator) that makes the pinned CLI the
 * `claude` on PATH for the rest of the Job command.
 *
 * Layout on the PVC:
 *   <data>/.local/lib/paperclip-k8s-runtimes/claude-code/<version>/   installed prefix
 *   .../<version>/.complete                                            written last
 *   .../.lock-<version>                                                mkdir lock
 *   .../.tmp-<version>-<pid>                                           staging dir
 *
 * Properties:
 * - idempotent: a complete install is reused by every later Job, any isolation key;
 * - serialized: `mkdir` of the lock dir is atomic on CephFS/NFS; losers wait
 *   (up to 5 min) for the winner's `.complete` marker instead of installing twice;
 * - crash-safe: a lock older than 20 min is reclaimed, a dir without `.complete`
 *   is rebuilt, and the staging dir is renamed into place only after the fresh
 *   binary answers `--version`;
 * - fail-open: if nothing usable exists afterwards the image CLI is used and
 *   the pod log says so;
 * - deterministic: DISABLE_AUTOUPDATER=1 keeps the managed copy at the pin
 *   unless the operator set that variable themselves.
 */
export function buildClaudeCodeRuntimeShell(opts: { version: string; dataMountPath: string }): string {
  const { version, dataMountPath } = opts;
  if (!EXACT_VERSION_RE.test(version)) throw new Error(`invalid claude-code version: ${JSON.stringify(version)}`);
  const root = `${dataMountPath.replace(/\/+$/, "")}/${RUNTIMES_DIR_RELATIVE}/claude-code`;
  const pkg = CLAUDE_CODE_PACKAGE;
  const spec = `${pkg}@$__pcver`;
  return [
    `__pcver=${shellSingleQuote(version)}`,
    `__pcroot=${shellSingleQuote(root)}`,
    '__pcdir="$__pcroot/$__pcver"',
    '__pcbin="$__pcdir/node_modules/.bin/claude"',
    'if [ ! -f "$__pcdir/.complete" ] || [ ! -x "$__pcbin" ]; then ' +
      'mkdir -p "$__pcroot" 2>/dev/null; __pclock="$__pcroot/.lock-$__pcver"; ' +
      'if [ -d "$__pclock" ] && [ -n "$(find "$__pclock" -maxdepth 0 -mmin +20 2>/dev/null)" ]; then ' +
        'echo "[paperclip] reclaiming stale claude-code install lock $__pclock" >&2; rmdir "$__pclock" 2>/dev/null; fi; ' +
      'if mkdir "$__pclock" 2>/dev/null; then ' +
        '__pctmp="$__pcroot/.tmp-$__pcver-$$"; rm -rf "$__pctmp" "$__pcdir"; mkdir -p "$__pctmp"; ' +
        `echo "[paperclip] installing ${spec} into $__pcdir" >&2; ` +
        `if npm install --prefix "$__pctmp" --omit=dev --no-audit --no-fund --no-package-lock --loglevel=error "${spec}" >&2 ` +
          '&& "$__pctmp/node_modules/.bin/claude" --version >/dev/null 2>&1; then ' +
          'mv "$__pctmp" "$__pcdir" && : > "$__pcdir/.complete"; ' +
        `else echo "[paperclip] ${spec} install failed" >&2; rm -rf "$__pctmp"; fi; ` +
        'rmdir "$__pclock" 2>/dev/null; ' +
      'else ' +
        `echo "[paperclip] waiting for a concurrent ${spec} install" >&2; ` +
        '__pci=0; while [ ! -f "$__pcdir/.complete" ] && [ -d "$__pclock" ] && [ "$__pci" -lt 300 ]; do sleep 1; __pci=$((__pci+1)); done; ' +
      'fi; ' +
    'fi',
    'if [ -f "$__pcdir/.complete" ] && [ -x "$__pcbin" ]; then ' +
      'export PATH="$__pcdir/node_modules/.bin:$PATH"; ' +
      '[ -n "${DISABLE_AUTOUPDATER+x}" ] || export DISABLE_AUTOUPDATER=1; ' +
      'echo "[paperclip] claude-code runtime $(claude --version 2>/dev/null) (adapter-managed, pinned $__pcver)" >&2; ' +
    'else ' +
      'echo "[paperclip] claude-code $__pcver unavailable; falling back to the image claude $(claude --version 2>/dev/null)" >&2; ' +
    'fi',
  ].join("; ");
}
