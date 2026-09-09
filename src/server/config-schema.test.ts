import { describe, it, expect } from "vitest";
import { getConfigSchema } from "./config-schema.js";

interface ConfigFieldSchema {
  key: string;
  label: string;
  type: string;
  default?: unknown;
  options?: { label: string; value: string }[];
}

describe("getConfigSchema", () => {
  it("returns a non-empty schema", () => {
    const schema = getConfigSchema();
    expect(schema.fields.length).toBeGreaterThan(0);
  });

  it("does not include platform-provided fields", () => {
    const schema = getConfigSchema();
    const keys = schema.fields.map((f: ConfigFieldSchema) => f.key);
    // These fields are provided by the platform and should not be duplicated
    expect(keys).not.toContain("model");
    expect(keys).not.toContain("effort");
    expect(keys).not.toContain("instructionsFilePath");
    expect(keys).not.toContain("timeoutSec");
    expect(keys).not.toContain("graceSec");
  });

  it("maxTurnsPerRun defaults to 1000", () => {
    const schema = getConfigSchema();
    const field = schema.fields.find((f: ConfigFieldSchema) => f.key === "maxTurnsPerRun");
    expect(field).toBeDefined();
    expect(field!.type).toBe("number");
    expect(field!.default).toBe(1000);
  });

  it("does not expose dangerouslySkipPermissions in UI schema", () => {
    const schema = getConfigSchema();
    const field = schema.fields.find((f: ConfigFieldSchema) => f.key === "dangerouslySkipPermissions");
    expect(field).toBeUndefined();
  });

  it("reattachOrphanedJobs defaults to true", () => {
    const schema = getConfigSchema();
    const field = schema.fields.find((f: ConfigFieldSchema) => f.key === "reattachOrphanedJobs");
    expect(field).toBeDefined();
    expect(field!.type).toBe("toggle");
    expect(field!.default).toBe(true);
  });

  it("exposes the Caveman and Ponytail fields", () => {
    const schema = getConfigSchema();
    const fields = new Map(schema.fields.map((field: ConfigFieldSchema) => [field.key, field]));
    expect(fields.get("agentCommand")?.type).toBe("text");
    expect(fields.get("ponytailPluginPath")?.type).toBe("text");
    expect(fields.get("ponytailDefaultMode")?.type).toBe("select");
    expect(fields.get("ponytailDefaultMode")?.options).toEqual([
      { value: "off", label: "Off" },
      { value: "lite", label: "Lite" },
      { value: "full", label: "Full" },
      { value: "ultra", label: "Ultra" },
    ]);
  });

  it("has imagePullPolicy as select with correct options", () => {
    const schema = getConfigSchema();
    const field = schema.fields.find((f: ConfigFieldSchema) => f.key === "imagePullPolicy");
    expect(field).toBeDefined();
    expect(field!.type).toBe("select");
    expect(field!.options).toEqual([
      { label: "IfNotPresent", value: "IfNotPresent" },
      { label: "Always", value: "Always" },
      { label: "Never", value: "Never" },
    ]);
  });
});
