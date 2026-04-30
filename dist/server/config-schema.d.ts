interface ConfigFieldOption {
    label: string;
    value: string;
    group?: string;
}
type ConfigFieldSchema = {
    type: "text";
    key: string;
    label: string;
    hint?: string;
    default?: unknown;
    meta?: Record<string, unknown>;
} | {
    type: "number";
    key: string;
    label: string;
    hint?: string;
    default?: unknown;
    meta?: Record<string, unknown>;
} | {
    type: "toggle";
    key: string;
    label: string;
    hint?: string;
    default?: unknown;
    meta?: Record<string, unknown>;
} | {
    type: "select";
    key: string;
    label: string;
    hint?: string;
    options: ConfigFieldOption[];
    default?: unknown;
    meta?: Record<string, unknown>;
} | {
    type: "textarea";
    key: string;
    label: string;
    hint?: string;
    default?: unknown;
    meta?: Record<string, unknown>;
} | {
    type: "combobox";
    key: string;
    label: string;
    hint?: string;
    options?: ConfigFieldOption[];
    default?: unknown;
    meta?: Record<string, unknown>;
};
interface AdapterConfigSchema {
    fields: ConfigFieldSchema[];
}
export declare function getConfigSchema(): AdapterConfigSchema;
export {};
//# sourceMappingURL=config-schema.d.ts.map