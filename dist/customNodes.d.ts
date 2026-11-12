import { ModelStore, type ModelEntry, type SecretCodec } from './modelStore';
type Data = Record<string, any>;
export declare const NODE_CONNECTION_FIELDS: readonly ["provider", "apiUrl", "apiKey", "apiFormat", "encrypted", "customHeaders", "encryptedHeaders", "allowUnauthorized", "rawUrl", "timeout", "maxRetries"];
export interface NativeGeminiTarget {
    id: string;
    displayName: string;
    model: string | number;
}
export declare function nativeGeminiTargets(directory: string): NativeGeminiTarget[];
export declare function rememberNativeGeminiCatalog(directory: string, upstream: unknown): void;
/** Resolve node credentials at request time so editing a node updates every binding. */
export declare function readRuntimeModelConfig(filename: string): ModelEntry[];
export declare class CustomNodeStore {
    readonly directory: string;
    private models;
    readonly store: ModelStore;
    constructor(directory: string, codec: SecretCodec, models: ModelStore);
    list(): ModelEntry[];
    save(input: Data): Data;
    delete(name: string): Data;
    saveBinding(input: Data): Data;
}
export {};
//# sourceMappingURL=customNodes.d.ts.map