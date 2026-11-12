/** Connection-first custom model configuration; credentials use the existing model secret store. */
import * as path from 'node:path';
import * as fs from 'node:fs';
import { randomUUID } from 'node:crypto';
import { ModelStore, readModelConfig, writeJsonAtomic, type ModelEntry, type SecretCodec } from './modelStore';

type Data = Record<string, any>;
export const NODE_CONNECTION_FIELDS = [
  'provider',
  'apiUrl',
  'apiKey',
  'apiFormat',
  'encrypted',
  'customHeaders',
  'encryptedHeaders',
  'allowUnauthorized',
  'rawUrl',
  'timeout',
  'maxRetries',
] as const;
export interface NativeGeminiTarget {
  id: string;
  displayName: string;
  model: string | number;
}

export function nativeGeminiTargets(directory: string): NativeGeminiTarget[] {
  try {
    return JSON.parse(fs.readFileSync(path.join(directory, 'native_gemini_catalog.json'), 'utf8'));
  } catch (error) {
    if ((error as NodeJS.ErrnoException).code === 'ENOENT') return [];
    throw error;
  }
}
export function rememberNativeGeminiCatalog(directory: string, upstream: unknown): void {
  if (!upstream || typeof upstream !== 'object') return;
  const object = upstream as Data;
  const collection = object.models || object.availableModels || object.available_models;
  if (!collection || typeof collection !== 'object') return;
  const entries = Array.isArray(collection)
    ? collection.map((m: Data) => [m.name?.replace(/^models\//, ''), m])
    : Object.entries(collection);
  const targets: NativeGeminiTarget[] = [];
  for (const [id, entry] of entries as [string, Data][]) {
    if (!id || !entry?.model || !/gemini/i.test(`${id} ${entry.displayName || ''}`) || /^custom-/.test(id)) continue;
    targets.push({ id, displayName: entry.displayName || id, model: entry.model });
  }
  if (targets.length) writeJsonAtomic(path.join(directory, 'native_gemini_catalog.json'), targets);
}

/** Resolve node credentials at request time so editing a node updates every binding. */
export function readRuntimeModelConfig(filename: string): ModelEntry[] {
  const models = readModelConfig(filename);
  const nodes = readModelConfig(path.join(path.dirname(filename), 'custom_nodes.json'));
  const targets = nativeGeminiTargets(path.dirname(filename));
  const result = models.map((model) => {
    const node = model.nodeId ? nodes.find((n) => n.name === model.nodeId) : undefined;
    if (model.nodeId && !node) return { ...model, enabled: false, toolProxy: false };
    const effective = { ...model };
    if (node) {
      for (const key of NODE_CONNECTION_FIELDS) {
        delete effective[key];
        if (node[key] !== undefined) (effective as Data)[key] = node[key];
      }
      effective.enabled = node.enabled !== false && model.enabled !== false;
      effective.toolProxy = false;
    }
    if (effective.nativeOverrideId) {
      const target = targets.find((t) => t.id === effective.nativeOverrideId);
      if (!target) effective.enabled = false;
      else effective.nativeOverrideModel = target.model;
    }
    return effective;
  });
  for (const node of nodes.filter((n) => n.toolProxy === true))
    result.push({ ...node, nodeToolSource: true, externalModelName: '__tool_proxy__' });
  return result;
}

export class CustomNodeStore {
  readonly store: ModelStore;
  constructor(
    readonly directory: string,
    codec: SecretCodec,
    private models: ModelStore,
  ) {
    this.store = new ModelStore(path.join(directory, 'custom_nodes.json'), codec);
  }
  list(): ModelEntry[] {
    return this.store.list();
  }
  save(input: Data): Data {
    const existing = input.name ? this.store.read().find((n) => n.name === input.name) : undefined;
    if (input.name && !existing) throw new Error('Unknown custom node.');
    if (typeof input.displayName !== 'string' || !input.displayName.trim() || input.displayName.length > 200)
      throw new Error('Use a node name of 1–200 characters.');
    if (input.toolProxy === true) {
      const others = [
        ...this.store.read().filter((n) => n.name !== input.name),
        ...this.models.read().filter((m) => !m.nodeId),
      ];
      if (others.some((n) => n.toolProxy === true))
        throw new Error('Disable the existing upstream API proxy source before selecting another node.');
    }
    const node: ModelEntry = {
      ...(existing || {}),
      name: input.name || `models/node-${randomUUID()}`,
      displayName: input.displayName.trim(),
      provider: input.provider,
      apiUrl: input.apiUrl,
      apiKey: input.apiKey || '',
      externalModelName: '__connection__',
      apiFormat: input.apiFormat,
      enabled: input.enabled !== false,
      toolProxy: input.toolProxy === true,
      customHeaders: input.customHeaders,
      allowUnauthorized: input.allowUnauthorized === true,
    };
    if (node.enabled !== false) {
      const targets = new Set(
        this.models
          .read()
          .filter((model) => model.nodeId === node.name && model.enabled !== false)
          .map((model) => model.nativeOverrideId)
          .filter(Boolean),
      );
      if (
        readRuntimeModelConfig(this.models.filename).some(
          (model) => model.nodeId !== node.name && model.enabled !== false && targets.has(model.nativeOverrideId),
        )
      )
        throw new Error(
          'Another enabled node already overrides one of these Gemini models. Disable that override before enabling this node.',
        );
    }
    this.store.save(node);
    return { success: true, name: node.name };
  }
  delete(name: string): Data {
    if (this.models.read().some((m) => m.nodeId === name))
      throw new Error('Remove the models under this node before deleting it.');
    this.store.delete(name);
    return { success: true };
  }
  saveBinding(input: Data): Data {
    const node = this.store.list().find((n) => n.name === input.nodeId);
    if (!node) throw new Error('Save a custom node first.');
    if (typeof input.externalModelName !== 'string' || !input.externalModelName.trim())
      throw new Error('An upstream model ID is required.');
    const existing = input.name ? this.models.read().find((m) => m.name === input.name) : undefined;
    if (input.name && !existing) throw new Error('Unknown node model.');
    if (existing && existing.nodeId !== node.name)
      throw new Error('Move models by adding a new binding to the destination node.');
    let target: NativeGeminiTarget | undefined;
    if (input.nativeOverrideId) {
      target = nativeGeminiTargets(this.directory).find((t) => t.id === input.nativeOverrideId);
      if (!target)
        throw new Error(
          'Select a Gemini model from the current native catalog. Open the native model picker to refresh it.',
        );
      if (
        input.enabled !== false &&
        readRuntimeModelConfig(this.models.filename).some(
          (m) => m.name !== input.name && m.enabled !== false && m.nativeOverrideId === target!.id,
        )
      )
        throw new Error('This Gemini model already has an enabled override.');
    }
    const binding: ModelEntry = {
      ...existing,
      name: input.name || `models/binding-${randomUUID()}`,
      displayName: input.displayName?.trim() || input.externalModelName.trim(),
      externalModelName: input.externalModelName.trim(),
      nodeId: node.name,
      nativeOverrideId: target?.id,
      enabled: input.enabled !== false,
      toolProxy: false,
      reasoningEffort: input.reasoningEffort || undefined,
    };
    // Bindings resolve their credentials from the node at request time.
    for (const key of NODE_CONNECTION_FIELDS) delete binding[key];
    Object.assign(binding, { provider: node.provider, apiUrl: node.apiUrl, apiKey: '', apiFormat: node.apiFormat });
    this.models.save(binding);
    return { success: true, name: binding.name };
  }
}
