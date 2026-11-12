import type { ModelEntry } from './modelStore';
import { nativeGeminiTargets } from './customNodes';
/** Transport-independent addon actions shared by Electron IPC and the local IDE manager. */
import type { createModelManager } from './modelManagement';
import { getDiagnostics, clearDiagnostics, getRecentModelNames } from './diagnostics';
import * as path from 'node:path';
import { allocateModelIdentities } from './modelIdentity';
import { nativeCatalogCapabilities, nativeModelExperiments, type CustomModelConfig } from './proxy/modelUtils';
import { getContextInformation } from './chatContext';

export function createModelActions(
  manager: ReturnType<typeof createModelManager>,
): Record<string, (...args: unknown[]) => unknown> {
  return {
    'storage:get-custom-nodes': () => manager.nodes.list(),
    'storage:save-custom-node': (input) => manager.nodes.save(input as any),
    'storage:delete-custom-node': (name: string) => manager.nodes.delete(name),
    'storage:save-node-model': (input) => manager.nodes.saveBinding(input as any),
    'storage:get-native-gemini-models': () => nativeGeminiTargets(path.dirname(manager.store.filename)),
    'storage:discover-node-models': (name: string) => {
      const node = manager.nodes.store.read().find((n) => n.name === name);
      if (!node) throw new Error('Unknown custom node.');
      return manager.discoverModels({ ...node, name: node.name } as any);
    },
    'storage:get-custom-models': () => {
      const nodes = manager.nodes.list();
      const models: ModelEntry[] = manager.store.list().map((model) => ({
        ...model,
        nodeEnabled: !model.nodeId || nodes.some((node) => node.name === model.nodeId && node.enabled !== false),
      }));
      // The proxy and both management transports share the same persistent
      // native identity registry. Do not regenerate enum IDs in the renderer.
      let identities: ReturnType<typeof allocateModelIdentities> = [];
      let identityError: string | undefined;
      try {
        identities = allocateModelIdentities(
          path.dirname(manager.store.filename),
          models.filter((model) => !model.nativeOverrideId),
        );
      } catch (error) {
        // Listing/editing saved providers remains possible when the identity
        // registry is unavailable; native injection must fail closed.
        identityError =
          (error instanceof Error ? error.message.replace(/[\u0000-\u001f\u007f]/g, ' ').slice(0, 512) : '') ||
          'Native model identity metadata is unavailable. Retry after resolving the local metadata error.';
      }
      const byName = new Map(identities.map((identity) => [identity.name, identity]));
      const recent = getRecentModelNames();
      const rank = (name: string) => {
        const index = recent.indexOf(name);
        return index < 0 ? recent.length : index;
      };
      return models
        .map((model) => {
          const identity = byName.get(model.name);
          const capabilities = nativeCatalogCapabilities(model as CustomModelConfig);
          return {
            ...model,
            nativeCatalogId: identity?.catalogId,
            nativeModelId: identity?.placeholderId,
            nativePickerAvailable: !!identity?.placeholderId,
            nativeContextWindow: capabilities.maxTokens,
            nativeMaxOutputTokens: capabilities.maxOutputTokens,
            nativeModelExperiments: nativeModelExperiments(model as CustomModelConfig),
            nativePickerError:
              model.nativeOverrideId || identity?.placeholderId
                ? undefined
                : identityError ||
                  'Native model picker capacity reached; direct API alias remains available. 651 supported slots include native reservations and retained deleted models.',
          };
        })
        .sort((a, b) => rank(a.name) - rank(b.name));
    },
    'storage:save-custom-model': manager.saveModel,
    'storage:delete-custom-model': (name: string) => {
      manager.store.delete(name);
      return { success: true };
    },
    'storage:test-model-connection': manager.testModel,
    'storage:test-model-generation': manager.testGeneration,
    'storage:create-effort-variants': manager.createEffortVariants,
    'storage:get-provider-presets': manager.presets,
    'storage:discover-models': manager.discoverModels,
    'storage:discover-local': manager.discoverLocal,
    'storage:export-custom-models': () => manager.store.export(),
    'storage:import-custom-models': (payload) => ({ success: true, count: manager.store.import(payload) }),
    'storage:get-gateway': manager.getGateway,
    'storage:save-gateway': manager.saveGateway,
    'storage:test-gateway': manager.testGateway,
    'storage:import-gateway-models': manager.importGatewayModels,
    'storage:open-gateway-dashboard': manager.openDashboard,
    'storage:google-login': manager.googleLogin,
    'storage:google-login-cancel': manager.googleLoginCancel,
    'storage:google-test-account': manager.googleTestAccount,
    'storage:google-account-prepare': manager.googlePrepareAccount,
    'storage:google-pool-status': manager.googlePoolStatus,
    'storage:google-link-account': manager.googleLinkAccount,
    'storage:google-select-account': manager.googleSelectAccount,
    'storage:google-remove-account': manager.googleRemoveAccount,
    'storage:get-diagnostics': getDiagnostics,
    'storage:clear-diagnostics': clearDiagnostics,
    'storage:get-chat-context-usage': (payload?: unknown) =>
      getContextInformation(payload, path.dirname(manager.store.filename), manager.store.list()),
  };
}
