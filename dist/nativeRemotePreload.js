"use strict";
Object.defineProperty(exports, "__esModule", { value: true });
exports.installNativeRemoteClient = installNativeRemoteClient;
/** Runs before the vendor script; extends its client and leaves all rendering to the vendor. */
const electron_1 = require("electron");
function installNativeRemoteClient() {
    const root = globalThis;
    const bridge = root.antigravitySshNative;
    if (!bridge || root.__AGY_SSH_NATIVE_INSTALLED__)
        return;
    root.__AGY_SSH_NATIVE_INSTALLED__ = true;
    const previous = root.__JETSKI_LS_CLIENT_DECORATOR__;
    root.__JETSKI_LS_CLIENT_DECORATOR__ = (original) => {
        const client = previous ? previous(original) : original;
        // 2.29.1 ships protobuf descriptors as globals in its classic main.js script.
        // Use the vendor constructor for scalar defaults, maps, oneofs and nested messages.
        const service = root.Fi;
        const create = root.$b;
        if (service?.typeName !== 'exa.language_server_pb.LanguageServerService' ||
            !Array.isArray(service.methods) ||
            typeof create !== 'function') {
            console.error('[SSH] The native language-service schema changed. SSH integration cannot attach to this desktop version.');
            return client;
        }
        const methods = new Map(service.methods.map((m) => [m.localName, m]));
        const initializer = (value) => {
            if (Array.isArray(value))
                return value.map(initializer);
            if (!value || typeof value !== 'object' || value instanceof Uint8Array)
                return value;
            return Object.fromEntries(Object.entries(value)
                .filter(([key]) => key !== '$typeName')
                .map(([key, item]) => [key, initializer(item)]));
        };
        // A spread protobuf message keeps $typeName. Buf then treats it as already
        // initialized and skips defaults on newly merged child messages.
        const normalize = (name, value) => create(methods.get(name).output, initializer(value));
        let summaryRevision = 0;
        const invoke = async (method, input = {}) => {
            const response = await bridge.call(method, input);
            if (!response.success) {
                // The native Delete hook removes its row optimistically before deleting
                // history. Republish the unchanged remote summary if deletion failed.
                if (method === 'deleteCascadeTrajectory')
                    summaryRevision++;
                throw new Error(response.error);
            }
            return response.result;
        };
        const snapshot = async () => {
            const response = await bridge.snapshot();
            if (!response.success)
                throw new Error(response.error);
            return response.result;
        };
        const pause = (signal) => new Promise((resolve) => {
            if (signal?.aborted)
                return resolve();
            const done = () => {
                clearTimeout(timer);
                signal?.removeEventListener('abort', done);
                resolve();
            };
            const timer = setTimeout(done, 400);
            signal?.addEventListener('abort', done, { once: true });
        });
        const key = (value) => JSON.stringify(value, (_key, item) => (typeof item === 'bigint' ? item.toString() : item));
        async function* merged(name, input, options) {
            const controller = new AbortController();
            const abort = () => controller.abort();
            options?.signal?.addEventListener('abort', abort, { once: true });
            if (options?.signal?.aborted)
                controller.abort();
            const queue = [];
            let wake;
            let failure;
            const put = (value) => {
                queue.push(value);
                wake?.();
            };
            const localIds = new Set();
            let remoteIds = new Set();
            const local = (async () => {
                try {
                    for await (const value of client[name](input, { ...options, signal: controller.signal })) {
                        if (name === 'projectUpdatesStream' && value.update?.case === 'projectList') {
                            localIds.clear();
                            value.update.value.projectIds.forEach((id) => localIds.add(id));
                            put(normalize(name, {
                                update: { case: 'projectList', value: { projectIds: [...new Set([...localIds, ...remoteIds])] } },
                            }));
                        }
                        else
                            put(value);
                    }
                }
                catch (error) {
                    if (!controller.signal.aborted) {
                        failure = error;
                        wake?.();
                    }
                }
            })();
            const remote = (async () => {
                let last = '';
                let oldSummaries = {};
                let seenSummaryRevision = summaryRevision;
                try {
                    while (!controller.signal.aborted) {
                        const state = await snapshot();
                        if (name === 'projectUpdatesStream') {
                            const current = key(state.projects);
                            if (last !== current) {
                                remoteIds = new Set(state.projects.map((p) => p.id));
                                put(normalize(name, {
                                    update: { case: 'projectList', value: { projectIds: [...new Set([...localIds, ...remoteIds])] } },
                                }));
                                for (const id of remoteIds)
                                    put(normalize(name, { update: { case: 'projectUpdatedId', value: id } }));
                                last = current;
                            }
                        }
                        else {
                            const updates = {};
                            for (const [id, summary] of Object.entries(state.summaries))
                                if (seenSummaryRevision !== summaryRevision || key(summary) !== key(oldSummaries[id]))
                                    updates[id] = summary;
                            const deletes = Object.keys(oldSummaries).filter((id) => !state.summaries[id]);
                            if (Object.keys(updates).length || deletes.length)
                                put(normalize(name, { updates, deletes }));
                            oldSummaries = state.summaries;
                            seenSummaryRevision = summaryRevision;
                        }
                        await pause(controller.signal);
                    }
                }
                catch (error) {
                    if (!controller.signal.aborted) {
                        failure = error;
                        wake?.();
                    }
                }
            })();
            try {
                while (!controller.signal.aborted) {
                    if (failure)
                        throw failure;
                    if (queue.length)
                        yield queue.shift();
                    else
                        await new Promise((resolve) => {
                            const done = () => {
                                controller.signal.removeEventListener('abort', done);
                                wake = undefined;
                                resolve();
                            };
                            wake = done;
                            controller.signal.addEventListener('abort', done, { once: true });
                        });
                }
            }
            finally {
                controller.abort();
                options?.signal?.removeEventListener('abort', abort);
                void local;
                void remote;
            }
        }
        async function* stream(name, input, options) {
            const first = await invoke(name, input);
            if (!first.handled) {
                yield* client[name](input, options);
                return;
            }
            let value = first;
            let last = '';
            while (!options?.signal?.aborted) {
                const current = key(value.value);
                if (current !== last) {
                    yield normalize(name, value.value);
                    last = current;
                }
                await pause(options?.signal);
                if (options?.signal?.aborted)
                    break;
                value = await invoke(name, input);
                if (!value.handled)
                    throw new Error('Remote conversation was removed.');
            }
        }
        const mergedUnary = new Set(['listProjects', 'readProjects', 'getAllCascadeTrajectories', 'getRepoInfos']);
        const output = { ...client };
        for (const [name, descriptor] of methods) {
            if (['projectUpdatesStream', 'jetboxSubscribeToSummaries'].includes(name))
                output[name] = (input, options) => merged(name, input, options);
            else if (descriptor.methodKind === 'server_streaming')
                output[name] = (input, options) => stream(name, input, options);
            else if (descriptor.methodKind === 'unary')
                output[name] = async (input, options) => {
                    if (options?.signal?.aborted)
                        throw new DOMException('Aborted', 'AbortError');
                    const isModels = name === 'getCascadeModelConfigs' || name === 'getCascadeModelConfigData';
                    const before = isModels || name === 'getUserStatus' ? await client[name](input, options) : undefined;
                    if (before)
                        await bridge.catalog(name === 'getUserStatus' ? before.userStatus?.cascadeModelConfigData || {} : before);
                    if (name === 'getUserStatus' || isModels)
                        return before;
                    const remote = await invoke(name, input);
                    if (!remote.handled) {
                        const local = await client[name](input, options);
                        if (name === 'getAvailableModels')
                            await bridge.catalog(local);
                        return local;
                    }
                    if (mergedUnary.has(name) || remote.input) {
                        const localInput = remote.input || input;
                        const needsLocal = !remote.input ||
                            (localInput.ids?.length ?? localInput.repoUris?.length ?? localInput.cascadeIds?.length ?? 1) > 0;
                        const local = before || (needsLocal ? await client[name](localInput, options) : {});
                        const result = { ...local, ...remote.value };
                        if (name === 'listProjects')
                            result.projectIds = [...new Set([...(local.projectIds || []), ...remote.value.projectIds])];
                        if (name === 'readProjects') {
                            result.projects = [...(local.projects || []), ...remote.value.projects];
                            result.notFoundIds = local.notFoundIds || [];
                            result.errorMessages = local.errorMessages || {};
                        }
                        if (name === 'getAllCascadeTrajectories')
                            result.trajectorySummaries = { ...local.trajectorySummaries, ...remote.value.trajectorySummaries };
                        if (name === 'getRepoInfos')
                            result.repos = [...(local.repos || []), ...remote.value.repos];
                        return normalize(name, result);
                    }
                    return normalize(name, remote.value);
                };
        }
        // Extend the native components, including their keyboard navigation and Redux
        // selection path. Never create a second project list or conversation surface.
        if (!root.__AGY_PROJECT_UI_INSTALLED__ &&
            root.A &&
            [
                'Ql',
                'LG',
                'cS',
                'gm',
                'Ul',
                'Kob',
                'hu',
                'hCa',
                'iCa',
                'iu',
                'eS',
                'ytc',
                'cna',
                'lCa',
                't4',
                'FBb',
                '$Y',
            ].every((name) => typeof root[name] === 'function')) {
            root.__AGY_PROJECT_UI_INSTALLED__ = true;
            const React = root.A;
            const select = root.Ql;
            const selectors = { models: root.hu, sorts: root.hCa, custom: root.iCa, loading: root.gCa, status: root.mu };
            const nativeCatalog = root.iu;
            const Picker = root.ytc;
            const Settings = root.FBb;
            const creationActions = root.t4;
            // The native normalizer coerces every resource URI to file://. Preserve
            // SSH authority so project stat, file mentions and workspace selection
            // keep reaching the remote file adapter instead of the local language server.
            const normalizeResourceUri = root.sg;
            if (typeof normalizeResourceUri === 'function')
                root.sg = (uri) => (typeof uri === 'string' ? uri.startsWith('agy-ssh://') : uri?.scheme === 'agy-ssh')
                    ? uri
                    : normalizeResourceUri(uri);
            const EnvironmentSelector = root.Lsc;
            if (typeof EnvironmentSelector === 'function')
                root.Lsc = (props) => {
                    const resources = props.resourceManagementProps?.availableResources || [];
                    const raw = props.resourceManagementProps?.rawResources || [];
                    const uri = [
                        ...resources.map((resource) => resource.uri),
                        ...raw.map((resource) => resource.type?.case === 'gitFolder' ? resource.type.value?.folderUri : resource.type?.value),
                    ].find((value) => typeof value === 'string' && value.startsWith('agy-ssh://'));
                    if (!uri)
                        return React.createElement(EnvironmentSelector, props);
                    return React.createElement('span', {
                        className: 'inline-flex items-center gap-1 text-muted-foreground',
                        title: uri,
                        'data-testid': 'ssh-project-environment',
                    }, React.createElement(root.lt, { name: 'terminal', size: 14 }), 'SSH');
                };
            const Context = React.createContext(undefined);
            let hookConversationId;
            const cache = new Map();
            const picks = new Map();
            const listeners = new Set();
            let revision = 0;
            let pendingProjectId;
            let metadata = [];
            let refreshAccount;
            const changed = () => {
                revision++;
                listeners.forEach((listener) => listener());
            };
            const subscribe = (listener) => {
                listeners.add(listener);
                return () => listeners.delete(listener);
            };
            const currentRevision = () => revision;
            const empty = [];
            const loadMetadata = async () => {
                try {
                    metadata = await bridge.models();
                    changed();
                }
                catch (error) {
                    console.error('[Models] Unable to read endpoint model metadata', error);
                }
            };
            void loadMetadata();
            root.addEventListener('agy-models-changed', () => {
                void loadMetadata();
                void refreshAccount?.();
            });
            root.addEventListener('agy-ssh-project-saved', (event) => {
                try {
                    const detail = JSON.parse(event.detail);
                    if (detail.created && typeof detail.projectId === 'string')
                        pendingProjectId = detail.projectId;
                }
                catch {
                    /* Ignore malformed UI events. */
                }
                cache.clear();
                changed();
            });
            const loadCatalog = (key, projectId, conversationId, refresh = false) => {
                const previous = cache.get(key);
                if (previous?.pending)
                    return previous.pending;
                if (previous && !refresh)
                    return Promise.resolve();
                const entry = { loading: true, configs: previous?.configs || empty };
                cache.set(key, entry);
                entry.pending = invoke('getScopedModelCatalog', { projectId, conversationId })
                    .then((result) => {
                    if (!result.handled)
                        throw new Error('SSH project no longer exists.');
                    const value = normalize('getCascadeModelConfigs', result.value);
                    entry.configs = value.clientModelConfigs || empty;
                    entry.error = entry.configs.length ? '' : 'This SSH agent did not offer any models.';
                })
                    .catch((error) => {
                    entry.configs = empty;
                    entry.error = String(error.message || error);
                })
                    .finally(() => {
                    entry.loading = false;
                    entry.pending = undefined;
                    changed();
                });
                changed();
                return entry.pending;
            };
            function useCatalog(conversationId) {
                const context = React.useContext(Context);
                const fallbackProject = root.LG();
                const conversationProject = root.cS(conversationId);
                const pendingProject = select(React.useCallback((state) => conversationId ? state.newConvoWorkspace?.projectIdsByCascadeId?.[conversationId] : undefined, [conversationId]));
                const projectId = conversationId ? conversationProject || pendingProject : fallbackProject;
                const service = root.gm();
                const auth = root.rd?.()?.core?.authService;
                React.useEffect(() => {
                    if (auth)
                        refreshAccount = () => auth.refreshUserStatus();
                }, [auth]);
                const provider = React.useMemo(() => (projectId ? service?.getProjectStateProvider(projectId) : undefined), [projectId, service]);
                const project = root.Ul(provider)?.project;
                const ssh = (project?.projectResources?.resources || []).some((resource) => resource.type?.case === 'folderUri' && String(resource.type.value).startsWith('agy-ssh:'));
                const cloud = root.Kob(conversationId, projectId) === 2;
                const local = select(nativeCatalog);
                const version = React.useSyncExternalStore(subscribe, currentRevision, currentRevision);
                const key = `${projectId || 'local'}:${conversationId || 'new'}`;
                React.useEffect(() => {
                    if (!ssh)
                        return;
                    void loadCatalog(key, projectId, conversationId);
                    const timer = setInterval(() => {
                        void loadCatalog(key, projectId, conversationId, true);
                    }, 30000);
                    return () => clearInterval(timer);
                }, [key, projectId, conversationId, ssh, version]);
                const own = React.useMemo(() => {
                    if (ssh) {
                        const entry = cache.get(key);
                        const configs = entry?.configs || empty;
                        const options = root.cna(configs);
                        return {
                            visibleModelConfigs: options,
                            customModels: empty,
                            modelSorts: [{ name: 'Recommended', groups: [{ groupName: project?.name || 'SSH', options }] }],
                            configs,
                            loading: !entry || entry.loading,
                            error: entry?.error,
                            ssh: true,
                            key,
                            retry: () => loadCatalog(key, projectId, conversationId, true),
                        };
                    }
                    const enabled = new Set(metadata
                        .filter((model) => model.enabled !== false && model.nodeEnabled !== false && !model.nativeOverrideId)
                        .map((model) => model.nativeCatalogId));
                    const allowed = (model) => !/^ssh-/.test(model.modelId || '') &&
                        (!/^custom-/.test(model.modelId || '') || (!cloud && enabled.has(model.modelId)));
                    const models = local.visibleModelConfigs.filter(allowed);
                    const sorts = local.modelSorts
                        .map((sort) => ({
                        ...sort,
                        groups: sort.groups
                            .map((group) => ({ ...group, options: group.options.filter(allowed) }))
                            .filter((group) => group.options.length),
                    }))
                        .filter((sort) => sort.groups.length);
                    return {
                        visibleModelConfigs: models,
                        customModels: cloud ? empty : local.customModels,
                        modelSorts: sorts,
                        key,
                        loading: false,
                        ssh: false,
                    };
                }, [local, cloud, ssh, key, version, projectId, conversationId]);
                return context && context.conversationId === conversationId ? context.catalog : own;
            }
            root.Ql = Object.assign((selector, ...args) => {
                const field = selector === selectors.models
                    ? 'visibleModelConfigs'
                    : selector === selectors.sorts
                        ? 'modelSorts'
                        : selector === selectors.custom
                            ? 'customModels'
                            : selector === selectors.loading
                                ? 'loading'
                                : undefined;
                if (!field)
                    return select(selector, ...args);
                const context = React.useContext(Context);
                const catalog = useCatalog(context?.conversationId ?? hookConversationId);
                const originalValue = select(selector, ...args);
                return field === 'loading' && !catalog.ssh ? originalValue : catalog[field];
            }, { withTypes: () => root.Ql });
            const bestOfN = root.ksc;
            if (typeof bestOfN === 'function')
                root.ksc = (storage, model, conversationId) => {
                    // The native hook executes synchronously; carry its explicit conversation
                    // into its model selectors and restore it before React renders another pane.
                    const previous = hookConversationId;
                    hookConversationId = conversationId;
                    try {
                        return bestOfN(storage, model, conversationId);
                    }
                    finally {
                        hookConversationId = previous;
                    }
                };
            root.eS = (conversationId) => {
                const catalog = useCatalog(conversationId);
                const selection = select(React.useCallback((state) => state.modelSelection || { default: {} }, []));
                const selected = (conversationId ? selection.byConversation?.[conversationId] : undefined) ?? selection.default;
                const all = [...catalog.visibleModelConfigs, ...catalog.customModels];
                const valid = all.find((model) => model.value === selected?.model && (!model.isCustom || model.label === selected?.customLabel));
                const remembered = picks.get(catalog.key);
                const result = valid ||
                    all.find((model) => model.value === remembered?.value && model.label === remembered?.label) ||
                    catalog.modelSorts[0]?.groups[0]?.options[0] ||
                    all[0];
                React.useEffect(() => {
                    if (result)
                        picks.set(catalog.key, result);
                }, [catalog.key, result]);
                return result;
            };
            root.ytc = (props) => {
                const openProject = root.$Y();
                const projectList = root.Ul(root.gm().projectListStateProvider);
                const catalog = useCatalog(props.conversationId);
                React.useEffect(() => {
                    if (pendingProjectId && projectList?.projectIds?.includes(pendingProjectId)) {
                        const id = pendingProjectId;
                        pendingProjectId = undefined;
                        openProject(id);
                    }
                }, [projectList, catalog, openProject]);
                const value = React.useMemo(() => ({ conversationId: props.conversationId, catalog }), [props.conversationId, catalog]);
                const message = catalog.ssh && (catalog.loading || catalog.error)
                    ? React.createElement('div', { role: 'status', className: 'px-3 py-2 text-xs text-muted-foreground' }, catalog.loading ? 'Loading SSH models…' : catalog.error, !catalog.loading &&
                        React.createElement('button', { type: 'button', onClick: catalog.retry, className: 'ml-2 underline' }, 'Retry'))
                    : null;
                return React.createElement(Context.Provider, { value }, React.createElement(Picker, {
                    ...props,
                    footerExtra: React.createElement(React.Fragment, null, message, props.footerExtra),
                }));
            };
            // Preserve only explicitly configured endpoint entries through the native
            // Gemini-only product filter. Native Google account filtering stays intact.
            const filterStatus = root.fna;
            if (typeof filterStatus === 'function')
                root.fna = (status, ...args) => {
                    const result = filterStatus(status, ...args);
                    if (!result?.cascadeModelConfigData || result === status)
                        return result;
                    const configs = result.cascadeModelConfigData.clientModelConfigs || [];
                    const additions = (status.cascadeModelConfigData?.clientModelConfigs || []).filter((model) => /^custom-/.test(model.modelId || '') && !configs.some((entry) => entry.modelId === model.modelId));
                    return {
                        ...result,
                        cascadeModelConfigData: {
                            ...result.cascadeModelConfigData,
                            clientModelConfigs: [...configs, ...additions],
                        },
                    };
                };
            const editProject = (id = '') => {
                root.document?.querySelector('[data-testid="project-selector-trigger"][aria-expanded="true"]')?.click();
                root.dispatchEvent(new CustomEvent('agy-ssh-project-settings', { detail: id }));
            };
            root.t4 = (options) => [
                ...creationActions(options),
                {
                    label: 'New SSH Project',
                    tooltipText: 'Connect a remote directory through SSH ACP.',
                    disabled: false,
                    icon: root.fpc,
                    type: 'DEFAULT',
                    onClick: () => editProject(),
                },
            ];
            const SidebarCreate = root.UFc;
            if (typeof SidebarCreate === 'function')
                root.UFc = (props) => {
                    const tree = SidebarCreate(props);
                    if (!React.isValidElement(tree))
                        return tree;
                    const sshItem = React.createElement(root.VE, { key: 'ssh-project', asChild: true }, React.createElement(root.RE, { onClick: () => editProject(), icon: root.C5('terminal') }, 'New SSH Project'));
                    if (tree.type === root.aB)
                        return React.createElement(root.SE, { placement: 'bottom-start', offset: 4, openOnMouseDown: true }, React.createElement(root.TE, { asChild: true }, React.cloneElement(tree, { onClick: undefined })), React.createElement(root.UE, { className: '!w-max max-w-[80vw] py-0.5 flex flex-col origin-top-left overflow-hidden' }, React.createElement(root.VE, { asChild: true }, React.createElement(root.RE, { onClick: tree.props.onClick, icon: root.C5('folder') }, 'New Project')), sshItem));
                    if (tree.type !== root.SE)
                        return tree;
                    const hasSsh = (node) => node === 'New SSH Project' ||
                        (React.isValidElement(node) && React.Children.toArray(node.props.children).some(hasSsh));
                    if (hasSsh(tree))
                        return tree;
                    return React.cloneElement(tree, {}, ...React.Children.toArray(tree.props.children).map((child) => React.isValidElement(child) && child.type === root.UE
                        ? React.cloneElement(child, {}, ...React.Children.toArray(child.props.children), sshItem)
                        : child));
                };
            const ProjectRow = root.jGc;
            if (typeof ProjectRow === 'function')
                root.jGc = (props) => {
                    const tree = ProjectRow(props);
                    const service = root.gm();
                    const project = root.Ul(service.getProjectStateProvider(props.projectId))?.project;
                    const ssh = project?.projectResources?.resources?.some((resource) => String(resource.type?.value).startsWith('agy-ssh:'));
                    if (!ssh || !React.isValidElement(tree))
                        return tree;
                    const action = {
                        id: 'ssh-connection',
                        label: 'Edit SSH connection',
                        icon: root.C5('terminal'),
                        testId: 'project-ssh-connection',
                        onClick: () => editProject(props.projectId),
                    };
                    const extra = tree.props.extraActions;
                    return React.cloneElement(tree, {
                        contextMenuItems: [...(tree.props.contextMenuItems || []), action],
                        extraActions: React.cloneElement(extra, {}, ...React.Children.toArray(extra.props.children).map((child) => React.isValidElement(child) && child.type === root.YFc
                            ? React.cloneElement(child, { renameLabel: action.label, onRenameClick: action.onClick })
                            : child)),
                    });
                };
            root.FBb = () => {
                const tree = Settings();
                const projectId = select((state) => state.settingsScreen?.contextProjectId);
                const service = root.gm();
                const project = root.Ul(React.useMemo(() => (projectId ? service?.getProjectStateProvider(projectId) : undefined), [projectId, service]))?.project;
                const uri = (project?.projectResources?.resources || [])
                    .map((resource) => resource.type?.value)
                    .find((value) => typeof value === 'string' && value.startsWith('agy-ssh:'));
                if (!uri)
                    return tree;
                const rewrite = (node) => {
                    if (!React.isValidElement(node))
                        return node;
                    if (node.type === root.Ayb ||
                        ['Customizations', 'Terminal', 'Google3 Configuration'].includes(node.props.title))
                        return null;
                    if (['Folders', 'Resources'].includes(node.props.title))
                        return React.createElement(root.YP, { key: node.key, title: 'SSH connection' }, React.createElement(root.WP, null, React.createElement('p', { className: 'text-xs text-muted-foreground break-all' }, decodeURI(uri)), React.createElement('p', { className: 'text-xs text-muted-foreground' }, 'Models, tools and permissions come from the remote agent.'), React.createElement('button', {
                            type: 'button',
                            className: 'px-3 py-1.5 border border-border rounded text-xs',
                            onClick: () => editProject(projectId),
                        }, 'Edit SSH connection')));
                    return React.cloneElement(node, node.type === root.UP ? { description: 'Manage this SSH project and its conversations.' } : {}, ...React.Children.toArray(node.props.children).map(rewrite));
                };
                return rewrite(tree);
            };
        }
        return output;
    };
}
electron_1.contextBridge.exposeInMainWorld('antigravitySshNative', {
    call: (method, input) => electron_1.ipcRenderer.invoke('remote-acp:native', method, input),
    models: async () => (await electron_1.ipcRenderer.invoke('storage:get-custom-models')).map((model) => ({
        nativeCatalogId: model.nativeCatalogId,
        nativeOverrideId: model.nativeOverrideId,
        enabled: model.enabled,
        nodeEnabled: model.nodeEnabled,
    })),
    snapshot: () => electron_1.ipcRenderer.invoke('remote-acp:native-snapshot'),
    catalog: (value) => electron_1.ipcRenderer.invoke('remote-acp:native-catalog', '', value),
});
electron_1.contextBridge.executeInMainWorld({ func: installNativeRemoteClient });
//# sourceMappingURL=nativeRemotePreload.js.map