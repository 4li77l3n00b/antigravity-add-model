"use strict";
var __createBinding = (this && this.__createBinding) || (Object.create ? (function(o, m, k, k2) {
    if (k2 === undefined) k2 = k;
    var desc = Object.getOwnPropertyDescriptor(m, k);
    if (!desc || ("get" in desc ? !m.__esModule : desc.writable || desc.configurable)) {
      desc = { enumerable: true, get: function() { return m[k]; } };
    }
    Object.defineProperty(o, k2, desc);
}) : (function(o, m, k, k2) {
    if (k2 === undefined) k2 = k;
    o[k2] = m[k];
}));
var __setModuleDefault = (this && this.__setModuleDefault) || (Object.create ? (function(o, v) {
    Object.defineProperty(o, "default", { enumerable: true, value: v });
}) : function(o, v) {
    o["default"] = v;
});
var __importStar = (this && this.__importStar) || (function () {
    var ownKeys = function(o) {
        ownKeys = Object.getOwnPropertyNames || function (o) {
            var ar = [];
            for (var k in o) if (Object.prototype.hasOwnProperty.call(o, k)) ar[ar.length] = k;
            return ar;
        };
        return ownKeys(o);
    };
    return function (mod) {
        if (mod && mod.__esModule) return mod;
        var result = {};
        if (mod != null) for (var k = ownKeys(mod), i = 0; i < k.length; i++) if (k[i] !== "default") __createBinding(result, mod, k[i]);
        __setModuleDefault(result, mod);
        return result;
    };
})();
Object.defineProperty(exports, "__esModule", { value: true });
exports.RemoteAcpManager = exports.RemoteAcpClient = exports.REMOTE_ACP_PROFILE = void 0;
exports.validateRemoteAcpConfig = validateRemoteAcpConfig;
exports.remoteSshArgs = remoteSshArgs;
exports.remoteAcpStatus = remoteAcpStatus;
/** ACP over a strict, non-interactive SSH stdio connection. Never replay a prompt. */
const node_child_process_1 = require("node:child_process");
const fs = __importStar(require("node:fs"));
const path = __importStar(require("node:path"));
const node_crypto_1 = require("node:crypto");
const nativeSwarm_1 = require("./nativeSwarm");
exports.REMOTE_ACP_PROFILE = {
    protocolVersion: 1,
    clientInfo: { name: 'antigravity-model-patch-ssh', version: '1.0.0' },
    clientCapabilities: {}, // Remote agy owns files/terminals; this client does not provide local tools.
};
const MAX_MESSAGE = 16 * 1024 * 1024;
const MAX_EVENTS = 256;
const MAX_EVENT_BYTES = 32 * 1024 * 1024;
const launch = (command, args) => (0, node_child_process_1.spawn)(command, args, { stdio: 'pipe', windowsHide: true, shell: false });
function remotePath(value, label) {
    if (typeof value !== 'string' || value.length > 4096 || !value.startsWith('/') || /[\x00-\x1f\x7f]/.test(value))
        throw new Error(`${label} must be an absolute remote Linux path without control characters.`);
    return value;
}
function validateRemoteAcpConfig(value) {
    const input = value;
    if (!input ||
        typeof input !== 'object' ||
        typeof input.host !== 'string' ||
        input.host.length > 255 ||
        !/^(?:[a-zA-Z0-9_][a-zA-Z0-9_.-]*@)?[a-zA-Z0-9_][a-zA-Z0-9_.-]*$/.test(input.host))
        throw new Error('Use an SSH config alias or user@hostname. Configure ports and keys in ~/.ssh/config.');
    if (typeof input.instance !== 'string' || !/^[a-zA-Z0-9][a-zA-Z0-9_-]{0,63}$/.test(input.instance))
        throw new Error('The ACP instance name must contain only letters, digits, underscores or hyphens.');
    return {
        host: input.host,
        command: remotePath(input.command, 'acp-ssh command'),
        instance: input.instance,
        cwd: remotePath(input.cwd, 'Working directory'),
    };
}
/** OpenSSH invokes a remote shell even though the local spawn uses shell:false. */
function remoteSshArgs(config, operation, jobId) {
    const checked = validateRemoteAcpConfig(config);
    if (operation === 'job' && (typeof jobId !== 'string' || !/^[a-zA-Z0-9][a-zA-Z0-9:_-]{0,127}$/.test(jobId)))
        throw new Error('Invalid remote job ID.');
    const quote = (value) => "'" + value.replace(/'/g, "'\\''") + "'";
    return [
        '-T',
        '-o',
        'BatchMode=yes',
        '-o',
        'StrictHostKeyChecking=yes',
        '-o',
        'ConnectTimeout=10',
        '-o',
        'ServerAliveInterval=30',
        '-o',
        'ServerAliveCountMax=3',
        '--',
        checked.host,
        [checked.command, operation, checked.instance, ...(operation === 'job' ? [jobId] : [])].map(quote).join(' '),
    ];
}
class RemoteAcpClient {
    constructor(config, spawnSsh = launch) {
        this.config = config;
        this.spawnSsh = spawnSsh;
        this.buffer = Buffer.alloc(0);
        this.calls = new Map();
        this.requests = new Map();
        this.events = [];
        this.eventBytes = 0;
        this.seq = 0;
        this.serial = 0;
        this.generation = (0, node_crypto_1.randomUUID)();
        this.turns = new Set();
        this.deleting = new Set();
        this.inheritedTurns = new Set();
        this.used = false;
        this.sessions = new Map();
        this.state = 'disconnected';
        this.failure = '';
        this.stderr = '';
        this.config = validateRemoteAcpConfig(config);
    }
    connect() {
        if (this.ready)
            return this.ready;
        if (this.used)
            return Promise.reject(new Error('Create a new controller to reconnect. Inspect remote status first.'));
        this.used = true;
        this.state = 'connecting';
        const process = this.spawnSsh('ssh', remoteSshArgs(this.config, 'connect'));
        this.process = process;
        process.stdout.on('data', (chunk) => this.receive(chunk));
        // stderr is kept separate from protocol data and never written to application logs.
        process.stderr.on('data', (chunk) => {
            this.stderr = (this.stderr + chunk.toString('utf8')).slice(-4096);
        });
        process.stdin.on('error', () => this.disconnect('SSH input closed. Accepted remote tasks may still be running.'));
        process.on('error', () => this.disconnect('Unable to start SSH. Check your SSH installation and saved connection.'));
        process.on('close', (code) => this.disconnect(`SSH disconnected (${code ?? 'signal'}). Check SSH keys, known_hosts and acp-ssh status. Accepted remote tasks may still be running.`));
        this.ready = this.sendRequest('initialize', exports.REMOTE_ACP_PROFILE, 30000)
            .then((result) => {
            if (result?.protocolVersion !== 1)
                throw new Error(`Unsupported ACP protocol version: ${result?.protocolVersion}`);
            this.info = result;
            this.state = 'connected';
            this.record('connected', result);
            return result;
        })
            .catch((error) => {
            this.disconnect(error.message);
            throw error;
        });
        return this.ready;
    }
    snapshot(after = 0) {
        if (!Number.isSafeInteger(after) || after < 0)
            throw new Error('Invalid event cursor.');
        return {
            connectionId: this.generation,
            state: this.state,
            failure: this.failure,
            stderr: this.stderr,
            config: this.config,
            initialize: this.info,
            events: this.events.filter((event) => event.seq > after).map(({ bytes: _bytes, ...event }) => event),
            cursor: this.seq,
            gap: !!this.events.length && after < this.events[0].seq - 1,
            pending: [...this.requests.values()].map(({ id: _id, ...request }) => request),
            activeSessions: [...new Set([...this.turns, ...this.inheritedTurns])],
            sessions: [...this.sessions.values()],
        };
    }
    syncRemoteStatus(status) {
        if (!Array.isArray(status?.active))
            throw new Error('Invalid remote active-task status.');
        this.inheritedTurns = new Set(status.active.map((job) => job.sessionId).filter((id) => typeof id === 'string' && !this.turns.has(id)));
    }
    async deleteSession(sessionId) {
        const capability = this.info?.agentCapabilities?._meta?.['maichart/sessionManagement'];
        const method = '_maichart/session/delete';
        if (capability?.version !== 1 ||
            capability.delete?.method !== method ||
            capability.delete.permanent !== true ||
            capability.delete.idempotent !== true ||
            capability.delete.relatedSessions !== 'reject')
            throw new Error('This SSH agent does not support permanent history deletion. Update and restart the remote ACP adapter first.');
        if (typeof sessionId !== 'string' || !sessionId || sessionId.length > 1024)
            throw new Error('A remote session ID is required.');
        if (this.deleting.has(sessionId))
            throw new Error('This remote session is already being deleted.');
        if (this.turns.has(sessionId) ||
            this.inheritedTurns.has(sessionId) ||
            [...this.calls.values()].some((call) => call.params.sessionId === sessionId) ||
            [...this.requests.values()].some((request) => request.params.sessionId === sessionId))
            throw new Error('Wait for this remote session’s tasks and pending requests to finish before deleting it.');
        this.deleting.add(sessionId);
        try {
            const result = await this.request(method, { sessionId });
            if (result?.deleted !== true ||
                result.sessionId !== sessionId ||
                !Array.isArray(result.deletedSessionIds) ||
                result.deletedSessionIds.length !== 1 ||
                result.deletedSessionIds[0] !== sessionId ||
                typeof result.alreadyDeleted !== 'boolean')
                throw new Error('The remote agent did not confirm permanent deletion. The local session was retained.');
            this.sessions.delete(sessionId);
        }
        finally {
            this.deleting.delete(sessionId);
        }
    }
    async request(method, params) {
        if (this.state !== 'connected')
            throw new Error('Connect to the remote agent first.');
        if (typeof method !== 'string' || !/^[a-zA-Z_][a-zA-Z0-9_/.\-]{0,127}$/.test(method) || method === 'initialize')
            throw new Error('Invalid ACP method. Initialization uses the fixed desktop client profile.');
        if (!params || typeof params !== 'object' || Array.isArray(params))
            throw new Error('ACP params must be an object.');
        const input = params;
        if (this.deleting.has(input.sessionId) && method !== '_maichart/session/delete')
            throw new Error('This remote session is being deleted.');
        if (['session/new', 'session/load', 'session/resume'].includes(method)) {
            remotePath(input.cwd, 'Working directory');
            if (!Array.isArray(input.mcpServers))
                throw new Error('Specify the remote MCP server array explicitly.');
        }
        if (method.startsWith('session/') && method !== 'session/new' && method !== 'session/list') {
            if (typeof input.sessionId !== 'string' || !input.sessionId || input.sessionId.length > 1024)
                throw new Error('A remote session ID is required.');
        }
        if ([
            'session/prompt',
            'session/load',
            'session/resume',
            'session/close',
            'session/fork',
            'session/set_model',
            'session/set_mode',
            'session/set_config_option',
        ].includes(method) &&
            (this.turns.has(input.sessionId) || this.inheritedTurns.has(input.sessionId)))
            throw new Error('This session has an active prompt. Wait for it to finish or request cancellation.');
        if (method === 'session/prompt' && (!Array.isArray(input.prompt) || !input.prompt.length))
            throw new Error('A prompt requires ACP content blocks.');
        if (method === 'session/cancel') {
            // Native swarm background permissions can outlive a coordinator turn.
            // Its cancel notification must not synthesize answers to those requests.
            for (const pending of (0, nativeSwarm_1.nativeSwarmCapabilities)(this.info) ? [] : [...this.requests.values()]) {
                if (pending.params.sessionId !== input.sessionId)
                    continue;
                this.respond(pending.token, pending.method === 'session/request_permission'
                    ? { result: { outcome: { outcome: 'cancelled' } } }
                    : { error: { code: -32800, message: 'Session cancelled' } });
            }
            this.write({ jsonrpc: '2.0', method, params: input });
            this.record('cancel_requested', input);
            return { cancellationRequested: true }; // A cancellation request is not a completed turn.
        }
        return this.sendRequest(method, input, method === 'session/prompt' ? 0 : 30000);
    }
    respond(token, reply) {
        if (typeof token !== 'string')
            throw new Error('Invalid reverse request token.');
        const pending = this.requests.get(token);
        if (!pending || this.state !== 'connected')
            throw new Error('This request is no longer pending on this connection.');
        const input = reply;
        if (!input ||
            typeof input !== 'object' ||
            Array.isArray(input) ||
            Object.prototype.hasOwnProperty.call(input, 'result') === Object.prototype.hasOwnProperty.call(input, 'error'))
            throw new Error('Provide either result or error.');
        if (input.error && (!Number.isInteger(input.error.code) || typeof input.error.message !== 'string'))
            throw new Error('Invalid JSON-RPC error.');
        if (pending.method === 'session/request_permission' && Object.prototype.hasOwnProperty.call(input, 'result')) {
            const outcome = input.result?.outcome;
            if (outcome?.outcome !== 'cancelled' &&
                !(outcome?.outcome === 'selected' &&
                    Array.isArray(pending.params.options) &&
                    pending.params.options.some((option) => option.optionId === outcome.optionId)))
                throw new Error('Select an actual remote permission option or cancel the request.');
        }
        this.write({
            jsonrpc: '2.0',
            id: pending.id,
            ...('error' in input ? { error: input.error } : { result: input.result }),
        });
        this.requests.delete(token);
    }
    disconnect(reason = 'Detached. Accepted remote tasks continue on the host; reconnect and inspect status before sending again.') {
        const child = this.process;
        this.process = undefined;
        if (this.state === 'disconnected' && !child)
            return;
        this.state = 'disconnected';
        this.failure = reason;
        this.ready = undefined;
        for (const call of this.calls.values()) {
            clearTimeout(call.timer);
            call.reject(new Error(reason));
        }
        this.calls.clear();
        this.requests.clear(); // Never synthesize a permission response during disconnect.
        this.record('disconnected', {
            message: reason,
            uncertainSessions: [...new Set([...this.turns, ...this.inheritedTurns])],
        });
        child?.stdin.destroy();
        child?.kill(); // Kills only the local SSH controller, never the persistent remote service.
    }
    record(kind, data) {
        const bytes = Buffer.byteLength(JSON.stringify(data) ?? 'null');
        this.events.push({ seq: ++this.seq, kind, data, bytes });
        this.eventBytes += bytes;
        while (this.events.length > MAX_EVENTS || this.eventBytes > MAX_EVENT_BYTES)
            this.eventBytes -= this.events.shift().bytes;
    }
    write(message) {
        if (!this.process)
            throw new Error('SSH is disconnected.');
        const data = Buffer.from(JSON.stringify(message) + '\n');
        if (data.length > MAX_MESSAGE)
            throw new Error('ACP message exceeds 16 MiB.');
        if (this.process.stdin.writableLength + data.length > MAX_EVENT_BYTES) {
            this.disconnect('SSH write queue exceeded 32 MiB. Remote tasks were not replayed.');
            throw new Error(this.failure);
        }
        this.process.stdin.write(data);
    }
    sendRequest(method, params, timeoutMs) {
        if (this.calls.size >= 128)
            return Promise.reject(new Error('Too many pending ACP requests.'));
        const id = `${this.generation}:${++this.serial}`;
        return new Promise((resolve, reject) => {
            const timer = timeoutMs
                ? setTimeout(() => {
                    // A timed-out RPC is uncertain, not safe to retry. Close its controller.
                    this.disconnect(`ACP ${method} timed out. Inspect remote status before retrying.`);
                }, timeoutMs)
                : undefined;
            this.calls.set(id, { method, params, resolve, reject, timer });
            try {
                this.write({ jsonrpc: '2.0', id, method, params });
                if (method === 'session/prompt') {
                    this.turns.add(params.sessionId);
                    this.record('user_prompt', params);
                }
            }
            catch (error) {
                clearTimeout(timer);
                this.calls.delete(id);
                reject(error);
            }
        });
    }
    receive(chunk) {
        try {
            this.buffer = Buffer.concat([this.buffer, chunk]);
            let newline;
            while ((newline = this.buffer.indexOf(10)) !== -1) {
                if (newline > MAX_MESSAGE)
                    throw new Error('ACP message exceeds 16 MiB.');
                const line = this.buffer.subarray(0, newline);
                this.buffer = this.buffer.subarray(newline + 1);
                if (!line.length)
                    continue;
                const message = JSON.parse(line.toString('utf8'));
                if (!message || typeof message !== 'object' || Array.isArray(message) || message.jsonrpc !== '2.0')
                    throw new Error('Expected ACP JSON-RPC on stdout. Remove remote shell welcome output.');
                this.dispatch(message);
            }
            if (this.buffer.length > MAX_MESSAGE)
                throw new Error('ACP message exceeds 16 MiB.');
        }
        catch (error) {
            this.disconnect(`Invalid ACP stream: ${error.message}`);
        }
    }
    dispatch(message) {
        if (typeof message.method === 'string') {
            if (Object.prototype.hasOwnProperty.call(message, 'id')) {
                if ((typeof message.id !== 'string' && typeof message.id !== 'number') || this.requests.size >= 128)
                    throw new Error('Invalid or excessive reverse requests.');
                if (/^(fs|terminal)\//.test(message.method)) {
                    this.write({
                        jsonrpc: '2.0',
                        id: message.id,
                        error: {
                            code: -32601,
                            message: 'This client does not provide local files or terminals. Use server-side agent tools.',
                        },
                    });
                    return;
                }
                if ([...this.requests.values()].some((item) => item.id === message.id))
                    throw new Error('Duplicate reverse request ID.');
                const token = (0, node_crypto_1.randomUUID)();
                this.requests.set(token, { token, id: message.id, method: message.method, params: message.params || {} });
            }
            else {
                if (message.method === 'session/update') {
                    const { sessionId, update } = message.params || {};
                    if (typeof sessionId === 'string' && update) {
                        const session = this.sessions.get(sessionId);
                        if (session) {
                            if (update.sessionUpdate === 'config_option_update')
                                session.configOptions = update.configOptions;
                            if (update.sessionUpdate === 'current_mode_update')
                                session.modes = { ...session.modes, currentModeId: update.currentModeId };
                        }
                    }
                }
                this.record('notification', message);
            }
            return;
        }
        const call = this.calls.get(message.id);
        if (!call)
            return; // Stale/unknown responses cannot complete another connection's request.
        if (Object.prototype.hasOwnProperty.call(message, 'result') === Object.prototype.hasOwnProperty.call(message, 'error'))
            throw new Error('Invalid ACP response.');
        this.calls.delete(message.id);
        clearTimeout(call.timer);
        if (call.method === 'session/prompt')
            this.turns.delete(call.params.sessionId);
        if (message.error) {
            this.record('rpc_error', { method: call.method, sessionId: call.params.sessionId, error: message.error });
            call.reject(Object.assign(new Error(typeof message.error.message === 'string' ? message.error.message : 'Remote ACP request failed.'), { code: message.error.code, data: message.error.data }));
            return;
        }
        if (['session/new', 'session/load', 'session/resume', 'session/fork'].includes(call.method)) {
            const sessionId = message.result?.sessionId || call.params.sessionId;
            if (typeof sessionId === 'string') {
                this.sessions.set(sessionId, { ...message.result, sessionId, cwd: call.params.cwd });
                if (this.sessions.size > 256)
                    this.sessions.delete(this.sessions.keys().next().value);
            }
        }
        if (call.method === '_maichart/swarm/attach') {
            const result = message.result;
            if (result?.attached !== true ||
                result.readOnly !== true ||
                result.sessionId !== call.params.sessionId ||
                result.source !== 'observed-native-acp') {
                call.reject(new Error('Invalid native swarm attach response.'));
                return;
            }
            const metadata = result.metadata || {};
            if ((metadata.nativeSession &&
                (typeof metadata.nativeSession !== 'object' || Array.isArray(metadata.nativeSession))) ||
                (metadata.staleFields !== undefined &&
                    (!Array.isArray(metadata.staleFields) ||
                        metadata.staleFields.some((field) => typeof field !== 'string')))) {
                call.reject(new Error('Invalid native swarm session metadata.'));
                return;
            }
            const observed = { ...metadata.nativeSession };
            for (const field of metadata.staleFields || [])
                delete observed[field];
            this.sessions.set(result.sessionId, {
                ...observed,
                sessionId: result.sessionId,
                cwd: this.config.cwd,
                _meta: {
                    nativeSwarmAttach: {
                        generation: result.generation,
                        freshness: metadata.freshness,
                        staleFields: metadata.staleFields || [],
                    },
                },
            });
        }
        const session = this.sessions.get(call.params.sessionId);
        if (session && call.method === 'session/set_config_option' && Array.isArray(message.result?.configOptions))
            session.configOptions = message.result.configOptions;
        if (session && call.method === 'session/set_model')
            session.models = { ...session.models, currentModelId: call.params.modelId };
        if (session && call.method === 'session/set_mode')
            session.modes = { ...session.modes, currentModeId: call.params.modeId };
        if (call.method === 'session/close')
            this.sessions.delete(call.params.sessionId);
        this.record('rpc_result', { method: call.method, sessionId: call.params.sessionId, result: message.result });
        call.resolve(message.result);
    }
}
exports.RemoteAcpClient = RemoteAcpClient;
/** One-shot management is independent of the ACP controller and never submits prompts. */
function remoteAcpStatus(config, spawnSsh = launch, jobId) {
    return new Promise((resolve, reject) => {
        const child = spawnSsh('ssh', remoteSshArgs(config, jobId === undefined ? 'status' : 'job', jobId));
        let output = Buffer.alloc(0);
        let error = '';
        let settled = false;
        const finish = (failure, result) => {
            if (settled)
                return;
            settled = true;
            clearTimeout(timer);
            if (failure) {
                child.kill();
                reject(failure);
            }
            else
                resolve(result);
        };
        const timer = setTimeout(() => finish(new Error('Remote status timed out.')), 15000);
        child.stdin.on('error', () => { });
        child.stdout.on('data', (chunk) => {
            if (settled)
                return;
            if (output.length + chunk.length > MAX_MESSAGE) {
                finish(new Error('Remote status exceeded 16 MiB.'));
                return;
            }
            output = Buffer.concat([output, chunk]);
        });
        child.stderr.on('data', (chunk) => {
            if (!settled)
                error = (error + chunk.toString('utf8')).slice(-4096);
        });
        child.on('error', () => finish(new Error('Unable to start SSH.')));
        child.on('close', (code) => {
            if (settled)
                return;
            if (code !== 0) {
                finish(new Error(error || `SSH status failed (${code}).`));
                return;
            }
            try {
                finish(undefined, JSON.parse(output.toString('utf8')));
            }
            catch {
                finish(new Error('Expected JSON from acp-ssh management.'));
            }
        });
        child.stdin.end();
    });
}
function remoteEndpoint(config) {
    return JSON.stringify([config.host, config.command, config.instance]);
}
function savePrivateJson(filename, value) {
    fs.mkdirSync(path.dirname(filename), { recursive: true, mode: 0o700 });
    const temporary = `${filename}.${(0, node_crypto_1.randomUUID)()}.tmp`;
    try {
        fs.writeFileSync(temporary, JSON.stringify(value, null, 2) + '\n', { flag: 'wx', mode: 0o600 });
        fs.renameSync(temporary, filename);
    }
    finally {
        fs.rmSync(temporary, { force: true });
    }
}
class RemoteAcpManager {
    constructor(directory, spawnSsh = launch) {
        this.directory = directory;
        this.spawnSsh = spawnSsh;
        this.lastRemoteStatus = 0;
        this.filename = path.join(directory, 'remote_acp.json');
        this.workspaceFile = path.join(directory, 'remote_acp_workspace.json');
    }
    config() {
        try {
            return validateRemoteAcpConfig(JSON.parse(fs.readFileSync(this.filename, 'utf8')));
        }
        catch (error) {
            if (error.code === 'ENOENT')
                return null;
            throw error;
        }
    }
    save(value) {
        const config = validateRemoteAcpConfig(value);
        savePrivateJson(this.filename, config);
        return config;
    }
    workspace() {
        try {
            const data = fs.readFileSync(this.workspaceFile);
            if (data.length > 4 * 1024 * 1024)
                throw new Error('Remote workspace index exceeds 4 MiB.');
            const workspace = JSON.parse(data.toString('utf8'));
            if (workspace.version !== 1 ||
                !Array.isArray(workspace.projects) ||
                !Array.isArray(workspace.sessions) ||
                workspace.projects.length > 64 ||
                workspace.sessions.length > 2048 ||
                (workspace.deletedSessions !== undefined &&
                    (!Array.isArray(workspace.deletedSessions) || workspace.deletedSessions.length > 2048)))
                throw new Error('Invalid remote workspace index.');
            for (const project of workspace.projects)
                validateRemoteAcpConfig(project.config);
            return workspace;
        }
        catch (error) {
            if (error.code === 'ENOENT')
                return { version: 1, projects: [], sessions: [] };
            throw error;
        }
    }
    // Called only after the remote agent confirms durable deletion. A session can
    // appear under more than one saved project on the same endpoint.
    removeDeletedSession(config, sessionId) {
        const workspace = this.workspace();
        const projects = workspace.projects.filter((project) => remoteEndpoint(project.config) === remoteEndpoint(config));
        const removed = workspace.sessions.filter((session) => session.sessionId === sessionId && projects.some((project) => project.id === session.projectId));
        workspace.sessions = workspace.sessions.filter((session) => !removed.includes(session));
        workspace.deletedSessions = [
            ...(workspace.deletedSessions || []),
            ...removed.map(({ projectId, sessionId, conversationId }) => ({ projectId, sessionId, conversationId })),
        ].slice(-2048);
        for (const project of projects)
            if (project.lastSessionId === sessionId)
                delete project.lastSessionId;
        savePrivateJson(this.workspaceFile, workspace);
        return removed;
    }
    updateWorkspace(operation, input) {
        const workspace = this.workspace();
        if (operation === 'project-save') {
            const config = validateRemoteAcpConfig(input.config);
            if (typeof input.name !== 'string' || !input.name.trim() || input.name.length > 200)
                throw new Error('Use a project name of 1–200 characters.');
            const project = input.id ? workspace.projects.find((entry) => entry.id === input.id) : undefined;
            if (input.id && !project)
                throw new Error('Unknown remote project.');
            if (input.mcpServers !== undefined && !Array.isArray(input.mcpServers))
                throw new Error('Remote MCP servers must be an array.');
            if (project) {
                if ((remoteEndpoint(project.config) !== remoteEndpoint(config) || project.config.cwd !== config.cwd) &&
                    workspace.sessions.some((entry) => entry.projectId === project.id))
                    throw new Error('Save a new project for a different host or directory so existing sessions keep their workspace.');
                project.name = input.name.trim();
                if (input.archived !== undefined)
                    project.archived = input.archived === true;
                project.config = config;
                if (input.mcpServers !== undefined)
                    project.mcpServers = input.mcpServers;
            }
            else {
                if (workspace.projects.length >= 64)
                    throw new Error('The remote project limit is 64.');
                workspace.projects.push({
                    id: (0, node_crypto_1.randomUUID)(),
                    name: input.name.trim(),
                    config,
                    mcpServers: input.mcpServers || [],
                });
            }
            workspace.selectedProjectId = project?.id || workspace.projects[workspace.projects.length - 1].id;
        }
        else if (operation === 'project-select') {
            if (!workspace.projects.some((entry) => entry.id === input.id))
                throw new Error('Unknown remote project.');
            workspace.selectedProjectId = input.id;
        }
        else if (operation === 'project-remove') {
            if (!workspace.projects.some((entry) => entry.id === input.id))
                throw new Error('Unknown remote project.');
            workspace.projects = workspace.projects.filter((entry) => entry.id !== input.id);
            workspace.sessions = workspace.sessions.filter((entry) => entry.projectId !== input.id);
            if (workspace.selectedProjectId === input.id)
                workspace.selectedProjectId = workspace.projects[0]?.id;
        }
        else if (operation === 'session-save' || operation === 'sessions-import') {
            const project = workspace.projects.find((entry) => entry.id === input.projectId);
            if (!project)
                throw new Error('Save a remote project first.');
            const entries = operation === 'sessions-import' ? input.sessions : [input];
            if (!Array.isArray(entries) || entries.length > 256)
                throw new Error('Import at most 256 remote sessions per page.');
            for (const entry of entries) {
                if (typeof entry.sessionId !== 'string' || !entry.sessionId || entry.sessionId.length > 1024)
                    throw new Error('Invalid remote session ID.');
                if (workspace.deletedSessions?.some((deleted) => deleted.projectId === project.id && deleted.sessionId === entry.sessionId)) {
                    if (operation === 'sessions-import')
                        continue;
                    throw new Error('This remote session has been permanently deleted.');
                }
                if (entry.cwd && entry.cwd !== project.config.cwd)
                    continue; // Never mix another project's history into this project.
                let session = workspace.sessions.find((item) => item.projectId === project.id && item.sessionId === entry.sessionId);
                if (!session) {
                    if (workspace.sessions.length >= 2048)
                        throw new Error('The remote session index limit is 2048.');
                    session = {
                        projectId: project.id,
                        sessionId: entry.sessionId,
                        title: '',
                        archived: false,
                        pinned: false,
                        updatedAt: new Date().toISOString(),
                    };
                    workspace.sessions.push(session);
                }
                if (operation === 'session-save' && entry.conversationId !== undefined) {
                    if (typeof entry.conversationId !== 'string' ||
                        !/^[a-f0-9]{8}-(?:[a-f0-9]{4}-){3}[a-f0-9]{12}$/i.test(entry.conversationId))
                        throw new Error('Invalid desktop conversation ID.');
                    if (session.conversationId && session.conversationId !== entry.conversationId)
                        throw new Error('The remote session already has a desktop conversation ID.');
                    if (workspace.sessions.some((other) => other !== session && other.conversationId === entry.conversationId))
                        throw new Error('The desktop conversation ID already belongs to another remote session.');
                    session.conversationId = entry.conversationId;
                }
                if (entry.title !== undefined) {
                    if (typeof entry.title !== 'string' || entry.title.length > 300)
                        throw new Error('Session titles are limited to 300 characters.');
                    if (operation === 'sessions-import')
                        session.remoteTitle = entry.title;
                    else
                        session.title = entry.title.trim();
                }
                if (input.annotations && typeof input.annotations === 'object')
                    // Native protobuf timestamps and int64 lists use BigInt. Persist their
                    // decimal representation without losing precision in the JSON index.
                    session.annotations = JSON.parse(JSON.stringify({ ...session.annotations, ...input.annotations }, (key, value) => key === '$typeName' ? undefined : typeof value === 'bigint' ? value.toString() : value));
                for (const flag of ['archived', 'pinned']) {
                    if (entry[flag] !== undefined) {
                        if (typeof entry[flag] !== 'boolean')
                            throw new Error('Invalid session flag.');
                        session[flag] = entry[flag];
                    }
                }
                if (typeof entry.updatedAt === 'string' && Number.isFinite(Date.parse(entry.updatedAt)))
                    session.updatedAt = entry.updatedAt;
                else if (operation === 'session-save')
                    session.updatedAt = new Date().toISOString();
                if (operation === 'session-save' && input.select === true)
                    project.lastSessionId = entry.sessionId;
            }
        }
        if (Buffer.byteLength(JSON.stringify(workspace)) > 4 * 1024 * 1024)
            throw new Error('Remote workspace index exceeds 4 MiB.');
        savePrivateJson(this.workspaceFile, workspace);
        return workspace;
    }
    async action(owner, operation, input = {}) {
        if (operation === 'workspace')
            return this.workspace();
        if (['project-save', 'project-select', 'project-remove', 'session-save', 'sessions-import'].includes(operation))
            return this.updateWorkspace(operation, input);
        if (operation === 'config')
            return this.config();
        if (operation === 'save')
            return this.save(input);
        if (operation === 'status' || operation === 'job') {
            const config = validateRemoteAcpConfig(operation === 'job' ? input.config : input);
            const result = await remoteAcpStatus(config, this.spawnSsh, operation === 'job' ? input.id : undefined);
            if (operation === 'status' &&
                this.owner === owner &&
                this.client &&
                remoteEndpoint(this.client.config) === remoteEndpoint(config))
                this.client.syncRemoteStatus(result);
            return result;
        }
        if (this.owner !== undefined && this.owner !== owner)
            throw new Error('Another desktop window owns this SSH connection.');
        if (operation === 'connect') {
            const config = validateRemoteAcpConfig(input);
            if (this.client && this.client.snapshot().state !== 'disconnected') {
                if (remoteEndpoint(this.client.config) !== remoteEndpoint(config))
                    throw new Error('Detach the current connection before changing hosts.');
            }
            else {
                this.client = new RemoteAcpClient(config, this.spawnSsh);
                this.owner = owner;
            }
            const client = this.client;
            try {
                client.syncRemoteStatus(await remoteAcpStatus(config, this.spawnSsh));
                if (this.client !== client)
                    throw new Error('SSH connection was detached.');
                await client.connect();
                client.syncRemoteStatus(await remoteAcpStatus(config, this.spawnSsh));
                if (this.client !== client)
                    throw new Error('SSH connection was detached.');
                this.lastRemoteStatus = Date.now();
                return client.snapshot();
            }
            catch (error) {
                client.disconnect();
                throw error;
            }
        }
        if (operation === 'poll' &&
            this.client?.snapshot().state === 'connected' &&
            this.client.snapshot().activeSessions.length > 0 &&
            Date.now() - this.lastRemoteStatus > 5000) {
            this.lastRemoteStatus = Date.now();
            const client = this.client;
            client.syncRemoteStatus(await remoteAcpStatus(client.config, this.spawnSsh));
        }
        if (operation === 'poll')
            return (this.client?.snapshot(input.after || 0) || {
                state: 'disconnected',
                events: [],
                pending: [],
                cursor: 0,
                sessions: [],
                activeSessions: [],
            });
        if (!this.client)
            throw new Error('Connect to the remote agent first.');
        if (operation === 'detach') {
            this.close();
            return {};
        }
        if (operation === 'rpc')
            return this.client.request(input.method, input.params);
        if (operation === 'respond') {
            this.client.respond(input.token, input.reply);
            return {};
        }
        throw new Error('Unknown remote ACP action.');
    }
    release(owner) {
        if (owner === this.owner)
            this.close();
    }
    close() {
        this.client?.disconnect();
        this.client = undefined;
        this.owner = undefined;
    }
}
exports.RemoteAcpManager = RemoteAcpManager;
//# sourceMappingURL=remoteAcp.js.map