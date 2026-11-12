import { afterEach, describe, expect, it, vi } from 'vitest';
import { EventEmitter } from 'node:events';
import { PassThrough } from 'node:stream';
import { mkdtempSync, readFileSync, rmSync, statSync } from 'node:fs';
import { tmpdir } from 'node:os';
import { join } from 'node:path';
import {
  REMOTE_ACP_PROFILE,
  RemoteAcpClient,
  RemoteAcpManager,
  remoteAcpStatus,
  remoteSshArgs,
  validateRemoteAcpConfig,
  type SshSpawn,
} from '../remoteAcp';
vi.mock('electron', () => ({
  app: {},
  ipcMain: {},
  contextBridge: { exposeInMainWorld: vi.fn(), executeInMainWorld: vi.fn() },
  ipcRenderer: { invoke: vi.fn() },
}));
import { trustedRemoteAcpFrame } from '../remoteAcpIpc';

const config = {
  host: 'developer@host.example',
  command: '/home/developer/.local/bin/acp-ssh',
  instance: 'agy',
  cwd: '/remote/work',
};
const cleanup: (() => void)[] = [];
afterEach(() => {
  for (const close of cleanup.splice(0)) close();
  vi.useRealTimers();
  vi.unstubAllGlobals();
});
function transport(remoteConfig = config) {
  const child = Object.assign(new EventEmitter(), {
    stdin: new PassThrough(),
    stdout: new PassThrough(),
    stderr: new PassThrough(),
    kill: vi.fn(),
  });
  const sent: any[] = [];
  let buffer = '';
  child.stdin.on('data', (chunk) => {
    buffer += chunk.toString();
    let index: number;
    while ((index = buffer.indexOf('\n')) !== -1) {
      sent.push(JSON.parse(buffer.slice(0, index)));
      buffer = buffer.slice(index + 1);
    }
  });
  const spawn = vi.fn(() => child) as unknown as SshSpawn;
  const receive = (message: any) => child.stdout.write(JSON.stringify({ jsonrpc: '2.0', ...message }) + '\n');
  const client = new RemoteAcpClient(remoteConfig, spawn);
  cleanup.push(() => client.disconnect());
  const connect = async (agentCapabilities: any = { loadSession: true }) => {
    const ready = client.connect();
    expect(sent[0]).toMatchObject({ method: 'initialize', params: REMOTE_ACP_PROFILE });
    receive({ id: sent[0].id, result: { protocolVersion: 1, agentCapabilities } });
    await ready;
  };
  return { child, sent, spawn, receive, client, connect };
}

const deletionCapabilities = {
  loadSession: true,
  _meta: {
    'maichart/sessionManagement': {
      version: 1,
      delete: { method: '_maichart/session/delete', permanent: true, idempotent: true, relatedSessions: 'reject' },
    },
  },
};
async function deletionFixture(capabilities: any = deletionCapabilities) {
  const { NativeRemoteService, remoteConversationId } = await import('../nativeRemote');
  const directory = mkdtempSync(join(tmpdir(), 'agy-delete-'));
  cleanup.push(() => rmSync(directory, { recursive: true, force: true }));
  const manager = new RemoteAcpManager(directory);
  await manager.action(0, 'project-save', { name: 'Remote', config });
  const project = manager.workspace().projects[0];
  await manager.action(0, 'session-save', {
    projectId: project.id,
    sessionId: 'saved',
    title: 'Keep until confirmed',
    select: true,
  });
  const f = transport();
  await f.connect(capabilities);
  const native = new NativeRemoteService(
    manager,
    () => f.client,
    async () => ({ active: [] }),
  );
  cleanup.push(() => native.close());
  return { ...f, manager, native, project, id: remoteConversationId(project.id, 'saved') };
}

describe('SSH ACP desktop controller', () => {
  it('uses strict SSH without a local shell and quotes remote arguments', () => {
    const args = remoteSshArgs({ ...config, command: "/remote/O'Brien/acp-ssh" }, 'connect');
    expect(args).toContain('BatchMode=yes');
    expect(args).toContain('StrictHostKeyChecking=yes');
    expect(args).toContain('-T');
    expect(args.at(-1)).toBe("'/remote/O'\\''Brien/acp-ssh' 'connect' 'agy'");
    for (const host of ['-oProxyCommand=evil', 'host;touch /tmp/x', '$(whoami)', 'user@host\n'])
      expect(() => validateRemoteAcpConfig({ ...config, host })).toThrow();
    expect(() => remoteSshArgs(config, 'job', '--file')).toThrow();
    expect(() => validateRemoteAcpConfig({ ...config, cwd: 'local/path' })).toThrow();
    expect(() => validateRemoteAcpConfig({ ...config, command: '/bin/x\n' })).toThrow();
  });
  it('handles fragmented JSONL, preserves extension data, and updates remote configuration', async () => {
    const f = transport();
    const ready = f.client.connect();
    const wire = Buffer.from(
      JSON.stringify({ jsonrpc: '2.0', id: f.sent[0].id, result: { protocolVersion: 1, _meta: { vendor: '原样' } } }) +
        '\n',
    );
    f.child.stdout.write(wire.subarray(0, wire.length - 6));
    f.child.stdout.write(wire.subarray(wire.length - 6));
    expect(await ready).toMatchObject({ _meta: { vendor: '原样' } });
    const created = f.client.request('session/new', { cwd: config.cwd, mcpServers: [], _meta: { extension: true } });
    expect(f.sent.at(-1).params._meta).toEqual({ extension: true });
    f.receive({
      id: f.sent.at(-1).id,
      result: { sessionId: 's', models: { currentModelId: 'a', availableModels: [{ modelId: 'b', name: 'B' }] } },
    });
    await created;
    const model = f.client.request('session/set_model', { sessionId: 's', modelId: 'b' });
    f.receive({ id: f.sent.at(-1).id, result: {} });
    await model;
    expect(f.client.snapshot().sessions[0].models.currentModelId).toBe('b');
    const setting = f.client.request('session/set_config_option', { sessionId: 's', configId: 'model', value: 'c' });
    f.receive({
      id: f.sent.at(-1).id,
      result: { configOptions: [{ id: 'model', type: 'select', currentValue: 'c' }] },
    });
    await setting;
    expect(f.client.snapshot().sessions[0].configOptions[0].currentValue).toBe('c');
  });
  it('streams prompts and permissions, rejects fabricated choices, and waits for cancel completion', async () => {
    const f = transport();
    await f.connect();
    const turn = f.client.request('session/prompt', {
      sessionId: 's',
      prompt: [{ type: 'text', text: 'hello' }],
      _meta: { opaque: 42 },
    });
    const promptId = f.sent.at(-1).id;
    f.receive({
      method: 'session/update',
      params: {
        sessionId: 's',
        update: {
          sessionUpdate: 'agent_message_chunk',
          content: { type: 'text', text: '<script>hi</script>' },
          _meta: { raw: true },
        },
      },
    });
    f.receive({
      id: 22,
      method: 'session/request_permission',
      params: { sessionId: 's', options: [{ optionId: 'once', kind: 'allow_once', name: 'Allow once' }] },
    });
    const token = f.client.snapshot().pending[0].token;
    expect(() =>
      f.client.respond(token, { result: { outcome: { outcome: 'selected', optionId: 'made-up' } } }),
    ).toThrow();
    f.client.respond(token, { result: { outcome: { outcome: 'selected', optionId: 'once' } } });
    expect(f.sent.at(-1)).toEqual({
      jsonrpc: '2.0',
      id: 22,
      result: { outcome: { outcome: 'selected', optionId: 'once' } },
    });
    expect(() => f.client.respond(token, { result: {} })).toThrow();
    await expect(f.client.request('session/load', { sessionId: 's', cwd: config.cwd, mcpServers: [] })).rejects.toThrow(
      'active prompt',
    );
    f.receive({ id: 23, method: 'session/request_permission', params: { sessionId: 's', options: [] } });
    expect(await f.client.request('session/cancel', { sessionId: 's' })).toEqual({ cancellationRequested: true });
    expect(f.sent.at(-2)).toMatchObject({ id: 23, result: { outcome: { outcome: 'cancelled' } } });
    expect(f.sent.at(-1)).toEqual({ jsonrpc: '2.0', method: 'session/cancel', params: { sessionId: 's' } });
    expect(f.client.snapshot().activeSessions).toEqual(['s']);
    expect(f.client.snapshot().pending).toEqual([]);
    f.receive({ id: promptId, result: { stopReason: 'cancelled' } });
    expect(await turn).toEqual({ stopReason: 'cancelled' });
    expect(f.client.snapshot().activeSessions).toEqual([]);
    expect(f.client.snapshot().events).toContainEqual(
      expect.objectContaining({
        kind: 'notification',
        data: expect.objectContaining({
          params: expect.objectContaining({ update: expect.objectContaining({ _meta: { raw: true } }) }),
        }),
      }),
    );
  });
  it('does not replay a disconnected prompt or answer pending approvals', async () => {
    const f = transport();
    await f.connect();
    const turn = f.client.request('session/prompt', { sessionId: 's', prompt: [{ type: 'text', text: 'once' }] });
    const rejected = expect(turn).rejects.toThrow('Detached');
    f.receive({ id: 'permission', method: 'session/request_permission', params: { sessionId: 's', options: [] } });
    const before = f.sent.length;
    f.client.disconnect();
    await rejected;
    expect(f.sent).toHaveLength(before);
    expect(f.child.kill).toHaveBeenCalledOnce();
    expect(f.client.snapshot().activeSessions).toEqual(['s']);
    await expect(f.client.connect()).rejects.toThrow('new controller');
    expect(f.spawn).toHaveBeenCalledOnce();
  });
  it('protects inherited active turns until management confirms completion', async () => {
    const f = transport();
    await f.connect();
    f.client.syncRemoteStatus({ active: [{ sessionId: 'old', id: 'job1' }] });
    await expect(
      f.client.request('session/prompt', { sessionId: 'old', prompt: [{ type: 'text', text: 'duplicate' }] }),
    ).rejects.toThrow('active prompt');
    f.client.syncRemoteStatus({ active: [] });
    const load = f.client.request('session/load', { sessionId: 'old', cwd: config.cwd, mcpServers: [] });
    f.receive({ id: f.sent.at(-1).id, result: {} });
    await load;
  });
  it('refuses undeclared local tools and invalid stdout, keeping stderr separate', async () => {
    const f = transport();
    await f.connect();
    f.receive({ id: 3, method: 'fs/read_text_file', params: { path: '/private' } });
    expect(f.sent.at(-1)).toMatchObject({ id: 3, error: { code: -32601 } });
    f.child.stderr.write('diagnostic only');
    expect(f.client.snapshot().state).toBe('connected');
    f.child.stdout.write('shell welcome\n');
    expect(f.client.snapshot().state).toBe('disconnected');
    expect(f.client.snapshot().failure).toContain('Invalid ACP stream');
  });
  it('retains bounded event history with a gap marker and disconnects on RPC timeout', async () => {
    vi.useFakeTimers();
    const f = transport();
    await f.connect();
    for (let i = 0; i < 260; i++) f.receive({ method: '_notice', params: { index: i } });
    expect(f.client.snapshot().gap).toBe(true);
    expect(f.client.snapshot().events).toHaveLength(256);
    const pending = f.client.request('session/list', {});
    const rejected = expect(pending).rejects.toThrow('timed out');
    await vi.advanceTimersByTimeAsync(30000);
    await rejected;
    expect(f.client.snapshot().state).toBe('disconnected');
  });
  it('persists only connection metadata with private permissions', async () => {
    const dir = mkdtempSync(join(tmpdir(), 'remote-acp-'));
    cleanup.push(() => rmSync(dir, { recursive: true, force: true }));
    const manager = new RemoteAcpManager(dir);
    expect(manager.config()).toBeNull();
    await manager.action(1, 'save', { ...config, privateKey: 'must-not-persist', token: 'secret' });
    expect(manager.config()).toEqual(config);
    const file = join(dir, 'remote_acp.json');
    // Windows uses ACLs and does not expose POSIX owner/group permission bits.
    if (process.platform !== 'win32') expect(statSync(file).mode & 0o777).toBe(0o600);
    expect(readFileSync(file, 'utf8')).not.toMatch(/secret|privateKey/);
    expect(await manager.action(2, 'poll')).toMatchObject({ state: 'disconnected' });
  });
  it('reads bounded management JSON and reports SSH errors and timeouts', async () => {
    const f = transport();
    const status = remoteAcpStatus(config, f.spawn);
    f.child.stdout.write('{"active":[]}');
    f.child.emit('close', 0);
    expect(await status).toEqual({ active: [] });
    const bad = transport();
    const failed = remoteAcpStatus(config, bad.spawn);
    const rejected = expect(failed).rejects.toThrow('denied');
    bad.child.stderr.write('denied');
    bad.child.emit('close', 255);
    await rejected;
    const large = transport();
    const oversized = expect(remoteAcpStatus(config, large.spawn)).rejects.toThrow('16 MiB');
    large.child.stdout.write(Buffer.alloc(16 * 1024 * 1024 + 1));
    await oversized;
    expect(large.child.kill).toHaveBeenCalledOnce();
    vi.useFakeTimers();
    const slow = transport();
    const timeout = expect(remoteAcpStatus(config, slow.spawn)).rejects.toThrow('timed out');
    await vi.advanceTimersByTimeAsync(15000);
    await timeout;
  });
  it('isolates a live controller to its owning desktop window', async () => {
    const children: any[] = [];
    const ssh: SshSpawn = (_command, args) => {
      const child = Object.assign(new EventEmitter(), {
        stdin: new PassThrough(),
        stdout: new PassThrough(),
        stderr: new PassThrough(),
        kill: vi.fn(),
      });
      children.push(child);
      if (args.at(-1)!.includes("'status'")) {
        child.stdin.on('finish', () =>
          queueMicrotask(() => {
            child.stdout.write('{"active":[]}');
            child.emit('close', 0);
          }),
        );
      } else {
        child.stdin.on('data', (chunk) => {
          const message = JSON.parse(chunk.toString());
          if (message.method === 'initialize')
            queueMicrotask(() =>
              child.stdout.write(
                JSON.stringify({ jsonrpc: '2.0', id: message.id, result: { protocolVersion: 1 } }) + '\n',
              ),
            );
        });
      }
      return child as any;
    };
    const dir = mkdtempSync(join(tmpdir(), 'remote-owner-'));
    const manager = new RemoteAcpManager(dir, ssh);
    cleanup.push(() => {
      manager.close();
      rmSync(dir, { recursive: true, force: true });
    });
    expect(await manager.action(1, 'connect', config)).toMatchObject({ state: 'connected' });
    await expect(manager.action(2, 'poll')).rejects.toThrow('Another desktop window');
    await expect(manager.action(2, 'connect', config)).rejects.toThrow('Another desktop window');
    await expect(manager.action(2, 'detach')).rejects.toThrow('Another desktop window');
    manager.release(2);
    expect(await manager.action(1, 'poll')).toMatchObject({ state: 'connected' });
    manager.release(1);
    expect(children[1].kill).toHaveBeenCalledOnce();
    expect(await manager.action(2, 'poll')).toMatchObject({ state: 'disconnected' });
  });
  it('restores remote projects and session metadata across restarts without mixing workspaces', async () => {
    const dir = mkdtempSync(join(tmpdir(), 'remote-workspace-'));
    cleanup.push(() => rmSync(dir, { recursive: true, force: true }));
    const manager = new RemoteAcpManager(dir);
    const first = await manager.action(1, 'project-save', {
      name: 'Maichart',
      config,
      mcpServers: [{ name: 'remote', command: '/remote/mcp', args: [], env: [] }],
    });
    const projectId = first.selectedProjectId;
    await manager.action(1, 'sessions-import', {
      projectId,
      sessions: [
        { sessionId: 'same-id', title: 'Remote title', cwd: config.cwd },
        { sessionId: 'other-directory', title: 'Wrong project', cwd: '/other' },
      ],
    });
    await manager.action(1, 'session-save', {
      projectId,
      sessionId: 'same-id',
      title: 'Local title',
      pinned: true,
      archived: true,
      select: true,
    });
    await manager.action(1, 'sessions-import', {
      projectId,
      sessions: [{ sessionId: 'same-id', title: 'Updated remote title', cwd: config.cwd }],
    });
    const second = await manager.action(1, 'project-save', {
      name: 'Another project',
      config: { ...config, cwd: '/another' },
    });
    const secondId = second.selectedProjectId;
    await manager.action(1, 'session-save', {
      projectId: secondId,
      sessionId: 'same-id',
      title: 'Independent title',
      select: true,
    });
    await manager.action(1, 'project-select', { id: projectId });
    const restored = new RemoteAcpManager(dir).workspace();
    expect(restored.selectedProjectId).toBe(projectId);
    expect(restored.projects.find((p) => p.id === projectId)).toMatchObject({
      name: 'Maichart',
      lastSessionId: 'same-id',
      config,
    });
    expect(restored.sessions).toHaveLength(2);
    expect(restored.sessions.find((s) => s.projectId === projectId)).toMatchObject({
      title: 'Local title',
      remoteTitle: 'Updated remote title',
      pinned: true,
      archived: true,
    });
    expect(restored.sessions.find((s) => s.projectId === secondId)).toMatchObject({
      title: 'Independent title',
      pinned: false,
      archived: false,
    });
    if (process.platform !== 'win32') expect(statSync(join(dir, 'remote_acp_workspace.json')).mode & 0o777).toBe(0o600);
    await expect(
      manager.action(1, 'project-save', { id: projectId, name: 'Moved', config: { ...config, cwd: '/moved' } }),
    ).rejects.toThrow('new project');
    await expect(manager.action(1, 'session-save', { projectId, sessionId: 's', archived: 'false' })).rejects.toThrow(
      'flag',
    );
    const removed = await manager.action(1, 'project-remove', { id: projectId });
    expect(removed.projects.map((p) => p.id)).toEqual([secondId]);
    expect(removed.sessions.map((s) => s.projectId)).toEqual([secondId]);
  });
  it('restricts SSH IPC to the current local main frame and port', () => {
    const frame = { url: 'http://127.0.0.1:54321/c/1' };
    const event = { senderFrame: frame, sender: { mainFrame: frame } };
    expect(trustedRemoteAcpFrame(event, 54321)).toBe(true);
    expect(trustedRemoteAcpFrame(event, 0)).toBe(false);
    expect(trustedRemoteAcpFrame(event, 54322)).toBe(false);
    expect(trustedRemoteAcpFrame({ ...event, senderFrame: { ...frame } }, 54321)).toBe(false);
    for (const url of ['https://evil.example:54321', 'http://user:password@localhost:54321', 'file:///tmp/page.html']) {
      frame.url = url;
      expect(trustedRemoteAcpFrame(event, 54321)).toBe(false);
    }
  });
});

describe('native SSH project and conversation adapter', () => {
  it('shares cold connection and history replay between the native page and its model picker', async () => {
    const { NativeRemoteService, remoteConversationId } = await import('../nativeRemote');
    const directory = mkdtempSync(join(tmpdir(), 'agy-cold-history-'));
    cleanup.push(() => rmSync(directory, { recursive: true, force: true }));
    const manager = new RemoteAcpManager(directory);
    await manager.action(0, 'project-save', { name: 'Remote', config });
    const project = manager.workspace().projects[0];
    await manager.action(0, 'session-save', { projectId: project.id, sessionId: 'saved' });
    const f = transport(); // Deliberately start disconnected, as the real desktop does.
    const createClient = vi.fn(() => f.client);
    const statusReady = Promise.withResolvers<any>();
    const status = vi.fn(() => statusReady.promise);
    const native = new NativeRemoteService(manager, createClient, status);
    cleanup.push(() => native.close());
    const conversationId = remoteConversationId(project.id, 'saved');
    const history = native.call('streamAgentStateUpdates', { conversationId });
    const models = native.call('getScopedModelCatalog', { conversationId });
    const metadata = native.call('getConversationMetadata', { conversationId });
    expect(createClient).toHaveBeenCalledTimes(1);
    expect(status).toHaveBeenCalledTimes(1);
    statusReady.resolve({ active: [] });
    await vi.waitFor(() => expect(f.sent).toHaveLength(1));
    f.receive({ id: f.sent[0].id, result: { protocolVersion: 1, agentCapabilities: { loadSession: true } } });
    await vi.waitFor(() => expect(f.sent.some((r) => r.method === 'session/load')).toBe(true));
    const load = f.sent.find((r) => r.method === 'session/load');
    for (const [sessionUpdate, text] of [
      ['user_message_chunk', 'Question'],
      ['agent_message_chunk', 'Saved answer'],
    ])
      f.receive({
        method: 'session/update',
        params: { sessionId: 'saved', update: { sessionUpdate, content: { type: 'text', text } } },
      });
    // The sidebar can drain history notifications before session/load responds.
    await native.snapshot();
    f.receive({
      id: load.id,
      result: { models: { currentModelId: 'remote', availableModels: [{ modelId: 'remote', name: 'Remote model' }] } },
    });
    const state = (await history).value.update;
    expect(state.mainTrajectoryUpdate.stepsUpdate.steps.map((s: any) => s.step.case)).toEqual([
      'userInput',
      'plannerResponse',
    ]);
    expect(state.mainTrajectoryUpdate.stepsUpdate.steps[0].step.value.items).toEqual([
      { chunk: { case: 'text', value: 'Question' } },
    ]);
    expect(state.mainTrajectoryUpdate.stepsUpdate.steps[1].step.value.response).toBe('Saved answer');
    expect((await models).value.clientModelConfigs.map((m: any) => m.label)).toEqual(['Remote model']);
    expect((await metadata).value.numSteps).toBe(2);
    await native.call('streamAgentStateUpdates', { conversationId });
    expect(createClient).toHaveBeenCalledTimes(1);
    expect(f.sent.map((r) => r.method)).toEqual(['initialize', 'session/load']);
  });

  it('deletes remote history once, clears endpoint aliases, and rejects stale imports after restart', async () => {
    const { NativeRemoteService, remoteConversationId } = await import('../nativeRemote');
    const f = await deletionFixture({ ...deletionCapabilities, sessionCapabilities: { list: {} } });
    await f.manager.action(0, 'project-save', { name: 'Alias', config });
    const alias = f.manager.workspace().projects[1];
    await f.manager.action(0, 'session-save', { projectId: alias.id, sessionId: 'saved', select: true });
    await f.manager.action(0, 'project-save', { name: 'Other endpoint', config: { ...config, host: 'other.example' } });
    const other = f.manager.workspace().projects[2];
    await f.manager.action(0, 'session-save', { projectId: other.id, sessionId: 'saved', select: true });
    // Native optimistic-summary rollback is not a destructive history action.
    expect(await f.native.call('jetboxDeleteSummary', { cascadeId: f.id })).toEqual({ handled: true, value: {} });
    expect(f.sent.map((r) => r.method)).toEqual(['initialize']);
    expect(f.manager.workspace().sessions).toHaveLength(3);
    const first = f.native.call('deleteCascadeTrajectory', { cascadeId: f.id });
    const duplicate = f.native.call('deleteCascadeTrajectory', { cascadeId: remoteConversationId(alias.id, 'saved') });
    await vi.waitFor(() => expect(f.sent.some((r) => r.method === '_maichart/session/delete')).toBe(true));
    const request = f.sent.find((r) => r.method === '_maichart/session/delete');
    expect(request.params).toEqual({ sessionId: 'saved' });
    expect(f.sent.filter((r) => r.method === '_maichart/session/delete')).toHaveLength(1);
    expect(f.manager.workspace().sessions).toHaveLength(3);
    await f.native.snapshot(); // Start a remote list before deletion finishes.
    await vi.waitFor(() => expect(f.sent.some((r) => r.method === 'session/list')).toBe(true));
    await expect(f.native.call('getCascadeTrajectory', { cascadeId: f.id })).rejects.toThrow('being deleted');
    f.receive({
      id: request.id,
      result: { deleted: true, sessionId: 'saved', deletedSessionIds: ['saved'], alreadyDeleted: false },
    });
    expect(await first).toEqual({ handled: true, value: {} });
    await duplicate;
    for (const list of f.sent.filter((r) => r.method === 'session/list'))
      f.receive({ id: list.id, result: { sessions: [{ sessionId: 'saved', cwd: config.cwd }] } });
    await vi.waitFor(() =>
      expect(
        f.client.snapshot().events.filter((e) => e.kind === 'rpc_result' && e.data.method === 'session/list').length,
      ).toBeGreaterThan(0),
    );
    expect(f.manager.workspace().sessions.map((session) => session.projectId)).toEqual([other.id]);
    expect(
      f.manager
        .workspace()
        .projects.slice(0, 2)
        .map((project) => project.lastSessionId),
    ).toEqual([undefined, undefined]);
    expect((await f.native.snapshot()).summaries[f.id]).toBeUndefined();
    await f.native.call('deleteCascadeTrajectory', { cascadeId: f.id });
    expect(f.sent.some((r) => ['session/new', 'session/load', 'session/close'].includes(r.method))).toBe(false);
    const restoredManager = new RemoteAcpManager(f.manager.directory);
    const restored = new NativeRemoteService(restoredManager, () => {
      throw new Error('Must not reconnect a deleted session');
    });
    cleanup.push(() => restored.close());
    await restoredManager.action(0, 'sessions-import', {
      projectId: f.project.id,
      sessions: [{ sessionId: 'saved', cwd: config.cwd }],
    });
    await expect(
      restoredManager.action(0, 'session-save', { projectId: f.project.id, sessionId: 'saved' }),
    ).rejects.toThrow('permanently deleted');
    await expect(restored.call('getCascadeTrajectory', { cascadeId: f.id })).rejects.toThrow('permanently deleted');
    expect(await restored.call('deleteCascadeTrajectory', { cascadeId: f.id })).toEqual({ handled: true, value: {} });
    expect(restoredManager.workspace().sessions).toHaveLength(1);
  });

  it.each(['unsupported', 'active', 'pending', 'refused', 'invalid-response'] as const)(
    'retains remote history and the local index when deletion is %s',
    async (failure) => {
      const f = await deletionFixture(failure === 'unsupported' ? { loadSession: true } : deletionCapabilities);
      // Establish the service connection before marking an inherited prompt active.
      const connect = f.native.call('getCascadeTrajectory', { cascadeId: f.id });
      await vi.waitFor(() => expect(f.sent.some((r) => r.method === 'session/load')).toBe(true));
      f.receive({ id: f.sent.find((r) => r.method === 'session/load').id, result: {} });
      await connect;
      if (failure === 'active')
        f.client.syncRemoteStatus({ active: [{ sessionId: 'saved', method: 'session/prompt' }] });
      if (failure === 'pending')
        f.receive({
          id: 'approval',
          method: 'session/request_permission',
          params: { sessionId: 'saved', options: [] },
        });
      const result = f.native.call('deleteCascadeTrajectory', { cascadeId: f.id }).then(
        () => null,
        (error) => error,
      );
      if (failure === 'refused' || failure === 'invalid-response') {
        await vi.waitFor(() => expect(f.sent.some((r) => r.method === '_maichart/session/delete')).toBe(true));
        f.receive({
          id: f.sent.at(-1).id,
          ...(failure === 'refused'
            ? { error: { code: -32000, message: 'Session has Swarm references' } }
            : {
                result: {
                  deleted: true,
                  sessionId: 'someone-else',
                  deletedSessionIds: ['saved'],
                  alreadyDeleted: false,
                },
              }),
        });
      }
      expect(await result).toBeInstanceOf(Error);
      expect(f.manager.workspace().sessions).toHaveLength(1);
      expect(f.manager.workspace().deletedSessions || []).toEqual([]);
      expect((await f.native.snapshot()).summaries[f.id].summary).toBe('Keep until confirmed');
      if (failure === 'unsupported' || failure === 'active' || failure === 'pending')
        expect(f.sent.some((r) => r.method === '_maichart/session/delete')).toBe(false);
      if (failure === 'refused' || failure === 'invalid-response') {
        const retry = f.native.call('deleteCascadeTrajectory', { cascadeId: f.id });
        await vi.waitFor(() => expect(f.sent.filter((r) => r.method === '_maichart/session/delete')).toHaveLength(2));
        f.receive({
          id: f.sent.at(-1).id,
          result: { deleted: true, sessionId: 'saved', deletedSessionIds: ['saved'], alreadyDeleted: true },
        });
        await retry;
        expect(f.manager.workspace().sessions).toEqual([]);
        expect(f.client.snapshot().sessions).toEqual([]);
      }
    },
  );

  it('retries a failed cold connection once for concurrent readers', async () => {
    const { NativeRemoteService, remoteConversationId } = await import('../nativeRemote');
    const directory = mkdtempSync(join(tmpdir(), 'agy-retry-history-'));
    cleanup.push(() => rmSync(directory, { recursive: true, force: true }));
    const manager = new RemoteAcpManager(directory);
    await manager.action(0, 'project-save', { name: 'Remote', config });
    const project = manager.workspace().projects[0];
    await manager.action(0, 'session-save', { projectId: project.id, sessionId: 'saved' });
    const failed = transport();
    const recovered = transport();
    const createClient = vi.fn().mockReturnValueOnce(failed.client).mockReturnValue(recovered.client);
    const native = new NativeRemoteService(manager, createClient, async () => ({ active: [] }));
    cleanup.push(() => native.close());
    const input = { conversationId: remoteConversationId(project.id, 'saved') };
    const first = Promise.allSettled([
      native.call('getConversationMetadata', input),
      native.call('getScopedModelCatalog', input),
    ]);
    await vi.waitFor(() => expect(failed.sent).toHaveLength(1));
    failed.child.emit('close', 1);
    expect((await first).map((r) => r.status)).toEqual(['rejected', 'rejected']);
    const retry = Promise.all([
      native.call('getConversationMetadata', input),
      native.call('getScopedModelCatalog', input),
    ]);
    await vi.waitFor(() => expect(recovered.sent).toHaveLength(1));
    recovered.receive({
      id: recovered.sent[0].id,
      result: { protocolVersion: 1, agentCapabilities: { loadSession: true } },
    });
    await vi.waitFor(() => expect(recovered.sent).toHaveLength(2));
    recovered.receive({ id: recovered.sent[1].id, result: {} });
    expect((await retry).every((r) => r.handled)).toBe(true);
    expect(createClient).toHaveBeenCalledTimes(2);
    await native.call('getConversationMetadata', input);
    expect(recovered.sent.map((r) => r.method)).toEqual(['initialize', 'session/load']);
  });

  it('scopes catalogs to the selected project, reuses the draft and sends the native planModel selection', async () => {
    const { NativeRemoteService } = await import('../nativeRemote');
    const directory = mkdtempSync(join(tmpdir(), 'agy-project-models-'));
    cleanup.push(() => rmSync(directory, { recursive: true, force: true }));
    const manager = new RemoteAcpManager(directory);
    await manager.action(0, 'project-save', { name: 'Alpha', config });
    await manager.action(0, 'project-save', { name: 'Beta', config: { ...config, cwd: '/remote/beta' } });
    const [alpha, beta] = manager.workspace().projects;
    const f = transport();
    await f.connect();
    const native = new NativeRemoteService(
      manager,
      () => f.client,
      async () => ({ active: [] }),
    );
    cleanup.push(() => native.close());
    expect(await native.call('getUserStatus')).toEqual({ handled: false });
    expect(await native.call('getCascadeModelConfigs')).toEqual({ handled: false });
    expect(f.sent.filter((request) => request.method === 'session/new')).toHaveLength(0);
    const pending = native.call('getScopedModelCatalog', { projectId: alpha.id });
    const concurrent = native.call('getScopedModelCatalog', { projectId: alpha.id });
    await new Promise((resolve) => setImmediate(resolve));
    const first = f.sent.filter((request) => request.method === 'session/new');
    expect(first).toHaveLength(1);
    f.receive({
      id: first[0].id,
      result: {
        sessionId: 'alpha-draft',
        models: {
          currentModelId: 'a',
          availableModels: [
            { modelId: 'a', name: 'Alpha model' },
            { modelId: 'a2', name: 'Alpha second' },
          ],
        },
      },
    });
    const catalog = (await pending).value.clientModelConfigs;
    expect((await concurrent).value.clientModelConfigs).toEqual(catalog);
    expect(catalog.map((model: any) => model.label)).toEqual(['Alpha model', 'Alpha second']);
    expect(manager.workspace().sessions).toHaveLength(0);
    const otherPending = native.call('getScopedModelCatalog', { projectId: beta.id });
    await new Promise((resolve) => setImmediate(resolve));
    f.receive({
      id: f.sent.at(-1).id,
      result: {
        sessionId: 'beta-draft',
        models: {
          currentModelId: 'b',
          availableModels: [{ modelId: 'b', name: 'Beta model' }],
        },
      },
    });
    const other = (await otherPending).value.clientModelConfigs;
    expect(other.map((model: any) => model.label)).toEqual(['Beta model']);
    const created = await native.call('startCascade', { projectEnvConfig: { projectId: alpha.id } });
    expect(f.sent.filter((request) => request.method === 'session/new')).toHaveLength(2);
    expect(manager.workspace().sessions).toHaveLength(1);
    const cascadeId = created.value.cascadeId;
    await expect(
      native.call('sendUserCascadeMessage', {
        cascadeId,
        items: [{ text: 'Wrong model' }],
        cascadeConfig: { plannerConfig: { planModel: other[0].modelOrAlias.choice.value } },
      }),
    ).rejects.toThrow('belonging');
    const send = native.call('sendUserCascadeMessage', {
      cascadeId,
      items: [{ text: 'Selected second' }],
      cascadeConfig: { plannerConfig: { planModel: catalog[1].modelOrAlias.choice.value } },
    });
    await new Promise((resolve) => setImmediate(resolve));
    expect(f.sent.at(-1)).toMatchObject({
      method: 'session/set_model',
      params: { sessionId: 'alpha-draft', modelId: 'a2' },
    });
    f.receive({ id: f.sent.at(-1).id, result: {} });
    await send;
    expect(f.sent.at(-1)).toMatchObject({ method: 'session/prompt', params: { sessionId: 'alpha-draft' } });
    f.receive({ id: f.sent.at(-1).id, result: { stopReason: 'end_turn' } });
  });

  it('preserves the composer ID through create, first message, list and restart', async () => {
    const { NativeRemoteService } = await import('../nativeRemote');
    const directory = mkdtempSync(join(tmpdir(), 'agy-composer-id-'));
    cleanup.push(() => rmSync(directory, { recursive: true, force: true }));
    const manager = new RemoteAcpManager(directory);
    await manager.action(0, 'project-save', { name: 'Remote', config });
    const project = manager.workspace().projects[0];
    const f = transport();
    await f.connect();
    const native = new NativeRemoteService(
      manager,
      () => f.client,
      async () => ({ active: [] }),
    );
    cleanup.push(() => native.close());
    const cascadeId = '334c30cc-079f-4d3e-a8fa-b641bed515fe';
    const input = { cascadeId, projectEnvConfig: { projectId: project.id } };
    const start = native.call('startCascade', input);
    await vi.waitFor(() => expect(f.sent.at(-1).method).toBe('session/new'));
    f.receive({
      id: f.sent.at(-1).id,
      result: {
        sessionId: 'upstream-session',
        models: { currentModelId: 'remote', availableModels: [{ modelId: 'remote', name: 'Remote model' }] },
      },
    });
    expect((await start).value.cascadeId).toBe(cascadeId);
    expect((await native.call('startCascade', input)).value.cascadeId).toBe(cascadeId);
    expect(f.sent.filter((r) => r.method === 'session/new')).toHaveLength(1);
    expect(manager.workspace().sessions[0].conversationId).toBe(cascadeId);
    const snapshot = await native.snapshot();
    expect(Object.keys(snapshot.summaries)).toEqual([cascadeId]);
    expect(snapshot.projects[0].projectConversations.conversations[0].conversationId).toBe(cascadeId);
    const model = (await native.call('getScopedModelCatalog', { conversationId: cascadeId })).value
      .clientModelConfigs[0];
    await native.call('sendUserCascadeMessage', {
      cascadeId,
      items: [{ chunk: { case: 'text', value: 'First message' } }],
      cascadeConfig: { plannerConfig: { planModel: model.modelOrAlias.choice.value } },
    });
    const prompt = f.sent.at(-1);
    expect(prompt).toMatchObject({
      method: 'session/prompt',
      params: { sessionId: 'upstream-session', prompt: [{ type: 'text', text: 'First message' }] },
    });
    f.receive({
      method: 'session/update',
      params: {
        sessionId: 'upstream-session',
        update: { sessionUpdate: 'agent_message_chunk', content: { type: 'text', text: 'Reply' } },
      },
    });
    f.receive({ id: prompt.id, result: { stopReason: 'end_turn' } });
    const state = (await native.call('streamAgentStateUpdates', { conversationId: cascadeId })).value.update;
    expect(state.conversationId).toBe(cascadeId);
    expect(state.mainTrajectoryUpdate.stepsUpdate.steps[1].step.value.response).toBe('Reply');
    // Importing upstream metadata must not replace a locally allocated conversation ID.
    await manager.action(0, 'sessions-import', {
      projectId: project.id,
      sessions: [{ sessionId: 'upstream-session', conversationId: 'untrusted', title: 'Title' }],
    });
    await native.call('updateConversationAnnotations', {
      cascadeIds: [cascadeId],
      annotations: {
        lastUserViewTime: { seconds: 1794450000n, nanos: 1 },
        cls: [9007199254740993n],
        markedAsUnread: false,
      },
    });
    const restored = new NativeRemoteService(
      new RemoteAcpManager(directory),
      () => f.client,
      async () => ({ active: [] }),
    );
    cleanup.push(() => restored.close());
    expect((await restored.snapshot()).summaries[cascadeId].annotations).toMatchObject({
      lastUserViewTime: { seconds: 1794450000n, nanos: 1 },
      cls: [9007199254740993n],
      markedAsUnread: false,
    });
    const reopen = restored.call('getConversationMetadata', { conversationId: cascadeId });
    await vi.waitFor(() => expect(f.sent.at(-1).method).toBe('session/load'));
    expect(f.sent.at(-1).params.sessionId).toBe('upstream-session');
    f.receive({ id: f.sent.at(-1).id, result: {} });
    expect((await reopen).value.trajectoryId).toBe(cascadeId);
    await expect(
      manager.action(0, 'session-save', { projectId: project.id, sessionId: 'another', conversationId: cascadeId }),
    ).rejects.toThrow('already belongs');
    await expect(
      manager.action(0, 'session-save', {
        projectId: project.id,
        sessionId: 'upstream-session',
        conversationId: '11111111-1111-4111-a111-111111111111',
      }),
    ).rejects.toThrow('already has');
  });

  it('feeds native history, tools, permissions, annotations and cancellation without local fallback', async () => {
    const { NativeRemoteService, remoteConversationId, remoteProjectUri } = await import('../nativeRemote');
    const directory = mkdtempSync(join(tmpdir(), 'agy-native-contract-'));
    cleanup.push(() => rmSync(directory, { recursive: true, force: true }));
    const manager = new RemoteAcpManager(directory);
    await manager.action(1, 'project-save', { name: 'Remote', config });
    const project = manager.workspace().projects[0];
    const f = transport();
    await f.connect();
    const native = new NativeRemoteService(
      manager,
      () => f.client,
      async () => ({ active: [] }),
    );
    cleanup.push(() => native.close());
    const creation = native.call('startCascade', { projectEnvConfig: { projectId: project.id } });
    await new Promise((resolve) => setImmediate(resolve));
    const request = f.sent.find((r) => r.method === 'session/new');
    expect(request.params).toEqual({ cwd: config.cwd, mcpServers: [] });
    f.receive({
      id: request.id,
      result: {
        sessionId: 'not-a-uuid',
        models: {
          currentModelId: 'remote-model',
          availableModels: [{ modelId: 'remote-model', name: 'Remote model' }],
        },
      },
    });
    const result = await creation;
    const id = remoteConversationId(project.id, 'not-a-uuid');
    expect(result).toMatchObject({ handled: true, value: { cascadeId: id } });
    expect(id).toMatch(/^[a-f0-9-]{36}$/);
    await expect(
      native.call('sendUserCascadeMessage', {
        cascadeId: id,
        media: [{ mimeType: 'image/png', payload: { case: 'inlineData', value: new Uint8Array([1, 2]) } }],
      }),
    ).rejects.toThrow('does not advertise image');
    const listed = await native.call('readProjects', { ids: ['local-project', project.id] });
    expect(listed.input.ids).toEqual(['local-project']);
    expect(listed.value.projects[0].projectResources.resources[0].type.value).toBe(remoteProjectUri(project));
    expect(await native.call('getConversationMetadata', { cascadeId: 'local' })).toEqual({ handled: false });
    f.receive({
      method: 'session/update',
      params: {
        sessionId: 'not-a-uuid',
        update: {
          sessionUpdate: 'available_commands_update',
          availableCommands: [{ name: 'help', description: 'Remote help' }],
        },
      },
    });
    expect((await native.call('getSlashCommands', { cascadeId: id })).value.commands[0]).toMatchObject({
      info: { name: 'help', modelFacingText: '/help' },
    });
    await native.call('sendUserCascadeMessage', {
      cascadeId: id,
      items: [
        {
          chunk: {
            case: 'item',
            value: { scopeItem: { case: 'slashCommand', value: { info: { name: 'help' }, argumentValues: {} } } },
          },
        },
        { text: 'Hello' },
      ],
    });
    const prompt = f.sent.find((r) => r.method === 'session/prompt');
    expect(prompt.params).toEqual({
      sessionId: 'not-a-uuid',
      prompt: [
        { type: 'text', text: '/help' },
        { type: 'text', text: 'Hello' },
      ],
    });
    f.receive({
      method: 'session/update',
      params: {
        sessionId: 'not-a-uuid',
        update: { sessionUpdate: 'agent_message_chunk', content: { type: 'text', text: 'Answer' } },
      },
    });
    f.receive({
      method: 'session/update',
      params: {
        sessionId: 'not-a-uuid',
        update: {
          sessionUpdate: 'tool_call',
          toolCallId: 'tool-1',
          title: 'Inspect remote file',
          status: 'pending',
          rawInput: { path: '/remote/work/file' },
        },
      },
    });
    f.receive({
      id: 'permission-rpc',
      method: 'session/request_permission',
      params: {
        sessionId: 'not-a-uuid',
        toolCall: { toolCallId: 'tool-1', title: 'Inspect remote file' },
        options: [
          { optionId: 'approve-actual', kind: 'allow_once', name: 'Allow once' },
          { optionId: 'deny-actual', kind: 'reject_once', name: 'Reject' },
        ],
      },
    });
    const state = (await native.call('streamAgentStateUpdates', { conversationId: id })).value.update;
    expect(state.mainTrajectoryUpdate.stepsUpdate.steps.map((s) => s.step.case)).toEqual([
      'userInput',
      'plannerResponse',
      'generic',
    ]);
    expect(state.mainTrajectoryUpdate.waitingSteps[0]).toMatchObject({ stepIndex: 2, step: { status: 9 } });
    const interaction = {
      cascadeId: id,
      interaction: {
        trajectoryId: id,
        stepIndex: 2,
        interaction: { case: 'permission', value: { allow: true, scope: 1 } },
      },
    };
    await native.call('handleCascadeUserInteraction', interaction);
    expect(f.sent.at(-1)).toMatchObject({
      id: 'permission-rpc',
      result: { outcome: { outcome: 'selected', optionId: 'approve-actual' } },
    });
    await expect(native.call('handleCascadeUserInteraction', interaction)).rejects.toThrow('no longer pending');
    await native.call('cancelCascadeInvocation', { cascadeId: id });
    expect(f.sent.at(-1)).toMatchObject({ method: 'session/cancel', params: { sessionId: 'not-a-uuid' } });
    expect((await native.call('streamAgentStateUpdates', { conversationId: id })).value.update.fullyIdle).toBe(false);
    f.receive({ id: prompt.id, result: { stopReason: 'cancelled' } });
    expect((await native.call('streamAgentStateUpdates', { conversationId: id })).value.update.fullyIdle).toBe(true);
    expect(
      (await native.call('streamAgentStateUpdates', { conversationId: id })).value.update.mainTrajectoryUpdate
        .stepsUpdate.steps[2].status,
    ).toBe(6);
    await native.call('updateConversationAnnotations', {
      cascadeId: id,
      annotations: { title: 'Renamed', archived: true, pinned: true },
    });
    expect((await native.snapshot()).summaries[id].annotations).toMatchObject({
      title: 'Renamed',
      archived: true,
      pinned: true,
    });
    await expect(native.call('writeFile', { uri: remoteProjectUri(project) + '/test', content: [] })).rejects.toThrow(
      'not sent',
    );
    await expect(native.call('runCommand', { conversationId: id })).rejects.toThrow('not sent');
    expect(f.sent.filter((r) => r.method === 'session/prompt')).toHaveLength(1);
  });
});

describe('native SSH frontend client', () => {
  it('republishes an unchanged remote summary after native optimistic Delete fails', async () => {
    vi.useFakeTimers();
    const { installNativeRemoteClient } = await import('../nativeRemotePreload');
    const summary = { summary: 'Remote history', trajectoryId: 'remote' };
    vi.stubGlobal('antigravitySshNative', {
      snapshot: async () => ({ success: true, result: { projects: [], summaries: { remote: summary } } }),
      call: async () => ({ success: false, error: 'Remote deletion refused' }),
    });
    vi.stubGlobal('__AGY_SSH_NATIVE_INSTALLED__', false);
    vi.stubGlobal('__JETSKI_LS_CLIENT_DECORATOR__', undefined);
    vi.stubGlobal('Fi', {
      typeName: 'exa.language_server_pb.LanguageServerService',
      methods: [
        { localName: 'jetboxSubscribeToSummaries', methodKind: 'server_streaming', output: 'summaries' },
        { localName: 'deleteCascadeTrajectory', methodKind: 'unary', output: 'delete' },
      ],
    });
    vi.stubGlobal('$b', (_schema: unknown, value: unknown) => value);
    const localDelete = vi.fn();
    installNativeRemoteClient();
    const client = (globalThis as any).__JETSKI_LS_CLIENT_DECORATOR__({
      jetboxSubscribeToSummaries: async function* (_input: unknown, options: { signal: AbortSignal }) {
        await new Promise<void>((resolve) => options.signal.addEventListener('abort', () => resolve(), { once: true }));
      },
      deleteCascadeTrajectory: localDelete,
    });
    const controller = new AbortController();
    const stream = client.jetboxSubscribeToSummaries({}, { signal: controller.signal });
    expect((await stream.next()).value).toEqual({ updates: { remote: summary }, deletes: [] });
    // The vendor removes the row optimistically. The backend summary did not change.
    await expect(client.deleteCascadeTrajectory({ cascadeId: 'remote' })).rejects.toThrow('Remote deletion refused');
    const restored = stream.next();
    await vi.advanceTimersByTimeAsync(400);
    expect((await restored).value).toEqual({ updates: { remote: summary }, deletes: [] });
    expect(localDelete).not.toHaveBeenCalled();
    controller.abort();
    await stream.return();
  });

  it('selects and sends only models from the active project, including empty offline catalogs', async () => {
    const { installNativeRemoteClient } = await import('../nativeRemotePreload');
    let projectId = 'local';
    const projects: Record<string, any> = {
      local: { backendType: 1 },
      cloud: { backendType: 2 },
      alpha: {
        backendType: 1,
        projectResources: { resources: [{ type: { case: 'folderUri', value: 'agy-ssh://alpha/work' } }] },
      },
      beta: {
        backendType: 1,
        projectResources: { resources: [{ type: { case: 'folderUri', value: 'agy-ssh://beta/work' } }] },
      },
      offline: {
        backendType: 1,
        projectResources: { resources: [{ type: { case: 'folderUri', value: 'agy-ssh://offline/work' } }] },
      },
    };
    const local = { label: 'Gemini', modelId: 'gemini-native', value: 1400 };
    const extra = { label: 'Custom', modelId: 'custom-extra', value: 1500 };
    const disabled = { label: 'Disabled', modelId: 'custom-disabled', value: 1501 };
    const state: any = { modelSelection: { default: { model: 1500 }, byConversation: {} } };
    const catalog = {
      visibleModelConfigs: [local, extra, disabled],
      customModels: [],
      modelSorts: [{ name: 'Recommended', groups: [{ options: [local, extra, disabled] }] }],
    };
    const globals: any = {
      __AGY_SSH_NATIVE_INSTALLED__: false,
      __AGY_PROJECT_UI_INSTALLED__: false,
      __JETSKI_LS_CLIENT_DECORATOR__: undefined,
      Fi: {
        typeName: 'exa.language_server_pb.LanguageServerService',
        methods: [{ localName: 'getCascadeModelConfigs', methodKind: 'unary', output: {} }],
      },
      $b: (_schema: any, value: any) => value,
      A: {
        createElement: (type: any, props: any, ...children: any[]) => ({ type, props: { ...props, children } }),
        createContext: () => ({}),
        useContext: () => undefined,
        useMemo: (fn: any) => fn(),
        useCallback: (fn: any) => fn,
        useSyncExternalStore: (_subscribe: any, get: any) => get(),
        useEffect: (fn: any) => {
          const close = fn();
          if (close) cleanup.push(close);
        },
      },
      Ql: (selector: any) => selector(state),
      LG: () => projectId,
      cS: (id: string) => id?.split(':')[0],
      gm: () => ({ getProjectStateProvider: (id: string) => ({ project: projects[id] }) }),
      Ul: (value: any) => value,
      Kob: (_conversation: any, id: string) => projects[id]?.backendType || 1,
      hu: () => catalog.visibleModelConfigs,
      hCa: () => catalog.modelSorts,
      iCa: () => [],
      iu: () => catalog,
      eS: vi.fn(),
      ytc: vi.fn(),
      FBb: vi.fn(),
      $Y: vi.fn(),
      t4: () => [{ label: 'Native action' }],
      sg: vi.fn(() => 'file:///normalized-local'),
      Lsc: vi.fn(),
      lt: vi.fn(),
      lCa: vi.fn(),
      gCa: () => false,
      mu: () => ({}),
      cna: (configs: any[]) => configs.map((model) => ({ ...model, value: model.modelOrAlias.choice.value })),
      addEventListener: vi.fn(),
    };
    const calls = vi.fn(async (_method: string, input: any) =>
      input.projectId === 'offline'
        ? { success: false, error: 'SSH unavailable' }
        : {
            success: true,
            result: {
              handled: true,
              value: {
                clientModelConfigs: [
                  {
                    label: input.projectId,
                    modelId: `ssh-${input.projectId}`,
                    modelOrAlias: { choice: { case: 'model', value: input.projectId === 'alpha' ? 1502 : 1503 } },
                  },
                ],
              },
            },
          },
    );
    globals.antigravitySshNative = {
      call: calls,
      models: async () => [
        { nativeCatalogId: 'custom-extra' },
        { nativeCatalogId: 'custom-disabled', nodeEnabled: false },
      ],
    };
    for (const [key, value] of Object.entries(globals)) vi.stubGlobal(key, value);
    installNativeRemoteClient();
    const root = globalThis as any;
    root.__JETSKI_LS_CLIENT_DECORATOR__({});
    await Promise.resolve();
    expect(root.Ql(root.hu).map((model: any) => model.label)).toEqual(['Gemini', 'Custom']);
    expect(root.eS()).toBe(extra);
    projectId = 'cloud';
    expect(root.Ql(root.hu).map((model: any) => model.label)).toEqual(['Gemini']);
    expect(root.eS()).toBe(local);
    projectId = 'alpha';
    expect(root.eS()).toBeUndefined();
    await vi.waitFor(() => expect(root.eS()?.label).toBe('alpha'));
    projectId = 'beta';
    await vi.waitFor(() => expect(root.eS()?.label).toBe('beta'));
    expect(root.eS('alpha:conversation')?.label).toBeUndefined();
    await vi.waitFor(() => expect(root.eS('alpha:conversation')?.label).toBe('alpha'));
    projectId = 'offline';
    await vi.waitFor(() => {
      expect(root.eS()).toBeUndefined();
      expect(calls).toHaveBeenCalledWith('getScopedModelCatalog', expect.objectContaining({ projectId: 'offline' }));
    });
    projectId = 'local';
    expect(root.eS()).toBe(extra);
    expect(root.t4({}).map((action: any) => action.label)).toEqual(['Native action', 'New SSH Project']);
    const uri = 'agy-ssh://alpha/remote/path%20with%20spaces';
    expect(root.sg(uri)).toBe(uri);
    const uriObject = { scheme: 'agy-ssh', authority: 'alpha', path: '/remote/path' };
    expect(root.sg(uriObject)).toBe(uriObject);
    expect(root.sg('file:///local/path')).toBe('file:///normalized-local');
    expect(globals.sg).toHaveBeenCalledTimes(1);
    const sshEnvironment = root.Lsc({ resourceManagementProps: { availableResources: [{ type: 0, uri }] } });
    expect(sshEnvironment.props).toMatchObject({ 'data-testid': 'ssh-project-environment', title: uri });
    expect(sshEnvironment.props.children).toContain('SSH');
    const localProps = { resourceManagementProps: { availableResources: [{ type: 0, uri: 'file:///local' }] } };
    expect(root.Lsc(localProps)).toMatchObject({ type: globals.Lsc, props: localProps });
  });

  it('keeps the global account catalog local and merges only projects, without contacting SSH for user status', async () => {
    const { installNativeRemoteClient } = await import('../nativeRemotePreload');
    const model = { label: 'Gemini · High · SSH', modelOrAlias: { choice: { case: 'model', value: 1500 } } };
    const localModel = { label: 'Local Gemini', modelOrAlias: { choice: { case: 'model', value: 1001 } } };
    const localStatus = {
      $typeName: 'GetUserStatusResponse',
      userStatus: {
        $typeName: 'UserStatus',
        pro: false,
        name: 'Real user',
        disableTelemetry: true,
        cascadeModelConfigData: {
          clientModelConfigs: [localModel],
          clientModelSorts: [{ name: 'All', groups: [{ groupName: 'Local', modelLabels: ['Local Gemini'] }] }],
        },
      },
    };
    const localRead = vi.fn(async () => ({ projects: [{ id: 'local' }], notFoundIds: [], errorMessages: {} }));
    const bridge = {
      catalog: vi.fn(async () => ({})),
      call: vi.fn(async (method: string) => ({
        success: true,
        result:
          method === 'getUserStatus'
            ? { handled: true, value: { clientModelConfigs: [model] } }
            : { handled: true, value: { projects: [{ id: 'remote' }] }, input: { ids: ['local'] } },
      })),
    };
    vi.stubGlobal('antigravitySshNative', bridge);
    vi.stubGlobal('__AGY_SSH_NATIVE_INSTALLED__', false);
    vi.stubGlobal('__JETSKI_LS_CLIENT_DECORATOR__', undefined);
    vi.stubGlobal('Fi', {
      typeName: 'exa.language_server_pb.LanguageServerService',
      methods: ['getUserStatus', 'readProjects'].map((localName) => ({
        localName,
        methodKind: 'unary',
        output: localName,
      })),
    });
    vi.stubGlobal('$b', (_schema: unknown, value: unknown) => value);
    installNativeRemoteClient();
    const client = (globalThis as any).__JETSKI_LS_CLIENT_DECORATOR__({
      getUserStatus: async () => localStatus,
      readProjects: localRead,
    });
    const status = await client.getUserStatus({});
    expect(status.userStatus).toMatchObject({ pro: false, name: 'Real user', disableTelemetry: true });
    expect(status.userStatus.cascadeModelConfigData.clientModelConfigs).toEqual([localModel]);
    expect(bridge.call).not.toHaveBeenCalled();
    const projects = await client.readProjects({ ids: ['local', 'remote'] });
    expect(localRead).toHaveBeenCalledWith({ ids: ['local'] }, undefined);
    expect(projects.projects).toEqual([{ id: 'local' }, { id: 'remote' }]);
    bridge.call.mockRejectedValueOnce(new Error('Remote offline'));
    expect(await client.getUserStatus({})).toBe(localStatus);
  });
});

describe('native swarm SSH integration', () => {
  const prefix = '_maichart/swarm/';
  const capabilities = {
    version: 1,
    implementation: 'native-agy',
    approvalMode: 'auto',
    methods: ['subscribe', 'snapshot', 'history', 'attach', 'sessions'].map((name) => prefix + name),
    readOnlyAttach: true,
    notification: prefix + 'update',
    directWorkerControl: false,
    endTurnMeansSwarmComplete: false,
  };
  const initialize = { protocolVersion: 1, agentCapabilities: { _meta: { 'maichart/nativeSwarm': capabilities } } };
  const snapshot = (extra: Record<string, any> = {}) => ({
    sessionId: 'root',
    source: 'native-local-files',
    runtimeVerified: false,
    nativeProcessAlive: true,
    coordinatorPromptActive: false,
    swarm: { id: 'root', title: 'Team' },
    members: [
      {
        name: 'worker-a',
        conversationId: 'worker-native-id',
        parentSessionId: 'root',
        native: { phase: 'RUNNING', applied_model: 'gemini-4-carbon-high' },
        transcriptRevision: 'r1',
      },
    ],
    messages: [{ seq: 1, agent: 'coordinator', text: 'brief' }],
    cursor: 1,
    head: 1,
    more: false,
    etag: 'e1',
    ...extra,
  });
  it('paginates and deduplicates the board, replaces rewritten worker records, and preserves read errors', async () => {
    const { NativeSwarmView, nativeSwarmCapabilities } = await import('../nativeSwarm');
    const cards = new Map<string, { markdown: string; error: boolean }>();
    const request = vi.fn(async (method: string, _params: any): Promise<any> => {
      if (method === prefix + 'subscribe') return { subscribed: true, snapshot: snapshot({ more: true, head: 2 }) };
      if (method === prefix + 'snapshot')
        return snapshot({ messages: [{ seq: 2, agent: 'worker-a', text: 'board-result' }], cursor: 2, head: 2 });
      return {
        sessionId: 'root',
        member: 'worker-a',
        conversationId: 'worker-native-id',
        source: 'native-transcript',
        records: [{ step_index: 0, content: 'original-worker-record' }],
        cursor: { offset: 100, generation: 'g1' },
        more: false,
      };
    });
    expect(nativeSwarmCapabilities(initialize)).toBe(capabilities);
    expect(nativeSwarmCapabilities({ agentCapabilities: {} })).toBeUndefined();
    expect(
      nativeSwarmCapabilities({
        agentCapabilities: { _meta: { 'maichart/nativeSwarm': { ...capabilities, methods: {} } } },
      }),
    ).toBeUndefined();
    const view = new NativeSwarmView({ request }, 'root', capabilities, (key, _title, markdown, error) =>
      cards.set(key, { markdown, error }),
    );
    cleanup.push(() => view.close());
    await view.start();
    await vi.waitFor(() =>
      expect([...cards.values()].some((c) => c.markdown.includes('original-worker-record'))).toBe(true),
    );
    expect(request).toHaveBeenCalledWith(prefix + 'snapshot', { sessionId: 'root', after: 1, limit: 100 });
    expect(cards.get('swarm-board')!.markdown.match(/board-result/g)).toHaveLength(1);
    view.update(snapshot({ messages: [{ seq: 1, agent: 'coordinator', text: 'brief' }], head: 2 }));
    expect(cards.get('swarm-board')!.markdown.match(/board-result/g)).toHaveLength(1);
    expect(cards.get('swarm-board')!.markdown).toContain('do not prove liveness or team completion');
    expect(cards.get('swarm-board')!.markdown).toContain('Approval mode: auto');
    view.update({ sessionId: 'root', readError: 'native read failed' });
    expect(cards.get('swarm-board')).toMatchObject({ error: true });
    expect(cards.get('swarm-board')!.markdown).toContain('board-result');
    expect(cards.get('swarm-board')!.markdown).toContain('stale');
    let page = 0;
    request.mockImplementation(async (method, params) => {
      if (method === prefix + 'history') {
        page++;
        if (page === 1)
          return {
            sessionId: 'root',
            member: 'worker-a',
            conversationId: 'worker-native-id',
            source: 'native-transcript',
            records: [{ content: 'removed-on-reset' }],
            cursor: { offset: 200, generation: 'g2' },
            more: true,
          };
        if (page === 2)
          return {
            sessionId: 'root',
            member: 'worker-a',
            conversationId: 'worker-native-id',
            source: 'native-transcript',
            records: [],
            resetRequired: true,
            cursor: { offset: 0, generation: 'g3' },
          };
        expect(params.cursor).toBeUndefined();
        return {
          sessionId: 'root',
          member: 'worker-a',
          conversationId: 'worker-native-id',
          source: 'native-transcript',
          records: [{ content: 'rewritten-worker-record' }],
          cursor: { offset: 70, generation: 'g3' },
          more: false,
        };
      }
      throw new Error('Unexpected board request');
    });
    vi.spyOn(Date, 'now').mockReturnValue(Date.now() + 2000);
    cleanup.push(() => vi.restoreAllMocks());
    view.update(
      snapshot({
        resetRequired: true,
        messages: [],
        cursor: 0,
        head: 0,
        etag: 'e2',
        members: [
          {
            name: 'worker-a',
            conversationId: 'worker-native-id',
            parentSessionId: 'root',
            native: { phase: 'WAITING' },
            transcriptRevision: 'r2',
          },
        ],
      }),
    );
    await vi.waitFor(() => {
      view.refresh();
      expect([...cards.values()].some((c) => c.markdown.includes('rewritten-worker-record'))).toBe(true);
    });
    const worker = [...cards.entries()].find(([key]) => key.startsWith('["swarm-member"'))![1];
    expect(worker.markdown).not.toContain('original-worker-record');
    expect(worker.markdown).not.toContain('removed-on-reset');
    expect(cards.get('swarm-board')!.markdown).not.toContain('board-result');
    expect(cards.get('swarm-board')!.error).toBe(false);
    vi.restoreAllMocks();
  });
  it('keeps background approvals after a coordinator end_turn and cancel', async () => {
    const f = transport();
    const connected = f.client.connect();
    f.receive({ id: f.sent.at(-1).id, result: initialize });
    await connected;
    const turn = f.client.request('session/prompt', {
      sessionId: 'root',
      prompt: [{ type: 'text', text: '/swarm Test' }],
    });
    const prompt = f.sent.at(-1);
    f.receive({
      id: 'background-permission',
      method: 'session/request_permission',
      params: { sessionId: 'root', options: [{ optionId: 'once', kind: 'allow_once' }] },
    });
    f.receive({ id: prompt.id, result: { stopReason: 'end_turn' } });
    await turn;
    expect(f.client.snapshot().pending).toHaveLength(1);
    await f.client.request('session/cancel', { sessionId: 'root' });
    expect(f.client.snapshot().pending).toHaveLength(1);
    expect(f.sent.filter((m) => m.id === 'background-permission')).toHaveLength(0);
    expect(f.sent.at(-1)).toMatchObject({ method: 'session/cancel' });
  });
  it('attaches live swarm views in the existing conversation without reloading or registering workers as sessions', async () => {
    const { NativeRemoteService, remoteConversationId } = await import('../nativeRemote');
    const directory = mkdtempSync(join(tmpdir(), 'agy-native-swarm-'));
    cleanup.push(() => rmSync(directory, { recursive: true, force: true }));
    const manager = new RemoteAcpManager(directory);
    await manager.action(1, 'project-save', { name: 'Swarm', config: { ...config, instance: 'agy-swarm' } });
    const project = manager.workspace().projects[0];
    await manager.action(1, 'session-save', { projectId: project.id, sessionId: 'root' });
    const f = transport({ ...config, instance: 'agy-swarm' });
    const connected = f.client.connect();
    f.receive({ id: f.sent.at(-1).id, result: initialize });
    await connected;
    const native = new NativeRemoteService(
      manager,
      () => f.client,
      async () => ({ active: [] }),
    );
    cleanup.push(() => native.close());
    const id = remoteConversationId(project.id, 'root');
    const statePromise = native.call('streamAgentStateUpdates', { conversationId: id });
    await vi.waitFor(() => expect(f.sent.some((m) => m.method === prefix + 'attach')).toBe(true));
    const attach = f.sent.find((m) => m.method === prefix + 'attach');
    const configOptions = [
      {
        id: 'model',
        type: 'select',
        currentValue: 'gemini-4-carbon',
        options: [{ value: 'gemini-4-carbon', name: 'Carbon' }],
      },
      { id: 'thought_level', type: 'select', currentValue: 'high', options: [{ value: 'high', name: 'High' }] },
    ];
    f.receive({
      id: attach.id,
      result: {
        sessionId: 'root',
        attached: true,
        readOnly: true,
        source: 'observed-native-acp',
        generation: 'persistent-1',
        metadata: {
          nativeSession: { configOptions, models: { currentModelId: 'stale' }, availableCommands: [{ name: 'swarm' }] },
          staleFields: ['models'],
          freshness: 'last-observed-not-live-query',
        },
        snapshot: snapshot(),
      },
    });
    await vi.waitFor(() => expect(f.sent.some((m) => m.method === prefix + 'subscribe')).toBe(true));
    const subscribe = f.sent.find((m) => m.method === prefix + 'subscribe');
    f.receive({ id: subscribe.id, result: { subscribed: true, snapshot: snapshot() } });
    const state = (await statePromise).value.update;
    expect(state.fullyIdle).toBe(true); // Coordinator is idle; worker files still report RUNNING.
    expect(state.mainTrajectoryUpdate.stepsUpdate.steps[0].step.value.result.stepRenderInfo.title).toContain(
      'Native swarm',
    );
    const history = f.sent.find((m) => m.method === prefix + 'history');
    expect(history.params).toMatchObject({ sessionId: 'root', member: 'worker-a' });
    f.receive({
      id: history.id,
      result: {
        sessionId: 'root',
        member: 'worker-a',
        conversationId: 'worker-native-id',
        source: 'native-transcript',
        records: [{ content: 'worker-body' }],
        cursor: { offset: 10, generation: 'g' },
        more: false,
      },
    });
    await vi.waitFor(async () => {
      const updated = (await native.call('streamAgentStateUpdates', { conversationId: id })).value.update;
      expect(
        JSON.stringify(updated, (_key, value) => (typeof value === 'bigint' ? value.toString() : value)),
      ).toContain('worker-body');
    });
    expect(f.client.snapshot().sessions[0].configOptions).toEqual(configOptions);
    expect(f.client.snapshot().sessions[0].models).toBeUndefined();
    expect((await native.call('getSlashCommands', { cascadeId: id })).value.commands[0].info.name).toBe('swarm');
    const catalog = (await native.call('getScopedModelCatalog', { conversationId: id })).value.clientModelConfigs;
    expect(catalog[0].label).toContain('Carbon · High');
    expect(f.sent.some((m) => m.method === 'session/load')).toBe(false);
    expect(manager.workspace().sessions.map((s) => s.sessionId)).toEqual(['root']);
    await expect(native.call('forceStopCascadeTree', { cascadeId: id })).rejects.toThrow('cannot stop the whole swarm');
    f.receive({ method: prefix + 'update', params: { sessionId: 'root', readError: 'board unavailable' } });
    const failed = (await native.call('streamAgentStateUpdates', { conversationId: id })).value.update;
    expect(failed.mainTrajectoryUpdate.stepsUpdate.steps[0].status).toBe(7);
    expect(failed.mainTrajectoryUpdate.stepsUpdate.steps[0].step.value.result.result).toContain('board unavailable');
    f.receive({ method: prefix + 'error', params: { message: 'Native ACP exited' } });
    const exited = (await native.call('streamAgentStateUpdates', { conversationId: id })).value.update;
    expect(exited.mainTrajectoryUpdate.stepsUpdate.steps[0].step.value.result.result).toContain('Native ACP exited');
  });
});
