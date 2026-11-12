/** Translate SSH ACP sessions into the desktop's existing language-service contracts. */
import { createHash } from 'node:crypto';
import { spawn } from 'node:child_process';
import { allocateModelIdentities } from './modelIdentity';
import { readModelConfig } from './modelStore';
import * as path from 'node:path';
import { rememberNativeGeminiCatalog } from './customNodes';
import { NativeSwarmView, nativeSwarmCapabilities } from './nativeSwarm';
import {
  RemoteAcpClient,
  RemoteAcpManager,
  remoteAcpStatus,
  type RemoteAcpConfig,
  type RemoteAcpProject,
} from './remoteAcp';

type Data = Record<string, any>;
interface Conversation {
  id: string;
  project: RemoteAcpProject;
  sessionId: string;
  steps: Data[];
  commands?: Data[];
  loaded: boolean;
  loading?: Promise<void>;
  active: boolean;
  error?: string;
  tools: Map<string, number>;
  permissions: Map<number, string>;
  revision: number;
  swarm?: NativeSwarmView;
  swarmSteps?: Map<string, number>;
}
interface Connection {
  client: RemoteAcpClient;
  cursor: number;
  ready: Promise<void>;
  connecting: boolean;
  lastStatus: number;
  statusPending?: Promise<void>;
}
export function remoteConversationId(projectId: string, sessionId: string, conversationId?: string): string {
  if (conversationId) return conversationId;
  const h = createHash('sha256')
    .update(JSON.stringify(['agy-ssh', projectId, sessionId]))
    .digest('hex');
  return `${h.slice(0, 8)}-${h.slice(8, 12)}-5${h.slice(13, 16)}-a${h.slice(17, 20)}-${h.slice(20, 32)}`;
}
export function remoteProjectUri(project: RemoteAcpProject): string {
  return `agy-ssh://${project.id}${project.config.cwd.split('/').map(encodeURIComponent).join('/')}`;
}
function timestamp(date: string | number = Date.now()): Data {
  const ms = typeof date === 'string' ? Date.parse(date) : date;
  return { seconds: BigInt(Math.floor(ms / 1000)), nanos: (ms % 1000) * 1000000 };
}
function contentText(block: Data): string {
  if (block?.type === 'text') return block.text || '';
  if (block?.type === 'resource_link') return `[${block.name || block.uri}](${block.uri})`;
  if (block?.type === 'resource') return block.resource?.text || block.resource?.uri || '';
  if (block?.type === 'diff') return `\`\`\`diff\n${block.oldText || ''}\n${block.newText || ''}\n\`\`\``;
  return JSON.stringify(block);
}
export class NativeRemoteService {
  private connections = new Map<string, Connection>();
  private refreshes = new Map<string, { at: number; pending?: Promise<void> }>();
  private pages = new Map<string, { conversationId: string; bounds: Data; followTail: boolean }>();
  private drafts = new Map<string, Promise<{ connection: Connection; sessionId: string; signature: string }>>();
  private conversations = new Map<string, Conversation>();
  private deletions = new Map<string, Promise<void>>();
  private deletedSessions = new Set<string>();
  private deletedConversations = new Set<string>();
  private modelSelections = new Map<
    string | number,
    { projectId: string; modelId: string; configModel?: string; thoughtLevel?: string }
  >();
  constructor(
    readonly manager: RemoteAcpManager,
    private createClient = (config: RemoteAcpConfig) => new RemoteAcpClient(config),
    private status = remoteAcpStatus,
  ) {}

  private conversation(id: string): Conversation | undefined {
    let conversation = this.conversations.get(id);
    if (conversation) return conversation;
    const workspace = this.manager.workspace();
    for (const session of workspace.sessions) {
      if (remoteConversationId(session.projectId, session.sessionId, session.conversationId) !== id) continue;
      const project = workspace.projects.find((p) => p.id === session.projectId);
      if (!project) return;
      conversation = {
        id,
        project,
        sessionId: session.sessionId,
        steps: [],
        loaded: false,
        active: false,
        tools: new Map(),
        permissions: new Map(),
        revision: 0,
      };
      this.conversations.set(id, conversation);
      return conversation;
    }
  }
  private endpoint(project: RemoteAcpProject): string {
    return JSON.stringify([project.config.host, project.config.command, project.config.instance]);
  }
  private sessionKey(project: RemoteAcpProject, sessionId: string): string {
    return JSON.stringify([this.endpoint(project), sessionId]);
  }
  private deleteConversation(conversation: Conversation): Promise<void> {
    const key = this.sessionKey(conversation.project, conversation.sessionId);
    const pending = this.deletions.get(key);
    if (pending) return pending;
    const deletion = (async () => {
      const connection = await this.connect(conversation.project);
      const aliases = [...this.conversations.values()].filter((c) => this.sessionKey(c.project, c.sessionId) === key);
      // Finish existing history reads; never open/load a session just to delete it.
      await Promise.allSettled(aliases.flatMap((c) => (c.loading ? [c.loading] : [])));
      if (aliases.some((c) => c.active))
        throw new Error('Wait for this remote session’s tasks to finish before deleting it.');
      await connection.client.deleteSession(conversation.sessionId);
      // Reject late list results and prompt completion writes after remote success,
      // including when saving the local index fails and deletion needs a retry.
      this.deletedSessions.add(key);
      const removed = this.manager.removeDeletedSession(conversation.project.config, conversation.sessionId);
      for (const session of removed)
        this.deletedConversations.add(
          remoteConversationId(session.projectId, session.sessionId, session.conversationId),
        );
      this.deletedConversations.add(conversation.id);
      for (const c of aliases) {
        c.swarm?.close();
        this.conversations.delete(c.id);
        this.deletedConversations.add(c.id);
      }
      for (const [subscriber, page] of this.pages)
        if (this.deletedConversations.has(page.conversationId)) this.pages.delete(subscriber);
    })().finally(() => this.deletions.delete(key));
    this.deletions.set(key, deletion);
    return deletion;
  }
  private async connect(project: RemoteAcpProject): Promise<Connection> {
    const key = this.endpoint(project);
    let connection = this.connections.get(key);
    // Before initialize starts, the client is still disconnected while status is in flight.
    // All page, history and model requests must share that pending connection.
    if (connection && !connection.connecting && connection.client.snapshot().state === 'disconnected') {
      this.connections.delete(key);
      for (const c of this.conversations.values())
        if (this.endpoint(c.project) === key) {
          c.swarm?.close();
          c.swarm = undefined;
          c.loaded = false;
        }
      connection = undefined;
    }
    if (!connection) {
      const client = this.createClient(project.config);
      connection = { client, cursor: 0, lastStatus: 0, ready: undefined!, connecting: true };
      this.connections.set(key, connection);
      connection.ready = (async () => {
        client.syncRemoteStatus(await this.status(project.config));
        await client.connect();
        client.syncRemoteStatus(await this.status(project.config));
        connection!.lastStatus = Date.now();
      })()
        .catch((error) => {
          client.disconnect();
          if (this.connections.get(key) === connection) this.connections.delete(key);
          throw error;
        })
        .finally(() => {
          connection!.connecting = false;
        });
    }
    await connection.ready;
    if (connection.client.snapshot().state !== 'connected') throw new Error(connection.client.snapshot().failure);
    return connection;
  }
  private refreshProject(project: RemoteAcpProject): Promise<void> {
    const previous = this.refreshes.get(project.id);
    if (previous?.pending) return previous.pending;
    if (previous && Date.now() - previous.at < 60000) return Promise.resolve();
    const refresh = { at: Date.now(), pending: undefined as Promise<void> | undefined };
    this.refreshes.set(project.id, refresh);
    refresh.pending = (async () => {
      const connection = await this.connect(project);
      if (!connection.client.snapshot().initialize?.agentCapabilities?.sessionCapabilities?.list) return;
      let cursor: string | undefined;
      for (let page = 0; page < 8; page++) {
        const result = await connection.client.request('session/list', {
          cwd: project.config.cwd,
          ...(cursor ? { cursor } : {}),
        });
        if (!Array.isArray(result.sessions)) throw new Error('Invalid remote session list.');
        const draftSessionId = (await this.drafts.get(project.id))?.sessionId;
        for (let offset = 0; offset < result.sessions.length; offset += 256)
          await this.manager.action(0, 'sessions-import', {
            projectId: project.id,
            sessions: result.sessions
              .slice(offset, offset + 256)
              .filter(
                (session: Data) =>
                  session.sessionId !== draftSessionId &&
                  !this.deletedSessions.has(this.sessionKey(project, session.sessionId)),
              ),
          });
        if (!result.nextCursor || result.nextCursor === cursor) break;
        cursor = result.nextCursor;
      }
    })().finally(() => {
      refresh.pending = undefined;
    });
    return refresh.pending;
  }
  private async attachSwarm(conversation: Conversation, connection: Connection, restore = false): Promise<boolean> {
    const capabilities = nativeSwarmCapabilities(connection.client.snapshot().initialize);
    if (!capabilities) return false;
    let attachment: Data | undefined;
    if (restore && capabilities.readOnlyAttach && capabilities.methods.includes('_maichart/swarm/attach')) {
      attachment = await connection.client.request('_maichart/swarm/attach', { sessionId: conversation.sessionId });
      const metadata = attachment!.metadata || {};
      if (!metadata.staleFields?.includes('availableCommands'))
        conversation.commands = metadata.nativeSession?.availableCommands;
    }
    if (!conversation.swarm) {
      conversation.swarmSteps ||= new Map();
      conversation.swarm = new NativeSwarmView(
        connection.client,
        conversation.sessionId,
        capabilities,
        (key, title, markdown, readError) => {
          let index = conversation.swarmSteps!.get(key);
          if (index === undefined) {
            index = conversation.steps.length;
            this.step(
              conversation,
              132,
              'generic',
              {
                args: {},
                result: { result: markdown, stepRenderInfo: { title, markdown } },
              },
              readError ? 7 : 3,
            );
            conversation.swarmSteps!.set(key, index);
          } else {
            const step = conversation.steps[index];
            step.status = readError ? 7 : 3;
            step.step.value.result = { result: markdown, stepRenderInfo: { title, markdown } };
            conversation.revision++;
          }
        },
      );
    }
    if (attachment) {
      conversation.swarm.metadataNotice(attachment.metadata);
      if (attachment.snapshot) conversation.swarm.update(attachment.snapshot);
      if (attachment.snapshotError) conversation.swarm.fail(new Error(attachment.snapshotError));
      try {
        await conversation.swarm.start();
      } catch (error) {
        conversation.swarm.fail(error as Error);
      }
    } else await conversation.swarm.start();
    return true;
  }
  private async load(conversation: Conversation): Promise<Connection> {
    const key = this.sessionKey(conversation.project, conversation.sessionId);
    if (this.deletions.has(key) || this.deletedSessions.has(key))
      throw new Error('This remote conversation is being deleted or has been deleted.');
    const connection = await this.connect(conversation.project);
    if (this.deletions.has(key) || this.deletedSessions.has(key))
      throw new Error('This remote conversation is being deleted or has been deleted.');
    if (conversation.loaded) {
      conversation.swarm?.refresh();
      return connection;
    }
    if (!conversation.loading) {
      conversation.loading = (async () => {
        // A persistent swarm may already be open upstream. Subscribe to its read-only
        // view first: loading that live coordinator again can disrupt native workers.
        if (nativeSwarmCapabilities(connection.client.snapshot().initialize)) {
          try {
            if (await this.attachSwarm(conversation, connection, true)) {
              conversation.loaded = true;
              conversation.active = connection.client.snapshot().activeSessions.includes(conversation.sessionId);
              this.drain(connection);
              return;
            }
          } catch (error) {
            // Only a root absent from the persistent adapter may use ACP load.
            // Read errors, transport failures and live-session refusals must not reload it.
            if (!/Coordinator is not open|Open or load the coordinator/.test((error as Error).message)) throw error;
          }
        }
        if (connection.client.snapshot().activeSessions.includes(conversation.sessionId)) {
          // Loading an active ACP session is forbidden. Continue live updates and permit cancellation.
          conversation.active = true;
          conversation.error =
            'This remote session is still running. Its live updates are attached; history can be loaded after it finishes.';
          return;
        }
        conversation.swarm?.close();
        conversation.swarm = undefined;
        conversation.swarmSteps = undefined;
        conversation.steps = [];
        conversation.tools.clear();
        this.drain(connection);
        await connection.client.request('session/load', {
          sessionId: conversation.sessionId,
          cwd: conversation.project.config.cwd,
          mcpServers: conversation.project.mcpServers,
        });
        this.drain(connection);
        for (const step of conversation.steps) if (step.status === 8) step.status = 3;
        conversation.loaded = true;
        conversation.error = undefined;
        try {
          await this.attachSwarm(conversation, connection);
        } catch (error) {
          conversation.swarm?.fail(error as Error);
        }
        conversation.revision++;
      })().finally(() => {
        conversation.loading = undefined;
      });
    }
    await conversation.loading;
    return connection;
  }
  private step(conversation: Conversation, type: number, name: string, value: Data, status = 3): Data {
    if (conversation.steps.length >= 20000) throw new Error('Remote conversation exceeds 20000 native steps.');
    const step = {
      type,
      status,
      metadata: { createdAt: timestamp(), startedAt: timestamp() },
      step: { case: name, value },
    };
    conversation.steps.push(step);
    conversation.revision++;
    return step;
  }
  private drain(connection: Connection): void {
    const snapshot = connection.client.snapshot(connection.cursor);
    if (snapshot.gap) {
      for (const c of this.conversations.values())
        if (this.endpoint(c.project) === this.endpoint({ config: snapshot.config } as RemoteAcpProject)) {
          c.error =
            'Remote event history has a gap. Reopen this conversation after the current task finishes to reload its history.';
          if (c.swarm) c.swarm.noteGap();
          else c.loaded = false;
        }
    }
    for (const event of snapshot.events) {
      connection.cursor = event.seq;
      const data = event.data;
      if (event.kind === 'notification' && data.method === '_maichart/swarm/error') {
        for (const c of this.conversations.values())
          if (this.endpoint(c.project) === this.endpoint({ config: snapshot.config } as RemoteAcpProject))
            c.swarm?.fail(new Error(data.params?.message || 'The remote native swarm reported an error.'));
        continue;
      }
      const sessionId = data.params?.sessionId || data.sessionId;
      const matches = [...this.conversations.values()].filter(
        (c) =>
          c.sessionId === sessionId &&
          this.endpoint(c.project) === this.endpoint({ config: snapshot.config } as RemoteAcpProject),
      );
      for (const c of matches) {
        if (event.kind === 'user_prompt') {
          this.step(c, 14, 'userInput', {
            items: data.prompt
              .filter((b: Data) => b.type === 'text')
              .map((b: Data) => ({ chunk: { case: 'text', value: b.text } })),
          });
          c.active = true;
        } else if (event.kind === 'notification' && data.method === '_maichart/swarm/update') {
          try {
            c.swarm?.update(data.params);
          } catch (error) {
            c.swarm?.fail(error as Error);
          }
        } else if (event.kind === 'notification' && data.method === 'session/update') {
          const update = data.params.update;
          const kind = update.sessionUpdate;
          if (kind === 'available_commands_update') {
            c.commands = Array.isArray(update.availableCommands) ? update.availableCommands : [];
          } else if (kind === 'user_message_chunk') {
            for (const step of c.steps) if (step.status === 8) step.status = 3;
            const last = c.steps[c.steps.length - 1];
            if (last?.step.case === 'userInput')
              last.step.value.items.push({ chunk: { case: 'text', value: contentText(update.content) } });
            else
              this.step(c, 14, 'userInput', {
                items: [{ chunk: { case: 'text', value: contentText(update.content) } }],
              });
          } else if (kind === 'agent_message_chunk' || kind === 'agent_thought_chunk') {
            let last = c.steps[c.steps.length - 1];
            if (last?.step.case !== 'plannerResponse' || last.status === 3)
              last = this.step(c, 15, 'plannerResponse', { response: '', thinking: '' }, 8);
            const field = kind === 'agent_thought_chunk' ? 'thinking' : 'response';
            last.step.value[field] += contentText(update.content);
          } else if (kind === 'tool_call' || kind === 'tool_call_update') {
            for (const step of c.steps) if (step.status === 8) step.status = 3;
            let index = c.tools.get(update.toolCallId);
            if (index === undefined) {
              index = c.steps.length;
              this.step(
                c,
                132,
                'generic',
                {
                  args: {},
                  result: { result: '', stepRenderInfo: { title: update.title || update.toolCallId, markdown: '' } },
                },
                1,
              );
              c.tools.set(update.toolCallId, index);
            }
            const step = c.steps[index];
            const value = step.step.value;
            if (update.title) value.result.stepRenderInfo.title = update.title;
            if (update.rawInput !== undefined) value.args = { input: JSON.stringify(update.rawInput) };
            if (update.content)
              value.result.result = value.result.stepRenderInfo.markdown = update.content
                .map((b: Data) => contentText(b.content || b))
                .join('\n\n');
            if (update.rawOutput !== undefined && !update.content)
              value.result.result = value.result.stepRenderInfo.markdown =
                typeof update.rawOutput === 'string' ? update.rawOutput : JSON.stringify(update.rawOutput, null, 2);
            if (update.status)
              step.status = ({ pending: 1, in_progress: 2, completed: 3, failed: 7 } as Data)[update.status] || 2;
          } else if (kind === 'plan') {
            this.step(c, 132, 'generic', {
              args: {},
              result: {
                result: '',
                stepRenderInfo: {
                  title: 'Remote plan',
                  markdown: (update.entries || [])
                    .map((entry: Data) => `- [${entry.status === 'completed' ? 'x' : ' '}] ${entry.content}`)
                    .join('\n'),
                },
              },
            });
          }
          c.revision++;
        } else if ((event.kind === 'rpc_result' || event.kind === 'rpc_error') && data.method === 'session/prompt') {
          c.active = false;
          for (const step of c.steps)
            if (step.status === 8 || (data.result?.stopReason === 'cancelled' && [1, 2, 9].includes(step.status)))
              step.status = data.error ? 7 : data.result?.stopReason === 'cancelled' ? 6 : 3;
          if (data.error)
            this.step(
              c,
              17,
              'errorMessage',
              { shouldShowUser: true, error: { userErrorMessage: data.error.message } },
              7,
            );
          c.revision++;
        }
      }
    }
    for (const c of this.conversations.values()) {
      if (this.endpoint(c.project) !== this.endpoint({ config: snapshot.config } as RemoteAcpProject)) continue;
      const active = snapshot.activeSessions.includes(c.sessionId);
      if (c.active !== active) {
        c.active = active;
        c.revision++;
      }
      if (snapshot.state === 'disconnected') {
        c.error = snapshot.failure;
        c.loaded = false;
        c.swarm?.fail(new Error(snapshot.failure));
        c.revision++;
      }
      const pending = snapshot.pending.filter((p) => p.params.sessionId === c.sessionId);
      const pendingTokens = new Set(pending.map((p) => p.token));
      for (const [index, token] of c.permissions)
        if (!pendingTokens.has(token)) {
          delete c.steps[index].requestedInteraction;
          if (c.steps[index].status === 9) c.steps[index].status = 2;
          c.permissions.delete(index);
          c.revision++;
        }
      for (const request of pending) {
        if ([...c.permissions.values()].includes(request.token)) continue;
        if (request.method !== 'session/request_permission') continue;
        const options = request.params.options || [];
        const index = c.tools.get(request.params.toolCall?.toolCallId) ?? c.steps.length;
        if (index === c.steps.length)
          this.step(
            c,
            132,
            'generic',
            {
              args: {},
              result: {
                stepRenderInfo: {
                  title: request.params.toolCall?.title || 'Remote permission request',
                  markdown: options.map((o: Data) => `${o.name} (${o.kind})`).join('\n'),
                },
              },
            },
            9,
          );
        const step = c.steps[index];
        step.status = 9;
        step.requestedInteraction = {
          interaction: {
            case: 'permission',
            value: {
              resource: {
                action: 'SSH remote tool',
                target: request.params.toolCall?.title || request.params.toolCall?.toolCallId || 'Remote action',
              },
              reason: options.map((o: Data) => `${o.name} (${o.kind})`).join('; '),
              actionDescription: request.params.toolCall?.title || 'Remote tool',
              approvalScopeFilter: [1],
            },
          },
        };
        c.permissions.set(index, request.token);
        c.revision++;
      }
    }
  }
  private async draft(
    project: RemoteAcpProject,
  ): Promise<{ connection: Connection; sessionId: string; signature: string }> {
    const connection = await this.connect(project);
    const signature = JSON.stringify([project.config, project.mcpServers]);
    const saved = this.drafts.get(project.id);
    if (saved) {
      const previous = await saved;
      if (previous.connection === connection && previous.signature === signature) return previous;
    }
    const pending = connection.client
      .request('session/new', {
        cwd: project.config.cwd,
        mcpServers: project.mcpServers,
      })
      .then((result) => ({ connection, sessionId: result.sessionId, signature }));
    this.drafts.set(project.id, pending);
    try {
      return await pending;
    } catch (error) {
      this.drafts.delete(project.id);
      throw error;
    }
  }
  private remoteModels(project: RemoteAcpProject, sessionId: string): Data[] {
    const models: Data[] = [];
    const connection = this.connections.get(this.endpoint(project));
    const session = connection?.client.snapshot().sessions.find((entry) => entry.sessionId === sessionId);
    const modelOption = session?.configOptions?.find((o: Data) => o.id === 'model');
    const thoughtOption = session?.configOptions?.find((o: Data) => o.id === 'thought_level');
    const modelOptions = (modelOption?.options || []).flatMap((o: Data) => o.options || [o]);
    const thoughtOptions = (thoughtOption?.options || []).flatMap((o: Data) => o.options || [o]);
    thoughtOptions.sort((a: Data, b: Data) => Number(b.value === 'medium') - Number(a.value === 'medium'));
    if (modelOptions.length) {
      for (const base of modelOptions)
        for (const effort of thoughtOptions.length ? thoughtOptions : [undefined]) {
          const name = `models/ssh-${project.id}-${base.value}-${effort?.value || 'default'}`;
          if (!models.some((m) => m.name === name))
            models.push({
              name,
              modelId: base.value,
              configModel: base.value,
              thoughtLevel: effort?.value,
              projectId: project.id,
              displayName: `${base.name || base.value}${effort ? ` · ${effort.name || effort.value}` : ''}`,
              supportsImages:
                connection?.client.snapshot().initialize?.agentCapabilities?.promptCapabilities?.image === true,
            });
        }
    } else
      for (const model of session?.models?.availableModels || []) {
        const name = `models/ssh-${project.id}-${model.modelId}`;
        if (!models.some((m) => m.name === name))
          models.push({
            name,
            modelId: model.modelId,
            projectId: project.id,
            displayName: `${model.name || model.modelId}`,
            supportsImages:
              connection?.client.snapshot().initialize?.agentCapabilities?.promptCapabilities?.image === true,
          });
      }
    if (!models.length) return [];
    const directory = this.manager.directory;
    const saved = readModelConfig(path.join(directory, 'custom_models.json')).filter(
      (model) => !model.nativeOverrideId,
    );
    const identities = allocateModelIdentities(directory, [...saved, ...models] as any);
    return models.flatMap((model) => {
      const identity = identities.find((i) => i.name === model.name);
      if (!identity?.placeholderValue) return [];
      const selection = {
        projectId: model.projectId,
        modelId: model.modelId,
        configModel: model.configModel,
        thoughtLevel: model.thoughtLevel,
      };
      this.modelSelections.set(identity.placeholderValue, selection);
      this.modelSelections.set(identity.catalogId, selection);
      return [
        {
          label: model.displayName,
          modelId: identity.catalogId,
          modelOrAlias: { choice: { case: 'model', value: identity.placeholderValue } },
          supportsImages: model.supportsImages === true,
          supportedMimeTypes: model.supportsImages
            ? { 'image/png': true, 'image/jpeg': true, 'image/webp': true, 'image/gif': true }
            : {},
        },
      ];
    });
  }
  private metadata(c: Conversation): Data {
    return {
      projectId: c.project.id,
      environmentId: c.project.id,
      rootConversationId: c.id,
      workspaceUris: [remoteProjectUri(c.project)],
      workspaces: [{ workspaceFolderAbsoluteUri: remoteProjectUri(c.project) }],
    };
  }
  private nativeProject(project: RemoteAcpProject): Data {
    const resources = { resources: [{ type: { case: 'folderUri', value: remoteProjectUri(project) } }] };
    return {
      id: project.id,
      name: project.name,
      backendType: 1,
      archived: !!project.archived,
      projectResources: resources,
      environments: { environments: [{ id: project.id, name: 'SSH', resources }] },
      projectConversations: {
        conversations: this.manager
          .workspace()
          .sessions.filter((s) => s.projectId === project.id)
          .map((s) => ({
            conversationId: remoteConversationId(project.id, s.sessionId, s.conversationId),
            environmentId: project.id,
          })),
      },
    };
  }
  captureCatalog(value: Data): void {
    if (value?.response) {
      rememberNativeGeminiCatalog(this.manager.directory, value.response);
      allocateModelIdentities(
        this.manager.directory,
        readModelConfig(path.join(this.manager.directory, 'custom_models.json')),
        value.response,
      );
    }
    if (Array.isArray(value?.clientModelConfigs)) {
      const models: Data = {};
      for (const entry of value.clientModelConfigs) {
        if (!entry.modelId || /^custom-|^ssh-/.test(entry.modelId)) continue;
        const model = entry.modelOrAlias?.choice?.value ?? entry.modelOrAlias?.value ?? entry.modelOrAlias?.model;
        if (typeof model === 'number' && model) models[entry.modelId] = { displayName: entry.label, model };
      }
      rememberNativeGeminiCatalog(this.manager.directory, { models });
      allocateModelIdentities(
        this.manager.directory,
        readModelConfig(path.join(this.manager.directory, 'custom_models.json')),
        { models },
      );
    }
  }
  async snapshot(): Promise<Data> {
    for (const connection of this.connections.values()) {
      const snap = connection.client.snapshot();
      if (
        snap.state === 'connected' &&
        snap.activeSessions.length &&
        Date.now() - connection.lastStatus > 5000 &&
        !connection.statusPending
      ) {
        connection.lastStatus = Date.now();
        connection.statusPending = this.status(snap.config)
          .then((status) => connection.client.syncRemoteStatus(status))
          .catch(() => {})
          .finally(() => {
            connection.statusPending = undefined;
          });
      }
      this.drain(connection);
    }
    for (const c of this.conversations.values())
      if (!this.deletions.has(this.sessionKey(c.project, c.sessionId))) c.swarm?.refresh();
    const workspace = this.manager.workspace();
    for (const project of workspace.projects)
      if (this.connections.has(this.endpoint(project))) void this.refreshProject(project).catch(() => {});
    const summaries: Data = {};
    for (const session of workspace.sessions) {
      const id = remoteConversationId(session.projectId, session.sessionId, session.conversationId);
      const c = this.conversation(id)!;
      const annotations = { ...session.annotations };
      for (const field of ['lastUserViewTime', 'archivalStatusTimestamp'])
        if (annotations[field]?.seconds !== undefined)
          annotations[field] = { ...annotations[field], seconds: BigInt(annotations[field].seconds) };
      if (Array.isArray(annotations.cls))
        annotations.cls = annotations.cls.map((value: string | bigint) => BigInt(value));
      summaries[id] = {
        summary: session.title || session.remoteTitle || 'Remote conversation',
        trajectoryId: id,
        stepCount: c.steps.length,
        status: c.active ? 2 : 1,
        notFullyIdle: c.active,
        lastModifiedTime: timestamp(session.updatedAt),
        createdTime: timestamp(session.updatedAt),
        workspaces: this.metadata(c).workspaces,
        trajectoryMetadata: this.metadata(c),
        annotations: {
          ...annotations,
          title: session.title || session.remoteTitle || '',
          archived: session.archived,
          pinned: session.pinned,
        },
      };
    }
    return { projects: workspace.projects.map((p) => this.nativeProject(p)), summaries };
  }
  private async saveSession(
    project: RemoteAcpProject,
    sessionId: string,
    conversationId?: string,
  ): Promise<Conversation> {
    if (this.deletedSessions.has(this.sessionKey(project, sessionId)))
      throw new Error('This remote session was deleted.');
    await this.manager.action(0, 'session-save', { projectId: project.id, sessionId, conversationId, select: true });
    return this.conversation(remoteConversationId(project.id, sessionId, conversationId))!;
  }
  async call(method: string, input: Data = {}): Promise<{ handled: boolean; value?: any; input?: Data }> {
    if (!/^[a-zA-Z][a-zA-Z0-9]{0,100}$/.test(method)) throw new Error('Invalid native method.');
    const workspace = this.manager.workspace();
    const project = workspace.projects.find(
      (p) =>
        p.id === (input.projectEnvConfig?.projectId || input.projectId || input.project?.id || input.id) ||
        [...(input.workspaceUris || []), ...(input.overrideWorkspaceUris || [])].includes(remoteProjectUri(p)),
    );
    const id = input.cascadeId || input.conversationId || input.trajectoryId;
    const c = typeof id === 'string' ? this.conversation(id) : undefined;
    const isDelete = method === 'jetboxDeleteSummary' || method === 'deleteCascadeTrajectory';
    if (
      this.deletedConversations.has(id) ||
      workspace.deletedSessions?.some(
        (session) => remoteConversationId(session.projectId, session.sessionId, session.conversationId) === id,
      )
    ) {
      if (isDelete) return { handled: true, value: {} };
      throw new Error('This remote conversation has been permanently deleted.');
    }
    if (isDelete && c) {
      // jetboxDeleteSummary is also used for optimistic new-chat rollback. It must
      // not delete remote history. Only the native history-delete action commits it.
      if (method === 'deleteCascadeTrajectory') await this.deleteConversation(c);
      return { handled: true, value: {} };
    }
    if (['getScopedModelCatalog', 'getCascadeModelConfigs', 'getCascadeModelConfigData'].includes(method)) {
      const target = c?.project || project;
      if (!target) return { handled: false };
      if (project && c && project.id !== c.project.id) throw new Error('Conversation belongs to another SSH project.');
      let sessionId: string;
      if (c) {
        await this.load(c);
        sessionId = c.sessionId;
      } else {
        // ACP models are session capabilities. Keep the empty session private and
        // reuse it when the user starts a conversation, rather than creating a chat on every refresh.
        const saved =
          workspace.sessions.find(
            (session) => session.projectId === target.id && session.sessionId === target.lastSessionId,
          ) || workspace.sessions.find((session) => session.projectId === target.id);
        if (saved) {
          const existing = this.conversation(remoteConversationId(target.id, saved.sessionId, saved.conversationId))!;
          await this.load(existing);
          sessionId = existing.sessionId;
        } else sessionId = (await this.draft(target)).sessionId;
      }
      return { handled: true, value: { clientModelConfigs: this.remoteModels(target, sessionId) } };
    }
    if (method === 'listProjects') return { handled: true, value: { projectIds: workspace.projects.map((p) => p.id) } };
    if (method === 'readProjects') {
      const ids = (input.ids || []) as string[];
      const remote = workspace.projects.filter((p) => ids.includes(p.id));
      return {
        handled: true,
        value: { projects: remote.map((p) => this.nativeProject(p)) },
        input: { ...input, ids: ids.filter((id) => !remote.some((p) => p.id === id)) },
      };
    }
    if (method === 'getAllCascadeTrajectories')
      return { handled: true, value: { trajectorySummaries: (await this.snapshot()).summaries } };
    if (method === 'readProject' && project) return { handled: true, value: { project: this.nativeProject(project) } };
    if (method === 'updateProject' && project) {
      const expected = this.nativeProject(project);
      if (
        JSON.stringify(input.project.projectResources) !== JSON.stringify(expected.projectResources) &&
        (input.project.projectResources?.resources || []).some((r: Data) => r.type?.value !== remoteProjectUri(project))
      )
        throw new Error('Change the SSH directory in SSH project settings.');
      if (
        JSON.stringify(input.project.settings || {}) !== JSON.stringify(expected.settings || {}) ||
        JSON.stringify(input.project.permissionGrants || {}) !== JSON.stringify(expected.permissionGrants || {})
      )
        throw new Error('Remote permissions and environment settings must be configured on the remote agent.');
      await this.manager.action(0, 'project-save', {
        id: project.id,
        name: input.project.name,
        config: project.config,
        archived: !!input.project.archived,
      });
      return { handled: true, value: {} };
    }
    if (method === 'deleteProject' && project) {
      if ([...this.conversations.values()].some((c) => c.project.id === project.id && c.active))
        throw new Error('Wait for this project’s remote tasks to finish before removing it.');
      await this.manager.action(0, 'project-remove', { id: project.id });
      this.drafts.delete(project.id);
      for (const [id, conversation] of this.conversations) {
        if (conversation.project.id === project.id) {
          conversation.swarm?.close();
          this.conversations.delete(id);
        }
      }
      return { handled: true, value: {} };
    }
    if (method === 'startCascade' && (project || c)) {
      // The native composer allocates its ID before startCascade and uses it for
      // the initial message and optimistic stream, regardless of the response ID.
      const target = project || c!.project;
      if (c) {
        if (c.project.id !== target.id) throw new Error('Conversation belongs to another SSH project.');
        await this.load(c);
        return {
          handled: true,
          value: {
            cascadeId: c.id,
            projectEnvInfo: { environmentId: target.id, workspaceUris: [remoteProjectUri(target)] },
          },
        };
      }
      if (
        input.cascadeId &&
        (typeof input.cascadeId !== 'string' || !/^[a-f0-9]{8}-(?:[a-f0-9]{4}-){3}[a-f0-9]{12}$/i.test(input.cascadeId))
      )
        throw new Error('Invalid desktop conversation ID.');
      const { connection, sessionId } = await this.draft(target);
      this.drafts.delete(target.id);
      const created = await this.saveSession(target, sessionId, input.cascadeId || undefined);
      this.remoteModels(target, sessionId);
      created.loaded = true;
      try {
        await this.attachSwarm(created, connection);
      } catch (error) {
        created.swarm?.fail(error as Error);
      }
      this.drain(connection);
      return {
        handled: true,
        value: {
          cascadeId: created.id,
          projectEnvInfo: { environmentId: target.id, workspaceUris: [remoteProjectUri(target)] },
        },
      };
    }
    if (method === 'updateConversationAnnotations') {
      const ids = input.cascadeIds?.length ? input.cascadeIds : [input.cascadeId];
      const remote = ids.map((id: string) => this.conversation(id)).filter(Boolean) as Conversation[];
      for (const target of remote) {
        if (
          this.deletions.has(this.sessionKey(target.project, target.sessionId)) ||
          this.deletedSessions.has(this.sessionKey(target.project, target.sessionId))
        )
          throw new Error('This remote conversation is being deleted or has been deleted.');
        const annotations = input.annotations || {};
        await this.manager.action(0, 'session-save', {
          projectId: target.project.id,
          sessionId: target.sessionId,
          title: annotations.title,
          archived: annotations.archived,
          pinned: annotations.pinned,
          annotations,
        });
      }
      if (remote.length)
        return {
          handled: true,
          value: {},
          ...(remote.length < ids.length
            ? { input: { ...input, cascadeId: '', cascadeIds: ids.filter((id: string) => !this.conversation(id)) } }
            : {}),
        };
    }
    const remoteUris: string[] = [];
    const scan = (value: any): void => {
      if (typeof value === 'string' && value.startsWith('agy-ssh:')) remoteUris.push(value);
      else if (Array.isArray(value)) value.forEach(scan);
      else if (value && typeof value === 'object' && !(value instanceof Uint8Array)) Object.values(value).forEach(scan);
    };
    scan(input);
    if (
      remoteUris.length &&
      (['statUri', 'resolveFolder', 'readFile', 'readDir', 'getRepoInfos'].includes(method) || !c)
    ) {
      if (method === 'getRepoInfos')
        return {
          handled: true,
          value: { repos: [] },
          input: { ...input, repoUris: (input.repoUris || []).filter((uri: string) => !uri.startsWith('agy-ssh:')) },
        };
      if (['statUri', 'resolveFolder', 'readFile', 'readDir'].includes(method)) {
        return { handled: true, value: await this.files(method, input, remoteUris[0]) };
      }
      throw new Error(`The remote ACP agent does not expose ${method}. This request was not sent to the local host.`);
    }
    if (!c) {
      const containsRemote = (value: any): boolean =>
        typeof value === 'string'
          ? workspace.projects.some((p) => p.id === value) || !!this.conversation(value)
          : Array.isArray(value)
            ? value.some(containsRemote)
            : value && typeof value === 'object' && !(value instanceof Uint8Array)
              ? Object.values(value).some(containsRemote)
              : false;
      if (containsRemote(input)) throw new Error(`The remote ACP agent does not expose ${method}.`);
      return { handled: false };
    }
    const connection = await this.load(c);
    if (
      this.deletions.has(this.sessionKey(c.project, c.sessionId)) ||
      this.deletedSessions.has(this.sessionKey(c.project, c.sessionId))
    )
      throw new Error('This remote conversation is being deleted or has been deleted.');
    this.drain(connection);
    let value: Data = {};
    if (method === 'sendUserCascadeMessage') {
      if (c.active) throw new Error('This remote session already has an active prompt.');
      if (
        input.artifactComments?.length ||
        input.fileDiffComments?.length ||
        input.fileComments?.length ||
        input.additionalSteps?.length
      )
        throw new Error('Remote ACP does not support these native comment attachments.');
      const prompt: Data[] = [];
      for (const item of input.items || []) {
        const attachment = item.chunk ? (item.chunk.case === 'item' ? item.chunk.value : undefined) : item.item;
        const text = item.chunk ? (item.chunk.case === 'text' ? item.chunk.value : undefined) : item.text;
        if (attachment) {
          const scope = attachment.scopeItem;
          const command = scope?.case === 'slashCommand' ? scope.value : undefined;
          if (
            !command ||
            Object.keys(command.argumentValues || {}).length ||
            !(c.commands || []).some((option) => option.name === command.info?.name)
          )
            throw new Error(
              'This native context attachment is not available through the remote ACP session. Send remote paths or command arguments as text.',
            );
          prompt.push({ type: 'text', text: '/' + command.info.name });
        }
        if (typeof text === 'string' && text) prompt.push({ type: 'text', text });
      }
      for (const image of input.images || []) {
        if (!image.base64Data) throw new Error('Unsupported native image attachment.');
        prompt.push({ type: 'image', data: image.base64Data, mimeType: image.mimeType || 'image/png' });
      }
      for (const media of input.media || []) {
        const inlineData = media.payload
          ? media.payload.case === 'inlineData'
            ? media.payload.value
            : undefined
          : media.inlineData;
        if (
          !['image/png', 'image/jpeg', 'image/webp', 'image/gif'].includes(media.mimeType) ||
          !(inlineData instanceof Uint8Array) ||
          !inlineData.length
        )
          throw new Error('Only inline image attachments are supported by this remote ACP session.');
        prompt.push({
          type: 'image',
          data: Buffer.from(inlineData).toString('base64'),
          mimeType: media.mimeType,
        });
      }
      if (
        prompt.some((block) => block.type === 'image') &&
        connection.client.snapshot().initialize?.agentCapabilities?.promptCapabilities?.image !== true
      )
        throw new Error('This remote ACP agent does not advertise image input.');
      if (!prompt.length) throw new Error('A remote prompt cannot be empty.');
      const planner = input.cascadeConfig?.plannerConfig;
      const requested = planner?.requestedModel;
      const number =
        requested?.choice?.value ?? requested?.value ?? requested?.model ?? planner?.planModel ?? input.requestedModel;
      const selection = this.modelSelections.get(planner?.modelName) || this.modelSelections.get(number);
      if (selection && selection.projectId !== c.project.id)
        throw new Error('Select a model belonging to this SSH project.');
      if (number && !selection && !planner?.modelName)
        throw new Error('Refresh the model list and select an available model for this SSH project.');
      const selected = selection?.modelId || planner?.modelName;
      if (selection?.configModel) {
        const current = connection.client.snapshot().sessions.find((session) => session.sessionId === c.sessionId);
        const offered = current?.configOptions?.find((option: Data) => option.id === 'model')?.options || [];
        if (
          !offered
            .flatMap((option: Data) => option.options || [option])
            .some((option: Data) => option.value === selection.configModel)
        )
          throw new Error('This SSH session no longer offers the selected model. Refresh the model list.');
        // ACP separates the base model from its offered thought_level values.
        // Legacy availableModels IDs can include suffixes and are not interchangeable.
        const configured = await connection.client.request('session/set_config_option', {
          sessionId: c.sessionId,
          configId: 'model',
          value: selection.configModel,
        });
        if (selection.thoughtLevel) {
          const options =
            configured.configOptions?.find((option: Data) => option.id === 'thought_level')?.options || [];
          if (
            !options
              .flatMap((option: Data) => option.options || [option])
              .some((option: Data) => option.value === selection.thoughtLevel)
          )
            throw new Error('This remote model no longer offers the selected thinking level. Refresh the model list.');
          await connection.client.request('session/set_config_option', {
            sessionId: c.sessionId,
            configId: 'thought_level',
            value: selection.thoughtLevel,
          });
        }
      } else if (selected) {
        const metadata = connection.client.snapshot().sessions.find((s) => s.sessionId === c.sessionId);
        const available = metadata?.models?.availableModels || [];
        if (!available.some((m: Data) => m.modelId === selected))
          throw new Error(
            `The remote agent does not offer model ${selected}. Refresh the model list for this SSH project.`,
          );
        if (metadata?.models?.currentModelId !== selected)
          await connection.client.request('session/set_model', { sessionId: c.sessionId, modelId: selected });
      }
      // Returning acknowledges dispatch only. The original native state stream owns completion.
      const pending = connection.client.request('session/prompt', { sessionId: c.sessionId, prompt });
      c.active = true;
      this.drain(connection);
      void pending
        .then(async () => {
          if (!this.deletedSessions.has(this.sessionKey(c.project, c.sessionId)))
            await this.manager.action(0, 'session-save', { projectId: c.project.id, sessionId: c.sessionId });
        })
        .catch((error) => {
          c.error = error.message;
          c.revision++;
        });
    } else if (['cancelCascadeInvocation', 'forceStopCascadeTree', 'cancelCascadeSteps'].includes(method)) {
      if (c.swarm && method !== 'cancelCascadeInvocation')
        throw new Error(
          'Native swarm supports cancellation of the current coordinator turn only. Send worker control instructions to the coordinator; this operation cannot stop the whole swarm or selected worker steps.',
        );
      await connection.client.request('session/cancel', { sessionId: c.sessionId });
    } else if (method === 'handleCascadeUserInteraction') {
      const index = input.interaction?.stepIndex;
      const token = c.permissions.get(index);
      const pending = connection.client.snapshot().pending.find((p) => p.token === token);
      if (!pending || input.interaction?.trajectoryId !== c.id)
        throw new Error('This remote permission request is no longer pending.');
      const interaction = input.interaction.interaction;
      if (interaction?.case !== 'permission' || ![0, 1].includes(interaction.value.scope || 0))
        throw new Error('Only an explicit one-time remote permission choice is supported.');
      const kind = interaction.value.allow ? 'allow_once' : 'reject_once';
      const options = pending.params.options.filter((o: Data) => o.kind === kind);
      if (options.length !== 1)
        throw new Error('The native permission choice does not map to exactly one remote option.');
      connection.client.respond(token, { result: { outcome: { outcome: 'selected', optionId: options[0].optionId } } });
      this.drain(connection);
    } else if (method === 'streamAgentStateUpdates') {
      if (c.error && connection.client.snapshot().state === 'disconnected') throw new Error(c.error);
      const subscriber = input.subscriberId || c.id;
      if (!this.pages.has(subscriber)) {
        const bounds = input.initialStepsPageBounds || {};
        this.pages.set(subscriber, {
          conversationId: c.id,
          bounds,
          followTail: !bounds.endIndexExclusive || bounds.endIndexExclusive >= c.steps.length,
        });
        if (this.pages.size > 512) this.pages.delete(this.pages.keys().next().value!);
      }
      const page = this.pages.get(subscriber)!;
      const end = page.followTail
        ? c.steps.length
        : Math.min(c.steps.length, page.bounds.endIndexExclusive || c.steps.length);
      const start = Math.max(0, Math.min(end, page.bounds.startIndex || 0));
      value = {
        update: {
          conversationId: c.id,
          trajectoryId: c.id,
          status: c.active ? 2 : 1,
          executableStatus: 1,
          executorLoopStatus: 1,
          fullyIdle: !c.active,
          mainTrajectoryUpdate: {
            stepsUpdate: {
              indices: c.steps.slice(start, end).map((_, i) => start + i),
              steps: c.steps.slice(start, end),
              totalLength: c.steps.length,
              pageBounds: { startIndex: start, endIndexExclusive: end },
            },
            metadata: this.metadata(c),
            status: c.active ? 2 : 1,
            fullyIdle: !c.active,
            waitingSteps: [...c.permissions.keys()].map((index) => ({ stepIndex: index, step: c.steps[index] })),
          },
          turnsUpdate: {
            turns: c.steps.flatMap((step, index) =>
              step.type === 14
                ? [
                    {
                      index: c.steps.slice(0, index).filter((s) => s.type === 14).length,
                      startStepIndex: index,
                      startTime: step.metadata.createdAt,
                    },
                  ]
                : [],
            ),
            totalLength: c.steps.filter((s) => s.type === 14).length,
          },
        },
      };
    } else if (method === 'getSlashCommands')
      value = {
        commands: (c.commands || []).map((command) => ({
          info: { name: command.name, displayName: command.name, modelFacingText: '/' + command.name, type: 1 },
          title: '/' + command.name,
          description: command.description || '',
          arguments: [],
        })),
      };
    else if (method === 'getConversationMetadata')
      value = { metadata: this.metadata(c), trajectoryId: c.id, numSteps: c.steps.length };
    else if (method === 'getCascadeTrajectory')
      value = {
        trajectory: { trajectoryId: c.id, cascadeId: c.id, steps: c.steps, metadata: this.metadata(c) },
        status: c.active ? 2 : 1,
        numTotalSteps: c.steps.length,
      };
    else if (method === 'getCascadeTrajectorySteps')
      value = { steps: c.steps.slice(input.startIndex || 0, input.endIndex || undefined) };
    else if (method === 'getTrajectoryTurns')
      value = {
        turns: c.steps.flatMap((step, index) => (step.type === 14 ? [{ startStepIndex: index, firstStep: step }] : [])),
      };
    else if (['getCascadeTrajectoryGeneratorMetadata', 'getCascadeTrajectoryExecutorMetadatas'].includes(method))
      value = {};
    else if (method === 'requestAgentStatePageUpdate') {
      const bounds = input.stepPageBounds || {};
      this.pages.set(input.subscriberId || c.id, {
        conversationId: c.id,
        bounds,
        followTail: !bounds.endIndexExclusive || bounds.endIndexExclusive >= c.steps.length,
      });
    } else if (method === 'warmUpEnvironment') value = {};
    else
      throw new Error(
        `The remote ACP agent does not expose ${method}. This request was not sent to the local language server.`,
      );
    return { handled: true, value };
  }
  private async files(method: string, input: Data, uri: string): Promise<Data> {
    const parsed = new URL(uri);
    const project = this.manager.workspace().projects.find((p) => p.id === parsed.hostname);
    if (!project) throw new Error('Unknown SSH project URI.');
    const remotePath = decodeURIComponent(parsed.pathname);
    if (remotePath !== project.config.cwd && !remotePath.startsWith(project.config.cwd.replace(/\/$/, '') + '/'))
      throw new Error('Remote file access must stay inside this project.');
    const script = `import json,os,sys,base64,stat\nx=json.load(sys.stdin)\np=os.path.realpath(x['path']);root=os.path.realpath(x['root'])\nif os.path.commonpath([root,p])!=root: raise ValueError('Path leaves remote project')\ns=os.stat(p);r={'directory':stat.S_ISDIR(s.st_mode),'size':s.st_size,'mtime':s.st_mtime}\nif x['method']=='readFile':\n if s.st_size>4194304: raise ValueError('File exceeds 4 MiB')\n with open(p,'rb') as f:r['content']=base64.b64encode(f.read(4194305)).decode()\nif x['method']=='readDir':r['entries']=[{'name':e.name,'isDirectory':e.is_dir(follow_symlinks=False)} for e in list(os.scandir(p))[:1000]]\nprint(json.dumps(r))`;
    const quote = (text: string) => "'" + text.replace(/'/g, "'\\''") + "'";
    const child = spawn(
      'ssh',
      [
        '-T',
        '-o',
        'BatchMode=yes',
        '-o',
        'StrictHostKeyChecking=yes',
        '-o',
        'ConnectTimeout=15',
        '--',
        project.config.host,
        'python3 -c ' + quote(script),
      ],
      { stdio: 'pipe', shell: false, windowsHide: true },
    );
    const result = await new Promise<Data>((resolve, reject) => {
      let output = Buffer.alloc(0);
      const timer = setTimeout(() => {
        child.kill();
        reject(new Error('Remote file inspection timed out.'));
      }, 20000);
      child.stdout.on('data', (chunk) => {
        output = Buffer.concat([output, chunk]);
        if (output.length > 6 * 1024 * 1024) {
          child.kill();
          reject(new Error('Remote file response exceeds limit.'));
        }
      });
      child.stderr.resume();
      child.on('error', (error) => {
        clearTimeout(timer);
        reject(error);
      });
      child.on('close', (code) => {
        clearTimeout(timer);
        if (code !== 0) reject(new Error('Unable to read the remote path.'));
        else
          try {
            resolve(JSON.parse(output.toString()));
          } catch {
            reject(new Error('Invalid remote file response.'));
          }
      });
      child.stdin.on('error', () => {});
      child.stdin.end(JSON.stringify({ method, path: remotePath, root: project.config.cwd }));
    });
    if (method === 'resolveFolder') return { resourceType: result.directory ? 1 : 4 };
    if (method === 'statUri')
      return {
        fileType: result.directory ? 2 : 1,
        size: BigInt(result.size),
        normalizedUri: uri,
        modTime: timestamp(result.mtime * 1000),
      };
    if (method === 'readFile') return { content: Uint8Array.from(Buffer.from(result.content, 'base64')) };
    return {
      entries: result.entries.map((entry: Data) => ({
        uri: uri.replace(/\/$/, '') + '/' + encodeURIComponent(entry.name),
        fileType: entry.isDirectory ? 2 : 1,
      })),
    };
  }
  close(): void {
    for (const c of this.conversations.values()) c.swarm?.close();
    for (const c of this.connections.values()) c.client.disconnect();
    this.connections.clear();
    this.conversations.clear();
    this.drafts.clear();
    this.modelSelections.clear();
    this.pages.clear();
  }
}
