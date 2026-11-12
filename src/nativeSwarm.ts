/** Read-only native swarm views. Worker transcripts are never ACP conversations. */
import type { RemoteAcpClient } from './remoteAcp';

type Data = Record<string, any>;
const PREFIX = '_maichart/swarm/';
const VIEW_BYTES = 8 * 1024 * 1024;
const VIEW_RECORDS = 10000;
export function nativeSwarmCapabilities(initialize: Data | undefined): Data | undefined {
  const value = initialize?.agentCapabilities?._meta?.['maichart/nativeSwarm'];
  if (
    value?.version === 1 &&
    value.implementation === 'native-agy' &&
    Array.isArray(value.methods) &&
    ['subscribe', 'snapshot', 'history'].every((name) => value.methods?.includes(PREFIX + name)) &&
    value.notification === PREFIX + 'update'
  )
    return value;
}
function literal(value: unknown): string {
  const text = typeof value === 'string' ? value : JSON.stringify(value, null, 2);
  let fenceLength = 3;
  for (const run of (text || '').match(/`+/g) || []) fenceLength = Math.max(fenceLength, run.length + 1);
  const fence = '`'.repeat(fenceLength);
  return `${fence}\n${text ?? ''}\n${fence}`;
}
interface MemberView {
  native: Data;
  revision?: string;
  cursor?: Data;
  records: Data[];
  more: boolean;
  error?: string;
  stepKey: string;
}
export class NativeSwarmView {
  private latest?: Data;
  private board = new Map<number, Data>();
  private cursor = 0;
  private more = false;
  private members = new Map<string, MemberView>();
  private subscription?: Promise<void>;
  private subscribed = false;
  private syncing?: Promise<void>;
  private lastSync = 0;
  private disposed = false;
  private error?: string;
  private limited = false;
  private bytes = 0;
  private revision = 0;
  private metadata?: Data;
  constructor(
    private client: Pick<RemoteAcpClient, 'request'>,
    readonly sessionId: string,
    readonly capabilities: Data,
    private render: (key: string, title: string, markdown: string, readError: boolean) => void,
  ) {}
  start(): Promise<void> {
    if (this.subscription) return this.subscription;
    this.subscription = this.client
      .request(PREFIX + 'subscribe', { sessionId: this.sessionId })
      .then((result) => {
        if (this.disposed) return;
        if (result?.subscribed !== true) throw new Error('Native swarm subscription was not accepted.');
        this.subscribed = true;
        this.update(result.snapshot);
      })
      .catch((error) => {
        this.subscription = undefined;
        throw error;
      });
    return this.subscription;
  }
  metadataNotice(metadata: Data): void {
    this.metadata = metadata;
  }
  update(snapshot: Data): void {
    if (this.disposed) return;
    if (snapshot?.sessionId !== this.sessionId) throw new Error('Native swarm snapshot belongs to another session.');
    if (snapshot.readError) {
      this.revision++;
      this.error = String(snapshot.readError);
      this.renderBoard();
      return;
    }
    if (
      snapshot.source !== 'native-local-files' ||
      !Array.isArray(snapshot.members) ||
      !Array.isArray(snapshot.messages) ||
      !Number.isSafeInteger(snapshot.cursor) ||
      snapshot.cursor < 0 ||
      (snapshot.head !== undefined && (!Number.isSafeInteger(snapshot.head) || snapshot.head < 0)) ||
      snapshot.members.some(
        (m: Data) =>
          typeof m.name !== 'string' || typeof m.conversationId !== 'string' || m.parentSessionId !== this.sessionId,
      )
    )
      throw new Error('Invalid native swarm snapshot.');
    this.revision++;
    if (
      snapshot.resetRequired ||
      (this.latest?.head !== undefined && snapshot.head < this.latest.head) ||
      !snapshot.swarm
    ) {
      this.bytes -= [...this.board.values()].reduce((sum, item) => sum + Buffer.byteLength(JSON.stringify(item)), 0);
      this.board.clear();
      this.cursor = 0;
      this.limited = false;
    }
    this.latest = snapshot;
    this.error = undefined;
    this.mergeBoard(snapshot);
    for (const member of snapshot.members) {
      const previous = this.members.get(member.name);
      if (previous?.native.conversationId === member.conversationId) previous.native = member;
      else {
        if (previous) {
          this.bytes -= previous.records.reduce((sum, record) => sum + Buffer.byteLength(JSON.stringify(record)), 0);
          this.render(
            previous.stepKey,
            `Native swarm record · ${member.name}`,
            'This member was replaced. The previous transcript is no longer current.',
            false,
          );
        }
        this.members.set(member.name, {
          native: member,
          records: [],
          more: true,
          stepKey: JSON.stringify(['swarm-member', member.name, member.conversationId]),
        });
      }
    }
    const names = new Set(snapshot.members.map((m: Data) => m.name));
    for (const [name, member] of this.members)
      if (!names.has(name)) {
        this.bytes -= member.records.reduce((sum, record) => sum + Buffer.byteLength(JSON.stringify(record)), 0);
        this.render(
          member.stepKey,
          `Native swarm record · ${name}`,
          'This member is absent from the current roster. Its previous file state does not prove completion.',
          false,
        );
        this.members.delete(name);
      }
    for (const member of this.members.values()) if (member.records.length || member.error) this.renderMember(member);
    this.renderBoard();
    this.refresh();
  }
  /** Notifications contain only the first board page; fill it without running models. */
  refresh(): void {
    if (
      this.disposed ||
      !this.subscribed ||
      this.syncing ||
      !this.latest ||
      this.error ||
      Date.now() - this.lastSync < 1000
    )
      return;
    this.lastSync = Date.now();
    this.syncing = this.readPages()
      .catch((error) => {
        if (!this.disposed) {
          this.error = error.message;
          this.renderBoard();
        }
      })
      .finally(() => {
        this.syncing = undefined;
      });
  }
  noteGap(): void {
    this.error =
      'ACP events were missed. Reattaching the current native snapshot; coordinator message history may be incomplete.';
    this.renderBoard();
    this.subscription = undefined;
    void this.start().catch((error) => this.fail(error));
  }
  fail(error: Error): void {
    if (this.disposed) return;
    this.error = error.message;
    this.renderBoard();
  }
  private mergeBoard(snapshot: Data): void {
    let previous = -1;
    for (const message of snapshot.messages) {
      if (
        !Number.isSafeInteger(message?.seq) ||
        message.seq < 1 ||
        message.seq <= previous ||
        message.seq > snapshot.cursor
      )
        throw new Error('Invalid native board sequence.');
      previous = message.seq;
      const old = this.board.get(message.seq);
      const added = Buffer.byteLength(JSON.stringify(message)) - (old ? Buffer.byteLength(JSON.stringify(old)) : 0);
      if (this.bytes + added > VIEW_BYTES || (!old && this.board.size >= VIEW_RECORDS)) {
        this.limited = true;
        break;
      }
      this.board.set(message.seq, message);
      this.bytes += added;
      this.cursor = Math.max(this.cursor, message.seq);
    }
    this.more = snapshot.more === true || (Number.isSafeInteger(snapshot.head) && this.cursor < snapshot.head);
  }
  private async readPages(): Promise<void> {
    const revision = this.revision;
    for (let page = 0; page < 4 && this.more && !this.limited; page++) {
      const before = this.cursor;
      const result = await this.client.request(PREFIX + 'snapshot', {
        sessionId: this.sessionId,
        after: before,
        limit: 100,
      });
      if (this.disposed || revision !== this.revision) return;
      if (
        result?.sessionId !== this.sessionId ||
        result.source !== 'native-local-files' ||
        !Array.isArray(result.messages) ||
        !Number.isSafeInteger(result.cursor)
      )
        throw new Error('Invalid native board page.');
      if (result.resetRequired || result.etag !== this.latest?.etag) {
        // Obtain an authoritative first page instead of merging different board generations.
        const first = await this.client.request(PREFIX + 'snapshot', {
          sessionId: this.sessionId,
          after: 0,
          limit: 100,
        });
        if (!this.disposed && revision === this.revision) this.update(first);
        return;
      }
      this.mergeBoard(result);
      if (this.cursor <= before) break; // A partial file tail must never create a busy loop.
    }
    this.renderBoard();
    for (const [name, member] of this.members) {
      if (this.disposed || revision !== this.revision) return;
      const desired = member.native.transcriptRevision;
      if (member.revision === desired && !member.more) continue;
      if (member.revision !== desired) {
        this.bytes -= member.records.reduce((sum, record) => sum + Buffer.byteLength(JSON.stringify(record)), 0);
        member.records = [];
        member.cursor = undefined;
        member.more = true;
        member.revision = desired;
      }
      try {
        for (let page = 0; page < 4 && member.more && !this.limited; page++) {
          const before = JSON.stringify(member.cursor);
          const result = await this.client.request(PREFIX + 'history', {
            sessionId: this.sessionId,
            member: name,
            limit: 100,
            ...(member.cursor ? { cursor: member.cursor } : {}),
          });
          if (this.disposed || revision !== this.revision) return;
          if (
            result?.sessionId !== this.sessionId ||
            result.member !== name ||
            result.conversationId !== member.native.conversationId ||
            result.source !== 'native-transcript' ||
            !Array.isArray(result.records) ||
            result.records.some((record: unknown) => !record || typeof record !== 'object' || Array.isArray(record)) ||
            !Number.isSafeInteger(result.cursor?.offset) ||
            result.cursor.offset < 0
          )
            throw new Error('Invalid native member history.');
          if (result.resetRequired) {
            this.bytes -= member.records.reduce((sum, record) => sum + Buffer.byteLength(JSON.stringify(record)), 0);
            member.records = [];
            member.cursor = undefined;
            member.more = true;
            continue;
          }
          for (const record of result.records) {
            const size = Buffer.byteLength(JSON.stringify(record));
            if (this.bytes + size > VIEW_BYTES || member.records.length >= VIEW_RECORDS) {
              this.limited = true;
              break;
            }
            member.records.push(record);
            this.bytes += size;
          }
          member.cursor = result.cursor;
          member.more = result.more === true;
          if (JSON.stringify(member.cursor) === before) break;
        }
        member.error = undefined;
      } catch (error) {
        member.error = (error as Error).message;
      }
      if (!this.disposed) this.renderMember(member);
    }
    this.renderBoard();
  }
  private renderBoard(): void {
    if (this.disposed) return;
    const snapshot = this.latest;
    const lines = [
      'Read-only native file snapshot. Member phases do not prove liveness or team completion.',
      `Approval mode: ${this.capabilities.approvalMode || 'unknown'}. Controls: send instructions to the coordinator in this conversation.`,
      'Stop cancels only the coordinator turn. It does not stop all workers.',
    ];
    if (this.capabilities.approvalMode === 'ask')
      lines.push('Native agy 1.5.5 may not forward worker permission requests to this connection.');
    if (this.metadata)
      lines.push(
        'Reconnected using last-observed native ACP configuration. Current model settings are not queried live.' +
          (this.metadata.staleFields?.length
            ? ` Stale fields were excluded: ${this.metadata.staleFields.join(', ')}.`
            : ''),
      );
    if (this.error) lines.push(`Read error: ${this.error}. Previous records may be stale.`);
    if (snapshot) {
      lines.push(
        'Members',
        ...snapshot.members.map((member: Data) =>
          literal({
            name: member.name,
            phase: member.native?.phase || 'unknown',
            model: member.native?.applied_model || 'unknown',
          }),
        ),
      );
      lines.push(
        'Coordinator turn: ' +
          (snapshot.coordinatorPromptActive ? 'active' : 'idle') +
          '. Native process: ' +
          (snapshot.nativeProcessAlive === true ? 'present' : 'not confirmed') +
          '.',
      );
      lines.push(
        'Message board',
        ...[...this.board.values()]
          .sort((a, b) => a.seq - b.seq)
          .map((message) =>
            literal({
              seq: message.seq,
              agent: message.agent,
              channel: message.channel,
              text: message.text,
              control: message.control,
            }),
          ),
      );
      /* Keep the source and paging state visible without exposing internal roster fields. */
      lines.push(
        'Native file source and pagination',
        literal({
          source: snapshot.source,
          swarm: snapshot.swarm,
          nativeProcessAlive: snapshot.nativeProcessAlive,
          coordinatorPromptActive: snapshot.coordinatorPromptActive,
          runtimeVerified: snapshot.runtimeVerified,
          cursor: this.cursor,
          head: snapshot.head,
          more: this.more,
          partialTail: snapshot.partialTail,
        }),
      );
    }
    if (this.limited)
      lines.push(
        'Display limit reached (8 MiB or 10000 records per view). Use the remote native history interface to inspect additional records.',
      );
    this.render('swarm-board', 'Native swarm · members and message board', lines.join('\n\n'), !!this.error);
  }
  private renderMember(member: MemberView): void {
    const lines = [
      'Read-only native transcript. This member is not a separate ACP session; file status is not proof of completion.',
      literal({
        name: member.native.name,
        conversationId: member.native.conversationId,
        source: 'native-transcript',
        phase: member.native.native?.phase,
      }),
      ...(member.error ? [`Read error: ${member.error}. Previous records may be stale.`] : []),
      ...member.records.map((record) =>
        literal({
          step: record.step_index,
          source: record.source,
          type: record.type,
          status: record.status,
          content: record.content,
          tool_calls: record.tool_calls,
          ...(record.content === undefined && record.tool_calls === undefined ? { record } : {}),
        }),
      ),
      ...(member.more ? ['Additional history is being read.'] : []),
    ];
    this.render(member.stepKey, `Native swarm record · ${member.native.name}`, lines.join('\n\n'), !!member.error);
  }
  close(): void {
    this.disposed = true;
  }
}
