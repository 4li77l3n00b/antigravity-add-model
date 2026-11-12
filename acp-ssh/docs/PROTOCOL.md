# Protocol compatibility

The bridge transports newline-delimited ACP JSON-RPC on SSH stdin/stdout. Diagnostics go to stderr. Extension names retain their historical `maichart` namespace for existing desktop clients; no corresponding project, directory or data is required.

## Persistent bridge

Initialize once per native process; reconnecting clients must use the same protocol version, clientInfo and clientCapabilities. Unknown payload fields are retained. Request IDs are mapped so a late response cannot answer a different connection's reused ID. Pending native reverse requests are replayed after reconnect initialize with stable bridge IDs. Respond only to requests actually received from the transport.

Native `session/list` is authoritative for native sessions. The bridge management status additionally tracks sessions it observed. Active session load/resume is rejected rather than silently cancelling a running prompt. `--preserve-background-requests` prevents parent end_turn/cancel from clearing background reverse requests; enable it for native Swarm. Disconnect does not retry or cancel work. Host/agent failure is visible and never treated as success.

Management commands use a private, same-user Unix socket and do not acquire the interactive controller. `submit` rejects an attached controller; a previously used request ID must have identical input. After deletion, metadata-only retired job IDs prevent an old submit retry from recreating deleted work. `watch` is a bounded persisted event log; detect retention gaps. Commands are documented by `acp-ssh --help`.

## Permanent deletion v1

Opt-in declaration at `initialize.result.agentCapabilities._meta["maichart/sessionManagement"]`:

```json
{"version":1,"delete":{"method":"_maichart/session/delete","permanent":true,"idempotent":true,"relatedSessions":"reject","activeSessions":"reject","backend":"native-local","nativeVersion":"1.5.5"}}
```

Request (must have a JSON-RPC ID; no force/cascade/path parameters):

```json
{"jsonrpc":"2.0","id":42,"method":"_maichart/session/delete","params":{"sessionId":"11111111-1111-4111-8111-111111111111"}}
```

Confirmed result:

```json
{"jsonrpc":"2.0","id":42,"result":{"deleted":true,"sessionId":"11111111-1111-4111-8111-111111111111","deletedSessionIds":["11111111-1111-4111-8111-111111111111"],"alreadyDeleted":false}}
```

Retry of an absent target confirms deletion with alreadyDeleted=true. No child sessions are implicitly deleted. Missing capability means unsupported, not permission to use session/close or local hiding.

The implementation invokes native `DeleteCascadeTrajectory` and `JetboxDeleteSummary`, closes the target, synchronizes the host's other native summary caches, and verifies native data/SQLite/legacy-summary absence. Native endpoint ownership and the binary SHA-256 are verified. A maintenance process sends no prompt and uses no real API credential. Bridge-owned session history, completed jobs, and events containing the deleted session are removed after native success; events that are shared list snapshots are removed as a whole. Metadata-only deletion/job-ID records remain to prevent resurrection and duplicate execution.

The host must have no outstanding native requests or tasks. Target reverse requests, grouped/battle/parent-child references, native Swarm membership, unknown storage, symlinks, another host using the same root, or open target files cause rejection. New requests that could create native caches are temporarily refused during deletion. Normal adapter registration or subscription alone is **not** a native Swarm relationship.

A durable pending record precedes changes. A partial failure returns error, not deleted=true; keep the client entry and retry the same deletion to reconcile. Pending survives restart and blocks target load/resume/prompt. Complete deletion also blocks reuse of the old ID, since the native load endpoint otherwise accepts nonexistent IDs. Neither state is permission to automatically delete additional sessions.

Errors have `data.reason`, `retryable`, and `mayBeDeleted`. `-32601` is unsupported; `-32602` is invalid parameters; `-32050` covers busy/reference/native/verification failures; `-32051` covers DELETE_IN_PROGRESS or SESSION_DELETED_OR_DELETING. Status exposes `sessionDeletion.configured/available/inProgress/unresolved`. A timeout can mean the original transaction still ran; retry idempotently.

The deletion scope is native local history in the configured root and the current bridge's history copies. It is not forensic erasure of backups, other clients, global diagnostic logs, filesystem free space or SQLite WAL/free pages. User workspace files are not targeted. Version 1.5.5 and a single verified Linux build are intentionally pinned; other builds need revalidation.

## Native Swarm v1, revision 2

Capability: `agentCapabilities._meta["maichart/nativeSwarm"]`. Important fields include `implementation:native-agy`, `readOnlyAttach:true`, `approvalMode:auto|ask`, `directWorkerControl:false`, `hardWorkerConcurrencyLimit:false`, `cancelScope:native-session-turn`, and `endTurnMeansSwarmComplete:false`.

| Method | Parameters | Result |
|---|---|---|
| `_maichart/swarm/capabilities` | `{}` | Current contract and limitations |
| `_maichart/swarm/sessions` | `{}` | Open coordinator IDs in persistent adapter memory, with generation |
| `_maichart/swarm/attach` | sessionId | Read-only cached native metadata, current snapshot, history entry point |
| `_maichart/swarm/snapshot` | sessionId, after=0, limit=100 | Native roster and board; after is board sequence |
| `_maichart/swarm/history` | sessionId, member, cursor?, limit=100 | Native JSONL transcript; coordinator works before a board exists |
| `_maichart/swarm/subscribe` | sessionId | Current snapshot and subsequent updates |
| `_maichart/swarm/unsubscribe` | sessionId | Stop updates |
| `_maichart/swarm/update` | notification | Snapshot or explicit readError |

A session must already be open in the persistent adapter; attach does not load/resume it. Reconnect with the same initialize profile, list open sessions, attach, read coordinator history, then subscribe again. Subscribe returns a current snapshot and forces an update even if unchanged. Page board messages/history using returned cursors and reset on resetRequired. Unknown notifications are forwarded live, not automatically replayed by initialize.

Attach's `metadata.nativeSession` contains only fields actually observed in native responses/notifications: models, configOptions, modes, availableCommands, title. Check fieldObservations/staleFields and `freshness:last-observed-not-live-query`. Missing values are not invented; silent native changes cannot be detected. Adapter restart changes generation and clears this cache. Never blindly load an active swarm to recover UI state.

Attach never replays approvals. Only the transport initialize path replays pending requests. Snapshot pending IDs are informational (`idNamespace:native-acp`, `actionable:false`) and differ from bridge-mapped request IDs; do not answer them. Native worker command approvals were not forwarded to parent ACP in the tested ask-mode build. Auto mode uses the native permission flag, not synthetic approval replies.

Native file state is not a liveness/completion proof (`runtimeVerified:false`). coordinatorPromptActive describes client-issued turns only. Worker transcripts are native JSONL, not worker ACP sessions or full ACP replay. Native local storage is required. Start/add/post/model/kill operations are coordinator prompts using the native tools, not deterministic extension RPCs. Native kill is permanent and may remove isolated workspaces; session/cancel only cancels the native turn and does not prove the team stopped.
