/** ACP over a strict, non-interactive SSH stdio connection. Never replay a prompt. */
import { type ChildProcessWithoutNullStreams } from 'node:child_process';
export interface RemoteAcpConfig {
    host: string;
    command: string;
    instance: string;
    cwd: string;
}
export declare const REMOTE_ACP_PROFILE: {
    protocolVersion: number;
    clientInfo: {
        name: string;
        version: string;
    };
    clientCapabilities: {};
};
type JsonObject = Record<string, any>;
export interface RemoteAcpEvent {
    seq: number;
    kind: string;
    data: any;
}
export type SshSpawn = (command: string, args: string[]) => ChildProcessWithoutNullStreams;
export declare function validateRemoteAcpConfig(value: unknown): RemoteAcpConfig;
/** OpenSSH invokes a remote shell even though the local spawn uses shell:false. */
export declare function remoteSshArgs(config: RemoteAcpConfig, operation: 'connect' | 'status' | 'job', jobId?: string): string[];
export declare class RemoteAcpClient {
    readonly config: RemoteAcpConfig;
    private spawnSsh;
    private process?;
    private buffer;
    private calls;
    private requests;
    private events;
    private eventBytes;
    private seq;
    private serial;
    private generation;
    private turns;
    private deleting;
    private inheritedTurns;
    private used;
    private sessions;
    private ready?;
    private info?;
    private state;
    private failure;
    private stderr;
    constructor(config: RemoteAcpConfig, spawnSsh?: SshSpawn);
    connect(): Promise<any>;
    snapshot(after?: number): {
        connectionId: `${string}-${string}-${string}-${string}-${string}`;
        state: string;
        failure: string;
        stderr: string;
        config: RemoteAcpConfig;
        initialize: JsonObject;
        events: {
            seq: number;
            kind: string;
            data: any;
        }[];
        cursor: number;
        gap: boolean;
        pending: {
            token: string;
            method: string;
            params: JsonObject;
        }[];
        activeSessions: string[];
        sessions: JsonObject[];
    };
    syncRemoteStatus(status: JsonObject): void;
    deleteSession(sessionId: string): Promise<void>;
    request(method: unknown, params: unknown): Promise<any>;
    respond(token: unknown, reply: unknown): void;
    disconnect(reason?: string): void;
    private record;
    private write;
    private sendRequest;
    private receive;
    private dispatch;
}
/** One-shot management is independent of the ACP controller and never submits prompts. */
export declare function remoteAcpStatus(config: RemoteAcpConfig, spawnSsh?: SshSpawn, jobId?: string): Promise<any>;
export interface RemoteAcpProject {
    id: string;
    name: string;
    config: RemoteAcpConfig;
    mcpServers: JsonObject[];
    lastSessionId?: string;
    archived?: boolean;
}
export interface RemoteAcpSessionIndex {
    projectId: string;
    conversationId?: string;
    sessionId: string;
    title: string;
    remoteTitle?: string;
    annotations?: JsonObject;
    archived: boolean;
    pinned: boolean;
    updatedAt: string;
}
export interface RemoteAcpWorkspace {
    version: 1;
    projects: RemoteAcpProject[];
    sessions: RemoteAcpSessionIndex[];
    deletedSessions?: Pick<RemoteAcpSessionIndex, 'projectId' | 'sessionId' | 'conversationId'>[];
    selectedProjectId?: string;
}
export declare class RemoteAcpManager {
    readonly directory: string;
    private spawnSsh;
    private client?;
    private owner?;
    private filename;
    private workspaceFile;
    private lastRemoteStatus;
    constructor(directory: string, spawnSsh?: SshSpawn);
    config(): RemoteAcpConfig | null;
    save(value: unknown): RemoteAcpConfig;
    workspace(): RemoteAcpWorkspace;
    removeDeletedSession(config: RemoteAcpConfig, sessionId: string): RemoteAcpSessionIndex[];
    private updateWorkspace;
    action(owner: number, operation: string, input?: JsonObject): Promise<any>;
    release(owner: number): void;
    close(): void;
}
export {};
//# sourceMappingURL=remoteAcp.d.ts.map