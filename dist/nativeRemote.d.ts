import { RemoteAcpClient, RemoteAcpManager, remoteAcpStatus, type RemoteAcpConfig, type RemoteAcpProject } from './remoteAcp';
type Data = Record<string, any>;
export declare function remoteConversationId(projectId: string, sessionId: string, conversationId?: string): string;
export declare function remoteProjectUri(project: RemoteAcpProject): string;
export declare class NativeRemoteService {
    readonly manager: RemoteAcpManager;
    private createClient;
    private status;
    private connections;
    private refreshes;
    private pages;
    private drafts;
    private conversations;
    private deletions;
    private deletedSessions;
    private deletedConversations;
    private modelSelections;
    constructor(manager: RemoteAcpManager, createClient?: (config: RemoteAcpConfig) => RemoteAcpClient, status?: typeof remoteAcpStatus);
    private conversation;
    private endpoint;
    private sessionKey;
    private deleteConversation;
    private connect;
    private refreshProject;
    private attachSwarm;
    private load;
    private step;
    private drain;
    private draft;
    private remoteModels;
    private metadata;
    private nativeProject;
    captureCatalog(value: Data): void;
    snapshot(): Promise<Data>;
    private saveSession;
    call(method: string, input?: Data): Promise<{
        handled: boolean;
        value?: any;
        input?: Data;
    }>;
    private files;
    close(): void;
}
export {};
//# sourceMappingURL=nativeRemote.d.ts.map