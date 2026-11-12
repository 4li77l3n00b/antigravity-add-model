/** Read-only native swarm views. Worker transcripts are never ACP conversations. */
import type { RemoteAcpClient } from './remoteAcp';
type Data = Record<string, any>;
export declare function nativeSwarmCapabilities(initialize: Data | undefined): Data | undefined;
export declare class NativeSwarmView {
    private client;
    readonly sessionId: string;
    readonly capabilities: Data;
    private render;
    private latest?;
    private board;
    private cursor;
    private more;
    private members;
    private subscription?;
    private subscribed;
    private syncing?;
    private lastSync;
    private disposed;
    private error?;
    private limited;
    private bytes;
    private revision;
    private metadata?;
    constructor(client: Pick<RemoteAcpClient, 'request'>, sessionId: string, capabilities: Data, render: (key: string, title: string, markdown: string, readError: boolean) => void);
    start(): Promise<void>;
    metadataNotice(metadata: Data): void;
    update(snapshot: Data): void;
    /** Notifications contain only the first board page; fill it without running models. */
    refresh(): void;
    noteGap(): void;
    fail(error: Error): void;
    private mergeBoard;
    private readPages;
    private renderBoard;
    private renderMember;
    close(): void;
}
export {};
//# sourceMappingURL=nativeSwarm.d.ts.map