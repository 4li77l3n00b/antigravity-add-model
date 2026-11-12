/** TokenNest tool transport. The saved source supplies credentials, never the incoming Google identity. */
import * as http from 'node:http';
import type { CustomModel } from '../proxy';
export declare function toolServiceBase(apiUrl: string): URL;
export declare function toolProxySource(models: CustomModel[]): CustomModel | undefined;
/** Remove legacy Metadata/API keys and unknown fields instead of forwarding them to a third party. */
export declare function publicRPCPayload(method: string, body: Buffer, framed?: boolean): Buffer;
/** Unary Connect JSON uses the same public schema as the binary bridge. */
export declare function publicRPCJSON(method: string, body: Buffer): Buffer;
export interface GenerationTool {
    action: 'search' | 'image';
    input: Record<string, unknown>;
}
export declare function generationTool(body: Record<string, unknown>): GenerationTool | undefined;
export declare function generationToolResult(action: string, output: Record<string, unknown>): Record<string, unknown>;
export declare function callToolProxy(source: CustomModel, pathname: string, body: Buffer, contentType: string, signal: AbortSignal, method?: 'POST' | 'GET'): Promise<{
    status: number;
    body: Buffer;
    grpcStatus: string;
}>;
export declare function sendToolRPC(res: http.ServerResponse, type: string, frame: Buffer, status: string): void;
/** Non-tool RPCs retain their native Google route and Google credentials. */
export declare function forwardGoogleRPC(req: http.IncomingMessage, res: http.ServerResponse, body: Buffer): void;
export declare function isNativeTool(method: string): boolean;
/** Decide synchronously; ordinary requests retain the original proxy lifecycle. Tool failures never replay. */
export declare function routeUpstreamTools(req: http.IncomingMessage, res: http.ServerResponse, body: Buffer, getSource: () => CustomModel | undefined): boolean;
/** The native ASR handshake must be answered before the recording is complete.
 * Each completed turn becomes one authenticated, billable site tool request.
 * Model, project and personal Google credentials from the stream are discarded.
 */
export declare function routeTranscriptionStream(req: http.IncomingMessage, res: http.ServerResponse, getSource: () => CustomModel | undefined): boolean;
//# sourceMappingURL=upstreamTools.d.ts.map