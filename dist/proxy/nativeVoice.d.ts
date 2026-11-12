/** Keep the vendor LS voice/tool tunnel; bridge its full-duplex gRPC over the site WebSocket. */
import * as http from 'node:http';
import type { CustomModel } from '../proxy';
export declare const VOICE_RPC_PATH = "/google.gca.aicode.v1alpha.JetskiRemoteAgentProxyService/StreamVoice";
export declare function stopNativeVoice(): void;
export declare function routeNativeVoice(req: http.IncomingMessage, res: http.ServerResponse, getSource: () => CustomModel | undefined): boolean;
//# sourceMappingURL=nativeVoice.d.ts.map