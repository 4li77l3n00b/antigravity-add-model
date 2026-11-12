/** Bridge desktop dictation RPCs before the LS opens its independent Google connection. */
import type * as http from 'node:http';
import type { CustomModel } from '../proxy';
export declare const LOCAL_AUDIO_PATH = "/__model_patch/audio/";
export declare const LOCAL_AUDIO_METHODS: string[];
/** Called only after the proxy's owned-loopback origin check. Never forwards caller identity or models. */
export declare function routeLocalTranscription(req: http.IncomingMessage, res: http.ServerResponse, getSource: () => CustomModel | undefined): boolean;
export declare function stopLocalTranscription(): void;
//# sourceMappingURL=localTranscription.d.ts.map