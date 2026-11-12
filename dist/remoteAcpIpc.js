"use strict";
var __createBinding = (this && this.__createBinding) || (Object.create ? (function(o, m, k, k2) {
    if (k2 === undefined) k2 = k;
    var desc = Object.getOwnPropertyDescriptor(m, k);
    if (!desc || ("get" in desc ? !m.__esModule : desc.writable || desc.configurable)) {
      desc = { enumerable: true, get: function() { return m[k]; } };
    }
    Object.defineProperty(o, k2, desc);
}) : (function(o, m, k, k2) {
    if (k2 === undefined) k2 = k;
    o[k2] = m[k];
}));
var __setModuleDefault = (this && this.__setModuleDefault) || (Object.create ? (function(o, v) {
    Object.defineProperty(o, "default", { enumerable: true, value: v });
}) : function(o, v) {
    o["default"] = v;
});
var __importStar = (this && this.__importStar) || (function () {
    var ownKeys = function(o) {
        ownKeys = Object.getOwnPropertyNames || function (o) {
            var ar = [];
            for (var k in o) if (Object.prototype.hasOwnProperty.call(o, k)) ar[ar.length] = k;
            return ar;
        };
        return ownKeys(o);
    };
    return function (mod) {
        if (mod && mod.__esModule) return mod;
        var result = {};
        if (mod != null) for (var k = ownKeys(mod), i = 0; i < k.length; i++) if (k[i] !== "default") __createBinding(result, mod, k[i]);
        __setModuleDefault(result, mod);
        return result;
    };
})();
Object.defineProperty(exports, "__esModule", { value: true });
exports.trustedRemoteAcpFrame = trustedRemoteAcpFrame;
exports.registerRemoteAcp = registerRemoteAcp;
/** Only the trusted local desktop top frame can launch an SSH ACP controller. */
const electron_1 = require("electron");
const path = __importStar(require("node:path"));
const remoteAcp_1 = require("./remoteAcp");
const nativeRemote_1 = require("./nativeRemote");
function trustedRemoteAcpFrame(event, port) {
    try {
        const url = new URL(event.senderFrame.url);
        return (port > 0 &&
            event.senderFrame === event.sender.mainFrame &&
            ['http:', 'https:'].includes(url.protocol) &&
            ['localhost', '127.0.0.1'].includes(url.hostname) &&
            Number(url.port) === port &&
            !url.username &&
            !url.password);
    }
    catch {
        return false;
    }
}
function registerRemoteAcp(getLanguageServerPort) {
    const manager = new remoteAcp_1.RemoteAcpManager(path.join(electron_1.app.getPath('home'), '.gemini', 'antigravity'));
    const native = new nativeRemote_1.NativeRemoteService(manager);
    const owners = new Set();
    for (const channel of ['remote-acp:native', 'remote-acp:native-snapshot', 'remote-acp:native-catalog']) {
        electron_1.ipcMain.handle(channel, async (event, method, input) => {
            if (!trustedRemoteAcpFrame(event, getLanguageServerPort()))
                return { success: false, error: 'SSH ACP requires the trusted desktop main frame.' };
            try {
                if (channel === 'remote-acp:native-catalog') {
                    native.captureCatalog(input || {});
                    return { success: true };
                }
                return {
                    success: true,
                    result: channel === 'remote-acp:native-snapshot' ? await native.snapshot() : await native.call(method, input),
                };
            }
            catch (error) {
                return { success: false, error: error.message };
            }
        });
    }
    electron_1.ipcMain.handle('remote-acp:action', async (event, operation, input) => {
        if (!trustedRemoteAcpFrame(event, getLanguageServerPort()))
            return { success: false, error: 'SSH ACP requires the trusted desktop main frame.' };
        const owner = event.sender.id;
        if (!owners.has(owner)) {
            owners.add(owner);
            event.sender.once('destroyed', () => {
                owners.delete(owner);
                manager.release(owner);
            });
        }
        try {
            return { success: true, result: await manager.action(owner, operation, input) };
        }
        catch (error) {
            return { success: false, error: error.message };
        }
    });
    electron_1.app.on('will-quit', () => {
        native.close();
        manager.close();
    });
}
//# sourceMappingURL=remoteAcpIpc.js.map