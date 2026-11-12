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
exports.startVoiceTransport = startVoiceTransport;
exports.stopVoiceTransport = stopVoiceTransport;
/** The LS voice client always uses TLS. Its vendor localhost policy accepts this ephemeral certificate. */
const http = __importStar(require("node:http"));
const http2 = __importStar(require("node:http2"));
const voiceWebSocket_1 = require("./voiceWebSocket");
const nativeVoice_1 = require("./nativeVoice");
let listener;
let starting;
let port = 0;
const sessions = new Set();
const upstreams = new Set();
async function startVoiceTransport(targetPort) {
    if (starting)
        return starting;
    if (listener && port)
        return port;
    starting = (async () => {
        // Keep the key only in memory. No global trust store or TLS policy changes.
        const certificate = await (0, voiceWebSocket_1.generateLoopbackCertificate)([{ name: 'commonName', value: 'localhost' }], {
            keyType: 'ec',
            curve: 'P-256',
            algorithm: 'sha256',
            notAfterDate: new Date(Date.now() + 24 * 60 * 60 * 1000),
            extensions: [{ name: 'subjectAltName', altNames: [{ type: 2, value: 'localhost' }] }],
        });
        listener = http2.createSecureServer({ key: certificate.private, cert: certificate.cert, minVersion: 'TLSv1.2' }, (req, res) => {
            if (req.headers[':authority'] !== `localhost:${port}` ||
                req.headers.origin !== undefined ||
                !req.url?.startsWith('/') ||
                /^[\\/]{2}/.test(req.url)) {
                res.writeHead(403);
                res.end();
                return;
            }
            if (req.url === nativeVoice_1.VOICE_RPC_PATH) {
                const headers = { host: `127.0.0.1:${targetPort}` };
                for (const [name, value] of Object.entries(req.headers))
                    if (!name.startsWith(':') && !['host', 'connection', 'transfer-encoding'].includes(name))
                        headers[name] = value;
                const outbound = http.request({ hostname: '127.0.0.1', port: targetPort, path: nativeVoice_1.VOICE_RPC_PATH, method: req.method, headers }, (upstream) => {
                    res.writeHead(upstream.statusCode || 502, { 'content-type': 'application/grpc+proto' });
                    upstream.on('error', () => res.destroy());
                    upstream.on('aborted', () => res.destroy());
                    upstream.on('end', () => {
                        res.addTrailers(upstream.trailers);
                        res.end();
                    });
                    upstream.pipe(res, { end: false });
                });
                outbound.on('error', () => res.destroy());
                req.on('aborted', () => outbound.destroy());
                res.on('close', () => outbound.destroy());
                req.pipe(outbound);
                return;
            }
            // AICODE_ENDPOINT_URL also governs other native remote-agent methods.
            // Preserve those against the fixed Google origin, outside the site proxy.
            const upstream = http2.connect('https://aicode.googleapis.com');
            upstreams.add(upstream);
            upstream.on('error', () => res.destroy());
            upstream.on('close', () => upstreams.delete(upstream));
            const headers = {
                ...req.headers,
                ':authority': 'aicode.googleapis.com',
                ':scheme': 'https',
            };
            const call = upstream.request(headers);
            call.on('error', () => res.destroy());
            call.on('response', (fields) => {
                const responseHeaders = Object.fromEntries(Object.entries(fields).filter(([name]) => !name.startsWith(':')));
                res.writeHead(Number(fields[':status'] || 502), responseHeaders);
            });
            call.on('trailers', (fields) => {
                res.addTrailers(fields);
            });
            call.on('end', () => res.end());
            req.on('aborted', () => call.destroy());
            res.on('close', () => upstream.destroy());
            call.pipe(res, { end: false });
            req.pipe(call);
        });
        listener.on('session', (session) => {
            sessions.add(session);
            session.on('error', () => session.destroy());
            session.on('close', () => sessions.delete(session));
        });
        listener.on('stream', (stream) => stream.on('error', () => { }));
        return await new Promise((resolve, reject) => {
            listener.once('error', reject);
            listener.listen(0, '127.0.0.1', () => {
                port = listener.address().port;
                resolve(port);
            });
        });
    })()
        .catch((error) => {
        listener?.close();
        listener = undefined;
        port = 0;
        throw error;
    })
        .finally(() => {
        starting = undefined;
    });
    return starting;
}
function stopVoiceTransport() {
    if (!starting && !listener)
        return undefined;
    return (async () => {
        if (starting)
            await starting.catch(() => { });
        const closing = listener;
        listener = undefined;
        port = 0;
        for (const session of sessions)
            session.destroy();
        for (const session of upstreams)
            session.destroy();
        if (closing)
            await new Promise((resolve) => closing.close(() => resolve()));
    })();
}
//# sourceMappingURL=voiceTransport.js.map