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
exports.startNativeTransport = startNativeTransport;
exports.stopNativeTransport = stopNativeTransport;
/** Loopback entry supporting native h2c gRPC and HTTP/1 Connect clients. */
const http = __importStar(require("node:http"));
const http2 = __importStar(require("node:http2"));
const net = __importStar(require("node:net"));
let server;
let h2Server;
let starting;
let port = 0;
const sessions = new Set();
const sockets = new Set();
const H2_PREFACE = Buffer.from('PRI * HTTP/2.0\r\n\r\nSM\r\n\r\n');
async function startNativeTransport(targetPort) {
    if (starting)
        return starting;
    if (server && port)
        return port;
    const handler = (req, res) => {
        const authority = String(req.headers[':authority'] || req.headers.host || '');
        if (![`localhost:${port}`, `127.0.0.1:${port}`].includes(authority) ||
            req.headers.origin !== undefined ||
            req.rawHeaders.filter((_, index) => index % 2 === 0 && req.rawHeaders[index].toLowerCase() === 'host').length >
                1 ||
            !req.url?.startsWith('/') ||
            /^[\\/]{2}/.test(req.url)) {
            res.statusCode = 403;
            res.end();
            return;
        }
        const headers = {};
        for (const [name, value] of Object.entries(req.headers)) {
            if (!name.startsWith(':') && !['host', 'connection', 'transfer-encoding'].includes(name))
                headers[name] = value;
        }
        headers.host = `127.0.0.1:${targetPort}`;
        const outbound = http.request({ hostname: '127.0.0.1', port: targetPort, path: req.url, method: req.method, headers }, (upstream) => {
            const responseHeaders = {};
            for (const [name, value] of Object.entries(upstream.headers)) {
                if (!['connection', 'keep-alive', 'transfer-encoding', 'trailer'].includes(name))
                    responseHeaders[name] = value;
            }
            // HTTP/1 needs a declaration; HTTP/2 carries the same trailing metadata natively.
            if (req.httpVersionMajor === 1 && upstream.headers.trailer)
                responseHeaders.trailer = upstream.headers.trailer;
            res.statusCode = upstream.statusCode || 502;
            for (const [name, value] of Object.entries(responseHeaders))
                if (value !== undefined)
                    res.setHeader(name, value);
            if (res instanceof http2.Http2ServerResponse)
                res.writeHead(res.statusCode);
            else
                res.writeHead(res.statusCode);
            upstream.on('error', () => res.destroy());
            upstream.on('aborted', () => res.destroy());
            upstream.on('end', () => {
                res.addTrailers(upstream.trailers);
                res.end();
            });
            upstream.pipe(res, { end: false });
        });
        outbound.on('error', () => {
            if (!res.headersSent)
                res.statusCode = 502;
            res.end();
        });
        req.on('aborted', () => outbound.destroy());
        req.on('error', () => outbound.destroy());
        res.on('close', () => outbound.destroy());
        req.pipe(outbound);
    };
    const http1 = http.createServer(handler);
    h2Server = http2.createServer(handler);
    h2Server.on('session', (session) => {
        sessions.add(session);
        session.on('error', () => session.destroy());
        session.on('close', () => sessions.delete(session));
    });
    starting = (async () => {
        const h2Port = await new Promise((resolve, reject) => {
            h2Server.once('error', reject);
            h2Server.listen(0, '127.0.0.1', () => resolve(h2Server.address().port));
        });
        server = net.createServer((socket) => {
            sockets.add(socket);
            socket.on('error', () => socket.destroy());
            socket.on('close', () => sockets.delete(socket));
            socket.setTimeout(10000, () => socket.destroy());
            let preface = Buffer.alloc(0);
            const identify = (chunk) => {
                preface = Buffer.concat([preface, chunk]);
                const size = Math.min(preface.length, H2_PREFACE.length);
                const h2 = preface.subarray(0, size).equals(H2_PREFACE.subarray(0, size));
                if (h2 && size < H2_PREFACE.length)
                    return;
                socket.pause();
                socket.off('data', identify);
                socket.setTimeout(0);
                if (!h2) {
                    socket.unshift(preface);
                    http1.emit('connection', socket);
                    socket.resume();
                    return;
                }
                // HTTP/2 uses native reads, so buffered preface bytes must go through a real socket.
                const upstream = net.connect(h2Port, '127.0.0.1', () => {
                    upstream.write(preface);
                    socket.pipe(upstream);
                    upstream.pipe(socket);
                    socket.resume();
                });
                upstream.on('error', () => socket.destroy());
                upstream.on('close', () => socket.destroy());
                socket.on('close', () => upstream.destroy());
            };
            socket.on('data', identify);
        });
        return await new Promise((resolve, reject) => {
            server.once('error', reject);
            server.listen(0, '127.0.0.1', () => {
                port = server.address().port;
                resolve(port);
            });
        });
    })()
        .catch(async (error) => {
        for (const socket of sockets)
            socket.destroy();
        h2Server?.close();
        server?.close();
        h2Server = undefined;
        server = undefined;
        port = 0;
        throw error;
    })
        .finally(() => {
        starting = undefined;
    });
    return starting;
}
function stopNativeTransport() {
    if (!starting && !server && !h2Server)
        return undefined;
    return (async () => {
        if (starting)
            await starting.catch(() => { });
        const closing = [server, h2Server];
        server = undefined;
        h2Server = undefined;
        port = 0;
        for (const socket of sockets)
            socket.destroy();
        for (const session of sessions)
            session.destroy();
        for (const listener of closing)
            if (listener)
                await new Promise((resolve) => listener.close(() => resolve()));
    })();
}
//# sourceMappingURL=nativeTransport.js.map