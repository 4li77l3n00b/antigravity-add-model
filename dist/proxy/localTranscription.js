"use strict";
Object.defineProperty(exports, "__esModule", { value: true });
exports.LOCAL_AUDIO_METHODS = exports.LOCAL_AUDIO_PATH = void 0;
exports.routeLocalTranscription = routeLocalTranscription;
exports.stopLocalTranscription = stopLocalTranscription;
const node_crypto_1 = require("node:crypto");
const node_zlib_1 = require("node:zlib");
const upstreamTools_1 = require("./upstreamTools");
exports.LOCAL_AUDIO_PATH = '/__model_patch/audio/';
exports.LOCAL_AUDIO_METHODS = ['StreamAudioTranscription', 'SendAudioChunk', 'EndAudioSession'];
const MAX_AUDIO_BYTES = 9 * 1024 * 1024 - 44;
const sessions = new Map();
const grpcCodes = {
    canceled: '1',
    invalid_argument: '3',
    deadline_exceeded: '4',
    not_found: '5',
    permission_denied: '7',
    resource_exhausted: '8',
    failed_precondition: '9',
    unimplemented: '12',
    internal: '13',
    unavailable: '14',
    unauthenticated: '16',
};
class AudioError extends Error {
    constructor(code) {
        super('Desktop transcription request was rejected or is unavailable.');
        this.code = code;
    }
}
function envelope(payload, flags = 0) {
    const header = Buffer.alloc(5);
    header[0] = flags;
    header.writeUInt32BE(payload.length, 1);
    return Buffer.concat([header, payload]);
}
function protobufBytes(field, data) {
    const bytes = typeof data === 'string' ? Buffer.from(data) : data;
    const length = [];
    for (let n = bytes.length;; n = Math.floor(n / 128)) {
        length.push((n % 128) | (n >= 128 ? 128 : 0));
        if (n < 128)
            break;
    }
    return Buffer.concat([Buffer.from([field * 8 + 2, ...length]), bytes]);
}
function decodeAudioRequest(method, body, json) {
    if (json) {
        const value = JSON.parse(body.toString('utf8'));
        if (!value || typeof value !== 'object' || Array.isArray(value))
            throw new AudioError('invalid_argument');
        return value;
    }
    let at = 0;
    const read = () => {
        let value = 0;
        for (let shift = 0; shift < 56 && at < body.length; shift += 7) {
            const byte = body[at++];
            value += (byte & 127) * 2 ** shift;
            if (byte < 128 && Number.isSafeInteger(value))
                return value;
        }
        throw new AudioError('invalid_argument');
    };
    const value = {};
    const names = method === 'StreamAudioTranscription'
        ? ['mimeType', 'model', 'preCursorText', 'postCursorText', 'cascadeId', 'continuous']
        : method === 'SendAudioChunk'
            ? ['sessionId', 'data', 'sequenceNumber']
            : ['sessionId'];
    while (at < body.length) {
        const tag = read(), field = Math.floor(tag / 8), wire = tag % 8;
        if (!field)
            throw new AudioError('invalid_argument');
        const key = names[field - 1];
        if (wire === 0) {
            const n = read();
            if (key === 'sequenceNumber')
                value[key] = n;
            else if (key === 'continuous')
                value[key] = n !== 0;
        }
        else if (wire === 2) {
            const length = read(), start = at;
            at += length;
            if (at > body.length)
                throw new AudioError('invalid_argument');
            if (key)
                value[key] =
                    key === 'data' ? body.subarray(start, at).toString('base64') : body.subarray(start, at).toString('utf8');
        }
        else if (wire === 1)
            at += 8;
        else if (wire === 5)
            at += 4;
        else
            throw new AudioError('invalid_argument');
        if (at > body.length)
            throw new AudioError('invalid_argument');
    }
    return value;
}
function transcriptionRecording(session) {
    const indices = [...session.chunks.keys()].sort((a, b) => a - b);
    if (indices.length && ![0, 1].includes(indices[0]))
        throw new AudioError('invalid_argument');
    for (let i = 1; i < indices.length; i++) {
        if (indices[i] !== indices[i - 1] + 1)
            throw new AudioError('invalid_argument');
    }
    const audio = Buffer.concat(indices.map((index) => session.chunks.get(index)));
    const pcm = /^audio\/pcm;rate=(\d+)$/.exec(session.mime);
    if (!pcm)
        return audio;
    if (audio.length % 2)
        throw new AudioError('invalid_argument');
    const rate = Number(pcm[1]), header = Buffer.alloc(44);
    header.write('RIFF');
    header.writeUInt32LE(audio.length + 36, 4);
    header.write('WAVEfmt ', 8);
    header.writeUInt32LE(16, 16);
    header.writeUInt16LE(1, 20);
    header.writeUInt16LE(1, 22);
    header.writeUInt32LE(rate, 24);
    header.writeUInt32LE(rate * 2, 28);
    header.writeUInt16LE(2, 32);
    header.writeUInt16LE(16, 34);
    header.write('data', 36);
    header.writeUInt32LE(audio.length, 40);
    return Buffer.concat([header, audio]);
}
async function completeAudioSession(session) {
    try {
        if (session.bytes) {
            const audio = transcriptionRecording(session);
            session.chunks.clear();
            const result = await (0, upstreamTools_1.callToolProxy)(session.source, '/v1/tools/antigravity/transcription', Buffer.from(JSON.stringify({
                audioData: audio.toString('base64'),
                audioMimeType: session.mime.startsWith('audio/pcm;') ? 'audio/wav' : session.mime,
            })), 'application/json', session.abort.signal);
            if (result.status !== 200)
                throw new AudioError({
                    400: 'invalid_argument',
                    401: 'unauthenticated',
                    403: 'permission_denied',
                    429: 'resource_exhausted',
                    501: 'unimplemented',
                    503: 'unavailable',
                    504: 'deadline_exceeded',
                }[result.status] || 'internal');
            const value = JSON.parse(result.body.toString('utf8'));
            if (typeof value.transcribedText !== 'string')
                throw new AudioError('internal');
            session.send({ transcription: { text: value.transcribedText, isFinal: true } }, protobufBytes(2, Buffer.concat([protobufBytes(1, value.transcribedText), Buffer.from([16, 1])])));
        }
        session.send({ complete: {} }, protobufBytes(3, Buffer.alloc(0)));
        session.finish();
    }
    catch (error) {
        session.finish(error instanceof AudioError ? error.code : 'internal');
    }
}
/** Called only after the proxy's owned-loopback origin check. Never forwards caller identity or models. */
function routeLocalTranscription(req, res, getSource) {
    const pathname = new URL(req.url, 'http://localhost').pathname;
    if (!pathname.startsWith(exports.LOCAL_AUDIO_PATH))
        return false;
    const method = pathname.slice(exports.LOCAL_AUDIO_PATH.length);
    res.setHeader('Cache-Control', 'no-store');
    if (req.headers.origin) {
        res.setHeader('Access-Control-Allow-Origin', req.headers.origin);
        res.setHeader('Access-Control-Allow-Credentials', 'true');
        res.setHeader('Vary', 'Origin');
        res.setHeader('Access-Control-Expose-Headers', 'Grpc-Status, Grpc-Message');
    }
    if (req.method === 'OPTIONS' && exports.LOCAL_AUDIO_METHODS.includes(method)) {
        res.writeHead(204, {
            'Access-Control-Allow-Methods': 'POST, OPTIONS',
            'Access-Control-Allow-Headers': String(req.headers['access-control-request-headers'] || 'content-type, connect-protocol-version'),
            'Access-Control-Max-Age': '600',
        });
        res.end();
        return true;
    }
    const type = String(req.headers['content-type'] || '').split(';')[0];
    const json = type === 'application/json' || type === 'application/connect+json';
    const grpc = type.startsWith('application/grpc');
    const framed = grpc || type.startsWith('application/connect+');
    const format = type.startsWith('application/grpc-web') ? 'grpc-web' : grpc ? 'grpc' : 'connect';
    const reply = (code) => {
        if (res.destroyed || res.writableEnded)
            return;
        if (grpc) {
            res.writeHead(200, { 'Content-Type': type, ...(format === 'grpc' ? { Trailer: 'Grpc-Status' } : {}) });
            if (!code)
                res.write(envelope(Buffer.alloc(0)));
            if (format === 'grpc-web')
                res.end(envelope(Buffer.from(`grpc-status: ${code ? grpcCodes[code] : '0'}\r\n`), 128));
            else {
                res.addTrailers({ 'grpc-status': code ? grpcCodes[code] : '0' });
                res.end();
            }
        }
        else {
            res.writeHead(code ? { unimplemented: 501, not_found: 404, resource_exhausted: 429, unavailable: 503 }[code] || 400 : 200, { 'Content-Type': code || json ? 'application/json' : 'application/proto' });
            res.end(code
                ? JSON.stringify({ code, message: 'Desktop transcription request was rejected or is unavailable.' })
                : json
                    ? '{}'
                    : Buffer.alloc(0));
        }
    };
    if (req.method !== 'POST' || !exports.LOCAL_AUDIO_METHODS.includes(method)) {
        reply('unimplemented');
        return true;
    }
    if (![
        'application/json',
        'application/proto',
        'application/protobuf',
        'application/connect+json',
        'application/connect+proto',
        'application/grpc',
        'application/grpc+proto',
        'application/grpc-web+proto',
    ].includes(type)) {
        reply('invalid_argument');
        return true;
    }
    const parts = [];
    let size = 0, rejected = false;
    req.on('data', (part) => {
        size += part.length;
        if (size > 1024 * 1024) {
            rejected = true;
            parts.length = 0;
            reply('resource_exhausted');
        }
        if (!rejected)
            parts.push(part);
    });
    req.on('error', () => reply('invalid_argument'));
    req.on('end', () => {
        if (rejected || res.destroyed)
            return;
        try {
            let body = Buffer.concat(parts);
            if (req.headers['content-encoding'] === 'gzip')
                body = (0, node_zlib_1.gunzipSync)(body, { maxOutputLength: 1024 * 1024 });
            else if (req.headers['content-encoding'] && req.headers['content-encoding'] !== 'identity')
                throw new AudioError('invalid_argument');
            if (framed) {
                if (body.length < 5 || body.readUInt32BE(1) !== body.length - 5)
                    throw new AudioError('invalid_argument');
                const flag = body[0];
                body = body.subarray(5);
                if (flag === 1 &&
                    (req.headers['grpc-encoding'] === 'gzip' || req.headers['connect-content-encoding'] === 'gzip'))
                    body = (0, node_zlib_1.gunzipSync)(body, { maxOutputLength: 1024 * 1024 });
                else if (flag !== 0)
                    throw new AudioError('invalid_argument');
            }
            const input = decodeAudioRequest(method, body, json);
            if (method === 'StreamAudioTranscription') {
                const source = getSource();
                if (!source)
                    throw new AudioError('failed_precondition');
                if (input.continuous === true)
                    throw new AudioError('unimplemented');
                const mime = String(input.mimeType ?? input.mime_type ?? 'audio/pcm;rate=16000');
                const pcm = /^audio\/pcm;rate=(\d+)$/.exec(mime);
                if (!(pcm && Number(pcm[1]) >= 8000 && Number(pcm[1]) <= 48000) &&
                    !['audio/wav', 'audio/ogg', 'audio/webm', 'audio/mpeg', 'audio/mp4', 'audio/flac'].includes(mime))
                    throw new AudioError('invalid_argument');
                if (sessions.size >= 8)
                    throw new AudioError('resource_exhausted');
                const id = (0, node_crypto_1.randomUUID)(), abort = new AbortController();
                res.writeHead(200, {
                    'Content-Type': format === 'connect' ? `application/connect+${json ? 'json' : 'proto'}` : type,
                    ...(format === 'grpc' ? { Trailer: 'Grpc-Status' } : {}),
                });
                res.flushHeaders();
                let closed = false;
                const session = {
                    source,
                    mime,
                    chunks: new Map(),
                    bytes: 0,
                    ending: false,
                    abort,
                    send: (value, protobuf) => {
                        if (!closed)
                            res.write(envelope(json ? Buffer.from(JSON.stringify(value)) : protobuf));
                    },
                    finish: (code) => {
                        if (closed)
                            return;
                        closed = true;
                        clearTimeout(timeout);
                        sessions.delete(id);
                        session.chunks.clear();
                        abort.abort();
                        if (res.destroyed)
                            return;
                        if (format === 'connect')
                            res.end(envelope(Buffer.from(JSON.stringify(code
                                ? { error: { code, message: 'Desktop transcription request was rejected or is unavailable.' } }
                                : {})), 2));
                        else if (format === 'grpc-web')
                            res.end(envelope(Buffer.from(`grpc-status: ${code ? grpcCodes[code] : '0'}\r\n`), 128));
                        else {
                            res.addTrailers({ 'grpc-status': code ? grpcCodes[code] : '0' });
                            res.end();
                        }
                    },
                };
                const timeout = setTimeout(() => session.finish('deadline_exceeded'), Math.max(1000, Math.min(source.timeout || 180000, 3600000)));
                timeout.unref();
                sessions.set(id, session);
                res.on('close', () => session.finish('canceled'));
                session.send({ ready: { sessionId: id } }, protobufBytes(1, protobufBytes(1, id)));
                return;
            }
            const id = input.sessionId ?? input.session_id;
            if (typeof id !== 'string' || !sessions.has(id))
                throw new AudioError('not_found');
            const session = sessions.get(id);
            if (session.ending)
                throw new AudioError('failed_precondition');
            if (method === 'SendAudioChunk') {
                const number = input.sequenceNumber ?? input.sequence_number ?? 0;
                if (!Number.isInteger(number) ||
                    Number(number) < 0 ||
                    Number(number) > 0xffffffff ||
                    typeof input.data !== 'string' ||
                    !/^(?:[A-Za-z0-9+/]{4})*(?:[A-Za-z0-9+/]{2}==|[A-Za-z0-9+/]{3}=)?$/.test(input.data))
                    throw new AudioError('invalid_argument');
                const data = Buffer.from(input.data, 'base64'), previous = session.chunks.get(Number(number));
                if (previous) {
                    if (!previous.equals(data))
                        throw new AudioError('invalid_argument');
                }
                else {
                    if (session.bytes + data.length > MAX_AUDIO_BYTES || session.chunks.size >= 4096) {
                        session.finish('resource_exhausted');
                        throw new AudioError('resource_exhausted');
                    }
                    session.chunks.set(Number(number), data);
                    session.bytes += data.length;
                }
                reply();
                return;
            }
            session.ending = true;
            reply();
            void completeAudioSession(session);
        }
        catch (error) {
            reply(error instanceof AudioError ? error.code : 'invalid_argument');
        }
    });
    return true;
}
function stopLocalTranscription() {
    for (const session of sessions.values())
        session.finish('canceled');
}
//# sourceMappingURL=localTranscription.js.map