/** Keep the vendor LS voice/tool tunnel; bridge its full-duplex gRPC over the site WebSocket. */
import * as http from 'node:http';
import * as http2 from 'node:http2';
import { gunzipSync } from 'node:zlib';
import type { CustomModel } from '../proxy';
import { toolServiceBase } from './upstreamTools';
import { VoiceWebSocket } from './voiceWebSocket';

export const VOICE_RPC_PATH = '/google.gca.aicode.v1alpha.JetskiRemoteAgentProxyService/StreamVoice';
const MAX_MESSAGE = 4 * 1024 * 1024;
const active = new Set<() => void>();

function frame(data: Buffer): Buffer {
  const header = Buffer.alloc(5);
  header.writeUInt32BE(data.length, 1);
  return Buffer.concat([header, data]);
}

export function stopNativeVoice(): void {
  for (const stop of [...active]) stop();
}

export function routeNativeVoice(
  req: http.IncomingMessage,
  res: http.ServerResponse,
  getSource: () => CustomModel | undefined,
): boolean {
  if (req.url !== VOICE_RPC_PATH) return false;
  let source: CustomModel | undefined;
  let done = false;
  let transport: VoiceWebSocket | undefined;
  let direct: http2.ClientHttp2Session | undefined;
  let timeout: NodeJS.Timeout;
  let heartbeat: NodeJS.Timeout;
  let pending = Buffer.alloc(0);
  const finish = (status = 14) => {
    if (done) return;
    done = true;
    active.delete(cancel);
    clearTimeout(timeout);
    clearInterval(heartbeat);
    if (!res.destroyed && !res.writableEnded) {
      if (!res.headersSent) res.writeHead(200, { 'content-type': 'application/grpc+proto', Trailer: 'grpc-status' });
      res.addTrailers({ 'grpc-status': String(status) });
      res.end();
    }
    if (transport) transport.terminate();
    direct?.destroy();
    // Stop reading the microphone as soon as the upstream session terminates.
    req.unpipe();
    req.resume();
  };
  const cancel = () => finish(1);
  if (req.method !== 'POST' || !String(req.headers['content-type'] || '').startsWith('application/grpc')) {
    finish(3);
    return true;
  }
  try {
    source = getSource();
  } catch {
    finish(9);
    return true;
  }
  active.add(cancel);
  timeout = setTimeout(() => finish(4), 15 * 60 * 1000);
  timeout.unref();
  res.once('close', cancel);
  req.once('aborted', cancel);
  req.once('error', cancel);
  if (!source) {
    // Preserve stock voice when no site tool connection has been configured.
    direct = http2.connect('https://aicode.googleapis.com');
    direct.once('error', () => finish());
    const headers: http2.OutgoingHttpHeaders = {
      ':method': 'POST',
      ':path': VOICE_RPC_PATH,
      'content-type': 'application/grpc+proto',
      te: 'trailers',
    };
    for (const name of ['authorization', 'user-agent', 'x-goog-user-project', 'grpc-encoding'])
      if (req.headers[name]) headers[name] = req.headers[name];
    const upstream = direct.request(headers);
    let status = 14;
    upstream.on('error', () => finish());
    upstream.on('response', (headers) => {
      if (headers[':status'] !== 200) return finish();
      if (headers['grpc-status'] !== undefined) status = Number(headers['grpc-status']);
      res.writeHead(200, {
        'content-type': 'application/grpc+proto',
        Trailer: 'grpc-status',
        ...(headers['grpc-encoding'] ? { 'grpc-encoding': headers['grpc-encoding'] } : {}),
      });
    });
    upstream.on('trailers', (headers) => {
      const value = Number(headers['grpc-status']);
      status = Number.isInteger(value) && value >= 0 && value <= 16 ? value : 14;
    });
    upstream.on('end', () => finish(status));
    upstream.pipe(res, { end: false });
    req.pipe(upstream);
    return true;
  }
  try {
    const url = toolServiceBase(source.apiUrl);
    url.protocol = url.protocol === 'https:' ? 'wss:' : 'ws:';
    if (url.protocol === 'ws:' && !['localhost', '127.0.0.1', '[::1]'].includes(url.hostname))
      throw new Error('Remote voice connections require HTTPS');
    url.pathname += 'v1/tools/antigravity/voice';
    const headers: http.OutgoingHttpHeaders = {};
    // Only the established group selector is needed for the site tool channel.
    // Google identity, caller cookies, proxies and arbitrary WS headers stay local.
    for (const [key, value] of Object.entries(source.customHeaders || {}))
      if (key.toLowerCase() === 'x-tokennest-group') headers['x-tokennest-group'] = value;
    const caller = (source.externalModelName || source.name).trim();
    if (!caller || Buffer.byteLength(caller) > 256) throw new Error('Invalid tool caller model');
    http.validateHeaderValue('x-tokennest-model', caller);
    headers['x-tokennest-model'] = caller;
    headers.authorization = `Bearer ${source.apiKey}`;
    transport = new VoiceWebSocket(url, 'tokennest-voice-v1', {
      headers,
      perMessageDeflate: false,
      maxPayload: MAX_MESSAGE,
      followRedirects: false,
      handshakeTimeout: 10000,
    });
    req.pause();
    transport.on('error', () => finish());
    transport.on('close', () => finish());
    transport.on('unexpected-response', (_request, response) => {
      const status =
        response.statusCode === 401 ? 16 : response.statusCode === 403 ? 7 : response.statusCode === 429 ? 8 : 14;
      response.resume();
      finish(status);
    });
    transport.on('open', () => {
      if (done) return;
      if (transport!.protocol !== 'tokennest-voice-v1') return finish();
      res.writeHead(200, { 'content-type': 'application/grpc+proto', Trailer: 'grpc-status' });
      req.resume();
      heartbeat = setInterval(() => {
        if (!done) transport!.ping();
      }, 20000);
      heartbeat.unref();
    });
    transport.on('message', (data, binary) => {
      if (done) return;
      const bytes = Buffer.isBuffer(data) ? data : Buffer.from(data as ArrayBuffer);
      if (binary) {
        if (!bytes.length || bytes.length > MAX_MESSAGE || res.writableLength > 8 * 1024 * 1024) return finish(8);
        res.write(frame(bytes));
        return;
      }
      try {
        const status = (JSON.parse(bytes.toString('utf8')) as { grpcStatus: number }).grpcStatus;
        if (!Number.isInteger(status) || status < 0 || status > 16 || pending.length) return finish();
        finish(status);
      } catch {
        finish();
      }
    });
    req.on('data', (chunk: Buffer) => {
      if (done) return;
      pending = Buffer.concat([pending, chunk]);
      if (pending.length > MAX_MESSAGE + 5) return finish(8);
      while (pending.length >= 5) {
        const length = pending.readUInt32BE(1),
          flags = pending[0];
        if (!length || length > MAX_MESSAGE || flags > 1) return finish(3);
        if (pending.length < length + 5) break;
        let payload = pending.subarray(5, length + 5);
        pending = pending.subarray(length + 5);
        if (flags === 1) {
          if (req.headers['grpc-encoding'] !== 'gzip') return finish(12);
          try {
            payload = gunzipSync(payload, { maxOutputLength: MAX_MESSAGE });
          } catch {
            return finish(3);
          }
        }
        if (transport!.readyState !== VoiceWebSocket.OPEN) return finish();
        if (transport!.bufferedAmount > 8 * 1024 * 1024) return finish(8);
        transport!.send(payload, { binary: true });
      }
    });
    req.on('end', () => {
      if (done) return;
      if (pending.length) return finish(3);
      if (transport!.readyState !== VoiceWebSocket.OPEN) return finish();
      transport!.send('{"type":"end"}');
    });
  } catch {
    finish(9);
  }
  return true;
}
