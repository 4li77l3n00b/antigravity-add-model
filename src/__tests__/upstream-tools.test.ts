import { afterEach, describe, expect, it, vi } from 'vitest';
import * as http from 'node:http';
import { gzipSync } from 'node:zlib';
import * as http2 from 'node:http2';
import { once } from 'node:events';
import type { CustomModel } from '../proxy';
import {
  callToolProxy,
  generationTool,
  generationToolResult,
  publicRPCPayload,
  publicRPCJSON,
  routeUpstreamTools,
  routeTranscriptionStream,
  toolProxySource,
  toolServiceBase,
} from '../proxy/upstreamTools';
import { startNativeTransport, stopNativeTransport } from '../proxy/nativeTransport';
import { routeLocalTranscription, stopLocalTranscription } from '../proxy/localTranscription';
import { WebSocketServer } from 'ws';
import { routeNativeVoice, stopNativeVoice, VOICE_RPC_PATH } from '../proxy/nativeVoice';
import { startVoiceTransport, stopVoiceTransport } from '../proxy/voiceTransport';
import { guardLoopbackRequest } from '../proxy/loopbackSecurity';

const servers: http.Server[] = [];
const rpcServers: http2.Http2Server[] = [];
const rpcSessions = new Set<http2.ServerHttp2Session>();
afterEach(async () => {
  stopLocalTranscription();
  stopNativeVoice();
  await stopVoiceTransport();
  await stopNativeTransport();
  for (const session of rpcSessions) session.destroy();
  rpcSessions.clear();
  for (const server of rpcServers.splice(0)) await new Promise<void>((resolve) => server.close(() => resolve()));
  for (const server of servers.splice(0)) {
    server.closeAllConnections();
    await new Promise<void>((resolve) => server.close(() => resolve()));
  }
});
async function serve(handler: http.RequestListener): Promise<string> {
  const server = http.createServer(handler);
  servers.push(server);
  server.listen(0, '127.0.0.1');
  await once(server, 'listening');
  return `http://127.0.0.1:${(server.address() as { port: number }).port}`;
}
async function serveRPC(
  handler: (req: http2.Http2ServerRequest, res: http2.Http2ServerResponse) => void,
): Promise<string> {
  const server = http2.createServer(handler);
  rpcServers.push(server);
  server.on('session', (session) => {
    rpcSessions.add(session);
    session.on('error', () => session.destroy());
    session.on('close', () => rpcSessions.delete(session));
  });
  server.on('stream', (stream) => stream.on('error', () => {}));
  server.listen(0, '127.0.0.1');
  await once(server, 'listening');
  return `http://127.0.0.1:${(server.address() as { port: number }).port}`;
}
function model(url: string): CustomModel {
  return {
    name: 'models/site',
    displayName: 'Site',
    description: '',
    provider: 'google',
    apiUrl: url + '/v1beta',
    apiKey: 'site-key',
    externalModelName: 'third-party',
    enabled: true,
    toolProxy: true,
    customHeaders: {
      'X-TokenNest-Group': 'fixture',
      'X-TokenNest-Model': 'untrusted-override',
      Authorization: 'must-be-overridden',
      ':authority': 'untrusted.example',
      Connection: 'upgrade',
      'Keep-Alive': 'timeout=5',
      'Proxy-Connection': 'keep-alive',
      Upgrade: 'websocket',
      'HTTP2-Settings': 'invalid',
    },
  };
}
function frame(payload: Buffer): Buffer {
  const header = Buffer.alloc(5);
  header.writeUInt32BE(payload.length, 1);
  return Buffer.concat([header, payload]);
}
function field(number: number, text: string | Buffer): Buffer {
  const value = typeof text === 'string' ? Buffer.from(text) : text;
  return Buffer.concat([Buffer.from([number * 8 + 2, value.length]), value]);
}
async function read(req: http.IncomingMessage | http2.Http2ServerRequest): Promise<Buffer> {
  const chunks: Buffer[] = [];
  for await (const chunk of req) chunks.push(chunk);
  return Buffer.concat(chunks);
}

describe('TokenNest desktop upstream tools', () => {
  it.each([
    '/v1',
    '/v1/chat/completions',
    '/v1/responses',
    '/v1/messages',
    '/v1beta',
    '/v1beta/models/gemini-test:generateContent',
  ])('normalizes a saved %s endpoint while preserving deployment prefixes', (suffix) => {
    expect(toolServiceBase('https://site.example/gateway' + suffix).href).toBe('https://site.example/gateway/');
  });
  it('rejects ambiguous, disabled and insecure sources', () => {
    const source = model('https://site.example');
    expect(toolProxySource([])).toBeUndefined();
    expect(() => toolProxySource([source, { ...source, name: 'models/other' }])).toThrow(/exactly one/);
    expect(() => toolProxySource([{ ...source, enabled: false }])).toThrow(/Enable/);
    expect(() => toolProxySource([{ ...source, apiUrl: 'http://remote.example/v1' }])).toThrow(/HTTPS/);
    expect(() => toolProxySource([{ ...source, allowUnauthorized: true }])).toThrow(/certificate/);
  });
  it('strips legacy account metadata and rejects incomplete native frames', () => {
    const query = field(2, 'query');
    const input = frame(Buffer.concat([field(1, 'usable-google-token'), query, field(12, 'unknown-account')]));
    expect(publicRPCPayload('GetWebSearchResults', input)).toEqual(frame(query));
    expect(() => publicRPCPayload('GetWebSearchResults', input.subarray(1))).toThrow(/frame/);
    expect(() => publicRPCPayload('WhoAmI', input)).toThrow(/Unsupported/);
  });
  it.each([
    ['0', false],
    ['12', false],
    ['16', false],
    ['0', true],
    ['12', true],
    ['16', true],
  ] as const)(
    'preserves gRPC status %s across HTTP/2 (trailers stripped=%s) and substitutes only the saved site key',
    async (status, stripped) => {
      const calls: { path: string; headers: http2.IncomingHttpHeaders; body: Buffer; version: string }[] = [];
      const output = frame(field(2, 'transcript'));
      const upstream = await serveRPC(async (req, res) => {
        calls.push({ path: req.url!, headers: req.headers, body: await read(req), version: req.httpVersion });
        res.writeHead(200, {
          'Content-Type': 'application/grpc+proto',
          ...(stripped ? { 'X-TokenNest-Grpc-Status': status } : { 'X-TokenNest-Grpc-Status': '13' }),
        });
        if (status === '0') res.write(output);
        if (!stripped) res.addTrailers({ 'grpc-status': status });
        res.end();
      });
      const source = model(upstream + '/gateway');
      const local = await serve(async (req, res) => {
        await routeUpstreamTools(req, res, await read(req), () => source);
      });
      const port = await startNativeTransport(Number(new URL(local).port));
      const session = http2.connect(`http://127.0.0.1:${port}`);
      const request = session.request({
        ':method': 'POST',
        ':path': '/exa.api_server_pb.ApiServerService/GetTranscription',
        'content-type': 'application/grpc+proto',
        authorization: 'Bearer desktop-google-token',
        cookie: 'desktop-cookie',
      });
      const chunks: Buffer[] = [];
      let trailers: http2.IncomingHttpHeaders = {};
      request.on('data', (chunk: Buffer) => chunks.push(chunk));
      request.on('trailers', (value) => {
        trailers = value;
      });
      request.end(frame(Buffer.concat([field(1, 'google-token-in-metadata'), field(2, 'audio')])));
      await once(request, 'end');
      session.close();
      expect(trailers['grpc-status']).toBe(status);
      expect(Buffer.concat(chunks)).toEqual(status === '0' ? output : Buffer.alloc(0));
      expect(calls).toHaveLength(1);
      expect(calls[0].path).toBe('/gateway/exa.api_server_pb.ApiServerService/GetTranscription');
      expect(calls[0].version).toBe('2.0');
      expect(calls[0].headers.te).toBe('trailers');
      expect(calls[0].headers.authorization).toBe('Bearer site-key');
      expect(calls[0].headers.cookie).toBeUndefined();
      expect(calls[0].headers[':authority']).toBe(new URL(upstream).host);
      for (const header of ['connection', 'keep-alive', 'proxy-connection', 'upgrade', 'http2-settings'])
        expect(calls[0].headers[header]).toBeUndefined();
      expect(calls[0].headers['x-tokennest-group']).toBe('fixture');
      expect(calls[0].headers['x-tokennest-model']).toBe('third-party');
      expect(calls[0].body.toString()).not.toContain('google-token');
    },
  );
  it.each(['cancel', 'reset', 'timeout', 'oversized'] as const)(
    'closes an HTTP/2 tool stream on %s without replaying the request',
    async (failure) => {
      const controller = new AbortController();
      const call = vi.fn();
      const upstream = await serveRPC(async (req, res) => {
        call();
        await read(req);
        if (failure === 'cancel') controller.abort();
        else if (failure === 'reset') res.stream.close(http2.constants.NGHTTP2_INTERNAL_ERROR);
        else if (failure === 'oversized') {
          res.writeHead(200, { 'content-type': 'application/grpc+proto', 'grpc-status': '0' });
          res.end(Buffer.alloc(16 * 1024 * 1024 + 1));
        }
      });
      const source = { ...model(upstream), timeout: failure === 'timeout' ? 100 : 5000 };
      await expect(
        callToolProxy(
          source,
          '/exa.api_server_pb.ApiServerService/GetEmbeddings',
          frame(field(1, 'query')),
          'application/grpc+proto',
          controller.signal,
        ),
      ).rejects.toThrow(
        failure === 'timeout' ? /timed out/ : failure === 'oversized' ? /16 MiB/ : /cancelled|interrupted/,
      );
      expect(call).toHaveBeenCalledTimes(1);
    },
  );
  it('rejects HTTP/1-only native upstreams without retrying over HTTP/1', async () => {
    const call = vi.fn();
    const upstream = await serve(call);
    await expect(
      callToolProxy(
        model(upstream),
        '/exa.api_server_pb.ApiServerService/GetEmbeddings',
        frame(field(1, 'query')),
        'application/grpc+proto',
        new AbortController().signal,
      ),
    ).rejects.toThrow();
    expect(call).not.toHaveBeenCalled();
  });
  it('keeps bare Connect protobuf unary responses compatible', async () => {
    const output = frame(field(1, 'https://example.com'));
    const upstream = await serveRPC(async (req, res) => {
      expect((await read(req)).subarray(5)).toEqual(field(1, 'https://example.com'));
      res.writeHead(200, { 'Content-Type': 'application/grpc+proto' });
      res.write(output);
      res.addTrailers({ 'grpc-status': '0' });
      res.end();
    });
    const local = await serve(async (req, res) => {
      await routeUpstreamTools(req, res, await read(req), () => model(upstream));
    });
    const response = await fetch(local + '/exa.api_server_pb.ApiServerService/GetWebSearchRedirect', {
      method: 'POST',
      headers: { 'Content-Type': 'application/proto' },
      body: field(1, 'https://example.com'),
    });
    expect(response.status).toBe(200);
    expect(Buffer.from(await response.arrayBuffer())).toEqual(output.subarray(5));
  });
  it('routes REST page utilities without forwarding project or OAuth metadata', async () => {
    const upstream = await serve(async (req, res) => {
      expect(req.url).toBe('/v1internal:rewriteUri');
      expect(JSON.parse((await read(req)).toString())).toEqual({ originalUri: 'https://example.com' });
      res.end('{"redirectUri":"https://example.com/source"}');
    });
    const local = await serve(async (req, res) => {
      await routeUpstreamTools(req, res, await read(req), () => model(upstream));
    });
    const response = await fetch(local + '/v1internal:rewriteUri?key=google-token', {
      method: 'POST',
      headers: { 'Content-Type': 'application/json', Authorization: 'Bearer google-token' },
      body: JSON.stringify({
        originalUri: 'https://example.com',
        project: 'private-project',
        metadata: { token: 'google-token' },
      }),
    });
    expect(await response.json()).toEqual({ redirectUri: 'https://example.com/source' });
  });
  it.each([false, true])(
    'restores real search grounding for an isolated built-in search (stream=%s)',
    async (stream) => {
      const upstream = await serve(async (req, res) => {
        expect(req.url).toBe('/v1beta/models/third-party:generateContent');
        expect(JSON.parse((await read(req)).toString())).toEqual({
          contents: [{ role: 'user', parts: [{ text: 'Go website' }] }],
          tools: [{ googleSearch: {} }],
        });
        res.end(
          JSON.stringify(
            generationToolResult('search', {
              answer: 'go.dev',
              queries: ['Go website'],
              results: [{ url: 'https://go.dev', title: 'Go' }],
            }),
          ),
        );
      });
      const source = model(upstream);
      const local = await serve(async (req, res) => {
        await routeUpstreamTools(req, res, await read(req), () => source);
      });
      const response = await fetch(local + '/v1internal:' + (stream ? 'streamGenerateContent' : 'generateContent'), {
        method: 'POST',
        body: JSON.stringify({
          model: 'MODEL_999',
          project: 'private',
          request: { contents: [{ role: 'user', parts: [{ text: 'Go website' }] }], tools: [{ googleSearch: {} }] },
        }),
      });
      const text = await response.text();
      const output = JSON.parse(stream ? text.slice('data: '.length).trim() : text);
      expect(output.response.candidates[0].groundingMetadata).toEqual({
        webSearchQueries: ['Go website'],
        groundingChunks: [{ web: { uri: 'https://go.dev', title: 'Go' } }],
      });
      expect(text).not.toContain('private');
    },
  );
  it('accepts HTTP/1 Connect on the same native entry and rejects browser origins', async () => {
    const output = frame(field(1, 'https://example.com/source'));
    const upstream = await serveRPC(async (req, res) => {
      await read(req);
      res.writeHead(200, { 'Content-Type': 'application/grpc+proto', 'grpc-status': '0' });
      res.end(output);
    });
    const local = await serve(async (req, res) => {
      routeUpstreamTools(req, res, await read(req), () => model(upstream));
    });
    const port = await startNativeTransport(Number(new URL(local).port));
    const url = `http://127.0.0.1:${port}/exa.api_server_pb.ApiServerService/GetWebSearchRedirect`;
    const options = {
      method: 'POST',
      headers: { 'Content-Type': 'application/proto' },
      body: field(1, 'https://example.com'),
    };
    const response = await fetch(url, options);
    expect(response.status).toBe(200);
    expect(Buffer.from(await response.arrayBuffer())).toEqual(output.subarray(5));
    expect(
      (await fetch(url, { ...options, headers: { ...options.headers, Origin: 'https://untrusted.example' } })).status,
    ).toBe(403);
  });
  it('encodes grpc-web errors without replaying a failed tool request', async () => {
    const call = vi.fn();
    const upstream = await serveRPC(async (req, res) => {
      call();
      await read(req);
      res.writeHead(200, { 'Content-Type': 'application/grpc+proto', 'grpc-status': '12' });
      res.end();
    });
    const local = await serve(async (req, res) => {
      routeUpstreamTools(req, res, await read(req), () => model(upstream));
    });
    const response = await fetch(local + '/exa.api_server_pb.ApiServerService/GetEmbeddings', {
      method: 'POST',
      headers: { 'Content-Type': 'application/grpc-web+proto' },
      body: frame(field(1, 'embedding-request')),
    });
    const body = Buffer.from(await response.arrayBuffer());
    expect(response.status).toBe(200);
    expect(body[0]).toBe(128);
    expect(body.subarray(5).toString()).toBe('grpc-status: 12\r\n');
    expect(call).toHaveBeenCalledTimes(1);
  });
  it('converts image tool jobs and preserves generated inline images', async () => {
    const upstream = await serve(async (req, res) => {
      expect(req.url).toBe('/v1beta/models/third-party:generateContent');
      expect(JSON.parse((await read(req)).toString())).toEqual({
        contents: [
          {
            role: 'user',
            parts: [{ text: 'draw a cat' }, { inlineData: { mimeType: 'image/png', data: 'aW5wdXQ=' } }],
          },
        ],
        generationConfig: { responseModalities: ['TEXT', 'IMAGE'], imageConfig: { aspectRatio: '16:9' } },
      });
      res.end(JSON.stringify({ text: 'A cat', images: [{ mimeType: 'image/png', inlineData: 'b3V0cHV0' }] }));
    });
    const local = await serve(async (req, res) => {
      routeUpstreamTools(req, res, await read(req), () => model(upstream));
    });
    const response = await fetch(local + '/v1beta/models/gemini-image:generateContent', {
      method: 'POST',
      body: JSON.stringify({
        contents: [
          {
            role: 'user',
            parts: [{ text: 'draw a cat' }, { inlineData: { mimeType: 'image/png', data: 'aW5wdXQ=' } }],
          },
        ],
        generationConfig: { responseModalities: ['TEXT', 'IMAGE'], imageConfig: { aspectRatio: '16:9' } },
      }),
    });
    const output = await response.json();
    expect(output.candidates[0].content.parts).toEqual([
      { text: 'A cat' },
      { inlineData: { mimeType: 'image/png', data: 'b3V0cHV0' } },
    ]);
  });
  it.each([
    { image: { mimeType: 'image/png', base64Data: 'aW1hZ2U=' } },
    { generatedMedia: { mimeType: 'image/png', inlineData: 'aW1hZ2U=' } },
    {
      images: [{ mimeType: 'image/png', inlineData: 'aW1hZ2U=' }],
      generatedMedia: { mimeType: 'image/png', inlineData: 'aW1hZ2U=' },
    },
  ])('converts native and managed image outputs without losing or duplicating media', (output) => {
    const result = generationToolResult('image', output) as { candidates: { content: { parts: unknown[] } }[] };
    expect(result.candidates[0].content.parts).toEqual([{ inlineData: { mimeType: 'image/png', data: 'aW1hZ2U=' } }]);
  });
  it.each([
    { image: { uri: 'gs://private/image' } },
    { generatedMedia: { blobref: { blobId: 'opaque' } } },
    { images: [{ mimeType: 'image/png', inlineData: '!!!' }] },
  ])('rejects unusable image results instead of returning an empty successful candidate', (output) => {
    expect(() => generationToolResult('image', output)).toThrow(/image/i);
  });
  it('leaves unsupported native image media unchanged when no tool source is configured', async () => {
    const local = await serve(async (req, res) => {
      expect(routeUpstreamTools(req, res, await read(req), () => undefined)).toBe(false);
      res.writeHead(204);
      res.end();
    });
    const response = await fetch(local + '/v1internal:generateContent', {
      method: 'POST',
      body: JSON.stringify({
        request: {
          contents: [{ parts: [{ fileData: { fileUri: 'gs://original-media' } }] }],
          generationConfig: { responseModalities: ['IMAGE'] },
        },
      }),
    });
    expect(response.status).toBe(204);
  });
  it('leaves ordinary model conversations and user function calls on their existing route', async () => {
    expect(
      generationTool({
        contents: [{ role: 'user', parts: [{ text: 'hello' }] }],
        tools: [{ functionDeclarations: [{ name: 'read_file' }] }],
      }),
    ).toBeUndefined();
    const getSource = vi.fn(() => {
      throw new Error('must not read source');
    });
    const local = await serve(async (req, res) => {
      const handled = await routeUpstreamTools(req, res, await read(req), getSource);
      expect(handled).toBe(false);
      res.writeHead(204);
      res.end();
    });
    expect(
      (
        await fetch(local + '/v1internal:generateContent', {
          method: 'POST',
          body: JSON.stringify({ request: { contents: [{ parts: [{ text: 'normal' }] }] } }),
        })
      ).status,
    ).toBe(204);
    expect(getSource).not.toHaveBeenCalled();
  });
});

describe('completed tool transports', () => {
  it('accepts unary Connect JSON while removing Google identities and model overrides', async () => {
    expect(
      JSON.parse(
        publicRPCJSON(
          'GetEmbeddings',
          Buffer.from(
            JSON.stringify({
              metadata: { apiKey: 'google-secret' },
              embeddingModel: 'MODEL_CALLER',
              request: {
                prompts: ['text'],
                prefix: 'EMBEDDING_PREFIX_NOMIC_SEARCH',
                model: 'MODEL_CALLER',
                metadata: { apiKey: 'google-secret' },
              },
            }),
          ),
        ).toString(),
      ),
    ).toEqual({ request: { prompts: ['text'], prefix: 'EMBEDDING_PREFIX_NOMIC_SEARCH' } });
    const upstream = await serve(async (req, res) => {
      expect(req.headers.authorization).toBe('Bearer site-key');
      expect(req.headers['content-type']).toBe('application/json');
      expect(JSON.parse((await read(req)).toString())).toEqual({ audioData: 'YXVkaW8=' });
      res.end('{"transcribedText":"hello"}');
    });
    const local = await serve(async (req, res) => {
      routeUpstreamTools(req, res, await read(req), () => model(upstream));
    });
    const response = await fetch(local + '/exa.api_server_pb.ApiServerService/GetTranscription', {
      method: 'POST',
      headers: {
        'Content-Type': 'application/json',
        'Content-Encoding': 'gzip',
        authorization: 'Bearer google-secret',
      },
      body: gzipSync(Buffer.from('{"metadata":{"apiKey":"google-secret"},"audioData":"YXVkaW8="}')),
    });
    expect(response.status).toBe(200);
    expect(await response.json()).toEqual({ transcribedText: 'hello' });
  });
  it('keeps Connect failures in the JSON error protocol', async () => {
    const upstream = await serve(async (req, res) => {
      await read(req);
      res.writeHead(501);
      res.end('{"code":"unimplemented","message":"unavailable"}');
    });
    const local = await serve(async (req, res) => {
      routeUpstreamTools(req, res, await read(req), () => model(upstream));
    });
    const response = await fetch(local + '/exa.api_server_pb.ApiServerService/GetEmbeddings', {
      method: 'POST',
      headers: { 'Content-Type': 'application/json' },
      body: '{"request":{"prompts":["text"]}}',
    });
    expect(response.status).toBe(501);
    expect(await response.json()).toEqual({ code: 'unimplemented', message: 'unavailable' });
  });
  it('decompresses one gRPC gzip frame before forwarding without replay', async () => {
    let calls = 0;
    const upstream = await serveRPC(async (req, res) => {
      calls++;
      expect(await read(req)).toEqual(frame(field(2, 'audio')));
      res.writeHead(200, { 'Content-Type': 'application/grpc', 'grpc-status': '0' });
      res.end(frame(field(2, 'hello')));
    });
    const local = await serve(async (req, res) => {
      routeUpstreamTools(req, res, await read(req), () => model(upstream));
    });
    const compressed = gzipSync(field(2, 'audio'));
    const body = frame(compressed);
    body[0] = 1;
    const response = await fetch(local + '/exa.api_server_pb.ApiServerService/GetTranscription', {
      method: 'POST',
      headers: { 'Content-Type': 'application/grpc', 'grpc-encoding': 'gzip' },
      body,
    });
    expect(Buffer.from(await response.arrayBuffer())).toEqual(frame(field(2, 'hello')));
    expect(calls).toBe(1);
  });
  it.each(['checkUrlDenylist', 'checkUrlAntivirus'])('routes %s with public URL arguments only', async (method) => {
    const upstream = await serve(async (req, res) => {
      expect(req.url).toBe('/v1internal:' + method);
      expect(JSON.parse((await read(req)).toString())).toEqual({ url: 'https://example.com' });
      res.end('{"isClean":false,"isDenied":true}');
    });
    const local = await serve(async (req, res) => {
      routeUpstreamTools(req, res, await read(req), () => model(upstream));
    });
    const response = await fetch(local + '/v1internal:' + method, {
      method: 'POST',
      body: '{"url":"https://example.com","project":"private","metadata":{"apiKey":"google-secret"}}',
    });
    expect(response.status).toBe(200);
    expect(await response.json()).toEqual({ isClean: false, isDenied: true });
  });
  it('uses the documentation GET resource with pagination and the saved site credential', async () => {
    const upstream = await serve(async (req, res) => {
      expect(req.method).toBe('GET');
      expect(req.url).toBe('/gateway/v1internal/webDocsOptions?pageSize=0&pageToken=page+2');
      expect(req.headers.authorization).toBe('Bearer site-key');
      expect((await read(req)).length).toBe(0);
      res.end('{"webDocsOptions":[]}');
    });
    const local = await serve(async (req, res) => {
      routeUpstreamTools(req, res, await read(req), () => model(upstream + '/gateway'));
    });
    const response = await fetch(local + '/v1internal/webDocsOptions?pageSize=0&pageToken=page+2&key=google-secret');
    expect(response.status).toBe(200);
    expect(await response.json()).toEqual({ webDocsOptions: [] });
  });
  it('preserves search constraints and URI image inputs in gateway jobs', async () => {
    const seen: Record<string, unknown>[] = [];
    const upstream = await serve(async (req, res) => {
      seen.push(JSON.parse((await read(req)).toString()));
      res.end('{"candidates":[{"content":{"role":"model","parts":[{"text":"done"}]},"finishReason":"STOP"}]}');
    });
    const local = await serve(async (req, res) => {
      routeUpstreamTools(req, res, await read(req), () => model(upstream));
    });
    const settings = {
      includedDomains: ['example.com'],
      excludedDomains: ['example.org'],
      enhancedContent: { imageSearch: { maxResultCount: 5 } },
    };
    const jobs = [
      { contents: [{ role: 'user', parts: [{ text: 'query' }] }], tools: [{ googleSearch: settings }] },
      {
        contents: [
          {
            role: 'user',
            parts: [
              { text: 'edit' },
              { fileData: { mimeType: 'image/png', fileUri: 'https://example.com/image.png' } },
            ],
          },
        ],
        generationConfig: { responseModalities: ['IMAGE'] },
      },
    ];
    for (const job of jobs) {
      const response = await fetch(local + '/v1internal:generateContent', {
        method: 'POST',
        body: JSON.stringify(job),
      });
      expect(response.status).toBe(200);
      await response.json();
    }
    expect(seen[0].tools).toEqual([{ googleSearch: settings }]);
    expect(seen[1].contents).toEqual(jobs[1].contents);
  });
  it.each([200, 403])('bridges current ASR streams through the billed transcription tool (HTTP %s)', async (status) => {
    const calls: { headers: http.IncomingHttpHeaders; body: Record<string, unknown> }[] = [];
    const site = await serve(async (req, res) => {
      expect(req.url).toBe('/v1/tools/antigravity/transcription');
      calls.push({ headers: req.headers, body: JSON.parse((await read(req)).toString('utf8')) });
      res.writeHead(status, { 'Content-Type': 'application/json' });
      res.end(JSON.stringify(status === 200 ? { transcribedText: 'Hello world' } : { error: 'not allowed' }));
    });
    const source = model(site);
    const local = await serve((req, res) => {
      expect(routeTranscriptionStream(req, res, () => source)).toBe(true);
    });
    const port = await startNativeTransport(Number(new URL(local).port));
    const session = http2.connect(`http://127.0.0.1:${port}`);
    const request = session.request({
      ':method': 'POST',
      ':path': '/google.gca.aicode.v1alpha.PredictionService/BidiGenerateContent',
      'content-type': 'application/grpc',
      authorization: 'Bearer personal-google-token',
      'x-goog-user-project': 'personal-project',
    });
    const chunks: Buffer[] = [];
    let trailers: http2.IncomingHttpHeaders = {};
    request.on('data', (chunk: Buffer) => chunks.push(chunk));
    request.on('trailers', (value) => {
      trailers = value;
    });
    // The client waits for setup_complete before sending audio. Buffering the
    // entire request before routing would deadlock this real protocol sequence.
    request.write(frame(field(1, field(1, 'models/caller-model'))));
    await once(request, 'data');
    expect(Buffer.concat(chunks)).toEqual(frame(field(1, Buffer.alloc(0))));
    expect(calls).toHaveLength(0);
    const audio = Buffer.from([1, 0, 2, 0]);
    const input = Buffer.concat([
      frame(field(3, field(5, Buffer.alloc(0)))),
      frame(field(3, field(1, Buffer.concat([field(1, 'audio/pcm;rate=16000'), field(2, audio)])))),
      frame(field(3, field(6, Buffer.alloc(0)))),
    ]);
    request.write(input.subarray(0, 9));
    request.end(input.subarray(9));
    await once(request, 'end');
    session.close();
    expect(trailers['grpc-status']).toBe(status === 200 ? '0' : '7');
    expect(calls).toHaveLength(1);
    expect(calls[0].headers.authorization).toBe('Bearer site-key');
    expect(calls[0].headers['x-goog-user-project']).toBeUndefined();
    expect(calls[0].body).not.toHaveProperty('model');
    const wav = Buffer.from(String(calls[0].body.audioData), 'base64');
    expect(wav.subarray(0, 4).toString()).toBe('RIFF');
    expect(wav.readUInt32LE(24)).toBe(16000);
    expect(wav.subarray(44)).toEqual(audio);
    const expected = [frame(field(1, Buffer.alloc(0)))];
    if (status === 200)
      expected.push(frame(field(2, Buffer.concat([field(6, field(1, 'Hello world')), Buffer.from([24, 1])]))));
    expect(Buffer.concat(chunks)).toEqual(Buffer.concat(expected));
  });
});

async function startDictation(local: string, type = 'application/connect+json') {
  const json = type.endsWith('+json');
  const request = http.request(local + '/__model_patch/audio/StreamAudioTranscription', {
    method: 'POST',
    headers: { 'Content-Type': type, Origin: 'https://127.0.0.1:54321' },
  });
  request.end(
    frame(
      json
        ? Buffer.from(
            JSON.stringify({
              mimeType: 'audio/pcm;rate=16000',
              model: 'caller-model',
              preCursorText: 'private context',
            }),
          )
        : field(1, 'audio/pcm;rate=16000'),
    ),
  );
  const [response] = (await once(request, 'response')) as [http.IncomingMessage];
  const parts: Buffer[] = [];
  const done = new Promise<Buffer>((resolve, reject) => {
    response.on('data', (data: Buffer) => parts.push(data));
    response.on('end', () => resolve(Buffer.concat(parts)));
    response.on('error', reject);
  });
  const first = await new Promise<Buffer>((resolve) => {
    let pending = Buffer.alloc(0);
    const accept = (chunk: Buffer) => {
      pending = Buffer.concat([pending, chunk]);
      if (pending.length >= 5 && pending.length >= pending.readUInt32BE(1) + 5) {
        response.off('data', accept);
        resolve(pending.subarray(5, pending.readUInt32BE(1) + 5));
      }
    };
    response.on('data', accept);
  });
  const id = json ? JSON.parse(first.toString()).ready.sessionId : first.subarray(4).toString();
  return { response, id: String(id), done };
}

function dictationMessages(bytes: Buffer) {
  const messages: { flags: number; body: Buffer }[] = [];
  while (bytes.length) {
    expect(bytes.length).toBeGreaterThanOrEqual(5);
    const size = bytes.readUInt32BE(1);
    expect(bytes.length).toBeGreaterThanOrEqual(size + 5);
    messages.push({ flags: bytes[0], body: bytes.subarray(5, size + 5) });
    bytes = bytes.subarray(size + 5);
  }
  return messages;
}

describe('desktop dictation session bridge', () => {
  it.each([
    'application/connect+json',
    'application/connect+proto',
    'application/grpc+proto',
    'application/grpc-web+proto',
  ])('completes the real ready/chunk/end sequence over %s', async (type) => {
    const calls: { body: Record<string, unknown>; headers: http.IncomingHttpHeaders }[] = [];
    const site = await serve(async (req, res) => {
      expect(req.url).toBe('/v1/tools/antigravity/transcription');
      calls.push({ body: JSON.parse((await read(req)).toString()), headers: req.headers });
      res.end(JSON.stringify({ transcribedText: 'Hello world' }));
    });
    const local = await serve((req, res) => {
      if (guardLoopbackRequest(req, res, 54321))
        expect(routeLocalTranscription(req, res, () => model(site))).toBe(true);
    });
    const { response, id, done } = await startDictation(local, type);
    expect(response.headers['access-control-allow-origin']).toBe('https://127.0.0.1:54321');
    expect(calls).toHaveLength(0);
    const audio = [Buffer.from([1, 0]), Buffer.from([2, 0])];
    for (const number of [1, 0, 1]) {
      const reply = await fetch(local + '/__model_patch/audio/SendAudioChunk', {
        method: 'POST',
        headers: { 'Content-Type': 'application/json', Authorization: 'Bearer caller-google-secret' },
        body: JSON.stringify({ sessionId: id, data: audio[number].toString('base64'), sequenceNumber: number }),
      });
      expect(reply.status).toBe(200);
      expect(await reply.json()).toEqual({});
    }
    expect(calls).toHaveLength(0);
    const end = await fetch(local + '/__model_patch/audio/EndAudioSession', {
      method: 'POST',
      headers: { 'Content-Type': 'application/proto' },
      body: field(1, id),
    });
    expect(end.status).toBe(200);
    await end.arrayBuffer();
    const messages = dictationMessages(await done);
    expect(calls).toHaveLength(1);
    expect(calls[0].headers.authorization).toBe('Bearer site-key');
    expect(calls[0].headers.cookie).toBeUndefined();
    expect(Object.keys(calls[0].body).sort()).toEqual(['audioData', 'audioMimeType']);
    const wav = Buffer.from(String(calls[0].body.audioData), 'base64');
    expect(wav.readUInt32LE(24)).toBe(16000);
    expect(wav.subarray(44)).toEqual(Buffer.concat(audio));
    if (type.endsWith('+json')) {
      expect(messages.map((value) => JSON.parse(value.body.toString()))).toEqual([
        { ready: { sessionId: id } },
        { transcription: { text: 'Hello world', isFinal: true } },
        { complete: {} },
        {},
      ]);
      expect(messages.at(-1)!.flags).toBe(2);
    } else {
      expect(messages[1].body).toEqual(field(2, Buffer.concat([field(1, 'Hello world'), Buffer.from([16, 1])])));
      expect(messages[2].body).toEqual(field(3, Buffer.alloc(0)));
      if (type === 'application/connect+proto') expect(messages.at(-1)).toEqual({ flags: 2, body: Buffer.from('{}') });
      if (type.startsWith('application/grpc-web'))
        expect(messages.at(-1)).toEqual({ flags: 128, body: Buffer.from('grpc-status: 0\r\n') });
      if (type === 'application/grpc+proto') expect(response.trailers['grpc-status']).toBe('0');
    }
    const repeat = await fetch(local + '/__model_patch/audio/EndAudioSession', {
      method: 'POST',
      headers: { 'Content-Type': 'application/json' },
      body: JSON.stringify({ sessionId: id }),
    });
    expect(repeat.status).toBe(404);
    await repeat.arrayBuffer();
    expect(calls).toHaveLength(1);
  });

  it.each(['upstream rejection', 'missing chunk', 'shutdown'])(
    'reports %s without fabricated completion',
    async (mode) => {
      let calls = 0;
      const site = await serve(async (req, res) => {
        calls++;
        await read(req);
        res.writeHead(403);
        res.end('private upstream error');
      });
      const local = await serve((req, res) => {
        routeLocalTranscription(req, res, () => model(site));
      });
      const { id, done } = await startDictation(local);
      if (mode === 'shutdown') stopLocalTranscription();
      else {
        const chunk = await fetch(local + '/__model_patch/audio/SendAudioChunk', {
          method: 'POST',
          headers: { 'Content-Type': 'application/json' },
          body: JSON.stringify({ sessionId: id, sequenceNumber: mode === 'missing chunk' ? 3 : 0, data: 'AQACAA==' }),
        });
        expect(chunk.status).toBe(200);
        await chunk.arrayBuffer();
        const end = await fetch(local + '/__model_patch/audio/EndAudioSession', {
          method: 'POST',
          headers: { 'Content-Type': 'application/json' },
          body: JSON.stringify({ sessionId: id }),
        });
        expect(end.status).toBe(200);
        await end.arrayBuffer();
      }
      const messages = dictationMessages(await done);
      expect(messages).toHaveLength(2);
      expect(messages[1].flags).toBe(2);
      const error = JSON.parse(messages[1].body.toString()).error;
      expect(error.code).toBe(
        mode === 'upstream rejection' ? 'permission_denied' : mode === 'shutdown' ? 'canceled' : 'invalid_argument',
      );
      expect(calls).toBe(mode === 'upstream rejection' ? 1 : 0);
      expect(messages[1].body.toString()).not.toContain('private upstream error');
    },
  );

  it('handles owned-origin CORS and rejects unknown sessions, invalid formats and unavailable continuous mode', async () => {
    const local = await serve((req, res) => {
      if (guardLoopbackRequest(req, res, 54321)) routeLocalTranscription(req, res, () => model('https://site.example'));
    });
    const preflight = await fetch(local + '/__model_patch/audio/SendAudioChunk', {
      method: 'OPTIONS',
      headers: {
        Origin: 'https://127.0.0.1:54321',
        'Access-Control-Request-Headers': 'content-type,x-codeium-csrf-token',
      },
    });
    expect(preflight.status).toBe(204);
    expect(preflight.headers.get('access-control-allow-credentials')).toBe('true');
    const evil = await fetch(local + '/__model_patch/audio/StreamAudioTranscription', {
      method: 'POST',
      headers: { Origin: 'https://evil.example', 'Content-Type': 'application/json' },
      body: '{}',
    });
    expect(evil.status).toBe(403);
    await evil.arrayBuffer();
    for (const [method, body, status] of [
      ['SendAudioChunk', { sessionId: 'unowned', data: 'AQACAA==' }, 404],
      ['StreamAudioTranscription', { mimeType: 'video/mp4' }, 400],
      ['StreamAudioTranscription', { continuous: true }, 501],
    ] as const) {
      const result = await fetch(local + '/__model_patch/audio/' + method, {
        method: 'POST',
        headers: { 'Content-Type': 'application/json' },
        body: JSON.stringify(body),
      });
      expect(result.status).toBe(status);
      await result.arrayBuffer();
    }
  });
});

describe('full duplex desktop voice', () => {
  it.each(['google', 'openai'] as const)(
    'keeps native tool replies and audio streaming through a %s site connection',
    async (format) => {
      const received: Buffer[] = [];
      const audio = Buffer.from([10, 4, 10, 2, 7, 8]);
      const toolCall = Buffer.from([10, 6, 26, 4, 8, 1, 18, 0]);
      const site = http.createServer();
      servers.push(site);
      const ws = new WebSocketServer({ server: site, handleProtocols: () => 'tokennest-voice-v1' });
      site.listen(0, '127.0.0.1');
      await once(site, 'listening');
      ws.on('connection', (peer, request) => {
        expect(request.url).toBe('/v1/tools/antigravity/voice');
        expect(request.headers.authorization).toBe('Bearer site-key');
        expect(request.headers['x-tokennest-model']).toBe('third-party');
        expect(request.headers['x-tokennest-group']).toBe('fixture');
        expect(request.headers.cookie).toBeUndefined();
        expect(request.headers['x-goog-user-project']).toBeUndefined();
        peer.on('message', (raw, binary) => {
          const data = Buffer.from(raw as Buffer);
          if (!binary) {
            expect(data.toString()).toBe('{"type":"end"}');
            peer.send('{"grpcStatus":0}');
            return;
          }
          received.push(data);
          if (received.length === 1) peer.send(Buffer.from([10, 4, 18, 2, 8, 2]));
          if (received.length === 2) {
            peer.send(toolCall);
            peer.send(audio);
          }
        });
      });
      const url = `http://127.0.0.1:${(site.address() as { port: number }).port}`;
      const selected = model(url);
      selected.apiFormat = format;
      selected.apiUrl = url + (format === 'openai' ? '/v1' : '/v1beta');
      const local = await serve((req, res) => {
        routeNativeVoice(req, res, () => selected);
      });
      const port = await startVoiceTransport(Number(new URL(local).port));
      const native = http2.connect(`https://localhost:${port}`, { rejectUnauthorized: false });
      try {
        const request = native.request({
          ':method': 'POST',
          ':path': VOICE_RPC_PATH,
          'content-type': 'application/grpc',
          'grpc-encoding': 'gzip',
          authorization: 'Bearer local-google-token',
          cookie: 'local-cookie',
          'x-goog-user-project': 'local-project',
        });
        request.on('error', () => {});
        let pending = Buffer.alloc(0);
        let status: string | undefined;
        const response = new Promise<Buffer[]>((resolve, reject) => {
          const messages: Buffer[] = [];
          request.on('error', reject);
          request.on('trailers', (headers) => {
            status = String(headers['grpc-status']);
          });
          request.on('data', (chunk: Buffer) => {
            pending = Buffer.concat([pending, chunk]);
            while (pending.length >= 5 && pending.length >= pending.readUInt32BE(1) + 5) {
              const length = pending.readUInt32BE(1);
              messages.push(pending.subarray(5, length + 5));
              pending = pending.subarray(length + 5);
              if (messages.length === 1) {
                // Microphone input remains open when LISTENING arrives.
                const packed = frame(gzipSync(Buffer.from([10, 4, 18, 2, 0, 0])));
                packed[0] = 1;
                request.write(packed);
              }
              if (messages.length === 3) {
                request.write(frame(Buffer.from([10, 4, 34, 2, 8, 1])));
                request.end();
              }
            }
          });
          request.on('end', () => resolve(messages));
        });
        request.write(frame(Buffer.from([10, 2, 10, 0])));
        expect(await response).toEqual([Buffer.from([10, 4, 18, 2, 8, 2]), toolCall, audio]);
        expect(status).toBe('0');
        expect(pending).toHaveLength(0);
        expect(received).toEqual([
          Buffer.from([10, 2, 10, 0]),
          Buffer.from([10, 4, 18, 2, 0, 0]),
          Buffer.from([10, 4, 34, 2, 8, 1]),
        ]);
      } finally {
        native.destroy();
        for (const peer of ws.clients) peer.terminate();
        ws.close();
      }
    },
  );

  it('reports an interrupted WebSocket as unavailable instead of successful gRPC completion', async () => {
    const site = http.createServer();
    servers.push(site);
    const ws = new WebSocketServer({ server: site });
    site.listen(0, '127.0.0.1');
    await once(site, 'listening');
    ws.on('connection', (peer) => peer.once('message', () => peer.terminate()));
    const selected = model(`http://127.0.0.1:${(site.address() as { port: number }).port}`);
    const local = await serve((req, res) => {
      routeNativeVoice(req, res, () => selected);
    });
    const port = await startVoiceTransport(Number(new URL(local).port));
    const native = http2.connect(`https://localhost:${port}`, { rejectUnauthorized: false });
    try {
      const request = native.request({
        ':method': 'POST',
        ':path': VOICE_RPC_PATH,
        'content-type': 'application/grpc',
      });
      let status: string | undefined;
      request.on('trailers', (headers) => {
        status = String(headers['grpc-status']);
      });
      request.resume();
      request.end(frame(Buffer.from([10, 2, 10, 0])));
      await once(request, 'end');
      expect(status).toBe('14');
    } finally {
      native.destroy();
      for (const peer of ws.clients) peer.terminate();
      ws.close();
    }
  });

  it('rejects browser origins and foreign authorities at the voice TLS listener', async () => {
    const called = vi.fn();
    const local = await serve(called);
    const port = await startVoiceTransport(Number(new URL(local).port));
    const native = http2.connect(`https://localhost:${port}`, { rejectUnauthorized: false });
    try {
      for (const headers of [{ origin: 'https://untrusted.example' }, { ':authority': 'untrusted.example' }]) {
        const request = native.request({ ':method': 'POST', ':path': VOICE_RPC_PATH, ...headers });
        const response = once(request, 'response');
        request.resume();
        request.end();
        expect((await response)[0][':status']).toBe(403);
        await once(request, 'end');
      }
      expect(called).not.toHaveBeenCalled();
    } finally {
      native.destroy();
    }
  });
});
