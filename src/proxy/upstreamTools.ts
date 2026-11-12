/** TokenNest tool transport. The saved source supplies credentials, never the incoming Google identity. */
import * as http from 'node:http';
import * as https from 'node:https';
import * as http2 from 'node:http2';
import { gunzipSync } from 'node:zlib';
import type { CustomModel } from '../proxy';

const RPC_FIELDS: Record<string, number[]> = {
  GetWebSearchResults: [2, 3, 4, 6],
  GetWebSearchRedirect: [1],
  GetEmbeddings: [1, 4],
  GetImageGeneration: [2, 3, 4, 6, 9],
  GetTranscription: [2, 3],
  GetMQuery: [2, 3, 4],
  GetWebDocsOptions: [],
};
const MAX_BYTES = 16 * 1024 * 1024;

export function toolServiceBase(apiUrl: string): URL {
  const url = new URL(apiUrl);
  if (
    (url.protocol !== 'https:' &&
      !(url.protocol === 'http:' && ['localhost', '127.0.0.1', '[::1]'].includes(url.hostname))) ||
    url.username ||
    url.password ||
    url.search ||
    url.hash
  )
    throw new Error('Tool proxy requires an HTTPS site URL (HTTP is allowed only on loopback).');
  url.pathname =
    url.pathname
      .replace(
        /\/(?:v1(?:\/chat\/completions|\/responses|\/messages)?|v1beta(?:\/models\/[^/]+:(?:streamGenerateContent|generateContent))?)\/?$/,
        '',
      )
      .replace(/\/$/, '') + '/';
  return url;
}

export function toolProxySource(models: CustomModel[]): CustomModel | undefined {
  const sources = models.filter((model) => model.toolProxy === true);
  if (!sources.length) return undefined;
  if (sources.length !== 1 || sources[0].enabled === false)
    throw new Error('Enable exactly one saved model as the TokenNest tool proxy source.');
  const source = sources[0];
  toolServiceBase(source.apiUrl);
  if (!source.apiKey || source.apiKey === 'none' || /[\r\n]/.test(source.apiKey))
    throw new Error('The tool proxy source requires a TokenNest API key.');
  if (source.allowUnauthorized) throw new Error('Tool proxy requires certificate verification.');
  return source;
}

/** Remove legacy Metadata/API keys and unknown fields instead of forwarding them to a third party. */
export function publicRPCPayload(method: string, body: Buffer, framed = true): Buffer {
  if (!RPC_FIELDS[method] || body.length > MAX_BYTES) throw new Error('Unsupported or oversized tool RPC.');
  if (framed && (body.length < 5 || body[0] !== 0 || body.readUInt32BE(1) !== body.length - 5))
    throw new Error('Only one uncompressed unary tool frame is supported.');
  const bytes = framed ? body.subarray(5) : body;
  let at = 0;
  const read = () => {
    let value = 0;
    for (let shift = 0; shift < 56 && at < bytes.length; shift += 7) {
      const byte = bytes[at++];
      value += (byte & 127) * 2 ** shift;
      if (!(byte & 128)) {
        if (!Number.isSafeInteger(value)) throw new Error('Invalid protobuf integer.');
        return value;
      }
    }
    throw new Error('Invalid protobuf integer.');
  };
  const fields: Buffer[] = [];
  while (at < bytes.length) {
    const start = at;
    const tag = read(),
      field = Math.floor(tag / 8),
      wire = tag % 8;
    if (!field) throw new Error('Invalid protobuf field.');
    if (wire === 0) read();
    else if (wire === 1) at += 8;
    else if (wire === 5) at += 4;
    else if (wire === 2) {
      const length = read();
      at += length;
    } else throw new Error('Unsupported protobuf wire type.');
    if (at > bytes.length) throw new Error('Incomplete protobuf field.');
    if (RPC_FIELDS[method].includes(field)) fields.push(bytes.subarray(start, at));
  }
  const payload = Buffer.concat(fields);
  const header = Buffer.alloc(5);
  header.writeUInt32BE(payload.length, 1);
  return Buffer.concat([header, payload]);
}

/** Unary Connect JSON uses the same public schema as the binary bridge. */
export function publicRPCJSON(method: string, body: Buffer): Buffer {
  const fields: Record<string, string[]> = {
    GetWebSearchResults: ['query', 'limit', 'domain', 'imageSearch'],
    GetWebSearchRedirect: ['originalUrl'],
    GetEmbeddings: ['request'],
    GetImageGeneration: ['prompt', 'inputImages', 'inputMedia', 'aspectRatio'],
    GetTranscription: ['audioData', 'prompt'],
    GetMQuery: ['query', 'cciWithSubrangeBatches'],
    GetWebDocsOptions: [],
  };
  if (!fields[method] || body.length > MAX_BYTES) throw new Error('Unsupported or oversized tool RPC.');
  const object = JSON.parse(body.toString('utf8')) as Record<string, unknown>;
  if (!object || typeof object !== 'object' || Array.isArray(object)) throw new Error('Invalid Connect object.');
  const clean = (value: unknown, depth: number): unknown => {
    if (depth > 32) throw new Error('Tool request nesting exceeds limit.');
    if (Array.isArray(value)) return value.map((item) => clean(item, depth + 1));
    if (!value || typeof value !== 'object') return value;
    return Object.fromEntries(
      Object.entries(value)
        .filter(
          ([key]) =>
            !/^(?:metadata|api_?key|access_?token|refresh_?token|id_?token|authorization|model|embedding_?model)$/i.test(
              key,
            ),
        )
        .map(([key, child]) => [key, clean(child, depth + 1)]),
    );
  };
  const output: Record<string, unknown> = {};
  for (const key of fields[method]) {
    const snake = key.replace(/[A-Z]/g, (letter) => '_' + letter.toLowerCase());
    const value = object[key] ?? object[snake];
    if (value !== undefined) output[key] = clean(value, 0);
  }
  return Buffer.from(JSON.stringify(output));
}

function toolRequestBody(req: http.IncomingMessage, body: Buffer, framed: boolean): Buffer {
  const encoding = String(req.headers['content-encoding'] || 'identity').toLowerCase();
  if (body.length > MAX_BYTES) throw new Error('Oversized tool request.');
  if (encoding === 'gzip') body = gunzipSync(body, { maxOutputLength: MAX_BYTES });
  else if (encoding !== 'identity') throw new Error('Unsupported tool compression.');
  if (framed && body.length >= 5 && body[0] === 1 && body.readUInt32BE(1) === body.length - 5) {
    if (req.headers['grpc-encoding'] !== 'gzip') throw new Error('Unsupported gRPC compression.');
    const payload = gunzipSync(body.subarray(5), { maxOutputLength: MAX_BYTES - 5 });
    const header = Buffer.alloc(5);
    header.writeUInt32BE(payload.length, 1);
    body = Buffer.concat([header, payload]);
  }
  return body;
}

export interface GenerationTool {
  action: 'search' | 'image';
  input: Record<string, unknown>;
}
export function generationTool(body: Record<string, unknown>): GenerationTool | undefined {
  const tools = body.tools as Record<string, unknown>[] | undefined;
  const search =
    tools?.length === 1 &&
    tools[0] &&
    ('googleSearch' in tools[0] || 'google_search' in tools[0] || 'googleSearchRetrieval' in tools[0]);
  const config = body.generationConfig as
    | { responseModalities?: string[]; imageConfig?: { aspectRatio?: string } }
    | undefined;
  const image = config?.responseModalities?.includes('IMAGE');
  if (!search && !image) return undefined;
  const contents = body.contents as
    | {
        role?: string;
        parts?: {
          text?: string;
          inlineData?: { mimeType: string; data: string };
          fileData?: { mimeType?: string; fileUri?: string };
        }[];
      }[]
    | undefined;
  // Only isolated built-in tool jobs: normal conversations and user function calls retain their model route.
  if (contents?.length !== 1 || contents[0].role === 'model' || (image && tools?.length)) return undefined;
  const parts = contents[0].parts || [];
  if (parts.some((part) => !part.text && !part.inlineData && !part.fileData))
    throw new Error('Tool jobs require text and inline image data.');
  const text = parts
    .map((part) => part.text || '')
    .join('\n')
    .trim();
  if (search) {
    if (parts.some((part) => part.inlineData || part.fileData)) throw new Error('Search requires text only.');
    const settings = (tools![0].googleSearch ||
      tools![0].google_search ||
      tools![0].googleSearchRetrieval ||
      {}) as Record<string, unknown>;
    const enhanced = settings.enhancedContent as { imageSearch?: { maxResultCount?: number } } | undefined;
    return {
      action: 'search',
      input: {
        query: text,
        includedDomains: settings.includedDomains,
        excludedDomains: settings.excludedDomains,
        imageSearch: enhanced?.imageSearch !== undefined,
        imageSearchMaxResults: enhanced?.imageSearch?.maxResultCount,
      },
    };
  }
  return {
    action: 'image',
    input: {
      prompt: text,
      aspectRatio: config?.imageConfig?.aspectRatio,
      inputMedia: parts
        .filter((part) => part.inlineData || part.fileData)
        .map((part) =>
          part.inlineData
            ? { mimeType: part.inlineData.mimeType, inlineData: part.inlineData.data }
            : { mimeType: part.fileData?.mimeType, uri: part.fileData?.fileUri },
        ),
    },
  };
}

export function generationToolResult(action: string, output: Record<string, unknown>): Record<string, unknown> {
  const parts: unknown[] = [];
  if (action === 'search') parts.push({ text: output.answer || '' });
  else {
    if (output.text) parts.push({ text: output.text });
    // Accept both the managed JSON format and older sites' native RPC JSON.
    let images = output.images as { mimeType: string; inlineData: string }[] | undefined;
    if (!images?.length) {
      const media = output.generatedMedia as { mimeType?: string; inlineData?: string } | undefined;
      const image = output.image as { mimeType?: string; base64Data?: string } | undefined;
      if (media?.inlineData) images = [{ mimeType: media.mimeType || '', inlineData: media.inlineData }];
      else if (image?.base64Data) images = [{ mimeType: image.mimeType || '', inlineData: image.base64Data }];
    }
    if (!images?.length) throw new Error('Image tool returned no usable inline image.');
    for (const image of images) {
      const bytes = Buffer.from(image.inlineData || '', 'base64');
      if (
        !image.mimeType?.startsWith('image/') ||
        !bytes.length ||
        bytes.toString('base64').replace(/=+$/, '') !== image.inlineData.replace(/=+$/, '')
      )
        throw new Error('Image tool returned invalid inline image data.');
      parts.push({ inlineData: { mimeType: image.mimeType, data: image.inlineData } });
    }
  }
  const candidate: Record<string, unknown> = { index: 0, finishReason: 'STOP', content: { role: 'model', parts } };
  if (action === 'search')
    candidate.groundingMetadata = output.groundingMetadata || {
      webSearchQueries: output.queries || [],
      groundingChunks: ((output.results || []) as { url: string; title: string }[]).map((row) => ({
        web: { uri: row.url, title: row.title },
      })),
    };
  return { candidates: [candidate] };
}

export async function callToolProxy(
  source: CustomModel,
  pathname: string,
  body: Buffer,
  contentType: string,
  signal: AbortSignal,
  method: 'POST' | 'GET' = 'POST',
): Promise<{ status: number; body: Buffer; grpcStatus: string }> {
  const url = toolServiceBase(source.apiUrl);
  // v1internal:verb is a path, not a URL scheme.
  const [path, search] = pathname.split('?');
  url.pathname += path.replace(/^\//, '');
  url.search = search || '';
  const headers: http.OutgoingHttpHeaders = {
    'content-type': contentType,
    'content-length': body.length,
    'accept-encoding': 'identity',
    'user-agent': 'antigravity/model-patch-tools',
  };
  for (const [name, value] of Object.entries(source.customHeaders || {})) {
    if (
      !name.startsWith(':') &&
      !/^(?:host|connection|proxy-connection|keep-alive|upgrade|http2-settings|content-length|content-type|transfer-encoding|te|cookie|authorization|x-tokennest-model|x-goog-.+)$/i.test(
        name,
      )
    )
      headers[name] = value;
  }
  // Scope tools to the selected site's model connection, never custom headers
  // or metadata from the desktop's Google account.
  const callerModel = (source.externalModelName || source.name).trim();
  if (!callerModel || Buffer.byteLength(callerModel) > 256) throw new Error('Invalid tool caller model');
  http.validateHeaderValue('x-tokennest-model', callerModel);
  headers['x-tokennest-model'] = callerModel;
  headers.authorization = `Bearer ${source.apiKey}`;
  if (contentType.startsWith('application/grpc')) {
    headers.te = 'trailers';
    // Native gRPC needs HTTP/2 through the CDN, including its trailing status.
    // Do not replay a failed stream over HTTP/1: a tool may already be billed.
    return new Promise((resolve, reject) => {
      const session = http2.connect(url.origin);
      let settled = false;
      const finish = (error?: Error) => {
        if (settled) return false;
        settled = true;
        clearTimeout(timeout);
        session.destroy();
        if (error) reject(error);
        return true;
      };
      const timeout = setTimeout(
        () => finish(new Error('Tool proxy timed out.')),
        Math.min(source.timeout || 180000, 3600000),
      );
      session.on('error', () => finish(new Error('Tool proxy connection failed or was cancelled.')));
      session.on('close', () => finish(new Error('Tool proxy connection closed before the response completed.')));
      let request: http2.ClientHttp2Stream;
      try {
        request = session.request({ ...headers, ':method': method, ':path': url.pathname + url.search }, { signal });
      } catch {
        finish(new Error('Tool proxy request could not be created.'));
        return;
      }
      const chunks: Buffer[] = [];
      let length = 0;
      let status = 502;
      let grpcStatus = '';
      request.on('response', (response) => {
        status = Number(response[':status']) || 502;
        grpcStatus = String(response['grpc-status'] || response['x-tokennest-grpc-status'] || '');
      });
      request.on('trailers', (trailers) => {
        if (trailers['grpc-status'] !== undefined) grpcStatus = String(trailers['grpc-status']);
      });
      request.on('data', (chunk: Buffer) => {
        length += chunk.length;
        if (length > MAX_BYTES) finish(new Error('Tool response exceeds 16 MiB.'));
        else chunks.push(chunk);
      });
      request.on('error', () => finish(new Error('Tool response failed or was cancelled.')));
      request.on('aborted', () => finish(new Error('Tool response was interrupted.')));
      request.on('close', () => {
        if (!request.readableEnded || request.rstCode !== http2.constants.NGHTTP2_NO_ERROR)
          finish(new Error('Tool response was interrupted.'));
      });
      request.on('end', () => {
        if (request.rstCode !== http2.constants.NGHTTP2_NO_ERROR) {
          finish(new Error('Tool response was interrupted.'));
          return;
        }
        if (finish()) resolve({ status, body: Buffer.concat(chunks), grpcStatus });
      });
      request.end(body);
    });
  }
  return new Promise((resolve, reject) => {
    const request = (url.protocol === 'https:' ? https : http).request(url, { method, headers, signal }, (response) => {
      const chunks: Buffer[] = [];
      let length = 0;
      response.on('data', (chunk: Buffer) => {
        length += chunk.length;
        if (length > MAX_BYTES) request.destroy(new Error('Tool response exceeds 16 MiB.'));
        else chunks.push(chunk);
      });
      response.on('error', () => reject(new Error('Tool response was interrupted.')));
      response.on('aborted', () => reject(new Error('Tool response was interrupted.')));
      response.on('end', () =>
        resolve({
          status: response.statusCode || 502,
          body: Buffer.concat(chunks),
          grpcStatus:
            response.trailers['grpc-status'] ||
            String(response.headers['grpc-status'] || response.headers['x-tokennest-grpc-status'] || ''),
        }),
      );
    });
    const timeout = setTimeout(
      () => request.destroy(new Error('Tool proxy timed out.')),
      Math.min(source.timeout || 180000, 3600000),
    );
    request.on('close', () => clearTimeout(timeout));
    request.on('error', () => reject(new Error('Tool proxy connection failed or was cancelled.')));
    request.end(body);
  });
}

export function sendToolRPC(res: http.ServerResponse, type: string, frame: Buffer, status: string): void {
  if (type === 'application/proto' || type === 'application/json') {
    if (status === '0') {
      res.writeHead(200, { 'Content-Type': type, 'Cache-Control': 'no-store' });
      res.end(type === 'application/proto' ? frame.subarray(5) : frame);
    } else {
      res.writeHead(
        ({ '3': 400, '16': 401, '7': 403, '8': 429, '12': 501, '14': 503, '4': 504 } as Record<string, number>)[
          status
        ] || 500,
        { 'Content-Type': 'application/json', 'Cache-Control': 'no-store' },
      );
      res.end(
        JSON.stringify({
          code:
            (
              {
                '3': 'invalid_argument',
                '16': 'unauthenticated',
                '7': 'permission_denied',
                '8': 'resource_exhausted',
                '12': 'unimplemented',
                '14': 'unavailable',
                '4': 'deadline_exceeded',
              } as Record<string, string>
            )[status] || 'internal',
          message: 'Tool request was rejected or is unavailable.',
        }),
      );
    }
    return;
  }
  res.writeHead(200, {
    'Content-Type': type.startsWith('application/grpc-web') ? 'application/grpc-web+proto' : 'application/grpc+proto',
    'Cache-Control': 'no-store',
    ...(type.startsWith('application/grpc-web') ? {} : { Trailer: 'Grpc-Status, Grpc-Message' }),
  });
  if (status === '0') res.write(frame);
  if (type.startsWith('application/grpc-web')) {
    const trailer = Buffer.from(`grpc-status: ${status}\r\n`),
      header = Buffer.alloc(5);
    header[0] = 128;
    header.writeUInt32BE(trailer.length, 1);
    res.write(Buffer.concat([header, trailer]));
  } else
    res.addTrailers({
      'grpc-status': status,
      ...(status !== '0' ? { 'grpc-message': 'Tool request was rejected or is unavailable.' } : {}),
    });
  res.end();
}

/** Non-tool RPCs retain their native Google route and Google credentials. */
export function forwardGoogleRPC(req: http.IncomingMessage, res: http.ServerResponse, body: Buffer): void {
  const session = http2.connect('https://aicode.googleapis.com');
  const headers: http2.OutgoingHttpHeaders = {
    ':method': 'POST',
    ':path': req.url!,
    'content-type': req.headers['content-type'] || 'application/grpc+proto',
  };
  for (const name of ['authorization', 'user-agent', 'grpc-timeout', 'x-goog-api-client', 'x-goog-user-project'])
    if (req.headers[name]) headers[name] = req.headers[name];
  const outbound = session.request(headers);
  const fail = () => {
    if (!res.writableEnded && !res.destroyed) {
      if (!res.headersSent) res.writeHead(502);
      res.end();
    }
    session.destroy();
  };
  session.on('error', fail);
  outbound.on('error', fail);
  outbound.on('response', (response) => {
    res.writeHead(Number(response[':status']) || 502, {
      'Content-Type': String(response['content-type'] || 'application/grpc+proto'),
      Trailer: 'Grpc-Status, Grpc-Message',
      ...(response['grpc-status'] !== undefined ? { 'grpc-status': String(response['grpc-status']) } : {}),
      ...(response['grpc-message'] !== undefined ? { 'grpc-message': String(response['grpc-message']) } : {}),
      ...(response['grpc-encoding'] !== undefined ? { 'grpc-encoding': String(response['grpc-encoding']) } : {}),
    });
  });
  outbound.on('trailers', (trailers) => {
    res.addTrailers({
      'grpc-status': String(trailers['grpc-status'] || '13'),
      'grpc-message': String(trailers['grpc-message'] || ''),
    });
  });
  outbound.on('end', () => {
    res.end();
    session.close();
  });
  outbound.setTimeout(180000, fail);
  res.on('close', () => session.destroy());
  outbound.pipe(res, { end: false });
  outbound.end(body);
}

export function isNativeTool(method: string): boolean {
  return Object.prototype.hasOwnProperty.call(RPC_FIELDS, method);
}

interface ManagedToolRequest {
  path: string;
  method: string;
  type: string;
  input?: Record<string, unknown>;
  tool?: GenerationTool;
}

function rejectToolRequest(res: http.ServerResponse, call: ManagedToolRequest): void {
  if (res.destroyed || res.writableEnded) return;
  if (call.method) sendToolRPC(res, call.type, Buffer.alloc(0), '13');
  else {
    res.writeHead(502, { 'Content-Type': 'application/json', 'Cache-Control': 'no-store' });
    res.end(
      JSON.stringify({
        error: {
          message:
            'Tool proxy request failed. Check the saved TokenNest tool source and site tool channel configuration.',
        },
      }),
    );
  }
}

async function forwardManagedTool(
  req: http.IncomingMessage,
  res: http.ServerResponse,
  body: Buffer,
  source: CustomModel,
  call: ManagedToolRequest,
): Promise<void> {
  const controller = new AbortController();
  const cancel = () => controller.abort();
  res.once('close', cancel);
  try {
    if (call.method) {
      if (
        ![
          'application/grpc',
          'application/grpc+proto',
          'application/grpc-web+proto',
          'application/proto',
          'application/json',
        ].includes(call.type)
      ) {
        throw new Error('Unsupported unary tool content type.');
      }
      body = toolRequestBody(req, body, call.type !== 'application/proto' && call.type !== 'application/json');
      if (call.type === 'application/json') {
        const result = await callToolProxy(
          source,
          call.path,
          publicRPCJSON(call.method, body),
          call.type,
          controller.signal,
        );
        if (!res.destroyed && !res.writableEnded) {
          res.writeHead(result.status, { 'Content-Type': 'application/json', 'Cache-Control': 'no-store' });
          res.end(result.body);
        }
        return;
      }
      const publicBody = publicRPCPayload(call.method, body, call.type !== 'application/proto');
      const result = await callToolProxy(source, call.path, publicBody, 'application/grpc+proto', controller.signal);
      const status =
        result.grpcStatus ||
        ({ 400: '3', 401: '16', 403: '7', 429: '8', 501: '12', 503: '14', 504: '4' } as Record<number, string>)[
          result.status
        ] ||
        '13';
      if (
        status === '0' &&
        (result.body.length < 5 || result.body[0] !== 0 || result.body.readUInt32BE(1) !== result.body.length - 5)
      )
        throw new Error('Invalid tool response frame.');
      if (!res.destroyed && !res.writableEnded) sendToolRPC(res, call.type, result.body, status);
      return;
    }
    const publicInput =
      call.tool?.input ||
      (call.path === '/v1internal:rewriteUri'
        ? { originalUri: call.input!.originalUri }
        : call.path === '/v1internal:listWebDocsOptions' || call.path === '/v1internal/webDocsOptions'
          ? { pageSize: call.input!.pageSize, pageToken: call.input!.pageToken }
          : {
              url: call.input!.url,
              ...(call.path === '/v1internal:fetchFromTrawlerCache' ? { liveFetch: call.input!.liveFetch } : {}),
            });
    let payload = publicInput;
    let path = call.path;
    if (call.tool) {
      // Enter the site's gateway handler so model scopes, the selected tool
      // channel and its billing apply just as they do for a native agy gateway.
      const parts: Record<string, unknown>[] = [{ text: publicInput.query || publicInput.prompt }];
      for (const image of (publicInput.inputMedia || []) as { mimeType: string; inlineData?: string; uri?: string }[])
        parts.push(
          image.uri
            ? { fileData: { mimeType: image.mimeType, fileUri: image.uri } }
            : { inlineData: { mimeType: image.mimeType, data: image.inlineData } },
        );
      payload = { contents: [{ role: 'user', parts }] };
      if (call.tool.action === 'search')
        payload.tools = [
          {
            googleSearch: {
              ...(publicInput.includedDomains ? { includedDomains: publicInput.includedDomains } : {}),
              ...(publicInput.excludedDomains ? { excludedDomains: publicInput.excludedDomains } : {}),
              ...(publicInput.imageSearch
                ? {
                    enhancedContent: {
                      imageSearch: {
                        ...(publicInput.imageSearchMaxResults
                          ? { maxResultCount: publicInput.imageSearchMaxResults }
                          : {}),
                      },
                    },
                  }
                : {}),
            },
          },
        ];
      else
        payload.generationConfig = {
          responseModalities: ['TEXT', 'IMAGE'],
          ...(publicInput.aspectRatio ? { imageConfig: { aspectRatio: publicInput.aspectRatio } } : {}),
        };
      const model = source.externalModelName.replace(/^models\//, '');
      if (!model || /[\r\n]/.test(model)) throw new Error('Tool source requires a configured site model.');
      path = `/v1beta/models/${encodeURIComponent(model)}:generateContent`;
    }
    const docs = call.path === '/v1internal/webDocsOptions';
    if (docs) {
      const params = new URLSearchParams();
      if (publicInput.pageSize !== undefined) params.set('pageSize', String(publicInput.pageSize));
      if (publicInput.pageToken !== undefined) params.set('pageToken', String(publicInput.pageToken));
      path += '?' + params.toString();
    }
    const result = await callToolProxy(
      source,
      path,
      docs ? Buffer.alloc(0) : Buffer.from(JSON.stringify(payload)),
      'application/json',
      controller.signal,
      docs ? 'GET' : 'POST',
    );
    if (res.destroyed || res.writableEnded) return;
    res.setHeader('Cache-Control', 'no-store');
    if (result.status !== 200 || !call.tool) {
      res.writeHead(result.status, { 'Content-Type': 'application/json' });
      res.end(result.body);
      return;
    }
    const returned = JSON.parse(result.body.toString('utf8')) as Record<string, unknown>;
    const response = Array.isArray(returned.candidates) ? returned : generationToolResult(call.tool.action, returned);
    const output = call.input!.request ? { response } : response;
    if (call.path.endsWith(':streamGenerateContent')) {
      res.writeHead(200, { 'Content-Type': 'text/event-stream' });
      res.end(`data: ${JSON.stringify(output)}\n\n`);
    } else {
      res.writeHead(200, { 'Content-Type': 'application/json' });
      res.end(JSON.stringify(output));
    }
  } finally {
    res.off('close', cancel);
  }
}

/** Decide synchronously; ordinary requests retain the original proxy lifecycle. Tool failures never replay. */
export function routeUpstreamTools(
  req: http.IncomingMessage,
  res: http.ServerResponse,
  body: Buffer,
  getSource: () => CustomModel | undefined,
): boolean {
  const path = new URL(req.url!, 'http://localhost').pathname;
  const method = path.startsWith('/exa.api_server_pb.ApiServerService/')
    ? path.slice('/exa.api_server_pb.ApiServerService/'.length)
    : '';
  const rest = [
    '/v1internal:rewriteUri',
    '/v1internal:fetchFromTrawlerCache',
    '/v1internal:checkUrlDenylist',
    '/v1internal:checkUrlAntivirus',
    '/v1internal:listWebDocsOptions',
    '/v1internal/webDocsOptions',
  ].includes(path);
  const generate = /(?:\/v1internal:|\/v1(?:beta)?\/models\/[^/]+:)(?:streamGenerateContent|generateContent)$/.test(
    path,
  );
  if (
    (req.method !== 'POST' && !(req.method === 'GET' && path === '/v1internal/webDocsOptions')) ||
    (!method && !rest && !generate)
  )
    return false;
  const call: ManagedToolRequest = { path, method, type: String(req.headers['content-type'] || '').split(';')[0] };
  try {
    if (method && !isNativeTool(method)) {
      forwardGoogleRPC(req, res, body);
      return true;
    }
    if (req.method === 'GET' && path === '/v1internal/webDocsOptions') {
      const params = new URL(req.url!, 'http://localhost').searchParams;
      call.input = {};
      if (params.has('pageSize')) call.input.pageSize = Number(params.get('pageSize'));
      if (params.has('pageToken')) call.input.pageToken = params.get('pageToken');
    } else
      call.input = method
        ? undefined
        : (JSON.parse(toolRequestBody(req, body, false).toString('utf8')) as Record<string, unknown>);
    if (generate) {
      try {
        call.tool = generationTool((call.input!.request || call.input!) as Record<string, unknown>);
      } catch (error) {
        // Unsupported tool media must keep the native behavior when the feature is not configured.
        if (!getSource()) return false;
        throw error;
      }
      if (!call.tool) return false;
    }
    const source = getSource();
    if (!source) {
      if (method) {
        forwardGoogleRPC(req, res, body);
        return true;
      }
      return false;
    }
    void forwardManagedTool(req, res, body, source, call).catch(() => rejectToolRequest(res, call));
  } catch {
    rejectToolRequest(res, call);
  }
  return true;
}

/** Decode only public protobuf fields needed by the streaming ASR adapter. */
function bidiFields(bytes: Buffer): Map<number, Buffer[]> {
  let at = 0;
  const fields = new Map<number, Buffer[]>();
  const read = () => {
    let value = 0;
    for (let shift = 0; shift < 56 && at < bytes.length; shift += 7) {
      const byte = bytes[at++];
      value += (byte & 127) * 2 ** shift;
      if (!(byte & 128)) {
        if (!Number.isSafeInteger(value)) throw new Error('Invalid protobuf integer.');
        return value;
      }
    }
    throw new Error('Incomplete protobuf integer.');
  };
  while (at < bytes.length) {
    const tag = read(),
      field = Math.floor(tag / 8),
      wire = tag % 8;
    if (!field) throw new Error('Invalid protobuf tag.');
    if (wire === 0) read();
    else if (wire === 1) at += 8;
    else if (wire === 5) at += 4;
    else if (wire === 2) {
      const length = read(),
        start = at;
      at += length;
      if (at > bytes.length) throw new Error('Incomplete protobuf field.');
      const values = fields.get(field) || [];
      values.push(bytes.subarray(start, at));
      fields.set(field, values);
    } else throw new Error('Unsupported protobuf field.');
    if (at > bytes.length) throw new Error('Incomplete protobuf field.');
  }
  return fields;
}

function bidiBytes(field: number, data: Buffer): Buffer {
  const length: number[] = [];
  for (let n = data.length; ; n = Math.floor(n / 128)) {
    length.push((n % 128) | (n >= 128 ? 128 : 0));
    if (n < 128) break;
  }
  return Buffer.concat([Buffer.from([field * 8 + 2, ...length]), data]);
}

/** The native ASR handshake must be answered before the recording is complete.
 * Each completed turn becomes one authenticated, billable site tool request.
 * Model, project and personal Google credentials from the stream are discarded.
 */
export function routeTranscriptionStream(
  req: http.IncomingMessage,
  res: http.ServerResponse,
  getSource: () => CustomModel | undefined,
): boolean {
  if (
    req.method !== 'POST' ||
    new URL(req.url!, 'http://localhost').pathname !==
      '/google.gca.aicode.v1alpha.PredictionService/BidiGenerateContent'
  )
    return false;
  let source: CustomModel | undefined;
  try {
    source = getSource();
  } catch {
    sendToolRPC(res, 'application/grpc', Buffer.alloc(0), '3');
    return true;
  }
  if (!source) return false;
  if (!String(req.headers['content-type'] || '').startsWith('application/grpc') || req.headers['content-encoding']) {
    sendToolRPC(res, 'application/grpc', Buffer.alloc(0), '3');
    return true;
  }
  const abort = new AbortController();
  let pending = Buffer.alloc(0),
    bytes = 0,
    audioBytes = 0,
    rate = 16000;
  let audio: Buffer[] = [],
    setup = false,
    finished = false;
  let turns = Promise.resolve();
  const finish = (status: string) => {
    if (finished) return;
    finished = true;
    clearTimeout(timeout);
    abort.abort();
    if (!res.headersSent)
      res.writeHead(200, { 'Content-Type': 'application/grpc', Trailer: 'Grpc-Status', 'Cache-Control': 'no-store' });
    res.addTrailers({ 'grpc-status': status });
    res.end();
  };
  const timeout = setTimeout(() => finish('4'), Math.min(source.timeout || 180000, 3600000));
  const write = (payload: Buffer) => {
    if (finished) return;
    if (!res.headersSent)
      res.writeHead(200, { 'Content-Type': 'application/grpc', Trailer: 'Grpc-Status', 'Cache-Control': 'no-store' });
    const header = Buffer.alloc(5);
    header.writeUInt32BE(payload.length, 1);
    res.write(Buffer.concat([header, payload]));
  };
  const endTurn = () => {
    const recording = Buffer.concat(audio),
      sampleRate = rate;
    audio = [];
    audioBytes = 0;
    turns = turns
      .then(async () => {
        if (finished) return;
        let transcript = '';
        if (recording.length) {
          const header = Buffer.alloc(44);
          header.write('RIFF');
          header.writeUInt32LE(recording.length + 36, 4);
          header.write('WAVEfmt ', 8);
          header.writeUInt32LE(16, 16);
          header.writeUInt16LE(1, 20);
          header.writeUInt16LE(1, 22);
          header.writeUInt32LE(sampleRate, 24);
          header.writeUInt32LE(sampleRate * 2, 28);
          header.writeUInt16LE(2, 32);
          header.writeUInt16LE(16, 34);
          header.write('data', 36);
          header.writeUInt32LE(recording.length, 40);
          const result = await callToolProxy(
            source!,
            '/v1/tools/antigravity/transcription',
            Buffer.from(
              JSON.stringify({
                audioData: Buffer.concat([header, recording]).toString('base64'),
                audioMimeType: 'audio/wav',
              }),
            ),
            'application/json',
            abort.signal,
          );
          if (result.status !== 200) {
            finish(
              ({ 400: '3', 401: '16', 403: '7', 429: '8', 501: '12', 503: '14', 504: '4' } as Record<number, string>)[
                result.status
              ] || '13',
            );
            return;
          }
          const resultBody = JSON.parse(result.body.toString('utf8')) as { transcribedText?: unknown };
          if (typeof resultBody.transcribedText !== 'string') throw new Error('Invalid transcript.');
          transcript = resultBody.transcribedText;
        }
        const content = Buffer.concat([bidiBytes(6, bidiBytes(1, Buffer.from(transcript))), Buffer.from([24, 1])]);
        write(bidiBytes(2, content));
      })
      .catch(() => finish('13'));
  };
  req.on('data', (chunk: Buffer) => {
    if (finished) return;
    try {
      bytes += chunk.length;
      if (bytes > MAX_BYTES) throw new Error('Oversized transcription stream.');
      pending = Buffer.concat([pending, chunk]);
      while (pending.length >= 5) {
        const length = pending.readUInt32BE(1),
          flag = pending[0];
        if (length > MAX_BYTES) throw new Error('Oversized transcription frame.');
        if (pending.length < length + 5) break;
        let payload = pending.subarray(5, length + 5);
        pending = pending.subarray(length + 5);
        if (flag === 1 && req.headers['grpc-encoding'] === 'gzip')
          payload = gunzipSync(payload, { maxOutputLength: MAX_BYTES });
        else if (flag !== 0) throw new Error('Unsupported transcription compression.');
        const fields = bidiFields(payload);
        if (fields.has(1)) {
          if (setup) throw new Error('Duplicate transcription setup.');
          setup = true;
          write(bidiBytes(1, Buffer.alloc(0)));
        }
        for (const input of fields.get(3) || []) {
          if (!setup) throw new Error('Transcription setup required.');
          const realtime = bidiFields(input);
          if (realtime.has(3)) throw new Error('Video is not an audio transcription tool.');
          for (const blob of realtime.get(1) || []) {
            const media = bidiFields(blob),
              mime = media.get(1)?.[0]?.toString('utf8') || '';
            const matched = /^audio\/pcm;rate=(\d+)$/.exec(mime);
            if (!matched || Number(matched[1]) < 8000 || Number(matched[1]) > 48000)
              throw new Error('Unsupported PCM format.');
            if (audioBytes && rate !== Number(matched[1])) throw new Error('Audio sample rate changed.');
            rate = Number(matched[1]);
            for (const data of media.get(2) || []) {
              audioBytes += data.length;
              if (audioBytes > 9 * 1024 * 1024 - 44 || data.length % 2)
                throw new Error('Oversized or incomplete PCM audio.');
              audio.push(data);
            }
          }
          if (realtime.has(6)) endTurn();
        }
      }
    } catch {
      finish('3');
    }
  });
  req.on('end', () => {
    if (finished) return;
    if (pending.length || !setup) {
      finish('3');
      return;
    }
    if (audioBytes) endTurn();
    void turns.then(() => finish('0'));
  });
  req.on('aborted', () => finish('1'));
  req.on('error', () => finish('13'));
  res.on('close', () => {
    clearTimeout(timeout);
    finished = true;
    abort.abort();
  });
  return true;
}
