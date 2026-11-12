/** The LS voice client always uses TLS. Its vendor localhost policy accepts this ephemeral certificate. */
import * as http from 'node:http';
import * as http2 from 'node:http2';
import { generateLoopbackCertificate } from './voiceWebSocket';
import { VOICE_RPC_PATH } from './nativeVoice';

let listener: http2.Http2SecureServer | undefined;
let starting: Promise<number> | undefined;
let port = 0;
const sessions = new Set<http2.ServerHttp2Session>();
const upstreams = new Set<http2.ClientHttp2Session>();

export async function startVoiceTransport(targetPort: number): Promise<number> {
  if (starting) return starting;
  if (listener && port) return port;
  starting = (async () => {
    // Keep the key only in memory. No global trust store or TLS policy changes.
    const certificate = await generateLoopbackCertificate([{ name: 'commonName', value: 'localhost' }], {
      keyType: 'ec',
      curve: 'P-256',
      algorithm: 'sha256',
      notAfterDate: new Date(Date.now() + 24 * 60 * 60 * 1000),
      extensions: [{ name: 'subjectAltName', altNames: [{ type: 2, value: 'localhost' }] }],
    });
    listener = http2.createSecureServer(
      { key: certificate.private, cert: certificate.cert, minVersion: 'TLSv1.2' },
      (req, res) => {
        if (
          req.headers[':authority'] !== `localhost:${port}` ||
          req.headers.origin !== undefined ||
          !req.url?.startsWith('/') ||
          /^[\\/]{2}/.test(req.url)
        ) {
          res.writeHead(403);
          res.end();
          return;
        }
        if (req.url === VOICE_RPC_PATH) {
          const headers: http.OutgoingHttpHeaders = { host: `127.0.0.1:${targetPort}` };
          for (const [name, value] of Object.entries(req.headers))
            if (!name.startsWith(':') && !['host', 'connection', 'transfer-encoding'].includes(name))
              headers[name] = value;
          const outbound = http.request(
            { hostname: '127.0.0.1', port: targetPort, path: VOICE_RPC_PATH, method: req.method, headers },
            (upstream) => {
              res.writeHead(upstream.statusCode || 502, { 'content-type': 'application/grpc+proto' });
              upstream.on('error', () => res.destroy());
              upstream.on('aborted', () => res.destroy());
              upstream.on('end', () => {
                res.addTrailers(upstream.trailers);
                res.end();
              });
              upstream.pipe(res, { end: false });
            },
          );
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
        const headers: http2.OutgoingHttpHeaders = {
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
      },
    );
    listener.on('session', (session) => {
      sessions.add(session);
      session.on('error', () => session.destroy());
      session.on('close', () => sessions.delete(session));
    });
    listener.on('stream', (stream) => stream.on('error', () => {}));
    return await new Promise<number>((resolve, reject) => {
      listener!.once('error', reject);
      listener!.listen(0, '127.0.0.1', () => {
        port = (listener!.address() as { port: number }).port;
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

export function stopVoiceTransport(): Promise<void> | undefined {
  if (!starting && !listener) return undefined;
  return (async () => {
    if (starting) await starting.catch(() => {});
    const closing = listener;
    listener = undefined;
    port = 0;
    for (const session of sessions) session.destroy();
    for (const session of upstreams) session.destroy();
    if (closing) await new Promise<void>((resolve) => closing.close(() => resolve()));
  })();
}
