# SPDX-License-Identifier: Apache-2.0
"""Per-invocation native agy gateway compatibility adapter (Linux, stdlib).

Only known Gemini effort aliases are normalized. No ACP interception and no retry.
The parent execs agy without a supervisor, retaining terminal/signal semantics.
The loopback child dies when that native process exits (PR_SET_PDEATHSIG).
"""

import contextlib
import ctypes
import hmac
import http.client
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
import json
import os
import secrets
import select
import signal
import ssl
import threading
import urllib.error
import urllib.parse
import urllib.request

MODELS = ("gemini-4-argon", "gemini-3.8-flash", "gemini-4-carbon")
EFFORTS = ("medium", "low", "high")  # Native fallback chooses the first variant.
CATALOG = ",".join(f"{model}-{effort}" for model in MODELS for effort in EFFORTS)
ALIASES = {
    f"{model}-{effort}": (model, effort) for model in MODELS for effort in EFFORTS
}
HOP_HEADERS = {
    "connection",
    "keep-alive",
    "proxy-authenticate",
    "proxy-authorization",
    "te",
    "trailer",
    "transfer-encoding",
    "upgrade",
    "host",
}
MAX_BODY = 64 * 1024 * 1024


def normalize(body):
    """Return a copy; preserve tools, history, streaming and all unknown fields."""
    if not isinstance(body, dict):
        raise ValueError("Expected a JSON object")
    result = dict(body)
    model = result.get("model")
    if isinstance(model, str) and model in ALIASES:
        base, effort = ALIASES[model]
        if result.get("reasoning_effort") not in (None, effort):
            raise ValueError("Model variant and reasoning_effort disagree")
        result["model"] = base
        result["reasoning_effort"] = effort
    return result


class NoRedirect(urllib.request.HTTPRedirectHandler):
    def redirect_request(self, req, fp, code, msg, headers, newurl):
        return None


class GatewayServer(ThreadingHTTPServer):
    daemon_threads = True
    allow_reuse_address = False

    def __init__(self, upstream, api_key, token, proxy_url=None, ca_cert=None):
        parsed = urllib.parse.urlsplit(upstream)
        if (
            parsed.scheme not in ("http", "https")
            or not parsed.netloc
            or parsed.query
            or parsed.fragment
            or parsed.username
        ):
            raise ValueError("Invalid fixed gateway URL")
        self.upstream = upstream.rstrip("/")
        self.api_key = api_key
        self.token = token
        self.proxy_url, self.ca_cert = proxy_url, ca_cert
        self.slots = threading.BoundedSemaphore(32)
        super().__init__(("127.0.0.1", 0), GatewayHandler)

    def process_request(self, request, address):
        # Bound thread count; an excess local caller gets a closed connection.
        if not self.slots.acquire(blocking=False):
            self.shutdown_request(request)
            return
        try:
            super().process_request(request, address)
        except Exception:
            self.slots.release()
            raise

    def process_request_thread(self, request, address):
        try:
            super().process_request_thread(request, address)
        finally:
            self.slots.release()


class GatewayHandler(BaseHTTPRequestHandler):
    protocol_version = "HTTP/1.1"

    def setup(self):
        super().setup()
        self.connection.settimeout(15)

    def log_message(self, *_):
        pass  # Never log credentials, prompts or request bodies.

    def fail(self, status, text):
        data = json.dumps(
            {"error": {"type": "agy_gateway_adapter", "message": text}}
        ).encode()
        self.send_response(status)
        self.send_header("Content-Type", "application/json")
        self.send_header("Content-Length", str(len(data)))
        self.send_header("Connection", "close")
        self.end_headers()
        self.wfile.write(data)
        self.close_connection = True

    def relay(self):
        if not hmac.compare_digest(
            self.headers.get("Authorization", ""), "Bearer " + self.server.token
        ):
            self.fail(401, "Invalid local gateway credential")
            return
        path = urllib.parse.urlsplit(self.path)
        if (
            path.scheme
            or path.netloc
            or not path.path.startswith("/")
            or path.path.startswith("//")
        ):
            self.fail(400, "Expected a relative gateway request path")
            return
        self.connection.settimeout(600)
        if self.headers.get("Transfer-Encoding"):
            self.fail(400, "Chunked request bodies are unsupported")
            return
        try:
            length = int(self.headers.get("Content-Length", "0"))
        except ValueError:
            self.fail(400, "Invalid Content-Length")
            return
        if not 0 <= length <= MAX_BODY:
            self.fail(413, "Request body exceeds 64 MiB")
            return
        payload = self.rfile.read(length) if length else None
        if payload is not None and len(payload) != length:
            self.fail(400, "Incomplete request body")
            return
        if self.command == "POST" and path.path.rstrip("/") == "/v1/chat/completions":
            if self.headers.get("Content-Encoding", "identity") != "identity":
                self.fail(415, "Expected uncompressed JSON for Chat Completions")
                return
            try:
                payload = json.dumps(
                    normalize(json.loads(payload or b"{}")), ensure_ascii=False
                ).encode()
            except (ValueError, UnicodeError) as exc:
                self.fail(400, str(exc))
                return
        excluded = HOP_HEADERS | {"content-length", "authorization"}
        excluded |= {
            v.strip().lower() for v in self.headers.get("Connection", "").split(",")
        }
        headers = {k: v for k, v in self.headers.items() if k.lower() not in excluded}
        headers["Authorization"] = "Bearer " + self.server.api_key
        request = urllib.request.Request(
            self.server.upstream + self.path,
            data=payload,
            headers=headers,
            method=self.command,
        )
        # The configured host is fixed; never forward credentials to a redirect target.
        handlers = [NoRedirect()]
        if self.server.proxy_url:
            handlers.append(
                urllib.request.ProxyHandler(
                    {"http": self.server.proxy_url, "https": self.server.proxy_url}
                )
            )
        if self.server.ca_cert:
            context = ssl.create_default_context()
            context.load_verify_locations(self.server.ca_cert)
            handlers.append(urllib.request.HTTPSHandler(context=context))
        opener = urllib.request.build_opener(*handlers)
        reply = None
        headers_sent = False
        try:
            try:
                reply = opener.open(request, timeout=600)
            except urllib.error.HTTPError as exc:
                if 300 <= exc.code < 400:
                    exc.close()
                    self.fail(
                        502,
                        "Upstream redirect refused; configure the final gateway URL",
                    )
                    return
                reply = exc  # Preserve native upstream error status/body, no retries.
            self.send_response(reply.status)
            excluded = HOP_HEADERS | {
                v.strip().lower()
                for v in reply.headers.get("Connection", "").split(",")
            }
            for key, value in reply.headers.items():
                if key.lower() not in excluded:
                    self.send_header(key, value)
            self.send_header("Connection", "close")
            self.end_headers()
            headers_sent = True
            if self.command != "HEAD":
                while chunk := reply.read1(65536):
                    self.wfile.write(chunk)
                    self.wfile.flush()  # SSE remains incremental, not buffered to completion.
        except (OSError, urllib.error.URLError, http.client.HTTPException):
            if not headers_sent:
                with contextlib.suppress(OSError):
                    self.fail(
                        502, "Gateway connection failed (request was not retried)"
                    )
            # Once streaming started, close the stream; never append a fake success.
        finally:
            if reply is not None:
                reply.close()
            self.close_connection = True

    do_GET = do_POST = do_PUT = do_PATCH = do_DELETE = do_HEAD = do_OPTIONS = relay


def exec_native(binary, args, env):
    """Start a private ephemeral relay, then replace this process with native agy."""
    token = secrets.token_urlsafe(32)
    server = GatewayServer(
        env["AGY_LLM_GATEWAY_URL"],
        env["AGY_LLM_GATEWAY_API_KEY"],
        token,
        env.get("AGY_LLM_GATEWAY_PROXY_URL"),
        env.get("AGY_LLM_GATEWAY_CA_CERT"),
    )
    ready_read, ready_write = os.pipe()
    parent = os.getpid()
    try:
        pid = os.fork()
    except BaseException:
        server.server_close()
        os.close(ready_read)
        os.close(ready_write)
        raise
    if pid == 0:
        try:
            os.close(ready_read)
            # The child must not keep ACP stdin/stdout alive after native agy exits.
            null = os.open(os.devnull, os.O_RDWR)
            for fd in (0, 1, 2):
                os.dup2(null, fd)
            keep = {0, 1, 2, ready_write, server.fileno()}
            for name in os.listdir("/proc/self/fd"):
                fd = int(name)
                if fd not in keep:
                    with contextlib.suppress(OSError):
                        os.close(fd)
            signal.signal(signal.SIGINT, signal.SIG_IGN)
            signal.signal(signal.SIGTERM, signal.SIG_DFL)
            libc = ctypes.CDLL(None, use_errno=True)
            if libc.prctl(1, signal.SIGTERM, 0, 0, 0) != 0 or os.getppid() != parent:
                os._exit(1)
            os.write(ready_write, b"1")
            os.close(ready_write)
            server.serve_forever()
        finally:
            os._exit(0)
    os.close(ready_write)
    port = server.server_port
    server.server_close()
    try:
        if (
            not select.select([ready_read], [], [], 5)[0]
            or os.read(ready_read, 1) != b"1"
        ):
            raise RuntimeError("Local gateway child failed to start")
        os.close(ready_read)
        ready_read = -1
        child_env = dict(env)
        child_env["AGY_LLM_GATEWAY_URL"] = f"http://127.0.0.1:{port}"
        child_env["AGY_LLM_GATEWAY_API_KEY"] = token
        for key in ("NO_PROXY", "no_proxy"):
            child_env[key] = ",".join(
                filter(None, [child_env.get(key), "127.0.0.1", "localhost"])
            )
        # Proxy/TLS options for native upstream are retained in this relay's environment,
        # not applied to its plain-HTTP local listener.
        child_env.pop("AGY_LLM_GATEWAY_PROXY_URL", None)
        os.execve(str(binary), [str(binary), *args], child_env)
    except BaseException:
        with contextlib.suppress(ProcessLookupError):
            os.kill(pid, signal.SIGTERM)
        with contextlib.suppress(ChildProcessError):
            os.waitpid(pid, 0)
        raise
    finally:
        if ready_read >= 0:
            os.close(ready_read)
