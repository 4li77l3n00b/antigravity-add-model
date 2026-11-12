# SPDX-License-Identifier: Apache-2.0
import contextlib
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
import http.client
import importlib.util
import json
import os
from pathlib import Path
import subprocess
import sys
import tempfile
import threading
import time
import unittest

SOURCE = Path(__file__).resolve().parents[1] / "gateway/gateway_adapter.py"
spec = importlib.util.spec_from_file_location("gateway_adapter", SOURCE)
adapter = importlib.util.module_from_spec(spec)
spec.loader.exec_module(adapter)


class Fixture(BaseHTTPRequestHandler):
    protocol_version = "HTTP/1.1"

    def log_message(self, *_):
        pass

    def do_POST(self):
        body = json.loads(self.rfile.read(int(self.headers.get("Content-Length", "0"))))
        self.server.received.append(
            {"path": self.path, "body": body, "key": self.headers.get("Authorization")}
        )
        if self.path == "/redirect":
            self.send_response(307)
            self.send_header("Location", "http://127.0.0.1:1/stolen")
            self.send_header("Content-Length", "0")
            self.end_headers()
            return
        if self.path == "/error":
            data = b'{"error":{"message":"upstream test error","status":"RESOURCE_EXHAUSTED","details":[{"reason":"MODEL_CAPACITY_EXHAUSTED"}]}}'
            self.send_response(429)
            self.send_header("Retry-After", "7")
            self.send_header("Content-Type", "application/json")
            self.send_header("Content-Length", str(len(data)))
            self.end_headers()
            self.wfile.write(data)
            return
        if self.path == "/stream":
            self.send_response(200)
            self.send_header("Content-Type", "text/event-stream")
            self.send_header("Connection", "close")
            self.end_headers()
            self.wfile.write(b"data: first\n\n")
            self.wfile.flush()
            self.server.release.wait(5)
            with contextlib.suppress(OSError):
                self.wfile.write(b"data: [DONE]\n\n")
            self.close_connection = True
            return
        data = json.dumps(body).encode()
        self.send_response(200)
        self.send_header("Content-Type", "application/json")
        self.send_header("Content-Length", str(len(data)))
        self.end_headers()
        self.wfile.write(data)


class GatewayTests(unittest.TestCase):
    def setUp(self):
        self.upstream = ThreadingHTTPServer(("127.0.0.1", 0), Fixture)
        self.upstream.received = []
        self.upstream.release = threading.Event()
        self.proxy = adapter.GatewayServer(
            f"http://127.0.0.1:{self.upstream.server_port}",
            "test-real-key",
            "test-local-key",
        )
        for server in (self.upstream, self.proxy):
            threading.Thread(target=server.serve_forever, daemon=True).start()

    def tearDown(self):
        self.upstream.release.set()
        for server in (self.proxy, self.upstream):
            server.shutdown()
            server.server_close()

    def request(self, path, body, key="test-local-key"):
        conn = http.client.HTTPConnection(
            "127.0.0.1", self.proxy.server_port, timeout=5
        )
        conn.request(
            "POST",
            path,
            json.dumps(body),
            {"Authorization": "Bearer " + key, "Content-Type": "application/json"},
        )
        return conn, conn.getresponse()

    def test_nine_aliases_preserve_tools_and_content(self):
        for model in adapter.MODELS:
            for effort in adapter.EFFORTS:
                body = {
                    "model": f"{model}-{effort}",
                    "reasoning_effort": effort,
                    "stream": True,
                    "messages": [{"role": "user", "content": "private prompt"}],
                    "tools": [{"unknown": {"x": 2}}],
                    "parallel_tool_calls": True,
                    "extra": {"untouched": 1},
                }
                conn, reply = self.request("/v1/chat/completions", body)
                result = json.loads(reply.read())
                conn.close()
                self.assertEqual(result, {**body, "model": model})
                self.assertEqual(
                    self.upstream.received[-1]["key"], "Bearer test-real-key"
                )

    def test_authentication_and_conflicting_effort(self):
        conn, r = self.request("/v1/chat/completions", {}, "wrong")
        self.assertEqual(r.status, 401)
        r.read()
        conn.close()
        conn, r = self.request(
            "/v1/chat/completions",
            {"model": "gemini-4-carbon-low", "reasoning_effort": "high"},
        )
        self.assertEqual(r.status, 400)
        r.read()
        conn.close()
        self.assertEqual(self.upstream.received, [])

    def test_unknown_models_and_other_paths_passthrough(self):
        for path, body in [
            (
                "/v1/chat/completions",
                {"model": "other-model", "reasoning_effort": "max"},
            ),
            ("/v1beta/tools", {"model": "gemini-4-carbon-high", "payload": [1, 2]}),
        ]:
            conn, r = self.request(path, body)
            self.assertEqual(json.loads(r.read()), body)
            conn.close()

    def test_errors_are_preserved_and_never_retried(self):
        conn, r = self.request("/error", {})
        self.assertEqual(r.status, 429)
        self.assertEqual(r.getheader("Retry-After"), "7")
        body = json.loads(r.read())
        conn.close()
        self.assertEqual(body["error"]["status"], "RESOURCE_EXHAUSTED")
        self.assertEqual(
            body["error"]["details"], [{"reason": "MODEL_CAPACITY_EXHAUSTED"}]
        )
        self.assertEqual(len(self.upstream.received), 1)

    def test_three_concurrent_streams_and_capacity_error(self):
        # Transport capacity test only: these are HTTP clients, not native swarm workers.
        streams = []
        try:
            for role in ("coordinator", "worker-a", "worker-b"):
                conn, reply = self.request("/stream", {"role": role})
                streams.append((conn, reply))
                self.assertEqual(reply.readline(), b"data: first\n")
            self.assertFalse(self.upstream.release.is_set())
            conn, reply = self.request("/error", {})
            self.assertEqual(reply.status, 429)
            self.assertEqual(reply.getheader("Retry-After"), "7")
            self.assertEqual(
                json.loads(reply.read())["error"]["status"], "RESOURCE_EXHAUSTED"
            )
            conn.close()
            self.upstream.release.set()
            for _, reply in streams:
                self.assertIn(b"[DONE]", reply.read())
            self.assertEqual(len(self.upstream.received), 4)
        finally:
            self.upstream.release.set()
            for conn, _ in streams:
                conn.close()

    def test_redirect_refused(self):
        conn, r = self.request("/redirect", {})
        self.assertEqual(r.status, 502)
        self.assertIn(b"redirect refused", r.read())
        conn.close()
        self.assertEqual(len(self.upstream.received), 1)

    def test_stream_first_event_arrives_before_completion(self):
        conn, r = self.request("/stream", {})
        self.assertFalse(self.upstream.release.is_set())
        self.assertEqual(r.readline(), b"data: first\n")
        self.upstream.release.set()
        self.assertIn(b"[DONE]", r.read())
        conn.close()

    def test_child_reaped_when_exec_parent_exits(self):
        with tempfile.TemporaryDirectory() as tmp:
            portfile = Path(tmp) / "port"
            code = (
                "import os,time;from pathlib import Path;Path("
                + repr(str(portfile))
                + ").write_text(os.environ['AGY_LLM_GATEWAY_URL']);time.sleep(30)"
            )
            parent = (
                "import sys,os;sys.path.insert(0,"
                + repr(str(SOURCE.parent))
                + ");import gateway_adapter as g;g.exec_native(sys.executable,['-c',"
                + repr(code)
                + "],dict(os.environ,AGY_LLM_GATEWAY_URL='http://127.0.0.1:1',AGY_LLM_GATEWAY_API_KEY='test'))"
            )
            process = subprocess.Popen(
                [sys.executable, "-c", parent],
                stdin=subprocess.PIPE,
                stdout=subprocess.PIPE,
                stderr=subprocess.PIPE,
            )
            try:
                for _ in range(100):
                    if portfile.exists():
                        break
                    time.sleep(0.02)
                self.assertTrue(portfile.exists())
                port = int(portfile.read_text().rsplit(":", 1)[1])
                process.terminate()
                process.wait(timeout=5)
                # No inherited stdio descriptors keep communicate hanging after native exit.
                process.communicate(timeout=5)
                import socket

                for _ in range(100):
                    try:
                        with socket.create_connection(("127.0.0.1", port), 0.1):
                            pass
                    except OSError:
                        break
                    time.sleep(0.02)
                else:
                    self.fail("Relay leaked after its exec parent exited")
            finally:
                if process.poll() is None:
                    process.kill()
                    process.wait()


if __name__ == "__main__":
    unittest.main()
