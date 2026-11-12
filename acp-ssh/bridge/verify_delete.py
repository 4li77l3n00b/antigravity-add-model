#!/usr/bin/env python3
# SPDX-License-Identifier: Apache-2.0
"""Real native deletion integration. Isolated HOME, mock model, no production host."""

import asyncio
import contextlib
import json
import os
from pathlib import Path
import signal
import sqlite3
import sys
import tempfile
import threading
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer

import acp_ssh as bridge
from native_delete import NativeDelete, METHOD, META

NATIVE = Path(
    os.environ.get(
        "AGY_NATIVE_BINARY", str(Path.home() / ".local/lib/antigravity-cli/agy")
    )
).resolve()


class Model(BaseHTTPRequestHandler):
    calls = 0

    def log_message(self, *args):
        pass

    def do_POST(self):
        self.rfile.read(int(self.headers.get("Content-Length", 0)))
        type(self).calls += 1
        chunks = [
            {
                "id": "fixture",
                "object": "chat.completion.chunk",
                "choices": [
                    {
                        "index": 0,
                        "delta": {
                            "role": "assistant",
                            "content": "ISOLATED_FIXTURE_REPLY",
                        },
                        "finish_reason": None,
                    }
                ],
            },
            {
                "id": "fixture",
                "object": "chat.completion.chunk",
                "choices": [{"index": 0, "delta": {}, "finish_reason": "stop"}],
            },
        ]
        body = (
            "".join("data: " + json.dumps(c) + "\n\n" for c in chunks)
            + "data: [DONE]\n\n"
        ).encode()
        self.send_response(200)
        self.send_header("Content-Type", "text/event-stream")
        self.send_header("Content-Length", str(len(body)))
        self.end_headers()
        self.wfile.write(body)


async def run(home, port):
    env = {
        "HOME": str(home),
        "PATH": os.environ.get("PATH", "/usr/bin:/bin"),
        "LANG": "C.UTF-8",
        "ACP_SSH_HOME": str(home / "bridge"),
        "AGY_LLM_GATEWAY_URL": f"http://127.0.0.1:{port}",
        "AGY_LLM_GATEWAY_WIRE_PROTOCOL": "openai",
        "AGY_LLM_GATEWAY_API_KEY": "fake-isolated",
        "AGY_LLM_GATEWAY_MODELS": "gemini-4-carbon-high",
        "NO_PROXY": "*",
    }
    os.environ["ACP_SSH_HOME"] = env["ACP_SSH_HOME"]
    folder, sock = bridge.paths("delete-test")
    folder.mkdir(parents=True, mode=0o700)
    conf = {
        "cwd": str(home),
        "command": [
            str(NATIVE),
            "acp",
            "--process-per-session",
            "--disable-telemetry",
            "--include-default-customizations=false",
        ],
        "native_delete": {
            "home": str(home),
            "app_data_name": "antigravity-cli",
            "binary": str(NATIVE),
        },
    }
    (folder / "config.json").write_text(json.dumps(conf))
    host = None
    writer = None
    seq = 0
    updates = []

    async def rpc(value):
        return await asyncio.to_thread(bridge.rpc, "delete-test", value, 5)

    async def start():
        nonlocal host, writer, reader
        host = await asyncio.create_subprocess_exec(
            sys.executable,
            str(Path(bridge.__file__)),
            "serve",
            "delete-test",
            env=env,
            stdout=asyncio.subprocess.DEVNULL,
            stderr=asyncio.subprocess.PIPE,
            start_new_session=True,
        )
        for _ in range(100):
            try:
                await rpc({"op": "status"})
                break
            except (OSError, ValueError, RuntimeError):
                await asyncio.sleep(0.05)
        else:
            raise RuntimeError("Test host did not start")
        reader, writer = await asyncio.open_unix_connection(
            str(sock), limit=16 * 1024 * 1024
        )
        writer.write(bridge.packet({"op": "attach"}))
        await writer.drain()
        assert json.loads(await reader.readline())["ok"]

    async def call(method, params):
        nonlocal seq
        seq += 1
        ident = seq
        writer.write(
            bridge.packet(
                {"jsonrpc": "2.0", "id": ident, "method": method, "params": params}
            )
        )
        await writer.drain()
        async with asyncio.timeout(80):
            while True:
                line = await reader.readline()
                if not line:
                    raise RuntimeError("Bridge closed")
                msg = json.loads(line)
                if msg.get("method") == "session/update":
                    updates.append(msg["params"])
                if msg.get("id") == ident and "method" not in msg:
                    return msg

    async def stop():
        nonlocal host, writer
        if writer:
            writer.close()
            await writer.wait_closed()
            writer = None
        if host and host.returncode is None:
            host.terminate()
            try:
                await asyncio.wait_for(host.wait(), 10)
            except asyncio.TimeoutError:
                os.killpg(host.pid, signal.SIGKILL)
                await host.wait()
        if host:
            err = (await host.stderr.read()).decode()
            assert "Task exception was never retrieved" not in err, err
            host = None

    reader = None
    profile = {
        "protocolVersion": 1,
        "clientInfo": {"name": "isolated-delete-test", "version": "1"},
        "clientCapabilities": {},
    }
    try:
        await start()
        init = await call("initialize", profile)
        assert init["result"]["agentCapabilities"]["_meta"][META]["delete"]["permanent"]
        ids = []
        for label in ("TARGET_SENTINEL", "SURVIVOR_SENTINEL"):
            new = await call("session/new", {"cwd": str(home), "mcpServers": []})
            sid = new["result"]["sessionId"]
            ids.append(sid)
            reply = await call(
                "session/prompt",
                {"sessionId": sid, "prompt": [{"type": "text", "text": label}]},
            )
            assert reply.get("result", {}).get("stopReason") == "end_turn", reply
        target, survivor = ids
        native = NativeDelete(conf["native_delete"])
        before = await rpc({"op": "status"})
        model_calls = Model.calls
        assert native.preflight(target) and native.preflight(survivor)
        # Native summary cache must actually contain generated history before deletion.
        count_before = len(updates)
        listed = await call("session/list", {})
        listed_before = any(
            x["sessionId"] == target for x in listed["result"]["sessions"]
        )
        result = await call(METHOD, {"sessionId": target})
        print("deleteResult", json.dumps(result), flush=True)
        assert result.get("result", {}).get("deleted") is True, result
        native.verify_absent(target)
        assert native.preflight(survivor)
        assert Model.calls == model_calls, "Deletion made a model request"
        again = await call(METHOD, {"sessionId": target})
        assert again["result"]["alreadyDeleted"], again
        blocked = await call(
            "session/load", {"sessionId": target, "cwd": str(home), "mcpServers": []}
        )
        assert blocked["error"]["code"] == -32051, blocked
        loaded = await call(
            "session/load", {"sessionId": survivor, "cwd": str(home), "mcpServers": []}
        )
        assert "result" in loaded, loaded
        assert any(
            p["sessionId"] == survivor
            and p["update"]["sessionUpdate"] == "user_message_chunk"
            for p in updates[count_before:]
        )
        listed = await call("session/list", {})
        assert not any(x["sessionId"] == target for x in listed["result"]["sessions"])
        with contextlib.closing(
            sqlite3.connect(
                "file:" + str(folder / "state.sqlite3") + "?mode=ro", uri=True
            )
        ) as db:
            assert (
                db.execute(
                    "SELECT count(*) FROM jobs WHERE session=?", (target,)
                ).fetchone()[0]
                == 0
            )
            assert (
                db.execute(
                    "SELECT count(*) FROM events WHERE instr(body,?)>0",
                    ("TARGET_SENTINEL",),
                ).fetchone()[0]
                == 0
            )
        after = await rpc({"op": "status"})
        assert before["agentPid"] == after["agentPid"] and before["pid"] == after["pid"]
        summary_file = native.root / "jetbox_summaries_proto.pb"
        print(
            "beforeRestart",
            json.dumps(
                {
                    "targetInLegacyIndex": summary_file.exists()
                    and target.encode() in summary_file.read_bytes(),
                    "targetSummaryRows": len(
                        [
                            r
                            for r in native.summary_rows(native.root)
                            if r["conversation_id"] == target
                        ]
                    ),
                }
            ),
            flush=True,
        )
        await stop()
        await start()
        await call("initialize", profile)
        blocked = await call(
            "session/load", {"sessionId": target, "cwd": str(home), "mcpServers": []}
        )
        assert blocked["error"]["code"] == -32051
        before_retry = native.preflight(target)
        print(
            "afterRestart",
            json.dumps(
                {
                    "targetDataExists": before_retry,
                    "paths": [
                        str(p.relative_to(home))
                        for p in native.history_paths(native.root, target)
                        if p.exists()
                    ],
                    "summaryRows": [
                        r
                        for r in native.summary_rows(native.root)
                        if r["conversation_id"] == target
                    ],
                }
            ),
            flush=True,
        )
        retry = await call(METHOD, {"sessionId": target})
        print("retryAfterRestart", json.dumps(retry), flush=True)
        assert retry["result"]["alreadyDeleted"]
        survivor_loaded = await call(
            "session/load", {"sessionId": survivor, "cwd": str(home), "mcpServers": []}
        )
        assert "result" in survivor_loaded
        return {
            "success": True,
            "nativeVersion": "1.5.5",
            "modelBackend": "isolated loopback mock",
            "targetListedBeforeDeletion": listed_before,
            "nativeFilesAndSummaryRemoved": True,
            "bridgeHistoryPurged": True,
            "survivorHistoryReloaded": True,
            "noModelCallDuringDeletion": True,
            "idempotentRetry": True,
            "resurrectionBlockedAfterHostRestart": True,
            "liveAgentNotRestartedForDeletion": True,
        }
    finally:
        await stop()


if __name__ == "__main__":
    http = ThreadingHTTPServer(("127.0.0.1", 0), Model)
    threading.Thread(target=http.serve_forever, daemon=True).start()
    try:
        manager = (
            contextlib.nullcontext(tempfile.mkdtemp(prefix="agy-delete-integration-"))
            if os.environ.get("KEEP_DELETE_FIXTURE")
            else tempfile.TemporaryDirectory(prefix="agy-delete-integration-")
        )
        with manager as tmp:
            print("fixture", tmp, flush=True)
            result = asyncio.run(run(Path(tmp), http.server_port))
            print(json.dumps(result, indent=2))
            if os.environ.get("ACP_TEST_REPORT"):
                Path(os.environ["ACP_TEST_REPORT"]).write_text(
                    json.dumps(result, indent=2) + "\n"
                )
    finally:
        http.shutdown()
        http.server_close()
