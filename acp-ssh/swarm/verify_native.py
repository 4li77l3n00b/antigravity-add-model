#!/usr/bin/env python3
# SPDX-License-Identifier: Apache-2.0
"""End-to-end native swarm facade test; isolated HOME and mock model, no real API calls."""

import asyncio, json, os, signal, tempfile, threading, sys
from pathlib import Path
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer

APPROVAL = "--approval-probe" in sys.argv
BRIDGE = "--bridge-reconnect" in sys.argv
PACKAGE = Path(__file__).resolve().parents[1]
ARTIFACTS = tempfile.TemporaryDirectory(prefix="acp-swarm-verification-")
OUT = Path(ARTIFACTS.name)
requests = []
events = []
stage = 0
barrier = threading.Barrier(2)
lock = threading.Lock()
active = 0
peak = 0


class Handler(BaseHTTPRequestHandler):
    def log_message(self, *a):
        pass

    def do_POST(self):
        data = json.loads(self.rfile.read(int(self.headers["Content-Length"])))
        requests.append(data)
        global stage, active, peak
        content = {"role": "assistant", "content": "MOCK_OK"}
        finish = "stop"
        users = " ".join(
            str(m.get("content", ""))
            for m in data.get("messages", [])
            if m["role"] == "user"
        )
        names = {t["function"]["name"] for t in data.get("tools", [])}
        if (
            names
            and "NATIVE_WORKER_" in users
            and "/swarm Create a test team" not in users
        ):
            worker = "2" if "NATIVE_WORKER_2" in users else "3"
            previous = [
                m.get("tool_call_id") for m in data["messages"] if m["role"] == "tool"
            ]
            if worker == "2" and "permission_probe" not in previous:
                content = {
                    "role": "assistant",
                    "tool_calls": [
                        {
                            "index": 0,
                            "id": "permission_probe",
                            "type": "function",
                            "function": {
                                "name": "run_command",
                                "arguments": json.dumps(
                                    {
                                        "CommandLine": "printf SWARM_PERMISSION_PROBE",
                                        "toolSummary": "Permission probe",
                                        "WaitMsBeforeAsync": 500,
                                    }
                                ),
                            },
                        }
                    ],
                }
                finish = "tool_calls"
            elif "post_" + worker not in previous:
                with lock:
                    active += 1
                    peak = max(peak, active)
                try:
                    barrier.wait(timeout=6)
                except threading.BrokenBarrierError:
                    pass
                __import__("time").sleep(0.2)
                with lock:
                    active -= 1
                content = {
                    "role": "assistant",
                    "tool_calls": [
                        {
                            "index": 0,
                            "id": "post_" + worker,
                            "type": "function",
                            "function": {
                                "name": "post_message",
                                "arguments": json.dumps(
                                    {
                                        "text": "@coordinator NATIVE_WORKER_"
                                        + worker
                                        + " done"
                                    }
                                ),
                            },
                        }
                    ],
                }
                finish = "tool_calls"
            else:
                content["content"] = "MOCK_WORKER_DONE"
        elif "swarm" in names and "/swarm Create a test team" in users:
            stage += 1
            if stage == 1:
                tool, args = "swarm", {"action": "title", "title": "Native ACP Test"}
            elif stage in (2, 3):
                tool, args = (
                    "swarm",
                    {
                        "action": "add",
                        "name": "worker-" + str(stage),
                        "brief": "NATIVE_WORKER_"
                        + str(stage)
                        + ": reply MOCK_WORKER_DONE. Do not use any tools or files.",
                    },
                )
            elif stage == 4:
                tool = None
            else:
                tool = None
            if tool:
                content = {
                    "role": "assistant",
                    "tool_calls": [
                        {
                            "index": 0,
                            "id": "native_" + str(stage),
                            "type": "function",
                            "function": {"name": tool, "arguments": json.dumps(args)},
                        }
                    ],
                }
                finish = "tool_calls"

        chunks = [
            {
                "id": "test",
                "object": "chat.completion.chunk",
                "choices": [{"index": 0, "delta": content, "finish_reason": None}],
            },
            {
                "id": "test",
                "object": "chat.completion.chunk",
                "choices": [{"index": 0, "delta": {}, "finish_reason": finish}],
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


async def main(home, port):
    native = Path(
        os.environ.get(
            "AGY_NATIVE_BINARY", str(Path.home() / ".local/lib/antigravity-cli/agy")
        )
    ).resolve()
    launch = home / "agy-gateway"
    import shlex

    launch.write_text(
        "#!/bin/sh\nexec "
        + shlex.quote(sys.executable)
        + " "
        + shlex.quote(str(PACKAGE / "gateway/launch.py"))
        + ' "$@"\n'
    )
    launch.chmod(0o700)
    env = {
        "PATH": os.environ.get("PATH", "/usr/bin:/bin"),
        "LANG": "C.UTF-8",
        "HOME": str(home),
        "AGY_NATIVE_BINARY": str(native),
        "AGY_LLM_GATEWAY_URL": f"http://127.0.0.1:{port}",
        "AGY_LLM_GATEWAY_API_KEY": "fake",
        "NO_PROXY": "127.0.0.1,localhost",
        "no_proxy": "127.0.0.1,localhost",
    }
    env.pop("AGY_LLM_GATEWAY_MODELS", None)
    env.pop("AGY_GATEWAY_ADAPTER", None)
    agent = home / ".gemini/config/agents/native-swarm-acp.md"
    agent.parent.mkdir(parents=True)
    agent.write_bytes((PACKAGE / "swarm/native-swarm-acp.md").read_bytes())
    command = [
        sys.executable,
        str(PACKAGE / "swarm/server.py"),
        "--native",
        str(launch),
        "--poll-seconds",
        "0.2",
        "--approval",
        "ask" if APPROVAL else "auto",
    ]
    host = None
    bridge = str(PACKAGE / "bridge/acp_ssh.py")

    async def bridge_control(*args):
        ctl = await asyncio.create_subprocess_exec(
            sys.executable,
            bridge,
            *args,
            env=env,
            stdout=asyncio.subprocess.PIPE,
            stderr=asyncio.subprocess.PIPE,
        )
        out, err = await asyncio.wait_for(ctl.communicate(), 10)
        if ctl.returncode:
            raise RuntimeError(err.decode())
        return json.loads(out)

    async def connect():
        return await asyncio.create_subprocess_exec(
            sys.executable,
            bridge,
            "connect",
            "verify",
            cwd=home,
            env=env,
            stdin=asyncio.subprocess.PIPE,
            stdout=asyncio.subprocess.PIPE,
            stderr=open(OUT / "connect.stderr", "a"),
            start_new_session=True,
        )

    if BRIDGE:
        env["ACP_SSH_HOME"] = str(home / "bridge-state")
        await bridge_control(
            "configure",
            "verify",
            "--cwd",
            str(home),
            "--preserve-background-requests",
            "--command",
            *command,
        )
        host = await asyncio.create_subprocess_exec(
            sys.executable,
            bridge,
            "serve",
            "verify",
            env=env,
            stdout=asyncio.subprocess.DEVNULL,
            stderr=open(OUT / "host.stderr", "w"),
            start_new_session=True,
        )
        for _ in range(50):
            try:
                await bridge_control("status", "verify")
                break
            except RuntimeError:
                await asyncio.sleep(0.1)
        else:
            raise RuntimeError("Isolated host did not start")
        p = await connect()
    else:
        p = await asyncio.create_subprocess_exec(
            *command,
            cwd=home,
            env=env,
            stdin=asyncio.subprocess.PIPE,
            stdout=asyncio.subprocess.PIPE,
            stderr=open(OUT / "stderr", "w"),
            start_new_session=True,
        )
    seq = 0

    async def call(method, params):
        nonlocal seq
        seq += 1
        i = seq
        p.stdin.write(
            (
                json.dumps(
                    {"jsonrpc": "2.0", "id": i, "method": method, "params": params}
                )
                + "\n"
            ).encode()
        )
        await p.stdin.drain()
        while True:
            line = await asyncio.wait_for(p.stdout.readline(), 30)
            if not line:
                raise RuntimeError("EOF")
            msg = json.loads(line)
            events.append(msg)
            if msg.get("id") == i and "method" not in msg:
                return msg
            if "id" in msg and "method" in msg:
                p.stdin.write(
                    (
                        json.dumps(
                            {
                                "jsonrpc": "2.0",
                                "id": msg["id"],
                                "result": {"outcome": {"outcome": "cancelled"}},
                            }
                        )
                        + "\n"
                    ).encode()
                )
                await p.stdin.drain()

    try:
        print(
            "init",
            await call(
                "initialize",
                {
                    "protocolVersion": 1,
                    "clientInfo": {
                        "name": "antigravity-model-patch-ssh",
                        "version": "1.0.0",
                    },
                    "clientCapabilities": {},
                },
            ),
            flush=True,
        )
        r = await call("session/new", {"cwd": str(home), "mcpServers": []})
        sid = r["result"]["sessionId"]
        print("new", sid, flush=True)
        await call(
            "session/set_config_option",
            {"sessionId": sid, "configId": "model", "value": "gemini-4-carbon"},
        )
        await call(
            "session/set_config_option",
            {"sessionId": sid, "configId": "thought_level", "value": "high"},
        )
        await call("_maichart/swarm/subscribe", {"sessionId": sid})
        print(
            "prompt",
            await call(
                "session/prompt",
                {
                    "sessionId": sid,
                    "prompt": [
                        {
                            "type": "text",
                            "text": "/swarm Create a test team of two workers; no file changes.",
                        }
                    ],
                },
            ),
            flush=True,
        )
        await asyncio.sleep(5)
        print("list", await call("session/list", {}), flush=True)
        snapshot = await call("_maichart/swarm/snapshot", {"sessionId": sid})
        print(
            "snapshot members",
            len(snapshot.get("result", {}).get("members", [])),
            flush=True,
        )
        if APPROVAL:
            permissions = [
                e for e in events if e.get("method") == "session/request_permission"
            ]
            summary = {
                "probe": "native-worker-permission",
                "forwardedPermissions": len(permissions),
                "worker2HasNoResultPost": not any(
                    m.get("agent") == "worker-2" for m in snapshot["result"]["messages"]
                ),
                "note": "This diagnostic does not establish successful worker approval support.",
            }
            (OUT / "summary.json").write_text(json.dumps(summary, indent=2))
            print(json.dumps(summary), flush=True)
            return
        assert len(snapshot["result"]["members"]) == 3, snapshot
        assert len(snapshot["result"]["messages"]) >= 4, snapshot
        assert peak == 2, peak
        assert not any(
            e.get("method") == "session/request_permission" for e in events
        ), events
        assert any(
            "SWARM_PERMISSION_PROBE" in str(m.get("content"))
            for r in requests
            for m in r.get("messages", [])
            if m.get("role") == "tool"
        ), "native worker command result missing"
        for name in ("worker-2", "worker-3"):
            hist = await call(
                "_maichart/swarm/history", {"sessionId": sid, "member": name}
            )
            assert hist["result"]["records"], hist
        assert all(
            m["native"]["applied_model"] == "gemini-4-carbon-high"
            for m in snapshot["result"]["members"]
        ), snapshot
        assert any(e.get("method") == "_maichart/swarm/update" for e in events), events
        initial_attach = await call("_maichart/swarm/attach", {"sessionId": sid})
        if BRIDGE:
            before = await bridge_control("status", "verify")
            p.stdin.close()
            await asyncio.wait_for(p.wait(), 5)
            for _ in range(50):
                if not (await bridge_control("status", "verify"))["attached"]:
                    break
                await asyncio.sleep(0.02)
            p = await connect()
            await call(
                "initialize",
                {
                    "protocolVersion": 1,
                    "clientInfo": {
                        "name": "antigravity-model-patch-ssh",
                        "version": "1.0.0",
                    },
                    "clientCapabilities": {},
                },
            )
            assert (await bridge_control("status", "verify"))["agentPid"] == before[
                "agentPid"
            ]
        opened = await call("_maichart/swarm/sessions", {})
        assert any(s["sessionId"] == sid for s in opened["result"]["sessions"]), opened
        attached = await call("_maichart/swarm/attach", {"sessionId": sid})
        assert (
            attached["result"]["generation"] == initial_attach["result"]["generation"]
        )
        options = attached["result"]["metadata"]["nativeSession"]["configOptions"]
        assert (
            next(c for c in options if c["id"] == "thought_level")["currentValue"]
            == "high"
        ), attached
        assert (
            next(c for c in options if c["id"] == "model")["currentValue"]
            == "gemini-4-carbon"
        ), attached
        parent_history = await call(
            "_maichart/swarm/history", {"sessionId": sid, "member": "coordinator"}
        )
        assert (
            parent_history["result"]["source"] == "native-transcript"
            and parent_history["result"]["records"]
        ), parent_history
        subscribed = await call("_maichart/swarm/subscribe", {"sessionId": sid})
        assert subscribed["result"]["snapshot"]["head"] >= 4, subscribed
        if BRIDGE:
            # The transport's stored requests are checked below via its management events API.
            import importlib.util

            spec = importlib.util.spec_from_file_location("verify_bridge", bridge)
            module = importlib.util.module_from_spec(spec)
            spec.loader.exec_module(module)
            old = os.environ.get("ACP_SSH_HOME")
            os.environ["ACP_SSH_HOME"] = env["ACP_SSH_HOME"]
            audit_events = []
            cursor = 0
            try:
                while True:
                    audit = await asyncio.to_thread(
                        module.rpc, "verify", {"op": "events", "after": cursor}, 5
                    )
                    assert not audit["gap"], audit
                    audit_events.extend(audit["events"])
                    if not audit["more"]:
                        break
                    assert audit["events"], audit
                    cursor = audit["events"][-1]["seq"]
            finally:
                if old is None:
                    os.environ.pop("ACP_SSH_HOME", None)
                else:
                    os.environ["ACP_SSH_HOME"] = old
            assert not any(
                e.get("kind") == "client"
                and e.get("data", {}).get("method")
                in ("session/load", "session/resume")
                for e in audit_events
            ), audit_events
        (OUT / "summary.json").write_text(
            json.dumps(
                {
                    "success": True,
                    "nativeWorkerConcurrency": peak,
                    "members": len(snapshot["result"]["members"]),
                    "boardMessages": len(snapshot["result"]["messages"]),
                    "model": "gemini-4-carbon",
                    "effort": "high",
                    "approvalMode": "auto",
                    "workerCommandCompleted": True,
                    "readOnlyAttach": True,
                    "bridgeControllerReconnected": BRIDGE,
                    "nativeProcessReused": BRIDGE,
                    "coordinatorHistoryRead": True,
                    "noLoadOrResume": True,
                },
                indent=2,
            )
        )
    finally:
        os.killpg(p.pid, signal.SIGTERM)
        try:
            await asyncio.wait_for(p.wait(), 5)
        except asyncio.TimeoutError:
            os.killpg(p.pid, signal.SIGKILL)
            await p.wait()
        if host is not None:
            host.terminate()
            try:
                await asyncio.wait_for(host.wait(), 10)
            except asyncio.TimeoutError:
                host.kill()
                await host.wait()
        (OUT / "requests.json").write_text(json.dumps(requests, indent=2))
        (OUT / "events.json").write_text(json.dumps(events, indent=2))
        print(
            "Isolated probe finished; temporary artifacts are cleaned on exit.",
            flush=True,
        )


server = ThreadingHTTPServer(("127.0.0.1", 0), Handler)
threading.Thread(target=server.serve_forever, daemon=True).start()
try:
    with tempfile.TemporaryDirectory(prefix="agy-swarm-test-") as tmp:
        asyncio.run(main(Path(tmp), server.server_port))
    print((OUT / "summary.json").read_text(), flush=True)
finally:
    server.shutdown()
    server.server_close()
    ARTIFACTS.cleanup()
