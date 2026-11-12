# SPDX-License-Identifier: Apache-2.0
import asyncio
import importlib.util
import json
import os
from pathlib import Path
import signal
import sys
import tempfile
import unittest

ROOT = Path(__file__).resolve().parents[1]
SCRIPT = ROOT / "bridge/acp_ssh.py"
FAKE = ROOT / "bridge/fake_agent.py"
spec = importlib.util.spec_from_file_location("acp_ssh", SCRIPT)
bridge = importlib.util.module_from_spec(spec)
spec.loader.exec_module(bridge)
PROFILE = {
    "protocolVersion": 1,
    "clientInfo": {"name": "test", "version": "1"},
    "clientCapabilities": {
        "fs": {"readTextFile": True},
        "terminal": True,
        "_meta": {"custom": True},
    },
}


class HostTests(unittest.IsolatedAsyncioTestCase):
    async def asyncSetUp(self):
        self.temp = tempfile.TemporaryDirectory()
        self.old = os.environ.get("ACP_SSH_HOME")
        os.environ["ACP_SSH_HOME"] = self.temp.name
        self.folder, self.sock = bridge.paths("test")
        self.folder.mkdir(mode=0o700)
        self.folder.joinpath("config.json").write_text(
            json.dumps(
                {
                    "cwd": self.temp.name,
                    "command": [sys.executable, str(FAKE)],
                    "event_bytes": 64 * 1024 * 1024,
                }
            )
        )
        self.connections = []
        await self.start()

    async def start(self):
        self.process = await asyncio.create_subprocess_exec(
            sys.executable,
            str(SCRIPT),
            "serve",
            "test",
            stdout=asyncio.subprocess.DEVNULL,
            stderr=asyncio.subprocess.PIPE,
        )
        for _ in range(100):
            try:
                await self.rpc({"op": "status"})
                return
            except (OSError, ValueError, RuntimeError):
                await asyncio.sleep(0.03)
        self.fail("Host failed to start")

    async def asyncTearDown(self):
        for w in self.connections:
            w.close()
        if self.process.returncode is None:
            self.process.terminate()
            await asyncio.wait_for(self.process.wait(), 8)
        errors = (await self.process.stderr.read()).decode()
        self.assertNotIn("Task exception was never retrieved", errors)
        self.assertNotIn("Future exception was never retrieved", errors)
        self.sock.unlink(missing_ok=True)
        with __import__("contextlib").suppress(OSError):
            self.sock.parent.rmdir()
        if self.old is None:
            os.environ.pop("ACP_SSH_HOME")
        else:
            os.environ["ACP_SSH_HOME"] = self.old
        self.temp.cleanup()

    async def rpc(self, msg):
        return await asyncio.to_thread(bridge.rpc, "test", msg, 5)

    async def attach(self):
        r, w = await asyncio.open_unix_connection(str(self.sock))
        self.connections.append(w)
        w.write(bridge.packet({"op": "attach"}))
        await w.drain()
        hello = json.loads(await r.readline())
        if not hello["ok"]:
            w.close()
            raise RuntimeError(hello["error"])
        return r, w

    async def send(self, w, ident, method, params, **extra):
        w.write(
            bridge.packet(
                {
                    "jsonrpc": "2.0",
                    "id": ident,
                    "method": method,
                    "params": params,
                    **extra,
                }
            )
        )
        await w.drain()

    async def recv(self, r):
        return json.loads(await asyncio.wait_for(r.readline(), 5))

    async def result(self, r, ident):
        while True:
            msg = await self.recv(r)
            if msg.get("id") == ident and "method" not in msg:
                return msg

    async def ready(self):
        r, w = await self.attach()
        await self.send(w, 1, "initialize", PROFILE)
        init = await self.result(r, 1)
        await self.send(w, 2, "session/new", {"cwd": self.temp.name, "mcpServers": []})
        new = await self.result(r, 2)
        return r, w, new["result"]["sessionId"], init

    async def detached(self, w):
        w.close()
        await w.wait_closed()
        for _ in range(50):
            if not (await self.rpc({"op": "status"}))["attached"]:
                return
            await asyncio.sleep(0.01)
        self.fail("Did not detach")

    async def until(self, predicate):
        for _ in range(100):
            value = await predicate()
            if value:
                return value
            await asyncio.sleep(0.03)
        self.fail("Condition timed out")

    async def test_full_methods_and_extensions(self):
        r, w, sid, init = await self.ready()
        self.assertEqual(init["result"]["_meta"]["received"], PROFILE)
        for method in (
            "authenticate",
            "session/set_mode",
            "session/set_model",
            "session/set_config_option",
            "session/list",
            "session/resume",
            "session/fork",
            "session/close",
            "custom/extension",
        ):
            params = {
                "sessionId": sid,
                "cwd": self.temp.name,
                "_meta": {"unknown": [1, 2]},
                "mcpServers": [{"arbitrary": "retained"}],
            }
            await self.send(w, 8, method, params, _meta={"top": "kept"})
            msg = await self.result(r, 8)
            self.assertEqual(msg["result"]["echo"]["params"], params)
            self.assertEqual(msg["_meta"], {"extension": True})
            self.assertEqual(msg["result"]["echo"].get("_meta"), {"top": "kept"})

    async def test_disconnect_keeps_turn_and_load_after_completion(self):
        r, w, sid, _ = await self.ready()
        await self.send(
            w,
            3,
            "session/prompt",
            {"sessionId": sid, "prompt": [{"type": "text", "text": "delayed"}]},
        )
        self.assertEqual((await self.recv(r))["method"], "session/update")
        await self.detached(w)

        async def finished():
            return not (await self.rpc({"op": "status"}))["active"]

        await self.until(finished)
        events = await self.rpc({"op": "events", "after": 0})
        self.assertIn("done", json.dumps(events))
        r, w = await self.attach()
        await self.send(w, 1, "initialize", PROFILE)
        self.assertIn("result", await self.result(r, 1))
        await self.send(
            w,
            2,
            "session/load",
            {"sessionId": sid, "cwd": self.temp.name, "mcpServers": []},
        )
        self.assertEqual((await self.recv(r))["method"], "session/update")
        self.assertIn("result", await self.result(r, 2))

    async def test_permission_survives_disconnect_and_responds_once(self):
        r, w, sid, _ = await self.ready()
        await self.send(
            w,
            3,
            "session/prompt",
            {"sessionId": sid, "prompt": [{"type": "text", "text": "permission"}]},
        )
        await self.recv(r)
        request = await self.recv(r)
        await self.detached(w)
        state = await self.rpc({"op": "status"})
        self.assertEqual(state["pending"][0]["id"], request["id"])
        with self.assertRaises(RuntimeError):
            await self.rpc({"op": "approve", "id": request["id"], "option": "invalid"})
        r, w = await self.attach()
        await self.send(w, 1, "initialize", PROFILE)
        await self.result(r, 1)
        replay = await self.recv(r)
        self.assertEqual(replay, request)
        w.write(
            bridge.packet(
                {
                    "jsonrpc": "2.0",
                    "id": request["id"],
                    "result": {"outcome": {"outcome": "selected", "optionId": "yes"}},
                }
            )
        )
        await w.drain()

        async def finished():
            return not (await self.rpc({"op": "status"}))["active"]

        await self.until(finished)
        self.assertEqual((await self.rpc({"op": "status"}))["pending"], [])

    async def test_arbitrary_reverse_request_preserved(self):
        r, w, sid, _ = await self.ready()
        await self.send(
            w,
            3,
            "session/prompt",
            {"sessionId": sid, "prompt": [{"type": "text", "text": "reverse"}]},
        )
        await self.recv(r)
        request = await self.recv(r)
        self.assertEqual(request["method"], "fs/read_text_file")
        self.assertTrue(request["_meta"]["roundtrip"])
        w.write(
            bridge.packet(
                {
                    "jsonrpc": "2.0",
                    "id": request["id"],
                    "result": {"content": "remote-content"},
                    "_meta": {"responseExtension": True},
                }
            )
        )
        await w.drain()
        reply = json.dumps(await self.recv(r))
        self.assertIn("remote-content", reply)
        self.assertIn("responseExtension", reply)
        self.assertIn("result", await self.result(r, 3))

    async def test_background_permission_survives_end_turn_cancel_and_reconnect(self):
        # Enable only for this isolated host; legacy hosts keep their previous behavior.
        await self.rpc({"op": "stop"})
        await self.process.wait()
        config = json.loads(self.folder.joinpath("config.json").read_text())
        config["preserve_background_requests"] = True
        self.folder.joinpath("config.json").write_text(json.dumps(config))
        await self.start()
        r, w, sid, _ = await self.ready()
        await self.send(
            w,
            3,
            "session/prompt",
            {
                "sessionId": sid,
                "prompt": [{"type": "text", "text": "background-permission"}],
            },
        )
        await self.recv(r)
        request = await self.recv(r)
        self.assertIn("result", await self.result(r, 3))
        await self.rpc({"op": "cancel", "sessionId": sid})
        await self.detached(w)
        state = await self.rpc({"op": "status"})
        self.assertEqual(state["active"], [])
        self.assertEqual(state["pending"][0]["id"], request["id"])
        r, w = await self.attach()
        await self.send(w, 1, "initialize", PROFILE)
        await self.result(r, 1)
        self.assertEqual(await self.recv(r), request)
        await self.detached(w)
        await self.rpc({"op": "approve", "id": request["id"]})
        self.assertEqual((await self.rpc({"op": "status"}))["pending"], [])

    async def test_exclusive_controller_and_incompatible_profile(self):
        r, w, _, _ = await self.ready()
        with self.assertRaises(RuntimeError):
            await self.attach()
        await self.detached(w)
        r, w = await self.attach()
        await self.send(
            w, 1, "initialize", {"protocolVersion": 1, "clientCapabilities": {}}
        )
        self.assertIn("error", await self.result(r, 1))

    async def test_submit_idempotent_cancel_and_restart(self):
        msg = {
            "op": "submit",
            "id": "unique",
            "prompt": [{"type": "text", "text": "wait"}],
        }
        a = await self.rpc(msg)
        b = await self.rpc(msg)
        self.assertEqual(a["sessionId"], b["sessionId"])
        self.assertEqual(len((await self.rpc({"op": "status"}))["sessions"]), 1)
        with self.assertRaises(RuntimeError):
            await self.rpc(dict(msg, prompt=[{"type": "text", "text": "different"}]))
        await self.rpc({"op": "cancel", "sessionId": a["sessionId"]})

        async def finished():
            job = await self.rpc({"op": "job", "id": "unique"})
            return job if job["state"] == "completed" else None

        self.assertEqual(
            (await self.until(finished))["result"]["result"]["stopReason"], "cancelled"
        )
        await self.rpc(
            {
                "op": "submit",
                "id": "interrupted",
                "prompt": [{"type": "text", "text": "wait"}],
            }
        )
        await self.rpc({"op": "stop"})
        await self.process.wait()
        await self.start()
        self.assertEqual(
            (await self.rpc({"op": "job", "id": "interrupted"}))["state"], "interrupted"
        )
        self.assertEqual((await self.rpc({"op": "status"}))["active"], [])

    async def test_agent_failure_is_observable(self):
        await self.rpc(
            {
                "op": "submit",
                "id": "crash",
                "prompt": [{"type": "text", "text": "crash"}],
            }
        )

        async def failed():
            return (await self.rpc({"op": "status"}))["failure"]

        await self.until(failed)
        self.assertEqual(
            (await self.rpc({"op": "job", "id": "crash"}))["state"], "interrupted"
        )
        with self.assertRaises(RuntimeError):
            await self.rpc(
                {
                    "op": "submit",
                    "id": "new",
                    "prompt": [{"type": "text", "text": "hello"}],
                }
            )

    async def test_detached_permission_cancel(self):
        await self.rpc(
            {
                "op": "submit",
                "id": "permission",
                "prompt": [{"type": "text", "text": "permission"}],
            }
        )

        async def waiting():
            status = await self.rpc({"op": "status"})
            return status if status["pending"] else None

        status = await self.until(waiting)
        await self.rpc({"op": "cancel", "sessionId": status["active"][0]["sessionId"]})

        async def finished():
            return not (await self.rpc({"op": "status"}))["active"]

        await self.until(finished)
        self.assertEqual((await self.rpc({"op": "status"}))["pending"], [])

    async def test_active_load_rejected_without_cancelling(self):
        r, w, sid, _ = await self.ready()
        await self.send(
            w,
            3,
            "session/prompt",
            {"sessionId": sid, "prompt": [{"type": "text", "text": "wait"}]},
        )
        await self.recv(r)
        await self.send(
            w,
            4,
            "session/load",
            {"sessionId": sid, "cwd": self.temp.name, "mcpServers": []},
        )
        self.assertIn("error", await self.result(r, 4))
        self.assertTrue((await self.rpc({"op": "status"}))["active"])
        await self.rpc({"op": "cancel", "sessionId": sid})
        self.assertEqual((await self.result(r, 3))["result"]["stopReason"], "cancelled")

    async def test_private_socket(self):
        self.assertEqual(self.sock.stat().st_mode & 0o777, 0o600)
        self.assertEqual(self.sock.parent.stat().st_mode & 0o777, 0o700)

    async def test_late_response_never_reuses_new_connection_id(self):
        r, w, sid, _ = await self.ready()
        await self.send(
            w,
            3,
            "session/prompt",
            {"sessionId": sid, "prompt": [{"type": "text", "text": "delayed"}]},
        )
        await self.recv(r)
        await self.detached(w)
        r, w = await self.attach()
        await self.send(w, 1, "initialize", PROFILE)
        await self.result(r, 1)
        await self.send(w, 3, "custom/check", {"value": "new-request"})
        self.assertEqual(
            (await self.result(r, 3))["result"]["echo"]["params"]["value"],
            "new-request",
        )
        self.assertEqual((await self.recv(r))["method"], "session/update")
        with self.assertRaises(asyncio.TimeoutError):
            await asyncio.wait_for(r.readline(), 0.15)

    async def test_oversized_slow_peer_disconnects(self):
        class Writer:
            def __init__(self):
                self.closed = False

            def is_closing(self):
                return self.closed

            def close(self):
                self.closed = True

        writer = Writer()
        peer = bridge.Peer(writer)
        peer.send({"payload": "x" * (33 * 1024 * 1024)})
        self.assertTrue(writer.closed)
        peer.close()


class StoreTests(unittest.TestCase):
    def test_event_quota_and_gap(self):
        with tempfile.TemporaryDirectory() as d:
            s = bridge.Store(Path(d), 2048)
            for _ in range(200):
                s.event("test", {"value": "x" * 100})
            self.assertLessEqual(s.bytes, 2048)
            self.assertTrue(s.events(1)["gap"])
            s.db.close()


if __name__ == "__main__":
    unittest.main()
