# SPDX-License-Identifier: Apache-2.0
import asyncio
import importlib.util
import json
from pathlib import Path
import sys
import tempfile
from types import SimpleNamespace
import unittest

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT / "swarm"))
from native_store import NativeStore
from server import Adapter, PREFIX, META


class StoreTests(unittest.TestCase):
    def setUp(self):
        self.tmp = tempfile.TemporaryDirectory()
        self.root = Path(self.tmp.name)
        self.store = NativeStore(self.root)
        self.folder = self.root / "brain/parent/.system_generated/swarm"
        self.folder.mkdir(parents=True)
        self.roster = {
            "title": "Test",
            "members": {
                "coordinator": {"conversation_id": "parent"},
                "a": {
                    "conversation_id": "child",
                    "phase": "RUNNING",
                    "wake_mode": "mentions",
                },
            },
        }
        (self.folder / "members.json").write_text(json.dumps(self.roster))
        (self.folder / "swarm.json").write_text('{"backend":"local","version":1}')
        (self.folder / "board.jsonl").write_text(
            '{"seq":1,"agent":"coordinator","text":"@a task"}\n{"seq":2,"agent":"a","text":"result"}\n'
        )
        self.log = self.root / "brain/child/.system_generated/logs/transcript.jsonl"
        self.log.parent.mkdir(parents=True)
        self.log.write_text(
            '{"step_index":0,"content":"hello"}\n{"step_index":1,"content":"done"}\n'
        )

    def tearDown(self):
        self.tmp.cleanup()

    def test_roster_provenance_paging_and_partial_board(self):
        snap = self.store.snapshot("parent", limit=1)
        self.assertFalse(snap["runtimeVerified"])
        self.assertTrue(snap["more"])
        self.assertEqual(snap["cursor"], 1)
        self.assertEqual(snap["members"][1]["parentSessionId"], "parent")
        self.assertEqual(
            self.store.snapshot("parent", after=1)["messages"][0]["text"], "result"
        )
        with (self.folder / "board.jsonl").open("a") as f:
            f.write('{"seq":3')
        self.assertTrue(self.store.snapshot("parent")["partialTail"])
        self.assertTrue(self.store.snapshot("parent", after=10)["resetRequired"])

    def test_history_cursor_and_rewrite_reset(self):
        first = self.store.history("parent", "a", limit=1)
        second = self.store.history("parent", "a", cursor=first["cursor"])
        self.assertEqual(second["records"][0]["step_index"], 1)
        self.log.write_text('{"step_index":0,"content":"rewritten"}\n')
        self.assertTrue(
            self.store.history("parent", "a", cursor=first["cursor"])["resetRequired"]
        )

    def test_paths_unknown_member_and_remote_storage_rejected(self):
        for sid in ("../secret", "a/b", ""):
            with self.assertRaises(ValueError):
                self.store.snapshot(sid)
        with self.assertRaises(ValueError):
            self.store.history("parent", "not-a-member")
        (self.folder / "swarm.json").write_text('{"backend":"spanner"}')
        with self.assertRaises(ValueError):
            self.store.snapshot("parent")

    def test_invalid_roster_not_reported_as_empty_or_completed(self):
        (self.folder / "members.json").write_text("{")
        with self.assertRaises(ValueError):
            self.store.snapshot("parent")

    def test_symlink_outside_root_rejected(self):
        self.log.unlink()
        self.log.symlink_to("/etc/passwd")
        with self.assertRaises(ValueError):
            self.store.history("parent", "a")

    def test_coordinator_history_before_first_board(self):
        log = self.root / "brain/ordinary/.system_generated/logs/transcript.jsonl"
        log.parent.mkdir(parents=True)
        log.write_text('{"step_index":0,"content":"original parent"}\n')
        result = self.store.history("ordinary", "coordinator")
        self.assertEqual(result["records"][0]["content"], "original parent")
        self.assertEqual(result["source"], "native-transcript")
        with self.assertRaises(ValueError):
            self.store.history("ordinary", "a")


class Sink:
    def __init__(self):
        self.messages = []

    def write(self, data):
        self.messages.append(json.loads(data))

    async def drain(self):
        pass


class AdapterTests(unittest.IsolatedAsyncioTestCase):
    async def asyncSetUp(self):
        self.tmp = tempfile.TemporaryDirectory()
        self.out = []

        async def emit(msg):
            self.out.append(msg)

        self.proc = SimpleNamespace(returncode=None, stdin=Sink())
        self.a = Adapter(self.proc, emit, self.tmp.name, 0.02)
        await self.a.client(
            {
                "jsonrpc": "2.0",
                "id": 1,
                "method": "initialize",
                "params": {"protocolVersion": 1, "clientCapabilities": {}},
            }
        )
        await self.a.native(
            {
                "jsonrpc": "2.0",
                "id": 1,
                "result": {
                    "protocolVersion": 1,
                    "agentCapabilities": {"_meta": {"existing": True}},
                },
            }
        )
        await self.a.client(
            {
                "jsonrpc": "2.0",
                "id": 2,
                "method": "session/new",
                "params": {"cwd": self.tmp.name, "mcpServers": []},
            }
        )
        await self.a.native(
            {"jsonrpc": "2.0", "id": 2, "result": {"sessionId": "parent"}}
        )

    async def asyncTearDown(self):
        self.tmp.cleanup()

    async def test_metadata_passthrough_and_no_fabricated_workers(self):
        self.assertTrue(self.out[0]["result"]["agentCapabilities"]["_meta"]["existing"])
        self.assertFalse(
            self.out[0]["result"]["agentCapabilities"]["_meta"][META][
                "directWorkerControl"
            ]
        )
        event = {
            "jsonrpc": "2.0",
            "method": "session/update",
            "params": {
                "sessionId": "parent",
                "update": {
                    "sessionUpdate": "tool_call",
                    "toolCallId": "native1",
                    "_meta": {"unknown": 1},
                },
            },
        }
        await self.a.native(event)
        self.assertEqual(self.out[-1], event)

    async def test_permission_survives_parent_completion_and_cancel(self):
        await self.a.client(
            {
                "jsonrpc": "2.0",
                "id": 3,
                "method": "session/prompt",
                "params": {"sessionId": "parent", "prompt": []},
            }
        )
        req = {
            "jsonrpc": "2.0",
            "id": "approval",
            "method": "session/request_permission",
            "params": {
                "sessionId": "parent",
                "toolCall": {"toolCallId": "worker-tool"},
                "options": [{"optionId": "yes", "kind": "allow_once", "name": "Allow"}],
            },
        }
        await self.a.native(req)
        await self.a.native(
            {"jsonrpc": "2.0", "id": 3, "result": {"stopReason": "end_turn"}}
        )
        await self.a.client(
            {
                "jsonrpc": "2.0",
                "method": "session/cancel",
                "params": {"sessionId": "parent"},
            }
        )
        self.assertIn("approval", self.a.pending)
        await self.a.client(
            {
                "jsonrpc": "2.0",
                "id": "approval",
                "result": {"outcome": {"outcome": "selected", "optionId": "bad"}},
            }
        )
        self.assertIn("approval", self.a.pending)
        await self.a.client(
            {
                "jsonrpc": "2.0",
                "id": "approval",
                "result": {"outcome": {"outcome": "cancelled"}},
            }
        )
        self.assertNotIn("approval", self.a.pending)
        self.assertEqual(
            self.proc.stdin.messages[-1]["result"]["outcome"]["outcome"], "cancelled"
        )

    async def test_busy_load_and_duplicate_prompt_are_rejected(self):
        await self.a.client(
            {
                "jsonrpc": "2.0",
                "id": 3,
                "method": "session/prompt",
                "params": {"sessionId": "parent", "prompt": []},
            }
        )
        for method in ("session/prompt", "session/load"):
            await self.a.client(
                {
                    "jsonrpc": "2.0",
                    "id": 4,
                    "method": method,
                    "params": {"sessionId": "parent"},
                }
            )
            self.assertIn("error", self.out[-1])
        self.assertIn("parent", self.a.busy)

    async def test_extension_scope_subscription_and_unknown_method(self):
        await self.a.client(
            {
                "jsonrpc": "2.0",
                "id": 4,
                "method": PREFIX + "snapshot",
                "params": {"sessionId": "foreign"},
            }
        )
        self.assertIn("error", self.out[-1])
        await self.a.client(
            {
                "jsonrpc": "2.0",
                "id": 5,
                "method": PREFIX + "subscribe",
                "params": {"sessionId": "parent"},
            }
        )
        watch = asyncio.create_task(self.a.watch())
        try:
            await asyncio.sleep(0.06)
            updates = [m for m in self.out if m.get("method") == PREFIX + "update"]
            self.assertEqual(len(updates), 1)
            self.assertIsNone(updates[0]["params"]["swarm"])
            # A new controller must resubscribe even if native files have not changed.
            await self.a.client(
                {
                    "jsonrpc": "2.0",
                    "id": 7,
                    "method": PREFIX + "subscribe",
                    "params": {"sessionId": "parent"},
                }
            )
            await asyncio.sleep(0.06)
            self.assertEqual(
                len([m for m in self.out if m.get("method") == PREFIX + "update"]), 2
            )
        finally:
            watch.cancel()
            with self.assertRaises(asyncio.CancelledError):
                await watch
        await self.a.client(
            {
                "jsonrpc": "2.0",
                "id": 6,
                "method": PREFIX + "unsupported",
                "params": {"sessionId": "parent"},
            }
        )
        self.assertEqual(self.out[-1]["error"]["code"], -32601)

    async def test_attach_is_read_only_while_prompt_and_approval_are_active(self):
        options = [
            {
                "id": "thought_level",
                "currentValue": "medium",
                "options": [{"value": "medium", "name": "Medium"}],
            }
        ]
        self.a.observe_session(
            "parent",
            {
                "configOptions": options,
                "models": {
                    "currentModelId": "native-id",
                    "availableModels": [
                        {"modelId": "native-id", "name": "Native name"}
                    ],
                },
            },
            "session/new:result",
        )
        self.a.busy.add("parent")
        self.a.pending["permission"] = {
            "method": "session/request_permission",
            "params": {"sessionId": "parent"},
        }
        writes = len(self.proc.stdin.messages)
        result = await self.a.extension(PREFIX + "attach", {"sessionId": "parent"})
        self.assertEqual(len(self.proc.stdin.messages), writes)
        self.assertEqual(result["metadata"]["nativeSession"]["configOptions"], options)
        self.assertTrue(result["snapshot"]["coordinatorPromptActive"])
        self.assertIn("permission", self.a.pending)
        self.assertFalse(result["snapshot"]["pendingRequests"][0]["actionable"])
        self.assertTrue(result["history"]["notAcpReplay"])
        self.assertEqual(result["history"]["params"]["member"], "coordinator")
        self.assertEqual(result["permissionReplay"], "transport-initialize-only")

    async def test_native_config_cache_never_invents_requested_choices(self):
        self.a.observe_session(
            "parent",
            {
                "models": {"currentModelId": "old-native"},
                "configOptions": [{"id": "thought_level", "currentValue": "medium"}],
            },
            "session/new:result",
        )
        await self.a.client(
            {
                "jsonrpc": "2.0",
                "id": 8,
                "method": "session/set_config_option",
                "params": {
                    "sessionId": "parent",
                    "configId": "thought_level",
                    "value": "imaginary",
                },
            }
        )
        result = await self.a.extension(PREFIX + "attach", {"sessionId": "parent"})
        self.assertEqual(
            result["metadata"]["nativeSession"]["configOptions"][0]["currentValue"],
            "medium",
        )
        self.assertIn("configOptions", result["metadata"]["staleFields"])
        await self.a.native(
            {
                "jsonrpc": "2.0",
                "id": 8,
                "error": {"code": -32602, "message": "not offered"},
            }
        )
        await self.a.native(
            {
                "jsonrpc": "2.0",
                "method": "session/update",
                "params": {
                    "sessionId": "parent",
                    "update": {
                        "sessionUpdate": "config_option_update",
                        "configOptions": [
                            {
                                "id": "thought_level",
                                "currentValue": "high",
                                "_meta": {"fromNative": True},
                            }
                        ],
                    },
                },
            }
        )
        result = await self.a.extension(PREFIX + "attach", {"sessionId": "parent"})
        metadata = result["metadata"]
        self.assertEqual(
            metadata["nativeSession"]["configOptions"][0]["currentValue"], "high"
        )
        self.assertNotIn("configOptions", metadata["staleFields"])
        self.assertIn("models", metadata["staleFields"])
        self.assertEqual(
            metadata["fieldObservations"]["configOptions"]["source"],
            "session/update:config_option_update",
        )

    async def test_attach_uses_successful_native_config_result(self):
        await self.a.client(
            {
                "jsonrpc": "2.0",
                "id": 9,
                "method": "session/set_config_option",
                "params": {
                    "sessionId": "parent",
                    "configId": "model",
                    "value": "requested-model",
                },
            }
        )
        actual = [
            {
                "id": "model",
                "currentValue": "native-returned-model",
                "options": [{"value": "native-returned-model", "name": "Actual model"}],
            }
        ]
        await self.a.native(
            {"jsonrpc": "2.0", "id": 9, "result": {"configOptions": actual}}
        )
        result = await self.a.extension(PREFIX + "attach", {"sessionId": "parent"})
        self.assertEqual(result["metadata"]["nativeSession"]["configOptions"], actual)
        self.assertNotIn("models", result["metadata"]["nativeSession"])

    async def test_attach_unknown_session_close_and_missing_metadata(self):
        with self.assertRaises(ValueError):
            await self.a.extension(PREFIX + "attach", {"sessionId": "not-open"})
        result = await self.a.extension(PREFIX + "attach", {"sessionId": "parent"})
        self.assertEqual(result["metadata"]["nativeSession"], {})
        sessions = await self.a.extension(PREFIX + "sessions", {})
        self.assertEqual(sessions["sessions"][0]["sessionId"], "parent")
        await self.a.client(
            {
                "jsonrpc": "2.0",
                "id": 10,
                "method": "session/close",
                "params": {"sessionId": "parent"},
            }
        )
        await self.a.native({"jsonrpc": "2.0", "id": 10, "result": {}})
        with self.assertRaises(ValueError):
            await self.a.extension(PREFIX + "attach", {"sessionId": "parent"})

    async def test_attach_keeps_metadata_when_board_is_partially_written(self):
        folder = Path(self.tmp.name) / "brain/parent/.system_generated/swarm"
        folder.mkdir(parents=True)
        (folder / "members.json").write_text("{")
        self.a.observe_session("parent", {"configOptions": []}, "session/new:result")
        result = await self.a.extension(PREFIX + "attach", {"sessionId": "parent"})
        self.assertIsNone(result["snapshot"])
        self.assertTrue(result["snapshotError"])
        self.assertEqual(result["metadata"]["nativeSession"]["configOptions"], [])


if __name__ == "__main__":
    unittest.main()
