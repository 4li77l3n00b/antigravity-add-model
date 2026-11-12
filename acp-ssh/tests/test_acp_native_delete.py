# SPDX-License-Identifier: Apache-2.0
import asyncio
import contextlib
import importlib.util
import json
import os
from pathlib import Path
import sqlite3
import sys
import tempfile
import unittest
from types import SimpleNamespace
from unittest.mock import patch

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT / "bridge"))
import acp_ssh as bridge
from native_delete import NativeDelete, DeletionError, valid_id, METHOD, META

SID = "11111111-1111-4111-8111-111111111111"
OTHER = "22222222-2222-4222-8222-222222222222"
SCHEMA = """CREATE TABLE conversation_summaries (
conversation_id text PRIMARY KEY,parent_conversation_id text DEFAULT '',group_id text DEFAULT '',
battle_id text DEFAULT '',winning_conversation_id text DEFAULT '',nesting_depth integer DEFAULT 0,
not_fully_idle integer DEFAULT 0)"""


class NativeStoreTests(unittest.TestCase):
    def setUp(self):
        self.temp = tempfile.TemporaryDirectory()
        self.home = Path(self.temp.name)
        self.n = NativeDelete.__new__(NativeDelete)
        self.n.home = self.home
        self.n.app = "test"
        self.n.root = self.home / ".gemini/test"
        self.n.binary = Path("/nonexistent")
        self.n.root.mkdir(parents=True)
        (self.n.root / "conversations").mkdir()
        self.file = self.n.root / "conversations" / f"{SID}.db"
        self.file.write_bytes(b"fixture")

    def tearDown(self):
        self.temp.cleanup()

    def summary(self, sql, args):
        with contextlib.closing(
            sqlite3.connect(self.n.root / "conversation_summaries.db")
        ) as db:
            db.execute(SCHEMA)
            db.execute(sql, args)
            db.commit()

    def test_uuid_and_path_checks(self):
        for value in ("../escape", SID.upper().replace("1", "A", 1), "", None):
            with self.assertRaises(DeletionError):
                valid_id(value)
        self.file.unlink()
        self.file.symlink_to("/etc/passwd")
        with self.assertRaises(DeletionError) as e:
            self.n.preflight(SID)
        self.assertEqual(e.exception.data["reason"], "UNSAFE_PATH")

    def test_absence_and_residual_data(self):
        self.assertTrue(self.n.preflight(SID))
        with self.assertRaises(DeletionError):
            self.n.verify_absent(SID)
        self.file.unlink()
        self.n.verify_absent(SID)

    def test_legacy_only_summary_requires_real_cleanup(self):
        self.file.unlink()
        (self.n.root / "jetbox_summaries_proto.pb").write_bytes(
            b"fixture" + SID.encode()
        )
        self.assertTrue(self.n.preflight(SID))
        with self.assertRaises(DeletionError):
            self.n.verify_absent(SID)

    def test_parent_and_child_references_rejected(self):
        self.summary(
            "INSERT INTO conversation_summaries(conversation_id,parent_conversation_id) VALUES(?,?)",
            (OTHER, SID),
        )
        with self.assertRaises(DeletionError) as e:
            self.n.preflight(SID)
        self.assertEqual(e.exception.data["reason"], "RELATED_SESSIONS")

    def test_group_member_rejected(self):
        self.summary(
            "INSERT INTO conversation_summaries(conversation_id,group_id) VALUES(?,?)",
            (SID, OTHER),
        )
        with self.assertRaises(DeletionError):
            self.n.preflight(SID)

    def test_swarm_roster_rejected(self):
        folder = self.n.root / "brain" / OTHER / ".system_generated/swarm"
        folder.mkdir(parents=True)
        (folder / "members.json").write_text(
            json.dumps({"members": {"worker": {"conversation_id": SID}}})
        )
        with self.assertRaises(DeletionError) as e:
            self.n.preflight(SID)
        self.assertEqual(e.exception.data["reason"], "RELATED_SESSIONS")

    def test_unknown_swarm_schema_rejected(self):
        folder = self.n.root / "brain" / OTHER / ".system_generated/swarm"
        folder.mkdir(parents=True)
        (folder / "members.json").write_text("{")
        with self.assertRaises(DeletionError):
            self.n.preflight(SID)

    def test_other_native_root_not_reported_deleted(self):
        self.file.unlink()
        other = self.home / ".gemini/other/conversations"
        other.mkdir(parents=True)
        (other / f"{SID}.db").write_bytes(b"other")
        with self.assertRaises(DeletionError) as e:
            self.n.preflight(SID)
        self.assertEqual(e.exception.data["reason"], "OUT_OF_SCOPE")

    def test_native_busy_rejected(self):
        self.summary(
            "INSERT INTO conversation_summaries(conversation_id,not_fully_idle) VALUES(?,1)",
            (SID,),
        )
        with self.assertRaises(DeletionError) as e:
            self.n.preflight(SID)
        self.assertEqual(e.exception.data["reason"], "NATIVE_BUSY")

    def test_open_fd_rejected(self):
        with self.file.open("rb"):
            with self.assertRaises(DeletionError) as e:
                self.n.assert_no_writer(SID)
        self.assertEqual(e.exception.data["reason"], "EXTERNAL_WRITER")

    def test_unverified_native_binary_rejected(self):
        binary = self.home / "agy"
        binary.write_bytes(b"fake")
        with self.assertRaises(DeletionError):
            NativeDelete(
                {"home": str(self.home), "binary": str(binary), "app_data_name": "test"}
            )


class Engine:
    def __init__(self):
        self.exists = True
        self.fail = False
        self.started = asyncio.Event()
        self.release = None

    def preflight(self, sid):
        return self.exists

    def capability(self):
        return {"version": 1, "delete": {"method": METHOD, "permanent": True}}

    def assert_no_writer(self, sid):
        pass

    def verify_absent(self, sid):
        if self.exists:
            raise DeletionError("DELETE_INCOMPLETE", "still exists")

    async def execute(self, sid, owner_pid):
        self.started.set()
        if self.release:
            await self.release.wait()
        if self.fail:
            raise DeletionError(
                "NATIVE_DELETE_FAILED", "fixture failure", uncertain=True
            )
        self.exists = False


class DeleteHostTests(unittest.IsolatedAsyncioTestCase):
    async def asyncSetUp(self):
        self.temp = tempfile.TemporaryDirectory()
        self.folder = Path(self.temp.name)
        self.h = bridge.Host(
            "test",
            self.folder,
            self.folder / "unused.sock",
            {"cwd": self.temp.name, "command": []},
        )
        self.h.proc = SimpleNamespace(pid=os.getpid())
        self.h.native_delete = Engine()
        self.h.delete_supported = True
        self.calls = []

        async def call(method, params):
            self.calls.append(method)
            return {"result": {}}

        self.h.call = call

    async def asyncTearDown(self):
        self.h.store.db.close()
        self.temp.cleanup()

    async def test_capability_gate_and_no_notification_delete(self):
        self.h.delete_supported = False
        with self.assertRaises(DeletionError) as e:
            await self.h.delete_session({"sessionId": SID})
        self.assertEqual(e.exception.code, -32601)
        self.h.delete_supported = True
        sent = []
        peer = SimpleNamespace(ready=True, send=sent.append, inflight=set())
        await self.h.acp(
            peer, {"jsonrpc": "2.0", "method": METHOD, "params": {"sessionId": SID}}
        )
        self.assertTrue(self.h.native_delete.exists)
        self.assertFalse(self.calls)

    async def test_initialize_capability_is_explicit_and_preserves_native_fields(self):
        native = {
            "agentInfo": {"version": "1.5.5"},
            "agentCapabilities": {"_meta": {"original": {"unknown": [1, 2]}}},
        }

        async def call(*args, **kwargs):
            return {"result": native}

        self.h.call = call
        result = await self.h.initialize({"protocolVersion": 1})
        self.assertTrue(
            result["agentCapabilities"]["_meta"][META]["delete"]["permanent"]
        )
        self.assertNotIn(META, native["agentCapabilities"]["_meta"])
        self.assertEqual(
            result["agentCapabilities"]["_meta"]["original"], {"unknown": [1, 2]}
        )

    async def test_busy_and_pending_reject_without_close(self):
        self.h.store.new_job("task", {"sessionId": SID, "prompt": []})
        with self.assertRaises(DeletionError) as e:
            await self.h.delete_session({"sessionId": SID})
        self.assertEqual(e.exception.data["reason"], "SESSION_BUSY")
        self.assertFalse(self.calls)
        self.h.store.finish("task", "completed")
        self.h.pending["p"] = ("native", {"params": {"sessionId": SID}})
        with self.assertRaises(DeletionError) as e:
            await self.h.delete_session({"sessionId": SID})
        self.assertEqual(e.exception.data["reason"], "PENDING_CLIENT_REQUEST")
        self.assertFalse(self.calls)

    async def test_failure_keeps_history_and_persists_unresolved(self):
        self.h.store.event("client", {"sessionId": SID, "content": "keep me"})
        self.h.native_delete.fail = True
        with self.assertRaises(DeletionError):
            await self.h.delete_session({"sessionId": SID})
        self.assertEqual(self.h.store.deletion(SID), "pending")
        self.assertEqual(len(self.h.store.events(0)["events"]), 1)
        self.assertTrue(self.h.native_delete.exists)

    async def test_success_purges_copies_and_retry_confirms_absence(self):
        self.h.store.event("client", {"sessionId": SID, "content": "secret"})
        self.h.store.event("agent", {"sessionId": OTHER, "content": "survivor"})
        self.h.store.new_job("task", {"sessionId": SID, "prompt": [{"text": "secret"}]})
        self.h.store.finish("task", "completed")
        r = await self.h.delete_session({"sessionId": SID})
        self.assertEqual(
            r,
            {
                "deleted": True,
                "sessionId": SID,
                "deletedSessionIds": [SID],
                "alreadyDeleted": False,
            },
        )
        self.assertIsNone(self.h.store.job("task"))
        with self.assertRaisesRegex(ValueError, "deleted session history"):
            await self.h.control(
                {"op": "submit", "id": "task", "prompt": [{"text": "secret"}]}
            )
        self.assertEqual(len(self.h.store.events(0)["events"]), 1)
        self.assertEqual(self.h.store.deletion(SID), "complete")
        self.assertTrue(
            (await self.h.delete_session({"sessionId": SID}))["alreadyDeleted"]
        )

    async def test_parallel_requests_cannot_resurrect_target(self):
        self.h.native_delete.release = asyncio.Event()
        task = asyncio.create_task(self.h.delete_session({"sessionId": SID}))
        await asyncio.wait_for(self.h.native_delete.started.wait(), 2)
        sent = []
        peer = SimpleNamespace(ready=True, send=sent.append, inflight=set())
        self.h.peer = peer
        await self.h.acp(
            peer,
            {
                "jsonrpc": "2.0",
                "id": 2,
                "method": "session/load",
                "params": {"sessionId": SID, "cwd": self.temp.name},
            },
        )
        self.assertEqual(sent[-1]["error"]["code"], -32051)
        await self.h.acp(
            peer,
            {
                "jsonrpc": "2.0",
                "id": 9,
                "method": "session/new",
                "params": {"cwd": self.temp.name},
            },
        )
        self.assertEqual(sent[-1]["error"]["data"]["reason"], "DELETE_IN_PROGRESS")
        self.h.native_delete.release.set()
        await task
        await self.h.acp(
            peer,
            {
                "jsonrpc": "2.0",
                "id": 3,
                "method": "session/prompt",
                "params": {"sessionId": SID, "prompt": []},
            },
        )
        self.assertEqual(sent[-1]["error"]["code"], -32051)
        self.assertEqual(self.calls, ["session/close"])

    async def test_crash_journal_survives_reopen(self):
        self.h.store.mark_deletion(SID, "pending")
        self.h.store.db.close()
        self.h.store = bridge.Store(self.folder, 64 * 1024 * 1024)
        self.assertEqual(self.h.store.deletion(SID), "pending")

    async def test_force_and_noncanonical_id_rejected(self):
        for params in ({"sessionId": SID, "force": True}, {"sessionId": "../x"}, []):
            with self.assertRaises(DeletionError):
                await self.h.delete_session(params)
        self.assertFalse(self.calls)


class EnableConfigTests(unittest.TestCase):
    def setUp(self):
        self.temp = tempfile.TemporaryDirectory()
        self.env = patch.dict(os.environ, {"ACP_SSH_HOME": self.temp.name})
        self.env.start()
        self.folder = Path(self.temp.name) / "example"
        self.folder.mkdir(mode=0o700)
        self.original = {
            "cwd": "/tmp",
            "command": ["custom", "--option"],
            "unknown": {"retain": [1, 2]},
            "preserve_background_requests": True,
        }
        self.path = self.folder / "config.json"
        self.path.write_text(json.dumps(self.original))

    def tearDown(self):
        self.env.stop()
        self.temp.cleanup()

    def test_preview_and_apply_preserve_every_existing_setting(self):
        import enable_native_delete as installer

        with patch.object(installer, "NativeDelete"):
            self.assertFalse(
                installer.enable("example", "test", "/tmp/mock")["applied"]
            )
            self.assertEqual(json.loads(self.path.read_text()), self.original)
            out = installer.enable("example", "test", "/tmp/mock", True)
            self.assertTrue(out["applied"])
            self.assertEqual(json.loads(Path(out["backup"]).read_text()), self.original)
            actual = json.loads(self.path.read_text())
            actual.pop("native_delete")
            self.assertEqual(actual, self.original)

    def test_running_host_refuses_configuration_change(self):
        import enable_native_delete as installer
        import fcntl

        with (self.folder / "host.lock").open("a") as lock:
            fcntl.flock(lock, fcntl.LOCK_EX | fcntl.LOCK_NB)
            with patch.object(installer, "NativeDelete"):
                with self.assertRaisesRegex(RuntimeError, "Host is running"):
                    installer.enable("example", "test", "/tmp/mock", True)
        self.assertEqual(json.loads(self.path.read_text()), self.original)


if __name__ == "__main__":
    unittest.main()
