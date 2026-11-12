#!/usr/bin/env python3
# SPDX-License-Identifier: Apache-2.0
"""Persistent ACP stdio host, reached over a private Unix socket / SSH.

Python 3.10+, Linux, standard library only. Never retries agent operations.
"""

import argparse
import asyncio
import contextlib
import copy
import fcntl
import hashlib
import json
import os
from pathlib import Path
import re
import signal
import socket
import sqlite3
import stat
import struct
import subprocess
import sys
import time
import uuid

sys.path.insert(0, str(Path(__file__).resolve().parent))
from native_delete import (
    NativeDelete,
    DeletionError,
    valid_id,
    METHOD as DELETE_METHOD,
    META as DELETE_META,
)

VERSION = "0.1.0"
MAX_LINE = 16 * 1024 * 1024
TERMINAL = ("completed", "failed", "interrupted")


def dumps(value):
    return json.dumps(value, ensure_ascii=False, separators=(",", ":"))


def packet(value):
    return (dumps(value) + "\n").encode()


def paths(name):
    if not re.fullmatch(r"[A-Za-z0-9][A-Za-z0-9_-]{0,39}", name):
        raise ValueError("Name must be 1–40 ASCII letters, digits, _ or -")
    root = Path(
        os.environ.get("ACP_SSH_HOME", Path.home() / ".local/state/acp-ssh")
    ).absolute()
    folder = root / name
    # Socket length stays below sockaddr_un limits even with a long state path.
    tag = hashlib.sha256(str(root).encode()).hexdigest()[:12]
    runtime = Path("/tmp") / f"acp-ssh-{os.getuid()}-{tag}"
    return folder, runtime / (name + ".sock")


def private_dir(path):
    path.mkdir(parents=True, mode=0o700, exist_ok=True)
    st = path.lstat()
    if not stat.S_ISDIR(st.st_mode) or st.st_uid != os.getuid() or st.st_mode & 0o077:
        raise ValueError(f"Expected an owned, private directory (mode 700): {path}")


def error(ident, message, code=-32000):
    return {"jsonrpc": "2.0", "id": ident, "error": {"code": code, "message": message}}


def response(ident, result):
    return {"jsonrpc": "2.0", "id": ident, "result": result}


class Store:
    def __init__(self, folder, quota):
        self.db = sqlite3.connect(folder / "state.sqlite3")
        self.db.execute("PRAGMA journal_mode=WAL")
        self.db.execute("PRAGMA synchronous=FULL")
        self.db.executescript("""
          CREATE TABLE IF NOT EXISTS events(seq INTEGER PRIMARY KEY AUTOINCREMENT, time REAL, body TEXT, size INTEGER);
          CREATE TABLE IF NOT EXISTS jobs(id TEXT PRIMARY KEY, session TEXT, state TEXT, request TEXT, result TEXT, updated REAL);
          CREATE TABLE IF NOT EXISTS sessions(id TEXT PRIMARY KEY, cwd TEXT, info TEXT);
          CREATE TABLE IF NOT EXISTS deletions(id TEXT PRIMARY KEY, state TEXT NOT NULL, updated REAL NOT NULL);
          CREATE TABLE IF NOT EXISTS deleted_jobs(id TEXT PRIMARY KEY, session TEXT NOT NULL);
        """)
        self.quota = quota
        self.bytes = self.db.execute(
            "SELECT coalesce(sum(size),0) FROM events"
        ).fetchone()[0]
        self.db.execute(
            "UPDATE jobs SET state='interrupted', updated=? WHERE state NOT IN ('completed','failed','interrupted')",
            (time.time(),),
        )
        self.db.commit()

    def event(self, kind, value):
        body = dumps({"kind": kind, "data": value})
        size = len(body.encode())
        cur = self.db.execute(
            "INSERT INTO events(time,body,size) VALUES(?,?,?)",
            (time.time(), body, size),
        )
        seq = cur.lastrowid
        self.bytes += size
        while self.bytes > self.quota:
            rows = self.db.execute(
                "SELECT seq,size FROM events ORDER BY seq LIMIT 128"
            ).fetchall()
            if not rows:
                break
            self.db.execute("DELETE FROM events WHERE seq<=?", (rows[-1][0],))
            self.bytes -= sum(x[1] for x in rows)
        self.db.commit()
        return seq

    def events(self, after, limit=100):
        first = self.db.execute("SELECT min(seq) FROM events").fetchone()[0]
        rows = self.db.execute(
            "SELECT seq,time,body FROM events WHERE seq>? ORDER BY seq LIMIT ?",
            (after, limit),
        )
        bounded = []
        total = 0
        for row in rows:
            if bounded and total + len(row[2]) > 4 * 1024 * 1024:
                break
            bounded.append(row)
            total += len(row[2])
        rows = bounded
        return {
            "oldest": first,
            "gap": first is not None and after < first - 1,
            "more": bool(rows)
            and self.db.execute(
                "SELECT 1 FROM events WHERE seq>? LIMIT 1", (rows[-1][0],)
            ).fetchone()
            is not None,
            "events": [dict(seq=s, time=t, **json.loads(b)) for s, t, b in rows],
        }

    def job(self, ident):
        row = self.db.execute(
            "SELECT id,session,state,request,result,updated FROM jobs WHERE id=?",
            (ident,),
        ).fetchone()
        if row is None:
            return None
        return dict(
            zip(
                ("id", "sessionId", "state", "request", "result", "updated"),
                [
                    *row[:3],
                    json.loads(row[3]),
                    json.loads(row[4]) if row[4] else None,
                    row[5],
                ],
            )
        )

    def new_job(self, ident, params):
        self.db.execute(
            "INSERT INTO jobs VALUES(?,?,?,?,?,?)",
            (ident, params["sessionId"], "accepted", dumps(params), None, time.time()),
        )
        self.db.commit()

    def finish(self, ident, state, result=None):
        self.db.execute(
            "UPDATE jobs SET state=?,result=?,updated=? WHERE id=?",
            (state, dumps(result), time.time(), ident),
        )
        self.db.commit()

    def active(self):
        return [
            dict(id=a, sessionId=b, state=c)
            for a, b, c in self.db.execute(
                "SELECT id,session,state FROM jobs WHERE state NOT IN ('completed','failed','interrupted')"
            )
        ]

    def deletion(self, sid):
        row = self.db.execute(
            "SELECT state FROM deletions WHERE id=?", (sid,)
        ).fetchone()
        return row[0] if row else None

    def mark_deletion(self, sid, state):
        self.db.execute(
            "INSERT OR REPLACE INTO deletions VALUES(?,?,?)", (sid, state, time.time())
        )
        self.db.commit()

    def forget_session(self, sid):
        # Audit snapshots can contain multiple session titles. Remove the entire
        # matching snapshot rather than retaining the deleted session's text.
        with self.db:
            self.db.execute(
                "INSERT OR IGNORE INTO deleted_jobs SELECT id,session FROM jobs WHERE session=?",
                (sid,),
            )
            self.db.execute("DELETE FROM jobs WHERE session=?", (sid,))
            self.db.execute("DELETE FROM sessions WHERE id=?", (sid,))
            self.db.execute("DELETE FROM events WHERE instr(body,?)>0", (sid,))
            self.db.execute(
                "INSERT OR REPLACE INTO deletions VALUES(?,'complete',?)",
                (sid, time.time()),
            )
        self.bytes = self.db.execute(
            "SELECT coalesce(sum(size),0) FROM events"
        ).fetchone()[0]


class Peer:
    def __init__(self, writer):
        self.writer = writer
        self.queue = asyncio.Queue(maxsize=256)
        self.ready = False
        self.queued_bytes = 0
        self.inflight = set()
        self.task = asyncio.create_task(self.pump())

    def send(self, value):
        if self.writer.is_closing():
            return
        data = packet(value)
        if self.queued_bytes + len(data) > 32 * 1024 * 1024:
            self.writer.close()
            return
        try:
            self.queue.put_nowait(data)
            self.queued_bytes += len(data)
        except asyncio.QueueFull:
            self.writer.close()

    async def pump(self):
        try:
            while True:
                data = await self.queue.get()
                self.queued_bytes -= len(data)
                self.writer.write(data)
                await asyncio.wait_for(self.writer.drain(), 10)
        except (ConnectionError, asyncio.TimeoutError):
            self.writer.close()

    def close(self):
        self.writer.close()
        self.task.cancel()


class Host:
    def __init__(self, name, folder, sock, config):
        self.name, self.folder, self.sock, self.config = name, folder, sock, config
        self.store = Store(folder, config.get("event_bytes", 64 * 1024 * 1024))
        self.peer = None
        self.proc = None
        self.calls = {}
        self.pending = {}
        self.tasks = set()
        self.init_params = None
        self.init_result = None
        self.init_extra = {}
        self.init_lock = asyncio.Lock()
        self.submit_lock = asyncio.Lock()
        self.write_lock = asyncio.Lock()
        self.stopping = asyncio.Event()
        self.failure = None
        self.max_line = config.get("max_message_bytes", MAX_LINE)
        self.deleting = set()
        self.delete_lock = asyncio.Lock()
        self.native_delete = (
            NativeDelete(config["native_delete"])
            if config.get("native_delete")
            else None
        )
        self.delete_supported = False

    def task(self, coro):
        t = asyncio.create_task(coro)
        self.tasks.add(t)
        t.add_done_callback(self.tasks.discard)
        return t

    async def send(self, msg):
        if self.failure or self.proc.returncode is not None:
            raise RuntimeError(self.failure or "Agent exited")
        line = packet(msg)
        if len(line) > self.max_line:
            raise ValueError("ACP message exceeds configured size limit")
        async with self.write_lock:
            self.proc.stdin.write(line)
            await asyncio.wait_for(self.proc.stdin.drain(), 30)

    async def call(self, method, params, job=None, envelope=None):
        ident = "host-" + uuid.uuid4().hex
        fut = asyncio.get_running_loop().create_future()
        self.calls[ident] = (fut, method, params, job)
        try:
            if job:
                # Durable before writing. A crash here is uncertain, never retry automatically.
                self.store.finish(job, "running")
            await self.send(
                {
                    **(envelope or {}),
                    "jsonrpc": "2.0",
                    "id": ident,
                    "method": method,
                    "params": params,
                }
            )
            return await asyncio.shield(fut)
        except Exception as exc:
            self.calls.pop(ident, None)
            if not fut.done():
                fut.cancel()
            if job:
                self.store.finish(job, "interrupted", {"message": str(exc)})
            raise

    async def initialize(self, params, envelope=None):
        async with self.init_lock:
            if self.init_result is not None:
                keys = ("protocolVersion", "clientCapabilities", "clientInfo")
                if any(params.get(k, {}) != self.init_params.get(k, {}) for k in keys):
                    raise ValueError(
                        "This live agent has a different client profile. Reconnect with the original initialize fields, or use a separate named host. No capabilities were dropped."
                    )
                return self.init_result
            msg = await self.call("initialize", params, envelope=envelope)
            if "error" in msg:
                raise ValueError(dumps(msg["error"]))
            self.init_params, self.init_result = params, copy.deepcopy(msg["result"])
            self.delete_supported = bool(
                self.native_delete
                and self.init_result.get("agentInfo", {}).get("version") == "1.5.5"
            )
            if self.delete_supported:
                self.init_result.setdefault("agentCapabilities", {}).setdefault(
                    "_meta", {}
                )[DELETE_META] = self.native_delete.capability()
            self.init_extra = {
                k: v for k, v in msg.items() if k not in ("jsonrpc", "id", "result")
            }
            self.store.event(
                "initialized", {"profile": params, "agent": self.init_result}
            )
            return self.init_result

    async def read_agent(self):
        try:
            while line := await self.proc.stdout.readline():
                msg = json.loads(line)
                if not isinstance(msg, dict) or msg.get("jsonrpc") != "2.0":
                    raise ValueError("Agent stdout contained a non-JSON-RPC message")
                # Native load accepts nonexistent IDs, and old frontends may cache
                # list results. A verified-deletion journal prevents resurrection.
                call_info = (
                    self.calls.get(msg.get("id")) if "method" not in msg else None
                )
                if (
                    call_info
                    and call_info[1] == "session/list"
                    and isinstance(msg.get("result"), dict)
                ):
                    entries = msg["result"].get("sessions")
                    if isinstance(entries, list):
                        msg["result"]["sessions"] = [
                            x
                            for x in entries
                            if self.store.deletion(x.get("sessionId")) != "complete"
                        ]
                sid = msg.get("params", {}).get("sessionId")
                deleted_notification = (
                    sid
                    and (sid in self.deleting or self.store.deletion(sid) == "complete")
                    and "id" not in msg
                )
                if deleted_notification:
                    continue
                self.store.event("agent", msg)
                if "method" in msg and "id" in msg:
                    token = "agent-" + uuid.uuid4().hex
                    original = msg["id"]
                    outgoing = dict(msg, id=token)
                    self.pending[token] = (original, outgoing)
                    if self.peer and self.peer.ready:
                        self.peer.send(outgoing)
                elif "method" in msg:
                    if self.peer and self.peer.ready:
                        self.peer.send(msg)
                elif msg.get("id") in self.calls:
                    fut, method, params, job = self.calls.pop(msg["id"])
                    if job:
                        self.store.finish(
                            job, "failed" if "error" in msg else "completed", msg
                        )
                        sid = params.get("sessionId")
                        if not self.config.get(
                            "preserve_background_requests", False
                        ) and not any(
                            x["sessionId"] == sid for x in self.store.active()
                        ):
                            for token, (_, request) in list(self.pending.items()):
                                if request.get("params", {}).get("sessionId") == sid:
                                    self.pending.pop(token, None)
                    if "result" in msg and method in (
                        "session/new",
                        "session/load",
                        "session/resume",
                    ):
                        sid = msg["result"].get("sessionId", params.get("sessionId"))
                        if sid:
                            self.store.db.execute(
                                "INSERT OR REPLACE INTO sessions VALUES(?,?,?)",
                                (
                                    sid,
                                    params.get("cwd", self.config["cwd"]),
                                    dumps(msg["result"]),
                                ),
                            )
                            self.store.db.commit()
                    if not fut.done():
                        fut.set_result(msg)
            raise RuntimeError(f"Agent stdout closed (exit={self.proc.returncode})")
        except asyncio.CancelledError:
            raise
        except Exception as exc:
            self.failure = str(exc)
            with contextlib.suppress(Exception):
                self.store.event("agent_failure", {"message": self.failure})
            with contextlib.suppress(ProcessLookupError):
                os.killpg(self.proc.pid, signal.SIGTERM)
            for fut, _, _, job in list(self.calls.values()):
                if job:
                    with contextlib.suppress(Exception):
                        self.store.finish(job, "interrupted", {"message": self.failure})
                if not fut.done():
                    fut.set_exception(RuntimeError(self.failure))
            self.calls.clear()
            self.pending.clear()
            # Keep the management socket alive for diagnosis, never restart/retry work.

    async def stderr(self):
        # Read byte chunks: logs without newlines cannot exhaust readline buffers.
        path = self.folder / "agent.stderr.log"
        while chunk := await self.proc.stderr.read(8192):
            if path.exists() and path.stat().st_size + len(chunk) > 1024 * 1024:
                for n in (2, 1):
                    src = Path(str(path) + f".{n}")
                    if src.exists():
                        src.replace(str(path) + f".{n + 1}")
                path.replace(str(path) + ".1")
            with path.open("ab") as f:
                f.write(chunk)

    def status(self):
        return {
            "name": self.name,
            "pid": os.getpid(),
            "agentPid": self.proc.pid,
            "agentExitCode": self.proc.returncode,
            "failure": self.failure,
            "attached": bool(self.peer),
            "initialized": self.init_result is not None,
            "clientProfile": self.init_params,
            "active": self.store.active(),
            "sessions": [
                dict(id=a, cwd=b)
                for a, b in self.store.db.execute("SELECT id,cwd FROM sessions")
            ],
            "pending": [{"id": k, **v[1]} for k, v in self.pending.items()],
            "sessionDeletion": {
                "configured": bool(self.native_delete),
                "available": self.delete_supported,
                "inProgress": sorted(self.deleting),
                "unresolved": [
                    x[0]
                    for x in self.store.db.execute(
                        "SELECT id FROM deletions WHERE state='pending'"
                    )
                ],
            },
        }

    async def answer(self, token, answer):
        if token not in self.pending:
            raise ValueError("Unknown/already answered client request")
        original, request = self.pending[token]
        if not isinstance(answer, dict) or "method" in answer:
            raise ValueError("Expected a JSON-RPC result or error, not a request")
        if ("result" in answer) == ("error" in answer):
            raise ValueError("Provide exactly one of result or error")
        if request["method"] == "session/request_permission" and "result" in answer:
            outcome = answer["result"].get("outcome", {})
            if outcome.get("outcome") == "selected":
                choices = {
                    x["optionId"] for x in request.get("params", {}).get("options", [])
                }
                if outcome.get("optionId") not in choices:
                    raise ValueError("Invalid permission optionId")
            elif outcome.get("outcome") != "cancelled":
                raise ValueError("Invalid permission outcome")
        await self.send({**answer, "jsonrpc": "2.0", "id": original})
        self.pending.pop(token, None)
        self.store.event("client_response", {"id": token, **answer})

    async def acp(self, peer, msg):
        ident = msg.get("id")
        try:
            if msg.get("jsonrpc") != "2.0":
                raise ValueError("Expected JSON-RPC 2.0")
            if "method" not in msg:
                await self.answer(
                    ident, {k: v for k, v in msg.items() if k not in ("id", "jsonrpc")}
                )
                return
            method, params = msg["method"], msg.get("params", {})
            if method == "initialize":
                result = await self.initialize(params, envelope=msg)
                peer.ready = True
                peer.send({**self.init_extra, **response(ident, result)})
                # The new controller must answer outstanding reverse requests. IDs stay stable.
                for _, request in self.pending.values():
                    peer.send(request)
                return
            if not peer.ready:
                raise ValueError("initialize is required")
            if method == DELETE_METHOD:
                if "id" not in msg:
                    raise ValueError("Deletion requires a JSON-RPC request ID")
                peer.send(response(ident, await self.delete_session(params)))
                return
            if self.deleting and method != "session/cancel":
                raise DeletionError(
                    "DELETE_IN_PROGRESS",
                    "Native deletion is synchronizing session caches; retry this request afterwards",
                    retryable=True,
                    code=-32051,
                )
            sid = params.get("sessionId")
            if sid and (sid in self.deleting or self.store.deletion(sid)):
                raise DeletionError(
                    "SESSION_DELETED_OR_DELETING",
                    "Session was deleted or has an unresolved deletion; retry deletion to reconcile",
                    code=-32051,
                )
            if method in ("session/load", "session/resume") and any(
                x["sessionId"] == params.get("sessionId") for x in self.store.active()
            ):
                raise ValueError(
                    "Session is still running. Use watch/status/respond/cancel; load after the current turn finishes. Reloading an active agent is deliberately prevented."
                )
            if method in ("session/new", "session/load", "session/resume"):
                cwd = params.get("cwd")
                if (
                    not isinstance(cwd, str)
                    or not Path(cwd).is_absolute()
                    or not Path(cwd).is_dir()
                ):
                    raise ValueError(
                        "cwd must be an existing absolute path on the server"
                    )
            self.store.event("client", msg)
            if "id" not in msg:
                await self.send(msg)
                if method == "session/cancel" and not self.config.get(
                    "preserve_background_requests", False
                ):
                    await self.cancel_pending(params.get("sessionId"))
                return
            job = None
            if method == "session/prompt":
                job = uuid.uuid4().hex
                self.store.new_job(job, params)
            result = await self.call(method, params, job, envelope=msg)
            # Responses belong to the original connection; never reuse IDs on a new one.
            if self.peer is peer:
                peer.send(dict(result, id=ident))
        except DeletionError as exc:
            if "id" in msg:
                peer.send(
                    {
                        "jsonrpc": "2.0",
                        "id": ident,
                        "error": {
                            "code": exc.code,
                            "message": str(exc),
                            "data": exc.data,
                        },
                    }
                )
        except Exception as exc:
            if "id" in msg:
                peer.send(error(ident, str(exc)))
        finally:
            if "method" in msg:
                peer.inflight.discard(ident)

    async def cancel_pending(self, sid):
        for token, (_, req) in list(self.pending.items()):
            if req.get("params", {}).get("sessionId") != sid:
                continue
            answer = (
                {"result": {"outcome": {"outcome": "cancelled"}}}
                if req["method"] == "session/request_permission"
                else {"error": {"code": -32800, "message": "Session cancelled"}}
            )
            await self.answer(token, answer)

    async def background(self, ident, params):
        try:
            await self.call("session/prompt", params, ident)
        except Exception:
            pass  # call/read_agent persisted the exact failure.

    async def delete_session(self, params):
        if not self.delete_supported:
            raise DeletionError(
                "UNSUPPORTED",
                "Native permanent deletion is not enabled on this host",
                code=-32601,
            )
        if not isinstance(params, dict) or set(params) != {"sessionId"}:
            raise DeletionError(
                "INVALID_PARAMS",
                "Deletion requires only sessionId; cascade and force are not supported",
                code=-32602,
            )
        sid = valid_id(params["sessionId"])
        if self.failure:
            raise DeletionError(
                "AGENT_UNAVAILABLE",
                "Cannot delete while the native frontend has failed",
                retryable=True,
            )
        # No await before setting the per-session guard. Existing native calls and
        # reverse requests must finish; deletion never cancels or approves them.
        if sid in self.deleting:
            raise DeletionError(
                "DELETE_BUSY", "Deletion is already in progress", retryable=True
            )
        if self.calls or self.store.active():
            raise DeletionError(
                "SESSION_BUSY",
                "The host must finish outstanding ACP requests and tasks before deletion",
                retryable=True,
            )
        if any(x["sessionId"] == sid for x in self.store.active()) or any(
            p.get("sessionId") == sid for _, _, p, _ in self.calls.values()
        ):
            raise DeletionError(
                "SESSION_BUSY", "Session has an active request or task", retryable=True
            )
        if any(
            req.get("params", {}).get("sessionId") == sid
            for _, req in self.pending.values()
        ):
            raise DeletionError(
                "PENDING_CLIENT_REQUEST",
                "Session has an unresolved client request",
                retryable=True,
            )
        self.deleting.add(sid)
        try:
            async with self.delete_lock:
                existed = await asyncio.to_thread(self.native_delete.preflight, sid)
                unresolved = self.store.deletion(sid) == "pending"
                # Persist intent before the first mutation. Crash/disconnect does
                # not permit load/prompt to recreate a partially deleted session.
                self.store.mark_deletion(sid, "pending")
                closed = await asyncio.wait_for(
                    self.call("session/close", {"sessionId": sid}), 20
                )
                if "error" in closed:
                    raise DeletionError(
                        "CLOSE_FAILED",
                        "Native session could not be closed; deletion remains pending",
                        retryable=True,
                    )
                # Close itself is not deletion. Recheck references after releasing
                # the target's native worker and before invoking native deletion.
                await asyncio.to_thread(self.native_delete.preflight, sid)
                await asyncio.to_thread(self.native_delete.assert_no_writer, sid)
                if existed or unresolved:
                    await self.native_delete.execute(sid, self.proc.pid)
                await asyncio.to_thread(self.native_delete.verify_absent, sid)
                self.store.forget_session(sid)
                return {
                    "deleted": True,
                    "sessionId": sid,
                    "deletedSessionIds": [sid],
                    "alreadyDeleted": not existed,
                }
        except DeletionError as exc:
            if self.store.deletion(sid) == "pending":
                exc.data["mayBeDeleted"] = True
            raise
        except Exception as exc:
            raise DeletionError(
                "DELETE_FAILED",
                "Deletion was not verified; retry to reconcile",
                retryable=True,
                uncertain=bool(self.store.deletion(sid)),
            ) from exc
        finally:
            self.deleting.discard(sid)

    async def control(self, msg):
        op = msg.get("op")
        if op == "status":
            return self.status()
        if op == "events":
            return self.store.events(int(msg.get("after", 0)))
        if op == "job":
            job = self.store.job(msg["id"])
            if job is None:
                raise ValueError("Unknown job")
            return job
        if op == "stop":
            self.stopping.set()
            return {"stopping": True}
        if op == "cancel":
            await self.send(
                {
                    "jsonrpc": "2.0",
                    "method": "session/cancel",
                    "params": {"sessionId": msg["sessionId"]},
                }
            )
            if not self.config.get("preserve_background_requests", False):
                await self.cancel_pending(msg["sessionId"])
            return {"cancelRequested": True}
        if op in ("respond", "approve"):
            if self.peer:
                raise ValueError(
                    "An ACP client is attached; answer through that client or detach it first"
                )
            answer = msg.get("answer")
            if op == "approve":
                token = msg["id"]
                if (
                    token not in self.pending
                    or self.pending[token][1]["method"] != "session/request_permission"
                ):
                    raise ValueError("approve only handles session/request_permission")
                outcome = (
                    {"outcome": "cancelled"}
                    if msg.get("option") is None
                    else {"outcome": "selected", "optionId": msg["option"]}
                )
                answer = {"result": {"outcome": outcome}}
            await self.answer(msg["id"], answer)
            return {"answered": True}
        if op == "submit":
            async with self.submit_lock:
                if self.deleting:
                    raise ValueError(
                        "Native deletion is in progress; retry submit afterwards"
                    )
                ident = msg["id"]
                if not isinstance(ident, str) or not 1 <= len(ident) <= 128:
                    raise ValueError("request id must have 1–128 characters")
                if self.store.db.execute(
                    "SELECT 1 FROM deleted_jobs WHERE id=?", (ident,)
                ).fetchone():
                    raise ValueError(
                        "Request ID belongs to deleted session history and cannot be submitted again"
                    )
                existing = self.store.job(ident)
                if existing:
                    supplied = msg["prompt"]
                    if existing["request"]["prompt"] != supplied or (
                        msg.get("sessionId")
                        and existing["sessionId"] != msg["sessionId"]
                    ):
                        raise ValueError("Request ID already used for different input")
                    return existing
                if self.peer:
                    raise ValueError(
                        "An ACP controller is attached; submit there or detach it"
                    )
                if self.failure:
                    raise RuntimeError(self.failure)
                if self.init_result is None:
                    await self.initialize(
                        {
                            "protocolVersion": 1,
                            "clientCapabilities": {},
                            "clientInfo": {
                                "name": "acp-ssh-background",
                                "version": VERSION,
                            },
                        }
                    )
                sid = msg.get("sessionId")
                if sid and (sid in self.deleting or self.store.deletion(sid)):
                    raise ValueError(
                        "Session was deleted or has an unresolved deletion"
                    )
                if sid is None:
                    result = await self.call(
                        "session/new", {"cwd": self.config["cwd"], "mcpServers": []}
                    )
                    if "error" in result:
                        raise ValueError(dumps(result["error"]))
                    sid = result["result"]["sessionId"]
                if any(x["sessionId"] == sid for x in self.store.active()):
                    raise ValueError("Session already has an active turn")
                params = {"sessionId": sid, "prompt": msg["prompt"]}
                self.store.new_job(ident, params)
                self.task(self.background(ident, params))
                return self.store.job(ident)
        raise ValueError("Unknown management operation")

    async def connection(self, reader, writer):
        peer = None
        try:
            raw = writer.get_extra_info("socket").getsockopt(
                socket.SOL_SOCKET, socket.SO_PEERCRED, 12
            )
            if struct.unpack("3i", raw)[1] != os.getuid():
                raise ValueError("Wrong Unix peer UID")
            line = await asyncio.wait_for(reader.readline(), 10)
            msg = json.loads(line)
            if msg.get("op") != "attach":
                result = await self.control(msg)
                writer.write(packet({"ok": True, "result": result}))
                await writer.drain()
                return
            if self.peer:
                raise ValueError(
                    "Another ACP controller is connected; close it before attaching"
                )
            peer = Peer(writer)
            self.peer = peer
            writer.write(packet({"ok": True}))
            await writer.drain()
            while line := await reader.readline():
                try:
                    req = json.loads(line)
                    if not isinstance(req, dict):
                        raise ValueError("Batches are not supported by ACP")
                except ValueError as exc:
                    peer.send(error(None, str(exc), -32700))
                    continue
                if "method" in req and "id" in req:
                    ident = req["id"]
                    if not isinstance(ident, (str, int)) or isinstance(ident, bool):
                        peer.send(error(None, "Invalid request ID", -32600))
                        continue
                    if ident in peer.inflight or len(peer.inflight) >= 128:
                        peer.send(
                            error(
                                ident,
                                "Duplicate request ID or too many in-flight requests",
                            )
                        )
                        continue
                    peer.inflight.add(ident)
                self.task(self.acp(peer, req))
        except Exception as exc:
            if peer:
                peer.send(error(None, str(exc)))
            else:
                with contextlib.suppress(ConnectionError):
                    writer.write(packet({"ok": False, "error": str(exc)}))
                    await writer.drain()
        finally:
            if peer:
                if self.peer is peer:
                    self.peer = None
                peer.close()
                with contextlib.suppress(Exception):
                    self.store.event("detached", {})
            writer.close()
            with contextlib.suppress(ConnectionError):
                await writer.wait_closed()

    async def run(self):
        self.proc = await asyncio.create_subprocess_exec(
            *self.config["command"],
            cwd=self.config["cwd"],
            stdin=asyncio.subprocess.PIPE,
            stdout=asyncio.subprocess.PIPE,
            stderr=asyncio.subprocess.PIPE,
            start_new_session=True,
            limit=self.max_line,
        )
        reader = self.task(self.read_agent())
        self.task(self.stderr())
        self.store.event("started", {"pid": os.getpid(), "agentPid": self.proc.pid})
        server = await asyncio.start_unix_server(
            self.connection, path=str(self.sock), limit=self.max_line
        )
        self.sock.chmod(0o600)
        loop = asyncio.get_running_loop()
        for sig in (signal.SIGINT, signal.SIGTERM):
            loop.add_signal_handler(sig, self.stopping.set)
        try:
            await self.stopping.wait()
        finally:
            server.close()
            await server.wait_closed()
            if self.peer:
                self.peer.close()
            # Kill only our own agent process group; no unrelated agy processes.
            with contextlib.suppress(ProcessLookupError):
                os.killpg(self.proc.pid, signal.SIGTERM)
            try:
                await asyncio.wait_for(self.proc.wait(), 5)
            except asyncio.TimeoutError:
                with contextlib.suppress(ProcessLookupError):
                    os.killpg(self.proc.pid, signal.SIGKILL)
                await self.proc.wait()
            # Also kill descendants left behind after their leader exited.
            with contextlib.suppress(ProcessLookupError):
                os.killpg(self.proc.pid, signal.SIGKILL)
            for job in self.store.active():
                self.store.finish(job["id"], "interrupted", {"message": "Host stopped"})
            for t in list(self.tasks):
                t.cancel()
            await asyncio.gather(*list(self.tasks), return_exceptions=True)
            self.sock.unlink(missing_ok=True)
            self.store.db.close()


def rpc(name, value, timeout=120):
    _, sock = paths(name)
    with socket.socket(socket.AF_UNIX) as conn:
        conn.settimeout(timeout)
        conn.connect(str(sock))
        conn.sendall(packet(value))
        with conn.makefile("rb") as f:
            data = json.loads(f.readline())
    if not data.get("ok"):
        raise RuntimeError(data.get("error", "Host error"))
    return data.get("result")


def connect(name):
    # Threads let stdin remain a pipe, terminal, or regular file. EOF only detaches.
    import threading

    _, sock = paths(name)
    with socket.socket(socket.AF_UNIX) as conn:
        conn.connect(str(sock))
        conn.sendall(packet({"op": "attach"}))
        f = conn.makefile("rb")
        hello = json.loads(f.readline())
        if not hello.get("ok"):
            raise RuntimeError(hello.get("error"))

        def upload():
            try:
                while line := sys.stdin.buffer.readline():
                    conn.sendall(line)
            except (BrokenPipeError, OSError):
                pass
            finally:
                with contextlib.suppress(OSError):
                    conn.shutdown(socket.SHUT_WR)

        threading.Thread(target=upload, daemon=True).start()
        try:
            while line := f.readline():
                sys.stdout.buffer.write(line)
                sys.stdout.buffer.flush()
        except BrokenPipeError:
            pass


def configure(args):
    if args.event_mib < 1 or args.message_mib < 1:
        raise ValueError("Byte limits must be positive MiB values")
    if args.event_mib < args.message_mib:
        raise ValueError("Event retention must be at least the message size limit")
    folder, sock = paths(args.name)
    private_dir(folder.parent)
    private_dir(folder)
    private_dir(sock.parent)
    config = folder / "config.json"
    if config.exists() and not args.force:
        raise ValueError("Config exists; use --force only after stopping this host")
    if sock.exists():
        try:
            rpc(args.name, {"op": "status"}, 2)
        except (OSError, ValueError):
            pass
        else:
            raise ValueError("Stop the live host before configuring")
    command = args.command or [
        os.environ.get("AGY_NATIVE_BINARY", str(Path.home() / ".local/bin/agy")),
        "acp",
        "--process-per-session",
    ]
    if command[0] == "--":
        command = command[1:]
    cwd = Path(args.cwd).resolve(strict=True)
    if not cwd.is_dir():
        raise ValueError("cwd must be a directory")
    data = {
        "cwd": str(cwd),
        "command": command,
        "event_bytes": args.event_mib * 1024 * 1024,
        "max_message_bytes": args.message_mib * 1024 * 1024,
        "preserve_background_requests": args.preserve_background_requests,
    }
    if args.native_delete_app_data:
        data["native_delete"] = {
            "home": str(Path.home()),
            "app_data_name": args.native_delete_app_data,
            "binary": str(Path(args.native_delete_binary).absolute()),
        }
        NativeDelete(
            data["native_delete"]
        )  # Fail configuration before writing if unverified.
    temp = config.with_suffix(".tmp")
    temp.write_text(json.dumps(data, indent=2) + "\n")
    temp.replace(config)
    return {"configured": args.name, "config": str(config)}


def serve(name):
    folder, sock = paths(name)
    private_dir(folder)
    private_dir(sock.parent)
    with (folder / "host.lock").open("a") as lock:
        fcntl.flock(lock, fcntl.LOCK_EX | fcntl.LOCK_NB)
        sock.unlink(missing_ok=True)
        config = json.loads((folder / "config.json").read_text())
        asyncio.run(Host(name, folder, sock, config).run())


def main():
    os.umask(0o077)
    p = argparse.ArgumentParser(description=__doc__)
    sub = p.add_subparsers(dest="op", required=True)
    c = sub.add_parser("configure", allow_abbrev=False)
    c.add_argument("name")
    c.add_argument("--cwd", required=True)
    c.add_argument("--force", action="store_true")
    c.add_argument("--event-mib", type=int, default=64)
    c.add_argument("--message-mib", type=int, default=16)
    c.add_argument(
        "--preserve-background-requests",
        action="store_true",
        help="Do not infer background request completion from parent prompt end/cancel",
    )
    c.add_argument(
        "--native-delete-app-data",
        help="Opt in to verified native permanent deletion for this agy app data name",
    )
    c.add_argument(
        "--native-delete-binary",
        default=os.environ.get(
            "AGY_NATIVE_BINARY", str(Path.home() / ".local/lib/antigravity-cli/agy")
        ),
    )
    c.add_argument("--command", nargs=argparse.REMAINDER)
    for op in ("serve", "connect", "status", "stop"):
        sub.add_parser(op).add_argument("name")
    c = sub.add_parser("start")
    c.add_argument("name")
    c.add_argument("--detached", action="store_true")
    c = sub.add_parser("submit")
    c.add_argument("name")
    c.add_argument("--request-id", required=True)
    c.add_argument("--session")
    c.add_argument("--file", default="-")
    c = sub.add_parser("job")
    c.add_argument("name")
    c.add_argument("id")
    c = sub.add_parser("watch")
    c.add_argument("name")
    c.add_argument("--after", type=int, default=0)
    c.add_argument("--follow", action="store_true")
    c = sub.add_parser("approve")
    c.add_argument("name")
    c.add_argument("id")
    c.add_argument("--option")
    c = sub.add_parser("respond")
    c.add_argument("name")
    c.add_argument("id")
    c.add_argument("--file", default="-")
    c = sub.add_parser("cancel")
    c.add_argument("name")
    c.add_argument("session")
    args = p.parse_args()
    if args.op == "configure":
        out = configure(args)
    elif args.op == "serve":
        serve(args.name)
        return
    elif args.op == "connect":
        connect(args.name)
        return
    elif args.op == "start":
        folder, _ = paths(args.name)
        try:
            existing = rpc(args.name, {"op": "status"}, 2)
        except (OSError, ValueError):
            existing = None
        if existing is not None:
            print(json.dumps(existing, ensure_ascii=False, indent=2))
            return
        if args.detached:
            with (folder / "host.log").open("ab") as log:
                subprocess.Popen(
                    [sys.executable, str(Path(__file__).resolve()), "serve", args.name],
                    stdin=subprocess.DEVNULL,
                    stdout=log,
                    stderr=log,
                    start_new_session=True,
                    close_fds=True,
                )
        else:
            subprocess.run(
                ["systemctl", "--user", "start", f"acp-ssh@{args.name}.service"],
                check=True,
            )
        deadline = time.monotonic() + 15
        while True:
            try:
                out = rpc(args.name, {"op": "status"}, 2)
                break
            except (OSError, ValueError):
                if time.monotonic() > deadline:
                    raise RuntimeError(
                        "Host did not become ready; inspect the service/host log"
                    )
                time.sleep(0.1)
    elif args.op == "watch":
        cursor = args.after
        while True:
            result = rpc(args.name, {"op": "events", "after": cursor})
            if result["gap"]:
                print(dumps({"gap": True, "oldest": result["oldest"]}), flush=True)
            for entry in result["events"]:
                print(dumps(entry), flush=True)
                cursor = entry["seq"]
            if result["more"]:
                continue
            if not args.follow:
                return
            time.sleep(0.5)
    else:
        msg = {"op": args.op}
        if args.op == "submit":
            content = (
                sys.stdin.read() if args.file == "-" else Path(args.file).read_text()
            )
            msg.update(id=args.request_id, prompt=[{"type": "text", "text": content}])
            if args.session:
                msg["sessionId"] = args.session
        elif args.op in ("job", "approve", "respond"):
            msg["id"] = args.id
            if args.op == "approve":
                msg["option"] = args.option
            elif args.op == "respond":
                msg["answer"] = json.loads(
                    sys.stdin.read()
                    if args.file == "-"
                    else Path(args.file).read_text()
                )
        elif args.op == "cancel":
            msg["sessionId"] = args.session
        out = rpc(args.name, msg)
    print(json.dumps(out, ensure_ascii=False, indent=2))


if __name__ == "__main__":
    try:
        main()
    except KeyboardInterrupt:
        sys.exit(130)
    except (OSError, ValueError, RuntimeError, subprocess.SubprocessError) as exc:
        print(f"acp-ssh: {exc}", file=sys.stderr)
        sys.exit(1)
