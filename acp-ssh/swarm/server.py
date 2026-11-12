#!/usr/bin/env python3
# SPDX-License-Identifier: Apache-2.0
"""ACP facade for agy's native swarm. No worker scheduler, model client or board writes."""

import argparse
import asyncio
import contextlib
import copy
import fcntl
from datetime import datetime, timezone
import json
import os
from pathlib import Path
import signal
import sys
import uuid

from native_store import NativeStore

PREFIX = "_maichart/swarm/"
META = "maichart/nativeSwarm"
MAX_LINE = 16 * 1024 * 1024
SUPPORTED_VERSION = "1.5.5"


def packet(value):
    return (
        json.dumps(value, ensure_ascii=False, separators=(",", ":")) + "\n"
    ).encode()


def error(ident, text, code=-32000):
    return {"jsonrpc": "2.0", "id": ident, "error": {"code": code, "message": text}}


class Adapter:
    def __init__(self, proc, emit, root, interval=1, approval="auto"):
        self.proc, self.emit = proc, emit
        self.store = NativeStore(root)
        self.interval = interval
        self.approval = approval
        self.calls = {}
        self.roots = set()
        self.session_views = {}
        self.generation = uuid.uuid4().hex
        self.busy = set()
        self.pending = {}
        self.subscriptions = {}
        self.initialized = False
        self.lock = asyncio.Lock()

    async def send_native(self, msg):
        data = packet(msg)
        if len(data) > MAX_LINE:
            raise ValueError("ACP message exceeds 16 MiB")
        if self.proc.returncode is not None:
            raise RuntimeError("Native ACP process has exited")
        async with self.lock:
            self.proc.stdin.write(data)
            await asyncio.wait_for(self.proc.stdin.drain(), 15)

    def capability(self):
        return {
            "version": 1,
            "revision": 2,
            "implementation": "native-agy",
            "nativeVersion": SUPPORTED_VERSION,
            "methods": [
                PREFIX + x
                for x in (
                    "capabilities",
                    "sessions",
                    "attach",
                    "snapshot",
                    "history",
                    "subscribe",
                    "unsubscribe",
                )
            ],
            "readOnlyAttach": True,
            "sessionMetadataSource": "observed-native-acp",
            "notification": PREFIX + "update",
            "storage": "native-local-files",
            "approvalMode": self.approval,
            "controls": "coordinator-prompt",
            "directWorkerControl": False,
            "workerApprovalVisibility": "not-forwarded-in-1.5.5-probe",
            "hardWorkerConcurrencyLimit": False,
            "cancelScope": "native-session-turn",
            "endTurnMeansSwarmComplete": False,
        }

    def require_root(self, params):
        sid = params.get("sessionId")
        if sid not in self.roots:
            raise ValueError(
                "Coordinator is not open in this persistent adapter; attach cannot load or revive it"
            )
        return sid

    def observe_session(self, sid, fields, source):
        """Cache only actual native fields; never manufacture model choices from requests."""
        view = self.session_views.setdefault(
            sid,
            {
                "nativeSession": {},
                "revision": 0,
                "fieldObservations": {},
                "staleFields": [],
            },
        )
        selected = {
            k: fields[k]
            for k in ("models", "configOptions", "modes", "availableCommands", "title")
            if k in fields
        }
        if not selected:
            return
        view["revision"] += 1
        observed = datetime.now(timezone.utc).isoformat()
        for key, value in selected.items():
            view["nativeSession"][key] = copy.deepcopy(value)
            view["fieldObservations"][key] = {
                "source": source,
                "observedAt": observed,
                "revision": view["revision"],
            }
            if key in view["staleFields"]:
                view["staleFields"].remove(key)

    def invalidate_session(self, sid, keys, reason):
        view = self.session_views.get(sid)
        if view is None:
            return
        for key in keys:
            if key in view["nativeSession"] and key not in view["staleFields"]:
                view["staleFields"].append(key)
        view["revision"] += 1
        view["lastInvalidation"] = reason

    async def attach(self, sid):
        # No native RPC, history replay, load, mode change, task or approval response.
        snapshot_error = None
        try:
            snapshot = await self.snapshot(sid)
        except (OSError, ValueError) as exc:
            snapshot = None
            snapshot_error = str(exc)
        view = copy.deepcopy(
            self.session_views.get(
                sid,
                {
                    "nativeSession": {},
                    "revision": 0,
                    "fieldObservations": {},
                    "staleFields": [],
                },
            )
        )
        view["freshness"] = "last-observed-not-live-query"
        return {
            "sessionId": sid,
            "attached": True,
            "readOnly": True,
            "generation": self.generation,
            "source": "observed-native-acp",
            "metadata": view,
            "snapshot": snapshot,
            "snapshotError": snapshot_error,
            "history": {
                "method": PREFIX + "history",
                "params": {"sessionId": sid, "member": "coordinator"},
                "source": "native-transcript",
                "format": "native-jsonl",
                "notAcpReplay": True,
            },
            "permissionReplay": "transport-initialize-only",
            "subscribed": sid in self.subscriptions,
        }

    async def snapshot(self, sid, after=0, limit=100):
        result = await asyncio.to_thread(self.store.snapshot, sid, after, limit)
        result.update(
            nativeProcessAlive=self.proc.returncode is None,
            coordinatorPromptActive=sid in self.busy,
            observedAt=datetime.now(timezone.utc).isoformat(),
        )
        result["approvalMode"] = self.approval
        result["limitations"] = [
            "In ask mode, worker command approvals were not forwarded by native ACP in the 1.5.5 probe.",
            "Native file state is not proof of liveness or team completion.",
        ]
        result["pendingRequests"] = [
            {
                "id": i,
                "idNamespace": "native-acp",
                "actionable": False,
                "method": p.get("method"),
                "sessionId": p.get("params", {}).get("sessionId"),
                "origin": "native-acp",
            }
            for i, p in self.pending.items()
            if p.get("params", {}).get("sessionId") == sid
        ]
        return result

    async def extension(self, method, params):
        name = method[len(PREFIX) :]
        if name == "capabilities":
            return self.capability()
        if name == "sessions":
            return {
                "generation": self.generation,
                "source": "persistent-adapter-memory",
                "sessions": [
                    {
                        "sessionId": sid,
                        "coordinatorPromptActive": sid in self.busy,
                        "metadataRevision": self.session_views.get(sid, {}).get(
                            "revision", 0
                        ),
                    }
                    for sid in sorted(self.roots)
                ],
            }
        sid = self.require_root(params)
        if name == "attach":
            return await self.attach(sid)
        if name == "snapshot":
            return await self.snapshot(
                sid, params.get("after", 0), params.get("limit", 100)
            )
        if name == "history":
            return await asyncio.to_thread(
                self.store.history,
                sid,
                params.get("member"),
                params.get("cursor"),
                params.get("limit", 100),
            )
        if name == "subscribe":
            if len(self.subscriptions) >= 64 and sid not in self.subscriptions:
                raise ValueError("Subscription limit reached")
            snap = await self.snapshot(sid)
            self.subscriptions[sid] = (
                None  # First notification always includes current snapshot.
            )
            return {"subscribed": True, "snapshot": snap}
        if name == "unsubscribe":
            self.subscriptions.pop(sid, None)
            return {"subscribed": False}
        raise LookupError("Unknown swarm extension")

    async def client(self, msg):
        ident = msg.get("id") if isinstance(msg, dict) else None
        try:
            if not isinstance(msg, dict) or msg.get("jsonrpc") != "2.0":
                raise ValueError("Expected JSON-RPC 2.0 object")
            if not isinstance(ident, (str, int, type(None))) or isinstance(ident, bool):
                raise ValueError("Invalid JSON-RPC ID")
            if "method" not in msg:
                if ident not in self.pending:
                    raise ValueError("Unknown or already answered native request")
                if ("result" in msg) == ("error" in msg):
                    raise ValueError("Expected exactly one result or error")
                request = self.pending[ident]
                if (
                    request["method"] == "session/request_permission"
                    and "result" in msg
                ):
                    outcome = msg["result"].get("outcome", {})
                    if outcome.get("outcome") == "selected":
                        choices = {
                            x["optionId"]
                            for x in request.get("params", {}).get("options", [])
                        }
                        if outcome.get("optionId") not in choices:
                            raise ValueError("Invalid permission optionId")
                    elif outcome.get("outcome") != "cancelled":
                        raise ValueError("Invalid permission outcome")
                await self.send_native(msg)
                self.pending.pop(ident, None)
                return
            method = msg["method"]
            params = msg.get("params", {})
            if not isinstance(method, str) or not isinstance(params, dict):
                raise ValueError("Invalid method or params")
            if method != "initialize" and not self.initialized:
                raise ValueError("initialize is required")
            if method.startswith(PREFIX):
                result = await self.extension(method, params)
                if "id" in msg:
                    await self.emit({"jsonrpc": "2.0", "id": ident, "result": result})
                return
            if "id" in msg:
                if ident in self.calls:
                    raise ValueError("Duplicate in-flight request ID")
                if len(self.calls) >= 128:
                    raise ValueError("Too many in-flight requests")
            sid = params.get("sessionId")
            if (
                method in ("session/prompt", "session/load", "session/resume")
                and sid in self.busy
            ):
                raise ValueError(
                    "Coordinator turn is already running; cancel or wait first"
                )
            if method in ("session/load", "session/resume") and sid in self.roots:
                snap = await self.snapshot(sid)
                if any(
                    not m["native"].get("killed")
                    for m in snap["members"]
                    if m["name"] != "coordinator"
                ):
                    raise ValueError(
                        "Native workers may still be alive. Use snapshot/history on reconnect; do not reload this live swarm"
                    )
            if method == "session/prompt" and "id" in msg:
                self.require_root(params)
                text = "\n".join(
                    c.get("text", "")
                    for c in params.get("prompt", [])
                    if c.get("type") == "text"
                )
                if text.strip() == "/swarm board":
                    snap = await self.snapshot(sid)
                    await self.emit(
                        {
                            "jsonrpc": "2.0",
                            "method": "session/update",
                            "params": {
                                "sessionId": sid,
                                "update": {
                                    "sessionUpdate": "agent_message_chunk",
                                    "content": {
                                        "type": "text",
                                        "text": "Native swarm snapshot (adapter, not model output):\n"
                                        + json.dumps(
                                            snap, ensure_ascii=False, indent=2
                                        ),
                                    },
                                    "_meta": {META: {"source": "native-local-files"}},
                                },
                            },
                        }
                    )
                    await self.emit(
                        {
                            "jsonrpc": "2.0",
                            "id": ident,
                            "result": {"stopReason": "end_turn"},
                        }
                    )
                    return
                self.busy.add(sid)
            if method in (
                "session/set_config_option",
                "session/set_model",
                "session/set_mode",
            ):
                # Pending/failed updates may leave an uncertain native state. Only a native
                # response/notification can establish the new selection; never use value/modelId.
                self.invalidate_session(
                    sid,
                    ("models", "configOptions", "modes"),
                    "native-setting-request-in-flight",
                )
            if "id" in msg:
                self.calls[ident] = (method, params)
            try:
                await self.send_native(msg)
            except BaseException:
                self.calls.pop(ident, None)
                if method == "session/prompt":
                    self.busy.discard(sid)
                raise
        except Exception as exc:
            if isinstance(msg, dict) and "method" not in msg:
                # Never reply to a reply (or accidentally answer a different reverse request).
                await self.emit(
                    {
                        "jsonrpc": "2.0",
                        "method": PREFIX + "error",
                        "params": {"message": str(exc)},
                    }
                )
            elif isinstance(msg, dict) and "id" in msg:
                await self.emit(
                    error(
                        ident,
                        str(exc),
                        -32601 if isinstance(exc, LookupError) else -32000,
                    )
                )

    async def native(self, msg):
        if "method" in msg:
            if "id" in msg:
                self.pending[msg["id"]] = msg
            params = msg.get("params", {})
            update = params.get("update", {})
            sid = params.get("sessionId")
            if msg["method"] == "session/update" and isinstance(sid, str):
                if update.get("sessionUpdate") == "config_option_update":
                    self.invalidate_session(
                        sid, ("models", "modes"), "native-config-option-update"
                    )
                self.observe_session(
                    sid,
                    update,
                    "session/update:" + str(update.get("sessionUpdate", "unknown")),
                )
                if update.get("sessionUpdate") == "current_mode_update":
                    # Keep the exact update separately; don't synthesize a full modes response.
                    view = self.session_views[sid]
                    view["currentModeUpdate"] = copy.deepcopy(update)
                    self.invalidate_session(
                        sid, ("modes",), "native-current-mode-update"
                    )
            if (
                msg["method"] == "session/update"
                and update.get("sessionUpdate") == "available_commands_update"
            ):
                commands = update.setdefault("availableCommands", [])
                if not any(c.get("name") == "swarm" for c in commands):
                    commands.append(
                        {
                            "name": "swarm",
                            "description": "Use the native swarm coordinator; /swarm board reads the native board.",
                        }
                    )
            await self.emit(msg)
            return
        method, params = self.calls.pop(msg.get("id"), ("", {}))
        sid = params.get("sessionId")
        if method == "session/prompt":
            self.busy.discard(sid)
        if "result" in msg:
            if method == "initialize":
                self.initialized = True
                capabilities = msg["result"].setdefault("agentCapabilities", {})
                capabilities.setdefault("_meta", {})[META] = self.capability()
            if method in ("session/new", "session/load", "session/resume"):
                sid = msg["result"].get("sessionId", sid)
                if sid:
                    self.roots.add(sid)
                    self.observe_session(sid, msg["result"], method + ":result")
                    msg["result"].setdefault("_meta", {})[META] = {
                        "coordinatorSessionId": sid,
                        "source": "native-agy",
                    }
            elif method in (
                "session/set_config_option",
                "session/set_model",
                "session/set_mode",
            ):
                self.observe_session(sid, msg["result"], method + ":result")
            elif method == "session/close":
                self.roots.discard(sid)
                self.subscriptions.pop(sid, None)
        # In particular: do not clear pending approvals when the parent's prompt ends.
        await self.emit(msg)

    async def watch(self):
        while True:
            await asyncio.sleep(self.interval)
            for sid in list(self.subscriptions):
                try:
                    snap = await self.snapshot(sid)
                    key = (
                        snap["etag"],
                        snap["coordinatorPromptActive"],
                        json.dumps(snap["pendingRequests"], sort_keys=True),
                    )
                    if self.subscriptions.get(sid) != key:
                        self.subscriptions[sid] = key
                        await self.emit(
                            {
                                "jsonrpc": "2.0",
                                "method": PREFIX + "update",
                                "params": snap,
                            }
                        )
                except Exception as exc:
                    key = ("error", str(exc))
                    if self.subscriptions.get(sid) != key:
                        self.subscriptions[sid] = key
                        await self.emit(
                            {
                                "jsonrpc": "2.0",
                                "method": PREFIX + "update",
                                "params": {
                                    "sessionId": sid,
                                    "source": "native-local-files",
                                    "readError": str(exc),
                                    "runtimeVerified": False,
                                },
                            }
                        )


async def run(args):
    # Known native storage layout and tool component names; never guess after upgrades.
    version = await asyncio.create_subprocess_exec(
        args.native,
        "--version",
        stdout=asyncio.subprocess.PIPE,
        stderr=asyncio.subprocess.PIPE,
    )
    try:
        out, _ = await asyncio.wait_for(version.communicate(), 10)
    except asyncio.TimeoutError:
        version.kill()
        await version.wait()
        raise RuntimeError("Native version check timed out")
    if version.returncode or out.decode().strip() != SUPPORTED_VERSION:
        raise RuntimeError(
            "This adapter is validated for agy 1.5.5 only; revalidate after upgrades"
        )
    proc = await asyncio.create_subprocess_exec(
        args.native,
        "acp",
        "--agent",
        "native-swarm-acp",
        "--app-data-dir",
        args.app_data_name,
        "--process-per-session",
        *(["--dangerously-skip-permissions"] if args.approval == "auto" else []),
        stdin=asyncio.subprocess.PIPE,
        stdout=asyncio.subprocess.PIPE,
        stderr=sys.stderr,
        limit=MAX_LINE,
        start_new_session=True,
    )
    loop = asyncio.get_running_loop()
    reader = asyncio.StreamReader(limit=MAX_LINE)
    transport, _ = await loop.connect_read_pipe(
        lambda: asyncio.StreamReaderProtocol(reader), sys.stdin.buffer
    )
    out_transport, protocol = await loop.connect_write_pipe(
        asyncio.streams.FlowControlMixin, sys.stdout.buffer
    )
    writer = asyncio.StreamWriter(out_transport, protocol, None, loop)
    output_lock = asyncio.Lock()

    async def emit(msg):
        data = packet(msg)
        if len(data) > MAX_LINE:
            raise ValueError("Output exceeds 16 MiB")
        async with output_lock:
            writer.write(data)
            await asyncio.wait_for(writer.drain(), 15)

    adapter = Adapter(
        proc,
        emit,
        Path.home() / ".gemini" / args.app_data_name,
        args.poll_seconds,
        args.approval,
    )

    async def client_loop():
        while line := await reader.readline():
            try:
                msg = json.loads(line)
            except (ValueError, UnicodeError):
                await emit(error(None, "Invalid JSON", -32700))
                continue
            await adapter.client(msg)

    async def native_loop():
        while line := await proc.stdout.readline():
            msg = json.loads(line)
            if not isinstance(msg, dict) or msg.get("jsonrpc") != "2.0":
                raise ValueError("Invalid native ACP output")
            await adapter.native(msg)
        for ident in list(adapter.calls):
            await emit(
                error(ident, "Native ACP exited; operation outcome may be unknown")
            )
        await emit(
            {
                "jsonrpc": "2.0",
                "method": PREFIX + "error",
                "params": {"message": "Native ACP exited", "runtimeVerified": False},
            }
        )

    tasks = [
        asyncio.create_task(client_loop()),
        asyncio.create_task(native_loop()),
        asyncio.create_task(adapter.watch()),
    ]
    current = asyncio.current_task()
    for sig in (signal.SIGTERM, signal.SIGINT):
        loop.add_signal_handler(sig, current.cancel)
    try:
        done, _ = await asyncio.wait(tasks, return_when=asyncio.FIRST_COMPLETED)
        for task in done:
            task.result()
    finally:
        for task in tasks:
            task.cancel()
        await asyncio.gather(*tasks, return_exceptions=True)
        with contextlib.suppress(ProcessLookupError):
            os.killpg(proc.pid, signal.SIGTERM)
        try:
            await asyncio.wait_for(proc.wait(), 5)
        except asyncio.TimeoutError:
            with contextlib.suppress(ProcessLookupError):
                os.killpg(proc.pid, signal.SIGKILL)
            await proc.wait()
        transport.close()
        writer.close()


def main():
    p = argparse.ArgumentParser(description=__doc__)
    p.add_argument(
        "--native",
        default=os.environ.get(
            "AGY_NATIVE_BINARY", str(Path.home() / ".local/bin/agy")
        ),
    )
    p.add_argument("--app-data-name", default="antigravity-swarm-acp")
    p.add_argument("--poll-seconds", type=float, default=1)
    p.add_argument(
        "--approval",
        choices=("auto", "ask"),
        default="auto",
        help="auto uses native --dangerously-skip-permissions (default); ask forwards native approvals",
    )
    args = p.parse_args()
    if not args.app_data_name or any(
        c not in "abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ0123456789-_"
        for c in args.app_data_name
    ):
        p.error("app-data-name must be a single directory name")
    if not 0.2 <= args.poll_seconds <= 60:
        p.error("poll-seconds must be between .2 and 60")
    os.umask(0o077)
    root = Path.home() / ".gemini" / args.app_data_name
    root.mkdir(parents=True, exist_ok=True, mode=0o700)
    try:
        # One owner per native app directory prevents loading a live swarm twice.
        with (root / ".swarm-acp.lock").open("a") as lock:
            fcntl.flock(lock, fcntl.LOCK_EX | fcntl.LOCK_NB)
            asyncio.run(run(args))
    except asyncio.CancelledError:
        pass
    except (OSError, ValueError, RuntimeError) as exc:
        print("agy-swarm-acp: " + str(exc), file=sys.stderr)
        raise SystemExit(1)


if __name__ == "__main__":
    main()
