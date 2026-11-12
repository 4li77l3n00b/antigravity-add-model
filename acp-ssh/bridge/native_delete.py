# SPDX-License-Identifier: Apache-2.0
"""Opt-in native agy 1.5.5 permanent deletion. Never unlink native history ourselves.

Only standalone local conversations are supported. The native LS performs deletion;
this module verifies its result. No prompt, external endpoint, or credential is used.
"""

import asyncio
import contextlib
import hashlib
import http.client
import json
import os
from pathlib import Path
import re
import signal
import sqlite3
import stat
import tempfile
import uuid

METHOD = "_maichart/session/delete"
META = "maichart/sessionManagement"
NATIVE_VERSION = "1.5.5"
NATIVE_SHA256 = "89f16bdb87f62e6d07484c52e6bafe76a81b0fd13d740110c77dc3c3c8d8d8b6"
RPC_PATH = "/exa.language_server_pb.LanguageServerService/DeleteCascadeTrajectory"


class DeletionError(Exception):
    def __init__(
        self, reason, message, *, retryable=False, uncertain=False, code=-32050
    ):
        super().__init__(message)
        self.code = code
        self.data = {
            "reason": reason,
            "retryable": retryable,
            "mayBeDeleted": uncertain,
        }


def valid_id(sid):
    try:
        if not isinstance(sid, str) or str(uuid.UUID(sid)) != sid:
            raise ValueError()
    except (ValueError, AttributeError):
        raise DeletionError(
            "INVALID_SESSION_ID", "Expected a canonical UUID sessionId", code=-32602
        )
    return sid


def regular(path):
    """Reject links, sockets and FIFOs, including in existing ancestors."""
    for item in (path, *path.parents):
        try:
            st = item.lstat()
        except FileNotFoundError:
            continue
        if stat.S_ISLNK(st.st_mode):
            raise DeletionError("UNSAFE_PATH", "Native storage contains a symlink")
        if not (stat.S_ISDIR(st.st_mode) or stat.S_ISREG(st.st_mode)):
            raise DeletionError("UNSAFE_PATH", "Native storage contains a special file")


def readonly_db(path):
    regular(path)
    return sqlite3.connect(path.as_uri() + "?mode=ro", uri=True, timeout=2)


class NativeDelete:
    def __init__(self, config):
        self.home = Path(config["home"]).absolute()
        self.binary = Path(config["binary"]).absolute()
        self.app = config["app_data_name"]
        if not isinstance(self.app, str) or not re.fullmatch(
            r"[A-Za-z0-9][A-Za-z0-9_-]{0,79}", self.app
        ):
            raise ValueError("Invalid native deletion app_data_name")
        self.root = self.home / ".gemini" / self.app
        regular(self.home)
        regular(self.root)
        regular(self.binary)
        self.assert_binary()

    def assert_binary(self):
        regular(self.binary)
        with self.binary.open("rb") as f:
            digest = hashlib.file_digest(f, "sha256").hexdigest()
        if digest != NATIVE_SHA256:
            raise DeletionError(
                "UNVERIFIED_NATIVE_VERSION",
                "Native deletion requires the verified agy 1.5.5 binary",
            )

    def capability(self):
        return {
            "version": 1,
            "delete": {
                "method": METHOD,
                "permanent": True,
                "idempotent": True,
                "relatedSessions": "reject",
                "activeSessions": "reject",
                "backend": "native-local",
                "nativeVersion": NATIVE_VERSION,
            },
        }

    def roots(self):
        base = self.home / ".gemini"
        regular(base)
        if not base.exists():
            return [self.root]
        result = {self.root}
        for p in base.iterdir():
            if p.is_dir() and any(
                (p / x).exists()
                for x in ("brain", "conversations", "conversation_summaries.db")
            ):
                regular(p)
                result.add(p)
        return sorted(result)

    def history_paths(self, root, sid):
        paths = [root / "brain" / sid]
        for name in ("conversations", "annotations"):
            folder = root / name
            regular(folder)
            if folder.exists():
                paths += [
                    p
                    for p in folder.iterdir()
                    if p.name == sid or p.name.startswith(sid + ".")
                ]
        return paths

    def legacy_reference(self, root, sid):
        legacy = root / "jetbox_summaries_proto.pb"
        regular(legacy)
        if not legacy.exists():
            return False
        if legacy.stat().st_size > 32 * 1024 * 1024:
            raise DeletionError(
                "UNKNOWN_NATIVE_SCHEMA", "Legacy summary exceeds verification limit"
            )
        return sid.encode() in legacy.read_bytes()

    def summary_rows(self, root):
        path = root / "conversation_summaries.db"
        if not path.exists():
            return []
        with contextlib.closing(readonly_db(path)) as db:
            columns = {
                x[1] for x in db.execute("PRAGMA table_info(conversation_summaries)")
            }
            wanted = (
                "conversation_id",
                "parent_conversation_id",
                "group_id",
                "battle_id",
                "winning_conversation_id",
                "nesting_depth",
                "not_fully_idle",
            )
            if not set(wanted).issubset(columns):
                raise DeletionError(
                    "UNKNOWN_NATIVE_SCHEMA", "Unrecognized native summary schema"
                )
            return [
                dict(zip(wanted, row))
                for row in db.execute(
                    "SELECT " + ",".join(wanted) + " FROM conversation_summaries"
                )
            ]

    def preflight(self, sid, *, allow_idle_flag=False):
        valid_id(sid)
        exists = False
        for root in self.roots():
            if self.legacy_reference(root, sid):
                if root != self.root:
                    raise DeletionError(
                        "OUT_OF_SCOPE", "Legacy summary exists in another app directory"
                    )
                exists = True
            for p in self.history_paths(root, sid):
                regular(p)
                if not p.exists():
                    continue
                if root != self.root:
                    raise DeletionError(
                        "OUT_OF_SCOPE",
                        "Session data exists in another native app directory",
                    )
                exists = True
                if p.is_dir():
                    for parent, dirs, files in os.walk(p, followlinks=False):
                        for name in dirs + files:
                            regular(Path(parent) / name)
            rows = self.summary_rows(root)
            for row in rows:
                ident = row["conversation_id"]
                if ident == sid:
                    if root != self.root:
                        raise DeletionError(
                            "OUT_OF_SCOPE",
                            "Session summary belongs to another app directory",
                        )
                    exists = True
                    if any(
                        row[k]
                        for k in (
                            "parent_conversation_id",
                            "group_id",
                            "battle_id",
                            "winning_conversation_id",
                            "nesting_depth",
                        )
                    ):
                        raise DeletionError(
                            "RELATED_SESSIONS",
                            "Grouped, child, battle and Swarm sessions require separate deletion support",
                        )
                    if row["not_fully_idle"] and not allow_idle_flag:
                        raise DeletionError(
                            "NATIVE_BUSY",
                            "Native summary marks this session non-idle",
                            retryable=True,
                        )
                elif any(
                    row[k] == sid
                    for k in (
                        "parent_conversation_id",
                        "group_id",
                        "battle_id",
                        "winning_conversation_id",
                    )
                ):
                    raise DeletionError(
                        "RELATED_SESSIONS", "Another session references this session"
                    )
            brain = root / "brain"
            regular(brain)
            if not brain.exists():
                continue
            for folder in brain.iterdir():
                swarm = folder / ".system_generated" / "swarm"
                regular(swarm)
                if not swarm.exists():
                    continue
                if folder.name == sid:
                    raise DeletionError(
                        "RELATED_SESSIONS",
                        "Swarm coordinator/member deletion is not supported",
                    )
                roster = swarm / "members.json"
                regular(roster)
                if not roster.exists():
                    raise DeletionError(
                        "UNKNOWN_SWARM_STATE", "Swarm membership cannot be verified"
                    )
                if roster.stat().st_size > 16 * 1024 * 1024:
                    raise DeletionError(
                        "UNKNOWN_SWARM_STATE", "Swarm roster exceeds verification limit"
                    )
                try:
                    members = json.loads(roster.read_bytes())["members"]
                    if not isinstance(members, dict) or not all(
                        isinstance(x, dict)
                        and isinstance(x.get("conversation_id"), str)
                        for x in members.values()
                    ):
                        raise ValueError()
                except (ValueError, KeyError, TypeError):
                    raise DeletionError(
                        "UNKNOWN_SWARM_STATE", "Unrecognized Swarm roster"
                    )
                if any(x["conversation_id"] == sid for x in members.values()):
                    raise DeletionError(
                        "RELATED_SESSIONS", "Swarm roster references this session"
                    )
        return exists

    def assert_no_writer(self, sid):
        """Target must be closed before a maintenance process may load it."""
        prefix = str(self.root / "conversations" / sid)
        brain = str(self.root / "brain" / sid) + "/"
        for proc in Path("/proc").iterdir():
            if not proc.name.isdecimal():
                continue
            try:
                if proc.stat().st_uid != os.getuid():
                    continue
                # All same-user processes with inspectable fds, not just named agy.
                for fd in (proc / "fd").iterdir():
                    try:
                        target = os.readlink(fd)
                    except FileNotFoundError:
                        continue
                    if (
                        target == prefix
                        or target.startswith(prefix + ".")
                        or target.startswith(brain)
                    ):
                        raise DeletionError(
                            "EXTERNAL_WRITER",
                            "A process still holds target session storage open",
                            retryable=True,
                        )
            except (FileNotFoundError, ProcessLookupError):
                continue
            except PermissionError:
                # sshd is deliberately non-dumpable; native agy must be inspectable.
                try:
                    native = (proc / "exe").resolve() == self.binary.resolve()
                except (OSError, RuntimeError):
                    native = False
                if native:
                    raise DeletionError(
                        "UNVERIFIED_WRITER",
                        "Cannot inspect a native process",
                        retryable=True,
                    )

    def verify_absent(self, sid):
        if self.preflight(sid, allow_idle_flag=True):
            raise DeletionError(
                "DELETE_INCOMPLETE",
                "Native history or summary remains after deletion",
                retryable=True,
                uncertain=True,
            )

    def summary_peers(self, owner_pid):
        """Discover only this host's native LS instances; refuse a second writer host.

        Each process has its own summary cache. Native DeleteCascadeTrajectory in
        one LS does not invalidate another LS's cache; shutdown can re-save it.
        """
        result = []
        for proc in Path("/proc").iterdir():
            if not proc.name.isdecimal():
                continue
            try:
                if proc.stat().st_uid != os.getuid():
                    continue
                try:
                    executable = (proc / "exe").resolve()
                except PermissionError:
                    if (proc / "comm").read_text().strip() != self.binary.name:
                        continue
                    raise
                if executable != self.binary.resolve():
                    continue
                argv = (proc / "cmdline").read_bytes().decode().split("\0")
                environment = (proc / "environ").read_bytes().split(b"\0")
                if ("HOME=" + str(self.home)).encode() not in environment:
                    continue

                def option(names, default):
                    for i, arg in enumerate(argv):
                        if arg in names and i + 1 < len(argv):
                            return argv[i + 1]
                        for name in names:
                            if arg.startswith(name + "="):
                                return arg[len(name) + 1 :]
                    return default

                if (
                    option(("--app-data-dir", "--app_data_dir"), "antigravity-cli")
                    != self.app
                ):
                    continue
                if option(("--gemini-dir", "--gemini_dir"), ".gemini") != ".gemini":
                    raise DeletionError(
                        "UNSUPPORTED_STORAGE",
                        "Native process uses another Gemini data root",
                    )
                current = int(proc.name)
                for _ in range(128):
                    if current == owner_pid:
                        break
                    if current <= 1:
                        raise DeletionError(
                            "EXTERNAL_NATIVE_PROCESS",
                            "Another native host shares this app directory; close it first",
                            retryable=True,
                        )
                    status = (Path("/proc") / str(current) / "status").read_text()
                    current = int(re.search(r"^PPid:\s+(\d+)", status, re.M).group(1))
                else:
                    raise DeletionError(
                        "UNKNOWN_PROCESS_TREE",
                        "Cannot establish native process ownership",
                    )
                # Front processes without an LS have no local cache to invalidate.
                logs = list((self.root / "log").glob("acp-*-" + proc.name + ".log"))
                ports = set()
                for log in logs:
                    regular(log)
                    with log.open("rb") as f:
                        head = f.read(2 * 1024 * 1024).decode(errors="replace")
                    ports.update(
                        int(x)
                        for x in re.findall(
                            r"Language server listening on random port at (\d+) for HTTP(?:\r?\n|$)",
                            head,
                        )
                    )
                if not ports and "--process-per-session" in argv:
                    continue
                if len(ports) != 1:
                    raise DeletionError(
                        "UNKNOWN_NATIVE_ENDPOINT",
                        "Cannot identify a native summary-cache endpoint",
                        retryable=True,
                    )
                port = ports.pop()
                self.check_port_owner(int(proc.name), port)
                start = (proc / "stat").read_text().rsplit(")", 1)[1].split()[19]
                result.append((int(proc.name), start, port))
            except (FileNotFoundError, ProcessLookupError):
                continue
            except PermissionError:
                raise DeletionError(
                    "UNVERIFIED_NATIVE_PROCESS",
                    "Cannot inspect a native summary-cache owner",
                    retryable=True,
                )
        return result

    @staticmethod
    def check_port_owner(pid, port):
        proc = Path("/proc") / str(pid)
        sockets = set()
        for fd in (proc / "fd").iterdir():
            try:
                target = os.readlink(fd)
            except FileNotFoundError:
                continue
            if target.startswith("socket:["):
                sockets.add(target[8:-1])
        for table in ("tcp", "tcp6"):
            for line in (proc / "net" / table).read_text().splitlines()[1:]:
                parts = line.split()
                if (
                    parts[3] == "0A"
                    and int(parts[1].split(":")[1], 16) == port
                    and parts[9] in sockets
                ):
                    return
        raise DeletionError(
            "ENDPOINT_OWNER_CHANGED",
            "Native endpoint is no longer owned by its expected process",
            retryable=True,
        )

    @staticmethod
    def native_rpc(port, path, sid):
        conn = http.client.HTTPConnection("127.0.0.1", port, timeout=15)
        try:
            conn.request(
                "POST",
                path,
                json.dumps({"cascadeId": sid}),
                {"Content-Type": "application/json", "Connect-Protocol-Version": "1"},
            )
            response = conn.getresponse()
            data = response.read(65537)
            if response.status != 200 or len(data) > 65536 or json.loads(data) != {}:
                raise DeletionError(
                    "NATIVE_DELETE_FAILED",
                    "Native deletion RPC did not confirm success",
                    retryable=True,
                    uncertain=True,
                )
        finally:
            conn.close()

    async def execute(self, sid, owner_pid):
        """Maintenance ACP process uses the same store, fake gateway, no user prompt."""
        await asyncio.to_thread(self.assert_binary)
        await asyncio.to_thread(self.assert_no_writer, sid)
        peers = await asyncio.to_thread(self.summary_peers, owner_pid)
        with tempfile.TemporaryDirectory(prefix="agy-native-delete-") as temp:
            log = Path(temp) / "native.log"
            env = {
                "HOME": str(self.home),
                "PATH": os.environ.get("PATH", "/usr/bin:/bin"),
                "LANG": "C.UTF-8",
                "NO_PROXY": "*",
                "AGY_LLM_GATEWAY_URL": "http://127.0.0.1:1",
                "AGY_LLM_GATEWAY_WIRE_PROTOCOL": "openai",
                "AGY_LLM_GATEWAY_API_KEY": "maintenance-no-model",
                "AGY_LLM_GATEWAY_MODELS": "gemini-4-carbon-high",
            }
            proc = await asyncio.create_subprocess_exec(
                str(self.binary),
                "acp",
                "--app-data-dir",
                self.app,
                "--cli_log_file",
                str(log),
                "--disable-telemetry",
                "--include-default-customizations=false",
                cwd=temp,
                env=env,
                stdin=asyncio.subprocess.PIPE,
                stdout=asyncio.subprocess.PIPE,
                stderr=asyncio.subprocess.DEVNULL,
                start_new_session=True,
                limit=16 * 1024 * 1024,
            )
            counter = 0

            async def call(method, params):
                nonlocal counter
                counter += 1
                proc.stdin.write(
                    (
                        json.dumps(
                            {
                                "jsonrpc": "2.0",
                                "id": counter,
                                "method": method,
                                "params": params,
                            }
                        )
                        + "\n"
                    ).encode()
                )
                await proc.stdin.drain()
                async with asyncio.timeout(20):
                    while True:
                        line = await proc.stdout.readline()
                        if not line:
                            raise DeletionError(
                                "NATIVE_EXIT",
                                "Native maintenance process exited",
                                retryable=True,
                                uncertain=True,
                            )
                        msg = json.loads(line)
                        if "method" in msg and "id" in msg:
                            raise DeletionError(
                                "UNEXPECTED_CLIENT_REQUEST",
                                "Maintenance unexpectedly requires a client tool or approval",
                            )
                        if msg.get("id") == counter and "method" not in msg:
                            if "error" in msg:
                                raise DeletionError(
                                    "NATIVE_RPC_FAILED",
                                    "Native maintenance ACP request failed",
                                    retryable=True,
                                    uncertain=True,
                                )
                            return msg["result"]

            try:
                await call(
                    "initialize",
                    {
                        "protocolVersion": 1,
                        "clientInfo": {"name": "acp-ssh-delete", "version": "1"},
                        "clientCapabilities": {},
                    },
                )
                await call(
                    "session/load", {"sessionId": sid, "cwd": temp, "mcpServers": []}
                )
                # Private log belongs only to this maintenance process. Never scan a
                # production log or use a client-supplied port/URL for destructive RPC.
                text = log.read_text(errors="replace")
                match = re.search(
                    r"Language server listening on random port at (\d+) for HTTP(?:\r?\n|$)",
                    text,
                )
                if not match:
                    raise DeletionError(
                        "NO_NATIVE_ENDPOINT",
                        "Maintenance HTTP endpoint was not discovered",
                    )
                port = int(match.group(1))
                await asyncio.to_thread(self.check_port_owner, proc.pid, port)
                await asyncio.to_thread(self.native_rpc, port, RPC_PATH, sid)
                summary_path = (
                    "/exa.language_server_pb.LanguageServerService/JetboxDeleteSummary"
                )
                await asyncio.to_thread(self.native_rpc, port, summary_path, sid)
                for pid, start, peer_port in peers:
                    process = Path("/proc") / str(pid)
                    if not process.exists():
                        continue
                    if (
                        process.joinpath("stat")
                        .read_text()
                        .rsplit(")", 1)[1]
                        .split()[19]
                        != start
                    ):
                        raise DeletionError(
                            "NATIVE_PROCESS_CHANGED",
                            "Native summary-cache owner changed during deletion",
                            retryable=True,
                            uncertain=True,
                        )
                    await asyncio.to_thread(self.check_port_owner, pid, peer_port)
                    await asyncio.to_thread(
                        self.native_rpc, peer_port, summary_path, sid
                    )
                await call("session/close", {"sessionId": sid})
            finally:
                with contextlib.suppress(ProcessLookupError):
                    os.killpg(proc.pid, signal.SIGTERM)
                try:
                    await asyncio.wait_for(proc.wait(), 5)
                except asyncio.TimeoutError:
                    with contextlib.suppress(ProcessLookupError):
                        os.killpg(proc.pid, signal.SIGKILL)
                    await proc.wait()
                with contextlib.suppress(ProcessLookupError):
                    os.killpg(proc.pid, signal.SIGKILL)
        await asyncio.to_thread(self.verify_absent, sid)
        current = await asyncio.to_thread(self.summary_peers, owner_pid)
        if set(current) - set(peers):
            raise DeletionError(
                "NATIVE_PROCESS_CHANGED",
                "A new summary-cache owner appeared during deletion; retry to reconcile",
                retryable=True,
                uncertain=True,
            )
