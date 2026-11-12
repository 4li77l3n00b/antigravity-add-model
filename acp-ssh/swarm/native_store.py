# SPDX-License-Identifier: Apache-2.0
"""Read-only view of agy 1.5.5's native local swarm files. Never schedule or edit."""

import hashlib
import json
import os
from pathlib import Path
import re
import stat

ID = re.compile(r"^[A-Za-z0-9][A-Za-z0-9_-]{0,127}$")
MAX_FILE = 16 * 1024 * 1024


class NativeStore:
    def __init__(self, root):
        self.root = Path(root).resolve()

    def path(self, conversation, *parts):
        if not isinstance(conversation, str) or not ID.fullmatch(conversation):
            raise ValueError("Invalid native conversation ID")
        path = self.root / "brain" / conversation / ".system_generated"
        for part in parts:
            path /= part
        # Do not follow native data symlinks outside this configured app directory.
        if not path.resolve().is_relative_to(self.root):
            raise ValueError("Native data path escapes configured app directory")
        return path

    def read(self, path, limit=MAX_FILE):
        # O_NONBLOCK prevents a malicious FIFO from blocking a protocol handler.
        fd = os.open(path, os.O_RDONLY | os.O_NONBLOCK | os.O_NOFOLLOW)
        with os.fdopen(fd, "rb") as f:
            st = os.fstat(f.fileno())
            if not stat.S_ISREG(st.st_mode):
                raise ValueError("Native data must be a regular file")
            data = f.read(limit + 1)
            if len(data) > limit:
                raise ValueError(
                    "Native file exceeds read limit; use paginated history"
                )
            return data

    def roster(self, sid):
        try:
            anchor = json.loads(self.read(self.path(sid, "swarm", "swarm.json")))
        except FileNotFoundError:
            anchor = None
        if anchor and anchor.get("backend") != "local":
            raise ValueError("Only native local swarm storage is supported")
        try:
            roster = json.loads(self.read(self.path(sid, "swarm", "members.json")))
        except FileNotFoundError:
            return None
        if not isinstance(roster, dict) or not isinstance(roster.get("members"), dict):
            raise ValueError("Unrecognized native roster schema")
        for name, member in roster["members"].items():
            if not isinstance(member, dict) or not ID.fullmatch(
                str(member.get("conversation_id", ""))
            ):
                raise ValueError("Unrecognized native member schema")
        return roster

    def revision(self, sid):
        path = self.path(sid, "logs", "transcript.jsonl")
        try:
            st = path.stat()
            return f"{st.st_dev}:{st.st_ino}:{st.st_size}:{st.st_mtime_ns}"
        except FileNotFoundError:
            return None

    def snapshot(self, sid, after=0, limit=100):
        if (
            type(after) is not int
            or after < 0
            or type(limit) is not int
            or not 1 <= limit <= 1000
        ):
            raise ValueError("after must be nonnegative; limit must be 1..1000")
        roster = self.roster(sid)
        result = {
            "sessionId": sid,
            "source": "native-local-files",
            "runtimeVerified": False,
            "swarm": None,
            "members": [],
            "messages": [],
            "cursor": after,
            "more": False,
            "partialTail": False,
        }
        if roster is None:
            result["etag"] = "absent"
            return result
        result["swarm"] = {"id": sid, "title": roster.get("title", "")}
        for name, member in roster["members"].items():
            result["members"].append(
                {
                    "name": name,
                    "conversationId": member["conversation_id"],
                    "parentSessionId": sid,
                    "native": member,
                    "transcriptRevision": self.revision(member["conversation_id"]),
                }
            )
        try:
            board = self.read(self.path(sid, "swarm", "board.jsonl"))
        except FileNotFoundError:
            board = b""
        result["partialTail"] = bool(board and not board.endswith(b"\n"))
        lines = board.splitlines(keepends=True)
        messages = []
        last = 0
        for line in lines:
            if not line.endswith(b"\n"):
                break
            if not line.strip():
                continue
            message = json.loads(line)
            seq = message.get("seq")
            if type(seq) is not int or seq <= last:
                raise ValueError(
                    "Native board sequence is invalid; do not advance cursor"
                )
            last = seq
            if seq > after:
                messages.append(message)
        result["messages"] = messages[:limit]
        result["cursor"] = (
            result["messages"][-1]["seq"] if result["messages"] else after
        )
        result["head"] = last
        result["resetRequired"] = after > last
        result["more"] = len(messages) > limit
        # Includes worker transcript revisions so subscriptions notice worker activity.
        result["etag"] = hashlib.sha256(
            json.dumps(
                [roster, result["members"], board.decode()], sort_keys=True
            ).encode()
        ).hexdigest()
        return result

    def history(self, sid, member_name, cursor=None, limit=100):
        if type(limit) is not int or not 1 <= limit <= 1000:
            raise ValueError("limit must be 1..1000")
        roster = self.roster(sid)
        if roster is None and member_name == "coordinator":
            conversation = (
                sid  # Registered coordinator before its first swarm board exists.
            )
        elif roster is None or member_name not in roster["members"]:
            raise ValueError("Member is not in this native swarm")
        else:
            conversation = roster["members"][member_name]["conversation_id"]
        path = self.path(conversation, "logs", "transcript.jsonl")
        cursor = cursor or {}
        offset = cursor.get("offset", 0)
        if type(offset) is not int or offset < 0:
            raise ValueError("Invalid history offset")
        result = {
            "sessionId": sid,
            "member": member_name,
            "conversationId": conversation,
            "source": "native-transcript",
            "records": [],
            "resetRequired": False,
            "more": False,
        }
        try:
            fd = os.open(path, os.O_RDONLY | os.O_NONBLOCK | os.O_NOFOLLOW)
        except FileNotFoundError:
            return dict(result, cursor={"offset": 0}, resetRequired=bool(offset))
        with os.fdopen(fd, "rb") as f:
            st = os.fstat(f.fileno())
            if not stat.S_ISREG(st.st_mode):
                raise ValueError("Transcript must be a regular file")
            identity = f"{st.st_dev}:{st.st_ino}"
            # Native rewrites compact transcripts. Check mtime as well as inode/size.
            generation = f"{identity}:{st.st_mtime_ns}:{st.st_size}"
            if cursor and (
                cursor.get("generation") != generation or offset > st.st_size
            ):
                return dict(
                    result,
                    resetRequired=True,
                    cursor={"offset": 0, "generation": generation},
                )
            if offset:
                f.seek(offset - 1)
                if f.read(1) != b"\n":
                    raise ValueError("History offset is not at a record boundary")
            else:
                f.seek(0)
            used = 0
            while len(result["records"]) < limit:
                start = f.tell()
                line = f.readline(MAX_FILE + 1)
                if not line:
                    break
                if len(line) > MAX_FILE:
                    raise ValueError("Native transcript record is too large")
                if not line.endswith(b"\n"):
                    f.seek(start)
                    break
                record = json.loads(line)
                if result["records"] and used + len(line) > 1024 * 1024:
                    f.seek(start)
                    break
                if len(line) > 1024 * 1024:
                    raise ValueError("Native transcript record exceeds response limit")
                result["records"].append(record)
                used += len(line)
            result["cursor"] = {"offset": f.tell(), "generation": generation}
            result["more"] = f.tell() < st.st_size
        return result
