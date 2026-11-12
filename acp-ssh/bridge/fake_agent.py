# SPDX-License-Identifier: Apache-2.0
"""Deterministic ACP fixture; never contacts a model or executes tools."""

import asyncio
import json
import os
import sys


async def main():
    pending = {}
    turns = {}
    count = 0

    def emit(msg):
        print(json.dumps({"jsonrpc": "2.0", **msg}), flush=True)

    def update(sid, text):
        emit(
            {
                "method": "session/update",
                "params": {
                    "sessionId": sid,
                    "update": {
                        "sessionUpdate": "agent_message_chunk",
                        "content": {"type": "text", "text": text},
                    },
                },
            }
        )

    async def handle(m):
        nonlocal count
        method, p = m.get("method"), m.get("params", {})
        ident = m.get("id")
        if not method:
            pending[m["id"]].set_result(m)
            return
        if method == "initialize":
            emit(
                {
                    "id": ident,
                    "result": {
                        "protocolVersion": 1,
                        "agentInfo": {"name": "fixture", "version": "1"},
                        "agentCapabilities": {
                            "loadSession": True,
                            "sessionCapabilities": {"list": {}, "resume": {}},
                            "_meta": {"fixture": True},
                        },
                        "_meta": {"received": p},
                    },
                }
            )
            return
        if method == "session/new":
            count += 1
            emit(
                {"id": ident, "result": {"sessionId": f"s{count}", "configOptions": []}}
            )
            return
        if method == "session/prompt":
            sid = p["sessionId"]
            text = p["prompt"][0]["text"]
            turns[sid] = asyncio.Event()
            update(sid, "started: " + text)
            if text == "crash":
                os._exit(17)
            if text in ("permission", "reverse", "background-permission"):
                rid = "reverse-" + sid
                pending[rid] = asyncio.get_running_loop().create_future()
                rmethod = (
                    "session/request_permission"
                    if text in ("permission", "background-permission")
                    else "fs/read_text_file"
                )
                params = {
                    "sessionId": sid,
                    "path": "/remote/file",
                    "options": [
                        {"optionId": "yes", "kind": "allow_once", "name": "Allow"}
                    ],
                }
                emit(
                    {
                        "id": rid,
                        "method": rmethod,
                        "params": params,
                        "_meta": {"roundtrip": True},
                    }
                )
                if text == "background-permission":
                    emit({"id": ident, "result": {"stopReason": "end_turn"}})
                answer = await pending[rid]
                update(sid, json.dumps(answer))
                if text == "background-permission":
                    return
            elif text == "wait":
                await turns[sid].wait()
            else:
                await asyncio.sleep(0.35)
            update(sid, "done")
            emit(
                {
                    "id": ident,
                    "result": {
                        "stopReason": "cancelled" if turns[sid].is_set() else "end_turn"
                    },
                }
            )
            return
        if method == "session/cancel":
            if p["sessionId"] in turns:
                turns[p["sessionId"]].set()
            return
        if method == "session/load":
            update(p["sessionId"], "history")
        if "id" in m:
            emit({"id": ident, "result": {"echo": m}, "_meta": {"extension": True}})

    reader = asyncio.StreamReader()
    await asyncio.get_running_loop().connect_read_pipe(
        lambda: asyncio.StreamReaderProtocol(reader), sys.stdin.buffer
    )
    tasks = set()
    while line := await reader.readline():
        t = asyncio.create_task(handle(json.loads(line)))
        tasks.add(t)
        t.add_done_callback(tasks.discard)


asyncio.run(main())
