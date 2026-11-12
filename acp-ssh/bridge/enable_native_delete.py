#!/usr/bin/env python3
# SPDX-License-Identifier: Apache-2.0
"""Preview/enable native deletion without replacing any other host settings.

--apply requires a stopped host, writes a private backup, and never starts/stops it.
"""

import argparse
import fcntl
import json
import os
from pathlib import Path
import tempfile
import uuid

import acp_ssh as bridge
from native_delete import NativeDelete, DeletionError, regular


def enable(name, app, binary, apply=False):
    folder, _ = bridge.paths(name)
    config = folder / "config.json"
    regular(config)
    data = {
        "home": str(Path.home()),
        "app_data_name": app,
        "binary": str(Path(binary).absolute()),
    }
    NativeDelete(data)
    original = config.read_bytes()
    parsed = json.loads(original)
    preview = {
        "instance": name,
        "native_delete": data,
        "preservedKeys": sorted(k for k in parsed if k != "native_delete"),
        "applied": False,
    }
    if not apply:
        return preview
    bridge.private_dir(folder)
    regular(folder / "host.lock")
    with (folder / "host.lock").open("a") as lock:
        try:
            fcntl.flock(lock, fcntl.LOCK_EX | fcntl.LOCK_NB)
        except BlockingIOError:
            raise RuntimeError(
                "Host is running. Disconnect the desktop, finish tasks, and stop this host before applying."
            )
        original = config.read_bytes()
        parsed = json.loads(original)
        if parsed.get("native_delete") == data:
            return {**preview, "alreadyConfigured": True}
        parsed["native_delete"] = data
        backup = folder / ("config.before-native-delete." + uuid.uuid4().hex + ".json")
        fd = os.open(backup, os.O_WRONLY | os.O_CREAT | os.O_EXCL, 0o600)
        with os.fdopen(fd, "wb") as f:
            f.write(original)
            f.flush()
            os.fsync(f.fileno())
        fd, path = tempfile.mkstemp(prefix="config.native-delete.", dir=folder)
        try:
            with os.fdopen(fd, "w") as f:
                json.dump(parsed, f, indent=2)
                f.write("\n")
                f.flush()
                os.fsync(f.fileno())
            os.replace(path, config)
            directory = os.open(folder, os.O_RDONLY | os.O_DIRECTORY)
            try:
                os.fsync(directory)
            finally:
                os.close(directory)
        finally:
            Path(path).unlink(missing_ok=True)
        return {**preview, "applied": True, "backup": str(backup)}


if __name__ == "__main__":
    p = argparse.ArgumentParser(description=__doc__)
    p.add_argument("name")
    p.add_argument("--app-data-name", required=True)
    p.add_argument(
        "--native-binary",
        default=os.environ.get(
            "AGY_NATIVE_BINARY", str(Path.home() / ".local/lib/antigravity-cli/agy")
        ),
    )
    p.add_argument("--apply", action="store_true")
    args = p.parse_args()
    try:
        print(
            json.dumps(
                enable(args.name, args.app_data_name, args.native_binary, args.apply),
                indent=2,
            )
        )
    except (OSError, ValueError, RuntimeError, DeletionError) as exc:
        raise SystemExit(str(exc))
