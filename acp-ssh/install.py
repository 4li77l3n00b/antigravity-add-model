#!/usr/bin/env python3
# SPDX-License-Identifier: Apache-2.0
"""Install a self-contained user copy. Never download agy or start/restart services."""

import argparse
import json
import os
from pathlib import Path
import shlex
import shutil
import sys
import tempfile

MARKER = "Managed by acp-ssh component"
ROOT = Path(__file__).resolve().parent
RUNTIME = {
    "bridge": ("acp_ssh.py", "native_delete.py", "enable_native_delete.py"),
    "gateway": ("gateway_adapter.py", "launch.py"),
    "swarm": ("server.py", "native_store.py", "native-swarm-acp.md"),
}
ENTRIES = {
    "acp-ssh": "bridge/acp_ssh.py",
    "acp-ssh-enable-delete": "bridge/enable_native_delete.py",
    "agy-gateway": "gateway/launch.py",
    "agy-swarm-acp": "swarm/server.py",
}


def owned_target(path, exact=None):
    if path.is_symlink():
        raise ValueError("Refusing a symlink installation target: " + str(path))
    if path.exists() and (
        not path.is_file()
        or (
            MARKER not in path.read_text()
            and (exact is None or path.read_bytes() != exact)
        )
    ):
        raise ValueError("Refusing to overwrite an unrelated file: " + str(path))


def atomic_write(path, data, mode):
    path.parent.mkdir(parents=True, exist_ok=True)
    fd, name = tempfile.mkstemp(prefix="." + path.name + ".", dir=path.parent)
    try:
        os.fchmod(fd, mode)
        with os.fdopen(fd, "wb") as f:
            f.write(data)
            f.flush()
            os.fsync(f.fileno())
        os.replace(name, path)
    finally:
        Path(name).unlink(missing_ok=True)


def install(prefix, native=None, systemd=False, with_swarm=False, upgrade=False):
    if sys.version_info < (3, 11):
        raise ValueError("Python 3.11 or newer is required")
    prefix = Path(prefix).expanduser().absolute()
    if any(c in str(prefix) for c in "\n\r\0"):
        raise ValueError("Invalid prefix")
    library = prefix / "lib/acp-ssh"
    binary_dir = prefix / "bin"
    if library.is_symlink():
        raise ValueError("Refusing symlink library directory")
    if library.exists() and (
        not upgrade or not (library / ".acp-ssh-component").is_file()
    ):
        raise ValueError(
            "Library already exists; stop its hosts and use --upgrade only for an owned installation"
        )
    if native:
        native = Path(native).expanduser().resolve(strict=True)
        if not native.is_file() or not os.access(native, os.X_OK):
            raise ValueError("Native binary must be executable")
        if native in [p.resolve() for p in (binary_dir / name for name in ENTRIES)]:
            raise ValueError("Native binary cannot be a component wrapper")
    unit = (
        Path(os.environ.get("XDG_CONFIG_HOME", str(Path.home() / ".config")))
        / "systemd/user/acp-ssh@.service"
    )
    agent = Path.home() / ".gemini/config/agents/native-swarm-acp.md"
    for name in ENTRIES:
        owned_target(binary_dir / name)
    if systemd:
        owned_target(unit)
    if with_swarm:
        owned_target(agent, (ROOT / "swarm/native-swarm-acp.md").read_bytes())
    library.parent.mkdir(parents=True, exist_ok=True)
    stage = Path(tempfile.mkdtemp(prefix=".acp-ssh-stage-", dir=library.parent))
    backup = None
    try:
        for directory, names in RUNTIME.items():
            (stage / directory).mkdir()
            for name in names:
                shutil.copyfile(ROOT / directory / name, stage / directory / name)
        for name in ("LICENSE", "NOTICE", "README.md"):
            shutil.copyfile(ROOT / name, stage / name)
        shutil.copytree(ROOT / "docs", stage / "docs")
        (stage / ".acp-ssh-component").write_text(MARKER + "\n")
        if library.exists():
            backup = Path(
                tempfile.mkdtemp(prefix=".acp-ssh-previous-", dir=library.parent)
            )
            backup.rmdir()
            os.replace(library, backup)
        os.replace(stage, library)
        for name, script in ENTRIES.items():
            # The installed entry references only the installed copy, never ROOT.
            line = "#!/bin/sh\n# " + MARKER + "\n"
            if native:
                line += (
                    'if [ -z "${AGY_NATIVE_BINARY:-}" ]; then\n  export AGY_NATIVE_BINARY='
                    + shlex.quote(str(native))
                    + "\nfi\n"
                )
            line += (
                "exec "
                + shlex.quote(sys.executable)
                + " "
                + shlex.quote(str(library / script))
                + ' "$@"\n'
            )
            atomic_write(binary_dir / name, line.encode(), 0o700)
        if with_swarm:
            data = (
                (ROOT / "swarm/native-swarm-acp.md")
                .read_text()
                .replace("---\n", "---\n# " + MARKER + "\n", 1)
            )
            atomic_write(agent, data.encode(), 0o600)
        if systemd:

            def quote(value):
                return (
                    '"'
                    + str(value)
                    .replace("\\", "\\\\")
                    .replace('"', '\\"')
                    .replace("%", "%%")
                    + '"'
                )

            body = f"""# {MARKER}
[Unit]
Description=Persistent ACP over SSH (%i)

[Service]
Type=simple
ExecStart={quote(binary_dir / "acp-ssh")} serve %i
Environment={quote("PATH=" + str(binary_dir) + ":/usr/local/bin:/usr/bin:/bin")}
EnvironmentFile=-%h/.config/acp-ssh/environment
WorkingDirectory=%h
UMask=0077
KillMode=control-group
TimeoutStopSec=15
Restart=no
StandardOutput=journal
StandardError=journal
"""
            atomic_write(unit, body.encode(), 0o600)
        result = {
            "prefix": str(prefix),
            "library": str(library),
            "commands": list(ENTRIES),
            "servicesStarted": False,
        }
        if systemd:
            result["systemdUnit"] = str(unit)
            result["nextStep"] = (
                "systemctl --user daemon-reload (does not start a host)"
            )
        if backup:
            result["previousLibrary"] = str(backup)
        return result
    finally:
        if stage.exists():
            shutil.rmtree(stage)


if __name__ == "__main__":
    os.umask(0o077)
    p = argparse.ArgumentParser(description=__doc__)
    p.add_argument("--prefix", default=str(Path.home() / ".local"))
    p.add_argument(
        "--native-binary",
        help="Existing native agy executable; never copied into this package",
    )
    p.add_argument(
        "--systemd",
        action="store_true",
        help="Write user unit; do not reload or start systemd",
    )
    p.add_argument(
        "--with-swarm", action="store_true", help="Install the native agent definition"
    )
    p.add_argument(
        "--upgrade",
        action="store_true",
        help="Replace an owned library after its hosts have been stopped",
    )
    a = p.parse_args()
    try:
        print(
            json.dumps(
                install(a.prefix, a.native_binary, a.systemd, a.with_swarm, a.upgrade),
                indent=2,
            )
        )
    except (ValueError, OSError) as exc:
        raise SystemExit(str(exc))
