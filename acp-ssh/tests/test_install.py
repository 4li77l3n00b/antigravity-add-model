# SPDX-License-Identifier: Apache-2.0
import json
import os
from pathlib import Path
import select
import shutil
import subprocess
import sys
import tempfile
import time
import unittest

ROOT = Path(__file__).resolve().parents[1]


class InstallationTests(unittest.TestCase):
    def test_copied_runtime_survives_source_move(self):
        with tempfile.TemporaryDirectory(prefix="acp install ") as directory:
            home = Path(directory)
            source = home / "source checkout"
            prefix = home / "local prefix"
            shutil.copytree(ROOT, source, ignore=shutil.ignore_patterns("__pycache__"))
            native = home / "fake native"
            native.write_text('#!/bin/sh\nprintf "1.5.5\\n"\n')
            native.chmod(0o700)
            blocker = home / "commands"
            blocker.mkdir()
            (blocker / "systemctl").write_text("#!/bin/sh\nexit 93\n")
            (blocker / "systemctl").chmod(0o700)
            env = {
                "HOME": str(home),
                "PATH": str(blocker)
                + os.pathsep
                + os.environ.get("PATH", "/usr/bin:/bin"),
                "ACP_SSH_HOME": str(home / "state"),
                "LANG": "C.UTF-8",
            }
            installed = subprocess.run(
                [
                    sys.executable,
                    str(source / "install.py"),
                    "--prefix",
                    str(prefix),
                    "--native-binary",
                    str(native),
                    "--systemd",
                    "--with-swarm",
                ],
                env=env,
                capture_output=True,
                text=True,
                timeout=15,
            )
            self.assertEqual(installed.returncode, 0, installed.stderr)
            self.assertFalse(json.loads(installed.stdout)["servicesStarted"])
            moved = home / "source moved"
            source.rename(moved)
            entry = prefix / "bin/acp-ssh"
            for command in ("acp-ssh", "acp-ssh-enable-delete", "agy-swarm-acp"):
                p = subprocess.run(
                    [str(prefix / "bin" / command), "--help"],
                    env=env,
                    capture_output=True,
                    text=True,
                    timeout=8,
                )
                self.assertEqual(p.returncode, 0, p.stderr)
                self.assertNotIn(str(source), (prefix / "bin" / command).read_text())
            self.assertEqual(
                subprocess.check_output(
                    [str(prefix / "bin/agy-gateway"), "--version"],
                    env=env,
                    text=True,
                    timeout=8,
                ).strip(),
                "1.5.5",
            )
            self.assertTrue(
                (home / ".gemini/config/agents/native-swarm-acp.md").exists()
            )
            unit = (home / ".config/systemd/user/acp-ssh@.service").read_text()
            self.assertIn(str(entry), unit)
            self.assertNotIn(str(source), unit)
            subprocess.run(
                [
                    str(entry),
                    "configure",
                    "fixture",
                    "--cwd",
                    str(home),
                    "--command",
                    sys.executable,
                    str(moved / "bridge/fake_agent.py"),
                    "--native",
                    "downstream-only",
                ],
                env=env,
                check=True,
                stdout=subprocess.DEVNULL,
                timeout=8,
            )
            controller = None
            try:
                subprocess.run(
                    [str(entry), "start", "fixture", "--detached"],
                    env=env,
                    check=True,
                    stdout=subprocess.DEVNULL,
                    timeout=10,
                )
                controller = subprocess.Popen(
                    [str(entry), "connect", "fixture"],
                    env=env,
                    stdin=subprocess.PIPE,
                    stdout=subprocess.PIPE,
                    stderr=subprocess.PIPE,
                    text=True,
                )
                controller.stdin.write(
                    json.dumps(
                        {
                            "jsonrpc": "2.0",
                            "id": 1,
                            "method": "initialize",
                            "params": {"protocolVersion": 1, "clientCapabilities": {}},
                        }
                    )
                    + "\n"
                )
                controller.stdin.flush()
                self.assertTrue(select.select([controller.stdout], [], [], 5)[0])
                self.assertEqual(
                    json.loads(controller.stdout.readline())["result"]["agentInfo"][
                        "name"
                    ],
                    "fixture",
                )
            finally:
                if controller:
                    controller.stdin.close()
                    try:
                        controller.wait(timeout=5)
                    except subprocess.TimeoutExpired:
                        controller.kill()
                        controller.wait()
                    controller.stdout.close()
                    controller.stderr.close()
                subprocess.run(
                    [str(entry), "stop", "fixture"],
                    env=env,
                    stdout=subprocess.DEVNULL,
                    stderr=subprocess.PIPE,
                    timeout=8,
                )
            for _ in range(50):
                p = subprocess.run(
                    [str(entry), "status", "fixture"],
                    env=env,
                    stdout=subprocess.DEVNULL,
                    stderr=subprocess.DEVNULL,
                    timeout=3,
                )
                if p.returncode:
                    break
                time.sleep(0.03)
            self.assertNotEqual(p.returncode, 0)

    def test_unknown_entry_is_not_overwritten(self):
        with tempfile.TemporaryDirectory() as directory:
            home = Path(directory)
            prefix = home / ".local"
            (prefix / "bin").mkdir(parents=True)
            target = prefix / "bin/acp-ssh"
            target.write_text("unrelated custom command")
            p = subprocess.run(
                [sys.executable, str(ROOT / "install.py"), "--prefix", str(prefix)],
                env={**os.environ, "HOME": str(home)},
                capture_output=True,
                text=True,
                timeout=8,
            )
            self.assertNotEqual(p.returncode, 0)
            self.assertEqual(target.read_text(), "unrelated custom command")
            self.assertFalse((prefix / "lib/acp-ssh").exists())


if __name__ == "__main__":
    unittest.main()
