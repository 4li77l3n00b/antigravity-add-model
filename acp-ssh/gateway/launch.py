#!/usr/bin/env python3
# SPDX-License-Identifier: Apache-2.0
"""Optional agy gateway launcher. Uses explicit configuration; never reads IDE auth."""

import json
import os
from pathlib import Path
import re
import shutil
import sys
from gateway_adapter import CATALOG, exec_native

UTILITY = {
    "models",
    "agent",
    "agents",
    "help",
    "changelog",
    "install",
    "update",
    "mcp",
    "plugin",
    "plugins",
    "mic-serve",
    "remote-control",
    "--help",
    "-h",
    "--version",
    "-version",
}


def configuration(env=None):
    env = dict(os.environ if env is None else env)
    config_home = Path(
        env.get(
            "XDG_CONFIG_HOME", str(Path(env.get("HOME", str(Path.home()))) / ".config")
        )
    )
    config_path = Path(
        env.get("AGY_GATEWAY_CONFIG", str(config_home / "acp-ssh/gateway.json"))
    )
    explicit = "AGY_GATEWAY_CONFIG" in env
    config = {}
    if config_path.exists() or explicit:
        try:
            config = json.loads(config_path.read_text())
        except (OSError, ValueError):
            raise ValueError("Cannot read gateway configuration JSON")
        if not isinstance(config, dict):
            raise ValueError("Gateway configuration must be an object")
    native = (
        env.get("AGY_NATIVE_BINARY")
        or config.get("native_binary")
        or shutil.which("agy", path=env.get("PATH"))
    )
    if not native:
        raise ValueError(
            "Set AGY_NATIVE_BINARY to your separately installed native agy executable"
        )
    binary = Path(native).expanduser().absolute()
    if (
        binary.resolve() == Path(__file__).resolve()
        or not binary.is_file()
        or not os.access(binary, os.X_OK)
    ):
        raise ValueError(
            "AGY_NATIVE_BINARY must identify an executable native agy, not this launcher"
        )
    for name, key in [
        ("AGY_LLM_GATEWAY_URL", "gateway_url"),
        ("AGY_LLM_GATEWAY_MODELS", "models"),
        ("AGY_LLM_GATEWAY_PROXY_URL", "proxy_url"),
        ("AGY_LLM_GATEWAY_CA_CERT", "ca_cert"),
    ]:
        if name not in env and key in config:
            env[name] = config[key]
    key_env = config.get("api_key_env")
    if key_env:
        if not isinstance(key_env, str) or not re.fullmatch(
            r"[A-Za-z_][A-Za-z0-9_]*", key_env
        ):
            raise ValueError("api_key_env must be an environment variable name")
        env.setdefault("AGY_LLM_GATEWAY_API_KEY", env.get(key_env, ""))
    env.setdefault("AGY_LLM_GATEWAY_MODELS", CATALOG)
    env.setdefault("AGY_LLM_GATEWAY_WIRE_PROTOCOL", "openai")
    return binary, env


def main():
    binary, env = configuration()
    args = sys.argv[1:]
    if (args and args[0] in UTILITY) or env.get("AGY_GATEWAY_ADAPTER") == "0":
        os.execve(str(binary), [str(binary), *args], env)
    if not env.get("AGY_LLM_GATEWAY_URL") or not env.get("AGY_LLM_GATEWAY_API_KEY"):
        raise ValueError(
            "Set AGY_LLM_GATEWAY_URL and AGY_LLM_GATEWAY_API_KEY, or configure gateway_url and api_key_env"
        )
    if env["AGY_LLM_GATEWAY_WIRE_PROTOCOL"] != "openai":
        raise ValueError(
            "The optional effort adapter requires OpenAI-compatible wire format; use native agy directly for other formats"
        )
    exec_native(binary, args, env)


if __name__ == "__main__":
    try:
        main()
    except (OSError, ValueError, RuntimeError):
        # Configuration paths, URLs and upstream errors may contain secrets.
        sys.exit(
            "agy-gateway: configuration/startup failed; check native binary, gateway URL and API-key environment settings"
        )
