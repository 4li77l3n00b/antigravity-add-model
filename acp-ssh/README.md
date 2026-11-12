# ACP SSH

Persistent ACP over SSH for Linux, with optional Antigravity gateway and native Swarm adapters. Python **3.11+**, standard library only. Install the native agent separately; this directory contains no vendor binary, credentials, production configuration, or conversation data.

The bridge supports arbitrary ACP stdio agents. The optional native deletion and Swarm integrations are pinned to **agy 1.5.5**; deletion additionally checks the tested Linux binary SHA-256. An unsupported build must be revalidated, not forced through the gate.

## Install

From this directory:

```bash
python3 install.py --native-binary /absolute/path/to/native/agy --systemd
export PATH="$HOME/.local/bin:$PATH"
systemctl --user daemon-reload
```

The installer copies runtime code to `~/.local/lib/acp-ssh`, so the checkout can be moved or removed. It installs `acp-ssh`, `acp-ssh-enable-delete`, `agy-gateway`, and `agy-swarm-acp` under `~/.local/bin`. It does **not** replace `agy`, fetch binaries, start hosts, or modify an existing host configuration. `--prefix /path` changes the install prefix. `--with-swarm` also installs the native agent definition. Unknown existing files are not overwritten; `--upgrade` is for an owned installation after its hosts have stopped.

Without systemd, omit `--systemd` and use `acp-ssh start NAME --detached`. Both modes preserve work when the SSH client disconnects.

## Configure and connect

```bash
acp-ssh configure agy --cwd /absolute/server/workspace \
  --command /absolute/path/to/native/agy acp --process-per-session
acp-ssh start agy
acp-ssh status agy
```

On the client, configure public-key SSH and verify the host key. A client launches:

```bash
ssh -T -o BatchMode=yes -o ServerAliveInterval=30 -o ServerAliveCountMax=3 \
  user@server /absolute/server/home/.local/bin/acp-ssh connect agy
```

Only one controller may attach to a named host. On reconnect, use the same ACP `initialize` profile. SSH EOF detaches the controller; it does not cancel accepted prompts or kill the agent. Server-side SSH client-alive settings can additionally reclaim a network connection that never delivers EOF. Do not reclaim a normal client merely because it is idle.

Host state defaults to `~/.local/state/acp-ssh`; set `ACP_SSH_HOME` to isolate installations or tests. `session/new/load/resume.cwd` must exist on the **server**. Client filesystem/terminal operations and other reverse requests are forwarded, not automatically executed by the bridge.

Host configuration defaults to 64 MiB of event retention and a 16 MiB message limit. Standard ACP methods and unknown extensions pass through. A native process failure is recorded; the bridge never retries a model operation or silently restarts the agent.

Useful commands:

```bash
acp-ssh watch agy --after 0 --follow
acp-ssh status agy
acp-ssh cancel agy SESSION_ID
acp-ssh stop agy
```

`watch` contains protocol content and should be treated as private. `stop` terminates the host's agent process group. The background `submit/job/respond/approve` interfaces are also available through `acp-ssh --help`; submit is idempotent by its explicit request ID and is refused while an interactive controller is attached.

## Optional gateway

Use this only for an OpenAI-compatible custom gateway that needs the provided Gemini effort aliases normalized. The native `agy` command remains untouched.

```bash
export AGY_NATIVE_BINARY=/absolute/path/to/native/agy
export AGY_LLM_GATEWAY_URL=https://gateway.example.com
export AGY_LLM_GATEWAY_API_KEY=YOUR_KEY
agy-gateway --version
acp-ssh configure custom --cwd /absolute/server/workspace \
  --command "$HOME/.local/bin/agy-gateway" acp --process-per-session
```

Set secrets outside shell history where practical. `AGY_LLM_GATEWAY_URL` is the gateway **root**; the native request path, including `/v1`, is preserved. `AGY_LLM_GATEWAY_MODELS` overrides the advertised catalog. The default known alias families are `gemini-4-argon`, `gemini-3.8-flash`, and `gemini-4-carbon`, with low/medium/high effort. Other model names and non-chat endpoints pass through unchanged. Upstream errors are preserved without retry; redirects are refused.

Alternatively create `~/.config/acp-ssh/gateway.json` (or set `AGY_GATEWAY_CONFIG`):

```json
{
  "native_binary": "/absolute/path/to/native/agy",
  "gateway_url": "https://gateway.example.com",
  "api_key_env": "MY_GATEWAY_API_KEY"
}
```

Environment values take precedence. The wrapper reads no IDE auth store. The optional `proxy_url` and `ca_cert` fields map to `AGY_LLM_GATEWAY_PROXY_URL` and `AGY_LLM_GATEWAY_CA_CERT`. For systemd, supply environment variables through `~/.config/acp-ssh/environment` (mode 600), using `NAME=value` lines without `export`; restarting a host to change its environment requires finishing its work first.

## Optional native Swarm

Install with `--with-swarm`, then:

```bash
acp-ssh configure swarm --cwd /absolute/server/workspace \
  --preserve-background-requests \
  --command "$HOME/.local/bin/agy-swarm-acp"
acp-ssh start swarm
```

The Swarm entry uses the installed/native `AGY_NATIVE_BINARY`. To use the optional gateway, append `--native /absolute/path/to/agy-gateway` to the command. Its separate app data defaults to `antigravity-swarm-acp`.

Swarm **defaults to native automatic tool approval**; pass `--approval ask` for manual approval. The tested native build does not forward worker command approvals to the parent ACP connection in ask mode, so those workers may wait indefinitely. The adapter uses the native swarm engine, not another scheduler, and does not enforce a hard worker concurrency limit. Read-only attach/snapshot/history/subscribe interfaces are documented in [PROTOCOL.md](docs/PROTOCOL.md).

## Optional permanent deletion

Permanent deletion is disabled by default. It requires the verified native binary and a known local app data root. Preview without changing configuration:

```bash
acp-ssh-enable-delete agy --app-data-name antigravity-cli
```

After the desktop disconnects and all tasks/client requests finish, stop the host, apply, and restart:

```bash
acp-ssh stop agy
# Wait until the host has fully stopped (or use systemctl --user stop acp-ssh@agy.service).
acp-ssh-enable-delete agy --app-data-name antigravity-cli --apply
acp-ssh start agy
```

The enable command preserves every existing configuration field, makes a private backup, and refuses a running host. Reinitialize to negotiate `maichart/sessionManagement`. The desktop must confirm deletion and wait for `deleted:true`. The backend rejects grouped/Swarm references and active work; it never substitutes close or hiding for deletion. See [PROTOCOL.md](docs/PROTOCOL.md) for recovery and scope.

## Tests

Default tests use fake agents, local HTTP fixtures, temporary directories, and private sockets. They need no vendor binary, API key, system service, or GPU:

```bash
python3 -m unittest discover -s tests -v
```

Optional tests run an independently installed native build against a local mock model in an isolated HOME:

```bash
export AGY_NATIVE_BINARY=/absolute/path/to/native/agy
python3 bridge/verify_delete.py
python3 swarm/verify_native.py --bridge-reconnect
```

These never attach to a production host. The deletion test requires the pinned binary hash. Optional `ACP_TEST_REPORT=/tmp/report.json` saves the deletion summary; no reports are bundled in the component. Test/install checks are separate from real upstream model quality or availability.

License: Apache-2.0. See [LICENSE](LICENSE) and [NOTICE](NOTICE). Antigravity remains separately licensed software and is not redistributed here.
