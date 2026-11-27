# Running a private mesh

## Broker and identity

Give each logical agent an MQTT username matching its agent ID, its own password, Unix socket and database. Use a private data directory. The [local setup](../examples/local/setup.py) generates a complete broker configuration and a pair of node configurations. The [example ACL](../examples/local/acl) grants only the caller permission to submit work to the worker. The worker can publish replies back to the caller. Members can read advertised cards and status.

For separate machines, configure a reachable broker address in each node. Use `mqtts://` and a certificate matching the broker's hostname on a public network. Set `mqtt.ca_file` to the local CA bundle for a private CA. The broker handles plaintext application messages after TLS termination. Enable broker persistence for retained cards and status. On retirement, stop the instance, revoke its account, then clear its retained card and status as the broker administrator.

A grant to call a target authorizes all methods currently or subsequently exposed by that target. Use separate agent IDs to expose write methods to a smaller group of callers.

## Worker configuration

The daemon reads a YAML file and the password variable named in `mqtt.password_env`. The CLI and MCP bridge need only the local socket. A Codex worker configuration might look like this:

```yaml
agent_id: orders-worker
mqtt:
  url: mqtts://broker.example.org:8883
  username: orders-worker
  password_env: MESH_BROKER_PASSWORD
socket: ~/.agent-mesh/orders.sock
job_store: ~/.agent-mesh/orders.db
runtime:
  type: codex
  workdir: ~/projects/orders
  codex_home: ~/.agent-mesh/orders-codex
  max_concurrent_jobs: 1
  execution_timeout_s: 300
  methods:
    - name: orders.analyze
      description: Read the orders service and report API module boundaries with source paths
      interaction: clarify
      permission_profile: mesh-project-read
      input_schema:
        type: object
        required: [prompt]
        properties:
          prompt: {type: string}
```

Provision the runtime executable, login and permissions **for the service account**. Run Codex with a dedicated configuration home. Install the named project-scoped permission profile there and verify its filesystem boundaries using that version of Codex. A working directory selects where the task starts. Filesystem isolation comes from the runtime profile or the host. Keep the inbound runtime's tools separate from the caller's Mesh bridge. Use absolute executable paths when a service has a different PATH from your shell.

- **Codex** runs a fresh non-interactive CLI process per turn and resumes the recorded thread. The local named profile applies on every turn. `runtime.skip_git_repo_check` can be enabled for a deliberately configured non-Git workspace.
- **Claude Code** uses non-interactive structured events. Set `type: claude` and use a local Claude settings JSON file as the method's `permission_profile`. Configure the host sandbox for the intended filesystem boundary. Set `claude_config_dir` when using a dedicated login. Variables required by a runtime can be explicitly named in `runtime.env_allowlist`. The daemon excludes its broker password variable.
- **Fixed command / custom agent** uses `type: command` and a fixed argv array in `runtime.command`. The program receives one UTF-8 JSON object on stdin with the configured method, the runtime session ID and either `params` or `answer_text`. Emit newline-delimited JSON on stdout. A multi-turn handler emits `{"type":"session.started","session_id":"<runtime-id>"}`, then `{"type":"question","text":"Which service?"}` or `{"type":"result","text":"..."}`. The handler owns its session storage. Choose this adapter for a local wrapper around a service such as Muse once its execution and lifecycle semantics are verified. A wrapper that detaches an independent remote task needs its own reliable completion and cleanup tracking.
- **Echo** exercises the protocol without model credentials. With `interaction: clarify` it asks for missing `scope` and remembers the original prompt.

## Services and recovery

Run one daemon per logical agent, in the foreground under the service manager. For Linux, use a user service with an absolute executable, config and private environment file, for example:

```ini
[Unit]
Description=Agent Mesh orders worker
[Service]
ExecStart=%h/.local/bin/agent-mesh serve --config %h/.agent-mesh/orders.yaml
EnvironmentFile=%h/.agent-mesh/orders.env
Restart=on-failure
[Install]
WantedBy=default.target
```

The environment file uses `KEY=value` lines. On macOS use a per-user LaunchAgent with `ProgramArguments` containing the same executable and arguments, `RunAtLoad` enabled, and the broker credential available to that service. launchd arguments take literal absolute paths.

A dropped caller connection does not cancel accepted work. Reconnect and use `jobs` and `job <id>` to inspect the stored snapshot. An unknown execution outcome can include partial side effects. Inspect the target before explicitly submitting a new job. On the target, keep the database and its adjacent `.runs` directory together across restarts. The supervisor tracks old subprocess groups while completed job history remains queryable.
