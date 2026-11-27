# Agent Mesh

Ask an agent on another machine to do work, answer its follow-up questions, and retrieve the result after a disconnect. Agent Mesh connects private agents through an MQTT 5 broker. Each host makes one outbound connection and keeps its own job history in SQLite. A local CLI and six MCP tools talk to the same daemon over a Unix socket.

For example, a laptop assistant can discover an agent with access to the orders repository on a VM, request a read-only analysis, and answer “API only” in the original session. The VM owner configures the available methods, working directory and runtime permissions.

## Try two nodes locally

You need macOS or Linux, Python 3.11+, [uv](https://docs.astral.sh/uv/) and [Mosquitto](https://mosquitto.org/download/) with its password utility. The demo starts an Echo worker so you can try a full conversation without a model account.

```sh
git clone https://github.com/UllrAI/AgentMesh.git
cd AgentMesh
uv sync --locked
uv run python examples/local/setup.py "$HOME/.agent-mesh/demo"
```

In three terminals, from the checkout, run one of these commands per terminal. The setup script generates fresh credentials, a one-way caller-to-worker ACL, and a broker configuration bound to loopback.

```sh
# Terminal 1
mosquitto -c "$HOME/.agent-mesh/demo/mosquitto.conf"

# Terminal 2
. "$HOME/.agent-mesh/demo/env"
uv run agent-mesh serve --config "$HOME/.agent-mesh/demo/worker.yaml"

# Terminal 3
. "$HOME/.agent-mesh/demo/env"
uv run agent-mesh serve --config "$HOME/.agent-mesh/demo/caller.yaml"
```

In a fourth terminal, list the nodes and call the worker. It asks which part of the project to analyze. The CLI prints an answer command containing the question's actual job ID.

```sh
export AGENT_MESH_SOCKET="$HOME/.agent-mesh/demo/caller.sock"
uv run agent-mesh list
uv run agent-mesh call worker demo.analyze --prompt 'Analyze the module boundaries' --wait
uv run agent-mesh answer <question-job-id> --text 'API only' --wait
uv run agent-mesh jobs
```

To skip the clarification, provide the scope in the first request.

```sh
uv run agent-mesh call worker demo.analyze --params '{"prompt":"Analyze the module boundaries","scope":"API"}' --wait
```

## Connect an MCP client

With the caller daemon running, register its stdio bridge. Run these commands from the checkout so the executable path is absolute.

```sh
codex mcp add agent-mesh -- "$PWD/.venv/bin/agent-mesh" mcp --socket "$HOME/.agent-mesh/demo/caller.sock"
claude mcp add agent-mesh -- "$PWD/.venv/bin/agent-mesh" mcp --socket "$HOME/.agent-mesh/demo/caller.sock"
```

The bridge exposes `mesh_list`, `mesh_inspect`, `mesh_invoke`, `mesh_answer`, `mesh_job_status` and `mesh_jobs`. An asynchronous submit returns a job ID. The host can use a bounded wait on that ID and deliver the result or question in the originating conversation. To install the CLI independently of the checkout, run `uv tool install git+https://github.com/UllrAI/AgentMesh.git`.

## Documentation

- [Deployment and runtime adapters](docs/deployment.md)
- [Protocol v1.3 and design rationale (中文)](docs/spec/v1.3.md)
- [Acceptance coverage and test commands](docs/acceptance.md)

The implementation uses Python, SQLite, Eclipse Paho via aiomqtt, and the official MCP Python SDK. The package version and the wire protocol version are independent.

Copyright 2026 UllrAI. [Apache License 2.0](LICENSE).
