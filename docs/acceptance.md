# Acceptance

The automated suite uses temporary state and generated broker credentials. On macOS or Linux, install Mosquitto (including `mosquitto_passwd`) and run:

```sh
uv sync --locked
uv run pytest -q
uv build
```

## What is exercised

| Layer | Checks |
| --- | --- |
| Storage and wire protocol | Exact-byte duplicate detection, conflicting IDs, concurrent answers, expiry, session ownership, original-request retry, recovery of occupied slots, successor lookup and immutable terminal snapshots |
| CLI and MCP | Commands over a Unix socket, stdio MCP discovery and structured tool results, terminal and scripted wait behavior, answer correlation and socket selection |
| Browser workbench | Packaged page routes, authenticated requests over the caller socket, daemon errors and failed job results. A real broker test starts the workbench with `ui --no-open`, answers a CLI-created question through its HTTP endpoint, then stops the workbench and retrieves the result through the CLI. |
| Runtime | Fresh and resumed processes, terminal-event parsing, timeouts, process-group cleanup on owner death, runtime environment filtering and command handlers |
| Broker integration | Discovery over MQTT 5, topic ACLs, retained message handling, multi-turn calls, disconnection and result recovery using two separately configured agents; SIGKILL during an execution, subprocess cleanup, historical lookup, no repeated side effect and a successful subsequent call |

Deterministic tests can run without model accounts. Provider-driven runs require credentials on the executing host. The [design experiments](spec/v1.3.md#71-已做的设计验证) predate the implementation; their simulated MCP backend is separate from the broker integration tests.

## Manual host runs

- **Browser workbench with the Echo worker on macOS.** Chromium opened the local launch link, submitted a prompt, preserved an unsent answer across reload and displayed the result in the same task. Two jobs for different services produced identical clarification questions. Their local request previews identified the intended job. Answering it left the other question pending. The English interface submitted structured JSON and continued the same session at a 390 px viewport width without horizontal page scrolling. Stopping the caller daemon showed its disconnected state and kept the displayed question and draft field available. [Clarification screen](images/task-workbench.zh-CN.png)
- **Codex CLI 0.166.0 on macOS.** Two daemons and Mosquitto exercised a direct result, clarification, task lookup from a fresh CLI process, an answer in the original runtime session, and another follow-up. A real Codex MCP client invoked the worker, waited for a question, answered and retrieved the result. The test account explicitly allowed the two submitting MCP tools. In a separate two-slot run, two independent Codex conversations delegated different tasks to a fixed-command worker. Each question was delayed by 45 seconds, with about 42 seconds of overlap. Each host queried only its own initial job once, resumed its original thread, answered, and retrieved its own result.
- **Codex project permissions.** With a named read-only profile, the runtime's actual tool process read the sample project file. A read of an unrelated temporary file and a write inside the project were denied. A project-configured MCP server was excluded from the inbound worker.
- **Claude Code 2.1.332 on macOS.** The real CLI loaded the Mesh stdio server, called a worker through two daemons and Mosquitto, retrieved its question, answered it and retrieved the matching result. A second run used two independent Claude conversations with different markers and 44-second worker delays. Each received its own question and final result. A local scripted Anthropic-compatible provider supplied the tool choices in both runs. The target adapter also started the real Claude CLI, obtained a structured question and resumed its session to recover a first-turn marker with the answer. Production Anthropic inference was not exercised.
- **Packaging.** Both the wheel and source distribution build. A clean virtual environment installed the wheel and ran the CLI.
