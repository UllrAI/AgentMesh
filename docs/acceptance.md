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
| Browser workbench | Page assets, authenticated requests over the caller socket and daemon errors. With two nodes and a real broker, the workbench lists the observing node and the worker’s advertised method, answers a CLI-created question, and retains the worker’s card when it goes offline. |
| Runtime | Fresh and resumed processes, terminal-event parsing, timeouts, process-group cleanup on owner death, runtime environment filtering and command handlers |
| Broker integration | Discovery over MQTT 5, topic ACLs, retained message handling, multi-turn calls, disconnection and result recovery using two separately configured agents; SIGKILL during an execution, subprocess cleanup, historical lookup, no repeated side effect and a successful subsequent call |

Deterministic tests can run without model accounts. Provider-driven runs require credentials on the executing host. The [design experiments](spec/v1.3.md#71-已做的设计验证) predate the implementation; their simulated MCP backend is separate from the broker integration tests.

## Browser checks

Chromium on macOS connected to the two-node Mosquitto demo. The node and task pages share the same caller daemon.

| User action | Observed behavior |
| --- | --- |
| 1. Open the workbench and select a node | The node directory opens first. The caller is marked as local and the worker lists its method. Refreshing this page requests only discovery. Stopping the worker leaves its card visible as offline. Disconnecting the broker changes its status to unknown. [Chinese interface](images/node-workspace.zh-CN.png) · [English interface](images/node-workspace.png) |
| 2. Call a method | With two methods on one worker, choosing the second submitted to that exact method. The completed result opened in Tasks. [Method form](images/call-method.zh-CN.png) |
| 3. Answer a question | Two requests produced identical questions. Choosing one by its local request preview and answering it left the other pending. The answer draft survived reloading and switching views. English JSON submission and a follow-up in the same session also succeeded. [Answer form](images/task-workbench.zh-CN.png) |

The node layout was checked at 320, 390, 768 and 1280 CSS pixels, including a 155-character method name. Neither the page nor the details panel overflowed horizontally. At 320 × 500 with touch emulation, expanding the input schema and submitting invalid JSON kept the action footer visible, focused the invalid field, and allowed the form body to scroll. The input used 16 px text and the submit button measured 44 px tall. Activating Call and Cancel with Enter returned focus to the initiating method.

In this run axe-core 4.14.1 reported zero violations for the WCAG 2/2.1 A and AA rules it checked on Nodes, Tasks and the method dialog in both system appearances. The browser checks cover Chromium. Screen-reader interaction, Safari and Firefox were not exercised.

## Manual host runs

- **Codex CLI 0.166.0 on macOS.** Two daemons and Mosquitto exercised a direct result, clarification, task lookup from a fresh CLI process, an answer in the original runtime session, and another follow-up. A real Codex MCP client invoked the worker, waited for a question, answered and retrieved the result. The test account explicitly allowed the two submitting MCP tools. In a separate two-slot run, two independent Codex conversations delegated different tasks to a fixed-command worker. Each question was delayed by 45 seconds, with about 42 seconds of overlap. Each host queried only its own initial job once, resumed its original thread, answered, and retrieved its own result.
- **Codex project permissions.** With a named read-only profile, the runtime's actual tool process read the sample project file. A read of an unrelated temporary file and a write inside the project were denied. A project-configured MCP server was excluded from the inbound worker.
- **Claude Code 2.1.332 on macOS.** The real CLI loaded the Mesh stdio server, called a worker through two daemons and Mosquitto, retrieved its question, answered it and retrieved the matching result. A second run used two independent Claude conversations with different markers and 44-second worker delays. Each received its own question and final result. A local scripted Anthropic-compatible provider supplied the tool choices in both runs. The target adapter also started the real Claude CLI, obtained a structured question and resumed its session to recover a first-turn marker with the answer. Production Anthropic inference was not exercised.
- **Packaging.** Both the wheel and source distribution build. A clean virtual environment installed the wheel and ran the CLI.
