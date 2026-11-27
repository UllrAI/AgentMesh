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
| Runtime | Fresh and resumed processes, terminal-event parsing, timeouts, process-group cleanup on owner death, runtime environment filtering and command handlers |
| Broker integration | Discovery over MQTT 5, topic ACLs, retained message handling, multi-turn calls, disconnection and result recovery using two separately configured agents |

Deterministic tests can run without model accounts. Provider-driven runs require credentials on the executing host. The [design experiments](spec/v1.3.md#71-已做的设计验证) predate the implementation; their simulated MCP backend is separate from the broker integration tests.
