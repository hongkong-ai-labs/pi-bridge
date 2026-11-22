# pi-bridge

An MCP server that drives the [Pi coding agent](https://github.com/earendil-works/pi) over its
own RPC protocol, so any MCP client — Hermes, an editor, another agent — can start work in a
repository, watch it run, steer it mid-turn, and stop it cleanly.

`pi -p "task"` is a one-shot: it blocks, tells you nothing until it exits, and you cannot correct
it while it works. Pi's RPC mode (`pi --mode rpc`) is a long-lived process speaking JSONL on
stdin/stdout. This bridge turns that into six MCP tools.

## Tools

| Tool | What it does |
|---|---|
| `pi_run` | Run one prompt to completion, return the assistant's final text (blocks up to `timeout_secs`, default 900) |
| `pi_start` | Start a prompt in the background, return a session id immediately |
| `pi_status` | Status, elapsed time, turn count, last event, latest assistant text — one session or all |
| `pi_say` | Send another message: steers a running turn, or continues an idle session |
| `pi_cancel` | Abort the current turn |
| `pi_stop` | Abort, kill the Pi subprocess, forget the session |

Sessions are in-process: one Pi subprocess per session, killed when the bridge exits.

## Install

```bash
cargo install --git https://github.com/hongkong-ai-labs/pi-bridge
# or build locally
cargo build --release   # target/release/pi-bridge
```

Pi itself must be on `PATH` (npm global `@earendil-works/pi-coding-agent`). Its model
credentials come from the environment it inherits — Pi reads `DEEPSEEK_API_KEY`,
`OPENROUTER_API_KEY`, `ANTHROPIC_API_KEY` and friends, or `~/.pi/agent/auth.json`.

## Wire it into Hermes

```bash
hermes mcp add pi --command /path/to/pi-bridge \
  --env DEEPSEEK_API_KEY=... OPENROUTER_API_KEY=...
```

Then start a new session (or `/reload-mcp`) and the six tools are callable.

Any other MCP client works the same way: it is a plain stdio server speaking MCP
`2025-06-18` (`initialize`, `tools/list`, `tools/call`).

## Configuration

| Env var | Meaning |
|---|---|
| `PI_BIN` | Binary to launch instead of `pi` from `PATH` |

Per-call: `cwd` is required (`pi_run`, `pi_start`), and `model` maps to Pi's `--model`.

## Design notes

- **Event-driven status, not polling Pi.** One reader task folds Pi's stdout events into a live
  view: `turn_start`/`agent_start` mean running, `turn_end`/`agent_end` mean idle, everything
  else leaves the status alone. Response acks never flip an idle session back to running.
- **Text comes from `message_end`.** The assistant's reply is recorded from the
  `message_end` event as it arrives, with `get_last_assistant_text` as a second source, because
  asking only after `turn_end` races the turn ending.
- **One writer task per session.** Commands are serialized through an mpsc channel, so
  concurrent MCP calls cannot interleave half-lines into Pi's stdin.
- **No shell.** Prompts travel as JSON `prompt` records; nothing is interpolated into a command
  line, and a `cwd` is the only place the agent operates.
- Pi has no sandbox of its own: point `cwd` at repositories you trust.

## Licence

MIT.
