# pi-msg

Drive the [Pi](https://pi.dev) coding agent **entirely from an XMPP chat client** —
1:1 or in a group chat (MUC).

`pi-msg` launches `pi --mode rpc`, then bridges Pi's JSONL event stream to XMPP
(via [mellium.im/xmpp](https://mellium.im/xmpp)): the assistant's replies are relayed
to you as chat messages, and your chat messages drive the agent — plain prompts **and**
slash commands, exactly as if you'd typed them into Pi locally.

Because it runs Pi in RPC mode, commands like `/new` work over chat (an earlier
in-process-extension version couldn't do this — `sendUserMessage` can't invoke Pi's
command layer).

## Supported features

- Pi RPC bridge with sessions resumed across restarts; `/new` starts fresh, and Pi
  slash commands work over chat.
- XMPP 1:1 and group chat, with trigger/broadcast/reply addressing, owner-versus-peer
  trust, threaded replies, allowlisted destinations, and `to: noop`. See
  [routing details](docs/routing.md).
- Room history via `read_room`; **MAM supported**. Only normal rooms are readable;
  rooms must be non-anonymous, and error rooms are write-only.
- Structured `send_file` (XEP-0363/0066) and `send_reaction` (XEP-0444) tools.
- Optional per-turn system-prompt text, OpenRouter credit monitoring, and one-shot
  fresh-session tasks via `--prompt`.
- Typing and presence/activity updates, read receipts, no-reply recovery, and hints for
  messages that still need an answer.
- Connection keepalives and reconnect recovery; proactive fleet restarts reprompt only
  accounts marked busy.

## Commands

Your chat messages → routed to Pi:

| You send | Becomes |
| --- | --- |
| plain text | a prompt to the agent |
| `/skill:name …`, `/template …`, any extension command | a prompt (Pi expands/runs it) |
| `/new` | `new_session` (fresh session; connection stays up) |
| `/compact [instructions]` | `compact` |
| `/model <provider/id>` or `/model <search>` | `set_model` |
| `/models` | list available models with the current one marked (no LLM turn) |
| `/session` | session stats — id, file, message counts, tokens, cost (no LLM turn) |
| `/name [name]` | show the session display name, or set it |
| `/think <off\|low\|medium\|high\|…>` | `set_thinking_level` |
| `/abort` (or `/stop`) | `clear_queue`, then `abort` — stops the run AND flushes pi's queued messages: a steer that landed mid-run can't start a fresh run the instant the aborted one stops. The reply names how many queued messages it dropped. |
| `!` | `abort` only — the quick interrupt. Stops whatever is currently running (a command or thinking) but leaves queued messages intact, so the next one is evaluated right after. |
| `/dump` (or `/dump pretty`) | send the session transcript to the owner — raw JSONL, or `pretty` for indented per-record JSON (no LLM turn) |
| `/export` | render the current session to HTML via pi's `export_html` RPC and **send it as a file over XMPP** (XEP-0363 HTTP Upload) — **deterministic**, no agent turn; the rendered session lands as an inline, downloadable file |
| `/quit` (or `/exit`) | shut down the bridge and Pi |

Every bridged command also works with a `!` prefix — `/new` and `!new` are
interchangeable. A lone `!` (no command name after it) is the quick
interrupt: it aborts the current run like `/abort`, but WITHOUT the queue
flush — whatever you queued behind the running message is still evaluated
next. The prefix only matters for the owner: non-owners' messages
are always treated as literal text.

## Configuration

Create `~/.config/pi-msg/config.json` (override the path with `PI_MSG_CONFIG`), then
`chmod 600` it:

```json
{
  "accounts": {
    "default": {
      "jid": "pi@chat.example.com",
      "password": "super-secret",
      "owner": "you@chat.example.com",
      "model": "anthropic/claude-sonnet-latest",
      "workdir": "/path/to/your/project"
    }
  }
}
```

Per-account fields:

| field | required | default | notes |
| --- | --- | --- | --- |
| `jid` | yes | — | bare JID of the bot account |
| `password` | yes | — | bot account password |
| `owner` | yes | — | the human this account relays to; the **canonical** (trusted) driver |
| `service` | no | `<jid-domain>:5222` | `host:port` (a leading `xmpp://` is tolerated) |
| `resource` | no | `pi-msg` | XMPP resource (client-session label) |
| `model` | no | Pi's default | model pattern passed to `pi --model` |
| `workdir` | no | current dir | working directory for the agent (also where Pi discovers `AGENTS.md`/`CLAUDE.md`) |
| `rooms` | no | `[]` | group-chat rooms: objects with `jid`, `role`, `trigger`, and `reactions`; legacy `room` and `errorRoom` fields are rejected |
| `nick` | no | JID localpart | occupant nickname used in the room(s) |
| `roomTrigger` | no | `nick` | account-wide address prefix that makes a room message a prompt (e.g. `pi` → `pi: …`); override per room |
| `uploadService` | no | auto-probed | XEP-0363 upload component JID for file transfer (e.g. `upload.chat.example.com`) |
| `pingInterval` | no | `60s` | keepalive cadence (Go duration): XEP-0199 server ping + XEP-0410 MUC self-ping; `0` disables |
| `reactions` | no | `false` | XEP-0444 reactions and lifecycle acknowledgements; override per room |
| `beforeAgentStartText` | no | — | literal text added to the system prompt before every turn; empty or whitespace means unset |
| `avatar` | no | — | path to a local image (PNG/JPEG/GIF) published as the bot's XEP-0153 vCard profile picture on connect |
| `creditWatch` | no | — | low-credit protection with a `minBelowUsd` floor. Reports the remaining OpenRouter balance after every `/new`, and a proactive watcher probes the balance hourly and DMs the owner when it drops below the floor (re-warns at most every 6h while still below). A model run that dies on an OpenRouter out-of-credits error (HTTP 402) is also reported to the owner directly instead of the generic "done (no reply)". e.g. `{ "creditWatch": { "minBelowUsd": 2 } }`. Only active when pi's auth file (`<config-dir>/auth.json`) holds an `openrouter` api key; otherwise it's skipped |
| `mam` | no | `true` | MAM support; set `false` to disable archive backfill |

Multiple accounts: add more keys under `accounts`; `default` is used unless you set
`PI_MSG_ACCOUNT=<name>`. In 1:1 mode only the `owner` JID may drive the agent.

## Run

```bash
go build -o pi-msg . && ./pi-msg     # from the repo
```

### Supported pi version

pi-msg targets **pi 0.84.0 or later**. Two reasons:

- Pi 0.84.0 removed the cumulative `message` field and
  `assistantMessageEvent.partial` from the `message_update` RPC event. pi-msg
  drives the typing indicator and the presence label from the deltas alone
  (`TestStreamDeltaContract` pins this), so it works on both shapes — but no
  new code may reach for the removed fields.
- `/abort` uses `clear_queue`, added in pi 0.84.4. On an older pi the command
  is unknown, so pi-msg logs the failure at `info` and aborts exactly as
  before, reporting no dropped messages.

### Nix

```bash
nix run   github:zachpmanson/pi-msg    # run the bridge
nix build github:zachpmanson/pi-msg    # build the package (bin: pi-msg)
```

Dev shell (Go + gopls) via `nix develop`, or automatically with
[direnv](https://direnv.net/) — the repo ships a `.envrc` (`use flake`); run
`direnv allow` once.

Set `PI_MSG_DEBUG=1` to print connection/status/stderr diagnostics. On startup the bot
simply comes **online** in your roster (presence `listening`); on shutdown or a pi crash it
goes **offline** with a `<status>` describing why and when — pi-msg no longer posts chat
banners for these lifecycle events.

### On-demand spawns: `--prompt`

`pi-msg --prompt "<task>"` (alias `--command`) spawns a **fresh, on-demand
persona** with the task as its very first prompt — no separate XMPP-send hop
needed to wake it. It intentionally does **not** resume the saved session
(stateless by construction) and skips restart-gap replay; the reply routes to
the owner per the normal routing contract. This backs the sentinel doer flow
(zachpmanson/beltino#18).

The same payload can ride the existing one-shot start-directive file
(`<config-dir>/<account>.start`, written via `writePromptDirective`):

```
prompt
resolve zachpmanson/pi-msg#35 and open a PR
```

Either way the directive file is consumed (one-shot); an explicit `--prompt`
flag overrides a file-delivered payload. Routine restarts that carry no prompt
keep the existing resume + proactive/idle behavior unchanged.

## Notes

- Pi runs tools autonomously (no built-in approval prompts). If some other extension
  raises a dialog (`select`/`confirm`/`input`/`editor`), pi-msg auto-dismisses it
  (nobody's at the TUI) and tells you over chat — so approval-gated tools are declined
  over the bridge.
- Auth uses SASL SCRAM-SHA-256 (mellium negotiates it cleanly against ejabberd);
  STARTTLS is required first.
