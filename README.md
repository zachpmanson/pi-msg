# pi-msg

Drive the [Pi](https://pi.dev) coding agent **entirely from an XMPP chat client**.

`pi-msg` launches `pi --mode rpc`, then bridges Pi's JSONL event stream to XMPP
(via [`@xmpp/client`](https://github.com/xmppjs/xmpp.js)): the assistant's replies
are relayed to you as chat messages, and your chat messages drive the agent — plain
prompts **and** slash commands, exactly as if you'd typed them into Pi locally.

Because it runs Pi in RPC mode, commands like `/new` work over chat (an earlier
in-process-extension version couldn't do this — `sendUserMessage` can't invoke Pi's
command layer).

## How it works

```
XMPP client (you)                 pi-msg                      pi --mode rpc
  │  "fix the build"  ───────────▶ prompt ──────────────────▶ agent runs
  │  ◀─────────────── assistant text ◀── message_end event ──┘
  │  "/new"           ───────────▶ {type:"new_session"} ─────▶ fresh session
```

- Each finished **assistant message** → sent to you as chat.
- When the agent settles → **`✅ ready — your turn`**.
- Your chat messages → routed to Pi:

| You send | Becomes |
| --- | --- |
| plain text | a prompt to the agent |
| `/skill:name …`, `/template …`, any extension command | a prompt (Pi expands/runs it) |
| `/new` | `new_session` (fresh session; connection stays up) |
| `/compact [instructions]` | `compact` |
| `/model <provider/id>` or `/model <search>` | `set_model` |
| `/think <off\|low\|medium\|high\|…>` | `set_thinking_level` |
| `/abort` (or `/stop`) | `abort` |
| `/quit` (or `/exit`) | shut down the bridge and Pi |

Only the account's configured **`owner`** JID may drive the agent.

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
| `owner` | yes | — | the human this account relays to (and the only sender it accepts) |
| `service` | no | `xmpp://<jid-domain>:5222` | TCP `xmpp://host:port` or WebSocket `wss://…` |
| `resource` | no | `pi-msg` | XMPP resource (client-session label) |
| `model` | no | Pi's default | model pattern passed to `pi --model` |
| `workdir` | no | current dir | working directory for the agent |

Multiple accounts: add more keys under `accounts`; `default` is used unless you set
`PI_MSG_ACCOUNT=<name>`.

## Run

```bash
cd ~/projects/pi-msg
npm install
node src/bridge.ts           # or: npm link  &&  pi-msg   (from anywhere)
```

Set `PI_MSG_DEBUG=1` to print connection/status/stderr diagnostics. On startup you
should see `🟢 pi-msg bridge up` in your chat client.

Requirements: Node ≥ 22.18 (runs the TypeScript entry directly via built-in type
stripping — no build step), and a `pi` on `PATH` that's logged into a provider
(`pi` → `/login`).

## Notes

- Pi runs tools autonomously (no built-in approval prompts). If some other extension
  raises a dialog (`select`/`confirm`/`input`), pi-msg auto-dismisses it (nobody's at
  the TUI) and tells you over chat.
- `@xmpp/client`'s bundled SCRAM-SHA-1 fails against ejabberd; `src/xmpp.ts` forces
  SASL `PLAIN`, which is safe because servers require STARTTLS (SASL runs post-TLS).
