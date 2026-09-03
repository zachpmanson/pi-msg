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

## How it works

Conversations are persisted across restarts: pi-msg records the pi session file
(`<config-dir>/<account>.session`) on startup, whenever the session changes
(`/new`, `/resume`, `/fork`), and on shutdown — then resumes it on the next
launch via `pi --session <file>`. A bridge restart therefore **continues the
previous conversation**; only `/new` resets context (or an explicit
`--prompt` on-demand spawn — see below). If the saved session file
is missing or empty, pi-msg starts a fresh session instead.

```mermaid
sequenceDiagram
    participant You as You (XMPP client)
    participant Bridge as pi-msg
    participant Pi as pi --mode rpc
    You->>Bridge: "fix the build"
    Bridge->>Pi: prompt
    Pi-->>Bridge: message_end event
    Bridge-->>You: assistant text
    You->>Bridge: "/new"
    Bridge->>Pi: {type:"new_session"}
    Note over Pi: fresh session
```

- Each finished **assistant message** → sent to you as chat.
- Agent state shows on three independent signals (1:1): a **typing indicator** while a
  reply is actually being written, presence **`<show>`** (`dnd` while busy, available
  when idle), and a presence **status** label of the current activity (`thinking…`,
  `running: <cmd>`, `replying…`, `retrying…`, `listening`). When a run settles
  without delivering anything you get a `✅ done (no reply) — your turn` nudge.
  A reply that was written but could not be routed does **not** count as
  delivered, so a dropped reply still raises the nudge instead of passing as an
  answer.
- **Empty-tail recovery**: when a run ends on a tool call with no reply text
  after it, the answer was never written and nothing can be sent. The bridge
  asks the agent once per turn to write the reply, then falls back to the
  `done (no reply)` nudge if that also produces nothing.
- **Unanswered-message hint**: a message that arrives mid-run is injected as a
  steer at the next yield point, usually the moment a tool result returns. The
  agent can read the new question before it writes the answer to the previous
  one, and then never write it. When a run takes in more messages than it sends
  replies, the bridge asks it once per turn to check for messages that still
  need an answer. The hint carries the run's chat history — every message in and
  every reply out, in order, each with its stanza id — because the agent cannot
  see the XMPP traffic and the bare counts leave it guessing which message it
  missed. The hint asks for `to: <jid|stanza-id>`: an id both routes the reply
  and marks it as a reply to the message it answers. The agent answers them all
  in that one turn, with several `to:` lines, or replies `to: noop` if it already
  covered everything. Each `to:` segment counts as one answer, so a single reply
  that fans out to three people is not mistaken for one unanswered message. The run that answers a hint is
  never hinted about in turn. `to: noop` counts as an answer, so deliberate
  silence is never flagged.
- Messages you send are acknowledged with a single **read receipt** — a XEP-0333
  chat marker (`displayed`) — when the agent takes them in, if your client requests it.
- Your chat messages → routed to Pi:

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
| `/abort` (or `/stop`, or a lone `!`) | `clear_queue`, then `abort` — the queue drain stops a steer that landed mid-run from starting a fresh run the instant the aborted one stops. The reply names how many queued messages it dropped. |
| `/dump` (or `/dump pretty`) | send the session transcript to the owner — raw JSONL, or `pretty` for indented per-record JSON (no LLM turn) |
| `/export` | render the current session to HTML via pi's `export_html` RPC and **send it as a file over XMPP** (XEP-0363 HTTP Upload) — **deterministic**, no agent turn; the rendered session lands as an inline, downloadable file |
| `/quit` (or `/exit`) | shut down the bridge and Pi |

Every bridged command also works with a `!` prefix — `/new` and `!new` are
interchangeable. A lone `!` (no command name after it) is shorthand for
`/abort`. The prefix only matters for the owner: non-owners' messages
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
| `room` | no | — | a bare MUC JID (or an **array** of them) to also join for **group chat** (see below) |
| `nick` | no | JID localpart | occupant nickname used in the room(s) |
| `roomTrigger` | no | `nick` | address prefix that makes a room message a prompt (e.g. `pi` → `pi: …`) |
| `uploadService` | no | auto-probed | XEP-0363 upload component JID for file transfer (e.g. `upload.chat.example.com`) |
| `errorRoom` | no | — | write-only MUC dumping ground for dropped/unrouteable agent replies (see below) |
| `pingInterval` | no | `60s` | keepalive cadence (Go duration): XEP-0199 server ping + XEP-0410 MUC self-ping; `0` disables |
| `reactions` | no | `false` | XEP-0444 emoji reactions on 1:1 owner messages: lifecycle → 👀 picked up / ✅ done / ⛔ aborted, and enables the agent-driven `send_reaction` tool (see [Agent tools](#agent-tools)) |
| `avatar` | no | — | path to a local image (PNG/JPEG/GIF) published as the bot's XEP-0153 vCard profile picture on connect |
| `creditWatch` | no | — | when set with a `minBelowUsd` floor, reports the remaining OpenRouter credit balance every time the agent runs `/new`. e.g. `{ "creditWatch": { "minBelowUsd": 2 } }`. Only active when pi's auth file (`<config-dir>/auth.json`) holds an `openrouter` api key; otherwise it's skipped. The remaining credit is shown on every `/new`; it's flagged as low when it drops below the floor |

Multiple accounts: add more keys under `accounts`; `default` is used unless you set
`PI_MSG_ACCOUNT=<name>`. In 1:1 mode only the `owner` JID may drive the agent.

## Group chat (MUC)

Set `room` on an account (a single MUC JID, or an array of them) and pi-msg
**also** joins each. **The owner's 1:1 stays the primary channel** — joining a
room is purely additive and doesn't change 1:1 behaviour (lifecycle notices, and
unsolicited output all still go to the owner). The **typing indicator** now tails
the reply's `to:` routing line (issue #44): it points at whichever 1:1 recipient
the reply names — the DM, another agent — and stays dark when the reply heads
to a room or `to: noop`. Each reply goes back to wherever its routing line
points, including the specific
room when several are joined. Room messages are handled on **two independent
axes**:

- **Trigger** — does the message start/steer a turn?
  - the **owner** → always
  - anyone else who **addresses the bot by name** (`pi: …` / `pi, …`) → always
  - all other chatter → never (it's buffered as ambient context)
- **Authority** — is the content trusted?
  - the **owner** → canonical (authoritative)
  - everyone else, even when addressing the bot → untrusted *commentary*; the agent is
    told to use its judgment and is under no obligation to act on it

Untriggered messages are buffered and, on the next turn, prepended to the prompt as a
clearly-labeled *"room commentary — non-canonical"* block, then the buffer clears.

**Reply routing (explicit `from:`/`to:`).** When an account has room access, routing is
fully explicit — no guessing. Each prompt the agent receives leads with a header naming
the message's origin:

```
from: <channel jid>     # the room (group msg) or the owner (DM) — reply here to answer in place
sender: <person jid>    # room messages only, when the real JID is known — reply here to DM them
stanza-id: <uuid>       # this message's id — reply here to answer this message specifically
<message body>
```

And **every** agent reply must begin with a `to:` line naming its destination:

- `to: <room jid>` → the group chat (groupchat)
- `to: <owner or occupant jid>` → that person, 1:1
- `to: <stanza-id>` → the author of that message, with the reply **stamped** to it
- `to: noop` → send nothing (deliberate silence); the only `to:` form a pure
  1:1 account parses, and it must be the reply's first line

One reply may contain **several `to:` blocks** — each `to:` line starts a new message, so
the agent can fan a single turn out to multiple destinations:

```
to: team@muc.chat.zachmanson.com
Deploying now — back in 5.
to: zach@chat.zachmanson.com
(privately: the staging creds are stale, heads up)
```

Destinations are **allowlisted**: the owner, joined room(s), and real JIDs currently seen
in a room. A reply whose `to:` is missing or points anywhere else is sent to the owner, so
nothing is silently lost — the agent can't message arbitrary users. In a pure 1:1 account
(no room) there are no prefixes; replies just go to the owner.

**Answering a specific message (`to: <stanza-id>`).** When several messages arrive before
any reply, nothing in an outbound reply says which one it answers. So a `to:` line may
name a **message** instead of a JID, using the `stanza-id:` value from the prompt. pi-msg
resolves the id to that message's author, sends there, and stamps the outbound stanza with
a **XEP-0461** `<reply xmlns="urn:xmpp:reply:0" to="<author>" id="<stanza id>"/>` element,
so the owner's client threads the reply under the message it answers. Rooms are stamped
too, and only the first chunk of a split reply carries the element. One run can emit
several replies, each stamped to its own message:

```
to: 3e2597d4-a470-4cdb-b972-431043bce34f
On the deploy: done, back in 5.
to: a8508c81-0e1b-4e48-ae16-61256b837670
On the creds: staging is stale, I'll rotate them next.
```

Warning: the id must be complete and known. An unknown or malformed id is a routing
failure that takes the normal reject path (error room plus a settle-time nudge), not a
silent fallback — so a wrong id is loud rather than quietly mis-delivered. Two id shapes
are recognised: the 8-4-4-4-12 hex UUID most clients emit, and the 16 bare hex characters
pi-msg emits for its own stanzas.

**File transfer.** The agent sends files with the **`send_file`** tool (a structured tool
call, not in-band text — see [Agent tools](#agent-tools) below): pi-msg uploads the file via
**XEP-0363 HTTP Upload** and sends the resulting URL as an **XEP-0066** out-of-band message,
so the recipient's client shows a downloadable file. The destination is allowlisted (owner,
joined rooms, known occupants) exactly like a `to:` reply. The upload component is discovered
automatically (`upload.<domain>` / `httpupload.<domain>`) or set explicitly via the
`uploadService` config field.

**The room must be non-anonymous** (ejabberd: *"Present real Jabber IDs to → anyone"*,
optionally *members-only*). The owner is recognized by real JID; in a semi-anonymous
room real JIDs are hidden, so the owner can't be distinguished and every message falls
through to the untrusted/ambient tiers.

**Errors dumping ground (`errorRoom`).** Set `errorRoom` to a bare MUC JID (e.g.
`errors@muc.chat.example.com`) and pi-msg uses it as a *write-only* dumping ground for
agent replies it can't route (no `to:` line, text before the first `to:`, or a
non-allowlisted destination). This lets you mute the room and only check it when you need
to recover something — without the dropped content spamming your 1:1.

The bridge joins the room at the **XMPP layer** (so groupchat sends are accepted and the
keepalive covers it), but deliberately keeps it **out of the agent-visible room set**: it is
never dispatched to the agent, never appears in the reply/file allowlist, and isn't tracked
for occupants. So the agent can't read the room or route anything to it — it's write-only by
construction, which keeps multiple agents from acting on each other's rejected output. If
`errorRoom` is unset, unrouteable replies fall back to the owner's 1:1 as before.

## Agent tools

Beyond reply text, the agent gets structured **tools** (registered by a small companion
extension that pi-msg loads into `pi --mode rpc`, which relays each call back to pi-msg to
perform the XMPP action):

| Tool | What it does | Enabled when |
| --- | --- | --- |
| `send_reaction` | React to the human's latest message with an emoji (XEP-0444) | `reactions` is on |
| `send_file` | Upload a local file and deliver it (XEP-0363 + XEP-0066); dest defaults to the current conversation, allowlisted | always |

Reply **routing** (`to:`) stays an in-band text convention (above); only these discrete
side-effect actions are tools.

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

Requirements: Go ≥ 1.26 (to build), and a `pi` on `PATH` that's logged into a provider
(`pi` → `/login`).

## Notes

- Pi runs tools autonomously (no built-in approval prompts). If some other extension
  raises a dialog (`select`/`confirm`/`input`/`editor`), pi-msg auto-dismisses it
  (nobody's at the TUI) and tells you over chat — so approval-gated tools are declined
  over the bridge.
- Auth uses SASL SCRAM-SHA-256 (mellium negotiates it cleanly against ejabberd);
  STARTTLS is required first.
