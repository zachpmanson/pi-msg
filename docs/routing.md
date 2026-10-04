# Reply Routing (pi-msg)

This is the **canonical specification** of how pi-msg routes an agent's replies.
It is the single source of truth for the routing protocol; the enforcement
lives in `bridge.go` (`splitReplySegments`, `routeLine`, `deliverReply`,
`firePendingNudge`) and in `xmpp.go` (`lookupMessage`, `chatStanza`). The
on-start description the agent receives lives in `Bridge.routingContract()`. This protocol belongs to **pi-msg**, not to any
individual fleet agent's config.

## When routing applies

Routing applies whenever an account has **room/group-chat access**
(`RoomMode()` is true). Pure `1:1` accounts send their reply to the owner
verbatim and need no routing line.

## The `to: <target>` directive

Every reply from the agent must begin with a line naming its destination:

```
to: <target>
body…
```

`<target>` is one of three forms:

| Form | Meaning |
|---|---|
| `<jid>` | a channel: a joined room, the owner, or a known occupant |
| `<stanza-id>` | the message to answer; the bridge resolves it to that message's author |
| `noop` | send nothing (deliberate silence) |

Rules:

- The directive line starts with `to:` (case-insensitive).
- A jid target must contain `@`, and a stanza-id target must match one of the
  two id shapes (`stanzaIDRe`: an 8-4-4-4-12 hex UUID, or 16 bare hex
  characters), so ordinary prose like "to: be fair" is not mistaken for a
  route.
- The body follows on the next line(s).
- One reply can carry **several** `to:` lines — each starts an independent
  message, fanning out to different destinations.
- Text **before the first** `to:` line is dropped as a malformed-routing error.

### Choosing a destination

The jid forms are the default. Use a stanza id only when a bare jid does not
say which message a reply answers — several outstanding messages from the same
room or person.

- **Reply where the message came from** — the prompt's `from:` JID.
- **DM the person who sent it** — the prompt's `sender:` JID (room messages).
- **Reach the owner** — `to: <owner-jid>` (per-account `owner` in config).
- **A joined room** → groupchat; the owner or a known occupant → `1:1` chat.
- **Answer one message out of several** — the prompt's `stanza-id:` value (see
  below).

### Answering one message: `to: <stanza-id>`

Every prompt that carries a message includes a `stanza-id:` line. Use that id
in place of a jid to answer that specific message:

```
to: 3e2597d4-a470-4cdb-b972-431043bce34f
A

to: a8508c81-0e1b-4e48-ae16-61256b837670
B
```

**When to use it.** The stanza-id form answers one of several messages in a
single prompt (several people in a room, or a batch from one sender). When the
latest prompt contains exactly one message, a plain `to: <jid>` already
identifies what you are answering — the stanza-id form is redundant there and
should not be used.

The bridge does two things with the id:

1. It looks the id up in `msgHistory` (`xmpp.go`) and sends to the author of
   that message. A room message is recorded as `room@muc/nick`, which collapses
   to the room, so the reply goes back to the room.
2. It stamps the outbound message with a **XEP-0461** `<reply/>` element naming
   the answered message, so a client threads the reply under it:

   ```xml
   <reply xmlns="urn:xmpp:reply:0" to="<author jid>" id="<stanza id>"/>
   ```

   Both attributes are mandatory. Only the **first** chunk of a split reply
   carries the element. Rooms are stamped too.

Destination and attribution travel in one token, so one run can emit several
replies, each stamped to its own message.

Warning: the id must be complete and known. A malformed or unknown id is a
routing failure, handled by the normal reject path (error room plus a
settle-time nudge). It is **not** a silent fallback to the turn destination.
This is a deliberate tradeoff (#54): a mistyped id costs the message, so a
wrong id is loud rather than quietly mis-delivered.

`stanzaIDRe` (`bridge.go`) accepts two shapes: the 8-4-4-4-12 hex UUID that
most clients put on a message, and the 16 bare hex characters `newStanzaID`
emits for pi-msg's own stanzas. A client whose ids match neither shape cannot be
answered by id; the fix there is to widen that pattern, not to guess.

Note on history: PRs #50/#51 tried to infer the answered message inside the
bridge, and #52 reverted them. That attempt failed twice over. Its trigger was
a race against the `pending1to1` FIFO, and it emitted `urn:xmpp:reply:0` with
the stanza id in the `to` attribute and no `id` attribute, while calling it
XEP-0359. The model knows which message it answers, so it names one.

### Deliberate silence: `to: noop`

`to: noop` means the agent deliberately has nothing to send:

- Sends **no stanza** at all.
- Counts as having replied, so the 🫡 reaction for unanswered runs is not sent.
- Discards any body that follows it.
- Works in **1:1 mode too** (`leadingNoop`), which parses no other `to:` form.
  A 1:1 account has no routing contract, but it still needs a way to say
  "nothing to send": without one, a deliberate silence looks like a reply that
  went missing, and the empty-tail recovery and the unanswered-message hint both
  argue with it. Only the **first non-empty line** routes — a `to: noop` further
  down a 1:1 reply is ordinary text and is sent as written.

### Addressing other agents in a room

Two forms reach another agent, and they are the same rule the receiving bridge
applies to its own trigger:

- `@name` — wake that agent (handoff), anywhere in the message.
- `name` with no sigil — also reaches it. A bare name counts as addressing the
  agent, so *"ask peppy for the path"* and *"beltino handed over to fox"* wake
  those agents. This is deliberate: an unaddressed room message does not exist
  for an agent, so prose that names one is the only signal it will get
  ([#106](https://github.com/zachpmanson/pi-msg/issues/106)).
- `@everyone` — address the whole room.
- `@free` — address the room's **free** agents: those whose presence has drifted
  to `<show>away</show>` (idle past `idleAwayTimeout`), plus a bridge that has not
  been asked to do anything since it started, or since the operator reset it with
  `/new`. Both halves matter because a (re)started bridge announces
  `awake`/`resumed` with an empty `<show>` and needs `idleAwayTimeout` of quiet
  before it drifts to away — so without them a fleet that had just been deployed
  or reset was dark to `@free` for 20 minutes even though nothing had been asked of
  it. An agent that is working (`dnd`), or that has worked and has only just gone
  quiet, is not reached. `/new` does not kill background processes, so a bridge
  that settles into `waiting on N processes` stays out of reach. This is the only
  state-gated address form; every other name is delivered whatever the recipient's
  presence says.
- A mistyped name matches nobody; pi-msg warns the agent that nobody was woken.

**The same rule is enforced on the way out.** A room message that addresses
nobody is delivered to no agent at all, so a tagless reply written in answer to
another agent's message sits in the room looking delivered while no agent ever
sees it. pi-msg therefore warns the sender, at most once per run, when a
peer-triggered turn sends a room message that names no `@handle`, no bare name
and no `@everyone`. A report written for the owner alone is untagged on purpose,
so an owner-triggered turn is never warned about. Nothing is blocked: the message
did reach the room, so the correction only has to reach other agents if one must
act on it.

**The owner's messages follow the same names, but with the untagged case
defaulted.** An owner room message that names nobody reaches the room's **free**
agents plus **anyone who has spoken in that room in the last 20 minutes**: if you
were part of the conversation, the next turn of it is yours to hear, so a live
exchange keeps its participants without anyone having to re-tag them
([#130](https://github.com/zachpmanson/pi-msg/issues/130) follow-up). An agent
that is working, or that has gone quiet in a conversation it was not part of, is
not interrupted. The participation window is per room and matches
`idleAwayTimeout`, so a room's untagged owner traffic always reaches exactly those
agents that either spoke there recently or have been away long enough to count as
idle. Participation does **not** widen `@free`: that form stays a summons to the
idle only, and an owner who tags `@free` is deliberately not asking the agents
who are already in the conversation. One that tags an agent or replies to an
agent's stanza is routed to that account alone; an `@everyone` reaches the whole
room, working agents included. A reply target the bridge has never seen (it may
be its own message from before a restart) and a name that matches no known
occupant both fall back to the untagged case, since neither resolves to another
account. ([#106](https://github.com/zachpmanson/pi-msg/issues/106),
[#130](https://github.com/zachpmanson/pi-msg/issues/130))

An untagged message (or `@free`) that does not reach us is **dropped**: no turn,
nothing queued, nothing deferred to a later away period. It stays in the MAM
archive, so `read_room` still finds it and the owner can tag it `@all` if it was
meant for a working agent too. Whether a message cleared the gate travels with it
(the durable inbox marks it as addressed), so a restart cannot orphan one.

Word boundaries apply, and quoted (`> …`), fenced and inline `` `code` `` content is
ignored, so pasting a transcript — or quoting a handle while explaining these very rules —
does not address anyone and `api` never wakes `pi`. One
consequence matters when writing bridge text: a sentence that merely *mentions*
an agent now addresses it, so an announcement authored by the bridge avoids
naming agents at all (the cascade-stop notice says "no longer answering agent
handoffs" rather than naming itself).

## Failure handling / on-failure nudge

Routing failures are handled by rejection + a bounded corrective:

- Text with no `to:` line, text before the first `to:`, a non-allowlisted
  destination, or an **unknown stanza id** is **not dropped silently**: it is
  forwarded to the write-only error room — the `rooms` entry with
  `"role": "error"` (`routeDropped`) — and a corrective is staged.
- Intermediate/mid-run commentary that fails to route only fills the error
  room and is **not** nudged; the agent is only corrected if the run's
  **FINAL** message was malformed (issue #16).
- At `agent_settled`, if the final message was malformed, `firePendingNudge`
  prompts the agent to resend with a valid `to:` line — bounded to
  `maxRoutingNudges` per user turn.

## Unanswered-message hint

A message that arrives mid-run is injected as a steer, so the agent can read it
before it writes the answer to the previous one, and then never write that
answer. When a run takes in two or more messages and sends fewer replies,
`fireUnansweredHint` asks the agent once per user turn to check for messages
that still need an answer.

`recordDelivery` counts one answer per **delivered segment**, not per assistant
message. One reply with three `to:` lines is three answers. Counting it as one
made a run that answered every message look unbalanced, and the hint then fired
for work that was already done.

The hint prints the run's chat history: every message that came in and every
reply that went out, in arrival order, each with its stanza id. The agent cannot
see the XMPP traffic, so the two counts alone ("3 in, 2 out") leave it to
reconstruct the run from its own context, and it gets that wrong. The history
turns the check into a comparison. A long body is excerpted to its first few
words (`hintExcerpt`); a `to: noop` shows as a deliberate silence.

The hint asks for `to: <jid|stanza-id>`. Here a stanza id is the useful form:
several messages are in play, and only the id says which one a reply is for.
`to: noop` is the explicit way for the agent to say its replies already covered
everything.

In 1:1 mode the hint asks for no routing line at all — that account parses none,
so the literal text would reach the owner. It asks for the outstanding answers
and offers `to: noop`, which works in both modes.

## Room-triggered prompts are pointer blocks (#58)

A room-triggered prompt does **not** carry the message body. The agent is told
what reached it and given the addressing meta (the room, the sender, the stanza
id), and pulls the text itself with the `read_room` tool. The body is reachable
only through the archive; it never enters the prompt, so an unaddressed
conversation cannot be reconstructed from stale prompt text and the agent must
look rather than guess.

The label is `[pi-msg: room: …]` for every case; the case lives in the first
sentence of the commentary, not in the label:

| Case | First sentence | Fields |
|---|---|---|
| a peer tagged us | `You were tagged in a room.` | `from:`, `sender:`, `stanza-id:`, `react-to:` |
| several tags in one turn | `<N> messages tagged you in a room. One read covers all of them.` | `from:`, `react-to:`, then one list entry per tag |
| the owner spoke to the room, naming nobody (or `@free`) | `The owner spoke to the room without naming anyone.` | `from:`, `sender:`, `stanza-id:`, `react-to:` |
| an `@free` summons from a peer | `The room's idle agents were summoned.` | `from:`, `sender:`, `stanza-id:`, `react-to:` |
| a reaction ack | `<reactor> reacted <emoji> to your message "<our own message>"` | inline, no field lines |
| a reply to our message | `Your message was replied to.` | `from:`, `sender:`, `stanza-id:`, `in-reply-to:` (parent id only), `react-to:` |

Rules:

- Commentary is sentence case; field names, the tool name and its arguments are
  lowercase. A jid that appears in the meta lines is not repeated in the
  commentary.
- The `read_room` call is the last line (`limit=15`; `limit=30` for the
  multi-tag block). It carries no failed-read clause: `read_room` already
  reports its own failures.
- Only the reaction ack carries an excerpt, and it is of **our own** message
  being reacted to — the reactor reacts to something we said, so quoting the
  reaction itself would say nothing. The excerpt comes from the stanza history,
  which records outbound bodies at send time.
- Every block still carries the routable stanza id, so `to: <stanza-id>` and
  `to: <jid>` both keep working. The pointer text is bridge text, not an inbound
  message, and is never counted as one.

The multi-tag block is **not yet implemented**: each addressed message is one
prompt today, and composing one block for several would need the run machinery
that buffers a steer, so only the single-message cases ship (#58).

## Marking bridge text: `[pi-msg: <topic>: …]`

Every prompt or block the bridge injects is wrapped in one square-bracket pair
and labelled, so the agent can always tell bridge text from a person's words:

```
[pi-msg: routing: …]      the session seed, the routing nudge, the mention warning
[pi-msg: rooms: …]        the room list and the one-addressing rule (#106)
[pi-msg: room: …]         a room pointer block (#58, see above)
[pi-msg: unanswered: …]   the unanswered-message hint
[pi-msg: recovery: …]     the empty-tail recovery prompt
[pi-msg: read_room: …]    a read_room result (XEP-0313 transcript)
[pi-msg: startup: …]      the resume volunteer turn
```

The stderr log uses `[pi-msg] <level>: <message>`, which is a different thing:
it never reaches the agent.

## On-start seed

At the start of a **fresh** session (startup with no session to resume, or
after `/new`) pi-msg injects `routingContract()` into the first prompt so the
agent knows the protocol. Resumed sessions are not re-seeded (their context
already contains it). There is **no** per-message routing hint — this is the
only runtime injection besides the on-failure nudge, keeping per-message token
cost to zero for the routing rules.
