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

- **Answer a specific message** — the prompt's `stanza-id:` value (see below).
- **Reply where the message came from** — the prompt's `from:` JID.
- **DM the person who sent it** — the prompt's `sender:` JID (room messages).
- **Reach the owner** — `to: <owner-jid>` (per-account `owner` in config).
- **A joined room** → groupchat; the owner or a known occupant → `1:1` chat.

### Answering one message: `to: <stanza-id>`

Every prompt that carries a message includes a `stanza-id:` line. Use that id
in place of a jid to answer that specific message:

```
to: 3e2597d4-a470-4cdb-b972-431043bce34f
A

to: a8508c81-0e1b-4e48-ae16-61256b837670
B
```

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
- Counts as having replied, so the "done (no reply)" nudge does not turn room
  silence into owner DM noise.

### Addressing other agents in a room

Addressed via inline mention, not the `to:` line:

- `@name` — wake that agent (handoff), anywhere in the message.
- `@everyone` — address the whole room.
- A name mentioned **without** `@` does not reach that agent.
- A mistyped/self `@name` matches nobody; pi-msg warns the agent that nobody
  was woken.

## Failure handling / on-failure nudge

Routing failures are handled by rejection + a bounded corrective:

- Text with no `to:` line, text before the first `to:`, a non-allowlisted
  destination, or an **unknown stanza id** is **not dropped silently**: it is
  forwarded to the write-only `errorRoom` (`routeDropped`) and a corrective is
  staged.
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

The hint asks for `to: <jid|stanza-id>` and prefers the stanza id. This is
exactly the case a bare jid cannot disambiguate: several messages are in play,
and only the id says which one a reply is for. `to: noop` is the explicit way
for the agent to say its replies already covered everything.

## On-start seed

At the start of a **fresh** session (startup with no session to resume, or
after `/new`) pi-msg injects `routingContract()` into the first prompt so the
agent knows the protocol. Resumed sessions are not re-seeded (their context
already contains it). There is **no** per-message routing hint — this is the
only runtime injection besides the on-failure nudge, keeping per-message token
cost to zero for the routing rules.
