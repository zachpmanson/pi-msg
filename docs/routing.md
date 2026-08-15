# Reply Routing (pi-msg)

This is the **canonical specification** of how pi-msg routes an agent's replies.
It is the single source of truth for the routing protocol; the enforcement
lives in `bridge.go` (`splitReplySegments`, `routeLine`, `deliverReply`,
`firePendingNudge`) and the on-start description the agent receives lives in
`Bridge.routingContract()`. This protocol belongs to **pi-msg**, not to any
individual fleet agent's config.

## When routing applies

Routing applies whenever an account has **room/group-chat access**
(`RoomMode()` is true). Pure `1:1` accounts send their reply to the owner
verbatim and need no routing line.

## The `to: <jid>` directive

Every reply from the agent must begin with a line naming its destination:

```
to: <jid>
body…
```

Rules:

- The directive line starts with `to:` (case-insensitive); the target must
  contain `@` so ordinary prose like "to: be fair" is not mistaken for a route.
- The body follows on the next line(s).
- One reply can carry **several** `to:` lines — each starts an independent
  message, fanning out to different destinations.
- Text **before the first** `to:` line is dropped as a malformed-routing error.

### Choosing a destination

- **Reply where the message came from** — the prompt's `from:` JID.
- **DM the person who sent it** — the prompt's `sender:` JID (room messages).
- **Reach the owner** — `to: <owner-jid>` (per-account `owner` in config).
- **A joined room** → groupchat; the owner or a known occupant → `1:1` chat.

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

- Text with no `to:` line, text before the first `to:`, or a non-allowlisted
  destination is **not dropped silently**: it is forwarded to the write-only
  `errorRoom` (`routeDropped`) and a corrective is staged.
- Intermediate/mid-run commentary that fails to route only fills the error
  room and is **not** nudged; the agent is only corrected if the run's
  **FINAL** message was malformed (issue #16).
- At `agent_settled`, if the final message was malformed, `firePendingNudge`
  prompts the agent to resend with a valid `to:` line — bounded to
  `maxRoutingNudges` per user turn.

## On-start seed

At the start of a **fresh** session (startup with no session to resume, or
after `/new`) pi-msg injects `routingContract()` into the first prompt so the
agent knows the protocol. Resumed sessions are not re-seeded (their context
already contains it). There is **no** per-message routing hint — this is the
only runtime injection besides the on-failure nudge, keeping per-message token
cost to zero for the routing rules.
