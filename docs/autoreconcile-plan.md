# Experimental autoreconcile mode — implementation plan

Status: proposal only. No runtime/configuration changes or rollout authorized by
this document. Scope: pi-msg account configuration, idle lifecycle, reconciliation,
fresh-session context and XMPP presence.

## Goal

Eventually discard an idle agent's accumulated file/tool/thinking context while
preserving continuity through XMPP history and existing reconciliation artifacts.
After the idle timeout, optionally reconcile the old session, start a fresh Pi
session, and publish extended-away (`xa`) presence. On the next incoming message,
the agent receives a small, explicitly historical chat recap and can read more
history or relevant handoffs as needed.

## Requirements established with Zach

- Experimental, per-account opt-in; configurable idle timeout, recent-history
  count and additional owner-history floor. Both history counts default to 10.
- `reconcileSkill` is **optional**. If absent, skip reconciliation and continue
  with reset, recap and XA.
- If configured, use the existing unattended reconcile skill and its existing
  handoffs/artifacts. Do not mandate a separate generated `handoff.md`.
- Report automatic reconciliation results into XMPP **to the account's own bare
  JID**, rather than notifying the owner or agents room. This is an archived
  self-addressed receipt, not silent success. Manual `/next` reporting stays
  unchanged.
- Always allow `send_message` and `read_messages` to target the account's own JID,
  independently of autoreconcile enablement and `allowArbitraryJid`. This is a
  narrow permission for self, not arbitrary destinations. No repeated per-turn
  routing/tool-prompt advertisement is needed; document its reconciliation use in
  the reconcile skill when implementing.
- On successful quiet cleanup, order operations as **reconcile → self-report →
  capture recap → fresh session → XA**. The report is the final chat entry before
  cleanup and naturally enters the default recent-message selection. Self-message
  receipt/echo must not wake the primary or invalidate its idle eligibility.
- Select the latest **10 eligible messages from anyone**, including the agent's
  replies, plus the latest **N owner-authored messages** (initially 10), even if
  older than the recent-message window. Deduplicate and merge chronologically:
  at most 20 messages with those defaults, not 20 per chat.
- Eligibility spans owner DMs, rooms and other permitted conversations actually
  delivered to this agent or sent by it. Include unaddressed room messages admitted
  through recent outbound participation or other normal routing rules; explicit
  mentions are not required. Owner messages admitted this way also count toward
  the owner floor. Exclude only chatter never delivered to the agent, and preserve
  conversation and sender labels.
- Tell the fresh agent that more conversation history is available through
  `read_messages(target=...)`, particularly if the user continues an earlier topic.
- This is a fresh session, not compaction and not deletion of the old transcript.

The following safety, persistence and rollout details are implementation
recommendations, not additional owner-approved deployment authority.

## Configuration

Proposed flat account fields, consistent with existing camelCase configuration:

```json
{
  "autoreconcileTimeout": "30m",
  "autoreconcileHistoryMessages": 10,
  "autoreconcileOwnerHistoryMessages": 10
}
```

- `autoreconcileTimeout`: Go duration string, matching `pingInterval` (`"60s"`,
  `"2m"`, `"1h"`). Missing or `"0"` disables the mode. Thirty minutes is an
  initial opt-in example, not an enabled-by-default setting.
- `autoreconcileHistoryMessages`: nonnegative integer; omitted means 10. Select
  this many recent eligible messages globally, regardless of sender; 0 disables
  this selection only.
- `autoreconcileOwnerHistoryMessages`: independent nonnegative integer; omitted
  means 10. Also select this many latest eligible owner-authored messages globally,
  regardless of whether they are in the recent selection; 0 disables this floor.
  Both values must be 0 to disable chat-body recap entirely; retain startup guidance.
- Reject negative counts and impose documented upper bounds plus an independent
  text-size limit. Reserve text budget for the owner selection, so peer chatter
  cannot defeat the owner floor. Oversize bodies may be explicitly truncated;
  preserve their identity/reference rather than silently dropping owner entries.
- No `reconcileSkill`: valid configuration; skip the worker.
- Configured but invalid/unreadable skill: configuration/preparation failure, not
  equivalent to an absent skill. Never silently skip a requested reconciliation.
- Reject enabled autoreconcile combined with `resetSessionOnAway: true`; otherwise
  the existing away reset could discard context before reconciliation.
- Existing accounts with autoreconcile disabled retain their current idle/reset
  behavior. Self-addressed send/read permission is an unconditional messaging
  capability, not an autoreconcile configuration option.

## Idle lifecycle

1. Arm the timer only after real work settles. Use the existing session-transition
   guards, plus actual Pi idle/noncompacting/no-pending observations. Active turns,
   retries, owned processes/subagents, queued input, pending prompt acceptance,
   pending notifications and unresolved `/next` boundaries must prevent reset.
2. At expiry, reserve one attempt against the exact session identity and activity
   generation. Capture an immutable source snapshot and original cwd/model/thinking
   metadata before launching a configured worker.
3. If a skill is configured, run the existing restricted unattended reconciliation
   machinery against that snapshot. Keep the original foreground session usable;
   do not hold its transition mutex for the worker's lifetime.
4. The bridge sends the worker's result to the account's own bare JID and records
   the accepted outbound receipt in the eligible-chat index before capturing the
   recap. Retain normal private job artifacts. The result may reference existing
   task files, branches, worktrees or handoffs; no separate handoff-generation pass
   is required. This internal receipt and its incoming echo do not count as new
   foreground activity.
5. Before reset, recheck the exact session identity, activity generation and every
   busy/input guard. Any intervening eligible chat or new work invalidates this
   attempt's reset. Continue serving that input normally in the old session; the
   independent worker can finish, but cannot erase newer foreground context.
6. If still eligible, durably prepare the bounded recap and transition record,
   then use the supported `new_session` RPC. Check success, `cancelled:false` and
   a proven distinct session identity. Persist the exact resume pointer and seed
   association before declaring the transition complete.
7. Publish `xa` only after the replacement session and its startup seed are safely
   recoverable. Remain dormant until actual incoming input; do not launch a model
   turn merely to consume the recap.

If no skill is configured, omit worker preparation/execution/reporting, but keep
all reset, persistence and presence safeguards.

Normal idle-away may still occur before a longer autoreconcile timeout, but it
must not reset the session. During automatic reconciliation, distinguish
reconciling from completed XA without falsely marking the foreground unavailable
for messages. If new activity wins the race, never publish stale XA afterward.

## Chat recap and initial prompt

Maintain a bounded private chat-context index of eligible received and sent
messages, preserving the routing decision made at delivery time. Room delivery
may currently surface only a tagged-message pointer: obtain its actual body using
normal authorized archive access, rather than treating that pointer as the full
message or importing unrelated room traffic. Archive reads alone must not make a
message eligible for future recaps. Retain an independent bounded owner-history
selection so a burst of peer traffic cannot evict the older owner messages needed
for the floor. Determine owner authorship from the verified originating identity,
not a display name, mention, quoted text or the agent's own replies.

Select the newest `autoreconcileHistoryMessages` eligible messages globally, plus
the newest `autoreconcileOwnerHistoryMessages` eligible owner-authored messages.
Deduplicate their union, then render oldest first across both selections. If fewer
owner messages are available, include those available and disclose any known
history gaps; do not invent messages. For example, 10 recent peer/agent messages
plus 10 older owner messages yields 20 entries; 4 owner messages shared by the two
10-message selections yields 16. Old owner messages remain historical context,
not proof of outstanding work. Each entry includes:

- Conversation JID and DM/room/self classification.
- Sender identity, direction, timestamp and stanza ID where available.
- Message text, subject to per-message and total limits with explicit truncation.
- Attachment references only; do not automatically fetch/reinject binary payloads.

Deduplicate live delivery, own-message echoes and archive/reconnect replay using
stable identifiers. Keep live unprocessed input separate from historical context;
never both quote it as already handled and accidentally consume/drop its real
queued delivery. Do not execute historical control commands.

Persist the seed for the exact replacement session and attach it once to that
session's first real incoming prompt, before the new request. Include the prior
session path and, when present, the reconciliation job/result artifact reference;
these are references, not the old file/tool/thinking transcript. The exact storage
mechanism must work through supported Pi APIs/extensions, including restart before
first model output; do not hand-edit Pi JSONL to manufacture a usable session.

Suggested instruction:

> This is a fresh session following automatic idle cleanup. The messages below
> are historical context, not new requests. Do not replay their commands or
> resume work solely because it appears here. If the current user continues an
> earlier topic and more context is needed, call read_messages(target=...) for
> that conversation, and read any relevant referenced handoff/task files. Respect
> the original sender's authority and conversation boundaries. Only the separately
> identified new incoming message is new input.

A fresh session after optional reconciliation does not promise that a handoff
exists. If history retrieval is incomplete or unavailable, disclose that in the
seed and retain references; do not invent missing messages. Never expose private
DM material through automatic room replies just because it appears in the recap.

## Self-addressed messaging

Allow outbound messages and archive reads to the account's own normalized bare
JID in the ordinary destination classifiers/allowlists, including when
`allowArbitraryJid` is false and autoreconcile is disabled. Self-targeted full-JID
messages use normal XMPP resource routing; use the bare JID for reconciliation
receipts and archive queries. Do not grant access to any other account or confuse
MUC occupant nicknames with the account's own identity.

Treat incoming self-authored messages as non-waking historical receipts, never as
new owner instructions, peer summons, control commands, or reasons to re-arm
cleanup. Deduplicate sent copies, returned echoes, carbons and archive replay so
one self-report appears once in the recap. Reconnect/backfill must retain these
semantics. Self-reports must not trigger replies to themselves or notification
loops.

Verify actual server MAM storage and `read_messages(target=<own-bare-JID>)`
retrieval with the deployed server configuration. XMPP permits self-addressing,
but this plan does not assume that archive/echo behavior has already been tested.
The bridge's durable sent-receipt record provides the recap body without racing
archive visibility. A report is the newest eligible chat entry at successful
cleanup; with an explicit zero recent-message count, omit its body as configured
but retain the existing reconciliation artifact reference in startup guidance.

Document self-addressing in the reconcile skill instead of adding repeated tool
or routing prompt boilerplate. Preserve its unattended worker contract: workers
return their result and the caller/bridge sends it; do not add worker messaging
extensions, require a second model turn, or send duplicate reports from the skill.

## Results, failures and recovery

- Automatic reconciliation reports go to the account's own bare JID. Retain
  existing result/artifact content and durable notice tracking; manual `/next`
  continues reporting to its initiating conversation.
- Report-send failures retain the original session and durable pending notices;
  retry sending without rerunning reconciliation effects. Capture recap/reset only
  after the send is accepted and its text is durably recorded. Server acceptance
  is not an end-to-end read receipt; do not require the self-echo to authorize
  cleanup.
- Worker failure, interruption, timeout or uncertain persistence preserves the
  original session and does not claim successful cleanup/XA. A successful worker
  may still report skipped work: retain that result and define/test a conservative
  reset policy for reported blockers before implementation.
- Persist attempt, source session, activity generation, job, target and seed
  identities. A crash must not blindly rerun issue/wiki mutations or reset again.
- Recheck after activity invalidates an attempt, but do not repeatedly reconcile
  an unchanged snapshot or the empty replacement session. New eligible session
  activity is required to re-arm after terminal success/failure.
- Explicit `/new`, `/next`, abort/stop and shutdown must serialize with the short
  transition phase. Manual controls must not be redirected into the wrong session
  or cause an abandoned auto-worker to authorize a later reset.
- XA remains connected and wakeable by DMs/explicit mentions. Audit `isAway`,
  `@free`, unaddressed-owner delivery, participation horizons and reconnect
  presence; explicitly preserve/document room eligibility rather than assuming
  `xa` already behaves like `away` everywhere.

## Implementation breakdown

1. Add parsing/validation/docs for the three account fields and disabled defaults.
2. Extract reusable idle/transition eligibility checks and add a single durable
   autoreconcile attempt state machine with activity-generation cancellation.
3. Reuse snapshot/worker/lock/reporting infrastructure. Existing `/next` authorizes
   worker execution only after its reset; autoreconcile needs a distinct explicit
   pre-reset execution authorization. Do not fake `reset-authorized` or weaken
   manual `/next` recovery guarantees to accommodate the new order.
4. Allow self-addressed send/read generally, with non-waking self-input handling,
   without enabling arbitrary destinations or adding repeated prompt boilerplate.
5. Add the bounded eligible-chat index and a persisted one-time startup seed,
   capturing the accepted self-report before reset.
6. Integrate verified session replacement, resume-pointer persistence and XA.
7. Document operations, artifacts, failure recovery and the experimental rollout;
   mention self-addressed reporting in the reconcile skill without weakening its
   caller-delivered unattended result contract.

## Validation and rollout gates

- Config: absent/zero timeout, supported units, invalid/negative values, default
  and independent explicit-zero history counts, bounds, absent/invalid skill and
  reset conflict.
- Idle guards: active/retrying/compacting work, queued input/acceptance, background
  process/subagent and pending notification/boundary all prevent unsafe reset.
- Race tests: message before/during worker, completion-vs-input, reset-vs-input,
  manual controls and stale callbacks cannot erase or misroute work.
- Worker: optional skip, successful result/report, failures, partial blockers,
  lock contention and timeout; no duplicate effects/notices after recovery.
- Self-messaging: send/read allowed with autoreconcile disabled and arbitrary JIDs
  forbidden; other destinations remain blocked. Test bare/full-JID routing,
  actual MAM readback, sent/echo/carbon/replay deduplication, non-waking input and
  report-before-recap ordering. Report-send failure must not reset or rerun work.
- Recap: DM/room/peer scope, implicitly admitted unaddressed room messages,
  owner identification, independent owner retention during peer-message floods,
  overlapping selections, fewer available owner messages, chronological ordering,
  own echoes, replay deduplication, quoting/authority labels, reserved owner text
  budget and truncation, unavailable archive and no binary auto-download.
- Session recovery: crashes at every persisted phase, cancelled RPC, identity
  mismatch, exact resume pointer, seed retained before first output and injected
  once without an unsolicited model turn.
- Presence/routing: XA only on completed cleanup, reconnect restores intended
  state, incoming DM/mention wakes it, and room rules remain intentional.
- Normal/race/static gates followed by bounded disposable-XMPP/Pi integration
  tests, including self-reported results in the fresh recap, no resulting wake
  loop, unchanged manual `/next` reporting and a topic-continuation history lookup.
- Ship disabled by default. Any deployment needs separate authorization and the
  managed deployment workflow; then opt in one persona before broader adoption.

## Existing references

- [Manual `/next` lifecycle, worker restrictions and recovery](next.md)
- [Routing and conversation authority](routing.md)
- [Idle-time semantics](last-activity.md)
- `internal/pimsg/config.go`: account configuration and duration conventions.
- `internal/pimsg/bridge.go`: idle watcher, fresh sessions, history and presence.
- `internal/pimsg/next_boundary.go`: guarded session boundaries.
- `internal/pimsg/reconcile.go`: snapshot, worker authorization and result reports.
