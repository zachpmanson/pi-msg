# `/next`: fresh chat, background reconciliation

Owner controls `/next`, `!next`, and a single standalone `&` all do the same
thing. They take **no arguments**. `& stuff`, `foo &`, and `&&` remain ordinary
messages. In rooms, normal owner/addressing rules still apply (for example,
`beltino: &`); peers cannot invoke bridge controls.

Configure each account explicitly:

```json
"reconcileSkill": "/home/beltino/beltino/.pi/skills/reconcile/SKILL.md"
```

Unset disables `/next` with an actionable error. The configured path must be
absolute and point to a nonempty, readable regular file. It is read and copied
before resetting. A sibling `../learn/SKILL.md`, when present, is also read and
copied and explicitly supplied. A customized, self-contained skill need not
have that sibling or use a particular frontmatter name. Nested skill resolution
uses supplied copies/original skill paths, not persona extension discovery;
missing nested instructions must be reported, never silently invented. Use an
unattended skill: no questions, interactive login, or waiting for approval.

## Sequence and notices

1. Durably accept one pending boundary and acknowledge its identity immediately,
   even while busy. Duplicate aliases refer to the same boundary and never retry
   a failed reset. Later eligible owner/peer chat is held in a protected durable
   inbox, FIFO by admission, not sent as old-session steers. Old work finishes
   normally, including settlement/recovery, pending prompt acceptance, heartbeats
   and owned background processes/subagents. An off-event-loop executor waits
   for every bridge guard and an ordered actual Pi idle/noncompacting/no-pending
   observation before reserving reset. No snapshot/reset occurs while busy.
2. Snapshot the old JSONL, original cwd, model and thinking level. Register a
   durable job and ready a separate internal runner **before** reset. Preparation
   errors leave the primary unchanged.
3. Call the same `new_session` RPC as `/new`. Failure or `cancelled:true` never
   authorizes reconciliation or a fresh/queued success notice. Persist successful
   reset authority before opening the worker gate or sending that notice.
   On success, prove a distinct actual identity and durably persist the exact
   resume pointer and target before held assignment/submission. Re-arm
   messaging seeding and fresh-session state, and return immediately without
   waiting for any model response.
4. Send `new session ready; [reconcile job-…] queued in background`. The runner
   waits for a shared advisory lock, then starts restricted Pi on a **fork of the
   private copy**, never on the original JSONL. On observed process start the
   bridge sends `reconciliation running in background`. A job finishing before
   the first poll instead gets the truthful `reconciliation ran in background`.
5. Release held chat FIFO into the proven target, preserving authority, original
   routing/reply/marker metadata and protected keys. Only one protected prompt
   enters asynchronous preflight at a time; correlated success releases its
   successor. Retire the admission fence once all held inputs are accepted and
   their exact target has a usable resume pointer, without waiting for settlement.
   Pi may accept prompts before writing the session file at first assistant
   output. Until that file becomes usable, keep the boundary in `releasing`
   and hold the admission fence; poll only the same target's release, without
   resubmitting accepted prompts, resetting, or restarting reconciliation. Later
   input still joins this target's FIFO. Protected submissions explicitly request
   native steering, valid both idle and busy, rather than relying on lagging bridge
   busy flags. Target tags remain until consumption. Rejection/submission failure
   or a missing/mismatched saved pointer blocks.
6. Send a labelled completion/failure/cancellation summary directly to the
   initiating conversation. Later turns, `/new`, and subsequent `/next` calls
   cannot redirect it. Worker output never enters the fresh primary context.

The final assistant output is relayed by the bridge, unlike ordinary chat
turns. Nonzero exits, absent final output, malformed events, and assistant
provider error/aborted/truncated endings do not count as success. A successful
process can still report skipped issues or wiki blockers in its summary.

## Scope and concurrency

The worker is instructed to perform only issue/wiki reconciliation, including
learn, not unfinished historical requests. No application edits, PR creation,
deployment, service restarts, messages, or child agents. The documented
wiki/raw/skill changes and their selectively staged commit/push remain allowed.
It loads no messaging/orchestration extensions, context files, prompt templates,
or discovered skills. Only the supplied skills and built-in
`read,bash,edit,write,grep,find,ls` tools are enabled; credentials remain available.
This is **not a sandbox**: bash and normal filesystem/network access are needed
for `gh` and wiki work. Scope depends on the reviewed skill and model adherence.

All pi-msg reconcile workers for the same OS user share
`~/.local/state/pi-msg/reconcile.lock`, independent of persona/config directory.
The lock covers the **entire** worker, including learn and persistence. Other
users, manual reconcile runs, and unrelated agents writing the wiki do **not**
participate. Their writes can still conflict; preserve peer edits, verify current
issue state, deduplicate actions, and selectively stage. Several queued jobs
can consume time; the fixed 30-minute deadline includes lock waiting.

## Recovery and operations

Private artifacts are retained in
`<config-dir>/<canonical-account>.reconcile/job-…/`:

- `job.json`: source/cwd/model/thinking, original skill paths, initiating
  destination, runner PID, state, summary and timestamp; atomic durable updates.
- `snapshot.jsonl`, snapshotted skill files, `sessions/`: private source and
  independent Pi fork; sensitive transcript content, not public exports.
- `events.jsonl`, `worker.stderr`, `runner.log`: worker output and diagnostics.
- `reset-authorized`: durable successful-reset authority and queued-notice intent,
  written only after a successful, non-cancelled reset.
- `runner.lock`, `queued-reported`, `running-reported`, `reported`: liveness and
  separate queued/start/final notice tracking.

States are `prepared`, `queued`, `running`, then `completed`, `failed`,
`cancelled`, or `interrupted`. The runner is independent of the primary/session
and can continue when just the bridge exits. **Service restarts may kill the
entire control group**, including runner and worker; there is no service escape
or guarantee of surviving a restart. Startup monitors live jobs and reports dead
nonterminal jobs as interrupted. No automatic rerun: GitHub/wiki effects may
already be partial. Inspect artifacts and current remote state before deciding
whether to invoke a new reconciliation. Never blindly replay a failed job.
A bridge crash after reset but before persisting authority leaves reset success
uncertain: no fresh/queued success is claimed and no worker is authorized. A crash
after authority but before the gate opens can still report the successful reset,
but the job is interrupted without worker effects. Neither gap is auto-replayed.

The deadline terminates Pi with SIGTERM (Pi cleans up its tracked shell children),
then escalates to its process group after five seconds. For manual cancellation,
verify the live runner PID from `job.json` against its command line and job lock
before sending SIGTERM. `/abort`, `/new`, and `!` without a pending boundary affect only the primary, not
these jobs. `/quit` does not promise cancellation of independent runners.
Hard kills or untracked/detached external processes can defeat graceful cleanup;
external requests already sent cannot be rolled back. Pi inherits the job and
shared locks so a surviving Pi cannot be mistaken for a dead runner or overlap
another reconciliation just because its runner died.

Queued/start/final notices retry in that order while the bridge cannot send,
including after reconnect/restart and when a worker finishes before the first
successful notice. A delayed queued acknowledgement refers to the authorized
reset; the subsequent start/final notice reports the observed job outcome.
Once XMPP accepts a stanza, a local
marker suppresses repeats across restarts; a crash between sending and marking
can duplicate a notice (not worker effects). This is not end-to-end delivery
acknowledgement. No automatic retention policy: archive/delete terminal job
directories manually after inspection; never delete live jobs or lock files.
Presence remains primary/session-owned and does not count these independent jobs.

## Queued-boundary recovery and candidate acceptance gates

The private pending record is `<config-dir>/<account>.next.json`. Protected inbox
entries retain boundary identity, local keys and (after proof) exact target
identity. They are never age-acked or capped like ordinary input. Encoded records
are limited to 1 MiB minus one byte (excluding LF), ensuring restart readability.
Append failures, including oversize input, are reported explicitly; input never
falls through to the old session. If not accepted durably, retain and resend it
after repair.

Interrupted pre-reset boundaries become **blocked**, even if startup appears idle.
A proven post-reset target may resume/replay input at least once, without another
reset or reopening a reconciliation gate. Missing/corrupt/mismatched state or
unusable target files hold with diagnostics; protected data never uses generic
fresh-session fallback. A crash while waiting for the target's first file write
cannot recover until a usable exact file and matching saved pointer exist;
startup refuses to substitute a fresh session. The historical blocked diagnostic
`accepted target has no usable exact resume pointer` alone is recoverable on
restart once that exact file/pointer is usable. Recovery still proves the active
RPC identity and file before assigning or replaying input, never resets or
reopens a reconciliation gate, and leaves every other blocked reason blocked.
Stateless initial-prompt launches refuse another boundary.
Assigned protected input without a pending record recovers only for exact-target
replay, proving actual resumed identity before dispatch; mixed or unassigned
orphan data fails closed. A later `&` creates a distinct boundary even during the
fresh turn, but no subsequent reset (including `/new` or idle reset) may cross
unresolved older exact-target input. Old tags are never reassigned.

Unresolved ordinary old-session input blocks reset and cannot age-ack while the
boundary is pending. Proven old consumption can clear it. Recovered ordinary
input is conservatively retained, not migrated into a newer boundary; historical
destructive controls are quarantined rather than executed against today's session.
Explicit pre-target `/new` may discard old primary inbox work, reporting the
count, while preserving every post-boundary protected entry.

While pending, native controls are refused without redirecting ongoing old-turn
output. Unknown extension commands are held as chat; peer apparent controls never
gain owner authority. **The owner decision for /abort, /stop and bare ! remains
outstanding**: this candidate refuses them, does not abort old work or discard
held chat, and is not control-complete. No-boundary abort/interrupt is unchanged.
/quit preserves the durable boundary and queue.

Explicit owner /new may supersede waiting/blocked reconciliation only before any
held entry is target-bound/submitted, and only while the executor is inactive.
It uses deliberate-new abort/reset semantics, proves and persists a distinct
target, then releases input without authorizing/restarting the old job. Already
assigned/partially released input is refused, never silently retargeted;
post-target recovery is limited to proven exact-target replay and the historical
session-materialization condition described above; other blocked states require
inspection.

Automatic companion count/heartbeat relays include captured session identity;
stale buffered old-session relays cannot change or wake the fresh primary.
Physical-work guards remain, but zero active subagent artifacts does **not** prove
completion notices were drained. Actual Pi replacement emits session_shutdown
and disposes old extensions, potentially cancelling undelivered batched notices.
No external plugin changes or sleeps are used, and neither a proven real
cross-session leak nor proven notification preservation is claimed. Installed-Pi
notification-drain/disposable-droid validation and independent review remain
required before rollout.
