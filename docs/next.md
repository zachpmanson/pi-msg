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

1. Refuse while the primary is streaming, compacting, awaits prompt acceptance,
   has pending messages, is still processing settlement (including recovery/hints/heartbeats),
   or owns background shells/subagents. Every bridge prompt is reserved before
   its pipe write, until its correlated acceptance/rejection response is processed;
   a rejected input does not invent permanent streaming activity. Synthetic
   submissions also reserve old-turn state inspection atomically against `/next`;
   old-session alarms cannot enter the fresh primary during the transition. Wait for work to settle; `/next` never
   aborts or snapshots a live run.
2. Snapshot the old JSONL, original cwd, model and thinking level. Register a
   durable job and ready a separate internal runner **before** reset. Preparation
   errors leave the primary unchanged.
3. Call the same `new_session` RPC as `/new`. Failure or `cancelled:true` never
   authorizes reconciliation or a fresh/queued success notice. Persist successful
   reset authority before opening the worker gate or sending that notice.
   On success, refresh the resume pointer, re-arm
   messaging seeding and fresh-session state, and return immediately without
   waiting for any model response.
4. Send `new session ready; [reconcile job-…] queued in background`. The runner
   waits for a shared advisory lock, then starts restricted Pi on a **fork of the
   private copy**, never on the original JSONL. On observed process start the
   bridge sends `reconciliation running in background`. A job finishing before
   the first poll instead gets the truthful `reconciliation ran in background`.
5. Send a labelled completion/failure/cancellation summary directly to the
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
before sending SIGTERM. `/abort`, `/new`, and `!` affect only the primary, not
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
