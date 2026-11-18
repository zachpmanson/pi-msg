# Experimental idle autoreconcile

Disabled unless an account sets `autoreconcileTimeout` to a positive Go duration,
for example `"30m"`. Missing or `"0"` is inert: no history index, worker, session
transition or XA. History counts alone do not enable it. Optional
`autoreconcileHistoryMessages` and `autoreconcileOwnerHistoryMessages` independently
default to 10 and accept 0..100. `resetSessionOnAway` cannot also be enabled.
`reconcileSkill` remains optional; an explicitly configured unreadable skill is a
preparation failure, not a silent skip. Manual `/next` behavior/reporting remains
separate.

`awayTimeout` independently sets the idle-to-away presence threshold (a positive
Go duration, default `"20m"`; zero/negative/malformed values are rejected). For
example, `"awayTimeout": "5m"` with `"autoreconcileTimeout": "10m"` announces away
after five idle minutes and starts eligible automatic cleanup after ten idle
minutes; XA follows successful worker/native-transition completion, not a timer
alone. Setting awayTimeout does not enable cleanup or change the standard 20m
room-participation window. Other accounts retain their defaults when unset.

The bridge waits for actual settled Pi state and existing input/background-work
and notification guards, snapshots the exact source, and runs the existing
restricted worker when configured. Worker execution has separate pre-reset
`idle-authorized` authority, never fake `/next` reset authority. Foreground input
continues during the worker and invalidates reset eligibility. Completed results,
including ordinary skipped-work limitations, may proceed; hard worker failures
report but never reset. Automatic results are sent to the account's own bare JID.
Durable pending notices retry with the same stanza ID, never rerunning worker
effects. Server acceptance is not an end-to-end read receipt; transport/server
archive duplicates remain possible after an uncertain send, but recap IDs dedup.

Eligible history includes actual routed room bodies (including implicitly admitted
unaddressed owner messages), admitted DMs and accepted outgoing messages, not
archive reads alone. Capture starts when the positive timeout is enabled; there
is no complete pre-opt-in archive bootstrap. Arbitrary room archive reads are not
admission proof. An initial recap can therefore have fewer recent/owner entries;
the prior source session and job references remain available for context lookup.
The last N verified inbound owner messages have an independent window: replies
never consume N. A separate optional reply tail retains ALL accepted direct DMs
to the owner at or after the earliest of those N timestamps (inclusive), without
a reply-count cap. Room, peer and self outgoing messages gain no such protection;
they retain only the independent recent-message policy. Recent=0 does not disable
owner/reply protection; owner N=0 or an empty owner floor clears the reply tail.
The recent and owner windows each hold at most 100 entries, but the reply tail
has no count limit. Bodies retain at most 2048 UTF-8 bytes plus an explicit
truncation marker, with bounded identity labels. Files are private (0600).
The recap deduplicates and orders the union chronologically, labels sender/conversation/direction, and JSON-quotes all
historical source as inert, non-authorizing context. On restart retained legacy
owner-directed DMs are migrated before recent trimming, and all windows are
normalized, deduplicated and durably pruned to the current quotas/owner floor,
without changing an in-flight attempt's immutable seed or receipt identity.
Historical room owner-floor identity requires retained original verified delivery
provenance, not today's nickname mapping. Unproven archived/delayed room senders
remain unverified in the recap; pre-provenance persisted room entries cannot
establish an owner floor. Direct archive bare-JID origins remain trustworthy. It includes history lookup
instructions, prior session and private job references; no extra handoff-generator
turn runs. Future seeds describe a fresh context window with existing XMPP inbox
conversations: the next message may continue where things left off or be unrelated.
They direct the agent to use the supplied recap first and look up the relevant
conversation or handoff files only when relevant context is missing or insufficient.
Existing stored seeds are not rewritten to update guidance.
Quota increases, late/out-of-order owner timestamps and legacy migration cannot
recover already-evicted/pruned messages or full truncated bodies. No archive
bootstrap is added. Ordering uses existing message timestamps (missing inbound
stamps fall back to capture time; sends use acceptance time), not causal order.
A late owner message may move the floor earlier without recovering pruned replies.
The self result is durably indexed before seed capture.

Future seeds are visible native custom messages. New `/export` HTML copies also
show previously hidden context-participating `custom_message` entries, without
changing source sessions, live model context, attempts or receipts. Opaque custom
extension state is unchanged. Unsupported/malformed native export payloads fail
with a diagnostic, not a hidden upload or inline fallback. Previously uploaded
HTML is immutable and is not updated. `/dump pretty` and `/dump table` include
custom message types and bodies regardless of their stored display flag; raw
JSONL dumps remain unchanged. Exports and dumps retain owner-only file delivery.

Own-JID `send_message` and `read_messages` permission is always available,
independent of automatic cleanup and arbitrary-peer permission. Full JIDs retain
normal resource routing. Verified self incoming copies/echoes/carbon/backfill are
passive; self sends never arm reply-timeout wakeups. Live room self identity also
uses the verified real bare JID, including alternate nicknames. In enabled mode,
old-nick room archives/delays use conversation + stable stanza ID + matching body
from accepted outbound provenance in the existing bounded history. Archive reads
do not create this proof, and no extra journal is installed for disabled accounts.
Genuinely unknown/preexisting/out-of-retention old-nick copies cannot be proven
ours; they retain ordinary permitted peer replay behavior, not a universal
passivity guarantee. The preexisting historical room routing dependency on the
current occupant map is unchanged: it is not proof of historical origin or recap
owner authority. A broader historical routing policy is outside this change. Other destinations retain their normal
permission rules, including file-send restrictions.

## Native transition and persistence

Pi 0.84.4 does not normally flush a session before assistant output. The private,
provenance-checked extension command instead creates a standalone native
`SessionManager`, exclusively creates its native target path as an empty 0600
file, invokes public `setSessionFile` to initialize Pi's own header/final UUID,
then appends model/thinking metadata and the custom historical seed through public
APIs. It fsyncs file and directory, and uses one native `ctx.switchSession` to
rebuild the Agent with that FINAL UUID. No JSONL is hand-edited, no assistant is
fabricated, and no provider/startup turn is requested. A nonce-bearing durable
outcome plus independent active-session/header/seed proof is required: RPC prompt
success alone is insufficient. The exact resume pointer and seed association must
be durable before XA. Incoming DMs/mentions wake XA normally; XA also retains away
semantics for `@free`/owner room eligibility and reconnect presence.

## Diagnostics and recovery

Private per-account artifacts beside the configured pi-msg config:

- `<account>.auto.json`: retained history, generation, immutable attempt source,
  phase, job, seed and exact target association.
- `<account>.auto.json.disabled`: sticky private constant-text overflow diagnostic;
  never a message journal.
- `<account>.auto.json.request` and `.receipt`: short native authority and durable
  nonce outcome. A receipt's header UUID, not its filename suffix, is authoritative.
- `<account>.next.json` and protected inbox records: short native-transition input
  fence, reusing existing exact-target `/next` release/consumption rules.
- `<account>.reconcile/`: private snapshots/jobs/results and runner locks.

The TOTAL auto-state byte cap remains 2 MiB, including history and the immutable
attempt seed. ALL retained owner-DM replies can exceed this cap (or a model's
context budget); no silent reply-count eviction is substituted. An oversized save
preserves the last good index and durably writes the private disable marker.
Automatic cleanup stays disabled across restart even if a subsequent save fits.
Loading the marker logs a diagnostic and sets the automatic-cleanup error without
preventing foreground use or existing exact-target recovery. Unset/zero mode does
not read or write it. A marker-write filesystem failure is reported explicitly;
crash durability cannot be promised when the filesystem itself refuses the write.
Repair is explicit operator work: reconcile and validate retained history, pending
attempt/receipt and native target proofs before removing the marker. Blind marker
deletion would falsely treat the old incomplete index as complete. There is no
automatic clear or pretend repair; evicted legacy history cannot be reconstructed.

No terminal attempt reruns an unchanged source snapshot, nor the unchanged empty
replacement seed. New admitted activity can invalidate cleanup but cannot authorize
blind retries of uncertain worker effects. Recovery of a worker reports its result
only; it does not restore lost reset authority or publish XA.

A completed native switch must retain its proven exact target pointer even when
new activity wins; the attempt becomes blocked without XA, and held input releases
only into that target. Explicit before-switch cancellation/preparation outcomes can
recover the verified original source. Missing/timeout/unknown outcomes keep input
DURABLY fenced. `/new` cannot supersede an unknown automatic switch. Late receipts
can resolve the running bridge's exact-target fence; restart also checks receipt,
header and seed before releasing protected input. If a crash or post-unlink
directory-sync failure retired the fence before terminal attempt persistence,
recovery additionally requires an already-matching durable resume pointer, then
records the exact target blocked/no-XA. Any protected orphan from that automatic
boundary must already be bound to its exact target; unbound/mixed input is never
retargeted. Later manual `/next` boundaries retain their own exact-target recovery.
Without proof, startup fails
closed rather than resuming a generic fresh session. Inspect the attempt, receipt,
source/target files and runner artifacts before operator repair; do not delete
protected inbox/boundary state or rerun worker mutations to clear the diagnostic.

This is source capability, not deployment authorization. Enablement, actual MAM
self-readback and external live-XMPP smoke require separate operator rollout.
