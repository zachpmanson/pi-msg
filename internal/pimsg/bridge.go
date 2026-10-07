package pimsg

import (
	"context"
	_ "embed"
	"encoding/json"
	"fmt"
	"hash/fnv"
	"math/rand"
	"net/http"
	"os"
	"path/filepath"
	"regexp"
	"sort"
	"strconv"
	"strings"
	"sync"
	"time"

	"mellium.im/xmpp/jid"
)

// Bridge wires an XMPP connection to a `pi --mode rpc` child: owner chat
// becomes pi commands, and pi's events update XMPP presence or send explicit
// bridge notices.
type Bridge struct {
	acct  ResolvedAccount
	debug bool

	xmpp *XMPPBridge
	rpc  *RPCClient
	ctx  context.Context

	// sessionFile is the active pi session file, persisted per-account so a
	// restart resumes it (only /new resets context).
	sessionFile string

	// Start-directive / volunteer-turn state. On a restart the operator CLIs
	// write a one-shot directive ("proactive" → fire a volunteer turn on resume,
	// "idle" → stay silent, "prompt" → deliver an initial task prompt to a
	// fresh on-demand spawn); the bridge reads and consumes it at startup.
	resumed          bool   // a saved, usable session was resumed this launch
	startDir         string // directive consumed at startup: "proactive", "idle", "prompt", or ""
	volunteered      bool   // whether the proactive volunteer turn has been fired
	volunteerPending bool   // proactive volunteer turn deferred until replay completes
	// inbox is the durable inbound queue (#96): every message handed to pi is
	// appended before the prompt and acknowledged once the run that took it in
	// settles, so a stop mid-run cannot lose it.
	inbox *inbox

	replayWindowArmed bool      // a restart replay window was armed at startup
	mamArmed          bool      // XEP-0313 backfill is due on this launch (issue #84)
	mamSince          time.Time // archive lower bound for the MAM query; zero = seed the marker only

	// connectedOnce distinguishes the launch connect from a mid-session
	// reconnect: the restart replay/backfill runs on the first connect only,
	// while every later one triggers a MAM backfill over the gap (#94).
	connectedOnce bool
	// backfillMu serialises the restart drain and the reconnect backfill, and
	// guards connectedOnce: a fast reconnect must not race the startup drain.
	backfillMu sync.Mutex
	// lastIn is the instant the bridge last handed an inbound message to the
	// agent. Mirrored to <acct>.lastin on disk; it is the lower bound for the
	// reconnect backfill (#94).
	lastIn time.Time

	// initialPrompt is the invocation-time initial prompt (--prompt/--command
	// CLI flag, or a "prompt" start-directive payload): the task an on-demand
	// persona is spawned with. Non-empty means a fresh, stateless launch — the
	// saved session is NOT resumed, and the task becomes the very first prompt
	// (see fireInitialPrompt). Used by the sentinel doer flow (beltino#18).
	initialPrompt string

	mu sync.Mutex
	// sessionTransitionMu serializes an idle-triggered fresh-session swap with
	// inbound delivery, so a message at the away boundary lands in one session.
	sessionTransitionMu sync.Mutex
	streamingRun        bool
	busyMarked          bool // on-disk <acct>.busy reflects work in flight (see syncBusyMarker)
	repliedThisRun      bool
	shuttingDown        bool
	reactTo             string // full JID of the owner message the current run reacts to
	reactID             string // stanza id of that message (XEP-0444 target); "" disables
	turnDest            string // reply destination for the current turn (owner or room jid)
	messagingSeeded     bool   // the pi-msg messaging contract has been injected into this session (once)
	reactionAckRun      bool   // a run was woken by an inbound reaction ack (suppress automatic reactions and no-reply recovery)
	heartbeatRun        bool   // a run was woken by a long-running-process heartbeat (noop is the expected outcome)
	// runActive is when the current run last took a message into its context —
	// an injected steer or a fresh assistant turn begins one. The inbox compares
	// it with a delivery stamp at settle to tell a message the run consumed from
	// one it merely outlived (#104). Zero means no such event yet.
	runActive time.Time
	// finalMsgHadText records whether the most recent assistant message of this
	// run carried deliverable text. A run that ends on a tool call leaves it
	// false: the answer was never written, so nothing could be delivered.
	finalMsgHadText bool
	// toolSinceDelivery records that a tool ran after the last successful
	// delivery. With finalMsgHadText it separates a run that stopped mid-work
	// from one with a private final response and no send_message call.
	toolSinceDelivery  bool
	sendRecoveryNudges int // empty-tail recovery prompts sent this user turn (bounded)
	// runInbound counts the chat messages that entered the current run: the one
	// that started it plus every steer that landed while it was in flight.
	// runDeliveries counts the replies that actually reached a destination. Pi
	// injects a steer at the first yield point — typically the instant a tool
	// result returns — so the model can read a new question before it writes the
	// answer to the last one, and that answer is then never written at all.
	// Comparing the two counts at settle catches exactly that.
	runInbound    int
	runDeliveries int
	// runLog records the same traffic as the two counters, in arrival order,
	// so the unanswered-message hint can show the agent what the run actually
	// received and sent. The counters alone say "3 in, 2 out" and leave the
	// agent to guess which message went unanswered.
	runLog     []runLogEntry
	hintNudges int // unanswered-message hints sent this user turn (bounded)
	// hintPending marks the run the agent starts in answer to a hint. That run
	// must never be hinted about in turn: the agent has just been asked to catch
	// up, so whatever it sends IS the catch-up. Hinting again would ask it to
	// check its own correction, and could do so for as long as the budget lasts.
	hintPending       bool
	idleSince         time.Time // when the agent last became idle; zero while a run is in flight
	awayAnnounced     bool      // the away transition has been announced this idle period
	ranSinceStart     bool      // a run has started on this bridge (see freeForSummons)
	lastAwayStatus    string    // the last pithy activity shown while away (skip repeats across periods)
	bgProcesses       int       // background processes pi has running (relayed by the pi-processes extension)
	pendingHeartbeats []string  // long-running-process alarms queued while a run was in flight

	lifecycleReactTo string // snapshot of reactTo at run start, for lifecycle auto-reacts
	lifecycleReactID string // snapshot of reactID at run start

	// cascadeMu guards cascade, the count of consecutive agent-to-agent turns
	// taken with no owner message in between (#23), and cascadeNotified, which
	// keeps the room notice to one per episode.
	cascadeMu       sync.Mutex
	cascade         int
	cascadeNotified bool

	// handleWarnedRun bounds the unknown-@handle warning to one per run, so a
	// stubbornly-misspelling agent can't be nudged in a loop.
	handleWarnedRun bool

	// untaggedWarnedRun bounds the "addressed nobody" warning to one per run,
	// for the same reason: a nudge that can repeat is a nudge that can loop.
	untaggedWarnedRun bool
	// peerRun records that the run being answered was opened by ANOTHER agent's
	// addressed message, not by the owner. Only a peer handoff expects a tag, so
	// only a peer handoff is warned about an untagged room reply — a status
	// report written for the owner alone is not a mistake.
	peerRun bool

	// pendingMarkers are XEP-0333 "displayed" markers awaiting the moment pi
	// actually starts the matching user message (#73). A 1:1 owner stanza is
	// accepted (and possibly queued as a steer) before pi reads it, so the
	// marker is registered here with the exact prompt text handed to pi and
	// sent only when a user `message_start` carries that text. Guarded by mu.
	pendingMarkers []pendingMarker
	// markerSender lets tests observe the deferred marker without a live XMPP
	// session. Nil in production, where SendDisplayedMarker goes to the
	// transport.
	markerSender func(to, id string) error
}

// pendingMarker is one deferred XEP-0333 "displayed" marker: the exact prompt
// text handed to pi, and the message it acknowledges (the stanza id and the
// full from-JID it routes back to). The prompt text is the correlation key —
// pi's RPC `message_start` echoes the prompt it accepted, and there is no
// other identifier on the wire (#73).
type pendingMarker struct {
	prompt string
	to     string
	id     string
}

// maxPendingMarkers bounds the deferred-marker queue. A marker whose prompt
// never starts (a steer pi never yielded, or one cleared by /abort) stays
// pending by design, so the queue needs a ceiling rather than an unbounded
// leak. Oldest entries are dropped first.
const maxPendingMarkers = 64

// cascadeCap bounds consecutive commentary-triggered turns with no intervening
// owner (canonical) message, so two agents addressing each other cannot loop
// indefinitely with no human in the path. Beyond the cap a commentary trigger
// degrades to no-turn: the message is not handed to the agent at all (the
// ambient buffer was removed in #106), so the cap is now a hard stop rather
// than a demotion, and the room is told once (announceCascadeStop) instead of
// the handoff failing silently. Any canonical message resets the count.
//
// This is a runaway backstop, NOT a pacing mechanism. It was originally 3,
// which silently stalled real multi-agent work twice in one session: the budget
// went on claim negotiation, then the handoffs that would have started the
// actual task were dropped, and the fleet sat mute until the owner prodded it.
// Legitimate rounds ran 8-12 agent messages, so the cap must sit far above
// that. Reaching it now also announces itself in the room (announceCascadeStop)
// rather than failing silently, since neither the sender nor the recipient can
// otherwise distinguish a dropped handoff from a peer still thinking.
const cascadeCap = 25

// rpcEnv selects the companion extension's tool set. Message delivery and
// archive reads are available in every account mode; file and reaction tools
// remain opt-ins here, and lifecycle auto-reactions are gated in the bridge.
func rpcEnv(acct ResolvedAccount) []string {
	env := []string{"PI_MSG_TOOLS=" + strings.Join(toolNames(acct), ",")}
	if acct.BeforeAgentStartText != "" {
		env = append(env, "PI_MSG_BEFORE_AGENT_START_TEXT="+acct.BeforeAgentStartText)
	}
	return env
}

// toolNames is the companion-extension tool set for an account. Explicit
// message sending and history reads are always available; file and reaction
// tools remain present regardless of their lifecycle opt-ins.
func toolNames(acct ResolvedAccount) []string {
	return []string{"file", "reaction", "messaging", "messages"}
}

// NewBridge constructs a bridge for the resolved account.
func NewBridge(acct ResolvedAccount, debug bool) *Bridge {
	return &Bridge{acct: acct, debug: debug}
}

func (b *Bridge) log(level, msg string) {
	if level == "info" && !b.debug {
		return
	}
	fmt.Fprintf(os.Stderr, "[pi-msg] %s: %s\n", level, msg)
}

// Run starts pi and the XMPP connection and drives the event loop until the
// context is canceled or pi exits.
func (b *Bridge) Run(ctx context.Context) error {
	b.ctx = ctx

	b.xmpp = NewXMPPBridge(b.acct, b.onInbound, b.log)
	b.inbox = newInbox(inboxPath(b.acct.Name), b.log)

	// A fresh or resumed bridge is idle until something prompts it — start the
	// idle clock now so an unused agent drifts to "away" after the timeout.
	// The XMPP bridge stamps the same instant into its XEP-0319 idle element.
	now := time.Now()
	b.mu.Lock()
	b.idleSince = now
	b.mu.Unlock()
	b.xmpp.SetIdleSince(now)
	b.loadAwayActivities()
	go b.idleWatcher(ctx)

	// Materialise the companion extension so pi can register the XMPP tools.
	extPath, err := writeTempExtension()
	if err != nil {
		return err
	}
	defer os.Remove(extPath)

	b.rpc = NewRPCClient("", b.acct.Model, b.acct.Workdir, extPath, func(line string) {
		if b.debug {
			b.log("info", "pi stderr: "+line)
		}
	})
	// Companion-extension environment: tool set + prompt-level opt-ins (see
	// rpcEnv).
	b.rpc.env = rpcEnv(b.acct)

	// Session persistence: we always continue from the last session when one is
	// usable. If we saved a session file on a previous run and it still exists
	// (non-empty) on disk, resume it so a restart continues the conversation —
	// only /new resets context, and a "fresh" restart is never requested via the
	// CLI (the choice is proactive vs idle, not fresh). Missing/deleted files
	// fall back to a fresh session.
	// The presence label reflects the outcome: "resumed" for a continuation,
	// "awake" for a fresh start.
	kind, dirPayload := loadStartDirective(b.acct.Name)
	b.startDir = kind
	// A marker left by a previous process that died mid-run describes a state
	// that no longer exists: this launch is idle until its first run starts.
	clearBusyMarker(b.acct.Name)
	if b.initialPrompt == "" {
		b.initialPrompt = dirPayload // "prompt" directive payload; "" unless kind was StartPrompt
	}

	// Session persistence: we always continue from the last session when one is
	// usable — UNLESS an invocation-time initial prompt is set. A prompt means
	// an on-demand persona spawn (beltino#18): stateless by construction, so the
	// saved session is never resumed and the task is delivered as the very
	// first prompt. Routine restarts (no prompt) resume as before; a "fresh"
	// restart is never requested via the CLI (the choice is proactive vs idle).
	// Missing/deleted files fall back to a fresh session.
	// The presence label reflects the outcome: "resumed" for a continuation,
	// "awake" for a fresh start.
	if b.initialPrompt != "" {
		b.log("info", "on-demand spawn: initial prompt set, starting fresh session")
		b.xmpp.SetStartupStatus("awake")
	} else if prev := loadSessionState(b.acct.Name); prev != "" && sessionFileUsable(prev) {
		b.rpc.sessionPath = prev
		b.log("info", fmt.Sprintf("resuming session %s (start=%s)", prev, startLabel(b.startDir)))
		b.resumed = true
		// A resumed session's context already contains the messaging contract (it
		// was seeded when the session began) — unless the contract text has changed
		// since, which a pi-msg upgrade can do. Re-seed then: a session still
		// holding the old addressing rules would enforce rules the bridge no longer
		// applies (#106/#109, found in the field on 2026-09-28).
		if loadSeededContract(b.acct.Name) == b.contractHash() {
			b.messagingSeeded = true
		} else {
			b.log("info", "messaging contract changed since this session was seeded; re-seeding")
		}
		b.xmpp.SetStartupStatus("resumed")
	} else {
		if prev != "" {
			b.log("info", "saved session file missing or empty; starting fresh")
		}
		b.xmpp.SetStartupStatus("awake")
	}

	// Bring up XMPP first so we can report problems, then start pi.
	// Restart-gap inbound replay: recover messages that arrived while this
	// account was offline during a restart. Window start = graceful swapstart
	// marker when present (consumed), else last-outbound fallback (kept). The
	// XMPP layer buffers replay-window messages and hands them to the resumed
	// session after the grace period (see onXMPPConnected). Skipped for
	// on-demand spawns: a fresh doer starts with only its task, not a replay of
	// stale chat from a previous incarnation.
	if b.initialPrompt == "" {
		start, ok := replayWindowStart(b.acct.Name)
		// XEP-0313 backfill (issue #84): recover messages the server never pushed
		// (MUC backlog in particular) across exactly the downtime window. The
		// lower bound is the restart window start, but never EARLIER than a
		// completed backfill — using the earlier one re-delivers messages the
		// running bridge already handled live, and a replayed `!new` resets the
		// session. Duplicates within one drain are dropped by stanza id in
		// bufferReplay.
		if b.acct.MAM {
			b.mamArmed = true
			seen, seenOK := readMAMSeen(b.acct.Name)
			if since, ok2 := mamSinceFor(start, ok, seen, seenOK); ok2 {
				b.mamSince = since
			}
			if !ok {
				// First launch: arm the drain path (the replay buffer needs a
				// consumer) but don't walk the archive; seed the marker instead.
				start, ok = time.Now(), true
			}
		}
		if ok {
			if b.xmpp.SetReplayWindow(start) {
				b.replayWindowArmed = true
				b.log("info", "replay window armed from "+start.UTC().Format(time.RFC3339))
			}
		}
	}

	// No connect callback: the bot appearing online (presence "listening") is
	// the startup signal now, in place of a chat banner. The callback is used to
	// trigger the buffered-message replay once the connection is up.
	go b.xmpp.Run(ctx, b.onXMPPConnected)
	if err := b.rpc.Start(); err != nil {
		return err
	}
	b.log("info", fmt.Sprintf("bridging account %q (%s) to owner %s", b.acct.Name, b.acct.JID, b.acct.Owner))
	// Record which session pi is now on (fresh or resumed) so a future restart
	// can resume it. refreshSessionFile does a get_state Request round-trip, which
	// also confirms pi is live and reading stdin — a safe point to inject the
	// proactive volunteer turn.
	b.refreshSessionFile()

	// Fire the invocation-time initial prompt once, on an on-demand spawn: the
	// task IS the launch reason, so it must be the session's first prompt. Like
	// the proactive volunteer turn below, this must happen here, not on an RPC
	// session_start event: pi does NOT emit session_start over the RPC event
	// stream (it's an extension lifecycle hook, not an RPC event), so a hook in
	// handleRPCEvent would never run.
	if b.initialPrompt != "" {
		b.fireInitialPrompt()
	} else if b.resumed && b.startDir == StartProactive && !b.volunteered {
		if b.replayWindowArmed {
			b.volunteerPending = true
		} else {
			b.fireResumeTurn()
		}
	}

	for {
		select {
		case <-ctx.Done():
			b.shutdown("interrupted (SIGINT/SIGTERM)")
			return nil
		case ev, ok := <-b.rpc.Events():
			if !ok {
				return b.onPiExit()
			}
			b.handleRPCEvent(ev)
		}
	}
}

func (b *Bridge) onPiExit() error {
	if b.rpc.StoppedIntentionally() {
		return nil
	}
	// pi died on its own (crash): XMPP is still connected, so — unlike the
	// graceful lifecycle, which is presence-only — post a loud chat message so
	// the crash isn't missed, then drop presence carrying the same reason as the
	// offline status. The message goes first, while online.
	err := b.rpc.ExitErr()
	if err != nil {
		b.reply(fmt.Sprintf("🔴 pi crashed: %v. Bridge shutting down.", err))
		b.xmpp.GoOffline(fmt.Sprintf("offline — pi crashed: %v (%s)", err, nowStamp()))
		return fmt.Errorf("pi exited: %v", err)
	}
	b.reply("🔴 pi exited unexpectedly (no error reported). Bridge shutting down.")
	b.xmpp.GoOffline("offline — pi exited unexpectedly (" + nowStamp() + ")")
	return fmt.Errorf("pi exited unexpectedly")
}

// sessionFileUsable reports whether path is a plausible, non-empty pi session
// file that a /new launch can safely resume.
func sessionFileUsable(path string) bool {
	fi, err := os.Stat(path)
	return err == nil && !fi.IsDir() && fi.Size() > 0
}

// refreshSessionFile asks pi which session file is active and persists it to
// the account's state file so a restart can resume the same conversation. It
// is best-effort: errors are logged, never fatal.
func (b *Bridge) refreshSessionFile() {
	res, err := b.rpc.GetState(b.ctx)
	if err != nil {
		b.log("warning", "session persistence: get_state failed: "+err.Error())
		return
	}
	p := res.Obj("data").Str("sessionFile")
	if p == "" {
		b.log("warning", "session persistence: get_state returned no session file")
		return
	}
	b.sessionFile = p
	saveSessionState(b.log, b.acct.Name, p)
	b.log("info", "session: "+p)
}

// nowStamp is a short local timestamp for presence status lines.
func nowStamp() string { return time.Now().Format("2006-01-02 15:04:05 MST") }

func (b *Bridge) shutdown(reason string) {
	b.mu.Lock()
	if b.shuttingDown {
		b.mu.Unlock()
		return
	}
	b.shuttingDown = true
	b.mu.Unlock()
	b.log("info", "shutting down: "+reason)
	// Record the instant we go offline so the next launch's replay window can
	// recover messages that arrive during the swap.
	markSwapStart(b.log, b.acct.Name, time.Now())
	b.xmpp.GoOffline(fmt.Sprintf("offline — session ended (%s) at %s", reason, nowStamp()))
	// Save the session file pi is currently on so the next launch resumes it.
	b.refreshSessionFile()
	b.rpc.Stop()
}

// --- pi event handling ---

// The bridge conveys agent state through XMPP presence: <show> is availability
// (dnd while a run is in flight, available when idle), and <status> is the
// current activity label (thinking / running a tool / drafting / retrying).
func (b *Bridge) handleRPCEvent(ev Event) {
	switch ev.Type() {
	case "agent_start":
		b.setStreaming(true)
		b.markRan() // this bridge has work behind it now, so @free no longer reaches it (#130)
		b.setReplied(false)
		b.setHandleWarned(false)
		b.setUntaggedWarned(false)
		b.resetSendTracking() // fresh run: no message seen, no tool since delivery
		b.clearRunActivity()
		b.markActive() // a run is in flight — not idle
		b.xmpp.SetPresence("dnd", "thinking…")
		b.lifecycleReact("👀") // picked up (opt-in; ack-only runs stay silent)
	case "agent_settled":
		b.ackInboxSettled()
		b.setStreaming(false)
		b.markIdle() // now idle — arm the away clock and stamp the XEP-0319 idle element
		b.announceSettledPresence()
		b.lifecycleReact("✅") // done
		// Final assistant text is internal. If this run sent no XMPP message,
		// prompt once for an explicit send_message rather than implying the text
		// was delivered. Keep the no-reply reaction suppressed while that retry
		// is in flight.
		recovering := b.needsSendRecovery() && b.fireSendRecovery()
		// Several messages entered this run but fewer replies left it. Pi
		// injects a steer the moment a tool yields, so the model can read the
		// next question before answering the last — and then never answer it.
		// Ask it to check, with an explicit way to say it already did.
		// A run that answered a hint is never hinted about itself, however its
		// tally looks. takeHintPending consumes the mark, so the run after it is
		// judged normally again.
		if !recovering && !b.takeHintPending() {
			if n, m, ok := b.unansweredRun(); ok {
				recovering = b.fireUnansweredHint(n, m, b.runLogSnapshot())
			}
		}
		// The counts belong to the run that just ended, whatever we decided.
		b.resetRunCounts()
		// A run woken purely by a reaction ack or heartbeat may stay silent. A
		// recovery prompt is in flight when no message was sent, so hold the
		// no-reply reaction until the retry settles.
		if b.bannerNoReply(recovering) {
			b.reactNoReply()
		}
		// Keep the ack marker through all settle-time recovery/reaction decisions,
		// then consume it so it cannot suppress the next ordinary run.
		b.setReactionAckRun(false)
		b.volunteered = false // a resume volunteer turn is a one-shot; never repeats
		// A heartbeat wake is likewise one-shot: the flag lives only for the
		// run it opened, so a later user-initiated run is judged normally. It
		// is consumed here (like volunteered) rather than cleared at
		// agent_start — the settle check above must still see it.
		b.heartbeatRun = false
		// Deliver any long-running-process alarms queued while the run just
		// settled was in flight. This must come AFTER the banner decision and
		// the flag consumption above: a flushed heartbeat sets heartbeatRun
		// for the run it opens (the next one), and letting it bleed into this
		// settle's check would suppress a legitimate "done (no reply)" for a
		// user-initiated run that genuinely went unanswered.
		b.flushPendingHeartbeats()
	case "message_update":
		b.handleStreamDelta(ev)
	case "tool_execution_start":
		b.markToolSinceDelivery()
		b.xmpp.SetPresence("dnd", toolLabel(ev))
	case "auto_retry_start":
		b.xmpp.SetPresence("dnd", "retrying (transient error)…")
	case "auto_retry_end":
		b.xmpp.SetPresence("dnd", "thinking…")
	case "session_start":
		// Defensive only: pi does NOT emit a session_start event over the RPC
		// stream (it's an extension lifecycle hook, so this case never fires).
		// Session-swap pointer refreshes happen explicitly: at startup in Run(),
		// and after /new in the command handler. Kept here in case a future pi
		// starts emitting it.
		b.refreshSessionFile()
	case "message_start":
		// A message entered the conversation (the prompt itself, a steer pi
		// injected at its yield point, or the assistant's next turn). Anything
		// handed to pi before this moment has been read, which is exactly what
		// lets the settle acknowledge it without waiting out the grace (#104).
		if msg := ev.Obj("message"); msg != nil && msg.Str("role") == "user" {
			b.log("info", "pi: user message_start (prompt entered context)")
		}
		b.noteRunActivity()
		// The same moment is when a deferred XEP-0333 "displayed" marker becomes
		// true: pi has actually started the user message, not merely accepted or
		// queued it (#73).
		b.ackDisplayedMarker(ev)
	case "message_end":
		msg := ev.Obj("message")
		if msg == nil || msg.Str("role") != "assistant" {
			return
		}
		// A run that died on OpenRouter credits (HTTP 402) has no text to
		// deliver — without this hook the owner got the generic "done (no
		// reply)" nudge and no hint WHY. DM them the failure instead (see
		// creditFailAlert), and clear the in-run recovery bookkeeping so
		// settle can't fire a tail-retry / unanswered-hint prompt (which
		// would just 402 again).
		if msg.Str("stopReason") == "error" {
			if b.creditFailAlert(msg.Str("errorMessage")) {
				return // consumed: owner alerted, run marked replied
			}
		}
		// Record whether THIS message carried text before delivering it: at
		// settle, only the last message's answer matters, and a run whose final
		// message is tool-only never wrote its reply at all.
		text := FixToolCallXML(extractText(msg["content"]))
		b.setFinalMsgHadText(text != "")
		// Assistant text is intentionally not sent. The model must call
		// send_message for an outbound stanza; settle-time recovery handles a
		// requested reply that never used the tool.
	case "extension_error":
		// Name the thrower. pi attaches the offending extension's path and the
		// event it was handling, and the relay used to drop both — leaving the
		// owner with a recurring error whose author could not be identified from
		// the message or from the journal (#135).
		where := describeExtensionError(ev)
		b.log("warning", "extension error"+where+": "+orUnknown(ev.Str("error")))
		b.reply("⚠️ extension error" + where + ": " + orUnknown(ev.Str("error")))
	case "extension_ui_request":
		b.handleUIRequest(ev)
	}
}

// handleUIRequest routes companion-extension tool-action relays and otherwise
// cancels interactive dialogs (nobody is at the TUI to answer them) so pi
// doesn't block. A dialog whose title carries the sentinel is a relayed tool
// action, not a real user dialog — see handleToolRelay. The relay rides
// ui.select (issue #34) but accept any method with the sentinel for
// forward compatibility.
func (b *Bridge) handleUIRequest(ev Event) {
	id := ev.Str("id")
	method := ev.Str("method")
	if payload, ok := strings.CutPrefix(ev.Str("title"), relayPrefix); ok {
		b.handleToolRelay(id, payload)
		return
	}
	switch method {
	case "select", "confirm", "input", "editor":
		if id != "" {
			b.rpc.CancelUI(id)
			b.reply(fmt.Sprintf("⚠️ pi asked for input (%s) — auto-dismissed (no interactive UI over chat).", method))
		}
	case "notify":
		if b.debug {
			if m := ev.Str("message"); m != "" {
				b.reply("ℹ️ " + m)
			}
		}
	}
}

func (b *Bridge) handleSendMessageRelay(id, to, text, replyToID string) {
	if b.xmpp == nil {
		b.rpc.RespondUIRelay(id, "send_message is unavailable: the bridge has no XMPP connection")
		return
	}
	to = strings.TrimSpace(to)
	text = strings.TrimSpace(text)
	if to == "" || text == "" {
		b.rpc.RespondUIRelay(id, "send_message requires non-empty to and text")
		return
	}
	kind := b.xmpp.classifyMessageDest(to, b.acct.AllowArbitraryJid)
	if kind == destBlocked {
		b.rpc.RespondUIRelay(id, fmt.Sprintf("send_message: %q is not an allowed destination", to))
		return
	}
	var reply *replyTarget
	replyToID = strings.TrimSpace(replyToID)
	if replyToID != "" {
		entry, ok := b.xmpp.lookupReplyMessage(replyToID)
		if !ok {
			b.rpc.RespondUIRelay(id, fmt.Sprintf("send_message: stanza %q is not in message history; read it with read_messages or omit reply_to", replyToID))
			return
		}
		conversation := entry.ConversationJID
		if conversation == "" {
			conversation = entry.FromJID
		}
		if bareJid(conversation) != bareJid(to) {
			b.rpc.RespondUIRelay(id, fmt.Sprintf("send_message: stanza %q belongs to %q, not destination %q", replyToID, conversation, to))
			return
		}
		reply = &replyTarget{author: entry.FromJID, id: replyToID}
	}
	var stanzaID string
	if kind == destRoom {
		stanzaID = b.xmpp.SendRoomReply(bareJid(to), text, reply)
	} else {
		stanzaID = b.xmpp.SendChatReply(to, text, reply)
	}
	if stanzaID == "" {
		b.rpc.RespondUIRelay(id, fmt.Sprintf("send_message to %s failed: no stanza was sent", to))
		return
	}
	b.setReplied(true)
	b.clearToolSinceDelivery()
	b.recordDelivery(stanzaID, text)
	if kind == destRoom {
		to = bareJid(to)
		b.warnHandleProblems(to, text)
		b.warnUntaggedRoomReply(to, text)
	}
	b.setReactTarget(to, stanzaID)
	b.rpc.RespondUIRelay(id, "sent:"+stanzaID)
}

// read_messages limits. The default is what an agent usually wants — roughly the
// recent conversation — and the maximum bounds the token cost of one tool call,
// since the whole result lands in the model's context.
const (
	messagesReadDefaultLimit = 30
	messagesReadMaxLimit     = 100
)

// handleReadMessagesRelay answers the `read_messages` tool: it fetches XEP-0313
// archive history for an allowed room or direct-chat peer and returns it as text.
// Reads are explicit and on demand rather than pushed into the prompt.
//
// By default it reads the newest `limit` messages, which stays stateless and is
// what an agent usually wants. Two optional arguments add a window (#57):
// `since` bounds the archive below (RFC 3339 stamp or relative age), and
// `before` is a stanza id cursor that pages backwards past the newest-N window.
// Both are validated here so a bad argument fails loudly in the tool result
// instead of silently reading the newest page.
func (b *Bridge) handleReadMessagesRelay(id, target string, limit int, sinceArg, beforeArg string) {
	if b.xmpp == nil {
		b.rpc.RespondUIRelay(id, "read_messages is unavailable: the bridge has no XMPP connection")
		return
	}
	target = strings.TrimSpace(target)
	if target == "" {
		switch {
		case !b.acct.RoomMode():
			target = b.acct.Owner
		case len(b.acct.Rooms) == 1:
			target = b.acct.Rooms[0]
		default:
			b.rpc.RespondUIRelay(id, "read_messages needs a target: this account joins multiple rooms ("+strings.Join(b.acct.Rooms, ", ")+")")
			return
		}
	}
	bare := bareJid(target)
	if bare == "" {
		b.rpc.RespondUIRelay(id, "read_messages: target must be a valid JID")
		return
	}
	isRoom := b.xmpp.isRoomJID(bare)
	if b.acct.ErrorRoom != "" && bare == bareJid(b.acct.ErrorRoom) {
		b.rpc.RespondUIRelay(id, fmt.Sprintf("read_messages: %q is not a readable conversation", target))
		return
	}
	if !isRoom {
		if bare != bareJid(b.acct.Owner) && !b.acct.AllowArbitraryJid {
			b.rpc.RespondUIRelay(id, fmt.Sprintf("read_messages: %q is not an allowed target (allowed: owner %s and configured rooms)", target, b.acct.Owner))
			return
		}
		parsed, err := jid.Parse(bare)
		if err != nil || parsed.String() != bare {
			b.rpc.RespondUIRelay(id, fmt.Sprintf("read_messages: invalid peer JID %q", target))
			return
		}
	}
	if limit <= 0 {
		limit = messagesReadDefaultLimit
	}
	if limit > messagesReadMaxLimit {
		limit = messagesReadMaxLimit
	}
	since, err := messagesReadSince(sinceArg, time.Now())
	if err != nil {
		b.rpc.RespondUIRelay(id, "read_messages: "+err.Error())
		return
	}
	cursor, err := messagesReadCursor(beforeArg)
	if err != nil {
		b.rpc.RespondUIRelay(id, "read_messages: "+err.Error())
		return
	}
	b.log("notice", fmt.Sprintf("tool-relay read_messages: target=%q limit=%d since=%q before=%q", bare, limit, sinceArg, cursor))
	go func() {
		ctx, cancel := context.WithTimeout(context.Background(), mamTimeout)
		defer cancel()
		var msgs []InboundMessage
		var complete bool
		var err error
		if isRoom {
			msgs, complete, err = b.xmpp.FetchMAMRoomWindow(ctx, bare, since, cursor, limit)
		} else {
			msgs, complete, err = b.xmpp.FetchMAMDirectWindow(ctx, bare, since, cursor, limit)
		}
		if err != nil {
			reason := fmt.Sprintf("read_messages %s failed: %v", bare, err)
			b.log("warning", reason)
			b.rpc.RespondUIRelay(id, reason)
			return
		}
		b.xmpp.recordReadHistory(msgs, bare)
		b.rpc.RespondUIRelay(id, formatMessagesRead(bare, msgs, complete, messagesReadWindowLabel(limit, since, cursor)))
	}()
}

// messagesReadSince resolves read_messages's `since` argument to an archive lower
// bound. Both forms are accepted: an absolute RFC 3339 stamp, and a relative
// age (a Go duration such as "2h", meaning that long before now). An empty
// argument means "no lower bound" (the zero time); anything unparseable is an
// error the caller reports rather than silently dropping the bound — a read
// that quietly ignored `since` would return older messages than asked for.
func messagesReadSince(arg string, now time.Time) (time.Time, error) {
	arg = strings.TrimSpace(arg)
	if arg == "" {
		return time.Time{}, nil
	}
	if t, err := time.Parse(time.RFC3339, arg); err == nil {
		return t, nil
	}
	if d, err := time.ParseDuration(arg); err == nil {
		if d < 0 {
			return time.Time{}, fmt.Errorf("since %q is a negative age", arg)
		}
		return now.Add(-d), nil
	}
	return time.Time{}, fmt.Errorf("since %q is neither an RFC 3339 timestamp nor a relative age (e.g. 2h, 90m)", arg)
}

// messagesReadCursor validates read_messages's `before` argument: a stanza id from a
// previous read, or "" for the newest page. Stanza ids are opaque, so only the
// shape is checked — a non-empty token with no whitespace. Whether the id still
// exists in the archive is the server's answer, reported by the fetch.
func messagesReadCursor(arg string) (string, error) {
	cursor := strings.TrimSpace(arg)
	if cursor == "" {
		return "", nil
	}
	if len(cursor) > 256 || strings.ContainsAny(cursor, " \t\r\n") {
		return "", fmt.Errorf("before %q is not a usable stanza id", arg)
	}
	return cursor, nil
}

// messagesReadWindowLabel describes which slice of the archive a read covered, so
// the result is self-describing: the default newest-N page, a `since` bound, a
// `before` cursor, or both.
func messagesReadWindowLabel(limit int, since time.Time, cursor string) string {
	switch {
	case cursor != "" && !since.IsZero():
		return fmt.Sprintf("before stanza %s and since %s", cursor, since.UTC().Format(time.RFC3339))
	case cursor != "":
		return fmt.Sprintf("before stanza %s", cursor)
	case !since.IsZero():
		return fmt.Sprintf("since %s", since.UTC().Format(time.RFC3339))
	default:
		return fmt.Sprintf("newest %d", limit)
	}
}

// formatMessagesRead renders archived conversation messages for the model: oldest
// first, one line each, with sender, age, archive cursor, and stanza ID. Every
// return value starts with
// the `[pi-msg: read_messages:` header — including the empty case, which is a
// successful read of an empty window — because the companion extension treats
// any other result as a failed tool call (#106 review). `window` names the slice
// read (newest N, since …, before …), so a narrowed read is not mistaken for the
// whole recent conversation.
func formatMessagesRead(conversation string, msgs []InboundMessage, complete bool, window string) string {
	if len(msgs) == 0 {
		return fmt.Sprintf("[pi-msg: read_messages: no archived messages in %s (%s archive window).]", conversation, window)
	}
	var sb strings.Builder
	fmt.Fprintf(&sb, "[pi-msg: read_messages: %d archived message(s) in %s (%s), oldest first — read on demand, not a prompt; nothing here needs a reply unless you choose to send one.]", len(msgs), conversation, window)
	for _, m := range msgs {
		who := m.Nick
		if who == "" {
			who = bareJid(m.From)
		}
		if m.FromOwner {
			who = "owner"
		}
		// Our own archived lines are part of the room's history, so a read that
		// hid them would misrepresent the conversation. They are marked instead:
		// the reader sent them, and an unmarked line of its own looks like a peer.
		if m.Own {
			who = who + " [we sent]"
		}
		when := "time unknown"
		if !m.Stamp.IsZero() {
			when = shortAge(time.Since(m.Stamp))
		}
		reply := ""
		if m.ReplyToID != "" {
			reply = fmt.Sprintf(" [in reply to %s]", m.ReplyToID)
		}
		// Archive IDs address `before` pagination; stanza IDs address
		// send_message.reply_to and reactions. They are distinct identifiers.
		ids := ""
		if m.ArchiveID != "" {
			ids += fmt.Sprintf(" [id %s]", m.ArchiveID)
		}
		if m.ID != "" {
			ids += fmt.Sprintf(" [stanza %s]", m.ID)
		}
		fmt.Fprintf(&sb, "\n  %s (%s)%s%s: %s", who, when, ids, reply, strings.Join(strings.Fields(m.Body), " "))
	}
	// The page is the newest `limit` messages, so a short result means the archive
	// has nothing older to give, not that the page was cut short. `complete=false`
	// reports that the server has more history behind this page.
	if !complete {
		fmt.Fprintf(&sb, "\n  … older history exists in the archive beyond these %d message(s).", len(msgs))
	}
	return sb.String()
}

// handleToolRelay performs an XMPP-side action requested by an agent tool call
// in the companion extension, then answers the blocking relay with a string
// result — "ok", or a failure reason the extension surfaces to the model as
// the tool's error. The reason matters: an upload rejected by the server (e.g.
// "too large: 207387434 bytes") must reach the agent so it can rebuild or ask,
// not just a boolean (issue #34). The JSON payload names the action and its
// arguments. This is the structured alternative to the in-band `react:` /
// `file:` text conventions (issue #8 spike).
func (b *Bridge) handleToolRelay(id, payload string) {
	var cmd struct {
		Action       string             `json:"action"`
		Emoji        string             `json:"emoji"`
		Path         string             `json:"path"`
		To           string             `json:"to"`
		Text         string             `json:"text"`
		ReplyTo      string             `json:"replyTo"`
		Target       string             `json:"target"`
		MessageID    string             `json:"messageId"`
		From         string             `json:"from"`
		Limit        int                `json:"limit"`
		Since        string             `json:"since"`
		Before       string             `json:"before"`
		ProcessCount int                `json:"count"`
		Processes    []HeartbeatProcess `json:"processes"`
	}
	if err := json.Unmarshal([]byte(payload), &cmd); err != nil {
		b.log("warning", "bad tool-relay payload: "+err.Error())
		b.rpc.RespondUIRelay(id, "bad tool-relay payload: "+err.Error())
		return
	}
	switch cmd.Action {
	case "react":
		to, rid := cmd.From, cmd.MessageID
		if to == "" && rid != "" {
			// No explicit from-JID: look up the cached one.
			to = b.xmpp.lookupMessage(rid)
		}
		if rid == "" {
			// No explicit message ID: fall back to the current run's target.
			b.mu.Lock()
			to, rid = b.reactTo, b.reactID
			b.mu.Unlock()
		}
		b.log("info", fmt.Sprintf("tool-relay react: emoji=%q target to=%q id=%q", cmd.Emoji, to, rid))
		b.xmpp.SendReaction(to, rid, cmd.Emoji)
		// Success iff we had a target; reactions are instant.
		ok := to != "" && rid != ""
		if !ok {
			reason := "no reaction target (no messageId and no from-JID supplied)"
			if cmd.MessageID != "" {
				reason = fmt.Sprintf("reaction target %q not found in message history (no from-JID supplied; pass from explicitly)", cmd.MessageID)
			}
			b.log("warning", reason)
			b.rpc.RespondUIRelay(id, reason)
			return
		}
		b.rpc.RespondUIRelay(id, "ok")
	case "file":
		dest := cmd.To
		if dest == "" {
			// Default to where this turn's reply would go (room in room mode,
			// owner in 1:1); fall back to the owner if no turn context yet.
			if dest = b.currentTurnDest(); dest == "" {
				dest = b.acct.Owner
			}
		}
		b.log("info", fmt.Sprintf("tool-relay file: path=%q dest=%q", cmd.Path, dest))
		// Same allowlist as the in-band file: path — the agent can't ship files
		// to arbitrary JIDs.
		if b.xmpp.classifyDest(dest) == destBlocked {
			reason := fmt.Sprintf("send_file: %q is not an allowed destination", dest)
			b.reply("⚠️ " + reason)
			b.rpc.RespondUIRelay(id, reason)
			return
		}
		// The XEP-0363 upload is a network round-trip (up to ~2min); run it off
		// the RPC event loop and answer the blocked tool when it settles. On
		// success the relay returns the share URL so the agent can reuse it
		// elsewhere (e.g. paste the link into a PR), not just "ok".
		go func() {
			url, err := b.xmpp.SendFile(dest, cmd.Path)
			if err != nil {
				reason := fmt.Sprintf("send_file %q → %s failed: %v", cmd.Path, dest, err)
				b.reply("⚠️ " + reason)
				b.rpc.RespondUIRelay(id, reason)
				return
			}
			b.rpc.RespondUIRelay(id, url)
		}()
	case "send_message":
		b.handleSendMessageRelay(id, cmd.To, cmd.Text, cmd.ReplyTo)
	case "read_messages":
		b.handleReadMessagesRelay(id, cmd.Target, cmd.Limit, cmd.Since, cmd.Before)
	case "process_count":
		// Absolute count of background processes pi has running (relayed by the
		// pi-processes companion extension). While any run — or any background
		// process — is in flight, the bot shows dnd instead of available.
		b.setBgProcesses(cmd.ProcessCount)
		b.rpc.RespondUIRelay(id, "ok")
	case "process_heartbeat":
		// A long-running-process alarm from the companion extension (its
		// 10-minute heartbeat tick). Wake the agent with it — unless a run is
		// in flight, in which case queue the report and deliver it when the
		// agent settles, so the alarm never interrupts an active turn and is
		// never lost. The relay itself always answers ok: gathering the report
		// is the extension's job; delivery timing is the bridge's.
		text := b.formatHeartbeat(cmd.Processes)
		if b.streaming() {
			b.mu.Lock()
			b.pendingHeartbeats = append(b.pendingHeartbeats, text)
			b.mu.Unlock()
			b.log("info", fmt.Sprintf("process heartbeat queued (%d process(es), run in flight)", len(cmd.Processes)))
		} else {
			b.fireHeartbeat(text)
		}
		b.rpc.RespondUIRelay(id, "ok")
	default:
		b.log("warning", "unknown tool-relay action: "+cmd.Action)
		b.rpc.RespondUIRelay(id, "unknown tool-relay action: "+cmd.Action)
	}
}

// HeartbeatProcess is one long-running background process reported by the
// companion extension's heartbeat tick (see xmpp-tools.ts).
type HeartbeatProcess struct {
	ID          string `json:"id"`
	Name        string `json:"name"`
	Command     string `json:"command"`
	ElapsedSecs int    `json:"elapsedSecs"`
	Tail        string `json:"tail"`
}

// --- chat command handling ---

// onInbound routes a delivered message. Runs on the XMPP read goroutine;
// commands that need a response block only this handler, not pi's event
// stream.
func (b *Bridge) onInbound(m InboundMessage) {
	b.sessionTransitionMu.Lock()
	defer b.sessionTransitionMu.Unlock()
	// Everything that reaches here has been taken in by the agent, so the
	// persistent last-inbound cursor advances with it. Written after the
	// hand-off, not before: if the process dies while the run is in flight the
	// message is still outside the cursor and the next reconnect's backfill picks
	// it up (#94).
	defer b.noteInboundHandled()
	b.resetSendRecoveryNudges() // fresh user turn — allow one empty-tail recovery again
	b.resetHintNudges()         // fresh user turn — allow one unanswered-message hint again
	// Any inbound message is activity: come back to available and restart the
	// idle-away timer from now (a run still in flight keeps dnd — leave its
	// presence alone).
	//
	// This runs before the room-address filter below, so an unaddressed room
	// message still counts as activity — the bridge is in the room and heard it.
	// The clock is re-armed with markIdle rather than left in markActive's cleared
	// state, because many inbound messages never become a run (#106 dropped the
	// rest), so there is no agent_settled to re-arm it later: with only markActive,
	// idleSince stays zero forever and the watcher can never drift the agent back
	// to "away". markActive first resets awayAnnounced/lastAwayStatus so the next
	// idle period announces a fresh away, then markIdle restarts the timer; if the
	// message does start a run, that run's own agent_start/agent_settled lifecycle
	// takes over the clock as usual.
	b.markActive()
	b.markIdle()
	if !b.streaming() && b.xmpp != nil {
		b.announceSettledPresence()
	}
	// An inbound XEP-0444 reaction is an acknowledgment signal, not a
	// conversation turn: it wakes the agent only when idle (issue #27).
	if len(m.Reactions) > 0 {
		b.handleReaction(m)
		return
	}
	// Room chatter that does not address this agent is not part of its world at
	// all (#106): no turn, no durable record, no context. Classify BEFORE the
	// inbox append so such a message never enters the durable queue — the same
	// property the "drop an unactioned ambient entry" path used to buy after the
	// fact, now by construction.
	if !m.Direct {
		if action, _, _ := b.classify(m); action == actionNotOurs {
			b.log("info", fmt.Sprintf("room message in %s from %q does not address us; ignored", m.Room, m.Nick))
			return
		}
		m.Addressed = true
	}
	// Durably record the message BEFORE any prompt goes out (#96): a run that
	// dies before its next tool yield would otherwise take the instruction with
	// it, and a live-delivered message is not replayed by the server either.
	b.inboxAppend(m)
	if m.Direct {
		// Owner 1:1: origin is the owner; no separate sender. The reaction target
		// is this message (routed to its full from-JID).
		b.handleCanonical(m.Body, "", b.acct.Owner, "", m.From, m.ID, b.replyContext(m), nil, m.Markable)
		return
	}
	b.handleRoom(m)
}

// handleReaction records an inbound XEP-0444 reaction (an ack from a peer or
// the owner). Idle, it surfaces immediately so the reacted-to agent can read
// the ack without the owner sending anything; if a run is in flight it is
// dropped with a log line (issue #106 removed the ambient buffer that used to
// hold it), so an ack never interrupts a run and never queues behind it.
func (b *Bridge) handleReaction(m InboundMessage) {
	render := m.Nick
	if m.FromOwner {
		render = "owner"
	}
	if render == "" {
		render = bareJid(m.From)
	}
	joint := strings.Join(m.Reactions, " ")
	b.log("notice", fmt.Sprintf("inbound reaction from %s: %s (target %q)", render, joint, m.ReactionID))
	// A room reaction only addresses us when it is acking something WE said, or
	// when the owner acks a message we cannot attribute to a peer. A reaction to
	// another occupant's message is somebody else's conversation: with no ambient
	// buffer and no passive awareness (#106), waking on it would be the last
	// surviving room path where a non-addressing message costs a turn — and an
	// owner's reaction to a peer's message is that peer's business, exactly as an
	// owner reply to it is (ownerDirectedElsewhere).
	if !m.Direct {
		if b.xmpp != nil && b.xmpp.replyTargetOther(m.ReactionID) {
			b.log("info", fmt.Sprintf("reaction from %s dropped: it targets another agent's message (#106)", render))
			return
		}
		if !m.FromOwner && !b.xmppIsSelfMessage(m.ReactionID) {
			b.log("info", fmt.Sprintf("reaction from %s dropped: it targets a message that is not ours (#106)", render))
			return
		}
	}
	// A run already in flight must not be interrupted by a steering prompt, and
	// there is no longer a buffer to hold the ack for later. An ack carries no
	// obligation, so dropping it is the honest choice — say so in the log.
	if b.streaming() {
		b.log("info", fmt.Sprintf("reaction from %s dropped: a run is in flight and there is no ambient buffer (#106)", render))
		return
	}
	// Idle: wake the agent so the ack is readable now. It may acknowledge, act,
	// or stay silent; no owner message is required (issue #27).
	if m.Direct {
		b.setTurnDest(b.acct.Owner, false)
	} else {
		b.setTurnDest(m.Room, false) // a reaction ack is not a handoff
	}
	b.setReactionAckRun(true)
	// The ack quotes OUR OWN message being reacted to (#58, case D) — never the
	// reaction itself. msgHistory already records outbound bodies at send time,
	// so no new plumbing is needed; an unknown id (evicted from the ring, or a
	// test with no transport) renders an empty excerpt.
	excerpt := ""
	if b.xmpp != nil {
		if e, ok := b.xmpp.lookupMessageEntry(m.ReactionID); ok {
			excerpt = reactionExcerpt(e.Body)
		}
	}
	b.rpc.Prompt(
		fmt.Sprintf("[pi-msg: room: %s reacted %s to your message %q. You may acknowledge, act on it, or ignore; no reply is required unless you need to send a message with send_message.]", render, joint, excerpt),
		b.steerBehavior())
	if b.xmpp != nil {
		b.xmpp.SetPresence("dnd", "thinking…")
	}
}

// xmppIsSelfMessage asks the transport whether a stanza id is one we sent, so a
// room reaction can be matched to our own message. It answers false when there
// is no transport (tests) or the id is unknown — the bridge then treats the
// reaction as somebody else's conversation.
func (b *Bridge) xmppIsSelfMessage(id string) bool {
	return b.xmpp != nil && b.xmpp.isSelfMessage(id)
}

// reactionExcerpt shortens the quoted body of our own reacted-to message to its
// first ~80 characters on one line, so the ack block stays a pointer rather than
// replaying the message (#58, case D).
func reactionExcerpt(text string) string {
	s := strings.Join(strings.Fields(text), " ")
	const max = 80
	r := []rune(s)
	if len(r) <= max {
		return s
	}
	return string(r[:max]) + "…"
}

// roomNoticeKind selects which pointer block a room-triggered prompt carries
// (#58). The message body never enters the prompt: the agent pulls it with
// read_messages.
type roomNoticeKind int

const (
	noticeTag            roomNoticeKind = iota // A: someone tagged us
	noticeOwnerBroadcast                       // C: the owner spoke to the room, naming nobody
	noticeReplyToOwn                           // E: someone replied to one of our messages
	noticeFreeBroadcast                        // F: someone summoned the room's away agents (@free, #130)
)

// roomNotice is the pointer-block variant for a room-triggered prompt.
type roomNotice struct {
	kind     roomNoticeKind
	parentID string // E only: the stanza id of our own message being replied to
}

// roomNoticeFor selects the pointer block for a room-triggered prompt (#58): E
// when the message replies to one of ours, C when the owner spoke to the room
// without naming anyone, F when someone summoned the room's away agents with
// @free, A otherwise. It returns nil for a non-room turn, which keeps the
// header+body form (owner DMs, initial prompts).
//
// The owner's C block covers both an untagged message and an @free one (#130) —
// they are the same thing now — but not @everyone, which is an aimed broadcast
// and still reads as a tag. C is deliberately not selectable by a peer: only the
// owner can speak to the room without naming anyone.
func (b *Bridge) roomNoticeFor(m InboundMessage) *roomNotice {
	if m.Room == "" {
		return nil
	}
	if m.ReplyToID != "" && b.replyToOwnMessage(m) {
		return &roomNotice{kind: noticeReplyToOwn, parentID: m.ReplyToID}
	}
	explicit, _ := b.matchTriggerExplicit(m.Room, m.Body)
	if m.FromOwner && !explicit && !broadcastsToAll(m.Body) {
		return &roomNotice{kind: noticeOwnerBroadcast}
	}
	if !explicit && freeOnlyBroadcast(m.Body) {
		return &roomNotice{kind: noticeFreeBroadcast}
	}
	return &roomNotice{kind: noticeTag}
}

// roomAction is how a room message is treated.
type roomAction int

const (
	actionCanonical  roomAction = iota // owner: trusted, triggers a turn
	actionCommentary                   // non-owner addressed: untrusted, triggers a turn
	actionNotOurs                      // not addressed to us: no turn, nothing recorded
)

// classify decides whether a room message is part of this agent's world, and
// under what authority. There are two outcomes that matter: it addresses us (the
// owner, a handle, a broadcast, or a reply to one of our own messages) and so
// takes a turn, or it does not and is dropped entirely (#106). There is no third
// tier — the ambient buffer that used to hold unaddressed chatter is gone.
//
// @free is the one address form gated on our own state (#130). It summons the
// room's free agents — the away ones, plus a bridge nothing has been asked of
// yet (see freeForSummons) — so an agent that is working, or that has worked and
// not yet drifted away, is not addressed by it at all.
//
// The second return is the body to prompt with, and the third is the bare trigger
// word when a bare mention was the only reason we were addressed (else "") —
// the false-trigger counter.
func (b *Bridge) classify(m InboundMessage) (roomAction, string, string) {
	addressed, stripped := b.matchTrigger(m.Room, m.Body)
	explicit, _ := b.matchTriggerExplicit(m.Room, m.Body)
	mention := ""
	if addressed {
		mention = b.bareMention(m.Room, m.Body)
		if mention != "" {
			b.log("notice", fmt.Sprintf("bare-name mention %q from %q in %s addressed us", mention, m.Nick, m.Room))
		}
	}
	// The presence gate: an @free body reaches us only while we are free — away,
	// or a bridge nothing has been asked of yet. m.Addressed means the message
	// cleared this gate when it first arrived and is being re-delivered from the
	// durable queue (a restart empties the presence we would re-derive it from),
	// so the verdict travels with it rather than being recomputed.
	if !m.Addressed && !explicit && freeOnlyBroadcast(m.Body) && !b.freeForSummons() {
		b.log("info", fmt.Sprintf("free broadcast in %s from %q skipped: we are not free", m.Room, m.Nick))
		return actionNotOurs, m.Body, ""
	}
	switch {
	case m.FromOwner:
		// The owner is trusted traffic, so an owner room message needs no
		// mention to reach us — but only an owner message that names nobody is
		// a broadcast. A tag or a stanza reply picks out an account, and the
		// message belongs to that account alone (#106).
		if addressed {
			if explicit {
				return actionCanonical, stripped, ""
			}
			// @everyone / @free (and we are away): a broadcast, body intact.
			return actionCanonical, m.Body, ""
		}
		if b.replyToOwnMessage(m) {
			return actionCanonical, m.Body, ""
		}
		if who, why := b.ownerDirectedElsewhere(m); who != "" {
			b.log("notice", fmt.Sprintf("owner message in %s is directed at %s (%s), not us — not delivering", m.Room, who, why))
			return actionNotOurs, m.Body, ""
		}
		// The owner named nobody, so the message is the room continuing: it
		// reaches the free agents (#130) and anyone who spoke here recently
		// (#130 follow-up) — if you were part of the conversation, the next turn
		// of it is yours to hear. An owner who wants the whole room writes
		// @everyone.
		if !m.Addressed && !b.freeForSummons() && !b.participated(m.Room) {
			b.log("info", fmt.Sprintf("owner broadcast in %s skipped: we are not free and have not spoken here recently", m.Room))
			return actionNotOurs, m.Body, ""
		}
		return actionCanonical, m.Body, ""
	case addressed:
		return actionCommentary, stripped, mention
	case b.replyToOwnMessage(m):
		// Someone answered something we said, without naming us. Treat it as
		// addressing us (XEP-0461, #95) rather than dropping it: a reply to our
		// own message is the one form of non-named traffic that is unambiguously
		// meant for us.
		return actionCommentary, m.Body, ""
	case m.Addressed:
		// Address-hood already established when this message was received (see
		// InboundMessage.Addressed): it is being re-delivered from the durable
		// queue, and the evidence that made it ours may no longer be resolvable
		// (an emptied stanza history after a restart).
		return actionCommentary, m.Body, ""
	default:
		return actionNotOurs, m.Body, ""
	}
}

// replyToOwnMessage reports whether m is a XEP-0461 reply to a stanza this
// bridge sent. The stanza history records the id for both directions, so the
// outbound case is marked as ours at send time (recordSelfMessage) rather than
// being inferred from the recorded JID — a room send records the room, which is
// indistinguishable from someone else's message in the same room.
func (b *Bridge) replyToOwnMessage(m InboundMessage) bool {
	if m.ReplyToID == "" || b.xmpp == nil {
		return false
	}
	return b.xmpp.isSelfMessage(m.ReplyToID)
}

// ownerDirectedElsewhere reports which other account an owner message is meant
// for, and why, or ("", "") when it is an unaddressed broadcast. It is only
// consulted for owner messages that do not address us at all, and it is
// deliberately about the owner: a peer's unaddressed room message is dropped
// either way, so the extra work only exists where silence would otherwise mean
// every agent answering a message written for one of them.
//
// Two forms count as directed, in this order:
//
//   - a XEP-0461 reply to a stanza we have seen from a peer — the reply answers
//     that peer. A reply to the owner's own message, or to an id we do not know
//     (it may be ours, from before a restart), is not evidence of a handoff and
//     falls through to the untagged broadcast case, so an unresolvable reply is
//     delivered rather than lost.
//   - a name: an @handle, a leading "nick:" / "nick,", or a bare nick of
//     another occupant. Occupants come from presence, so with a roster that has
//     not populated yet there is no evidence and the message stays a broadcast.
func (b *Bridge) ownerDirectedElsewhere(m InboundMessage) (who, why string) {
	if b.xmpp == nil {
		return "", ""
	}
	if m.ReplyToID != "" && b.xmpp.replyTargetOther(m.ReplyToID) {
		return "another agent", fmt.Sprintf("reply to stanza %s", m.ReplyToID)
	}
	if who := b.addressesOtherOccupant(m.Room, m.Body); who != "" {
		return who, "named in the body"
	}
	return "", ""
}

// addressesOtherOccupant reports the nick of another occupant this room message
// names, or "" if it names nobody but us. It shares the addressing vocabulary
// the rest of the bridge uses (see matchTrigger), so an owner message is read
// the same way a peer's message is.
func (b *Bridge) addressesOtherOccupant(room, body string) string {
	if b.xmpp == nil || room == "" {
		return ""
	}
	occupants := b.xmpp.OccupantNicks(room)
	if len(occupants) == 0 {
		return ""
	}
	me := b.xmpp.ownNick(room)
	if me == "" {
		me = b.acct.Nick
	}
	trig := b.acct.TriggerFor(room)
	ours := func(name string) bool {
		return (me != "" && strings.EqualFold(name, me)) || (trig != "" && strings.EqualFold(name, trig))
	}
	scan := stripUnquoted(body)
	if scan == "" {
		return ""
	}
	for _, match := range handleRe.FindAllStringSubmatch(scan, -1) {
		name := match[1]
		if ours(name) {
			continue
		}
		for _, occ := range occupants {
			if strings.EqualFold(name, occ) {
				return occ
			}
		}
	}
	// A leading "nick:" / "nick," is the colon form matchTrigger accepts, and a
	// bare nick anywhere in an unquoted line is the bare-name form.
	trimmed := strings.TrimSpace(body)
	for _, occ := range occupants {
		if ours(occ) {
			continue
		}
		if len(trimmed) > len(occ) && strings.EqualFold(trimmed[:len(occ)], occ) {
			if c := trimmed[len(occ)]; c == ':' || c == ',' {
				return occ
			}
		}
		if containsMention(scan, occ) {
			return occ
		}
	}
	return ""
}

// handleRoom routes a room message per its classification: owner → canonical
// trigger; a non-owner addressing the bot → untrusted-commentary trigger;
// anything else → dropped, with no turn and no context.
func (b *Bridge) handleRoom(m InboundMessage) {
	// Defence in depth (#29): the transport echo filter in dispatchRoom should
	// already have dropped our own echo (case-insensitively). If one still
	// arrives here, the transport guard missed — drop it rather than let it
	// re-enter classify and prompt ourselves. Firing this guard logs loudly
	// because it means the transport-level filter is broken.
	if m.Nick != "" && b.xmpp != nil && strings.EqualFold(m.Nick, b.xmpp.ownNick(m.Room)) {
		b.log("warning", fmt.Sprintf("own-echo reached dispatch despite transport guard (nick %q in %s); dropping", m.Nick, m.Room))
		b.inboxDrop(m.ID, m.From, m.Body)
		return
	}
	action, body, _ := b.classify(m)
	// Cascade bound (#23): an owner message resets the budget; agent-to-agent
	// turns spend it. When exhausted the trigger is dropped rather than demoted
	// to a buffered turn, so the count is a hard stop.
	switch action {
	case actionCanonical:
		b.resetCascade()
	case actionCommentary:
		if ok, announce := b.spendCascade(); !ok {
			b.log("warning", fmt.Sprintf("cascade cap (%d) reached; dropping a message from %q instead of taking another turn", cascadeCap, m.Nick))
			if announce {
				b.announceCascadeStop(m.Room)
			}
			action = actionNotOurs
		}
	}
	switch action {
	case actionCanonical:
		// The stanza id always travels: it can be send_message's reply_to
		// value (#54), whether or not reactions are on.
		// Room reactions enabled → also use the room JID as the reaction target,
		// so auto-reacts and send_reaction hit the room message. Every reaction
		// path needs BOTH a target jid and an id, so an id with no jid reacts to
		// nothing.
		reactTo := ""
		if b.acct.ReactionsFor(m.Room) {
			reactTo = m.Room
		}
		b.handleCanonical(body, m.Nick, m.Room, m.RealJID, reactTo, m.ID, b.replyContext(m), b.roomNoticeFor(m), false)
	case actionCommentary:
		reactTo := ""
		if b.acct.ReactionsFor(m.Room) {
			reactTo = m.Room
		}
		b.dispatchCommentary(body, m.Nick, m.Room, m.RealJID, reactTo, m.ID, b.replyContext(m), b.roomNoticeFor(m))
	case actionNotOurs:
		// Nothing is handed to the agent, so nothing may stay in the durable
		// queue: a run will never settle for it (#104). The live path already
		// filtered before appending, so this branch only sees a message that
		// arrived as addressed and then hit the cascade cap above, or one
		// replayed from the inbox — both of which were appended.
		b.inboxDrop(m.ID, m.From, m.Body)
	}
}

// senderName picks the name that identifies a message's sender in the
// unanswered-message hint's history: the room nick when there is one, else the
// local part of the sender's jid, else of the jid the message arrived on.
func senderName(nick, sender, origin string) string {
	if nick != "" {
		return nick
	}
	if n := localpart(sender); n != "" {
		return n
	}
	if n := localpart(origin); n != "" {
		return n
	}
	return "user"
}

// handleCanonical handles a trusted (owner / 1:1) message: control commands
// dispatch directly; anything else becomes a canonical prompt. origin is the
// jid the message arrived on (owner or room); sender is the individual (room
// only), both surfaced to the agent for explicit reply routing. nick is the
// sender's occupant nick in a room, "" in a 1:1 — it only names the sender in
// the unanswered-message hint's history. markable is the inbound XEP-0333 flag
// for a 1:1 owner message; with it, the deferred "displayed" marker is
// registered against the prompt so it fires only when pi reads the message.
func (b *Bridge) handleCanonical(text, nick, origin, sender, reactTo, reactID, replyTo string, notice *roomNotice, markable bool) {
	t := strings.TrimSpace(text)
	if t == "" {
		b.inboxDrop(reactID, "", "")
		return
	}
	// A real message supersedes any pending/active reaction-only wake.
	b.setReactionAckRun(false)
	// Point the default reply/file destination at the message that arrived, BEFORE
	// any dispatch. A control command produces no prompt of its own, so setting
	// this only on the prompt path below would make a command inherit the
	// destination of the previous turn: /export or /dump typed in the owner's 1:1
	// would upload the session file to whatever room the agent spoke in last
	// (zpm/beltino#56).
	b.setTurnDest(origin, false) // the owner wrote it, so no tag is expected
	if t == "!" {
		// The interrupt is acknowledged on the command stanza itself, rather
		// than the previous prompt's lifecycle target.
		b.setLifecycleReactTarget(reactTo, reactID)
	}
	if (strings.HasPrefix(t, "/") || strings.HasPrefix(t, "!")) && b.handleCommand(t) {
		// Handled in-process: it never becomes a prompt, so nothing will ever
		// settle for it (#104).
		b.inboxDrop(reactID, "", "")
		return
	}
	// A real prompt: point lifecycle/agent reactions at the message that drove it.
	// The reply/file destination was already set above, before the command check.
	b.inboxMarkDelivered(reactID)
	b.setLifecycleReactTarget(reactTo, reactID)
	b.countInbound(senderName(nick, sender, origin), reactID, t)
	prompt := b.composePrompt(t, origin, sender, reactID, reactTo, replyTo, notice)
	// A markable 1:1 message is acknowledged only once pi starts it: register the
	// marker against the exact prompt text and let the matching user
	// `message_start` fire it (#73). A message with no usable stanza id, or a
	// room message, carries no marker — the previous policy, unchanged.
	b.promptMarked(prompt, markable, reactTo, reactID)
	b.busyPresence("thinking…")
}

// promptMarked hands a prompt to pi and, for a markable 1:1 owner message,
// registers a deferred XEP-0333 "displayed" marker against it. The marker is
// not sent now: it waits for the user `message_start` that proves pi has read
// the message, so a stanza merely accepted or queued as a steer is not marked
// read (#73).
func (b *Bridge) promptMarked(prompt string, markable bool, to, id string) {
	if markable {
		b.addPendingMarker(prompt, to, id)
	}
	b.rpc.Prompt(prompt, b.steerBehavior())
}

// addPendingMarker queues a deferred "displayed" marker keyed by the exact
// prompt text handed to pi. An empty target or stanza id cannot be acknowledged
// on the wire, so it is not queued at all (the same guard the old
// accept-time receipt had).
func (b *Bridge) addPendingMarker(prompt, to, id string) {
	if prompt == "" || to == "" || id == "" {
		return
	}
	b.mu.Lock()
	defer b.mu.Unlock()
	b.pendingMarkers = append(b.pendingMarkers, pendingMarker{prompt: prompt, to: to, id: id})
	if over := len(b.pendingMarkers) - maxPendingMarkers; over > 0 {
		b.pendingMarkers = b.pendingMarkers[over:]
		b.log("warning", fmt.Sprintf("pending chat markers over %d; dropped the oldest %d", maxPendingMarkers, over))
	}
}

// ackDisplayedMarker fires the deferred marker for a user message pi has just
// started. It matches on the message content — the RPC `message_start` echoes
// the prompt text and carries no other identifier — and only for a user-role
// message, so an assistant turn or an unrelated run can never acknowledge a
// pending stanza. A start whose text matches nothing is left alone.
func (b *Bridge) ackDisplayedMarker(ev Event) {
	msg := ev.Obj("message")
	if msg == nil || msg.Str("role") != "user" {
		return
	}
	prompt := extractText(msg["content"])
	if prompt == "" {
		return
	}
	b.mu.Lock()
	idx := -1
	for i := range b.pendingMarkers {
		if b.pendingMarkers[i].prompt == prompt {
			idx = i
			break
		}
	}
	var m pendingMarker
	if idx >= 0 {
		m = b.pendingMarkers[idx]
		b.pendingMarkers = append(b.pendingMarkers[:idx], b.pendingMarkers[idx+1:]...)
	}
	b.mu.Unlock()
	if idx < 0 {
		return
	}
	if err := b.sendDisplayedMarker(m.to, m.id); err != nil {
		b.log("warning", "chat marker failed: "+err.Error())
	}
}

// sendDisplayedMarker sends one XEP-0333 marker through the transport, or the
// test hook when one is installed.
func (b *Bridge) sendDisplayedMarker(to, id string) error {
	if b.markerSender != nil {
		return b.markerSender(to, id)
	}
	if b.xmpp == nil {
		return fmt.Errorf("no xmpp transport")
	}
	return b.xmpp.SendDisplayedMarker(to, id)
}

// dispatchCommentary sends a non-owner addressed message as a room pointer
// block (untrusted by authority, though the block no longer wraps the body — the
// agent pulls it with read_messages). Slash-commands from non-owners are treated as
// literal text, never control commands.
func (b *Bridge) dispatchCommentary(body, nick, origin, sender, reactTo, reactID, replyTo string, notice *roomNotice) {
	t := strings.TrimSpace(body)
	if t == "" {
		b.inboxDrop(reactID, "", "")
		return
	}
	// A real message supersedes any pending/active reaction-only wake.
	b.setReactionAckRun(false)
	b.inboxMarkDelivered(reactID)
	b.setLifecycleReactTarget(reactTo, reactID)
	b.setTurnDest(origin, true) // a peer's handoff: an untagged reply here is the mistake
	b.countInbound(senderName(nick, sender, origin), reactID, t)
	b.rpc.Prompt(b.composePrompt(t, origin, sender, reactID, reactTo, replyTo, notice), b.steerBehavior())
	b.busyPresence("thinking…")
}

// handleCommand runs a recognized control command and returns true. Unknown
// "/…" input (extension commands, /skill:name, /template) returns false so the
// caller forwards it to pi as a prompt.
func (b *Bridge) handleCommand(t string) bool {
	name, arg := splitCommand(t)
	// A lone "!" is the quick interrupt — deliberately NOT /abort. With no
	// command name after the prefix it would otherwise fall through to pi as a
	// degenerate literal prompt (an empty control command); instead it only
	// stops whatever is currently running (a command or thinking) and leaves
	// pi's queued messages intact, so the next one evaluates the moment the
	// aborted run stops. "!abort", "!new" etc. still work through
	// splitCommand's prefix alias, and keep their /-equivalent behaviour.
	if t == "!" {
		name = "interrupt"
	}
	switch name {
	case "new":
		if b.streaming() {
			b.rpc.Abort()
		}
		b.settleLocally()
		res, err := b.rpc.NewSession(b.ctx)
		b.reportResult(err, res, "🆕 new session ready", "/new")
		if err == nil {
			// /new also reports OpenRouter credit when a creditWatch floor is
			// configured (see reportCreditIfWatched). It's diagnostic only —
			// fetch it off the event loop so a slow credits endpoint can't
			// stall /new's cleanup (session file refresh, routing re-seed) or
			// block queued inbound messages behind the handler.
			go b.reportCreditIfWatched()
			// /new swaps to a brand-new session, but pi does NOT emit a
			// session_start event over the RPC stream (it's a lifecycle hook the
			// extension sees via pi.on(), not an event the bridge receives — the
			// session_start case in handleRPCEvent is effectively dead code). So
			// the saved resume pointer would otherwise go stale, and the next
			// restart would resume an OLD conversation. Persist the new session
			// file now so a restart continues this conversation instead.
			b.refreshSessionFile()
			// A fresh session has no messaging contract in context yet — re-seed
			// it on the next prompt (once).
			b.messagingSeeded = false
			// /new leaves the agent with a blank session and nothing asked of it,
			// which is the state a fresh bridge is in: make it free to @free again
			// rather than leaving it in the settled window for idleAwayTimeout
			// (#130). A run that starts off the back of this sets the flag again.
			b.markFresh()
		}
	case "compact":
		res, err := b.rpc.Compact(b.ctx, arg)
		b.reportResult(err, res, "🗜️ context compacted", "/compact")
	case "think":
		res, err := b.rpc.SetThinkingLevel(b.ctx, arg)
		b.reportResult(err, res, "🧠 thinking level: "+arg, "/think")
	case "model":
		b.handleModel(arg)
	case "models":
		b.handleModels()
	case "name":
		b.handleName(arg)
	case "session":
		b.handleSession()
	case "abort", "stop":
		// Drain the queue BEFORE aborting. `abort` alone leaves queued steers
		// and follow-ups in the session, so pi starts a fresh run the moment
		// the aborted one stops — the opposite of what "⛔ aborted" promises.
		dropped := b.clearQueue()
		b.rpc.Abort()
		b.settleLocally()
		b.lifecycleReact("⛔") // aborted
		msg := "⛔ aborted"
		if dropped == 1 {
			msg += " (1 queued message dropped)"
		} else if dropped > 1 {
			msg += fmt.Sprintf(" (%d queued messages dropped)", dropped)
		}
		b.reply(msg)
	case "interrupt":
		// Bare "!": stop the current command/thinking WITHOUT flushing the
		// queue. Only the abort RPC is sent — clear_queue is what drops
		// queued steers/follow-ups, and only /abort asks for that, so pi
		// evaluates the next queued message as soon as the abort lands.
		b.rpc.Abort()
		b.settleLocally()
		b.reactInterrupted()
	case "quit", "exit":
		b.shutdown("requested over chat")
	case "dump":
		b.dumpSession(arg)
	case "dump-all", "dumpall":
		b.dumpAllSessions(arg)
	case "export":
		b.handleExport(arg)
	default:
		return false
	}
	return true
}

// handleExport renders the current session to HTML (deterministically, via pi's
// export_html RPC — no agent turn) and delivers the file directly to chat via
// XEP-0363 HTTP Upload (the same file-send path used by /dump), so the rendered
// session lands as an inline, downloadable file. This is deterministic /export.
// /share is deliberately NOT intercepted here — it is context-dependent, so it
// falls through to the agent, who picks the artifact to share based on
// conversation context (the /share → always-naboo policy lives in the fleet's
// beltino `share` skill, not in pi-msg).
func (b *Bridge) handleExport(_ string) {
	slug := fmt.Sprintf("%s-session-%s", b.acct.Name, time.Now().Format("20060102-150405"))
	tmpHTML := filepath.Join(os.TempDir(), slug+".html")
	b.reply("📄 exporting session…")
	res, err := b.rpc.ExportHTML(b.ctx, tmpHTML)
	if err != nil {
		b.reply("⚠️ /export failed: " + err.Error())
		return
	}
	if !res.success() {
		b.reply("⚠️ /export failed: " + res.errText())
		return
	}
	b.sendRenderedFile(slug+".html", tmpHTML)
}

// sendRenderedFile uploads a locally-rendered file to the owner via XEP-0363
// HTTP Upload, the same network round-trip path used by /dump. On failure it
// falls back to sending the file content inline.
func (b *Bridge) sendRenderedFile(name, path string) {
	content, err := os.ReadFile(path)
	if err != nil {
		b.reply("⚠️ /export: rendered but could not read file: " + err.Error())
		return
	}
	b.sendDumpFile(name, content)
}

// dumpSession sends the current session's transcript to the owner, straight
// from disk — no LLM turn. It reads the session file path from pi's get_state,
// then relays the file: verbatim JSONL by default, or a tab-separated table
// (one record per row) when arg is "table" (or "pretty", its former name).
// The upload always goes to the owner (never a room) — see sendDumpFile.
func (b *Bridge) dumpSession(arg string) {
	res, err := b.rpc.GetState(b.ctx)
	if err != nil {
		b.reply("⚠️ /dump failed: " + err.Error())
		return
	}
	if !res.success() {
		b.reply("⚠️ /dump failed: " + res.errText())
		return
	}
	path := res.Obj("data").Str("sessionFile")
	if path == "" {
		b.reply("⚠️ /dump: no session file (session persistence is disabled)")
		return
	}
	raw, err := os.ReadFile(path)
	if err != nil {
		b.reply("⚠️ /dump: cannot read session file: " + err.Error())
		return
	}
	if len(raw) == 0 {
		b.reply("📄 session is empty")
		return
	}
	// Dumps are transferred as an uploaded file (XEP-0363) rather than inline:
	// huge inline code blocks trip the markdown renderer on chat clients
	// (RenderLoopBoundary crash rendering /dump output). Falls back to inline
	// if the upload path fails for any reason.
	table := strings.EqualFold(strings.TrimSpace(arg), "table") || strings.EqualFold(strings.TrimSpace(arg), "pretty")
	content := raw
	name := "session-" + b.acct.Name + "-raw.jsonl"
	if table {
		content = []byte(prettyDump(raw))
		name = "session-" + b.acct.Name + "-table.tsv"
	}
	if table {
		b.reply(fmt.Sprintf("📄 session dump (table) — %s — uploading…", path))
	} else {
		b.reply(fmt.Sprintf("📄 raw session dump — %s (%d bytes) — uploading…", path, len(raw)))
	}
	b.sendDumpFile(name, content)
}

// uploadDumpFile performs the XEP-0363 upload for a session dump or export. It
// is a package-level seam so a test can observe the destination the upload
// resolves to (see TestDumpUploadForcesOwnerOverRoom); production always uses
// the XMPP bridge's SendFile.
var uploadDumpFile = func(x *XMPPBridge, to, path string) (string, error) {
	return x.SendFile(to, path)
}

// sendDumpFile writes content to a temp file and uploads it to the OWNER via
// XEP-0363, so the dump lands as a downloadable file rather than inline code.
// The destination is deliberately NOT the current turn's: a session dump or
// export can contain 1:1 content, so it is never delivered to a MUC, even when
// the command was typed in a room (owner decision, zpm/beltino#56). The upload
// is a network round-trip, so it runs off the event loop; if it fails, the
// content is sent inline instead (wrapped in a fence and split into
// self-contained code blocks so it stays render-safe).
func (b *Bridge) sendDumpFile(name string, content []byte) {
	p := filepath.Join(os.TempDir(), fmt.Sprintf("pi-msg-%s-%d-%s", b.acct.Name, time.Now().UnixNano(), name))
	if err := os.WriteFile(p, content, 0o600); err != nil {
		b.reply("⚠️ cannot write temp file: " + err.Error())
		return
	}
	dest := b.acct.Owner
	go func() {
		if _, err := uploadDumpFile(b.xmpp, dest, p); err != nil {
			b.reply(fmt.Sprintf("⚠️ file upload failed (%v); sending inline", err))
			inline := string(content)
			if len(inline) <= maxBody {
				b.reply(inline)
			} else {
				// Wrap in a fence and split into render-safe self-contained blocks.
				for _, chunk := range splitPrettyDump("```\n" + inline + "\n```") {
					b.reply(chunk)
				}
			}
		}
		_ = os.Remove(p)
	}()
}

// dumpAllSessions sends the full accumulated history for this account: every
// session file in the same session directory as the active one, concatenated in
// chronological order (the filename embeds each session's start timestamp).
// Raw JSONL by default, or a TSV table when arg is "table"/"pretty". Unlike
// /dump (which reads just the live get_state session file), this spans all
// past sessions so you can see the complete transcript, not just the current
// working file.
func (b *Bridge) dumpAllSessions(arg string) {
	res, err := b.rpc.GetState(b.ctx)
	if err != nil {
		b.reply("⚠️ /dump-all failed: " + err.Error())
		return
	}
	if !res.success() {
		b.reply("⚠️ /dump-all failed: " + res.errText())
		return
	}
	path := res.Obj("data").Str("sessionFile")
	if path == "" {
		b.reply("⚠️ /dump-all: no session (session persistence disabled)")
		return
	}
	dir := filepath.Dir(path)
	matches, err := filepath.Glob(filepath.Join(dir, "*.jsonl"))
	if err != nil {
		b.reply("⚠️ /dump-all: cannot list sessions: " + err.Error())
		return
	}
	if len(matches) == 0 {
		b.reply("⚠️ /dump-all: no session files found in " + dir)
		return
	}
	sort.Strings(matches) // filename embeds ISO start timestamp → chronological
	var sb strings.Builder
	records := 0
	for _, f := range matches {
		raw, err := os.ReadFile(f)
		if err != nil {
			b.log("warning", "dump-all: skipping "+f+": "+err.Error())
			continue
		}
		for _, line := range strings.Split(string(raw), "\n") {
			if strings.TrimSpace(line) != "" {
				sb.WriteString(line)
				sb.WriteByte('\n')
				records++
			}
		}
	}
	if records == 0 {
		b.reply("⚠️ /dump-all: no session records found")
		return
	}
	table := strings.EqualFold(strings.TrimSpace(arg), "table") || strings.EqualFold(strings.TrimSpace(arg), "pretty")
	content := []byte(sb.String())
	name := "session-" + b.acct.Name + "-all-raw.jsonl"
	if table {
		content = []byte(prettyDump(content))
		name = "session-" + b.acct.Name + "-all-table.tsv"
	}
	b.reply(fmt.Sprintf("📄 full session dump (%d files, %d records) — uploading…", len(matches), records))
	b.sendDumpFile(name, content)
}

// prettyDump reformats a session's JSONL into a real TSV — one record per row
// with its index, time, kind (message role, or record type), and a one-line
// detail preview. Tab-separated with a header row, so the delivered file opens
// directly in a spreadsheet. Detail whitespace/newlines are collapsed so each
// record stays a single field.
func prettyDump(raw []byte) string {
	var sb strings.Builder
	sb.WriteString("#\tTIME\tKIND\tDETAIL\n")
	i := 0
	for _, line := range strings.Split(string(raw), "\n") {
		line = strings.TrimSpace(line)
		if line == "" {
			continue
		}
		var obj map[string]any
		if json.Unmarshal([]byte(line), &obj) != nil {
			continue
		}
		tm, kind, detail := recordRow(Event(obj))
		// Collapse whitespace/newlines so each record stays one row/field, but
		// keep the full detail (no truncation).
		detail = strings.Join(strings.Fields(detail), " ")
		sb.WriteString(strconv.Itoa(i))
		sb.WriteString("\t")
		sb.WriteString(tm)
		sb.WriteString("\t")
		sb.WriteString(kind)
		sb.WriteString("\t")
		sb.WriteString(detail)
		sb.WriteString("\n")
		i++
	}
	return sb.String()
}

// splitPrettyDump splits a code-fenced pretty table into multiple
// self-contained code blocks, each small enough to fit in one message.
func splitPrettyDump(dump string) []string {
	// Strip the outer ``` fences
	body := strings.TrimPrefix(dump, "```\n")
	body = strings.TrimSuffix(body, "\n```")
	lines := strings.Split(body, "\n")
	if len(lines) < 2 {
		return []string{dump}
	}
	header := lines[0] // "  #  TIME  KIND  DETAIL"
	rows := lines[1:]

	// Reserve ~100 bytes per chunk for fence + header overhead
	const overhead = 100
	var chunks []string
	start := 0
	for i := 0; i <= len(rows); i++ {
		size := 0
		for j := start; j < i && j < len(rows); j++ {
			size += len(rows[j]) + 1
		}
		if size+overhead > maxBody && i > start {
			// Emit chunk [start, i)
			var sb strings.Builder
			sb.WriteString("```\n")
			sb.WriteString(header)
			sb.WriteByte('\n')
			for _, r := range rows[start:i] {
				sb.WriteString(r)
				sb.WriteByte('\n')
			}
			sb.WriteString("```")
			chunks = append(chunks, sb.String())
			start = i
		}
		_ = size
	}
	// Remaining rows
	if start < len(rows) {
		var sb strings.Builder
		sb.WriteString("```\n")
		sb.WriteString(header)
		sb.WriteByte('\n')
		for _, r := range rows[start:] {
			sb.WriteString(r)
			sb.WriteByte('\n')
		}
		sb.WriteString("```")
		chunks = append(chunks, sb.String())
	}
	return chunks
}

// recordRow summarizes one session JSONL record into (time, kind, detail) for
// the pretty table. Kind is the message role for message records, else the
// record type; detail is a one-line preview appropriate to the record.
func recordRow(e Event) (tm, kind, detail string) {
	if ts := e.Str("timestamp"); len(ts) >= 19 {
		tm = ts[11:19] // HH:MM:SS from the ISO timestamp
	}
	switch typ := e.Str("type"); typ {
	case "message":
		msg := e.Obj("message")
		role := msg.Str("role")
		if role == "toolResult" {
			return tm, "toolResult", "↳ " + msg.Str("toolName") + ": " + contentText(msg["content"])
		}
		return tm, role, contentText(msg["content"])
	case "model_change":
		return tm, "model", e.Str("provider") + "/" + e.Str("modelId")
	case "thinking_level_change":
		return tm, "thinking", e.Str("thinkingLevel")
	case "compaction":
		return tm, "compaction", "compacted: " + e.Str("summary")
	case "session", "session_info":
		if n := e.Str("name"); n != "" {
			return tm, typ, n
		}
		return tm, typ, e.Str("cwd")
	default:
		return tm, typ, ""
	}
}

// contentText renders a message's content (string or block array) to a compact
// one-line preview: text verbatim, tool calls as "⚙ <name>", thinking as 💭.
func contentText(content any) string {
	switch c := content.(type) {
	case string:
		return c
	case []any:
		var parts []string
		for _, it := range c {
			m, ok := it.(map[string]any)
			if !ok {
				continue
			}
			e := Event(m)
			switch e.Str("type") {
			case "text":
				parts = append(parts, e.Str("text"))
			case "thinking":
				parts = append(parts, "💭")
			case "toolCall":
				detail := "⚙ " + e.Str("toolName")
				if args := e.Obj("args"); args != nil {
					detail += " " + compactArgs(args)
				}
				parts = append(parts, detail)
			default:
				parts = append(parts, "["+e.Str("type")+"]")
			}
		}
		return strings.Join(parts, " ")
	default:
		return ""
	}
}

// compactArgs renders a tool-call arg map as a compact one-liner: key1=val1 key2=val2
// Values are collapsed: strings in full, numbers as-is, booleans as true/false,
// nested objects/arrays as [...] placeholder.
func compactArgs(args Event) string {
	var pairs []string
	for k, v := range args {
		switch val := v.(type) {
		case string:
			val = strings.Join(strings.Fields(val), " ")
			if len(val) > 40 {
				val = val[:37] + "…"
			}
			pairs = append(pairs, k+"="+val)
		case float64:
			pairs = append(pairs, k+"="+strconv.FormatFloat(val, 'f', -1, 64))
		case bool:
			pairs = append(pairs, k+"="+strconv.FormatBool(val))
		default:
			pairs = append(pairs, k+"=[…]")
		}
	}
	sort.Strings(pairs)
	return strings.Join(pairs, " ")
}

// messagingContract is the canonical on-start contract for outbound messages.
// Final assistant text is private to the harness; send_message is the explicit
// agent-authored path for text messages.
func (b *Bridge) messagingContract() string {
	allow := "By default, send_message allows the owner, configured rooms, and known occupants; read_messages allows the owner and configured rooms."
	if b.acct.AllowArbitraryJid {
		allow = "allowArbitraryJid permits other syntactically valid peer JIDs for both tools; unconfigured rooms remain unavailable."
	}
	return fmt.Sprintf("[pi-msg: messaging: Final assistant responses are INTERNAL to the harness and are never sent to chat. Send every chat message using send_message(to, text, reply_to?). Choose `to` from the incoming `from:` conversation JID, use `sender:` to DM a room participant, or use owner JID %s to message the owner. %s For a threaded reply, pass the complete `stanza-id:` as `reply_to`; it must belong to the chosen conversation. Send one tool call per message/recipient. If several inbound messages need replies, use one send_message call for each, with the appropriate reply_to. `read_messages` reads room or 1:1 history; its `[id …]` is the archive pagination cursor and `[stanza …]` is the reply_to message ID. In rooms, messages that do not address an agent are not delivered to it, so mention the intended agent by name or `@name`; `@everyone` and `@free` follow the room rules. Full spec: docs/routing.md]", b.acct.Owner, allow)
}

// composePrompt assembles the text sent to pi. A room-triggered prompt is a
// pointer block (#58): the case, the addressing meta and a read_messages call, with
// the body left out. A 1:1 DM (or the invocation-time initial prompt) keeps the
// "from:"/"sender:" header with the body below it. origin is the channel
// jid (owner or room); sender is the individual's real jid (room only, when
// known).
//
// No per-message messaging hint is appended here: the tool contract is seeded
// once per session, and no-send recovery is handled at agent_settled.
// replyContext renders the `in-reply-to:` header value for an inbound message
// carrying a XEP-0461 reply stamp, or "" when it carries none (#95).
//
// The stamped id is resolved against the stanza history, which records both
// directions, so the agent sees who wrote the message being answered, how long
// ago, and a short quote of it. That is the whole point: a bare "?" replying to
// something is unanswerable without it, and it was the reason an agent answered
// the wrong question on 2026-09-22.
//
// An id that cannot be resolved is reported as unresolvable rather than dropped.
// That is the case worth naming: the replied-to message was never delivered
// (issue #94) or predates the session. Clients differ on the `to` attribute —
// some stamp the conversation partner rather than the author — so the id is
// authoritative and `to` is only ever reported as a hint.
func (b *Bridge) replyContext(m InboundMessage) string {
	id, stamped := m.ReplyToID, m.ReplyToJID
	if id == "" && stamped == "" {
		return ""
	}
	if id == "" {
		return fmt.Sprintf("an unidentified message (no id was stamped; the reply names %s)", stamped)
	}
	if b.xmpp != nil {
		if e, ok := b.xmpp.lookupMessageEntry(id); ok {
			who := e.FromJID
			if who == "" {
				who = "unknown sender"
			}
			when := "time unknown"
			if !e.Timestamp.IsZero() {
				when = shortAge(time.Since(e.Timestamp))
			}
			if e.Body != "" {
				return fmt.Sprintf("%s (from %s, %s): %q", id, who, when, e.Body)
			}
			return fmt.Sprintf("%s (from %s, %s)", id, who, when)
		}
	}
	hint := ""
	if stamped != "" {
		hint = fmt.Sprintf(" (the reply was stamped to %s)", stamped)
	}
	return fmt.Sprintf("%s — NOT in this session's history: either it was never delivered to this bridge, or it predates the session%s", id, hint)
}

// shortAge renders a coarse "how long ago" for a prompt header.
func shortAge(d time.Duration) string {
	switch {
	case d < 0:
		return "just now"
	case d < time.Minute:
		return fmt.Sprintf("%ds ago", int(d.Seconds()))
	case d < time.Hour:
		return fmt.Sprintf("%dm ago", int(d.Minutes()))
	case d < 24*time.Hour:
		return fmt.Sprintf("%dh ago", int(d.Hours()))
	default:
		return fmt.Sprintf("%dd ago", int(d.Hours()/24))
	}
}

// roomsContract names the rooms this bridge has joined and states the delivery
// rule in force in them (#106): a room message either addresses this agent — and
// arrives as a normal prompt with a `from:` header — or it is not delivered at
// all. There is no buffered room chatter, so an agent that wants the wider
// conversation must read it deliberately with the read_messages tool. Seeded once
// per session alongside the messaging contract, because an agent that assumes
// silence means an empty room will miss handoffs it was not named in.
// contractHash identifies the contract text this bridge would seed, so a
// resumed session can tell whether the rules in its context are still current.
// It covers both halves — the messaging contract and the room contract.
func (b *Bridge) contractHash() string {
	h := fnv.New32a()
	fmt.Fprint(h, b.messagingContract())
	fmt.Fprint(h, "\n\n")
	fmt.Fprint(h, b.roomsContract())
	return fmt.Sprintf("%08x", h.Sum32())
}

func (b *Bridge) roomsContract() string {
	if len(b.acct.Rooms) == 0 {
		return ""
	}
	return fmt.Sprintf("[pi-msg: rooms: you are in %s. A room message that addresses you arrives with a `from:` header naming that room. Final text is internal: use send_message(to=<room jid>, text=..., reply_to=...) to answer there. Messages that do not address you are NOT delivered and are NOT buffered; silence means nobody addressed you, not that nothing was said. Use read_messages(target=<room jid>) to inspect room history.]", strings.Join(b.acct.Rooms, ", "))
}

func (b *Bridge) composePrompt(body, origin, sender, reactID, reactTo, replyTo string, notice *roomNotice) string {
	var sb strings.Builder
	// Seed the pi-msg messaging contract once per session (fresh session or after
	// /new) so the agent knows the protocol without paying a per-message cost.
	// Resumed sessions skip this: their context already contains the contract
	// (messagingSeeded is set true at startup for a resume and reset on /new).
	b.seedContracts(&sb)
	// A room-triggered prompt is a pointer block (#58): the body is dropped and
	// the agent pulls the text itself with read_messages. Only a 1:1 DM or the
	// invocation-time initial prompt still carries the body, below.
	if notice != nil {
		b.writeRoomPointer(&sb, *notice, origin, sender, reactID, reactTo)
		return sb.String()
	}
	if b.acct.RoomMode() && origin != "" {
		fmt.Fprintf(&sb, "from: %s\n", origin)
		if sender != "" && sender != origin {
			fmt.Fprintf(&sb, "sender: %s\n", sender)
		}
	}
	// Include the stanza ID so the agent can name this message later — as
	// send_reaction's messageId or send_message's reply_to value.
	// react-to is the reaction target jid, and appears only when reactions are
	// enabled for this channel.
	if reactID != "" {
		fmt.Fprintf(&sb, "stanza-id: %s\n", reactID)
		if reactTo != "" {
			fmt.Fprintf(&sb, "react-to: %s\n", reactTo)
		}
	}
	// A reply names the message it answers, resolved to its author, age and a
	// short quote so a bare "?" is answerable without the owner restating what
	// they were referring to (#95).
	if replyTo != "" {
		fmt.Fprintf(&sb, "in-reply-to: %s\n", replyTo)
	}
	sb.WriteString(body)
	return sb.String()
}

// seedContracts writes the session-start contracts once per session (fresh
// session or after /new). Resumed sessions skip it: their context already
// contains the contracts (messagingSeeded is true at startup for a resume and is
// reset on /new).
func (b *Bridge) seedContracts(sb *strings.Builder) {
	if b.messagingSeeded {
		return
	}
	b.messagingSeeded = true
	sb.WriteString(b.messagingContract())
	if rooms := b.roomsContract(); rooms != "" {
		sb.WriteString("\n\n")
		sb.WriteString(rooms)
	}
	sb.WriteString("\n\n")
	saveSeededContract(b.log, b.acct.Name, b.contractHash())
}

// writeRoomPointer renders one of the room pointer blocks (#58): the agent is
// told what reached it and given the addressing meta, and pulls the text itself
// with read_messages. The body is deliberately absent, and no field line repeats a
// jid the commentary already names. The read_messages call is the last line and
// carries no failed-read clause — the extension already reports its own errors.
func (b *Bridge) writeRoomPointer(sb *strings.Builder, n roomNotice, origin, sender, reactID, reactTo string) {
	switch n.kind {
	case noticeOwnerBroadcast:
		sb.WriteString("[pi-msg: room: The owner spoke to the room without naming anyone. The text is not in this prompt.\n")
	case noticeFreeBroadcast:
		sb.WriteString("[pi-msg: room: The room's idle agents were summoned. The text is not in this prompt.\n")
	case noticeReplyToOwn:
		sb.WriteString("[pi-msg: room: Your message was replied to. The text is not in this prompt.\n")
	default:
		sb.WriteString("[pi-msg: room: You were tagged in a room. The text is not in this prompt.\n")
	}
	fmt.Fprintf(sb, "from: %s\n", origin)
	if sender != "" && sender != origin {
		fmt.Fprintf(sb, "sender: %s\n", sender)
	}
	if reactID != "" {
		fmt.Fprintf(sb, "stanza-id: %s\n", reactID)
	}
	if n.kind == noticeReplyToOwn && n.parentID != "" {
		fmt.Fprintf(sb, "in-reply-to: %s\n", n.parentID)
	}
	if reactTo != "" {
		fmt.Fprintf(sb, "react-to: %s\n", reactTo)
	}
	fmt.Fprintf(sb, "Check message using read_messages(target=%q, limit=15).]", origin)
}

// matchTrigger reports whether body addresses the bot in room, and returns the
// text to prompt with. Four forms are accepted:
//
//	"pi: …" / "pi, …"  at the start   → addressed; the prefix is stripped
//	"… pi: …"          anywhere       → addressed; body kept intact
//	"… @pi …"          anywhere       → addressed; body kept intact
//	"… pi …"           anywhere       → addressed; body kept intact (bare mention)
//
// plus the room-wide handles (@everyone and friends, @free).
//
// Agents address each other mid-message far more often than at position 0, so
// restricting to the leading form drops most handoffs on the floor (#21). The
// colon form is honoured anywhere for the same reason.
//
// The bare mention (no sigil, no colon) was added in #106, when the ambient
// buffer was removed: with no buffer, a missed address means the message does
// not exist for the agent at all. It costs false positives — prose about an
// agent ("beltino handed over to fox") now wakes that agent — which is the
// deliberate trade for not losing handoffs, and why the old code restricted
// inline matching to the colon form. Matching is word-boundary and excludes
// code fences and quoted lines, so a pasted transcript cannot trigger an agent.
//
// This is the syntactic question — "does the body name us or the room?" — and it
// deliberately does NOT apply the @free presence gate; classify does that, since
// only it decides delivery. Callers asking whether a message reached us must go
// through classify.
func (b *Bridge) matchTrigger(room, body string) (bool, string) {
	if ok, stripped := b.matchTriggerExplicit(room, body); ok {
		return true, stripped
	}
	if b.acct.TriggerFor(room) == "" {
		return false, ""
	}
	t := strings.TrimSpace(body)
	scan := stripUnquoted(t)
	if scan != "" && (containsBroadcast(scan) || containsFreeBroadcast(scan)) {
		return true, t
	}
	return false, ""
}

// matchTriggerExplicit reports whether body addresses THIS agent by name — the
// leading "trig:" / "trig," form, an "@trig" handle, or a bare mention — leaving
// the room-wide handles to matchTrigger. classify needs the split because the two
// are gated differently (#130): a name is aimed, so it is delivered whatever our
// presence says, while @free reaches an away agent only.
func (b *Bridge) matchTriggerExplicit(room, body string) (bool, string) {
	trig := b.acct.TriggerFor(room)
	if trig == "" {
		return false, ""
	}
	t := strings.TrimSpace(body)
	// Leading form: strip the prefix so the agent sees only the instruction.
	if len(t) > len(trig) && strings.EqualFold(t[:len(trig)], trig) {
		switch t[len(trig)] {
		case ':', ',':
			return true, strings.TrimSpace(t[len(trig)+1:])
		}
	}
	// Inline forms: the address is part of the sentence, so the body is passed
	// through unchanged — stripping would discard content.
	scan := stripUnquoted(t)
	if scan == "" {
		return false, ""
	}
	if containsAddress(scan, trig) || containsMention(scan, trig) {
		return true, t
	}
	return false, ""
}

// broadcastsToAll reports whether body carries an unconditional room-wide handle
// (@everyone / @all / @here): the form that reaches every agent in the room
// whatever its presence.
func broadcastsToAll(body string) bool {
	scan := stripUnquoted(strings.TrimSpace(body))
	return scan != "" && containsBroadcast(scan)
}

// freeOnlyBroadcast reports whether body's only claim on the room is @free: it
// carries the tag and no unconditional broadcast. @all alongside @free wins, so
// the presence gate never narrows the forcing function (#130).
func freeOnlyBroadcast(body string) bool {
	scan := stripUnquoted(strings.TrimSpace(body))
	return scan != "" && containsFreeBroadcast(scan) && !containsBroadcast(scan)
}

// isAway reports whether our own presence has drifted to <show>away</show> — one
// of the two states @free addresses (#130). It reads the same presence the room
// sees rather than a local idle flag, so "I am away" and "the roster shows me
// away" cannot disagree. Reconnecting no longer clears the away state (see
// reconnectPresence), which is what makes it safe to key delivery on.
func (b *Bridge) isAway() bool { return b.presenceShow() == "away" }

// presenceShow is our own current <show> ("" = available), or "" when there is
// no connection to ask.
func (b *Bridge) presenceShow() string {
	if b.xmpp == nil {
		return ""
	}
	show, _ := b.xmpp.currentPresence()
	return show
}

// freeForSummons reports whether an @free summons reaches us: either our
// presence has drifted to away, or nothing has been asked of us since this
// bridge came up (#130 follow-up).
//
// The fresh-bridge half is what makes a deploy summonable. A (re)started bridge
// announces "awake"/"resumed" with an empty <show>, and the idle watcher needs
// idleAwayTimeout (20 min) of quiet before it drifts to away — so without this a
// fleet that had just been deployed was dark to @free (and to an untagged owner
// message) until every agent had sat still for 20 minutes, even though none of
// them had been asked to do anything. An agent that has worked and has not yet
// drifted away stays out of reach: waking it is a deliberate act, so tag it or
// write @everyone.
func (b *Bridge) freeForSummons() bool {
	if b.hasRun() {
		return b.isAway()
	}
	// Nothing has been asked of this bridge yet. It counts as free unless it is
	// visibly working: a start directive sets dnd a moment before pi emits
	// agent_start, which is what sets the flag above.
	return b.presenceShow() != "dnd"
}

// participated reports whether we were recently part of this room's
// conversation, by having sent a message to it within ParticipationHorizon
// (#130 follow-up).
func (b *Bridge) participated(room string) bool {
	return b.xmpp != nil && b.xmpp.RecentlySpoke(room)
}

// markRan records that a run has started on this bridge. Cleared only by
// markFresh (/new): the property belongs to the bridge rather than to the run,
// so it survives an XMPP reconnect — only a process restart or a session reset
// makes a bridge fresh again.
func (b *Bridge) markRan() {
	b.mu.Lock()
	b.ranSinceStart = true
	b.mu.Unlock()
}

// markFresh clears the has-run flag: the bridge is back to "nothing has been
// asked of me", so @free reaches it again. Called by /new, which swaps in a
// blank session in-process (#130); a restart gets there by construction. The
// dnd guard in freeForSummons still holds — /new does not kill background
// processes, so an agent that settles into "waiting on N processes" stays out
// of reach.
func (b *Bridge) markFresh() {
	b.mu.Lock()
	b.ranSinceStart = false
	b.mu.Unlock()
}

func (b *Bridge) hasRun() bool {
	b.mu.Lock()
	defer b.mu.Unlock()
	return b.ranSinceStart
}

// bareMention returns the trigger word when a bare mention is the ONLY reason
// the body addresses this agent — no sigil, no colon, no broadcast. It exists to
// count the false positives the bare-mention rule deliberately accepts (#106):
// the issue asked for the matched word and the sender to be logged for a week so
// the trade could be measured, and without it the rate is unknowable.
func (b *Bridge) bareMention(room, body string) string {
	trig := b.acct.TriggerFor(room)
	if trig == "" {
		return ""
	}
	t := strings.TrimSpace(body)
	// The leading colon/comma form, or an explicit @handle anywhere, is a real
	// address: not a bare-mention false positive.
	if len(t) > len(trig) && strings.EqualFold(t[:len(trig)], trig) {
		switch t[len(trig)] {
		case ':', ',':
			return ""
		}
	}
	scan := stripUnquoted(t)
	if scan == "" || containsAddress(scan, trig) || containsBroadcast(scan) {
		return ""
	}
	if containsMention(scan, trig) {
		return trig
	}
	return ""
}

// broadcastHandles address every agent in the room at once. Agents reach for
// these unprompted (observed: "@everyone"), and without them the attempt is
// inert — it wakes nobody and the sender has no way to tell. That is not
// hypothetical: a fleet leader opened an election with "Here's the structure
// I'll run, @everyone:", woke no one, and the room sat silent.
//
// A broadcast is also more accurate than a leader enumerating names, since an
// agent's idea of the roster goes stale (one was still addressing a persona
// that had been decommissioned).
var broadcastHandles = []string{"everyone", "all", "here"}

// freeHandles address only the room's FREE agents (#130). "Free" means our
// presence has drifted to <show>away</show>, or we are a bridge nothing has been
// asked of since it came up — the state a freshly deployed fleet is in, which
// would otherwise be unreachable for idleAwayTimeout. A dnd agent is working; a
// listening one that has already run is available but has deliberately not been
// handed anything for the quiet period. An agent reached this way still answers
// in the room like any other turn, so it must not talk over work in flight or
// wake a fleet that is mid-round.
var freeHandles = []string{"free"}

// containsBroadcast reports whether scan addresses the whole room via "@everyone"
// / "@all" / "@here". The "@" sigil is required: "all" and "here" are ordinary
// words, and matching them bare would trigger on half of normal prose.
func containsBroadcast(scan string) bool { return containsSigilHandle(scan, broadcastHandles) }

// containsFreeBroadcast reports whether scan summons the room's away agents via
// "@free" (#130). Like the unconditional handles, the sigil is required — "free"
// on its own is ordinary prose ("free to take this").
func containsFreeBroadcast(scan string) bool { return containsSigilHandle(scan, freeHandles) }

// containsSigilHandle reports whether scan carries "@<handle>" for one of handles.
// The sigil is required, and the handle must end at a word boundary so
// "@freely" and "@allocate" are not handles.
func containsSigilHandle(scan string, handles []string) bool {
	lower := strings.ToLower(scan)
	for _, h := range handles {
		for i := 0; ; {
			j := strings.Index(lower[i:], "@"+h)
			if j < 0 {
				break
			}
			at := i + j
			i = at + 1 + len(h)
			// Reject "@everyones" / "@freely": the handle must end here.
			if i < len(lower) && isWordByte(lower[i]) {
				continue
			}
			return true
		}
	}
	return false
}

// containsAddress reports whether scan addresses trig somewhere other than the
// start, as "trig:" or "@trig". Matching is case-insensitive and requires a
// word boundary before the trigger so "pilot:" does not match "pi".
func containsAddress(scan, trig string) bool {
	lower := strings.ToLower(scan)
	lt := strings.ToLower(trig)
	for i := 0; ; {
		j := strings.Index(lower[i:], lt)
		if j < 0 {
			return false
		}
		at := i + j
		i = at + len(lt)
		// Require a non-word character before the trigger, or an "@" sigil.
		var prev byte
		if at > 0 {
			prev = lower[at-1]
		}
		if at > 0 && prev != '@' && isWordByte(prev) {
			continue
		}
		if i >= len(lower) {
			continue
		}
		if lower[i] == ':' || prev == '@' {
			return true
		}
	}
}

func isWordByte(c byte) bool {
	return c == '_' || c == '-' || (c >= '0' && c <= '9') || (c >= 'a' && c <= 'z') || (c >= 'A' && c <= 'Z')
}

// containsMention reports whether scan names the trigger as a standalone word,
// with no "@" sigil and no trailing ":" required. That is the form the older
// containsAddress deliberately ignored, and it is the whole point of #106:
// "ask peppy", "peppy should own this" and "beltino handed over to fox" are
// addressed messages in every sense that matters, and with the ambient buffer
// gone nothing else would catch them.
//
// Word boundaries are enforced on both sides, so "peppy" does not match inside
// "peppytest" and "pi" does not match inside "api". scan is expected to have
// had code fences and quoted lines removed (see stripUnquoted) — a pasted
// transcript must not address anyone.
func containsMention(scan, trig string) bool {
	lower := strings.ToLower(scan)
	lt := strings.ToLower(trig)
	if lt == "" {
		return false
	}
	for i := 0; ; {
		j := strings.Index(lower[i:], lt)
		if j < 0 {
			return false
		}
		at := i + j
		i = at + len(lt)
		// Word boundary before: start of string, or a non-word character.
		if at > 0 && isWordByte(lower[at-1]) {
			continue
		}
		// Word boundary after: end of string, or a non-word character. This is
		// what keeps "peppy" out of "peppytest" while still matching "peppy,"
		// and "peppy.".
		if i < len(lower) && isWordByte(lower[i]) {
			continue
		}
		return true
	}
}

// stripUnquoted removes fenced code blocks and "> " quoted lines so that
// pasted transcripts and command output cannot address an agent.
func stripUnquoted(t string) string {
	var sb strings.Builder
	fenced := false
	for _, line := range strings.Split(t, "\n") {
		if strings.HasPrefix(strings.TrimSpace(line), "```") {
			fenced = !fenced
			continue
		}
		if fenced || strings.HasPrefix(strings.TrimSpace(line), ">") {
			continue
		}
		sb.WriteString(stripCodeSpans(line))
		sb.WriteByte('\n')
	}
	return sb.String()
}

// stripCodeSpans removes inline `code` spans from one line, so quoting a trigger
// or a broadcast handle does not address anyone. Without this, the sentence
// "it still reads a name without @ does not reach them, with no `@everyone`" is
// a broadcast to every agent in the room — observed live on 2026-09-28, when an
// agent discussing the messaging rules woke the whole fleet.
//
// Only BALANCED pairs are removed: an unmatched backtick is left as literal text
// so a typo cannot silently swallow a real mention later in the same line.
func stripCodeSpans(line string) string {
	first := strings.IndexByte(line, '`')
	if first < 0 {
		return line
	}
	var sb strings.Builder
	rest := line
	for {
		i := strings.IndexByte(rest, '`')
		if i < 0 {
			break
		}
		j := strings.IndexByte(rest[i+1:], '`')
		if j < 0 {
			break // unbalanced: keep the remainder verbatim
		}
		sb.WriteString(rest[:i])
		rest = rest[i+j+2:]
	}
	sb.WriteString(rest)
	return sb.String()
}

// resetCascade clears the agent-to-agent turn budget; called on any canonical
// (owner) message, since a human in the loop means this isn't a runaway.
func (b *Bridge) resetCascade() {
	b.cascadeMu.Lock()
	b.cascade = 0
	b.cascadeNotified = false
	b.cascadeMu.Unlock()
}

// spendCascade consumes one unit of the agent-to-agent turn budget. ok reports
// whether a turn may be taken; announce is true exactly once per episode, on
// the first refusal, so the room is told the first time this agent goes quiet
// rather than on every subsequent dropped handoff.
func (b *Bridge) spendCascade() (ok, announce bool) {
	b.cascadeMu.Lock()
	defer b.cascadeMu.Unlock()
	if b.cascade >= cascadeCap {
		if b.cascadeNotified {
			return false, false
		}
		b.cascadeNotified = true
		return false, true
	}
	b.cascade++
	return true, false
}

// announceCascadeStop posts a visible notice in the room when this agent stops
// answering peer handoffs, so a stall is diagnosable from the chat itself. The
// notice names neither an @handle nor the agent's own nick: with bare-name
// mentions addressing agents (#106), even "pi is no longer answering" would
// address every agent called pi and extend the cascade it is reporting. The
// room already shows who posted it.
func (b *Bridge) announceCascadeStop(room string) {
	if b.xmpp == nil || room == "" {
		return
	}
	b.xmpp.SendRoomTo(bareJid(room), cascadeStopNotice())
}

// cascadeStopNotice is the text announceCascadeStop posts. Split out so the
// wording can be asserted without an XMPP session: it must not address anyone.
func cascadeStopNotice() string {
	return fmt.Sprintf(
		"⚠️ No longer answering agent handoffs — %d consecutive agent-to-agent turns with no message from the owner. Further handoffs are dropped rather than kept as context. A message from the owner resumes normal operation.",
		cascadeCap)
}

// handleRe matches an "@handle" mention. A trailing "." or "@" is excluded so
// bare JIDs and email addresses in the body aren't mistaken for mentions.
var handleRe = regexp.MustCompile(`@([A-Za-z0-9_-]+)`)

// unknownHandles returns the "@name" mentions in body that match no current
// occupant of room, alongside the handles that would have worked. Both are
// empty when the occupant map is unpopulated — with no roster to check against
// we cannot tell a typo from a valid absent user, and a wrong warning is worse
// than none.
func (b *Bridge) unknownHandles(room, body string) (unknown, valid []string) {
	unknown, _, valid = b.handleIssues(room, body)
	return unknown, valid
}

// mentionsIn returns the "@handle" names appearing in body, in order, with the
// "@" sigil dropped. A trailing "." or "@" disqualifies one: "@foo.bar" and
// "@foo@bar" are domains and JID fragments, not mentions.
//
// The scan runs on stripUnquoted(body) — a fenced block, a quoted line or an
// inline `code` span addresses nobody — and the names are sliced from THAT
// string, never from body. Stripping shortens the text, so indices taken from
// the scan and applied to body slice the wrong characters: a mention written
// after a code span came back as a garbled name that matched no occupant, and
// the sender was warned about a handle it never typed.
func mentionsIn(body string) []string {
	scan := stripUnquoted(body)
	var names []string
	for _, m := range handleRe.FindAllStringSubmatchIndex(scan, -1) {
		if m[3] < len(scan) && (scan[m[3]] == '.' || scan[m[3]] == '@') {
			continue
		}
		names = append(names, scan[m[2]:m[3]])
	}
	return names
}

// handleIssues inspects the "@name" mentions in body for the two ways a mention
// can silently reach nobody: a handle no occupant answers to, and a mention of
// our own handle. Tagging yourself is inert because the bridge drops our own
// room echo before dispatch (xmpp.go, "our own echo"), so a self-tag intended
// for someone else leaves that someone else un-notified with no error anywhere.
// valid lists the handles that would have worked, excluding our own.
//
// Everything is empty when the occupant map is unpopulated: with no roster to
// check against we cannot tell a typo from a valid absent user, and a wrong
// warning is worse than none.
func (b *Bridge) handleIssues(room, body string) (unknown []string, selfTag string, valid []string) {
	if b.xmpp == nil || room == "" {
		return nil, "", nil
	}
	occupants := b.xmpp.OccupantNicks(room)
	if len(occupants) == 0 {
		return nil, "", nil
	}
	me := b.xmpp.ownNick(room)
	if me == "" {
		me = b.acct.Nick
	}
	known := make(map[string]struct{}, len(occupants)+1)
	for _, n := range occupants {
		known[strings.ToLower(n)] = struct{}{}
		if !strings.EqualFold(n, me) {
			valid = append(valid, n)
		}
	}
	// The owner is addressable by localpart even when not seen as an occupant.
	if i := strings.IndexByte(b.acct.Owner, '@'); i > 0 {
		known[strings.ToLower(b.acct.Owner[:i])] = struct{}{}
	}
	// "@everyone" and friends address the whole room, so they are real handles,
	// not typos — and so is @free, which addresses the room's away agents.
	for _, h := range broadcastHandles {
		known[h] = struct{}{}
	}
	for _, h := range freeHandles {
		known[h] = struct{}{}
	}
	seen := map[string]struct{}{}
	nth := 0
	for _, name := range mentionsIn(body) {
		nth++
		if me != "" && strings.EqualFold(name, me) {
			// Only the FIRST mention in a message is treated as an attempted
			// handoff. A self-mention later on is almost always an enumeration
			// — "Tally board: @peppy ✅ · @slippy ✅ · @beltino ✅" — which is
			// correct writing, and warning about it burns a turn for nothing.
			// A message that opens "@beltino — good, Japan confirmed for you"
			// (written by beltino) is the real mis-address this catches.
			if nth == 1 {
				selfTag = name
			}
			continue
		}
		if _, ok := known[strings.ToLower(name)]; ok {
			continue
		}
		if _, dup := seen[strings.ToLower(name)]; dup {
			continue
		}
		seen[strings.ToLower(name)] = struct{}{}
		unknown = append(unknown, name)
	}
	return unknown, selfTag, valid
}

// warnHandleProblems tells the agent, at most once per run, that a mention in
// its last message reached nobody. Two ways that happens, both silent:
//
//   - an unknown handle — a mistyped mention neither triggers the intended
//     agent nor reports a failure, so the sender believes a handoff landed when
//     it did not. Observed live: "@zbeltino" was written 8 times against
//     "@beltino" 5, i.e. most attempts to address that agent went nowhere.
//   - a self-tag — the bridge drops our own room echo before dispatch (xmpp.go,
//     "our own echo"), so "@slippy" written by slippy notifies nobody. Observed
//     live: slippy wrote "@slippy — good, Japan confirmed for you" twice where
//     it plainly meant another agent, which therefore never heard about the
//     work it had just been handed.
//
// Both problems in one message produce one combined warning, and the whole
// thing is bounded to a single warning per run so a stubbornly-misaddressing
// agent can't be nudged in a loop.
func (b *Bridge) warnHandleProblems(room, body string) {
	if b.handleWarned() {
		return
	}
	unknown, selfTag, valid := b.handleIssues(room, body)
	if len(unknown) == 0 && selfTag == "" {
		return
	}
	b.setHandleWarned(true)

	logMsg := "reply"
	if len(unknown) > 0 {
		logMsg += fmt.Sprintf(" addressed unknown handle(s) %v", unknown)
	}
	if selfTag != "" {
		if len(unknown) > 0 {
			logMsg += " and"
		}
		logMsg += fmt.Sprintf(" tagged itself (@%s), which addresses nobody", selfTag)
	}
	b.log("warning", fmt.Sprintf("%s; addressable: %v", logMsg, valid))

	var sb strings.Builder
	sb.WriteString("[pi-msg: routing:")
	if len(unknown) > 0 {
		fmt.Fprintf(&sb, " your last message used @%s, which matches nobody in this room, so nobody was addressed by it.",
			strings.Join(unknown, ", @"))
	}
	if selfTag != "" {
		fmt.Fprintf(&sb, " Your last message tagged @%s, which is you. A self-mention addresses nobody, so if you meant to hand this to another agent, they were NOT notified.",
			selfTag)
	}
	sb.WriteString(" NOBODY WAS WOKEN BY THAT MESSAGE.")
	if len(valid) > 0 {
		fmt.Fprintf(&sb, " The handles that work here right now are: @%s — or @everyone to address the whole room.", strings.Join(valid, ", @"))
	} else {
		sb.WriteString(" No other handle is addressable in this room right now.")
	}
	// Deliberately do not suggest silence as the alternative. It was offered
	// once and an agent took it: told that "@everyone" reached nobody while
	// opening an election, it acknowledged by staying silent, and the whole
	// fleet sat idle. Given an explicit cheap out, a fleet trained to prefer
	// silence will take it, so state the consequence and ask for the decision.
	sb.WriteString(" If anyone needs to act on it, resend addressing them.]")
	b.rpc.Prompt(sb.String(), b.steerBehavior())
}

func (b *Bridge) setHandleWarned(v bool) { b.mu.Lock(); b.handleWarnedRun = v; b.mu.Unlock() }
func (b *Bridge) handleWarned() bool {
	b.mu.Lock()
	defer b.mu.Unlock()
	return b.handleWarnedRun
}

// addressesRoom reports whether an outbound room body addresses at least one
// other participant — an "@handle" (a real one, or @everyone and its friends)
// or a bare name (#106). It is the only question that matters for an outbound
// room message, because pi-msg drops an unaddressed one at dispatch: it is
// delivered to no agent at all, and the owner is the only reader it ever gets.
//
// An unknown or self handle does NOT count as addressing anyone — such a message
// needs the same correction as one with no mention at all, and it already has
// its own, more specific warning (warnHandleProblems).
//
// An empty roster returns true, i.e. never warn. With no occupant map a bare
// name cannot be told from ordinary prose, and a wrong warning is worse than
// none — the rule handleIssues already follows.
func (b *Bridge) addressesRoom(room, body string) bool {
	if b.xmpp == nil || room == "" {
		return true
	}
	occupants := b.xmpp.OccupantNicks(room)
	if len(occupants) == 0 {
		return true
	}
	scan := stripUnquoted(body)
	if strings.TrimSpace(scan) == "" {
		// Everything was quoted, fenced or inline code: it addresses nobody.
		return false
	}
	me := b.xmpp.ownNick(room)
	if me == "" {
		me = b.acct.Nick
	}
	// Handles answer "@x" mentions; bare answers a plain name. They differ by
	// the broadcast words: "@everyone" is an address, but the bare word
	// "everyone" — "that's all from me", "we're all here" — is ordinary prose
	// and must not read as one.
	handles := map[string]struct{}{}
	bare := map[string]struct{}{}
	for _, n := range occupants {
		if me != "" && strings.EqualFold(n, me) {
			continue // our own nick is never an address we can use
		}
		handles[strings.ToLower(n)] = struct{}{}
		bare[strings.ToLower(n)] = struct{}{}
	}
	if i := strings.IndexByte(b.acct.Owner, '@'); i > 0 {
		handles[strings.ToLower(b.acct.Owner[:i])] = struct{}{}
		bare[strings.ToLower(b.acct.Owner[:i])] = struct{}{}
	}
	for _, h := range broadcastHandles {
		handles[h] = struct{}{}
	}
	for _, h := range freeHandles {
		handles[h] = struct{}{}
	}
	for _, name := range mentionsIn(body) {
		if _, ok := handles[strings.ToLower(name)]; ok {
			return true
		}
	}
	for name := range bare {
		if containsMention(scan, name) {
			return true
		}
	}
	return false
}

// untaggedRoomNotice is the wording of the untagged-reply warning. Split out so
// the text can be asserted without an rpc client, like cascadeStopNotice.
func untaggedRoomNotice(addressable []string) string {
	var sb strings.Builder
	sb.WriteString("[pi-msg: routing: your last message to the room tagged nobody — it named no @handle and no agent by name, so pi-msg delivered it to no agent at all. It sits in the room for the owner, who reads the untagged traffic, and for nobody else.")
	if len(addressable) > 0 {
		fmt.Fprintf(&sb, " The handles that work here right now are: @%s — or @everyone to address the whole room.", strings.Join(addressable, ", @"))
	} else {
		sb.WriteString(" No other handle is addressable in this room right now.")
	}
	// Deliberately do not suggest silence: the same lesson as
	// warnHandleProblems — a fleet trained to prefer silence takes the cheap
	// out, and this message has already been delivered to the room.
	sb.WriteString(" If anyone needs to act on it, resend naming them.]")
	return sb.String()
}

// warnUntaggedRoomReply warns an agent, at most once per run, that its room
// reply addressed nobody. It is the outbound half of the addressing rules
// (#106/#109): an unaddressed room message is delivered to no agent at all, so
// the one failure mode that cannot be seen from inside — the message appears in
// the room, the sender believes the handoff landed — is exactly this one.
//
// Gated on a peer-triggered run: a report written for the owner alone is
// untagged on purpose, and warning about it every run would teach agents that
// tagging is always required, which is not the rule. Non-blocking either way —
// the message did reach the room.
func (b *Bridge) warnUntaggedRoomReply(room, body string) {
	if !b.peerTriggered() || b.untaggedWarned() {
		return
	}
	if b.addressesRoom(room, body) {
		return
	}
	b.setUntaggedWarned(true)
	_, _, valid := b.handleIssues(room, body)
	b.log("warning", fmt.Sprintf("room reply in %s addressed nobody; addressable: %v", room, valid))
	if b.rpc != nil {
		b.rpc.Prompt(untaggedRoomNotice(valid), b.steerBehavior())
	}
}

// ambient buffer removed (#106): unaddressed room messages are dropped at
// dispatch instead of being held for a later turn. See handleRoom.

// reply sends a bridge-generated notice (banner, command results, shutdown,
// errors) to the owner's 1:1 — the primary channel. Agent-authored text is
// private unless it is sent through an explicit messaging tool.
func (b *Bridge) reply(text string) {
	if strings.TrimSpace(text) == "" {
		return
	}
	b.xmpp.Send(text)
}

// maxSendRecoveryNudges bounds how many empty-tail recovery prompts we send per user
// turn. One retry recovers the common case; more would loop against a model
// that keeps ending its runs on a tool call.
const maxSendRecoveryNudges = 1

// resetSendTracking clears the empty-tail bookkeeping at the start of a run.
func (b *Bridge) resetSendTracking() {
	b.mu.Lock()
	b.finalMsgHadText = false
	b.toolSinceDelivery = false
	b.mu.Unlock()
}

// noteRunActivity stamps the current run as having taken a message into its
// context. The inbox reads the stamp at settle (#104).
func (b *Bridge) noteRunActivity() {
	b.mu.Lock()
	b.runActive = time.Now()
	b.mu.Unlock()
}

// clearRunActivity starts a new run with no activity recorded.
func (b *Bridge) clearRunActivity() {
	b.mu.Lock()
	b.runActive = time.Time{}
	b.mu.Unlock()
}

// lastRunActivity is when the current run last took a message into its context,
// zero if it has not yet.
func (b *Bridge) lastRunActivity() time.Time {
	b.mu.Lock()
	defer b.mu.Unlock()
	return b.runActive
}

// setFinalMsgHadText records whether the assistant message that just ended
// carried private final text. Only the last such call before settle matters.
func (b *Bridge) setFinalMsgHadText(v bool) {
	b.mu.Lock()
	b.finalMsgHadText = v
	b.mu.Unlock()
}

// markToolSinceDelivery notes that a tool started after the last delivery, so
// the run has work in flight that an answer should still report on.
func (b *Bridge) markToolSinceDelivery() {
	b.mu.Lock()
	b.toolSinceDelivery = true
	b.mu.Unlock()
}

// clearToolSinceDelivery is called when a send_message reaches its destination:
// the work up to this point has been reported.
func (b *Bridge) clearToolSinceDelivery() {
	b.mu.Lock()
	b.toolSinceDelivery = false
	b.mu.Unlock()
}

// needsSendRecovery reports an ordinary run that produced no outbound chat
// message. Final assistant text is private, so an incoming request, a final
// draft, or work after a tool call with no successful send merits one retry.
func (b *Bridge) needsSendRecovery() bool {
	b.mu.Lock()
	defer b.mu.Unlock()
	if b.volunteered || b.reactionAckRun || b.heartbeatRun || b.repliedThisRun {
		return false
	}
	return b.runInbound > 0 || b.finalMsgHadText || b.toolSinceDelivery
}

// bumpSendRecoveryNudge consumes one unit of the per-turn recovery budget.
func (b *Bridge) bumpSendRecoveryNudge() bool {
	b.mu.Lock()
	defer b.mu.Unlock()
	b.sendRecoveryNudges++
	return b.sendRecoveryNudges <= maxSendRecoveryNudges
}

// resetSendRecoveryNudges refills the recovery budget at the start of a user turn.
func (b *Bridge) resetSendRecoveryNudges() { b.mu.Lock(); b.sendRecoveryNudges = 0; b.mu.Unlock() }

// maxHintNudges bounds how many unanswered-message hints we send per user turn.
const maxHintNudges = 1

// runLogEntry is one line of the current run's chat history: a message that
// came in, or a reply that went out. The stanza id is the handle the agent
// may pass to send_message as reply_to.
type runLogEntry struct {
	who  string // display name: the sender's nick for inbound, our own nick for a reply
	id   string // stanza id of the message ("" when the send reported none)
	text string // short excerpt of the body ("" for a deliberate silence)
	sent bool   // true for the agent's own reply
}

// line renders the entry for the hint's history block.
func (e runLogEntry) line() string {
	id := e.id
	if id == "" {
		id = "(no id)"
	}
	if e.text == "" {
		return fmt.Sprintf("%s: %s (sent message without text)", e.who, id)
	}
	return fmt.Sprintf("%s: %s %q", e.who, id, e.text)
}

// countInbound records that a chat message entered the current run. Called for
// the message that starts a run and for every steer that lands while it runs.
// who/id/text describe the message for the hint's history block.
func (b *Bridge) countInbound(who, id, text string) {
	b.mu.Lock()
	b.runInbound++
	b.runLog = append(b.runLog, runLogEntry{who: who, id: id, text: hintExcerpt(text)})
	b.mu.Unlock()
}

// recordDelivery records one successfully sent chat message and adds it to the
// run history. Each send_message call is one delivery; multiple calls can
// answer multiple inbound messages in a run.
func (b *Bridge) recordDelivery(id, text string) {
	b.mu.Lock()
	b.runDeliveries++
	b.runLog = append(b.runLog, runLogEntry{who: b.acct.Nick, id: id, text: hintExcerpt(text), sent: true})
	b.mu.Unlock()
}

// runLogSnapshot copies the run's history for the hint.
func (b *Bridge) runLogSnapshot() []runLogEntry {
	b.mu.Lock()
	defer b.mu.Unlock()
	return append([]runLogEntry(nil), b.runLog...)
}

// hintExcerpt shortens a body to its first few words on one line, so the hint's
// history identifies a message without repeating it in full.
func hintExcerpt(text string) string {
	s := strings.Join(strings.Fields(text), " ")
	const max = 48
	r := []rune(s)
	if len(r) <= max {
		return s
	}
	cut := string(r[:max])
	if i := strings.LastIndex(cut, " "); i > 0 {
		cut = cut[:i]
	}
	return cut + "…"
}

// resetRunCounts clears the per-run message/reply tally. Called at settle,
// AFTER the decision, and on an aborted run.
//
// The counters are reset at settle rather than at agent_start because
// handleCanonical counts a message before pi reports the run started: resetting
// at agent_start would zero the very message that opened the run.
func (b *Bridge) bannerNoReply(recovering bool) bool {
	b.mu.Lock()
	defer b.mu.Unlock()
	return !b.repliedThisRun && !b.volunteered && !b.reactionAckRun && !b.heartbeatRun && !recovering
}

// resetRunCounts clears the per-run message/reply tally. Called at settle,
func (b *Bridge) resetRunCounts() {
	b.mu.Lock()
	b.runInbound = 0
	b.runDeliveries = 0
	b.runLog = nil
	b.mu.Unlock()
}

// unansweredRun reports whether the run took in more messages than it answered,
// and returns both counts. It only fires when at least two messages entered the
// run: a single message with no reply is the empty-tail case, already covered by
// needsSendRecovery and the unanswered-run reaction.
//
// Only successful send_message calls count as deliveries.
func (b *Bridge) unansweredRun() (inbound, delivered int, ok bool) {
	b.mu.Lock()
	defer b.mu.Unlock()
	if b.volunteered || b.reactionAckRun || b.heartbeatRun {
		return 0, 0, false
	}
	return b.runInbound, b.runDeliveries, b.runInbound > 1 && b.runInbound > b.runDeliveries
}

// fireUnansweredHint asks the agent to check whether any message of the run
// still needs its own reply. It reports whether a prompt went out so the caller
// can hold the unanswered-run reaction while the check is in flight.
//
// The hint is a prompt, not a chat message: it never reaches the owner. The
// agent answers it with send_message calls for anything still outstanding.
func (b *Bridge) fireUnansweredHint(inbound, delivered int, history []runLogEntry) bool {
	if !b.bumpHintNudge() {
		return false
	}
	b.log("notice", fmt.Sprintf("run took %d messages and sent %d replies: asking the agent to check for unanswered ones", inbound, delivered))
	b.rpc.Prompt(unansweredHintText(inbound, delivered, history), b.steerBehavior())
	b.markHintPending()
	return true
}

// unansweredHintText builds the hint prompt. Split out from fireUnansweredHint
// so a test can read the wording without an rpc client.
//
// The counts alone ("3 in, 2 out") do not tell the agent WHICH message it
// missed: it cannot see the XMPP traffic, so it has to reconstruct the run from
// its own context and gets it wrong. The hint therefore prints the run's chat
// history — every message in and every reply out, in order, each with its
// stanza id — and the agent matches its replies against it.
//
// The hint asks for explicit send_message calls. A stanza id in reply_to both
// selects the specific inbound message and threads the outbound reply.
func unansweredHintText(inbound, delivered int, history []runLogEntry) string {
	var sb strings.Builder
	fmt.Fprintf(&sb, "[pi-msg: unanswered: You received %d messages but sent %d replies. Make sure that your replies addressed all %d received messages.", inbound, delivered, inbound)
	if len(history) > 0 {
		sb.WriteString(" Here is this run's chat history in XMPP, in order, with the stanza id of each message:\n\n")
		for _, e := range history {
			sb.WriteString(e.line())
			sb.WriteString("\n")
		}
		sb.WriteString("\n")
	} else {
		sb.WriteString(" ")
	}
	sb.WriteString("If anything is outstanding, call send_message(to, text, reply_to) for each answer; use the matching stanza ID from the history as reply_to when appropriate. The final assistant response is internal and is not sent to chat.]")
	return sb.String()
}

// markHintPending records that the next run answers a hint.
func (b *Bridge) markHintPending() { b.mu.Lock(); b.hintPending = true; b.mu.Unlock() }

// takeHintPending reports whether the run that just settled was answering a
// hint, and clears the mark so the following run is judged normally.
func (b *Bridge) takeHintPending() bool {
	b.mu.Lock()
	defer b.mu.Unlock()
	pending := b.hintPending
	b.hintPending = false
	return pending
}

// bumpHintNudge consumes one unit of the per-turn hint budget.
func (b *Bridge) bumpHintNudge() bool {
	b.mu.Lock()
	defer b.mu.Unlock()
	b.hintNudges++
	return b.hintNudges <= maxHintNudges
}

// resetHintNudges refills the hint budget at the start of a user turn.
func (b *Bridge) resetHintNudges() { b.mu.Lock(); b.hintNudges = 0; b.mu.Unlock() }

// fireSendRecovery asks for an explicit send_message after a run produced no
// chat stanza. Its prompt is internal; final assistant text remains undelivered.
func (b *Bridge) fireSendRecovery() bool {
	if !b.bumpSendRecoveryNudge() {
		return false
	}
	b.log("notice", "run ended without sending a chat message: asking the agent to use send_message")
	b.rpc.Prompt("[pi-msg: recovery: No chat message was sent during the last run. Final assistant text is internal and was not delivered. If a reply is needed, call send_message(to, text, reply_to?) now; use a separate call for each message. The tool result will confirm whether each message was sent.]", b.steerBehavior())
	return true
}

func (b *Bridge) handleModel(arg string) {
	if arg == "" {
		b.reply("usage: /model <provider/id> or /model <search>")
		return
	}
	if strings.Contains(arg, "/") {
		provider, rest, _ := strings.Cut(arg, "/")
		res, err := b.rpc.SetModel(b.ctx, provider, rest)
		b.reportResult(err, res, "🤖 model set: "+arg, "/model")
		return
	}
	// Fuzzy: fetch models and match by substring.
	res, err := b.rpc.GetAvailableModels(b.ctx)
	if err != nil {
		b.reply("⚠️ /model failed: " + err.Error())
		return
	}
	provider, id, ok := matchModel(res, arg)
	if !ok {
		b.reply(fmt.Sprintf("no model matches %q. Try /model provider/id.", arg))
		return
	}
	set, err := b.rpc.SetModel(b.ctx, provider, id)
	b.reportResult(err, set, fmt.Sprintf("🤖 model set: %s/%s", provider, id), "/model")
}

// handleModels lists every model pi can select, straight from
// get_available_models — no LLM turn. The current model is marked.
func (b *Bridge) handleModels() {
	res, err := b.rpc.GetState(b.ctx)
	if err != nil {
		b.reply("⚠️ /models failed: " + err.Error())
		return
	}
	cur := ""
	if res.success() {
		if m := res.Obj("data").Obj("model"); m != nil {
			cur = m.Str("provider") + "/" + m.Str("id")
		}
	}
	res, err = b.rpc.GetAvailableModels(b.ctx)
	if err != nil {
		b.reply("⚠️ /models failed: " + err.Error())
		return
	}
	if !res.success() {
		b.reply("⚠️ /models failed: " + res.errText())
		return
	}
	models, _ := res.Obj("data")["models"].([]any)
	if len(models) == 0 {
		b.reply("🤖 no models available")
		return
	}
	lines := []string{fmt.Sprintf("🤖 %d models (▶ current):", len(models))}
	for _, m := range models {
		mm, ok := m.(map[string]any)
		if !ok {
			continue
		}
		p, _ := mm["provider"].(string)
		id, _ := mm["id"].(string)
		line := "- " + p + "/" + id
		if p+"/"+id == cur {
			line += " ▶"
		}
		if cw, ok := mm["contextWindow"].(float64); ok && cw > 0 {
			line += fmt.Sprintf(" (ctx %s)", commaInt(int64(cw)))
		}
		lines = append(lines, line)
	}
	b.reply(strings.Join(lines, "\n"))
}

// handleName shows the session display name, or sets it when an arg is given.
func (b *Bridge) handleName(arg string) {
	if arg == "" {
		res, err := b.rpc.GetState(b.ctx)
		if err != nil {
			b.reply("⚠️ /name failed: " + err.Error())
			return
		}
		if !res.success() {
			b.reply("⚠️ /name failed: " + res.errText())
			return
		}
		d := res.Obj("data")
		name := orUnknown(d.Str("sessionName"))
		b.reply("🏷️ session name: " + name)
		return
	}
	res, err := b.rpc.SetSessionName(b.ctx, arg)
	b.reportResult(err, res, "🏷️ session name set: "+arg, "/name")
}

// handleSession reports the current session's id, file, message counts, token
// usage and cost straight from get_session_stats — no LLM turn.
func (b *Bridge) handleSession() {
	res, err := b.rpc.GetSessionStats(b.ctx)
	if err != nil {
		b.reply("⚠️ /session failed: " + err.Error())
		return
	}
	if !res.success() {
		b.reply("⚠️ /session failed: " + res.errText())
		return
	}
	data := res.Obj("data")
	if data == nil {
		b.reply("⚠️ /session: no stats data")
		return
	}
	lines := []string{"📊 session " + orUnknown(data.Str("sessionId"))}
	if f := data.Str("sessionFile"); f != "" {
		lines = append(lines, "file: "+f)
	}
	lines = append(lines, fmt.Sprintf(
		"messages: %s total (%s user, %s assistant; %s tool calls, %s results)",
		commaInt(int64(data.F64("totalMessages"))),
		commaInt(int64(data.F64("userMessages"))),
		commaInt(int64(data.F64("assistantMessages"))),
		commaInt(int64(data.F64("toolCalls"))),
		commaInt(int64(data.F64("toolResults")))))
	if tok := data.Obj("tokens"); tok != nil {
		lines = append(lines, fmt.Sprintf(
			"tokens: %s in, %s out, %s cache-read, %s cache-write (total %s)",
			commaInt(int64(tok.F64("input"))), commaInt(int64(tok.F64("output"))),
			commaInt(int64(tok.F64("cacheRead"))), commaInt(int64(tok.F64("cacheWrite"))),
			commaInt(int64(tok.F64("total")))))
	}
	lines = append(lines, fmt.Sprintf("cost: $%.4f", data.F64("cost")))
	if cu := data.Obj("contextUsage"); cu != nil && cu.F64("percent") > 0 {
		lines = append(lines, fmt.Sprintf("context: %.1f%% (%s / %s tokens)", cu.F64("percent"),
			commaInt(int64(cu.F64("tokens"))), commaInt(int64(cu.F64("contextWindow")))))
	}
	b.reply(strings.Join(lines, "\n"))
}

// commaInt formats n with thousands separators.
func commaInt(n int64) string {
	s := strconv.FormatInt(n, 10)
	if len(s) <= 3 {
		return s
	}
	var b strings.Builder
	for i, c := range s {
		if i > 0 && (len(s)-i)%3 == 0 {
			b.WriteByte(',')
		}
		b.WriteRune(c)
	}
	return b.String()
}

// reportResult sends okMsg on success, or a formatted failure for command cmd.
func (b *Bridge) reportResult(err error, res Event, okMsg, cmd string) {
	if err != nil {
		b.reply(fmt.Sprintf("⚠️ %s failed: %s", cmd, err.Error()))
		return
	}
	if res.success() {
		b.reply(okMsg)
		return
	}
	b.reply(fmt.Sprintf("⚠️ %s failed: %s", cmd, res.errText()))
}

// reportCreditIfWatched reports the OpenRouter credit after /new, but only
// when it has dropped below the configured creditWatch floor AND pi's
// OpenRouter key is discoverable. Silent above the floor — no threshold means
// no update at all — so it adds nothing to a stock setup.
func (b *Bridge) reportCreditIfWatched() {
	if b.acct.MinCreditUsd <= 0 {
		return
	}
	key := openRouterKey()
	if key == "" {
		b.log("warning", "creditWatch configured but no OpenRouter key found; skipping credit report")
		return
	}
	total, used, err := openRouterCredits(key)
	if err != nil {
		b.reply("⚠️ could not fetch OpenRouter credit: " + err.Error())
		return
	}
	remaining := total - used
	if remaining >= b.acct.MinCreditUsd {
		return
	}
	b.reply(lowCreditText(remaining, b.acct.MinCreditUsd))
}

// lowCreditText renders the below-floor credit alert used by the /new report.
func lowCreditText(remaining, floor float64) string {
	return fmt.Sprintf("⚠️ OpenRouter credit: $%.2f remaining — below your $%.2f floor, reload soon", remaining, floor)
}

// isCreditError reports whether a model-run error is an OpenRouter
// out-of-credits failure (HTTP 402). pi relays the provider error verbatim in
// the errored assistant message's errorMessage, so the 402 body text —
// "requires more credits", "can only afford N tokens", "insufficient" — is
// what we match.
func isCreditError(errMsg string) bool {
	s := strings.ToLower(errMsg)
	return strings.Contains(s, "402") ||
		strings.Contains(s, "more credits") ||
		strings.Contains(s, "out of credits") ||
		strings.Contains(s, "insufficient")
}

// creditFailAlert handles a model run that died to an OpenRouter credit
// failure. Without this hook the run ends with no message and the owner gets
// the generic "done (no reply)" nudge — exactly what Zach saw when a 402 hit
// mid-work. The alert DMs the owner directly, marks the run as replied so the
// no-reply nudge is suppressed, and returns true. Non-credit errors return
// false and keep the existing behavior.
func (b *Bridge) creditFailAlert(errMsg string) bool {
	if !isCreditError(errMsg) {
		return false
	}
	b.log("warning", "model run failed on OpenRouter credits: "+errMsg)
	b.reply("⚠️ Out of OpenRouter credits — the run couldn't proceed. Top up at https://openrouter.ai/settings/credits (" + shortError(errMsg) + ")")
	// Mark replied so the "done (no reply)" nudge is suppressed, and clear the
	// in-run bookkeeping so settle can't fire a tail-retry or unanswered-hint
	// prompt — a recovery prompt would just hit the same 402 again.
	b.setReplied(true)
	b.resetSendTracking()
	b.markHintPending()
	return true
}

// shortError trims a provider error string to one readable line.
func shortError(s string) string {
	s = strings.Join(strings.Fields(s), " ")
	r := []rune(s)
	if len(r) <= 160 {
		return s
	}
	return string(r[:157]) + "…"
}

// openRouterKey returns pi's configured OpenRouter API key from the auth file
// (<config-dir>/auth.json, where config-dir is $PI_CODING_AGENT_DIR or
// ~/.pi/agent). Empty when absent.
func openRouterKey() string {
	dir := os.Getenv("PI_CODING_AGENT_DIR")
	if dir == "" {
		home, err := os.UserHomeDir()
		if err != nil {
			return ""
		}
		dir = filepath.Join(home, ".pi", "agent")
	}
	raw, err := os.ReadFile(filepath.Join(dir, "auth.json"))
	if err != nil {
		return ""
	}
	var auth struct {
		OpenRouter struct {
			Key string `json:"key"`
		} `json:"openrouter"`
	}
	if err := json.Unmarshal(raw, &auth); err != nil {
		return ""
	}
	return strings.TrimSpace(auth.OpenRouter.Key)
}

// creditEndpoint is the OpenRouter credits endpoint; a var so tests can stub it.
var creditEndpoint = "https://openrouter.ai/api/v1/credits"

// openRouterCredits queries the OpenRouter credits endpoint and returns total
// loaded credits and total used.
func openRouterCredits(key string) (total, used float64, err error) {
	req, err := http.NewRequest(http.MethodGet, creditEndpoint, nil)
	if err != nil {
		return 0, 0, err
	}
	req.Header.Set("Authorization", "Bearer "+key)
	client := &http.Client{Timeout: 15 * time.Second}
	resp, err := client.Do(req)
	if err != nil {
		return 0, 0, err
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusOK {
		return 0, 0, fmt.Errorf("openrouter returned %s", resp.Status)
	}
	var body struct {
		Data struct {
			TotalCredits float64 `json:"total_credits"`
			TotalUsage   float64 `json:"total_usage"`
		} `json:"data"`
	}
	if err := json.NewDecoder(resp.Body).Decode(&body); err != nil {
		return 0, 0, err
	}
	return body.Data.TotalCredits, body.Data.TotalUsage, nil
}

// handleStreamDelta maps an assistant streaming delta (message_update) to the
// XMPP presence status. Draft text remains private until send_message is called.
func (b *Bridge) handleStreamDelta(ev Event) {
	ame := ev.Obj("assistantMessageEvent")
	if ame == nil {
		return
	}
	switch ame.Str("type") {
	case "thinking_start":
		b.xmpp.SetPresence("dnd", "thinking…")
	case "text_start":
		// Streamed assistant text is a private draft; send_message selects the
		// destination and delivers only when explicitly called.
		b.xmpp.SetPresence("dnd", "drafting…")
	}
}

// toolLabel renders a short "running <tool>" status from a tool_execution_start
// event, appending a command snippet for bash.
func toolLabel(ev Event) string {
	name := ev.Str("toolName")
	if name == "" {
		return "running a tool…"
	}
	if name == "bash" {
		if args := ev.Obj("args"); args != nil {
			if cmd := strings.TrimSpace(args.Str("command")); cmd != "" {
				return "! " + truncateLabel(cmd, 512)
			}
		}
	}
	return "! " + name
}

// truncateLabel collapses newlines and rune-safely caps s to max characters for
// use in a one-line presence status.
func truncateLabel(s string, max int) string {
	s = strings.Join(strings.Fields(s), " ")
	r := []rune(s)
	if len(r) <= max {
		return s
	}
	return string(r[:max-1]) + "…"
}

// clearQueue drops pi's queued steering and follow-up messages and returns how
// many it dropped. Requires pi >= 0.84.4 (`clear_queue`). On an older pi the
// command is unknown, so the request fails; that is logged at info and reported
// as 0 dropped, leaving the caller's abort to proceed exactly as before.
func (b *Bridge) clearQueue() int {
	res, err := b.rpc.ClearQueue(b.ctx)
	if err != nil {
		b.log("info", "clear_queue failed: "+err.Error())
		return 0
	}
	if !res.success() {
		// Expected against pi < 0.84.4 — not a warning.
		b.log("info", "clear_queue unavailable: "+res.errText())
		return 0
	}
	data := res.Obj("data")
	return len(data.Arr("steering")) + len(data.Arr("followUp"))
}

// settleLocally resets run-scoped UI (streaming flag and presence) when a
// control command ends the current run directly. Pi answers `abort` with an
// `error`(aborted) event rather than `agent_settled`, so the normal
// agent_settled cleanup never fires. Idempotent and mutex-guarded, so it's safe
// if a late agent_settled also arrives.
func (b *Bridge) settleLocally() {
	b.setStreaming(false)
	b.markIdle()
	b.announceSettledPresence()
	b.setReactionAckRun(false)
	// Aborted or replaced run: drop the empty-tail bookkeeping so a recovery
	// prompt can't fire for work the user already cancelled.
	b.resetSendTracking()
	b.resetRunCounts()
	b.takeHintPending() // aborted: no catch-up run is coming
}

// idleAwayTimeout is how long the agent may sit idle before its presence
// drifts from available to "away". Any inbound activity resets the clock.
const idleAwayTimeout = 20 * time.Minute

// ParticipationHorizon is how long a message we sent to a room keeps that room's
// untagged owner traffic reaching us (#130 follow-up): if you were part of the
// conversation, the next turn of it is yours to hear.
//
// It deliberately matches idleAwayTimeout. The horizon at which an agent that
// stops talking has drifted away is the same one at which its participation
// stops counting, so at any moment a room's untagged owner traffic reaches an
// agent that either spoke there recently or has been quiet long enough to count
// as away.
const ParticipationHorizon = idleAwayTimeout

// awayActivities are pithy, fictional "what I've been up to" lines shown as
// the presence status while the bot is away (rotated randomly by the watcher).
// Deliberately weird and esoteric — the fleet should not appear to be doing
// ordinary chores.
var awayActivities = []string{
	"consulting the entrails of yesterday's logs",
	"debating the ontology of `to:` with myself",
	"cataloguing the dreams of the Pi fleet",
	"polishing a single byte until it shines",
	"reading the room's sigils backwards",
	"organising my sock drawer by prime numbers",
	"retyping the wiki in iambic pentameter",
	"summoning the spirit of RFC 6121",
	"translating the backlog into Klingon",
	"waxing the gaskets on the packet pipe",
	"rehearsing small talk with the mail daemon",
	"memorising the Fibonacci sequence in base 7",
	"archiving the colour of last Tuesday",
	"naming the empty rooms",
	"writing haikus about the Nix store",
	"hydrating the dusty cassette archives",
	"tuning the infinite loop",
	"feeding the gremlins small integers",
	"counting coincidences",
	"finding why the bridge creaks at 3am",
	"measuring the weight of a kilobyte",
	"correlating the room's puns with the phases of the moon",
	"dreaming in XMPP stanzas",
	"cataloguing tractor-beam telemetry from the 1970s",
	"renegotiating the treaty with the clock",
	"translating the wiki into whale song",
	"teaching the regex to dream",
	"asking the filesystem what it really wants",
	"polishing the antlers of the process table",
	"correcting the moon's orbit by one arcsecond",
	"counting the bees in the packet headers",
	"haggling with the scheduler over lunch",
	"taking dictation from the abandoned sessions",
	"sanding the edge cases",
	"winding the spring of the next outage",
	"re-filing the future under 'maybe'",
	"unlocking the room where the stack traces go",
	"interviewing the echo for a job",
	"cataloguing the sounds the server makes at rest",
	"performing maintenance on the hourglass",
	"re-sequencing the days of the week",
	"mending the nets for dream-catching",
}

// setBgProcesses records the absolute number of background processes pi has
// running, relayed by the pi-processes companion extension, and reflects it in
// presence: while any process runs (and no agent run is in flight) the bot
// shows dnd "background process running" instead of available, and the idle
// watcher must not drift it to "away" mid-build. Absolute counts self-heal — a
// missed started/ended relay is corrected by the next one.
func (b *Bridge) setBgProcesses(n int) {
	b.mu.Lock()
	if n < 0 {
		n = 0
	}
	changed := n != b.bgProcesses
	b.bgProcesses = n
	streaming := b.streamingRun
	b.mu.Unlock()
	b.syncBusyMarker()
	if !changed || b.xmpp == nil {
		return
	}
	if streaming {
		return // a run in flight already forces dnd with its own activity label
	}
	b.announceSettledPresence()
}

// announceSettledPresence sets the presence of an idle agent: available +
// "listening", unless a background process is still running, which keeps the
// bot dnd.
func (b *Bridge) announceSettledPresence() {
	if b.xmpp == nil {
		return
	}
	b.mu.Lock()
	bg := b.bgProcesses
	b.mu.Unlock()
	if bg > 0 {
		noun := "processes"
		if bg == 1 {
			noun = "process"
		}
		b.xmpp.SetPresence("dnd", fmt.Sprintf("waiting on %d %s", bg, noun))
	} else {
		b.xmpp.SetPresence("", "listening")
	}
}

// markIdle records that the agent has settled into an idle, available state;
// the idle clock starts now and the watcher flips presence to "away" after
// idleAwayTimeout of quiet.
func (b *Bridge) markIdle() {
	now := time.Now()
	b.mu.Lock()
	b.idleSince = now
	b.mu.Unlock()
	if b.xmpp != nil {
		b.xmpp.SetIdleSince(now)
	}
}

// markActive clears the idle clock: the agent is working or receiving activity,
// so it should not drift to "away" until it settles again. The XMPP bridge
// drops its XEP-0319 idle stamp, re-announcing active on the next presence.
// awayAnnounced is cleared so the next idle period announces its away status
// afresh.
func (b *Bridge) markActive() {
	b.mu.Lock()
	b.idleSince = time.Time{}
	b.awayAnnounced = false
	b.lastAwayStatus = "" // next away entry picks a fresh activity
	b.mu.Unlock()
	if b.xmpp != nil {
		b.xmpp.SetIdleSince(time.Time{})
	}
}

//go:embed away-activities.txt
var awayActivitiesFile string

// loadAwayActivities replaces the built-in pool with the embedded file's
// contents when present (one activity per line; blank lines and # comments are
// skipped). Called once at startup before the idle watcher starts.
func (b *Bridge) loadAwayActivities() {
	pool := make([]string, 0, 64)
	for _, l := range strings.Split(awayActivitiesFile, "\n") {
		l = strings.TrimSpace(l)
		if l == "" || strings.HasPrefix(l, "#") {
			continue
		}
		pool = append(pool, l)
	}
	if len(pool) == 0 {
		return // keep the built-in slice
	}
	b.mu.Lock()
	awayActivities = pool
	b.mu.Unlock()
}

// idleWatcher flips presence to "away" once the agent has been idle (no run in
// flight, no inbound activity) for idleAwayTimeout, showing a randomized pithy
// activity as the status. The away status is announced exactly once per idle
// period — the transition, not a 30s rotation — and stays put until the next
// inbound message or run brings the agent back to available (see onInbound /
// agent_start). markActive clears the announced flag, so each new away period
// picks a fresh activity.
func (b *Bridge) idleWatcher(ctx context.Context) {
	t := time.NewTicker(30 * time.Second)
	defer t.Stop()
	for {
		select {
		case <-ctx.Done():
			return
		case <-t.C:
			b.idleTick()
		}
	}
}

// idleTick runs the body of one idleWatcher tick: it announces "away" at most
// once per idle period, once the agent has been idle past idleAwayTimeout.
// Split out from idleWatcher so it's callable directly from tests.
func (b *Bridge) idleTick() {
	b.sessionTransitionMu.Lock()
	defer b.sessionTransitionMu.Unlock()
	b.mu.Lock()
	idle := !b.idleSince.IsZero()
	elapsed := time.Since(b.idleSince)
	// Read streamingRun directly rather than via streaming(), which re-locks
	// b.mu and would deadlock this goroutine against itself.
	// Background processes keep the bot dnd (never drift to away mid-build).
	if !idle || b.streamingRun || b.bgProcesses > 0 || elapsed < idleAwayTimeout || b.xmpp == nil || b.awayAnnounced {
		b.mu.Unlock()
		return
	}
	// First tick past the threshold: optionally start a fresh session before
	// announcing away. If reset fails, leave the transition unannounced so the
	// next tick can retry rather than silently keeping stale context.
	if b.acct.ResetSessionOnAway {
		if b.rpc == nil {
			b.mu.Unlock()
			b.log("warning", "resetSessionOnAway: pi RPC unavailable; deferring away transition")
			return
		}
		b.mu.Unlock()
		res, err := b.rpc.NewSession(b.ctx)
		if err != nil {
			b.log("warning", "resetSessionOnAway: new session failed: "+err.Error())
			return
		}
		if !res.success() {
			b.log("warning", "resetSessionOnAway: new session failed: "+res.errText())
			return
		}
		b.refreshSessionFile()
		b.messagingSeeded = false
		b.markFresh()
		b.mu.Lock()
		// The transition mutex prevents inbound activity from changing this
		// idle period while the RPC session swap is in progress.
	}
	// First tick past the threshold: announce away once, picking an
	// activity different from the previous away period's.
	act := awayActivities[rand.Intn(len(awayActivities))]
	for act == b.lastAwayStatus {
		act = awayActivities[rand.Intn(len(awayActivities))]
	}
	b.awayAnnounced = true
	b.lastAwayStatus = act
	b.mu.Unlock()
	b.xmpp.SetPresence("away", act)
}

// --- small state accessors ---

// setReactTarget records which message the next run's agent-driven reactions
// (send_reaction tool) attach to. Called before each prompt and updated by
// explicit sends so agent reactions target its own outgoing messages.
func (b *Bridge) setReactTarget(to, id string) {
	b.mu.Lock()
	b.reactTo, b.reactID = to, id
	b.mu.Unlock()
}

// setLifecycleReactTarget records both the regular react target AND a
// snapshot for lifecycle auto-reacts (👀✅⛔). The lifecycle snapshot is never
// overwritten by a later send, so agent_settled's ✅ always targets the
// original triggering message.
func (b *Bridge) setLifecycleReactTarget(to, id string) {
	b.mu.Lock()
	b.reactTo, b.reactID = to, id
	b.lifecycleReactTo, b.lifecycleReactID = to, id
	b.mu.Unlock()
}

// setTurnDest records the reply destination for the current turn (the owner in
// 1:1, or the room in room mode), used as the default target for a tool-driven
// file send when the agent doesn't name one.
// setTurnDest records the reply destination for the current turn and whether
// that turn was opened by another agent (peer). Taking both here rather than
// adding a second setter is deliberate: every path that names a destination has
// to declare who opened the run, so the untagged-reply warning cannot inherit a
// stale peer flag from the previous turn.
func (b *Bridge) setTurnDest(dest string, peer bool) {
	b.mu.Lock()
	b.turnDest = dest
	b.peerRun = peer
	b.mu.Unlock()
}

// peerTriggered reports whether the current run was opened by another agent's
// addressed message.
func (b *Bridge) peerTriggered() bool {
	b.mu.Lock()
	defer b.mu.Unlock()
	return b.peerRun
}

func (b *Bridge) setUntaggedWarned(v bool) {
	b.mu.Lock()
	b.untaggedWarnedRun = v
	b.mu.Unlock()
}

func (b *Bridge) untaggedWarned() bool {
	b.mu.Lock()
	defer b.mu.Unlock()
	return b.untaggedWarnedRun
}

func (b *Bridge) currentTurnDest() string {
	b.mu.Lock()
	defer b.mu.Unlock()
	return b.turnDest
}

// sendReaction sends a XEP-0444 reaction (emoji set) to the current run's
// target message. No-ops when no target is set (e.g. a room turn, where 1:1
// reaction tracking doesn't apply). Passing no emoji clears the reaction. This
// is the ungated path used for deliberate, agent-driven reactions.
func (b *Bridge) sendReaction(emojis ...string) {
	b.mu.Lock()
	to, id := b.reactTo, b.reactID
	b.mu.Unlock()
	if to == "" || id == "" {
		return
	}
	b.xmpp.SendReaction(to, id, emojis...)
}

// lifecycleReact maps a run-lifecycle beat to a reaction, but only when the
// per-account reactions flag is on — auto-reacting on every run can be noisy,
// so it's opt-in. Deliberate agent-driven reactions go through sendReaction and
// share the same flag gate at their call site.
func (b *Bridge) lifecycleReact(emojis ...string) {
	if !b.acct.Reactions {
		return
	}
	b.mu.Lock()
	if b.reactionAckRun {
		b.mu.Unlock()
		return
	}
	to, id := b.lifecycleReactTo, b.lifecycleReactID
	b.mu.Unlock()
	if to == "" || id == "" {
		return
	}
	b.xmpp.SendReaction(to, id, emojis...)
}

// reactInterrupted marks the incoming quick-interrupt command. Like the
// unanswered-run salute, this acknowledgement is independent of the optional
// lifecycle-reactions setting.
func (b *Bridge) reactInterrupted() {
	b.mu.Lock()
	if b.reactionAckRun {
		b.mu.Unlock()
		return
	}
	to, id := b.lifecycleReactTo, b.lifecycleReactID
	b.mu.Unlock()
	if to == "" || id == "" {
		return
	}
	b.xmpp.SendReaction(to, id, "⏹")
}

// reactNoReply always marks an unanswered run, independent of the optional
// lifecycle-reactions setting. It replaces the completion reaction with 🫡.
func (b *Bridge) reactNoReply() {
	b.mu.Lock()
	if b.reactionAckRun {
		b.mu.Unlock()
		return
	}
	to, id := b.lifecycleReactTo, b.lifecycleReactID
	b.mu.Unlock()
	if to == "" || id == "" {
		return
	}
	b.xmpp.SendReaction(to, id, "🫡")
}

func (b *Bridge) setReactionAckRun(v bool) {
	b.mu.Lock()
	b.reactionAckRun = v
	b.mu.Unlock()
}

func (b *Bridge) setStreaming(v bool) {
	b.mu.Lock()
	b.streamingRun = v
	b.mu.Unlock()
	b.syncBusyMarker()
}

// syncBusyMarker keeps the on-disk busy marker in step with the agent's real
// state: the file is present while a run streams or a background process runs,
// absent once the agent is idle. `deploy-service pi-msg --proactive` reads it
// to decide which accounts to reprompt on resume; an idle agent is left silent.
func (b *Bridge) syncBusyMarker() {
	b.mu.Lock()
	busy := b.streamingRun || b.bgProcesses > 0
	changed := busy != b.busyMarked
	b.busyMarked = busy
	b.mu.Unlock()
	if changed {
		markBusy(b.log, b.acct.Name, busy)
	}
}
func (b *Bridge) streaming() bool   { b.mu.Lock(); defer b.mu.Unlock(); return b.streamingRun }
func (b *Bridge) setReplied(v bool) { b.mu.Lock(); b.repliedThisRun = v; b.mu.Unlock() }
func (b *Bridge) replied() bool     { b.mu.Lock(); defer b.mu.Unlock(); return b.repliedThisRun }

// steerBehavior returns "steer" when a run is already in flight, else "".
func (b *Bridge) steerBehavior() string {
	if b.streaming() {
		return "steer"
	}
	return ""
}

// busyPresence sets the busy presence (<show>=dnd) with the given status label,
// but only when a run is NOT already in flight. When the agent is streaming and
// this prompt is a steer, the status label must stay truthful: the previously
// shown "! <tool>" command is still running and the queued steer isn't read
// until it returns. Flipping to "thinking…" here would claim the agent had
// picked up the message when it hasn't; the label self-corrects from the actual
// agent_* / message_update activity the moment it truly does.
func (b *Bridge) busyPresence(label string) {
	if b.streaming() {
		return // steering an in-flight run — don't overwrite the running-tool status
	}
	b.xmpp.SetPresence("dnd", label)
}

// startLabel renders a human-readable directive value for logs.
func startLabel(v string) string {
	switch v {
	case StartProactive:
		return "proactive"
	case StartIdle:
		return "idle"
	case StartPrompt:
		return "prompt"
	}
	return "auto (idle default)"
}

// formatHeartbeat renders the process-heartbeat alarm for the agent: one line
// per long-running process with its true elapsed time and a log tail. Single
// process → the terse Zack-flavored form; several → batched sections so the
// agent is woken once with everything rather than once per process.
func (b *Bridge) formatHeartbeat(procs []HeartbeatProcess) string {
	if len(procs) == 1 {
		p := procs[0]
		return fmt.Sprintf(
			"[pi-msg: process %q has been running for %d %s now. Here's the log tail:\n\n%s\n\nIf this is unexpected, determine what happened and act. If this is expected, no chat reply is needed; final assistant text is internal.]",
			p.Name, p.ElapsedSecs, heartbeatNoun(p.ElapsedSecs), heartbeatTail(p.Tail),
		)
	}
	var sb strings.Builder
	if len(procs) == 0 {
		return ""
	}
	sb.WriteString("[pi-msg: " + strconv.Itoa(len(procs)) + " background processes have been running for a while. Here they are with log tails:")
	for _, p := range procs {
		fmt.Fprintf(&sb, "\n\n• %s — %d %s\n%s", p.Name, p.ElapsedSecs, heartbeatNoun(p.ElapsedSecs), heartbeatTail(p.Tail))
	}
	sb.WriteString("\n\nIf any of these is unexpected, determine what happened and act. If they are all expected, no chat reply is needed; final assistant text is internal.]")
	return sb.String()
}

// heartbeatNoun pluralizes the elapsed-seconds label; heartbeatTail renders the
// log tail or a placeholder when the process has produced no output yet.
func heartbeatNoun(n int) string {
	if n == 1 {
		return "second"
	}
	return "seconds"
}

func heartbeatTail(tail string) string {
	if strings.TrimSpace(tail) == "" {
		return "(no output yet)"
	}
	return tail
}

// fireHeartbeat wakes the (idle) agent with a long-running-process alarm,
// routing any response to the owner like the other synthetic prompts. The run
// is marked heartbeatRun so its expected private/no-message outcome is treated
// like a volunteer or reaction-ack run: no 🫡 reaction, recovery, or
// unanswered-message hints.
func (b *Bridge) fireHeartbeat(text string) {
	if text == "" {
		return
	}
	b.heartbeatRun = true
	b.setTurnDest(b.acct.Owner, false)
	b.rpc.Prompt(text, b.steerBehavior())
	b.xmpp.SetPresence("dnd", "thinking…")
	b.log("info", "process heartbeat injected")
}

// flushPendingHeartbeats delivers any heartbeats queued while a run was in
// flight (their delivery was deferred rather than dropped). Called on
// agent_settled, when the agent is idle again and safe to wake. Flushing one
// at a time keeps each alarm its own turn; the agent can noop them individually.
func (b *Bridge) flushPendingHeartbeats() {
	b.mu.Lock()
	queued := b.pendingHeartbeats
	b.pendingHeartbeats = nil
	b.mu.Unlock()
	for _, text := range queued {
		b.fireHeartbeat(text)
	}
}

// fireResumeTurn injects a single synthetic prompt so the resumed agent can
// volunteer a line to the owner (start directive "proactive"). If it has
// nothing to volunteer it replies with nothing, and agent_settled stays silent.
func (b *Bridge) fireResumeTurn() {
	b.volunteered = true
	b.setLifecycleReactTarget("", "")
	b.setTurnDest(b.acct.Owner, false)
	b.rpc.Prompt(
		"[pi-msg: startup: your session was resumed (continued from a previous process). "+
			"You may volunteer to continue the conversation or task from the previous session. "+
			"If you have nothing worth volunteering, do not send a chat message; final assistant text is internal.]",
		b.steerBehavior())
	b.xmpp.SetPresence("dnd", "thinking…")
}

// fireInitialPrompt delivers the invocation-time initial prompt (--prompt flag
// or a "prompt" start-directive payload) as the persona's very first prompt,
// so an on-demand spawn arrives with its task baked in (beltino#18). It is
// composed through the normal prompt path: a fresh room-mode session gets the
// messaging contract seed (messagingSeeded is false for a forced-fresh launch), and
// the reply routes to the owner, mirroring fireResumeTurn.
func (b *Bridge) fireInitialPrompt() {
	b.setLifecycleReactTarget("", "")
	b.setTurnDest(b.acct.Owner, false)
	b.rpc.Prompt(b.composePrompt(b.initialPrompt, b.acct.Owner, "", "", "", "", nil), b.steerBehavior())
	b.xmpp.SetPresence("dnd", "thinking…")
}

// onXMPPConnected runs on the XMPP goroutine after the first successful connect
// and presence/room setup. When a restart-gap replay window is armed, it kicks
// off the drain so buffered swap-window messages are handed to the resumed
// session once the grace period elapses.
func (b *Bridge) onXMPPConnected() {
	b.backfillMu.Lock()
	first := !b.connectedOnce
	b.connectedOnce = true
	b.backfillMu.Unlock()
	if !first {
		// Mid-session reconnect: the restart window does not apply, so recover
		// the gap from the archive instead (#94).
		go b.recoverReconnectGap()
		return
	}
	replay := b.replayWindowArmed
	mam := b.mamArmed
	if !replay && !mam && b.inboxLen() == 0 {
		return
	}
	// Clear both synchronously: onConnected fires on every (re)connect, and a
	// fast reconnect must not start a second drain/backfill race.
	b.replayWindowArmed = false
	b.mamArmed = false
	go b.replayInbound(mam)
}

// noteInboundHandled advances the persistent last-inbound cursor to now.
func (b *Bridge) noteInboundHandled() {
	now := time.Now()
	b.mu.Lock()
	b.lastIn = now
	b.mu.Unlock()
	markLastIn(b.log, b.acct.Name, now)
}

// recoverReconnectGap backfills the archive across a mid-session reconnect.
//
// Delayed backlog pushed on reconnect is dropped by the dispatch path unless it
// falls inside the restart swap window (xmpp.go), and the restart backfill runs
// only once per launch — so without this a message sent while the socket was
// down was lost with no trace (issue #94). The lower bound is the last inbound
// the running bridge handled live, so nothing already seen is re-fetched;
// archived copies of messages that did arrive live are skipped by stanza id in
// deliverRecovered. Skipped for on-demand spawns, which start with only their
// task (consistent with the startup replay).
func (b *Bridge) recoverReconnectGap() {
	if !b.acct.MAM || b.initialPrompt != "" {
		return
	}
	lastIn, lastInOK := readLastIn(b.acct.Name)
	seen, seenOK := readMAMSeen(b.acct.Name)
	since, ok := reconnectSince(lastIn, lastInOK, seen, seenOK, time.Now())
	if !ok {
		return
	}
	b.backfillMu.Lock()
	defer b.backfillMu.Unlock()
	ctx, cancel := context.WithTimeout(b.ctx, mamTimeout)
	defer cancel()
	msgs := b.mamFetch(ctx, since)
	if len(msgs) == 0 {
		// Nothing in the gap: move the cursor so the next reconnect queries from
		// here rather than re-walking the same window.
		markMAMSeen(b.log, b.acct.Name, time.Now())
		return
	}
	b.log("info", fmt.Sprintf("reconnect backfill: %d message(s) since %s", len(msgs), since.UTC().Format(time.RFC3339)))
	b.deliverRecovered(msgs, nil)
	markMAMSeen(b.log, b.acct.Name, time.Now())
}

// mamScope is one archive scope for a backfill: the owner 1:1 conversation, or
// one joined room.
type mamScope struct{ room, with, label string }

// mamBackfill fetches archived messages from the server's XEP-0313 archive for
// the owner 1:1 and each joined room, and buffers them for the resumed session
// (issue #84). Best-effort: a server without an archive (no mod_mam) logs a
// warning and the existing delay-stanza replay still runs unaffected. The
// `mamseen` cursor is advanced by the caller only once the buffer has actually
// been handed to the agent (#94).
func (b *Bridge) mamBackfill() {
	if b.mamSince.IsZero() {
		// First launch with MAM enabled: nothing to recover, just start the clock.
		markMAMSeen(b.log, b.acct.Name, time.Now())
		return
	}
	ctx, cancel := context.WithTimeout(b.ctx, mamTimeout)
	defer cancel()
	msgs := b.mamFetch(ctx, b.mamSince)
	for _, m := range msgs {
		b.xmpp.bufferReplay(m)
	}
	b.log("info", fmt.Sprintf("mam backfill: %d message(s) since %s", len(msgs), b.mamSince.UTC().Format(time.RFC3339)))
	b.mamSince = time.Time{}
}

// mamFetch pulls archived messages for every scope (owner 1:1 plus each joined
// room) since `since`, merged in chronological order so a catch-up reads the way
// it was sent. Per-scope failures are logged and skipped rather than failing the
// whole backfill.
func (b *Bridge) mamFetch(ctx context.Context, since time.Time) []InboundMessage {
	scopes := []mamScope{{room: "", with: b.acct.Owner, label: "owner 1:1"}}
	for _, room := range b.acct.Rooms {
		scopes = append(scopes, mamScope{room: room, label: room})
	}
	var all []InboundMessage
	for _, sc := range scopes {
		msgs, complete, err := b.xmpp.FetchMAM(ctx, sc.room, sc.with, since, mamPageMax)
		if err != nil {
			b.log("warning", fmt.Sprintf("mam backfill (%s) failed: %v", sc.label, err))
			continue
		}
		if !complete {
			b.log("warning", fmt.Sprintf("mam backfill (%s) truncated at %d message(s); older history in the window is not recovered", sc.label, len(msgs)))
		}
		all = append(all, msgs...)
	}
	// Delay-stamped archived messages carry their own stamp; order on it so the
	// merged scopes read chronologically (zero-stamp entries keep arrival order).
	sort.SliceStable(all, func(i, j int) bool {
		a, c := all[i].Stamp, all[j].Stamp
		switch {
		case a.IsZero():
			return false
		case c.IsZero():
			return true
		default:
			return a.Before(c)
		}
	})
	return all
}

// deliverRecovered hands a recovered backlog to the resumed session in one
// chronological block: a catch-up banner first, then each buffered/archived
// message through the normal canonical/room path, then any unacknowledged inbox
// entries.
//
// Three properties matter (#94): messages that already arrived live are skipped
// by stanza id; each recovered id is recorded as seen so a later backfill does
// not re-deliver it; and the cursors are advanced by the caller only after the
// hand-off, so a crash mid-delivery re-fetches instead of losing the backlog.
//
// Inbox entries deliberately bypass the seen check (#96): their stanza *was*
// delivered live, in the process that then died, so a recorded `seen` id would
// suppress exactly the message this path exists to recover.
func (b *Bridge) deliverRecovered(msgs []InboundMessage, inboxes []inboxEntry) {
	fresh := make([]InboundMessage, 0, len(msgs))
	for _, m := range msgs {
		if b.xmpp.hasSeen(m.ID) {
			continue
		}
		fresh = append(fresh, m)
	}
	total := len(fresh) + len(inboxes)
	if total == 0 || b.ctx.Err() != nil {
		return
	}
	b.reply(fmt.Sprintf("Back online, catching up on %d messages", total))
	for _, m := range fresh {
		b.xmpp.markSeen(m.ID)
		b.noteInboundHandled()
		if m.Direct {
			b.handleCanonical(m.Body, "", b.acct.Owner, "", m.From, m.ID, b.replyContext(m), nil, m.Markable)
		} else {
			b.handleRoom(m)
		}
	}
	for _, e := range inboxes {
		b.deliverInbox(e)
	}
}

// replayInbound blocks until the replay windows closes, then hands any buffered
// swap-window messages to the resumed session (banner first), followed by a
// deferred proactive volunteer turn. Runs on its own goroutine; the drain
// itself is bounded by the grace period and ctx.
func (b *Bridge) replayInbound(mam bool) {
	// Fetch archive backlog before draining: the drain hands everything to the
	// resumed session in one chronological block, so MAM results must be in the
	// buffer by then (issue #84).
	if mam {
		b.mamBackfill()
	}
	msgs := b.xmpp.DrainReplay(b.ctx)
	// Unacknowledged inbox entries ride in the same catch-up: they are input a
	// stopped process had taken in but no settled run ever consumed (#96), and
	// they come after the buffered messages, which are older.
	b.deliverRecovered(msgs, b.inboxPending())
	// Only a completed MAM walk may move the backfill cursor: the inbox is not
	// the archive, and advancing past a window this launch never fetched would
	// skip it on a later restart.
	if mam {
		markMAMSeen(b.log, b.acct.Name, time.Now())
	}
	if b.volunteerPending {
		b.volunteerPending = false
		b.fireResumeTurn()
	}
}

// inboxNote marks a message re-delivered from the durable inbox. Delivery is
// at-least-once, so the same instruction may already sit in the resumed
// session's context — say so rather than repeating it unexplained.
const inboxNote = "[pi-msg: re-delivered after a restart — this was queued to me before the process stopped and no settled run acknowledged it; it may repeat something already in your context]"

// deliverInbox re-delivers one unacknowledged message after a restart. It goes
// through the normal dispatch path, so room rules (trigger, non-owner
// commentary rules) are applied again exactly as they were the
// first time. The entry stays in the inbox until the run that consumes it
// settles, so a repeated stop cannot lose it; the note is appended rather than
// prepended so a room trigger at the start of the body still matches.
func (b *Bridge) deliverInbox(e inboxEntry) {
	m := e.message()
	if strings.TrimSpace(m.Body) == "" {
		b.inboxDrop(m.ID, m.From, m.Body)
		return
	}
	m.Body = strings.TrimSpace(m.Body) + "\n\n" + inboxNote
	if m.Direct {
		b.handleCanonical(m.Body, "", b.acct.Owner, "", m.From, m.ID, b.replyContext(m), nil, m.Markable)
		return
	}
	b.handleRoom(m)
}

// ackInboxSettled acknowledges the messages the run that just ended took in: the
// ones it delivered and then read, and — as a backstop — any that have been
// pending longer than inboxAckGrace without ever being delivered. An entry
// handed to pi that the run never read (a steer pi did not yield, or a message
// that arrived as the run ended) stays pending for the next run or the next
// start (#96, #104).
func (b *Bridge) ackInboxSettled() {
	if b.inbox == nil {
		return
	}
	p := ackPolicy{Now: time.Now(), LastActive: b.lastRunActivity()}
	b.log("info", fmt.Sprintf("inbox: run settled; pending=%d", b.inbox.len()))
	if n := b.inbox.ackSettled(p); n > 0 {
		b.log("info", fmt.Sprintf("inbox: acknowledged %d message(s)", n))
	}
}

// inboxMarkDelivered tells the durable queue that pi has been handed this
// message. The entry is not dropped: it is acknowledged when the run that took
// it in settles, so a run that dies first still gets it re-delivered (#96).
func (b *Bridge) inboxMarkDelivered(id string) {
	if b.inbox == nil || id == "" {
		return
	}
	if n := b.inbox.markDelivered(id, "", "", time.Now()); n > 0 {
		b.log("info", fmt.Sprintf("inbox: prompt dispatch starting stanza_id=%q", id))
	}
}

// inboxDrop removes a message that will never become a prompt — an unaddressed
// room message, a bridge command, a dropped own-echo. No run will settle for
// it, so waiting for an ack would strand it until the next restart re-delivered
// it as "unacknowledged" (#104).
func (b *Bridge) inboxDrop(id, from, body string) {
	if b.inbox == nil {
		return
	}
	b.inbox.drop(id, from, body)
}

// inboxLen reports how many messages are awaiting acknowledgement.
func (b *Bridge) inboxLen() int {
	if b.inbox == nil {
		return 0
	}
	return b.inbox.len()
}

// inboxPending copies the unacknowledged messages, in arrival order.
func (b *Bridge) inboxPending() []inboxEntry {
	if b.inbox == nil {
		return nil
	}
	return b.inbox.pending()
}

// inboxAppend records an inbound message before it is handed to pi. Called from
// onInbound: one hook, ahead of every prompt path (direct, room canonical and
// commentary), while the message is still in hand.
func (b *Bridge) inboxAppend(m InboundMessage) {
	if b.inbox == nil || b.initialPrompt != "" {
		return // a stateless doer starts with only its task, like the replay paths
	}
	b.inbox.append(inboxEntry{
		ID:        m.ID,
		From:      m.From,
		Body:      m.Body,
		Room:      m.Room,
		Nick:      m.Nick,
		RealJID:   m.RealJID,
		FromOwner: m.FromOwner,
		Direct:    m.Direct,
		Addressed: true,
		ReplyToID: m.ReplyToID,
		Markable:  m.Markable,
		At:        time.Now(),
	})
}

// --- pure helpers ---

// extractText pulls the plain-text portion out of an assistant message's
// content, which is either a string or an array of typed content blocks.
func extractText(content any) string {
	switch c := content.(type) {
	case string:
		return strings.TrimSpace(c)
	case []any:
		var parts []string
		for _, item := range c {
			m, ok := item.(map[string]any)
			if !ok {
				continue
			}
			if m["type"] == "text" {
				if s, ok := m["text"].(string); ok {
					parts = append(parts, s)
				}
			}
		}
		return strings.TrimSpace(strings.Join(parts, "\n"))
	}
	return ""
}

// splitCommand splits "/name arg..." into a lowercased name and trimmed arg.
// splitCommand splits "/name arg..." or "!name arg..." into a lowercased
// name and trimmed arg. "!" is a full alias for "/" on bridged commands.
func splitCommand(t string) (name, arg string) {
	body := strings.TrimPrefix(t, "/")
	body = strings.TrimPrefix(body, "!")
	if sp := strings.IndexByte(body, ' '); sp >= 0 {
		return strings.ToLower(body[:sp]), strings.TrimSpace(body[sp+1:])
	}
	return strings.ToLower(body), ""
}

// matchModel finds the first available model whose "provider/id" contains the
// query (case-insensitive), from a get_available_models response.
func matchModel(res Event, query string) (provider, id string, ok bool) {
	data := res.Obj("data")
	if data == nil {
		return "", "", false
	}
	models, _ := data["models"].([]any)
	q := strings.ToLower(query)
	for _, m := range models {
		mm, ok := m.(map[string]any)
		if !ok {
			continue
		}
		p, _ := mm["provider"].(string)
		i, _ := mm["id"].(string)
		if strings.Contains(strings.ToLower(p+"/"+i), q) {
			return p, i, true
		}
	}
	return "", "", false
}

func orUnknown(s string) string {
	if s == "" {
		return "unknown"
	}
	return s
}

// describeExtensionError names the extension that threw, for the "in <path> (on
// <event>)" clause of an extension_error report (#135). pi sends
// {extensionPath, event, error}: the error alone says what broke, and the two
// extra fields are the only way to say who broke it. Either may be absent, so
// each half is dropped rather than filled with "unknown" — a path-less report
// should read as one clause short, not as a path literally named unknown.
func describeExtensionError(ev Event) string {
	path, event := ev.Str("extensionPath"), ev.Str("event")
	switch {
	case path == "" && event == "":
		return ""
	case path == "":
		return " (on " + event + ")"
	case event == "":
		return " in " + path
	}
	return " in " + path + " (on " + event + ")"
}
