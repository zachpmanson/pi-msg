package pimsg

import (
	"fmt"
	"io"
	"path/filepath"
	"strings"
	"sync"
	"sync/atomic"
	"time"
)

// nextMu serializes admission and release, not lifecycle processing. No RPC
// request may be made while holding it or sessionTransitionMu.
type nextQueue struct {
	mu          sync.Mutex
	record      *nextBoundaryRecord
	wake        chan struct{}
	started     bool
	executing   bool
	supersede   bool
	submitted   map[string]bool
	fenced      atomic.Bool // event consumers must never wait for admission/disk IO
	quarantined atomic.Bool // historical ordinary controls must not age-ack
}

func (b *Bridge) nextPath() string {
	return strings.TrimSuffix(inboxPath(b.acct.Name), ".inbox.jsonl") + ".next.json"
}

// Historical releases used to block on Pi's delayed first session-file write.
// Only this exact diagnostic is eligible for automatic post-target recovery.
const nextAcceptedTargetNotDurable = "accepted target has no usable exact resume pointer"

func (b *Bridge) nextResumeUsable(r *nextBoundaryRecord) bool {
	return r.FreshSessionID != "" && r.FreshSessionFile != "" &&
		loadSessionState(b.acct.Name) == r.FreshSessionFile && sessionFileUsable(r.FreshSessionFile)
}

func (b *Bridge) loadNextQueue() error {
	r, err := loadNextBoundary(b.nextPath())
	if err != nil {
		return err
	}
	if r != nil && b.inbox.loadError() != nil {
		return b.inbox.loadError()
	}
	entries := b.inbox.protectedPending()
	if len(entries) > 0 && b.inbox.loadError() != nil {
		return b.inbox.loadError()
	}
	if r == nil && len(entries) > 0 {
		first := entries[0]
		prev := loadSessionState(b.acct.Name)
		if first.NextBoundaryID == "" || first.TargetSessionID == "" || !sessionFileUsable(prev) || b.initialPrompt != "" {
			return fmt.Errorf("orphan protected inbox requires assigned exact target and usable resume pointer")
		}
		// This extra proof belongs only to the retired automatic boundary.
		// A later manual /next has its own exact-target orphan authority; a
		// historical blocked auto attempt must not override that newer boundary.
		if a := b.auto.state.Attempt; a != nil && a.Error == autoRetiredFenceRecovered && first.NextBoundaryID == a.Token && (first.TargetSessionID != a.TargetID || prev != a.TargetFile) {
			return fmt.Errorf("retired automatic fence orphan does not match proven exact target")
		}
		for _, e := range entries {
			if e.NextBoundaryID != first.NextBoundaryID || e.TargetSessionID != first.TargetSessionID {
				return fmt.Errorf("mixed orphan protected targets/boundaries; refusing recovery")
			}
		}
		// Persist the replay-only fence before admitting new unassigned input;
		// a crash before assignment must retain its exact target authority.
		// releaseNext still proves actual RPC identity; this phase can never
		// reset or authorize a reconciliation worker.
		r = &nextBoundaryRecord{Version: nextBoundaryVersion, ID: first.NextBoundaryID, Command: "exact-target-replay", AcceptedAt: time.Now(), Phase: nextBoundaryReleasing, FreshSessionID: first.TargetSessionID, FreshSessionFile: prev, Destination: b.acct.Owner}
		if err := saveNextBoundary(b.nextPath(), r); err != nil {
			return err
		}
	}
	for _, e := range b.inbox.pending() {
		if !e.protected() && historicalBoundaryControl(e.message(), b) {
			b.next.quarantined.Store(true)
		}
	}
	if r != nil && r.Phase == nextBoundaryBlocked && r.LastError == nextAcceptedTargetNotDurable && b.initialPrompt == "" && b.nextResumeUsable(r) {
		// This is replay authority only. releaseNext must still prove the active
		// RPC identity/file before assignment, and never resets or opens a job gate.
		r.Phase, r.LastError = nextBoundaryReleasing, ""
		if err := saveNextBoundary(b.nextPath(), r); err != nil {
			return err
		}
	}
	if r != nil && (r.Phase == nextBoundaryResetConfirmed || r.Phase == nextBoundaryReleasing) && len(entries) > 0 && !b.nextResumeUsable(r) {
		// After a crash an unmaterialized target cannot be resumed safely. Do
		// not replay into a generic fresh fallback; retain the boundary and input.
		return fmt.Errorf("pending /next requires usable exact target resume pointer")
	}
	if r != nil && r.Phase != nextBoundaryResetConfirmed && r.Phase != nextBoundaryReleasing && r.Phase != nextBoundaryBlocked {
		r.Phase = nextBoundaryBlocked
		r.LastError = "interrupted before proven target; inspect pending boundary and job artifacts; not automatically retried"
		if err := saveNextBoundary(b.nextPath(), r); err != nil {
			return err
		}
	}
	b.next.record = r
	b.next.fenced.Store(r != nil)
	return nil
}

func (b *Bridge) nextPending() bool {
	b.next.mu.Lock()
	defer b.next.mu.Unlock()
	return b.next.record != nil
}

func (b *Bridge) wakeNext() {
	// Event handlers must not wait for durable admission/release writes. A
	// coalesced wake can be skipped; the bounded read-only poll covers it.
	if !b.next.mu.TryLock() {
		return
	}
	if b.next.wake != nil {
		select {
		case b.next.wake <- struct{}{}:
		default:
		}
	}
	b.next.mu.Unlock()
}

func (b *Bridge) startNextQueue() {
	b.next.mu.Lock()
	if b.next.started {
		b.next.mu.Unlock()
		return
	}
	b.next.started = true
	b.next.wake = make(chan struct{}, 1)
	wake := b.next.wake
	b.next.mu.Unlock()
	go func() {
		ticker := time.NewTicker(time.Second) // read-only compaction/pending-state polling
		defer ticker.Stop()
		for {
			select {
			case <-b.ctx.Done():
				return
			case <-wake:
			case <-ticker.C:
			}
			b.executeNextQueue()
		}
	}()
	b.wakeNext()
}

func boundaryCommand(m InboundMessage, b *Bridge) (string, string, bool) {
	text := m.Body
	trusted := m.Direct || m.FromOwner
	if !m.Direct {
		action, stripped, _ := b.classify(m)
		text = stripped
		trusted = action == actionCanonical
	}
	t := strings.TrimSpace(text)
	if !trusted || !(strings.HasPrefix(t, "/") || strings.HasPrefix(t, "!") || t == "&") {
		return "", "", false
	}
	name, arg := splitCommand(t)
	if t == "&" {
		name = "next"
	}
	if t == "!" {
		name = "interrupt"
	}
	return name, arg, true
}

// Called after permission/addressing classification, before ANY old-turn state
// mutation. It also consumes pending native controls without redirecting replies.
type nextAdmission int

const (
	nextNotHeld nextAdmission = iota
	nextControlConsumed
	nextDurablyHeld
	nextHoldFailed
)

func (b *Bridge) admitNext(m InboundMessage) bool {
	return b.admitNextResult(m) != nextNotHeld
}

func (b *Bridge) admitNextResult(m InboundMessage) nextAdmission {
	name, arg, control := boundaryCommand(m, b)
	dest := m.Room
	if m.Direct {
		dest = b.acct.Owner
	}
	b.next.mu.Lock()
	defer b.next.mu.Unlock()
	r := b.next.record
	if name == "next" && control {
		if arg != "" {
			b.sendReconcileNotice(dest, "/next: takes no arguments")
			return nextControlConsumed
		}
		if r == nil {
			if b.initialPrompt != "" || b.inbox == nil {
				b.sendReconcileNotice(dest, "/next: durable queue unavailable in stateless mode")
				return nextControlConsumed
			}
			if err := b.inbox.loadError(); err != nil {
				b.sendReconcileNotice(dest, "/next: "+err.Error())
				return nextControlConsumed
			}
			// Fence age acknowledgement before the durable boundary write.
			b.next.fenced.Store(true)
			var err error
			r, err = newNextBoundaryRecord(dest, strings.TrimSpace(m.Body), time.Now())
			if err == nil {
				err = saveNextBoundary(b.nextPath(), r)
			}
			if err != nil {
				if r != nil {
					r.Phase = nextBoundaryBlocked
					r.LastError = "boundary persistence failed/uncertain: " + err.Error()
					b.next.record = r
				}
				b.sendReconcileNotice(dest, "/next: could not persist boundary; holding future chat until repair: "+err.Error())
				return nextControlConsumed
			}
			b.next.record = r
		}
		b.sendReconcileNotice(r.Destination, fmt.Sprintf("/next: boundary %s %s; wait for active work to settle; later chat held for the new session. %s", r.ID, r.Phase, r.LastError))
		if b.next.wake != nil {
			select {
			case b.next.wake <- struct{}{}:
			default:
			}
		}
		return nextControlConsumed
	}
	if r == nil {
		return nextNotHeld
	}
	if control {
		switch name {
		case "quit", "exit":
			// Existing shutdown is allowed; durable input and boundary are untouched.
			go b.shutdown("requested over chat; pending /next retained")
			return nextControlConsumed
		case "new":
			if r.Command == "automatic-idle-recovery" && r.FreshSessionID == "" {
				b.sendReconcileNotice(dest, "/new: automatic native switch outcome is unknown; input remains protected until exact session recovery; no reset replay")
				return nextControlConsumed
			}
			targeted := r.FreshSessionID != ""
			for _, e := range b.inbox.protectedPending() {
				targeted = targeted || e.TargetSessionID != ""
			}
			if b.next.executing || targeted {
				b.sendReconcileNotice(dest, "/new: pending /next is executing or already target-bound; cannot safely supersede/retarget input; inspect boundary artifacts")
				return nextControlConsumed
			}
			b.next.supersede = true
			b.sendReconcileNotice(dest, "/new: lossless supersession requested; held input retained until a distinct durable target is proven")
			if b.next.wake != nil {
				select {
				case b.next.wake <- struct{}{}:
				default:
				}
			}
			return nextControlConsumed
		case "abort", "stop", "interrupt", "compact", "think", "model", "models", "name", "session", "dump", "dump-all", "dumpall", "export":
			b.sendReconcileNotice(dest, fmt.Sprintf("/%s: refused while /next boundary %s is %s; held input retained. Pending control/recovery policy is not yet enabled.", name, r.ID, r.Phase))
			return nextControlConsumed
		}
	}
	// The protected append is the admission commit, never an ordinary old steer.
	for _, e := range b.inbox.protectedPending() {
		if sameProtectedStanza(e, m) {
			return nextDurablyHeld
		}
	}
	_, err := b.inbox.appendProtected(inboxEntry{NextBoundaryID: r.ID, ID: m.ID, From: m.From, Body: m.Body, Room: m.Room, Nick: m.Nick, RealJID: m.RealJID, FromOwner: m.FromOwner, Direct: m.Direct, Addressed: true, ReplyToID: m.ReplyToID, Markable: m.Markable, At: time.Now()})
	if err != nil {
		// Never fall through to the old session, including oversized records.
		b.blockNext(r, fmt.Errorf("message append failed/uncertain: %w", err))
		b.sendReconcileNotice(dest, "/next: message NOT accepted durably; retain and resend it after repair: "+err.Error())
		return nextHoldFailed
	}
	if b.next.wake != nil {
		select {
		case b.next.wake <- struct{}{}:
		default:
		}
	}
	return nextDurablyHeld
}

func sameProtectedStanza(e inboxEntry, m InboundMessage) bool {
	conversation := func(room, from string, direct bool) string {
		if !direct {
			return bareJid(room)
		}
		return bareJid(from)
	}
	sender := func(from, real, nick string) string {
		if from != "" {
			return from
		}
		if real != "" {
			return real
		}
		return nick
	}
	return m.ID != "" && e.ID == m.ID &&
		conversation(e.Room, e.From, e.Direct) == conversation(m.Room, m.From, m.Direct) &&
		sender(e.From, e.RealJID, e.Nick) == sender(m.From, m.RealJID, m.Nick)
}

func historicalBoundaryControl(m InboundMessage, b *Bridge) bool {
	name, _, control := boundaryCommand(m, b)
	return control && (name == "next" || name == "new" || name == "abort" || name == "stop" || name == "interrupt")
}

func (b *Bridge) nextBusyLocked() bool {
	return b.shuttingDown || b.streamingRun || b.settlingRun || b.syntheticPrompts > 0 || len(b.pendingPrompts) > 0 || len(b.pendingHeartbeats) > 0 || b.bgProcesses > 0 || b.bgSubagents > 0 || b.nextTransition
}
func stateIdentity(s Event) string {
	if id := s.Str("sessionId"); id != "" {
		return "id:" + id
	}
	if f := s.Str("sessionFile"); f != "" && filepath.IsAbs(f) {
		return "file:" + f
	}
	return ""
}
func (b *Bridge) blockNext(r *nextBoundaryRecord, err error) {
	r.Phase, r.LastError = nextBoundaryBlocked, err.Error()
	if e := saveNextBoundary(b.nextPath(), r); e != nil {
		r.LastError += "; persist blocked state: " + e.Error()
	}
	b.sendReconcileNotice(r.Destination, "/next: blocked; held input retained: "+r.LastError)
}

func (b *Bridge) executeNextQueue() {
	b.next.mu.Lock()
	r := b.next.record
	if r == nil || b.next.executing || (r.Phase == nextBoundaryBlocked && !b.next.supersede) {
		b.next.mu.Unlock()
		return
	}
	b.next.executing = true
	supersede := b.next.supersede
	b.next.supersede = false
	defer func() { b.next.mu.Lock(); b.next.executing = false; b.next.mu.Unlock() }()
	phase := r.Phase
	b.mu.Lock()
	promptErr := b.protectedPromptError
	b.protectedPromptError = ""
	b.mu.Unlock()
	if promptErr != "" {
		b.blockNext(r, fmt.Errorf("%s", promptErr))
		b.next.mu.Unlock()
		return
	}
	b.next.mu.Unlock()
	// Older exact-target input is never reassigned to a later boundary.
	for _, e := range b.inbox.protectedPending() {
		if e.NextBoundaryID != r.ID {
			b.next.mu.Lock()
			if r.LastError == "" {
				r.LastError = fmt.Sprintf("older exact-target input %s unresolved; waiting for consumption in its original session before reset", e.Key)
				if err := saveNextBoundary(b.nextPath(), r); err != nil {
					b.blockNext(r, err)
				} else {
					b.sendReconcileNotice(r.Destination, "/next: waiting; "+r.LastError)
				}
			}
			b.next.mu.Unlock()
			return
		}
	}
	if supersede {
		b.supersedeNext(r)
		return
	}
	if phase == nextBoundaryResetConfirmed || phase == nextBoundaryReleasing {
		b.releaseNext(r)
		return
	}
	for _, e := range b.inbox.pending() {
		if !e.protected() {
			// Keep waiting: a proven old settle may still acknowledge this input.
			b.next.mu.Lock()
			if r.LastError == "" {
				r.LastError = "unresolved ordinary old-session inbox; waiting for proven old consumption or explicit /new (discards old primary work)"
				if err := saveNextBoundary(b.nextPath(), r); err != nil {
					b.blockNext(r, err)
				} else {
					b.sendReconcileNotice(r.Destination, "/next: waiting; "+r.LastError)
				}
			}
			b.next.mu.Unlock()
			return
		}
	}
	b.mu.Lock()
	busy := b.nextBusyLocked()
	b.mu.Unlock()
	if busy {
		return
	}
	// The response is observed in lifecycle order, not merely reader order.
	res, err := b.rpc.observedState(b.ctx)
	if err != nil || !res.success() {
		b.next.mu.Lock()
		b.blockNext(r, fmt.Errorf("state observation failed: %v %s", err, res.errText()))
		b.next.mu.Unlock()
		return
	}
	state := res.Obj("data")
	if state.Bool("isStreaming") || state.Bool("isCompacting") || state.F64("pendingMessageCount") > 0 {
		return
	}
	b.sessionTransitionMu.Lock()
	b.mu.Lock()
	busy = b.nextBusyLocked()
	if !busy {
		b.nextTransition = true
	}
	b.mu.Unlock()
	b.sessionTransitionMu.Unlock()
	if busy {
		return
	}
	defer func() { b.mu.Lock(); b.nextTransition = false; b.mu.Unlock() }()
	b.next.mu.Lock()
	r.OldSessionID, r.OldSessionFile = stateIdentity(state), state.Str("sessionFile")
	r.Phase = nextBoundaryPreparing
	err = saveNextBoundary(b.nextPath(), r)
	b.next.mu.Unlock()
	fail := func(e error) { b.next.mu.Lock(); b.blockNext(r, e); b.next.mu.Unlock() }
	if err != nil {
		fail(err)
		return
	}
	if r.OldSessionID == "" {
		fail(fmt.Errorf("source identity missing"))
		return
	}
	dir, j, err := prepareReconcile(b.acct, state, r.Destination, b.rpc.bin)
	if err != nil {
		fail(err)
		return
	}
	gate, err := startReconcile(dir)
	if err != nil {
		j.State, j.Summary = "failed", err.Error()
		_ = saveReconcile(dir, j)
		fail(err)
		return
	}
	defer gate.Close()
	b.next.mu.Lock()
	r.JobDir, r.Phase = dir, nextBoundaryResetting
	err = saveNextBoundary(b.nextPath(), r)
	b.next.mu.Unlock()
	if err != nil {
		fail(err)
		return
	}
	res, err = b.rpc.NewSession(b.ctx)
	if err != nil || !res.success() || res.Obj("data").Bool("cancelled") {
		_, _ = io.WriteString(gate, "cancel\n")
		fail(fmt.Errorf("reset failed/cancelled/uncertain: %v %s", err, res.errText()))
		return
	}
	res, err = b.rpc.observedState(b.ctx)
	if err != nil || !res.success() {
		fail(fmt.Errorf("reset target observation failed: %v", err))
		return
	}
	fresh := res.Obj("data")
	id := stateIdentity(fresh)
	if id == "" || id == r.OldSessionID || fresh.Str("sessionFile") == "" || fresh.Str("sessionFile") == r.OldSessionFile {
		fail(fmt.Errorf("reset target identity not proven distinct"))
		return
	}
	// Persist the exact resume pointer before any target assignment or submission.
	if err = durableWritePrivate(sessionStatePath(b.acct.Name), []byte(fresh.Str("sessionFile")+"\n")); err != nil {
		fail(err)
		return
	}
	b.next.mu.Lock()
	r.FreshSessionID, r.FreshSessionFile, r.Phase = id, fresh.Str("sessionFile"), nextBoundaryResetConfirmed
	err = saveNextBoundary(b.nextPath(), r)
	b.next.mu.Unlock()
	if err != nil {
		fail(err)
		return
	}
	if err = authorizeReconcile(dir); err != nil {
		fail(fmt.Errorf("target proven but worker not authorized: %w", err))
		return
	}
	b.settleLocally()
	b.mu.Lock()
	b.sessionID = id
	b.mu.Unlock()
	b.sessionFile = fresh.Str("sessionFile")
	b.messagingSeeded = false
	b.markFresh()
	b.reportReconcileQueued(dir, j)
	if _, err = io.WriteString(gate, "run\n"); err != nil {
		b.sendReconcileNotice(r.Destination, "/next: target ready but worker gate failed: "+err.Error())
	}
	// Release needs prompt submission enabled, while the admission fence stays up.
	b.mu.Lock()
	b.nextTransition = false
	b.mu.Unlock()
	b.releaseNext(r)
}

func (b *Bridge) releaseNext(r *nextBoundaryRecord) {
	res, err := b.rpc.observedState(b.ctx)
	if err != nil || !res.success() || stateIdentity(res.Obj("data")) != r.FreshSessionID || res.Obj("data").Str("sessionFile") != r.FreshSessionFile {
		b.next.mu.Lock()
		b.blockNext(r, fmt.Errorf("saved target does not match active session; inspect artifacts"))
		b.next.mu.Unlock()
		return
	}
	b.next.mu.Lock()
	defer b.next.mu.Unlock()
	if r.FreshSessionID == "" || r.FreshSessionFile == "" || loadSessionState(b.acct.Name) != r.FreshSessionFile {
		b.blockNext(r, fmt.Errorf("target resume pointer missing or mismatched; inspect artifacts"))
		return
	}
	b.mu.Lock()
	b.sessionID = r.FreshSessionID
	b.mu.Unlock()
	for _, e := range b.inbox.pending() {
		if !e.protected() {
			b.blockNext(r, fmt.Errorf("ordinary input remains after target reset; original retained, inspect old-session provenance before recovery"))
			return
		}
	}
	if r.Phase != nextBoundaryReleasing {
		r.Phase = nextBoundaryReleasing
		if err = saveNextBoundary(b.nextPath(), r); err != nil {
			b.blockNext(r, err)
			return
		}
	}
	if _, err = b.inbox.assignTargetSession(r.ID, r.FreshSessionID); err != nil {
		b.blockNext(r, err)
		return
	}
	b.mu.Lock()
	awaiting := len(b.protectedPrompts) > 0
	promptErr := b.protectedPromptError
	b.mu.Unlock()
	if promptErr != "" {
		b.blockNext(r, fmt.Errorf("%s", promptErr))
		return
	}
	if awaiting {
		return // asynchronous native preflight: never submit its successor yet
	}
	for _, e := range b.inbox.protectedPending() {
		if e.NextBoundaryID != r.ID || e.TargetSessionID != r.FreshSessionID {
			b.blockNext(r, fmt.Errorf("foreign protected entry %s", e.Key))
			return
		}
		// A successful correlated preflight marks deliveredAt. A pipe write alone
		// is not admission evidence, and may not release the successor.
		if b.next.submitted[e.Key] || !e.deliveredAt.IsZero() {
			continue
		}
		if !b.deliverProtected(e) {
			b.blockNext(r, fmt.Errorf("target submission failed for %s", e.Key))
			return
		}
		if b.next.submitted == nil {
			b.next.submitted = make(map[string]bool)
		}
		b.next.submitted[e.Key] = true
		return
	}
	// All held prompts have been accepted, not necessarily consumed. Durable
	// target tags survive retirement and support exact-target replay on restart.
	if len(b.inbox.protectedPending()) > 0 && loadSessionState(b.acct.Name) != r.FreshSessionFile {
		b.blockNext(r, fmt.Errorf("target resume pointer missing or mismatched; inspect artifacts"))
		return
	}
	if len(b.inbox.protectedPending()) > 0 && !sessionFileUsable(r.FreshSessionFile) {
		// Pi may acknowledge a prompt before persisting its session (normally
		// at first assistant output). Keep the admission fence and exact target;
		// ordered observations/polls retry only release, not accepted prompts,
		// reset, or reconciliation. New arrivals still join this target's FIFO.
		return
	}
	if err = removeNextBoundary(b.nextPath()); err != nil {
		b.blockNext(r, err)
		return
	}
	b.next.record = nil
	b.next.submitted = nil
	b.next.fenced.Store(false)
}

func (b *Bridge) deliverProtected(e inboxEntry) bool {
	m := e.message()
	body := m.Body
	origin, sender := b.acct.Owner, ""
	var notice *roomNotice
	peer := false
	if !m.Direct {
		action, stripped, _ := b.classify(m)
		if action == actionNotOurs {
			return false
		}
		body = stripped
		origin = m.Room
		sender = m.RealJID
		peer = action == actionCommentary
		notice = b.roomNoticeFor(m)
	}
	b.setReactionAckRun(false)
	b.setTurnDest(origin, peer)
	reactTo := m.From
	if !m.Direct {
		reactTo = ""
		if b.acct.ReactionsFor(m.Room) {
			reactTo = m.Room
		}
	}
	b.setLifecycleReactTarget(reactTo, m.ID)
	b.countInbound(senderName(m.Nick, sender, origin), m.ID, strings.TrimSpace(body))
	prompt := b.composePrompt(strings.TrimSpace(body), origin, sender, m.ID, reactTo, b.replyContext(m), notice)
	if m.Direct && m.Markable {
		b.addPendingMarker(prompt, m.From, m.ID)
	}
	// Native acceptance can precede agent_start, leaving local busy flags idle.
	// Explicit steer is also valid when Pi is idle and safely queues the FIFO
	// successor if Pi is already processing the previously accepted prompt.
	if !b.submitProtectedPrompt(prompt, "steer", e) {
		return false
	}
	return true
}

// Written-prompt correlation retains rejected protected input without inventing
// consumption evidence. Lifecycle handling remains on the ordered event stream.
type protectedPrompt struct {
	key, session string
	at           time.Time
}

func (b *Bridge) submitProtectedPrompt(text, behavior string, e inboxEntry) bool {
	b.mu.Lock()
	b.promptSequence++
	id := fmt.Sprintf("bridge-prompt-%d", b.promptSequence)
	if b.pendingPrompts == nil {
		b.pendingPrompts = make(map[string]struct{})
	}
	if b.protectedPrompts == nil {
		b.protectedPrompts = make(map[string]protectedPrompt)
	}
	b.pendingPrompts[id] = struct{}{}
	b.protectedPrompts[id] = protectedPrompt{e.Key, e.TargetSessionID, time.Now()}
	b.mu.Unlock()
	b.syncBusyMarker()
	if err := b.rpc.promptWithID(id, text, behavior); err != nil {
		b.finishPromptAcceptance(id, true)
		return false
	}
	return true
}

// Explicit owner recovery only: never reopens a reconciliation gate, never
// retargets previously assigned input, and never retries an uncertain reset.
func (b *Bridge) supersedeNext(r *nextBoundaryRecord) {
	b.sessionTransitionMu.Lock()
	b.mu.Lock()
	b.nextTransition = true
	active := b.streamingRun
	b.mu.Unlock()
	b.sessionTransitionMu.Unlock()
	defer func() { b.mu.Lock(); b.nextTransition = false; b.mu.Unlock() }()
	fail := func(err error) { b.next.mu.Lock(); b.blockNext(r, err); b.next.mu.Unlock() }
	if active {
		b.rpc.Abort()
	}
	b.settleLocally()
	// Explicit /new retains its existing destructive old-primary-queue semantics,
	// but never clears post-boundary protected input.
	if n, err := b.inbox.clearOrdinary(); err != nil {
		fail(err)
		return
	} else if n > 0 {
		b.next.quarantined.Store(false)
		b.sendReconcileNotice(r.Destination, fmt.Sprintf("/new: discarded %d unresolved OLD primary inbox entries; all post-boundary input retained", n))
	}
	state, err := b.rpc.observedState(b.ctx)
	if err != nil || !state.success() || stateIdentity(state.Obj("data")) == "" {
		fail(fmt.Errorf("/new source identity unavailable: %v", err))
		return
	}
	b.next.mu.Lock()
	r.OldSessionID, r.OldSessionFile = stateIdentity(state.Obj("data")), state.Obj("data").Str("sessionFile")
	r.Phase = nextBoundaryResetting
	r.LastError = "explicit /new supersedes unexecuted reconciliation; worker not restarted"
	err = saveNextBoundary(b.nextPath(), r)
	b.next.mu.Unlock()
	if err != nil {
		fail(err)
		return
	}
	res, err := b.rpc.NewSession(b.ctx)
	if err != nil || !res.success() || res.Obj("data").Bool("cancelled") {
		fail(fmt.Errorf("/new reset failed/cancelled/uncertain: %v %s", err, res.errText()))
		return
	}
	res, err = b.rpc.observedState(b.ctx)
	fresh := res.Obj("data")
	id := stateIdentity(fresh)
	if err != nil || !res.success() || id == "" || id == r.OldSessionID || fresh.Str("sessionFile") == "" || fresh.Str("sessionFile") == r.OldSessionFile {
		fail(fmt.Errorf("/new target not proven distinct: %v", err))
		return
	}
	if err = durableWritePrivate(sessionStatePath(b.acct.Name), []byte(fresh.Str("sessionFile")+"\n")); err != nil {
		fail(err)
		return
	}
	b.next.mu.Lock()
	r.FreshSessionID, r.FreshSessionFile, r.Phase = id, fresh.Str("sessionFile"), nextBoundaryResetConfirmed
	err = saveNextBoundary(b.nextPath(), r)
	b.next.mu.Unlock()
	if err != nil {
		fail(err)
		return
	}
	b.mu.Lock()
	b.sessionID = id
	b.mu.Unlock()
	b.sessionFile = fresh.Str("sessionFile")
	b.messagingSeeded = false
	b.markFresh()
	b.sendReconcileNotice(r.Destination, "🆕 /new session ready; pending reconciliation superseded, held input retained for this target")
	b.mu.Lock()
	b.nextTransition = false
	b.mu.Unlock()
	b.releaseNext(r)
}
