package pimsg

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"time"
)

type autoAttempt struct {
	Token, Phase, SourceID, SourceFile, SnapshotHash, Fingerprint, Job, Seed, TargetID, TargetFile, TargetHash, ReportID, ReportText, Error string
	Generation                                                                                                                              uint64
	State                                                                                                                                   Event
}
type autoState struct {
	Version    int
	Generation uint64
	History    autoHistory
	Attempt    *autoAttempt
}
type autoReconcile struct {
	mu      sync.Mutex
	state   autoState
	running bool
	ready   bool
	err     error
}

func (b *Bridge) autoPath() string {
	return strings.TrimSuffix(inboxPath(b.acct.Name), ".inbox.jsonl") + ".auto.json"
}
func (b *Bridge) autoRequestPath() string { return b.autoPath() + ".request" }

const autoOverflowDiagnostic = "autoreconcile state exceeds private 2 MiB byte cap; automatic cleanup disabled until explicit history reconciliation and repair"

var errAutoOverflow = errors.New(autoOverflowDiagnostic)

func (b *Bridge) autoDisabledPath() string { return b.autoPath() + ".disabled" }
func (b *Bridge) saveAutoLocked() error {
	if b.acct.AutoreconcileTimeout <= 0 {
		return nil
	}
	b.auto.state.Version = 1
	raw, err := json.Marshal(b.auto.state)
	if err != nil {
		return err
	}
	if len(raw) > 2<<20 {
		if err := durableWritePrivate(b.autoDisabledPath(), []byte(autoOverflowDiagnostic+"\n")); err != nil {
			return fmt.Errorf("%s; cannot persist disable marker: %w", autoOverflowDiagnostic, err)
		}
		return errAutoOverflow
	}
	return durableWritePrivate(b.autoPath(), raw)
}
func (b *Bridge) loadAuto() (loadErr error) {
	if b.acct.AutoreconcileTimeout <= 0 {
		return nil
	}
	// A sticky overflow marker disables only automatic cleanup, not foreground
	// startup or the existing exact-target recovery below.
	if _, err := os.Stat(b.autoDisabledPath()); err == nil {
		b.auto.err = errAutoOverflow
		b.log("warning", autoOverflowDiagnostic)
	} else if !os.IsNotExist(err) {
		return fmt.Errorf("autoreconcile disable marker: %w", err)
	}
	defer func() {
		if errors.Is(loadErr, errAutoOverflow) {
			b.auto.err = loadErr
			b.log("warning", loadErr.Error())
			loadErr = nil
		}
	}()
	raw, err := os.ReadFile(b.autoPath())
	if os.IsNotExist(err) {
		return nil
	}
	if err != nil {
		return err
	}
	if len(raw) > 2<<20 {
		return fmt.Errorf("autoreconcile state exceeds byte cap")
	}
	if err = json.Unmarshal(raw, &b.auto.state); err != nil {
		return err
	}
	if b.auto.state.Version != 1 {
		return fmt.Errorf("unsupported autoreconcile state")
	}
	// Reapply independent current quotas before any future selection. The
	// attempt's immutable seed remains untouched: its receipt proves those bytes.
	b.auto.state.History.retireUnprovenRoomOrigins()
	b.auto.state.History.normalize(b.acct.AutoreconcileHistoryMessages, b.acct.AutoreconcileOwnerHistoryMessages, b.acct.Owner, b.acct.JID)
	trimmed, err := json.Marshal(b.auto.state)
	if err != nil {
		return err
	}
	if string(trimmed) != string(raw) {
		if err = b.saveAutoLocked(); err != nil {
			if !errors.Is(err, errAutoOverflow) {
				return err
			}
			b.auto.err = err
			b.log("warning", err.Error())
		}
	}
	a := b.auto.state.Attempt
	if a != nil && (a.Phase == "worker" || a.Phase == "recovering" || a.Phase == "notice-pending" || a.Phase == "reporting") {
		a.Phase = "recovering"
		a.Error = "recovered attempt: report result only; never automatically reset or rerun worker"
		return b.saveAutoLocked()
	}
	if a != nil && (a.Phase == "resetting" || a.Phase == "uncertain") {
		r, e := loadNextBoundary(b.nextPath())
		if e != nil {
			return e
		}
		if r == nil {
			// Retirement can unlink successfully and then fail directory sync, or
			// crash before terminal auto-state persistence. Absence alone grants
			// no authority: only the exact seeded target AND an already committed
			// pointer prove this side of retirement. Never recover the source here.
			receipt, err := b.readAutoReceipt(a)
			if err != nil || loadSessionState(b.acct.Name) != receipt.Str("file") || (a.TargetID != "" && (a.TargetID != receipt.Str("id") || a.TargetFile != receipt.Str("file"))) {
				return fmt.Errorf("uncertain autoreconcile missing durable input fence and exact receipt/pointer proof")
			}
			a.TargetID, a.TargetFile = receipt.Str("id"), receipt.Str("file")
			target, err := readRegular(a.TargetFile)
			if err != nil {
				return err
			}
			a.TargetHash = autoHash(target)
			a.Phase, a.Error = "blocked", autoRetiredFenceRecovered
			// loadNextQueue still validates any protected orphan; it must already
			// be assigned to this exact target, never rebound by this recovery.
			return b.saveAutoLocked()
		}
		if r.ID != a.Token {
			return fmt.Errorf("uncertain autoreconcile mismatched durable input fence")
		}
		id, file := "", ""
		if receipt, e := b.readAutoReceipt(a); e == nil {
			id, file = receipt.Str("id"), receipt.Str("file")
		} else if b.sourceOutcome(a) {
			id, file = a.SourceID, a.SourceFile
		} else {
			return fmt.Errorf("uncertain autoreconcile outcome: input remains fenced; inspect nonce receipt and native sessions before repair")
		}
		headerRaw, headerErr := readRegular(file)
		if headerErr != nil {
			return fmt.Errorf("exact recovered native session unavailable: %w", headerErr)
		}
		var header Event
		first, _, _ := strings.Cut(string(headerRaw), "\n")
		if json.Unmarshal([]byte(first), &header) != nil || "id:"+header.Str("id") != id {
			return fmt.Errorf("recovered source/target header mismatch")
		}
		if e = durableWritePrivate(sessionStatePath(b.acct.Name), []byte(file+"\n")); e != nil {
			return e
		}
		if r.FreshSessionID != "" && (r.FreshSessionID != id || r.FreshSessionFile != file) {
			return fmt.Errorf("automatic protected target mismatch")
		}
		r.FreshSessionID, r.FreshSessionFile, r.Phase = id, file, nextBoundaryReleasing
		if e = saveNextBoundary(b.nextPath(), r); e != nil {
			return e
		}
		if id != a.SourceID {
			a.TargetID, a.TargetFile = id, file
			raw, _ := readRegular(file)
			a.TargetHash = autoHash(raw)
		}
		a.Phase, a.Error = "blocked", "recovered exact native target; protected input release only, no XA"
		return b.saveAutoLocked()
	}
	if a != nil && a.Phase != "done" && a.Phase != "blocked" {
		// Recovery never replays effects or an uncertain reset. A durable native
		// receipt can recover the exact seed/pointer, but does not authorize XA.
		if a.Phase == "resetting" {
			if r, e := b.readAutoReceipt(a); e == nil {
				if e = durableWritePrivate(sessionStatePath(b.acct.Name), []byte(r.Str("file")+"\n")); e != nil {
					return e
				}
				a.TargetID, a.TargetFile = r.Str("id"), r.Str("file")
			}
		}
		a.Phase, a.Error = "blocked", "interrupted attempt; never automatically rerun effects or reset; inspect artifacts"
		return b.saveAutoLocked()
	}
	return nil
}

// markActive is also called for synthetic turns, retries and lifecycle work.
// Persisting its generation makes a crash unable to revive a stale authority.
func (b *Bridge) autoActivity() {
	if b.acct.AutoreconcileTimeout <= 0 {
		return
	}
	b.auto.mu.Lock()
	defer b.auto.mu.Unlock()
	b.auto.state.Generation++
	if err := b.saveAutoLocked(); err != nil {
		b.auto.err = err
	}
}
func autoIdleState(s Event) bool {
	return !s.Bool("isStreaming") && !s.Bool("isCompacting") && s.F64("pendingMessageCount") == 0 && stateIdentity(s) != "" && filepath.IsAbs(s.Str("sessionFile"))
}
func (b *Bridge) autoEligible() bool {
	b.replyWatchMu.Lock()
	watches := len(b.replyWatches) > 0
	b.replyWatchMu.Unlock()
	if watches || b.rpc == nil || b.inbox == nil || b.initialPrompt != "" || b.inbox.loadError() != nil || len(b.inbox.pending()) > 0 || b.nextPending() || b.next.fenced.Load() || b.next.quarantined.Load() {
		return false
	}
	b.mu.Lock()
	defer b.mu.Unlock()
	return !b.nextBusyLocked() && len(b.pendingMarkers) == 0 && !b.idleSince.IsZero() && time.Since(b.idleSince) >= b.acct.AutoreconcileTimeout
}

// The existing idle watcher only launches this executor. It never waits for a
// worker while holding the foreground session transition mutex.
func (b *Bridge) autoTick() {
	if b.acct.AutoreconcileTimeout <= 0 {
		return
	}
	b.auto.mu.Lock()
	if !b.auto.ready || b.auto.running || b.auto.err != nil {
		b.auto.mu.Unlock()
		return
	}
	b.auto.running = true
	b.auto.mu.Unlock()
	go func() {
		defer func() { b.auto.mu.Lock(); b.auto.running = false; b.auto.mu.Unlock() }()
		b.executeAuto()
	}()
}
func (b *Bridge) autoSave(a *autoAttempt, phase string) error {
	b.auto.mu.Lock()
	defer b.auto.mu.Unlock()
	a.Phase = phase
	saved := *a
	b.auto.state.Attempt = &saved
	err := b.saveAutoLocked()
	if err != nil {
		b.auto.err = err
	}
	return err
}
func (b *Bridge) autoBlock(a *autoAttempt, err error) {
	a.Error = err.Error()
	_ = b.autoSave(a, "blocked")
	b.log("warning", "autoreconcile: "+err.Error())
}
func autoHash(raw []byte) string { s := sha256.Sum256(raw); return hex.EncodeToString(s[:]) }
func (b *Bridge) executeAuto() {
	b.auto.mu.Lock()
	pending := b.auto.state.Attempt
	if pending != nil {
		copy := *pending
		pending = &copy
	}
	b.auto.mu.Unlock()
	if pending != nil && pending.Phase == "uncertain" {
		b.resolveAutoBoundary(pending)
		return
	}
	if pending != nil && (pending.Phase == "recovering" || pending.Phase == "notice-pending" || pending.Phase == "reporting") {
		b.resumeAutoNotice(pending)
		return
	}
	if !b.autoEligible() {
		return
	}
	res, err := b.rpc.observedState(b.ctx)
	if err != nil || !res.success() || !autoIdleState(res.Obj("data")) {
		return
	}
	state := res.Obj("data")
	// Reserve and snapshot under the short admission lock, not the worker lock.
	b.sessionTransitionMu.Lock()
	if !b.autoEligible() {
		b.sessionTransitionMu.Unlock()
		return
	}
	current, e := b.rpc.GetState(b.ctx)
	if e != nil || !current.success() || !autoIdleState(current.Obj("data")) || stateIdentity(current.Obj("data")) != stateIdentity(state) {
		b.sessionTransitionMu.Unlock()
		return
	}
	raw, err := readRegular(state.Str("sessionFile"))
	if err != nil {
		b.sessionTransitionMu.Unlock()
		return
	}
	hash := autoHash(raw)
	fingerprint := stateIdentity(state) + ":" + hash
	b.auto.mu.Lock()
	prev := b.auto.state.Attempt
	if prev != nil && (prev.Fingerprint == fingerprint || (prev.TargetID == stateIdentity(state) && prev.TargetHash == hash)) {
		b.auto.mu.Unlock()
		b.sessionTransitionMu.Unlock()
		return
	}
	token, err := randomLocalKey()
	a := &autoAttempt{Token: token, SourceID: stateIdentity(state), SourceFile: state.Str("sessionFile"), SnapshotHash: hash, Fingerprint: fingerprint, Generation: b.auto.state.Generation, State: state}
	b.auto.mu.Unlock()
	if err == nil {
		err = b.autoSave(a, "reserved")
	}
	if err == nil {
		if b.acct.ReconcileSkill != "" {
			var j *reconcileJob
			a.Job, j, err = prepareReconcile(b.acct, state, bareJid(b.acct.JID), b.rpc.bin)
			if err == nil && autoHash(mustReadSnapshot(a.Job)) != hash {
				err = fmt.Errorf("source changed during snapshot preparation")
			}
			if err == nil {
				j.Automatic = true
				err = saveReconcile(a.Job, j)
			}
		} else {
			root := reconcileRoot(b.acct.Name)
			err = os.MkdirAll(root, 0700)
			if err == nil {
				a.Job, err = os.MkdirTemp(root, "auto-")
			}
			if err == nil {
				err = durableWritePrivate(filepath.Join(a.Job, "snapshot.jsonl"), raw)
			}
		}
	}
	if err == nil {
		err = b.autoSave(a, "prepared")
	}
	b.sessionTransitionMu.Unlock()
	if err != nil {
		b.autoBlock(a, err)
		return
	}
	if b.acct.ReconcileSkill != "" {
		// Explicit pre-reset authority is NOT /next's reset-authorized marker.
		if err = durableWritePrivate(filepath.Join(a.Job, "idle-authorized"), []byte(a.Token+"\n")); err != nil {
			b.autoBlock(a, err)
			return
		}
		if err = b.autoSave(a, "worker"); err != nil {
			return
		}
		gate, e := startReconcile(a.Job)
		if e != nil {
			b.autoBlock(a, e)
			return
		}
		_, err = io.WriteString(gate, "run-idle\n")
		_ = gate.Close()
		if err != nil {
			b.autoBlock(a, err)
			return
		}
		ticker := time.NewTicker(time.Second)
		defer ticker.Stop()
		var j *reconcileJob
		deadline := time.NewTimer(reconcileTimeout + time.Minute)
		defer deadline.Stop()
		for {
			select {
			case <-b.ctx.Done():
				return
			case <-deadline.C:
				b.autoBlock(a, fmt.Errorf("worker result timeout; no replay"))
				return
			case <-ticker.C:
			}
			j, err = loadReconcile(a.Job)
			if err != nil {
				b.autoBlock(a, err)
				return
			}
			if reconcileTerminal(j.State) {
				break
			}
		}
		if j.State != "completed" {
			a.Error = fmt.Sprintf("worker %s: %s", j.State, j.Summary)
		}
		a.ReportID = newStanzaID()
		a.ReportText = fmt.Sprintf("[autoreconcile %s] %s\n%s\nArtifacts: %s", j.ID, j.State, j.Summary, a.Job)
		if err = b.autoSave(a, "notice-pending"); err != nil {
			return
		}
		if !b.sendAutoNotice(a) {
			return
		}
		if a.Error != "" {
			b.autoBlock(a, fmt.Errorf("%s", a.Error))
			return
		}
	}
	b.finishAuto(a)
}
func mustReadSnapshot(dir string) []byte {
	raw, _ := os.ReadFile(filepath.Join(dir, "snapshot.jsonl"))
	return raw
}
func (b *Bridge) autoCurrent(a *autoAttempt) bool {
	b.auto.mu.Lock()
	defer b.auto.mu.Unlock()
	return b.auto.err == nil && b.auto.state.Generation == a.Generation && b.auto.state.Attempt != nil && b.auto.state.Attempt.Token == a.Token
}
func (b *Bridge) finishAuto(a *autoAttempt) {
	// RPC observation is outside the transition lock: ordered lifecycle consumers
	// must remain free. Input admitted before acquisition invalidates generation.
	res, err := b.rpc.observedState(b.ctx)
	if err != nil || !res.success() || !autoIdleState(res.Obj("data")) || stateIdentity(res.Obj("data")) != a.SourceID {
		b.autoBlock(a, fmt.Errorf("source no longer idle/exact"))
		return
	}
	b.sessionTransitionMu.Lock()
	defer b.sessionTransitionMu.Unlock()
	if !b.autoCurrent(a) || !b.autoEligible() {
		b.autoBlock(a, fmt.Errorf("new activity invalidated cleanup"))
		return
	}
	raw, err := readRegular(a.SourceFile)
	if err != nil || autoHash(raw) != a.SnapshotHash {
		b.autoBlock(a, fmt.Errorf("source snapshot changed"))
		return
	}
	b.auto.mu.Lock()
	a.Seed = b.auto.state.History.seed(a.SourceFile, a.Job)
	b.auto.mu.Unlock()
	commands, e := b.rpc.Request(b.ctx, map[string]any{"type": "get_commands"}, 5*time.Second)
	if e != nil || !commands.success() || !autoCommandPresent(commands, b.rpc.extPath) {
		b.autoBlock(a, fmt.Errorf("private native command unavailable or provenance mismatch"))
		return
	}
	if err = b.beginAutoBoundary(a); err != nil {
		b.autoUncertain(a, err)
		return
	}
	if err = b.autoSave(a, "resetting"); err != nil {
		return
	}
	request := map[string]string{"token": a.Token, "sourceID": a.SourceID, "sourceFile": a.SourceFile, "hash": a.SnapshotHash, "seed": a.Seed, "generation": fmt.Sprint(a.Generation), "thinking": a.State.Str("thinkingLevel"), "provider": a.State.Obj("model").Str("provider"), "model": a.State.Obj("model").Str("id")}
	payload, _ := json.Marshal(request)
	if err = durableWritePrivate(b.autoRequestPath(), payload); err != nil {
		b.autoUncertain(a, err)
		return
	}
	defer durableRemove(b.autoRequestPath())
	b.mu.Lock()
	b.nextTransition = true
	b.mu.Unlock()
	defer func() { b.mu.Lock(); b.nextTransition = false; b.mu.Unlock() }()
	ctx, cancel := context.WithTimeout(b.ctx, 30*time.Second)
	defer cancel()
	res, err = b.rpc.Request(ctx, map[string]any{"type": "prompt", "message": "/pi-msg-idle-reset " + a.Token}, 30*time.Second)
	if err != nil || !res.success() {
		b.autoUncertain(a, fmt.Errorf("native reset failed/uncertain: %v %s", err, res.errText()))
		return
	}
	receipt, err := b.readAutoReceipt(a)
	if err != nil {
		b.autoUncertain(a, err)
		return
	}
	// The command receipt proves the FINAL native UUID (setSessionFile creates a
	// second UUID). Observe RPC identity too; never infer it from the filename.
	res, err = b.rpc.GetState(ctx)
	if err != nil || !res.success() || stateIdentity(res.Obj("data")) != receipt.Str("id") || res.Obj("data").Str("sessionFile") != receipt.Str("file") {
		b.autoUncertain(a, fmt.Errorf("native target identity mismatch"))
		return
	}

	a.TargetID, a.TargetFile = receipt.Str("id"), receipt.Str("file")
	targetRaw, e := readRegular(a.TargetFile)
	if e != nil {
		b.autoUncertain(a, e)
		return
	}
	a.TargetHash = autoHash(targetRaw)
	if err = durableWritePrivate(sessionStatePath(b.acct.Name), []byte(a.TargetFile+"\n")); err != nil {
		b.autoUncertain(a, err)
		return
	}
	if !b.autoCurrent(a) || receipt.Str("status") != "success" || receipt.Bool("stale") {
		if e = b.releaseAutoBoundary(a, a.TargetID, a.TargetFile); e != nil {
			b.autoUncertain(a, e)
			return
		}
		b.autoBlock(a, fmt.Errorf("completed native switch invalidated by activity; exact target retained without XA"))
		b.wakeNext()
		return
	}
	// Serialize the final XA commit with pre-admission activity invalidation.
	b.auto.mu.Lock()
	if b.auto.state.Generation != a.Generation || b.auto.err != nil {
		b.auto.mu.Unlock()
		if e = b.releaseAutoBoundary(a, a.TargetID, a.TargetFile); e != nil {
			b.autoUncertain(a, e)
			return
		}
		b.autoBlock(a, fmt.Errorf("input won final cleanup commit; exact target retained without XA"))
		b.wakeNext()
		return
	}
	if e = b.clearAutoBoundary(a); e != nil {
		b.auto.mu.Unlock()
		b.autoUncertain(a, e)
		return
	}
	a.Phase = "done"
	saved := *a
	b.auto.state.Attempt = &saved
	err = b.saveAutoLocked()
	if err != nil {
		b.auto.err = err
		b.auto.mu.Unlock()
		return
	}
	b.sessionFile = a.TargetFile
	b.messagingSeeded = false
	b.mu.Lock()
	b.sessionID = a.TargetID
	b.awayAnnounced = true
	b.mu.Unlock()
	b.markFresh()
	b.xmpp.SetPresence("xa", "idle cleanup complete")
	b.auto.mu.Unlock()
}
func (b *Bridge) readAutoReceipt(a *autoAttempt) (Event, error) {
	raw, err := os.ReadFile(b.autoRequestPath() + ".receipt")
	if err != nil {
		return nil, err
	}
	var r Event
	if err = json.Unmarshal(raw, &r); err != nil {
		return nil, err
	}
	if (r.Str("status") != "success" && r.Str("status") != "switched-failed") || r.Bool("cancelled") || r.Str("seedHash") != autoHash([]byte(a.Seed)) || r.Str("token") != a.Token || r.Str("sourceID") != a.SourceID || r.Str("id") == "" || r.Str("id") == a.SourceID || !filepath.IsAbs(r.Str("file")) || r.Str("file") == a.SourceFile {
		return nil, fmt.Errorf("invalid native reset receipt")
	}
	raw, err = readRegular(r.Str("file"))
	if err != nil {
		return nil, err
	}
	lines := strings.Split(strings.TrimSpace(string(raw)), "\n")
	var header Event
	if json.Unmarshal([]byte(lines[0]), &header) != nil || "id:"+header.Str("id") != r.Str("id") {
		return nil, fmt.Errorf("native header identity mismatch")
	}
	for _, line := range lines[1:] {
		var e Event
		if json.Unmarshal([]byte(line), &e) == nil && e.Type() == "custom_message" && e.Str("customType") == "pi-msg-idle-seed" && e.Str("content") == a.Seed && e.Obj("details").Str("token") == a.Token {
			return r, nil
		}
	}
	return nil, fmt.Errorf("durable native seed missing")
}

func autoCommandPresent(res Event, path string) bool {
	if path == "" {
		return false
	}
	for _, item := range res.Obj("data").Arr("commands") {
		m, ok := item.(map[string]any)
		if !ok {
			continue
		}
		e := Event(m)
		if e.Str("name") == "pi-msg-idle-reset" && e.Str("source") == "extension" && e.Obj("sourceInfo").Str("path") == path && e.Obj("sourceInfo").Str("source") == "cli" {
			return true
		}
	}
	return false
}

// Stable stanza identity allows reconnect retries without re-running worker
// effects. Accepted copies enter the durable recap index at the transport seam.
func (b *Bridge) sendAutoNotice(a *autoAttempt) bool {
	if a.ReportID == "" || a.ReportText == "" {
		b.autoBlock(a, fmt.Errorf("missing durable report identity"))
		return false
	}
	if err := b.autoSave(a, "notice-pending"); err != nil {
		return false
	}
	if b.xmpp == nil || b.xmpp.sendRecordedChat(bareJid(b.acct.JID), a.ReportID, a.ReportText) != nil {
		return false
	}
	b.auto.mu.Lock()
	err := b.auto.err
	b.auto.mu.Unlock()
	if err != nil {
		b.autoBlock(a, err)
		return false
	}
	return b.autoSave(a, "reported") == nil
}
func (b *Bridge) resumeAutoNotice(a *autoAttempt) {
	recovered := a.Error != ""
	if a.ReportText == "" {
		j, err := loadReconcile(a.Job)
		if err != nil {
			b.autoBlock(a, err)
			return
		}
		if !reconcileTerminal(j.State) {
			lock, e := lockFile(filepath.Join(a.Job, "runner.lock"))
			if e != nil {
				return
			}
			defer lock.Close()
			j.State, j.Summary = "interrupted", "runner disappeared; partial effects possible; no replay"
			if e = saveReconcile(a.Job, j); e != nil {
				b.autoBlock(a, e)
				return
			}
		}
		a.ReportID = newStanzaID()
		a.ReportText = fmt.Sprintf("[autoreconcile %s] %s\n%s\nArtifacts: %s", j.ID, j.State, j.Summary, a.Job)
	}
	if !b.sendAutoNotice(a) {
		return
	}
	if recovered {
		b.autoBlock(a, fmt.Errorf("recovered result reported; reset authority was lost"))
		return
	}
	b.finishAuto(a)
}
