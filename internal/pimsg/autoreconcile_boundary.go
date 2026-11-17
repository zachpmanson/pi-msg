package pimsg

import (
	"encoding/json"
	"fmt"
	"os"
	"time"
)

const autoRetiredFenceRecovered = "recovered retired automatic fence: exact native target; no XA"

// Reuse /next's durable protected-input fence for the SHORT native switch only.
// This record can release into an exact proven session, but never starts a
// worker or retries a reset. Worker lifetime remains entirely unfenced.
func (b *Bridge) beginAutoBoundary(a *autoAttempt) error {
	b.next.mu.Lock()
	defer b.next.mu.Unlock()
	if b.next.record != nil {
		return fmt.Errorf("another session boundary won admission")
	}
	r, err := newNextBoundaryRecord(bareJid(b.acct.JID), "automatic-idle-recovery", time.Now())
	if err != nil {
		return err
	}
	r.ID = a.Token
	r.Phase = nextBoundaryBlocked
	r.OldSessionID = a.SourceID
	r.OldSessionFile = a.SourceFile
	r.LastError = "automatic native switch pending exact outcome; input retained, never replay reset"
	b.next.fenced.Store(true)
	b.next.record = r // retain in-memory fence even if persistence is uncertain
	return saveNextBoundary(b.nextPath(), r)
}
func (b *Bridge) clearAutoBoundary(a *autoAttempt) error {
	b.next.mu.Lock()
	defer b.next.mu.Unlock()
	if b.next.record == nil || b.next.record.ID != a.Token {
		return fmt.Errorf("automatic transition fence missing/mismatched")
	}
	if len(b.inbox.protectedPending()) > 0 {
		return fmt.Errorf("automatic transition has held input; exact-target release required")
	}
	if err := removeNextBoundary(b.nextPath()); err != nil {
		return err
	}
	b.next.record = nil
	b.next.fenced.Store(false)
	return nil
}

// Called only after independently proving the active exact target or an
// explicit before-switch cancellation/source outcome. Protected input is never
// retargeted: use the existing assignment/release path and its RPC identity gate.
func (b *Bridge) releaseAutoBoundary(a *autoAttempt, id, file string) error {
	if err := durableWritePrivate(sessionStatePath(b.acct.Name), []byte(file+"\n")); err != nil {
		return err
	}
	b.next.mu.Lock()
	defer b.next.mu.Unlock()
	r := b.next.record
	if r == nil || r.ID != a.Token {
		return fmt.Errorf("automatic transition fence missing/mismatched")
	}
	if r.FreshSessionID != "" && (r.FreshSessionID != id || r.FreshSessionFile != file) {
		return fmt.Errorf("automatic protected target cannot change")
	}
	r.FreshSessionID, r.FreshSessionFile, r.Phase = id, file, nextBoundaryReleasing
	r.LastError = "exact native outcome recovered; input release only, no reset or worker replay"
	if err := saveNextBoundary(b.nextPath(), r); err != nil {
		return err
	}
	b.sessionFile = file
	b.messagingSeeded = false
	b.mu.Lock()
	b.sessionID = id
	b.mu.Unlock()
	return nil
}
func (b *Bridge) autoUncertain(a *autoAttempt, err error) {
	a.Error = "uncertain native transition; protected input fenced: " + err.Error()
	_ = b.autoSave(a, "uncertain")
	b.log("warning", "autoreconcile: "+a.Error)
}
func (b *Bridge) sourceOutcome(a *autoAttempt) bool {
	raw, err := os.ReadFile(b.autoRequestPath() + ".receipt")
	if err != nil {
		return false
	}
	var r Event
	if json.Unmarshal(raw, &r) != nil {
		return false
	}
	return r.Str("token") == a.Token && r.Str("sourceID") == a.SourceID && r.Bool("sourceAuthoritative") && (r.Str("status") == "cancelled" || r.Str("status") == "failed")
}
func (b *Bridge) resolveAutoBoundary(a *autoAttempt) {
	receipt, err := b.readAutoReceipt(a)
	id, file := "", ""
	if err == nil {
		id, file = receipt.Str("id"), receipt.Str("file")
	} else if b.sourceOutcome(a) {
		id, file = a.SourceID, a.SourceFile
	} else {
		return
	}
	res, e := b.rpc.observedState(b.ctx)
	if e != nil || !res.success() || stateIdentity(res.Obj("data")) != id || res.Obj("data").Str("sessionFile") != file {
		return
	}
	b.sessionTransitionMu.Lock()
	defer b.sessionTransitionMu.Unlock()
	if e = b.releaseAutoBoundary(a, id, file); e != nil {
		b.autoUncertain(a, e)
		return
	}
	if id != a.SourceID {
		a.TargetID, a.TargetFile = id, file
		raw, _ := readRegular(file)
		a.TargetHash = autoHash(raw)
	}
	b.autoBlock(a, fmt.Errorf("native outcome recovered; exact input target retained; no XA"))
	b.wakeNext()
}
