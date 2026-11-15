package pimsg

import (
	"bufio"
	"bytes"
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"sync"
	"time"
)

// Durable inbound inbox (issue #96).
//
// Inbound input had no durable representation. It existed only as (a) a
// deliverable stanza and (b) whatever pi held in its process-local steering /
// follow-up queue, so a message handed to a run in flight — where pi injects it
// only at the next tool yield — was gone if the process stopped first. Because
// it had been delivered live, the server had no reason to replay it either, and
// the restart window starts at the shutdown instant, which is always later. On
// 2026-09-22 an owner instruction steered into a running turn was killed by a
// config switch 55 seconds later and never arrived; the owner re-sent it twice.
//
// The inbox is append-before-prompt, ack-at-settle:
//
//   - every inbound message (reactions aside) is appended to
//     <config-dir>/<account>.inbox.jsonl BEFORE any prompt is sent, from one
//     hook ahead of the direct, room and commentary paths;
//   - entries are acknowledged once the run that took them in has settled;
//   - at start, anything still unacknowledged is re-delivered through the normal
//     classify/dispatch path, so the last acknowledged inbound is the real
//     replay cursor.
//
// An entry is acknowledged at settle when either the run that is settling
// consumed it — it was handed to pi (markDelivered) and the run produced
// assistant text or ran a tool after that (ackPolicy.LastActive) — or it has been
// pending longer than inboxAckGrace without being delivered at all, so no run
// will ever consume it.
//
// Delivery is at-least-once, not exactly-once, but the window is now bounded by a
// turn rather than open-ended: a delivery with no activity after it (a steer pi
// never yielded, or a message that arrived as the run ended) stays pending and is
// re-delivered at the next start.
//
// The older, age-only rule was the source of a real defect (issue #104): an entry
// that arrived within the grace window of the settle that consumed it stayed
// pending, and if the account then went quiet no later settle ever cleared it. It
// survived for days and was re-delivered — and announced to the owner — on every
// restart, so agents "caught up" on stale messages that were not new at all.
//
// Messages that never become a prompt (an unaddressed room message, a bridge
// command, a dropped own-echo) are dropped outright (drop): no run will ever
// settle for them, so leaving them pending would strand them the same way.
//
// The file holds only unacknowledged entries: a settle rewrites what remains.
// It is therefore empty in steady state, and bounded by inboxCap if a run never
// settles.

// inboxAckGrace is how long an entry must have been pending before a settle
// acknowledges it *without* evidence that a run consumed it. Without it, a
// message that arrives in the same instant a run ends would be acked by that
// run's settle — and lost if the *next* run never got to consume it (the exact
// failure this file exists to prevent). It is a backstop for entries that were
// never delivered at all; a delivered entry is acknowledged as soon as the run
// that took it in shows activity (see ackPolicy).
const inboxAckGrace = 5 * time.Second

// inboxCap bounds ordinary entries only; protected promises are never evicted.
const inboxCap = 500

// Includes JSON escaping/metadata, excludes the terminating newline. Protected
// writes must never accept a record that the restart scanner cannot recover.
const maxInboxRecordBytes = (1 << 20) - 1

// inboxEntry is one inbound message received by the bridge but not yet
// acknowledged by a settled run.
type inboxEntry struct {
	Key             string `json:"key,omitempty"`
	NextBoundaryID  string `json:"nextBoundaryID,omitempty"`
	TargetSessionID string `json:"targetSessionID,omitempty"`
	ID              string `json:"id,omitempty"`
	From            string `json:"from,omitempty"`
	Body            string `json:"body"`
	Room            string `json:"room,omitempty"`
	Nick            string `json:"nick,omitempty"`
	RealJID         string `json:"realJID,omitempty"`
	FromOwner       bool   `json:"fromOwner,omitempty"`
	Direct          bool   `json:"direct,omitempty"`
	// Addressed is the verdict of the room-address check that gated this append
	// (see InboundMessage.Addressed): re-delivery must not have to re-derive it.
	Addressed bool `json:"addressed,omitempty"`
	// ReplyToID is the XEP-0461 stamp of the message this one answers, so a
	// re-delivered reply still names its target (#95).
	ReplyToID string `json:"replyToID,omitempty"`
	// Markable is the inbound XEP-0333 <markable/> flag, carried so a message
	// re-delivered after a restart can still send its deferred "displayed"
	// marker once pi reads it (#73).
	Markable bool      `json:"markable,omitempty"`
	At       time.Time `json:"at"`

	// deliveredAt is when pi was handed this message during the current process.
	// In-memory only: a delivery does not survive a restart (the point of
	// re-delivery is that the new process has not seen the entry yet), and a
	// stale on-disk value would let a settle in the new process acknowledge an
	// entry it never delivered.
	deliveredAt        time.Time
	deliveredSessionID string
}

func (e inboxEntry) protected() bool { return e.NextBoundaryID != "" || e.TargetSessionID != "" }

// message turns the entry back into the transport-agnostic inbound message the
// dispatch path works on, so re-delivery runs the normal classification.
func (e inboxEntry) message() InboundMessage {
	return InboundMessage{
		Body:      e.Body,
		Nick:      e.Nick,
		RealJID:   e.RealJID,
		FromOwner: e.FromOwner,
		Direct:    e.Direct,
		ReplyToID: e.ReplyToID,
		Addressed: e.Addressed,
		Room:      e.Room,
		ID:        e.ID,
		From:      e.From,
		Markable:  e.Markable,
	}
}

// inbox is the per-account durable queue. Safe for concurrent use: the XMPP
// read loop appends while the RPC event loop acknowledges.
type inbox struct {
	mu      sync.Mutex
	path    string
	log     func(level, msg string)
	entries []inboxEntry
	loadErr error
}

// newInbox opens (and loads) the inbox at path. A missing file is an empty
// inbox; an unreadable or corrupt file is logged and treated as empty rather
// than failing the launch.
func newInbox(path string, log func(level, msg string)) *inbox {
	in := &inbox{path: path, log: log}
	in.load()
	return in
}

func (in *inbox) warn(msg string) {
	if in.log != nil {
		in.log("warning", "inbox: "+msg)
	}
}

func (in *inbox) load() {
	f, err := os.Open(in.path)
	if err != nil {
		if !os.IsNotExist(err) {
			in.loadErr = fmt.Errorf("open inbox: %w", err)
			in.warn(in.loadErr.Error())
		}
		return
	}
	defer f.Close()
	sc := bufio.NewScanner(f)
	sc.Buffer(make([]byte, 0, 64*1024), maxInboxRecordBytes+1)
	var entries []inboxEntry
	keys := make(map[string]bool)
	for sc.Scan() {
		line := sc.Bytes()
		if len(line) == 0 {
			continue
		}
		var e inboxEntry
		if err := json.Unmarshal(line, &e); err != nil {
			in.loadErr = errors.Join(in.loadErr, fmt.Errorf("unparsable inbox entry: %w", err))
			in.warn("skipping unparsable entry: " + err.Error())
			continue
		}
		if e.protected() && (e.Key == "" || keys[e.Key]) {
			in.loadErr = errors.Join(in.loadErr, fmt.Errorf("protected inbox entry has missing or duplicate key %q", e.Key))
		}
		if e.Key != "" {
			keys[e.Key] = true
		}
		entries = append(entries, e)
	}
	if err := sc.Err(); err != nil {
		in.loadErr = errors.Join(in.loadErr, fmt.Errorf("read inbox: %w", err))
		in.warn("read: " + err.Error())
	}
	if len(entries) > 0 {
		in.logf("info", fmt.Sprintf("%d unacknowledged inbound message(s) recovered from %s", len(entries), filepath.Base(in.path)))
	}
	in.entries = entries
	if len(in.protectedPendingLocked()) > inboxCap {
		in.warn(fmt.Sprintf("protected storage exceeds %d entries; retaining all promised input", inboxCap))
	}
}

func (in *inbox) logf(level, msg string) {
	if in.log != nil {
		in.log(level, msg)
	}
}

// append records an ordinary message before it is handed to pi.
func (in *inbox) append(e inboxEntry) {
	// Protected callers must use the error-returning append path.
	if e.protected() {
		in.warn("protected entry requires appendProtected")
		return
	}
	if e.At.IsZero() {
		e.At = time.Now()
	}
	in.mu.Lock()
	defer in.mu.Unlock()
	next := append(append([]inboxEntry(nil), in.entries...), e)
	next = in.capOrdinary(next)
	if err := in.commitLocked(next, false); err != nil {
		in.warn(err.Error())
	}
}

// loadError retains diagnostics even when legacy corrupt-line recovery succeeds.
func (in *inbox) loadError() error {
	in.mu.Lock()
	defer in.mu.Unlock()
	return in.loadErr
}

func (in *inbox) protectedReadyLocked() error {
	if in.loadErr != nil {
		return fmt.Errorf("protected inbox unavailable; repair/reload %s: %w", in.path, in.loadErr)
	}
	return nil
}

func (in *inbox) appendProtected(e inboxEntry) (inboxEntry, error) {
	in.mu.Lock()
	defer in.mu.Unlock()
	if err := in.protectedReadyLocked(); err != nil {
		return inboxEntry{}, err
	}
	if e.NextBoundaryID == "" {
		return inboxEntry{}, fmt.Errorf("protected append requires boundary ID")
	}
	if e.Key == "" {
		var err error
		e.Key, err = randomLocalKey()
		if err != nil {
			return inboxEntry{}, err
		}
	}
	for _, old := range in.entries {
		if old.Key == e.Key {
			return inboxEntry{}, fmt.Errorf("duplicate inbox key %q", e.Key)
		}
	}
	if e.At.IsZero() {
		e.At = time.Now()
	}
	e.deliveredAt = time.Time{}
	e.deliveredSessionID = ""
	next := append(append([]inboxEntry(nil), in.entries...), e)
	if err := in.commitLocked(next, true); err != nil {
		return inboxEntry{}, err
	}
	if len(in.protectedPendingLocked()) > inboxCap {
		in.warn(fmt.Sprintf("protected storage exceeds %d entries; retaining all promised input", inboxCap))
	}
	return e, nil
}

func (in *inbox) assignTargetSession(boundaryID, sessionID string) (int, error) {
	in.mu.Lock()
	defer in.mu.Unlock()
	if err := in.protectedReadyLocked(); err != nil {
		return 0, err
	}
	if boundaryID == "" || sessionID == "" {
		return 0, fmt.Errorf("target assignment requires boundary and session IDs")
	}
	next := append([]inboxEntry(nil), in.entries...)
	n := 0
	for i := range next {
		if next[i].NextBoundaryID != boundaryID {
			continue
		}
		if next[i].TargetSessionID != "" && next[i].TargetSessionID != sessionID {
			return 0, fmt.Errorf("inbox key %q already targets session %q", next[i].Key, next[i].TargetSessionID)
		}
		next[i].TargetSessionID = sessionID
		n++
	}
	if n == 0 {
		return 0, nil
	}
	if err := in.commitLocked(next, true); err != nil {
		return 0, err
	}
	return n, nil
}

func (in *inbox) protectedPendingLocked() []inboxEntry {
	var entries []inboxEntry
	for _, e := range in.entries {
		if e.protected() {
			entries = append(entries, e)
		}
	}
	return entries
}

func (in *inbox) protectedPending() []inboxEntry {
	in.mu.Lock()
	defer in.mu.Unlock()
	return in.protectedPendingLocked()
}

// Delivery evidence is local and explicitly tied to the exact target session.
func (in *inbox) markDeliveredKey(key, sessionID string, at time.Time) (bool, error) {
	in.mu.Lock()
	defer in.mu.Unlock()
	if err := in.protectedReadyLocked(); err != nil {
		return false, err
	}
	if key == "" || sessionID == "" || at.IsZero() {
		return false, fmt.Errorf("delivery requires key, session and timestamp")
	}
	for i := range in.entries {
		e := &in.entries[i]
		if e.Key != key || !e.protected() {
			continue
		}
		if e.TargetSessionID != sessionID {
			return false, fmt.Errorf("inbox key %q does not target session %q", key, sessionID)
		}
		e.deliveredAt, e.deliveredSessionID = at, sessionID
		return true, nil
	}
	return false, nil
}

func (in *inbox) dropKey(key string) (bool, error) {
	in.mu.Lock()
	defer in.mu.Unlock()
	if err := in.protectedReadyLocked(); err != nil {
		return false, err
	}
	if key == "" {
		return false, fmt.Errorf("drop requires inbox key")
	}
	for i, e := range in.entries {
		if e.Key != key || !e.protected() {
			continue
		}
		next := append([]inboxEntry(nil), in.entries[:i]...)
		next = append(next, in.entries[i+1:]...)
		if err := in.commitLocked(next, true); err != nil {
			return false, err
		}
		return true, nil
	}
	return false, nil
}

func (in *inbox) capOrdinary(entries []inboxEntry) []inboxEntry {
	ordinary := 0
	for _, e := range entries {
		if !e.protected() {
			ordinary++
		}
	}
	drop := ordinary - inboxCap
	if drop <= 0 {
		return entries
	}
	in.warn(fmt.Sprintf("over %d ordinary entries; dropped the oldest %d", inboxCap, drop))
	kept := make([]inboxEntry, 0, len(entries)-drop)
	for _, e := range entries {
		if !e.protected() && drop > 0 {
			drop--
			continue
		}
		kept = append(kept, e)
	}
	return kept
}

func hasProtected(entries []inboxEntry) bool {
	for _, e := range entries {
		if e.protected() {
			return true
		}
	}
	return false
}

// Stage a separate slice: failed protected persistence must not mutate memory.
func (in *inbox) commitLocked(next []inboxEntry, forceDurable bool) error {
	// A skipped corrupt record may itself be protected. Even an ordinary append,
	// drop or settle must not overwrite that unknown input after a failed load.
	if err := in.protectedReadyLocked(); err != nil {
		return err
	}
	if forceDurable || hasProtected(in.entries) || hasProtected(next) {
		var err error
		if len(next) == 0 {
			err = durableRemove(in.path)
		} else {
			var b bytes.Buffer
			enc := json.NewEncoder(&b)
			for _, e := range next {
				start := b.Len()
				if err := enc.Encode(e); err != nil {
					return fmt.Errorf("encode inbox: %w", err)
				}
				if b.Len()-start-1 > maxInboxRecordBytes {
					return fmt.Errorf("encoded inbox record exceeds %d bytes; input not durably accepted", maxInboxRecordBytes)
				}
			}
			err = durableWritePrivate(in.path, b.Bytes())
		}
		if err != nil {
			return fmt.Errorf("persist protected inbox %s: %w", in.path, err)
		}
		in.entries = next
		return nil
	}
	in.entries = next
	in.flushLocked()
	return nil
}

// pending copies the unacknowledged entries in arrival order.
func (in *inbox) pending() []inboxEntry {
	in.mu.Lock()
	defer in.mu.Unlock()
	return append([]inboxEntry(nil), in.entries...)
}

// clearOrdinary is explicit /new recovery only. Protected promises are untouched.
func (in *inbox) clearOrdinary() (int, error) {
	in.mu.Lock()
	defer in.mu.Unlock()
	kept := in.protectedPendingLocked()
	n := len(in.entries) - len(kept)
	if n == 0 {
		return 0, nil
	}
	if err := in.commitLocked(kept, true); err != nil {
		return 0, err
	}
	return n, nil
}

// markDelivered records that pi was handed this message, so the settle that ends
// the run can acknowledge it without waiting for the grace. It matches by stanza
// id when there is one, else by (from, body). Returns how many entries matched.
func (in *inbox) markDelivered(id, from, body string, at time.Time) int {
	in.mu.Lock()
	defer in.mu.Unlock()
	n := 0
	for i := range in.entries {
		if !in.entries[i].protected() && matchesEntry(in.entries[i], id, from, body) {
			in.entries[i].deliveredAt = at
			n++
		}
	}
	return n
}

// drop removes entries for a message that never became a prompt — an
// unaddressed room message, a bridge command handled in-process, a dropped
// own-echo. No run will settle for such an entry, so it must not wait for an
// ack: before issue #104 they stayed pending and were re-delivered as
// "unacknowledged" on every restart.
func (in *inbox) drop(id, from, body string) int {
	in.mu.Lock()
	defer in.mu.Unlock()
	kept := make([]inboxEntry, 0, len(in.entries))
	dropped := 0
	for _, e := range in.entries {
		if !e.protected() && matchesEntry(e, id, from, body) {
			dropped++
			continue
		}
		kept = append(kept, e)
	}
	if dropped == 0 {
		return 0
	}
	if err := in.commitLocked(kept, false); err != nil {
		in.warn(err.Error())
		return 0
	}
	return dropped
}

// matchesEntry reports whether an entry is the message identified by these
// fields. The stanza id is authoritative when present; (from, body) is the
// fallback for stanzas that arrive without one (XEP-0359 is not universal).
func matchesEntry(e inboxEntry, id, from, body string) bool {
	if id != "" {
		return e.ID == id
	}
	if from == "" && body == "" {
		return false
	}
	return e.From == from && e.Body == body
}

// len reports how many entries are unacknowledged.
func (in *inbox) len() int {
	in.mu.Lock()
	defer in.mu.Unlock()
	return len(in.entries)
}

// ackPolicy is what a settling run reports about itself, so the inbox can tell
// the entries it consumed from the ones it merely outlived.
type ackPolicy struct {
	// Default false preserves legacy ordinary behaviour outside an admission fence.
	SuppressOrdinaryGrace bool
	// SessionID is the exact session whose delivery/activity is settling.
	SessionID string
	// Now is the settle time.
	Now time.Time
	// LastActive is when the settling run last produced assistant text or ran a
	// tool (zero when it produced neither). An entry delivered before that moment
	// was read by the model — pi injects a steer at the next tool yield — so it
	// can be acknowledged immediately. An entry delivered *after* it was not
	// consumed and must stay pending for the next run (or at the next start).
	LastActive time.Time
}

// ackSettled acknowledges everything the settling run took in: entries it
// consumed (delivered, with run activity after the delivery), and entries that
// have been pending longer than inboxAckGrace without any delivery evidence.
func (in *inbox) ackSettled(p ackPolicy) int {
	n, err := in.ackSettledErr(p)
	if err != nil {
		in.warn(err.Error())
	}
	return n
}

// Boundary callers use the error-returning variant to detect persistence failure.
func (in *inbox) ackSettledErr(p ackPolicy) (int, error) {
	in.mu.Lock()
	defer in.mu.Unlock()
	kept := make([]inboxEntry, 0, len(in.entries))
	acked := 0
	var ackLogs []string
	for _, e := range in.entries {
		consumed := !e.deliveredAt.IsZero() && !p.LastActive.IsZero() && !p.LastActive.Before(e.deliveredAt)
		eligible := consumed || (!p.SuppressOrdinaryGrace && p.Now.Sub(e.At) >= inboxAckGrace)
		if e.protected() {
			eligible = consumed && p.SessionID != "" && e.TargetSessionID == p.SessionID && e.deliveredSessionID == p.SessionID
		}
		if eligible {
			reason := "grace_period"
			if consumed {
				reason = "run_activity_after_delivery"
			}
			ackLogs = append(ackLogs, fmt.Sprintf("inbox: acknowledged stanza_id=%q reason=%s", e.ID, reason))
			acked++
			continue
		}
		kept = append(kept, e)
	}
	if acked == 0 {
		return 0, nil
	}
	if err := in.commitLocked(kept, false); err != nil {
		return 0, err
	}
	for _, msg := range ackLogs {
		in.logf("info", msg)
	}
	return acked, nil
}

// flushLocked is the legacy best-effort writer, used only by commitLocked when
// neither the old nor new queue contains protected entries. An empty queue
// removes the file.
func (in *inbox) flushLocked() {
	if len(in.entries) == 0 {
		if err := os.Remove(in.path); err != nil && !os.IsNotExist(err) {
			in.warn("remove: " + err.Error())
		}
		return
	}
	if err := os.MkdirAll(filepath.Dir(in.path), 0o700); err != nil {
		in.warn("mkdir: " + err.Error())
		return
	}
	tmp := in.path + ".tmp"
	f, err := os.OpenFile(tmp, os.O_CREATE|os.O_WRONLY|os.O_TRUNC, 0o600)
	if err != nil {
		in.warn("write: " + err.Error())
		return
	}
	w := bufio.NewWriter(f)
	for _, e := range in.entries {
		b, err := json.Marshal(e)
		if err != nil {
			continue
		}
		if _, err := w.Write(append(b, '\n')); err != nil {
			f.Close()
			os.Remove(tmp)
			in.warn("write: " + err.Error())
			return
		}
	}
	if err := w.Flush(); err != nil {
		f.Close()
		os.Remove(tmp)
		in.warn("write: " + err.Error())
		return
	}
	if err := f.Close(); err != nil {
		os.Remove(tmp)
		in.warn("write: " + err.Error())
		return
	}
	if err := os.Rename(tmp, in.path); err != nil {
		os.Remove(tmp)
		in.warn("rename: " + err.Error())
	}
}
