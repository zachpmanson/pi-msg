package main

import (
	"bufio"
	"encoding/json"
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
// Messages that never become a prompt (buffered ambient chatter, a bridge
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

// inboxCap bounds the file. Entries past it are dropped oldest-first with a
// warning: a run that never settles must not grow the inbox forever.
const inboxCap = 500

// inboxEntry is one inbound message received by the bridge but not yet
// acknowledged by a settled run.
type inboxEntry struct {
	ID        string `json:"id,omitempty"`
	From      string `json:"from,omitempty"`
	Body      string `json:"body"`
	Room      string `json:"room,omitempty"`
	Nick      string `json:"nick,omitempty"`
	RealJID   string `json:"realJID,omitempty"`
	FromOwner bool   `json:"fromOwner,omitempty"`
	Direct    bool   `json:"direct,omitempty"`
	// ReplyToID is the XEP-0461 stamp of the message this one answers, so a
	// re-delivered reply still names its target (#95).
	ReplyToID string    `json:"replyToID,omitempty"`
	At        time.Time `json:"at"`

	// deliveredAt is when pi was handed this message during the current process.
	// In-memory only: a delivery does not survive a restart (the point of
	// re-delivery is that the new process has not seen the entry yet), and a
	// stale on-disk value would let a settle in the new process acknowledge an
	// entry it never delivered.
	deliveredAt time.Time
}

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
		Room:      e.Room,
		ID:        e.ID,
		From:      e.From,
	}
}

// inbox is the per-account durable queue. Safe for concurrent use: the XMPP
// read loop appends while the RPC event loop acknowledges.
type inbox struct {
	mu      sync.Mutex
	path    string
	log     func(level, msg string)
	entries []inboxEntry
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
			in.warn("open: " + err.Error())
		}
		return
	}
	defer f.Close()
	sc := bufio.NewScanner(f)
	sc.Buffer(make([]byte, 0, 64*1024), 1024*1024)
	var entries []inboxEntry
	for sc.Scan() {
		line := sc.Bytes()
		if len(line) == 0 {
			continue
		}
		var e inboxEntry
		if err := json.Unmarshal(line, &e); err != nil {
			in.warn("skipping unparsable entry: " + err.Error())
			continue
		}
		entries = append(entries, e)
	}
	if err := sc.Err(); err != nil {
		in.warn("read: " + err.Error())
	}
	if len(entries) > 0 {
		in.logf("info", fmt.Sprintf("%d unacknowledged inbound message(s) recovered from %s", len(entries), filepath.Base(in.path)))
	}
	in.entries = entries
}

func (in *inbox) logf(level, msg string) {
	if in.log != nil {
		in.log(level, msg)
	}
}

// append records a message before it is handed to pi, and returns the entry.
func (in *inbox) append(e inboxEntry) {
	if e.At.IsZero() {
		e.At = time.Now()
	}
	in.mu.Lock()
	defer in.mu.Unlock()
	in.entries = append(in.entries, e)
	if len(in.entries) > inboxCap {
		drop := len(in.entries) - inboxCap
		in.entries = in.entries[drop:]
		in.warn(fmt.Sprintf("over %d entries; dropped the oldest %d", inboxCap, drop))
	}
	in.flushLocked()
}

// pending copies the unacknowledged entries in arrival order.
func (in *inbox) pending() []inboxEntry {
	in.mu.Lock()
	defer in.mu.Unlock()
	return append([]inboxEntry(nil), in.entries...)
}

// markDelivered records that pi was handed this message, so the settle that ends
// the run can acknowledge it without waiting for the grace. It matches by stanza
// id when there is one, else by (from, body). Returns how many entries matched.
func (in *inbox) markDelivered(id, from, body string, at time.Time) int {
	in.mu.Lock()
	defer in.mu.Unlock()
	n := 0
	for i := range in.entries {
		if matchesEntry(in.entries[i], id, from, body) {
			in.entries[i].deliveredAt = at
			n++
		}
	}
	return n
}

// drop removes entries for a message that never became a prompt — buffered
// ambient chatter, a bridge command handled in-process, a dropped own-echo. No
// run will ever settle for these, so they must not wait for an ack: before issue
// #104 they stayed pending and were re-delivered as "unacknowledged" on every
// restart.
func (in *inbox) drop(id, from, body string) int {
	in.mu.Lock()
	defer in.mu.Unlock()
	kept := in.entries[:0]
	dropped := 0
	for _, e := range in.entries {
		if matchesEntry(e, id, from, body) {
			dropped++
			continue
		}
		kept = append(kept, e)
	}
	if dropped == 0 {
		return 0
	}
	in.entries = kept
	in.flushLocked()
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
	in.mu.Lock()
	defer in.mu.Unlock()
	kept := in.entries[:0]
	acked := 0
	for _, e := range in.entries {
		consumed := !e.deliveredAt.IsZero() && !p.LastActive.IsZero() && !p.LastActive.Before(e.deliveredAt)
		if consumed || p.Now.Sub(e.At) >= inboxAckGrace {
			acked++
			continue
		}
		kept = append(kept, e)
	}
	if acked == 0 {
		return 0
	}
	in.entries = kept
	in.flushLocked()
	return acked
}

// flushLocked rewrites the file with what remains, atomically (temp + rename) so
// a crash mid-write cannot truncate the queue. An empty queue removes the file.
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
