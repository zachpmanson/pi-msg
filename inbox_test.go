package main

import (
	"bytes"
	"context"
	"os"
	"strings"
	"sync"
	"testing"
	"time"
)

// The inbox is the durable record of inbound input, so the property that matters
// is: what was appended survives a restart, and what a settled run acknowledged
// does not come back (issue #96).
func TestInboxPersistsAcrossRestart(t *testing.T) {
	path := inboxPath("t")
	dir := t.TempDir()
	path = dir + "/acct.inbox.jsonl"

	in := newInbox(path, nil)
	in.append(inboxEntry{ID: "m1", From: "zach@x/phone", Body: "merge to master", Direct: true, FromOwner: true})
	in.append(inboxEntry{ID: "m2", Body: "then send me latest master apk", Direct: true, FromOwner: true})

	// A fresh instance is what the next process sees.
	reloaded := newInbox(path, nil)
	got := reloaded.pending()
	if len(got) != 2 {
		t.Fatalf("reloaded %d entries, want 2", len(got))
	}
	if got[0].ID != "m1" || got[0].Body != "merge to master" || !got[0].Direct || !got[0].FromOwner {
		t.Errorf("entry 0 = %+v", got[0])
	}
	if got[1].ID != "m2" || got[1].At.IsZero() {
		t.Errorf("entry 1 = %+v", got[1])
	}

	// Acknowledging empties the queue and removes the file: in steady state the
	// inbox holds nothing. The ack happens when a run settles, by which point the
	// entries are older than the grace (see TestInboxAckGraceProtectsTheJustArrived).
	if n := reloaded.ackSettled(time.Now().Add(inboxAckGrace + time.Second)); n != 2 {
		t.Errorf("acked %d, want 2", n)
	}
	if n := reloaded.len(); n != 0 {
		t.Errorf("%d entries left after ack", n)
	}
	if _, err := os.Stat(path); !os.IsNotExist(err) {
		t.Errorf("file still present after the last ack: %v", err)
	}
	if n := newInbox(path, nil).len(); n != 0 {
		t.Errorf("reload after ack = %d entries, want 0", n)
	}
}

// The ack must not swallow a message that arrived in the same instant the run
// ended: that entry may belong to the run that is only just starting, and acking
// it would lose it exactly the way the queue exists to prevent.
func TestInboxAckGraceProtectsTheJustArrived(t *testing.T) {
	path := t.TempDir() + "/acct.inbox.jsonl"
	in := newInbox(path, nil)
	in.append(inboxEntry{ID: "fresh", Body: "sent as the run ended"})

	now := time.Now()
	if n := in.ackSettled(now); n != 0 {
		t.Errorf("acked %d young entries, want 0", n)
	}
	if in.len() != 1 {
		t.Fatalf("young entry was dropped")
	}
	// Once it has been pending longer than the grace, the next settle takes it.
	if n := in.ackSettled(now.Add(inboxAckGrace + time.Second)); n != 1 {
		t.Errorf("acked %d, want 1", n)
	}
	if n := in.len(); n != 0 {
		t.Errorf("%d entries left", n)
	}

	// A mixed queue: only the settled-by-age entry goes.
	in.append(inboxEntry{ID: "old", Body: "from the run that settled", At: now.Add(-time.Minute)})
	in.append(inboxEntry{ID: "new", Body: "arrived just now"})
	if n := in.ackSettled(now); n != 1 {
		t.Errorf("acked %d, want 1 (only the old entry)", n)
	}
	if p := in.pending(); len(p) != 1 || p[0].ID != "new" {
		t.Errorf("pending after mixed ack = %+v, want just 'new'", p)
	}
}

// A run that never settles must not grow the file forever.
func TestInboxCapDropsOldest(t *testing.T) {
	path := t.TempDir() + "/acct.inbox.jsonl"
	in := newInbox(path, nil)
	for i := 0; i < inboxCap+10; i++ {
		in.append(inboxEntry{ID: string(rune('a' + i%26)), Body: "spam"})
	}
	if n := in.len(); n != inboxCap {
		t.Errorf("len = %d, want the cap %d", n, inboxCap)
	}
	// The file matches memory: cap lines, all parsable.
	reloaded := newInbox(path, nil)
	if n := len(reloaded.pending()); n != inboxCap {
		t.Errorf("reloaded %d, want %d", n, inboxCap)
	}
}

// A corrupt line is skipped, never fatal: the inbox is a safety net and must not
// be the thing that stops a bridge from starting.
func TestInboxToleratesCorruptFile(t *testing.T) {
	path := t.TempDir() + "/acct.inbox.jsonl"
	if err := os.WriteFile(path, []byte("{\"body\":\"good\",\"at\":\"2026-09-22T11:24:30Z\"}\nnot json at all\n\n"), 0o600); err != nil {
		t.Fatal(err)
	}
	in := newInbox(path, nil)
	p := in.pending()
	if len(p) != 1 || p[0].Body != "good" {
		t.Errorf("pending = %+v, want the one parsable entry", p)
	}
}

// Re-delivery after a restart goes through the normal dispatch path and hands
// the resumed run the original text plus a note explaining the repeat. The entry
// itself stays unacknowledged until the run that consumes it settles.
func TestInboxRedeliveryReachesTheSession(t *testing.T) {
	acct := ResolvedAccount{Owner: "zach@x", Name: "t"}
	b := newTestBridge(acct)
	var buf bytes.Buffer
	b.rpc = &RPCClient{stdin: &nopClose{buf: &buf}, mu: sync.Mutex{}}
	b.inbox = newInbox(t.TempDir()+"/acct.inbox.jsonl", nil)

	e := inboxEntry{ID: "m1", From: "zach@x/phone", Body: "merge to master", Direct: true, FromOwner: true}
	b.inbox.append(e)
	b.deliverInbox(e)

	prompt := buf.String()
	if !strings.Contains(prompt, "merge to master") {
		t.Fatalf("prompt does not carry the message: %q", prompt)
	}
	if !strings.Contains(prompt, inboxNote) {
		t.Errorf("prompt does not explain the re-delivery: %q", prompt)
	}
	if strings.HasPrefix(strings.TrimSpace(prompt), inboxNote) {
		t.Errorf("the note must not lead the body — a room trigger has to match: %q", prompt)
	}
	if b.inbox.len() != 1 {
		t.Errorf("entry was acknowledged before any run settled")
	}
	// The run that takes it in settles: now it can be acknowledged.
	b.inbox.append(inboxEntry{ID: "old", Body: "x", At: time.Now().Add(-time.Minute)})
	b.ackInboxSettled()
	if n := b.inbox.len(); n != 1 {
		t.Errorf("%d entries left, want just the young one", n)
	}
}

// A room message re-delivered from the inbox is classified again, so an
// untriggered remark cannot become a prompt just because it was recovered.
func TestInboxRedeliveryRespectsRoomRules(t *testing.T) {
	acct := ResolvedAccount{Owner: "zach@x", Name: "t", Rooms: []string{"team@muc.x"}, RoomTrigger: "pi"}
	b := newTestBridge(acct)
	var buf bytes.Buffer
	b.rpc = &RPCClient{stdin: &nopClose{buf: &buf}, mu: sync.Mutex{}}
	b.inbox = newInbox(t.TempDir()+"/acct.inbox.jsonl", nil)

	// An ambient (untriggered, non-owner) remark: buffered as context, no turn.
	b.deliverInbox(inboxEntry{ID: "r1", Room: "team@muc.x", Nick: "slippy", Body: "roster shows peppy and slippy"})
	if strings.Contains(buf.String(), "roster shows peppy") {
		t.Errorf("an untriggered room remark must not be prompted on re-delivery: %q", buf.String())
	}
	if !strings.Contains(b.drainAmbient(), "roster shows peppy") {
		t.Errorf("it should be buffered as ambient context instead")
	}

	// Addressed: it prompts, and the trigger still matches with the note appended.
	b.deliverInbox(inboxEntry{ID: "r2", Room: "team@muc.x", Nick: "zach", RealJID: "zach@x", FromOwner: true, Body: "pi: do it"})
	if !strings.Contains(buf.String(), "do it") {
		t.Errorf("an addressed message must prompt on re-delivery: %q", buf.String())
	}
}

// The inbox and the archive backfill must not cancel each other out. A steered
// message was delivered LIVE in the process that then died, so its stanza id is
// exactly the kind of id the dedup map remembers — filtering the inbox by
// "have I seen this?" would suppress the very message the queue exists to
// recover (#96). Archived copies of messages that did arrive live stay deduped.
func TestInboxRedeliveryIsNotSuppressedBySeen(t *testing.T) {
	acct := ResolvedAccount{Owner: "zach@x", Name: "t"}
	b := newTestBridge(acct)
	b.ctx = context.Background()
	var buf bytes.Buffer
	b.rpc = &RPCClient{stdin: &nopClose{buf: &buf}, mu: sync.Mutex{}}
	b.inbox = newInbox(t.TempDir()+"/acct.inbox.jsonl", nil)

	e := inboxEntry{ID: "live-1", From: "zach@x/phone", Body: "merge to master", Direct: true, FromOwner: true}
	b.xmpp.markSeen(e.ID) // as the process that took it in live would have
	b.inbox.append(e)

	b.deliverRecovered(nil, b.inbox.pending())
	out := buf.String()
	if !strings.Contains(out, "merge to master") {
		t.Errorf("an unacknowledged inbox entry must be re-delivered even though its id was seen: %q", out)
	}

	// An archived copy of a message that arrived live is still skipped.
	b.xmpp.markSeen("archived-1")
	buf.Reset()
	b.deliverRecovered([]InboundMessage{{ID: "archived-1", Body: "already had this", Direct: true, FromOwner: true}}, nil)
	if strings.Contains(buf.String(), "already had this") {
		t.Errorf("an archived duplicate must still be skipped: %q", buf.String())
	}

	// Buffer and inbox arrive in one block, banner counted once.
	buf.Reset()
	b.deliverRecovered(
		[]InboundMessage{{ID: "fresh-1", Body: "from the archive", Direct: true, FromOwner: true}},
		[]inboxEntry{{ID: "live-2", Body: "from the inbox", Direct: true, FromOwner: true}},
	)
	out = buf.String()
	if !strings.Contains(out, "from the archive") || !strings.Contains(out, "from the inbox") {
		t.Errorf("both sources must be delivered: %q", out)
	}
}
