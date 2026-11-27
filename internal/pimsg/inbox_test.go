package pimsg

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
	if n := reloaded.ackSettled(ackPolicy{Now: time.Now().Add(inboxAckGrace + time.Second)}); n != 2 {
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
// ended and was never handed to pi: it may belong to the run that is only just
// starting, and acking it would lose it exactly the way the queue exists to
// prevent. This is the backstop — an entry that *was* delivered is judged by its
// delivery stamp instead (TestInboxAckConsumesWhatTheRunRead).
func TestInboxAckGraceProtectsTheJustArrived(t *testing.T) {
	path := t.TempDir() + "/acct.inbox.jsonl"
	in := newInbox(path, nil)
	in.append(inboxEntry{ID: "fresh", Body: "sent as the run ended"})

	now := time.Now()
	if n := in.ackSettled(ackPolicy{Now: now}); n != 0 {
		t.Errorf("acked %d young entries, want 0", n)
	}
	if in.len() != 1 {
		t.Fatalf("young entry was dropped")
	}
	// Once it has been pending longer than the grace, the next settle takes it.
	if n := in.ackSettled(ackPolicy{Now: now.Add(inboxAckGrace + time.Second)}); n != 1 {
		t.Errorf("acked %d, want 1", n)
	}
	if n := in.len(); n != 0 {
		t.Errorf("%d entries left", n)
	}

	// A mixed queue: only the settled-by-age entry goes.
	in.append(inboxEntry{ID: "old", Body: "from the run that settled", At: now.Add(-time.Minute)})
	in.append(inboxEntry{ID: "new", Body: "arrived just now"})
	if n := in.ackSettled(ackPolicy{Now: now}); n != 1 {
		t.Errorf("acked %d, want 1 (only the old entry)", n)
	}
	if p := in.pending(); len(p) != 1 || p[0].ID != "new" {
		t.Errorf("pending after mixed ack = %+v, want just 'new'", p)
	}
}

// The defect #104 was an entry that the run did read staying pending because it
// arrived inside the grace window. A run that took the message into its context
// after handing it to pi has consumed it, so the settle acknowledges it at once.
func TestInboxAckConsumesWhatTheRunRead(t *testing.T) {
	path := t.TempDir() + "/acct.inbox.jsonl"
	in := newInbox(path, nil)
	delivered := time.Now()
	in.append(inboxEntry{ID: "m1", Body: "merge to master", At: delivered})
	if n := in.markDelivered("m1", "", "", delivered); n != 1 {
		t.Fatalf("marked %d deliveries, want 1", n)
	}

	// The run read it: a message entered the context a second after delivery.
	p := ackPolicy{Now: delivered.Add(2 * time.Second), LastActive: delivered.Add(time.Second)}
	if n := in.ackSettled(p); n != 1 {
		t.Errorf("acked %d, want the consumed entry", n)
	}
	if in.len() != 0 {
		t.Errorf("entry survived a settle that read it: %d left", in.len())
	}
	if n := newInbox(path, nil).len(); n != 0 {
		t.Errorf("a restart would re-deliver a consumed message: %d pending", n)
	}
}

// The other half of #104: a message handed to pi that the run never read — a
// steer pi did not yield, or one that landed as the run ended — must stay
// pending, regardless of how little time passed (#96).
func TestInboxAckKeepsDeliveredButUnread(t *testing.T) {
	path := t.TempDir() + "/acct.inbox.jsonl"
	in := newInbox(path, nil)
	now := time.Now()
	in.append(inboxEntry{ID: "m1", Body: "steered, never yielded", At: now})
	in.markDelivered("m1", "", "", now)

	// The run produced nothing at all.
	if n := in.ackSettled(ackPolicy{Now: now.Add(time.Second)}); n != 0 {
		t.Errorf("acked %d unread entries, want 0", n)
	}
	// The run's last activity predates the delivery, so it cannot have read it:
	// this is the message that arrived mid-run and settled before the yield.
	if n := in.ackSettled(ackPolicy{Now: now.Add(time.Second), LastActive: now.Add(-time.Minute)}); n != 0 {
		t.Errorf("acked %d entries older than the run's activity, want 0", n)
	}
	if p := in.pending(); len(p) != 1 {
		t.Errorf("unread entry was lost: %+v", p)
	}
	// A later run reads it: the next settle takes it.
	if n := in.ackSettled(ackPolicy{Now: now.Add(time.Minute), LastActive: now.Add(30 * time.Second)}); n != 1 {
		t.Errorf("acked %d after a run read it, want 1", n)
	}
}

// Not every inbound message becomes a prompt. One that never does — ambient
// room chatter, a bridge command, a dropped own-echo — is dropped outright:
// there is no run to settle for it, and before #104 it waited in the file until
// the next restart announced it as unacknowledged.
func TestInboxDropsMessagesThatNeverPrompt(t *testing.T) {
	path := t.TempDir() + "/acct.inbox.jsonl"
	in := newInbox(path, nil)
	in.append(inboxEntry{ID: "m1", From: "slippy@x/room", Body: "ambient remark"})
	in.append(inboxEntry{ID: "m2", Body: "/status"})
	// No stanza id: the (from, body) fallback still finds it.
	in.append(inboxEntry{From: "slippy@x/room", Body: "another remark"})
	in.append(inboxEntry{ID: "keep", Body: "a real instruction"})

	if n := in.drop("m1", "", ""); n != 1 {
		t.Errorf("dropped %d by id, want 1", n)
	}
	if n := in.drop("m2", "", ""); n != 1 {
		t.Errorf("dropped %d commands, want 1", n)
	}
	if n := in.drop("", "slippy@x/room", "another remark"); n != 1 {
		t.Errorf("dropped %d by from+body, want 1", n)
	}
	if n := in.drop("nope", "", ""); n != 0 {
		t.Errorf("dropped %d entries for an unknown id, want 0", n)
	}
	if p := in.pending(); len(p) != 1 || p[0].ID != "keep" {
		t.Errorf("pending = %+v, want only the real instruction", p)
	}
	if n := newInbox(path, nil).len(); n != 1 {
		t.Errorf("the file still carries dropped entries: %d", n)
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

// A live message that becomes a prompt is marked delivered, and the settle of the
// run that read it takes it — even though it arrived seconds before the settle.
// This is the defect #104 in situ: an instruction from yesterday announced as
// fresh catch-up on every restart.
func TestLivePromptIsAckedByTheRunThatReadIt(t *testing.T) {
	acct := ResolvedAccount{Owner: "zach@x", Name: "t"}
	b := newTestBridge(acct)
	b.ctx = context.Background()
	var buf bytes.Buffer
	b.rpc = &RPCClient{stdin: &nopClose{buf: &buf}, mu: sync.Mutex{}}
	b.inbox = newInbox(t.TempDir()+"/acct.inbox.jsonl", nil)

	m := InboundMessage{ID: "live-1", From: "zach@x/phone", Body: "merge to master", Direct: true, FromOwner: true}
	b.onInbound(m)
	if !strings.Contains(buf.String(), "merge to master") {
		t.Fatalf("no prompt went out: %q", buf.String())
	}
	if n := b.inbox.len(); n != 1 {
		t.Fatalf("%d entries after the live hand-off, want 1 (unacknowledged until settle)", n)
	}

	// Pi takes the message into its context, then the run settles a moment later.
	b.handleRPCEvent(Event{"type": "agent_start"})
	b.handleRPCEvent(Event{"type": "message_start", "message": map[string]any{"role": "user"}})
	b.handleRPCEvent(Event{"type": "agent_settled"})
	if n := b.inbox.len(); n != 0 {
		t.Errorf("%d entries left after the settle that read the message, want 0", n)
	}
	if n := newInbox(b.inbox.path, nil).len(); n != 0 {
		t.Errorf("a restart would re-deliver it: %d pending", n)
	}
}

// A run that never read the delivery leaves it pending: the next start (or the
// next run) re-delivers it rather than losing it (#96).
func TestInboxKeepsAnUnreadLivePrompt(t *testing.T) {
	acct := ResolvedAccount{Owner: "zach@x", Name: "t"}
	b := newTestBridge(acct)
	b.ctx = context.Background()
	b.rpc = &RPCClient{stdin: &nopClose{buf: &bytes.Buffer{}}, mu: sync.Mutex{}}
	b.inbox = newInbox(t.TempDir()+"/acct.inbox.jsonl", nil)

	b.onInbound(InboundMessage{ID: "live-1", From: "zach@x/phone", Body: "stop", Direct: true, FromOwner: true})
	b.handleRPCEvent(Event{"type": "agent_start"})
	// The run ends without ever starting a message of its own.
	b.handleRPCEvent(Event{"type": "agent_settled"})
	if n := b.inbox.len(); n != 1 {
		t.Errorf("%d entries left, want the unread one kept", n)
	}
}

// Untriggered room chatter never becomes a prompt and is not part of the
// agent's world at all (#106), so it must not be queued and must not be held
// anywhere else either.
func TestAmbientRoomChatterIsNotQueued(t *testing.T) {
	acct := ResolvedAccount{Owner: "zach@x", Name: "t", Rooms: []string{"team@muc.x"}, RoomTrigger: "pi"}
	b := newTestBridge(acct)
	b.ctx = context.Background()
	b.rpc = &RPCClient{stdin: &nopClose{buf: &bytes.Buffer{}}, mu: sync.Mutex{}}
	b.inbox = newInbox(t.TempDir()+"/acct.inbox.jsonl", nil)

	b.onInbound(InboundMessage{ID: "r1", Room: "team@muc.x", Nick: "slippy", From: "team@muc.x/slippy", Body: "roster shows peppy"})
	if n := b.inbox.len(); n != 0 {
		t.Errorf("unaddressed remark left %d entries in the durable queue, want 0", n)
	}
	// An addressed message still queues: it becomes a prompt, so a settle can ack it.
	b.onInbound(InboundMessage{ID: "r2", Room: "team@muc.x", Nick: "zach", From: "team@muc.x/zach", Body: "pi: do it", FromOwner: true})
	if n := b.inbox.len(); n != 1 {
		t.Errorf("an addressed room message left %d entries, want 1", n)
	}
	// A message with nothing to prompt never queues either — the empty body and
	// a bridge-handled command take the same drop path (the command half is
	// unit-tested in TestInboxDropsMessagesThatNeverPrompt).
	b.inbox.append(inboxEntry{ID: "c1", From: "zach@x/phone", Body: "   ", Direct: true, FromOwner: true})
	b.handleCanonical("   ", InboundMessage{Direct: true, ID: "c1"})
	if n := b.inbox.len(); n != 1 {
		t.Errorf("a handled command left %d entries, want the count unchanged", n)
	}
}

// A room message re-delivered from the inbox is classified again, so a message
// that no longer addresses anyone cannot become a prompt just because it was
// recovered — and it is dropped from the queue rather than parked (#104).
func TestInboxRedeliveryRespectsRoomRules(t *testing.T) {
	acct := ResolvedAccount{Owner: "zach@x", Name: "t", Rooms: []string{"team@muc.x"}, RoomTrigger: "pi"}
	b := newTestBridge(acct)
	var buf bytes.Buffer
	b.rpc = &RPCClient{stdin: &nopClose{buf: &buf}, mu: sync.Mutex{}}
	b.inbox = newInbox(t.TempDir()+"/acct.inbox.jsonl", nil)

	// An unaddressed (untriggered, non-owner) remark: no turn, and dropped.
	b.deliverInbox(inboxEntry{ID: "r1", Room: "team@muc.x", Nick: "slippy", Body: "roster shows zephyr and quill"})
	if strings.Contains(buf.String(), "roster shows zephyr") {
		t.Errorf("an addressee-less room remark must not be prompted on re-delivery: %q", buf.String())
	}
	if n := b.inbox.len(); n != 0 {
		t.Errorf("the remark was left in the queue: %d entries, want 0", n)
	}

	// Addressed: it prompts (as a pointer block — the body is pulled with
	// read_messages, #58) even with the re-delivery note appended.
	b.deliverInbox(inboxEntry{ID: "r2", Room: "team@muc.x", Nick: "zach", RealJID: "zach@x", FromOwner: true, Body: "pi: do it"})
	if !strings.Contains(buf.String(), "[pi-msg: room: You were tagged") || !strings.Contains(buf.String(), "stanza-id: r2") {
		t.Errorf("an addressed message must prompt on re-delivery: %q", buf.String())
	}
	if strings.Contains(buf.String(), "do it") {
		t.Errorf("the body must not enter the prompt: %q", buf.String())
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

func TestInboxProtectedSessionAndRestart(t *testing.T) {
	path := t.TempDir() + "/private/inbox.jsonl"
	at := time.Unix(100, 0).UTC()
	in := newInbox(path, nil)
	e, err := in.appendProtected(inboxEntry{NextBoundaryID: "boundary", Body: "held", At: at})
	if err != nil || e.Key == "" {
		t.Fatalf("append: %+v %v", e, err)
	}
	for i := 0; i < 3; i++ {
		if n := in.ackSettled(ackPolicy{Now: at.Add(24 * time.Hour), SessionID: "old", LastActive: at.Add(time.Hour)}); n != 0 {
			t.Fatal("aged protected entry acknowledged")
		}
		in = newInbox(path, nil)
	}
	if n, err := in.assignTargetSession("boundary", "fresh"); err != nil || n != 1 {
		t.Fatalf("assign = %d %v", n, err)
	}
	in = newInbox(path, nil)
	got := in.protectedPending()
	if len(got) != 1 || got[0].Key != e.Key || got[0].NextBoundaryID != "boundary" || got[0].TargetSessionID != "fresh" {
		t.Fatalf("lost tags: %+v", got)
	}
	got[0].Body = "mutated copy"
	if in.protectedPending()[0].Body != "held" {
		t.Fatal("pending aliases queue")
	}
	if ok, err := in.markDeliveredKey(e.Key, "old", at); err == nil || ok {
		t.Fatalf("wrong delivery = %v %v", ok, err)
	}
	if n := in.markDelivered("", "", "held", at); n != 0 {
		t.Fatal("legacy mark touched protected entry")
	}
	if n := in.ackSettled(ackPolicy{Now: at.Add(24 * time.Hour), SessionID: "fresh", LastActive: at.Add(time.Hour)}); n != 0 {
		t.Fatal("assigned entry age-acked")
	}
	if ok, err := in.markDeliveredKey(e.Key, "fresh", at); err != nil || !ok {
		t.Fatalf("delivery = %v %v", ok, err)
	}
	for _, p := range []ackPolicy{
		{Now: at.Add(24 * time.Hour), SessionID: "old", LastActive: at.Add(time.Hour)},
		{Now: at.Add(24 * time.Hour), LastActive: at.Add(time.Hour)},
		{Now: at.Add(24 * time.Hour), SessionID: "fresh"},
		{Now: at.Add(24 * time.Hour), SessionID: "fresh", LastActive: at.Add(-time.Second)},
	} {
		if n := in.ackSettled(p); n != 0 {
			t.Fatalf("invalid evidence acked: %+v", p)
		}
	}
	// Delivery evidence must not survive a process restart.
	in = newInbox(path, nil)
	p := ackPolicy{Now: at.Add(24 * time.Hour), SessionID: "fresh", LastActive: at.Add(time.Hour)}
	if n := in.ackSettled(p); n != 0 {
		t.Fatal("persisted delivery evidence")
	}
	if ok, err := in.markDeliveredKey(e.Key, "fresh", at); err != nil || !ok {
		t.Fatal(err)
	}
	if n, err := in.ackSettledErr(p); err != nil || n != 1 {
		t.Fatalf("consume = %d %v", n, err)
	}
	if n := newInbox(path, nil).len(); n != 0 {
		t.Fatalf("consumed entry reloaded: %d", n)
	}
}

func TestInboxProtectedUniqueKeys(t *testing.T) {
	in := newInbox(t.TempDir()+"/inbox.jsonl", nil)
	now := time.Unix(100, 0)
	for _, id := range []string{"duplicate", ""} {
		var entries []inboxEntry
		for _, room := range []string{"a@rooms", "b@rooms"} {
			e, err := in.appendProtected(inboxEntry{ID: id, From: "same", Room: room, Body: "identical", NextBoundaryID: "b", TargetSessionID: "s", At: now})
			if err != nil {
				t.Fatal(err)
			}
			entries = append(entries, e)
		}
		if entries[0].Key == entries[1].Key {
			t.Fatal("duplicate keys")
		}
		if n := in.markDelivered(id, "same", "identical", now); n != 0 {
			t.Fatal("legacy mark touched protected")
		}
		if n := in.drop(id, "same", "identical"); n != 0 {
			t.Fatal("legacy drop touched protected")
		}
		if ok, err := in.markDeliveredKey(entries[1].Key, "s", now); err != nil || !ok {
			t.Fatalf("mark = %v %v", ok, err)
		}
		if ok, err := in.dropKey(entries[0].Key); err != nil || !ok {
			t.Fatalf("drop = %v %v", ok, err)
		}
		if n, err := in.ackSettledErr(ackPolicy{Now: now.Add(time.Hour), SessionID: "s", LastActive: now}); err != nil || n != 1 {
			t.Fatalf("ack = %d %v", n, err)
		}
		if in.len() != 0 {
			t.Fatal("key operation changed wrong entry")
		}
	}
}

func TestInboxProtectedCap(t *testing.T) {
	path := t.TempDir() + "/inbox.jsonl"
	warned := false
	in := newInbox(path, func(level, msg string) {
		if strings.Contains(msg, "protected storage exceeds") {
			warned = true
		}
	})
	for i := 0; i < inboxCap+2; i++ {
		if _, err := in.appendProtected(inboxEntry{Body: "held", NextBoundaryID: "b"}); err != nil {
			t.Fatal(err)
		}
	}
	for i := 0; i < inboxCap+3; i++ {
		in.append(inboxEntry{Body: "ordinary"})
	}
	if !warned {
		t.Fatal("no protected-cap warning")
	}
	in = newInbox(path, nil)
	if in.len() != 2*inboxCap+2 || len(in.protectedPending()) != inboxCap+2 {
		t.Fatalf("cap lost protected input: total=%d protected=%d", in.len(), len(in.protectedPending()))
	}
	if n := in.ackSettled(ackPolicy{Now: time.Now().Add(time.Hour)}); n != inboxCap {
		t.Fatalf("ordinary ack = %d", n)
	}
	if len(newInbox(path, nil).protectedPending()) != inboxCap+2 {
		t.Fatal("rewrite lost protected entries")
	}
}

func TestInboxProtectedLoadDiagnostics(t *testing.T) {
	for _, data := range []string{
		"not json\n",
		"{\"body\":\"legacy\"}\nnot json\n",
		"{\"body\":\"held\",\"nextBoundaryID\":\"b\"}\n",
		"{\"key\":\"k\",\"nextBoundaryID\":\"b\"}\n{\"key\":\"k\",\"targetSessionID\":\"s\"}\n",
	} {
		path := t.TempDir() + "/inbox.jsonl"
		if err := os.WriteFile(path, []byte(data), 0o600); err != nil {
			t.Fatal(err)
		}
		in := newInbox(path, nil)
		if in.loadError() == nil {
			t.Fatalf("missing diagnostic for %q", data)
		}
		if _, err := in.appendProtected(inboxEntry{NextBoundaryID: "b"}); err == nil {
			t.Fatal("overwrote corrupt inbox")
		}
		if _, err := in.assignTargetSession("b", "s"); err == nil {
			t.Fatal("assigned corrupt inbox")
		}
		if _, err := in.dropKey("k"); err == nil {
			t.Fatal("dropped from corrupt inbox")
		}
		if _, err := in.markDeliveredKey("k", "s", time.Unix(1, 0)); err == nil {
			t.Fatal("marked corrupt inbox")
		}
		got, err := os.ReadFile(path)
		if err != nil || string(got) != data {
			t.Fatal("failed operation changed disk")
		}
	}
	in := newInbox(t.TempDir(), nil)
	if in.loadError() == nil {
		t.Fatal("missing unreadable diagnostic")
	}
}

func TestInboxProtectedFailedPersistence(t *testing.T) {
	path := t.TempDir() + "/inbox.jsonl"
	in := newInbox(path, nil)
	e, err := in.appendProtected(inboxEntry{NextBoundaryID: "b", Body: "prior", At: time.Unix(1, 0)})
	if err != nil {
		t.Fatal(err)
	}
	before, err := os.ReadFile(path)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := in.appendProtected(inboxEntry{NextBoundaryID: "b", At: time.Date(10000, 1, 1, 0, 0, 0, 0, time.UTC)}); err == nil {
		t.Fatal("invalid timestamp persisted")
	}
	if in.len() != 1 {
		t.Fatal("failed save committed memory")
	}
	after, _ := os.ReadFile(path)
	if string(after) != string(before) {
		t.Fatal("failed save changed disk")
	}
	// A nonempty directory forces a deterministic rename error, even as root.
	blocked := t.TempDir()
	if err := os.WriteFile(blocked+"/marker", []byte("prior"), 0o600); err != nil {
		t.Fatal(err)
	}
	in.path = blocked
	if _, err := in.appendProtected(inboxEntry{NextBoundaryID: "b"}); err == nil {
		t.Fatal("append reported success")
	}
	if _, err := in.assignTargetSession("b", "s"); err == nil {
		t.Fatal("assignment reported success")
	}
	if _, err := in.dropKey(e.Key); err == nil {
		t.Fatal("drop reported success")
	}
	in.append(inboxEntry{Body: "ordinary"})
	if in.len() != 1 || in.pending()[0].TargetSessionID != "" {
		t.Fatalf("failure changed memory: %+v", in.pending())
	}
	// Simulate prior valid process-local delivery so acknowledgement also rewrites.
	in.entries[0].TargetSessionID = "s"
	if _, err := in.markDeliveredKey(e.Key, "s", time.Unix(2, 0)); err != nil {
		t.Fatal(err)
	}
	if n, err := in.ackSettledErr(ackPolicy{Now: time.Unix(100, 0), LastActive: time.Unix(3, 0), SessionID: "s"}); err == nil || n != 0 {
		t.Fatalf("ack failure = %d %v", n, err)
	}
	if in.len() != 1 {
		t.Fatal("failed ack changed memory")
	}
	if got, err := os.ReadFile(blocked + "/marker"); err != nil || string(got) != "prior" {
		t.Fatal("rename changed prior destination")
	}
	after, _ = os.ReadFile(path)
	if string(after) != string(before) {
		t.Fatal("rename failure changed prior inbox file")
	}
}

func TestInboxProtectedValidationAndModes(t *testing.T) {
	path := t.TempDir() + "/private/inbox.jsonl"
	in := newInbox(path, nil)
	if _, err := in.appendProtected(inboxEntry{TargetSessionID: "s"}); err == nil {
		t.Fatal("accepted without boundary")
	}
	e, err := in.appendProtected(inboxEntry{Key: "explicit", NextBoundaryID: "b"})
	if err != nil {
		t.Fatal(err)
	}
	if _, err := in.appendProtected(e); err == nil {
		t.Fatal("accepted duplicate key")
	}
	if _, err := in.assignTargetSession("b", ""); err == nil {
		t.Fatal("accepted empty target")
	}
	if _, err := in.assignTargetSession("b", "s"); err != nil {
		t.Fatal(err)
	}
	if _, err := in.assignTargetSession("b", "other"); err == nil {
		t.Fatal("retargeted assigned entry")
	}
	st, err := os.Stat(path)
	if err != nil || st.Mode().Perm() != 0o600 {
		t.Fatalf("private file: %v %v", st, err)
	}
	st, err = os.Stat(strings.TrimSuffix(path, "/inbox.jsonl"))
	if err != nil || st.Mode().Perm() != 0o700 {
		t.Fatalf("private directory: %v %v", st, err)
	}
}

func TestInboxProtectedAssignmentFIFOAndTargetOnly(t *testing.T) {
	path := t.TempDir() + "/inbox.jsonl"
	in := newInbox(path, nil)
	at := time.Unix(1, 0)
	var keys []string
	for _, boundary := range []string{"b", "other", "b"} {
		e, err := in.appendProtected(inboxEntry{NextBoundaryID: boundary, Body: boundary, At: at})
		if err != nil {
			t.Fatal(err)
		}
		keys = append(keys, e.Key)
	}
	if n, err := in.assignTargetSession("b", "fresh"); err != nil || n != 2 {
		t.Fatalf("assign = %d %v", n, err)
	}
	in = newInbox(path, nil)
	for i, e := range in.protectedPending() {
		if e.Key != keys[i] {
			t.Fatal("assignment changed FIFO")
		}
		want := "fresh"
		if i == 1 {
			want = ""
		}
		if e.TargetSessionID != want {
			t.Fatalf("target = %q want %q", e.TargetSessionID, want)
		}
	}
	// Protection is retained even when only the target tag is present on disk.
	if err := os.WriteFile(path, []byte("{\"key\":\"target-only\",\"targetSessionID\":\"fresh\",\"body\":\"held\",\"at\":\"1970-01-01T00:00:01Z\"}\n"), 0o600); err != nil {
		t.Fatal(err)
	}
	in = newInbox(path, nil)
	if in.loadError() != nil {
		t.Fatal(in.loadError())
	}
	if in.drop("", "", "held") != 0 || in.markDelivered("", "", "held", at) != 0 {
		t.Fatal("legacy paths mutated target-only entry")
	}
	if n := in.ackSettled(ackPolicy{Now: at.Add(24 * time.Hour), SessionID: "fresh", LastActive: at}); n != 0 {
		t.Fatal("target-only entry age-acked")
	}
	if ok, err := in.markDeliveredKey("target-only", "fresh", at); err != nil || !ok {
		t.Fatalf("mark = %v %v", ok, err)
	}
	if n, err := in.ackSettledErr(ackPolicy{Now: at.Add(time.Hour), SessionID: "fresh", LastActive: at}); err != nil || n != 1 {
		t.Fatalf("consume = %d %v", n, err)
	}
}
