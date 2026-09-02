package main

import (
	"encoding/xml"
	"strings"
	"testing"

	"mellium.im/xmpp/jid"
	"mellium.im/xmpp/stanza"
)

// A well-formed stanza id is a third routing target form, alongside a jid and
// "noop" (#54). It must not collide with either, and it must not swallow prose.
func TestRouteLineStanzaID(t *testing.T) {
	const id = "3e2597d4-a470-4cdb-b972-431043bce34f"
	cases := []struct {
		in      string
		dest    string
		replyTo string
		inline  string
		ok      bool
	}{
		// The new form.
		{"to: " + id, "", id, "", true},
		{"  to: " + id + " here you go", "", id, "here you go", true},
		{"to: 3E2597D4-A470-4CDB-B972-431043BCE34F", "", "3E2597D4-A470-4CDB-B972-431043BCE34F", "", true},
		// pi-msg's own newStanzaID format: 16 bare hex characters.
		{"to: 3e2597d4a4704cdb", "", "3e2597d4a4704cdb", "", true},
		// Rejected shapes: a full id is required, so a wrong id is loud.
		{"to: 3e2597d4-a470-4cdb-b972-431043bce34", "", "", "", false},   // one hex digit short
		{"to: 3e2597d4-a470-4cdb-b972-431043bce34fa", "", "", "", false}, // one too many
		{"to: 3e2597d4a4704cd", "", "", "", false},                       // 15 bare hex, one short
		{"to: 3e2597d4a4704cdbb", "", "", "", false},                     // 17 bare hex, one too many
		{"to: 3e2597d4-a470-4cdb-b972-431043bce34g", "", "", "", false},  // non-hex digit
		{"to: whom it may concern", "", "", "", false},                   // prose
		// Regression: the two existing forms are untouched.
		{"to: zach@x.com", "zach@x.com", "", "", true},
		{"to: zach@x.com hello", "zach@x.com", "", "hello", true},
		{"to: noop", "noop", "", "", true},
	}
	for _, c := range cases {
		dest, replyTo, inline, ok := routeLine(c.in)
		if ok != c.ok || dest != c.dest || replyTo != c.replyTo || inline != c.inline {
			t.Errorf("routeLine(%q) = (%q,%q,%q,%v), want (%q,%q,%q,%v)",
				c.in, dest, replyTo, inline, ok, c.dest, c.replyTo, c.inline, c.ok)
		}
	}
}

// One run may answer several messages, each stamped to its own — the shape the
// reverted lastReplyTo FIFO (#50/#51) structurally could not express.
func TestSplitReplySegmentsPerMessageRoutes(t *testing.T) {
	const a = "3e2597d4-a470-4cdb-b972-431043bce34f"
	const bID = "a8508c81-0e1b-4e48-ae16-61256b837670"
	segs, leading := splitReplySegments("to: " + a + "\nA\n\nto: " + bID + "\nB")
	if leading != "" {
		t.Fatalf("leading = %q, want empty", leading)
	}
	if len(segs) != 2 {
		t.Fatalf("got %d segments, want 2", len(segs))
	}
	if segs[0].replyTo != a || segs[0].body != "A" || segs[0].dest != "" {
		t.Errorf("segment 0 = %+v", segs[0])
	}
	if segs[1].replyTo != bID || segs[1].body != "B" || segs[1].dest != "" {
		t.Errorf("segment 1 = %+v", segs[1])
	}
}

// stagedNudgeReason reads the reason of the staged routing correction, or ""
// when nothing was rejected.
func stagedNudgeReason(b *Bridge) string {
	b.mu.Lock()
	defer b.mu.Unlock()
	if b.pendingNudge == nil {
		return ""
	}
	return b.pendingNudge.reason
}

// deliverReply must resolve a stanza id to the author of that message. The
// bridge is offline in tests, so nothing is delivered either way; what is under
// test is whether the segment reaches the send path or the reject path.
func TestDeliverReplyResolvesStanzaID(t *testing.T) {
	acct := ResolvedAccount{Rooms: []string{"team@muc.x"}, ErrorRoom: "errors@muc.x", Owner: "zach@x"}
	const ownerMsg = "3e2597d4-a470-4cdb-b972-431043bce34f"
	const roomMsg = "a8508c81-0e1b-4e48-ae16-61256b837670"
	const unknown = "ffffffff-ffff-ffff-ffff-ffffffffffff"

	// A known 1:1 id resolves to the owner and is not rejected.
	b := newTestBridge(acct)
	b.xmpp.recordMessage(ownerMsg, "zach@x/phone")
	b.deliverReply("to: " + ownerMsg + "\n\nanswering that one")
	if r := stagedNudgeReason(b); r != "" {
		t.Errorf("a known 1:1 stanza id was rejected: %s", r)
	}

	// A room message records "room@muc/nick"; the existing bareJid path must
	// collapse that to the room, which classifies as a room destination.
	b = newTestBridge(acct)
	b.xmpp.recordMessage(roomMsg, "team@muc.x/alice")
	b.deliverReply("to: " + roomMsg + "\n\nanswering alice")
	if r := stagedNudgeReason(b); r != "" {
		t.Errorf("a known room stanza id was rejected: %s", r)
	}

	// An unknown id is a routing failure, not a silent fallback.
	b = newTestBridge(acct)
	if b.deliverReply("to: " + unknown + "\n\nlost") {
		t.Error("an unknown stanza id must not count as delivered")
	}
	r := stagedNudgeReason(b)
	if !strings.Contains(r, "unknown stanza id") {
		t.Errorf("reject reason = %q, want it to name an unknown stanza id", r)
	}

	// An id whose author is not allowlisted still takes the reject path.
	b = newTestBridge(acct)
	b.xmpp.recordMessage(ownerMsg, "stranger@elsewhere/x")
	b.deliverReply("to: " + ownerMsg + "\n\nhello stranger")
	if r := stagedNudgeReason(b); !strings.Contains(r, "not an allowed destination") {
		t.Errorf("reject reason = %q, want a blocked-destination reason", r)
	}
}

// The reverted attempt (#50/#51) put the stanza id in `to` and emitted no `id`
// at all, so no client threaded it. XEP-0461 needs both attributes.
func TestReplyStanzaGoldenXML(t *testing.T) {
	to := jid.MustParse("team@muc.x")
	msg := chatStanza("out-1", to, stanza.GroupChatMessage, "answering alice",
		&replyTarget{author: "team@muc.x/alice", id: "a8508c81-0e1b-4e48-ae16-61256b837670"})
	out, err := xml.Marshal(msg)
	if err != nil {
		t.Fatalf("marshal: %v", err)
	}
	got := string(out)
	for _, want := range []string{
		`<reply xmlns="urn:xmpp:reply:0"`,
		`to="team@muc.x/alice"`,
		`id="a8508c81-0e1b-4e48-ae16-61256b837670"`,
	} {
		if !strings.Contains(got, want) {
			t.Errorf("stanza %s\nis missing %s", got, want)
		}
	}

	// No reply target: no element. Nothing about an ordinary send changes.
	plain, err := xml.Marshal(chatStanza("out-2", to, stanza.GroupChatMessage, "hi", nil))
	if err != nil {
		t.Fatalf("marshal: %v", err)
	}
	if strings.Contains(string(plain), "reply") {
		t.Errorf("unstamped stanza carries a reply element: %s", plain)
	}

	// A half-filled target emits nothing: one attribute alone threads nowhere.
	for _, half := range []*replyTarget{{author: "team@muc.x/alice"}, {id: "abc"}} {
		out, err := xml.Marshal(chatStanza("out-3", to, stanza.GroupChatMessage, "hi", half))
		if err != nil {
			t.Fatalf("marshal: %v", err)
		}
		if strings.Contains(string(out), "reply") {
			t.Errorf("half-filled target %+v emitted an element: %s", half, out)
		}
	}
}

// A long reply is split across stanzas. Only the first chunk carries the stamp:
// a client threads on the first stanza, and a stamp on every chunk makes each
// chunk a separate reply to the same parent.
func TestReplyStampFirstChunkOnly(t *testing.T) {
	rt := &replyTarget{author: "zach@x/phone", id: "3e2597d4-a470-4cdb-b972-431043bce34f"}
	if got := replyForChunk(0, rt); got != rt {
		t.Errorf("chunk 0 stamp = %+v, want the reply target", got)
	}
	for _, i := range []int{1, 2, 7} {
		if got := replyForChunk(i, rt); got != nil {
			t.Errorf("chunk %d stamp = %+v, want none", i, got)
		}
	}
}

// The typing indicator reads the routing line before the reply is delivered, so
// it must resolve a stanza id the same way deliverReply does.
func TestStreamTypingTargetStanzaID(t *testing.T) {
	acct := ResolvedAccount{Rooms: []string{"team@muc.x"}, Owner: "zach@x"}
	x := NewXMPPBridge(acct, func(InboundMessage) {}, func(string, string) {})
	const ownerMsg = "3e2597d4-a470-4cdb-b972-431043bce34f"
	const roomMsg = "a8508c81-0e1b-4e48-ae16-61256b837670"
	x.recordMessage(ownerMsg, "zach@x/phone")
	x.recordMessage(roomMsg, "team@muc.x/alice")

	// A 1:1 message resolves to the owner, so the composer lights up.
	target, decided := streamTypingTarget("to: "+ownerMsg+"\n", x)
	if !decided || target != "zach@x" {
		t.Errorf("owner id = (%q,%v), want (\"zach@x\",true)", target, decided)
	}
	// A room message is not a 1:1 chat, so the composer stays dark.
	target, decided = streamTypingTarget("to: "+roomMsg+"\n", x)
	if !decided || target != "" {
		t.Errorf("room id = (%q,%v), want (\"\",true)", target, decided)
	}
	// An unknown id is a routing failure, and a failure never types.
	target, decided = streamTypingTarget("to: ffffffff-ffff-ffff-ffff-ffffffffffff\n", x)
	if !decided || target != "" {
		t.Errorf("unknown id = (%q,%v), want (\"\",true)", target, decided)
	}
}

// The unanswered-message hint fires exactly when several messages are in play,
// which is the case a bare jid cannot disambiguate. It must offer the stanza-id
// form, and keep "to: noop" as the way to say the replies already covered
// everything.
func TestUnansweredHintOffersStanzaID(t *testing.T) {
	got := unansweredHintText(5, 1)
	for _, want := range []string{
		"Received 5 messages but sent 1 replies",
		`"to: <jid|stanza-id>"`,
		`"stanza-id:"`,
		`"to: noop"`,
	} {
		if !strings.Contains(got, want) {
			t.Errorf("hint is missing %s:\n%s", want, got)
		}
	}
}

// The stanza id is the handle for reply routing, so it must reach the agent in
// every room turn — not only when XEP-0444 reactions happen to be enabled.
func TestStanzaIDSurfacedWithoutRoomReactions(t *testing.T) {
	const id = "3e2597d4-a470-4cdb-b972-431043bce34f"
	acct := ResolvedAccount{Rooms: []string{"team@muc.x"}, Owner: "zach@x", RoomTrigger: "pi"}
	b := newTestBridge(acct) // RoomReactions is off
	prompt := b.composePrompt("do it", true, "", "team@muc.x", "zach@x", id, "")
	if !strings.Contains(prompt, "stanza-id: "+id) {
		t.Errorf("prompt is missing the stanza id:\n%s", prompt)
	}
	if strings.Contains(prompt, "react-to:") {
		t.Errorf("reactions are off, so react-to must not appear:\n%s", prompt)
	}
	if !strings.Contains(prompt, "to: <stanza-id>") {
		t.Errorf("the routing contract must document the stanza-id form:\n%s", prompt)
	}
}
