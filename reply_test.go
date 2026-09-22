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
	target, decided, delivers := streamTypingTarget("to: "+ownerMsg+"\n", x)
	if !decided || target != "zach@x" || !delivers {
		t.Errorf("owner id = (%q,%v,%v), want (\"zach@x\",true,true)", target, decided, delivers)
	}
	// A room message is not a 1:1 chat, so the composer stays dark — but it
	// still delivers, so the label upgrades to replying.
	target, decided, delivers = streamTypingTarget("to: "+roomMsg+"\n", x)
	if !decided || target != "" || !delivers {
		t.Errorf("room id = (%q,%v,%v), want (\"\",true,true)", target, decided, delivers)
	}
	// An unknown id is a routing failure, and a failure never types or
	// delivers.
	target, decided, delivers = streamTypingTarget("to: ffffffff-ffff-ffff-ffff-ffffffffffff\n", x)
	if !decided || target != "" || delivers {
		t.Errorf("unknown id = (%q,%v,%v), want (\"\",true,false)", target, decided, delivers)
	}
}

// The unanswered-message hint fires exactly when several messages are in play,
// which is the case a bare jid cannot disambiguate. It must offer the stanza-id
// form, and keep "to: noop" as the way to say the replies already covered
// everything.
func TestUnansweredHintOffersStanzaID(t *testing.T) {
	got := unansweredHintText(5, 1, nil, true)
	for _, want := range []string{
		"You received 5 messages but sent 1 replies",
		`"to: <jid|stanza-id>"`,
		`"to: noop"`,
	} {
		if !strings.Contains(got, want) {
			t.Errorf("hint is missing %s:\n%s", want, got)
		}
	}
}

// A pure 1:1 account parses no "to: <destination>" line, so the hint must not
// ask for one: the literal text would land in front of the owner. "to: noop"
// works in both modes and stays.
func TestUnansweredHintOneToOneWording(t *testing.T) {
	got := unansweredHintText(3, 1, nil, false)
	if strings.Contains(got, "jid") || strings.Contains(got, "stanza-id>") {
		t.Errorf("the 1:1 hint must not ask for a routing line:\n%s", got)
	}
	if !strings.Contains(got, `"to: noop"`) {
		t.Errorf("the 1:1 hint must still offer \"to: noop\":\n%s", got)
	}
}

// The agent cannot see the XMPP traffic, so counts alone leave it guessing
// which message it missed. The hint must print the run's history: each message
// in and each reply out, with the stanza id that answers it.
func TestUnansweredHintShowsHistory(t *testing.T) {
	got := unansweredHintText(3, 2, []runLogEntry{
		{who: "zach", id: "id-a", text: "check the log"},
		{who: "zach", id: "id-b", text: "also the metrics"},
		{who: "slippy", id: "id-c", text: "the log looks clean", sent: true},
		{who: "zach", id: "id-d", text: "and the disk"},
		{who: "slippy", id: "", text: "", sent: true},
	}, true)
	for _, want := range []string{
		`zach: id-a "check the log"`,
		`zach: id-b "also the metrics"`,
		`slippy: id-c "the log looks clean"`,
		`zach: id-d "and the disk"`,
		"slippy: (no id) (deliberate silence, to: noop)",
	} {
		if !strings.Contains(got, want) {
			t.Errorf("history is missing %s:\n%s", want, got)
		}
	}
	// The lines must stay in arrival order, or the agent cannot pair a reply
	// with the message it answered.
	if strings.Index(got, "id-a") > strings.Index(got, "id-d") {
		t.Errorf("history is out of order:\n%s", got)
	}
}

// A long message is excerpted, not repeated in full: the history is an index
// into the run, and the messages themselves are already in the agent's context.
func TestHintExcerptShortens(t *testing.T) {
	long := strings.Repeat("word ", 40)
	got := hintExcerpt(long)
	if len([]rune(got)) > 50 || !strings.HasSuffix(got, "…") {
		t.Errorf("excerpt = %q, want a short string ending in an ellipsis", got)
	}
	if got := hintExcerpt("two\nlines"); got != "two lines" {
		t.Errorf("excerpt = %q, want the newline folded to a space", got)
	}
}

// The stanza id is the handle for reply routing, so it must reach the agent in
// every room turn — not only when XEP-0444 reactions happen to be enabled.
func TestStanzaIDSurfacedWithoutRoomReactions(t *testing.T) {
	const id = "3e2597d4-a470-4cdb-b972-431043bce34f"
	acct := ResolvedAccount{Rooms: []string{"team@muc.x"}, Owner: "zach@x", RoomTrigger: "pi"}
	b := newTestBridge(acct) // RoomReactions is off
	prompt := b.composePrompt("do it", true, "", "team@muc.x", "zach@x", id, "", "")
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

// An inbound XEP-0461 reply must reach the agent with enough context to know
// what is being answered (#95). Before this, an owner replying "?" to a
// message got answered about something else entirely, because the prompt
// carried the reply's own text and nothing about its target.
func TestInboundReplyContextResolves(t *testing.T) {
	acct := ResolvedAccount{Owner: "zach@x", Rooms: []string{"team@muc.x"}, RoomTrigger: "pi"}
	b := newTestBridge(acct)
	const orig = "6e7c6ed8-5485-4c01-be7a-07750c59ed27"
	b.xmpp.recordMessageBody(orig, "zach@x/phone", "then send me latest master apk")

	// Resolvable: id, author, age and a quote of what is being answered.
	got := b.replyContext(InboundMessage{ReplyToID: orig, ReplyToJID: "pi@x"})
	for _, want := range []string{orig, "zach@x/phone", `"then send me latest master apk"`} {
		if !strings.Contains(got, want) {
			t.Errorf("replyContext = %q, missing %q", got, want)
		}
	}

	// Unresolvable: named as such, with the client's `to` as a hint. This is the
	// case that actually happened — a reply to a message that never arrived.
	unknown := "aaaaaaaa-1111-2222-3333-444444444444"
	got = b.replyContext(InboundMessage{ReplyToID: unknown, ReplyToJID: "pi@x"})
	if !strings.Contains(got, unknown) || !strings.Contains(got, "NOT in this session's history") {
		t.Errorf("unresolvable reply = %q, want the id and an explicit miss", got)
	}
	if !strings.Contains(got, "pi@x") {
		t.Errorf("unresolvable reply = %q, want the stamped-to jid as a hint", got)
	}

	// A stamp with no id (some clients only set `to`).
	if got := b.replyContext(InboundMessage{ReplyToJID: "pi@x"}); !strings.Contains(got, "no id was stamped") {
		t.Errorf("id-less reply = %q", got)
	}
	// No stamp at all: no header.
	if got := b.replyContext(InboundMessage{}); got != "" {
		t.Errorf("no reply stamp should render nothing, got %q", got)
	}
}

// The rendered header sits with the other metadata lines, above the message.
func TestComposePromptSurfacesInReplyTo(t *testing.T) {
	acct := ResolvedAccount{Owner: "zach@x", Rooms: []string{"team@muc.x"}, RoomTrigger: "pi"}
	b := newTestBridge(acct)
	const orig = "6e7c6ed8-5485-4c01-be7a-07750c59ed27"
	b.xmpp.recordMessageBody(orig, "zach@x/phone", "merge to master")
	m := InboundMessage{ReplyToID: orig, ReplyToJID: "pi@x"}
	prompt := b.composePrompt("?", true, "", "team@muc.x", "zach@x", "id-123", "team@muc.x", b.replyContext(m))
	lines := strings.Split(prompt, "\n")
	fromIdx, idx := -1, -1
	for i, l := range lines {
		switch {
		case strings.HasPrefix(l, "from: "):
			fromIdx = i
		case strings.HasPrefix(l, "in-reply-to: "):
			idx = i
		}
	}
	if fromIdx < 0 {
		t.Fatalf("prompt has no from line:\n%s", prompt)
	}
	if idx < 0 {
		t.Fatalf("prompt has no in-reply-to line:\n%s", prompt)
	}
	if idx < fromIdx {
		t.Errorf("in-reply-to must follow the from/stanza-id header lines:\n%s", prompt)
	}
	if !strings.Contains(lines[idx], `"merge to master"`) {
		t.Errorf("in-reply-to line lacks the quote: %q", lines[idx])
	}
	// The header block must precede the message body itself.
	if idx >= len(lines)-1 || strings.TrimSpace(lines[len(lines)-1]) != "?" {
		t.Errorf("body not last after the header block:\n%s", prompt)
	}
}

// <body> is a local name shared with the XHTML-IM payload and the XEP-0461
// <fallback> quote. Only a direct child of the stanza counts, or a client that
// sends HTML or fallback text could have the wrong body prompted (#95).
func TestChildTextOnlyMatchesDirectChild(t *testing.T) {
	// A direct <body> plus an HTML payload and a fallback quote: the direct one wins.
	toks := []xml.Token{
		xml.StartElement{Name: xml.Name{Local: "body"}},
		xml.CharData("the real text"),
		xml.EndElement{Name: xml.Name{Local: "body"}},
		xml.StartElement{Name: xml.Name{Space: "http://jabber.org/protocol/xhtml-im", Local: "html"}},
		xml.StartElement{Name: xml.Name{Space: "http://www.w3.org/1999/xhtml", Local: "body"}},
		xml.CharData("html text"),
		xml.EndElement{Name: xml.Name{Local: "body"}},
		xml.EndElement{Name: xml.Name{Local: "html"}},
	}
	if got := childText(toks, "body"); got != "the real text" {
		t.Errorf("childText = %q, want the direct child", got)
	}
	// Only nested copies (html + fallback): must NOT be mistaken for the body.
	nested := []xml.Token{
		xml.StartElement{Name: xml.Name{Local: "html"}},
		xml.StartElement{Name: xml.Name{Local: "body"}},
		xml.CharData("html text"),
		xml.EndElement{Name: xml.Name{Local: "body"}},
		xml.EndElement{Name: xml.Name{Local: "html"}},
		xml.StartElement{Name: xml.Name{Space: "urn:xmpp:fallback:0", Local: "fallback"}},
		xml.StartElement{Name: xml.Name{Local: "body"}},
		xml.CharData("quoted text"),
		xml.EndElement{Name: xml.Name{Local: "body"}},
		xml.EndElement{Name: xml.Name{Local: "fallback"}},
	}
	if got := childText(nested, "body"); got != "" {
		t.Errorf("childText = %q, want \"\" (no direct child body)", got)
	}
}

// A reply target's quote is bounded and single-lined: it is pasted into a
// prompt header, so a 10KB message must not be reproduced there.
func TestMsgHistoryBodyBounded(t *testing.T) {
	b := NewXMPPBridge(ResolvedAccount{Owner: "zach@x"}, func(InboundMessage) {}, nil)
	long := strings.Repeat("x", 5000) + "\nsecond line"
	b.recordMessageBody("id-1", "zach@x/phone", long)
	e, ok := b.lookupMessageEntry("id-1")
	if !ok {
		t.Fatal("entry not recorded")
	}
	if n := len([]rune(e.Body)); n > msgHistoryBodyCap {
		t.Errorf("stored body is %d runes, want <= %d", n, msgHistoryBodyCap)
	}
	if strings.Contains(e.Body, "\n") {
		t.Errorf("stored body kept a newline: %q", e.Body)
	}
	// Outbound sends record a body too, so our own message can be quoted when the
	// owner replies to it.
	x := NewXMPPBridge(ResolvedAccount{Owner: "zach@x"}, func(InboundMessage) {}, nil)
	x.recordMessageBody("id-2", "zach@x", "here is the apk")
	if e, ok := x.lookupMessageEntry("id-2"); !ok || e.Body != "here is the apk" {
		t.Errorf("lookup = (%+v,%v)", e, ok)
	}
	if _, ok := x.lookupMessageEntry("never-seen"); ok {
		t.Error("unknown id reported as found")
	}
}
