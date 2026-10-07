package pimsg

import (
	"encoding/xml"
	"strings"
	"testing"

	"mellium.im/xmpp/jid"
	"mellium.im/xmpp/stanza"
)

// A well-formed stanza id is a third routing target form, alongside a jid and
// "noop" (#54). It must not collide with either, and it must not swallow prose.
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

// The catch-up hint requests explicit sends and reply_to IDs.
func TestUnansweredHintOffersStanzaID(t *testing.T) {
	got := unansweredHintText(5, 1, nil)
	for _, want := range []string{
		"You received 5 messages but sent 1 replies",
		"send_message(to, text, reply_to)",
		"stanza ID from the history as reply_to",
		"final assistant response is internal",
	} {
		if !strings.Contains(got, want) {
			t.Errorf("hint is missing %s:\n%s", want, got)
		}
	}
}

// The explicit send hint is independent of room-vs-DM account mode.
func TestUnansweredHintOneToOneWording(t *testing.T) {
	got := unansweredHintText(3, 1, nil)
	if !strings.Contains(got, "send_message(to, text, reply_to)") || !strings.Contains(got, "internal") {
		t.Errorf("the 1:1 hint must use explicit messaging:\n%s", got)
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
	})
	for _, want := range []string{
		`zach: id-a "check the log"`,
		`zach: id-b "also the metrics"`,
		`slippy: id-c "the log looks clean"`,
		`zach: id-d "and the disk"`,
		"slippy: (no id) (sent message without text)",
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
	prompt := b.composePrompt("do it", "team@muc.x", "zach@x", id, "", "", &roomNotice{kind: noticeTag})
	if !strings.Contains(prompt, "stanza-id: "+id) {
		t.Errorf("prompt is missing the stanza id:\n%s", prompt)
	}
	if strings.Contains(prompt, "react-to:") {
		t.Errorf("reactions are off, so react-to must not appear:\n%s", prompt)
	}
	if !strings.Contains(prompt, "reply_to") {
		t.Errorf("the messaging contract must document reply_to:\n%s", prompt)
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
// This is the 1:1 path (#95): a room reply to our own message is pointer case E
// and carries the parent id only (#58).
func TestComposePromptSurfacesInReplyTo(t *testing.T) {
	acct := ResolvedAccount{Owner: "zach@x", Rooms: []string{"team@muc.x"}, RoomTrigger: "pi"}
	b := newTestBridge(acct)
	const orig = "6e7c6ed8-5485-4c01-be7a-07750c59ed27"
	b.xmpp.recordMessageBody(orig, "zach@x/phone", "merge to master")
	m := InboundMessage{ReplyToID: orig, ReplyToJID: "pi@x"}
	prompt := b.composePrompt("?", "zach@x", "", "id-123", "", b.replyContext(m), nil)
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
