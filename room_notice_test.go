package main

import (
	"bytes"
	"encoding/json"
	"strings"
	"sync"
	"testing"
)

// capturedPrompt reads the first prompt JSON line an RPCClient wrote and returns
// its message text, so a test can compare the exact block the agent receives.
func capturedPrompt(t *testing.T, buf *bytes.Buffer) string {
	t.Helper()
	line, err := buf.ReadString('\n')
	if err != nil && line == "" {
		t.Fatalf("no prompt was written: %v", err)
	}
	var cmd struct {
		Message string `json:"message"`
	}
	if err := json.Unmarshal([]byte(line), &cmd); err != nil {
		t.Fatalf("prompt line is not JSON: %v (%q)", err, line)
	}
	return cmd.Message
}

// The five room pointer blocks (#58): the agent is told what reached it and
// given the addressing meta, and pulls the text itself with read_room. These
// strings are character-comparable against the issue's spec, with the JIDs
// substituted.
func TestRoomPointerBlocks(t *testing.T) {
	const (
		room   = "testing@muc.chat.zachmanson.com"
		fox    = "fox@chat.zachmanson.com"
		owner  = "zach@chat.zachmanson.com"
		sid    = "3ac658d3ac0f8e88"
		osid   = "05099902-4f1d-4c29"
		parent = "f37efb047ec9978c"
	)
	b := newTestBridge(ResolvedAccount{Owner: owner, Rooms: []string{room}, Nick: "pi", RoomTrigger: "pi"})
	b.routingSeeded = true // skip the seed; these tests assert the block alone

	cases := []struct {
		name   string
		notice *roomNotice
		sender string
		id     string
		want   string
	}{
		{
			name:   "A peer tagged us",
			notice: &roomNotice{kind: noticeTag},
			sender: fox,
			id:     sid,
			want: "[pi-msg: room: You were tagged in a room. The text is not in this prompt.\n" +
				"from: " + room + "\n" +
				"sender: " + fox + "\n" +
				"stanza-id: " + sid + "\n" +
				"react-to: " + room + "\n" +
				`Check message using read_room(room="` + room + `", limit=15).]`,
		},
		{
			name:   "C the owner broadcast",
			notice: &roomNotice{kind: noticeOwnerBroadcast},
			sender: owner,
			id:     osid,
			want: "[pi-msg: room: The owner broadcast to the room's idle agents. The text is not in this prompt.\n" +
				"from: " + room + "\n" +
				"sender: " + owner + "\n" +
				"stanza-id: " + osid + "\n" +
				"react-to: " + room + "\n" +
				`Check message using read_room(room="` + room + `", limit=15).]`,
		},
		{
			name:   "F the room's idle agents were summoned",
			notice: &roomNotice{kind: noticeFreeBroadcast},
			sender: fox,
			id:     sid,
			want: "[pi-msg: room: The room's idle agents were summoned. The text is not in this prompt.\n" +
				"from: " + room + "\n" +
				"sender: " + fox + "\n" +
				"stanza-id: " + sid + "\n" +
				"react-to: " + room + "\n" +
				`Check message using read_room(room="` + room + `", limit=15).]`,
		},
		{
			name:   "E a reply to our message",
			notice: &roomNotice{kind: noticeReplyToOwn, parentID: parent},
			sender: fox,
			id:     sid,
			want: "[pi-msg: room: Your message was replied to. The text is not in this prompt.\n" +
				"from: " + room + "\n" +
				"sender: " + fox + "\n" +
				"stanza-id: " + sid + "\n" +
				"in-reply-to: " + parent + "\n" +
				"react-to: " + room + "\n" +
				`Check message using read_room(room="` + room + `", limit=15).]`,
		},
	}
	for _, tc := range cases {
		got := b.composePrompt("THE BODY MUST NOT APPEAR", room, tc.sender, tc.id, room, "", tc.notice)
		if got != tc.want {
			t.Errorf("%s:\n got: %q\nwant: %q", tc.name, got, tc.want)
		}
		if strings.Contains(got, "THE BODY MUST NOT APPEAR") {
			t.Errorf("%s: the body entered the prompt", tc.name)
		}
	}
}

// A tagged room message produces a prompt with no body text in it (#58): the
// body is reachable only through read_room.
func TestTaggedRoomMessagePromptHasNoBody(t *testing.T) {
	const room = "team@muc.x"
	acct := ResolvedAccount{Owner: "zach@x", Name: "t", Rooms: []string{room}, Nick: "pi", RoomTrigger: "pi"}
	b := newTestBridge(acct)
	b.routingSeeded = true
	var buf bytes.Buffer
	b.rpc = &RPCClient{stdin: &nopClose{buf: &buf}, mu: sync.Mutex{}}

	b.handleRoom(InboundMessage{
		ID: "abc123", Room: room, Nick: "alice", From: room + "/alice", RealJID: "alice@x",
		Body: "pi: FROGBODY deploy the thing",
	})

	got := capturedPrompt(t, &buf)
	if !strings.Contains(got, "[pi-msg: room: You were tagged in a room.") {
		t.Errorf("tagged message should produce a pointer block: %q", got)
	}
	if !strings.Contains(got, "stanza-id: abc123") {
		t.Errorf("pointer block must still carry the stanza id: %q", got)
	}
	if strings.Contains(got, "FROGBODY") {
		t.Errorf("the body must not enter the prompt: %q", got)
	}
}

// D: the reaction ack keeps the only excerpt in the set — of OUR OWN message
// being reacted to (#58). The excerpt comes from msgHistory, recorded at send
// time, so no new plumbing is needed.
func TestReactionAckQuotesOurOwnMessage(t *testing.T) {
	const room = "testing@muc.chat.zachmanson.com"
	acct := ResolvedAccount{Owner: "zach@chat.zachmanson.com", Name: "t", Rooms: []string{room}, Nick: "pi", RoomTrigger: "pi"}
	b := newTestBridge(acct)
	var buf bytes.Buffer
	b.rpc = &RPCClient{stdin: &nopClose{buf: &buf}, mu: sync.Mutex{}}
	const target = "c0ffee00c0ffee00"
	b.xmpp.recordSelfMessage(target, room, "**D.** Baton → @slippy for **E**")

	b.handleReaction(InboundMessage{
		Room: room, Nick: "fox", From: room + "/fox",
		ReactionID: target, Reactions: []string{"✅"},
	})

	want := `[pi-msg: room: fox reacted ✅ to your message "**D.** Baton → @slippy for **E**". You may acknowledge, act on it, or ignore — reply with "to: noop" if you have nothing to add.]`
	if got := capturedPrompt(t, &buf); got != want {
		t.Errorf("reaction ack:\n got: %q\nwant: %q", got, want)
	}
}

// The reactor's nick renders as "owner" when the owner is the reactor.
func TestReactionAckOwnerNick(t *testing.T) {
	const room = "testing@muc.chat.zachmanson.com"
	acct := ResolvedAccount{Owner: "zach@chat.zachmanson.com", Name: "t", Rooms: []string{room}, Nick: "pi", RoomTrigger: "pi"}
	b := newTestBridge(acct)
	var buf bytes.Buffer
	b.rpc = &RPCClient{stdin: &nopClose{buf: &buf}, mu: sync.Mutex{}}
	const target = "c0ffee00c0ffee00"
	b.xmpp.recordSelfMessage(target, room, "short reply")

	b.handleReaction(InboundMessage{
		Room: room, Nick: "zach", From: room + "/zach", FromOwner: true,
		ReactionID: target, Reactions: []string{"👍"},
	})

	got := capturedPrompt(t, &buf)
	if !strings.HasPrefix(got, "[pi-msg: room: owner reacted 👍 to your message ") {
		t.Errorf("owner reactor should render as owner: %q", got)
	}
}

// reactionExcerpt shortens a long outbound body to its first ~80 characters.
func TestReactionExcerptTruncates(t *testing.T) {
	long := strings.Repeat("a", 200)
	got := reactionExcerpt(long)
	if r := []rune(got); len(r) != 81 { // 80 runes + the ellipsis
		t.Errorf("excerpt rune length = %d, want 81: %q", len(r), got)
	}
	if !strings.HasSuffix(got, "…") {
		t.Errorf("a truncated excerpt should end with an ellipsis: %q", got)
	}
	if short := reactionExcerpt("keep me"); short != "keep me" {
		t.Errorf("a short excerpt should pass through: %q", short)
	}
	if folded := reactionExcerpt("a\nb   c"); folded != "a b c" {
		t.Errorf("whitespace should fold to spaces: %q", folded)
	}
}

// roomNoticeFor selects the block from the message: E for a reply to our own
// message, C for an owner broadcast, A otherwise, and nil off-room.
func TestRoomNoticeFor(t *testing.T) {
	const room = "team@muc.x"
	acct := ResolvedAccount{Owner: "zach@x", Rooms: []string{room}, Nick: "pi", RoomTrigger: "pi"}
	b := newTestBridge(acct)
	const ours = "ours-1"
	b.xmpp.recordSelfMessage(ours, room, "our message")

	cases := []struct {
		name string
		m    InboundMessage
		want roomNoticeKind
	}{
		{"peer tag", InboundMessage{Room: room, Body: "pi: hi"}, noticeTag},
		{"owner tag", InboundMessage{Room: room, Body: "pi: hi", FromOwner: true}, noticeTag},
		{"owner broadcast", InboundMessage{Room: room, Body: "everyone please note", FromOwner: true}, noticeOwnerBroadcast},
		{"owner @free", InboundMessage{Room: room, Body: "@free report in", FromOwner: true}, noticeOwnerBroadcast},
		{"peer @free", InboundMessage{Room: room, Body: "@free report in"}, noticeFreeBroadcast},
		{"peer @free naming us too", InboundMessage{Room: room, Body: "@free and pi: report in"}, noticeTag},
		{"owner broadcast to all", InboundMessage{Room: room, Body: "@everyone hello", FromOwner: true}, noticeTag},
		{"reply to ours", InboundMessage{Room: room, Body: "and another thing", ReplyToID: ours}, noticeReplyToOwn},
	}
	for _, tc := range cases {
		got := b.roomNoticeFor(tc.m)
		if got == nil {
			t.Errorf("%s: got nil notice", tc.name)
			continue
		}
		if got.kind != tc.want {
			t.Errorf("%s: kind = %d, want %d", tc.name, got.kind, tc.want)
		}
		if tc.want == noticeReplyToOwn && got.parentID != ours {
			t.Errorf("%s: parentID = %q, want %q", tc.name, got.parentID, ours)
		}
	}
	if got := b.roomNoticeFor(InboundMessage{Body: "hi"}); got != nil {
		t.Errorf("a non-room message should have no room notice, got %+v", got)
	}
}
