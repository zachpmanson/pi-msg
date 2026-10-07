package pimsg

import (
	"bytes"
	"strings"
	"sync"
	"testing"
)

func relayOutput(t *testing.T, acct ResolvedAccount, payload string) string {
	t.Helper()
	b := newTestBridge(acct)
	var buf bytes.Buffer
	b.rpc = &RPCClient{stdin: &nopClose{buf: &buf}, mu: sync.Mutex{}}
	b.handleToolRelay("r1", payload)
	return buf.String()
}

func TestDefaultMessageDestinationPolicy(t *testing.T) {
	x := NewXMPPBridge(
		ResolvedAccount{Rooms: []string{"team@muc.x"}, Owner: "zach@x"},
		func(InboundMessage) {}, func(string, string) {},
	)
	x.occupants["team@muc.x"] = map[string]string{"alice": "alice@x"}
	for dest, want := range map[string]destKind{
		"zach@x":     destUser,
		"team@muc.x": destRoom,
		"alice@x":    destUser,
		"stranger@x": destBlocked,
	} {
		if got := x.classifyMessageDest(dest, false); got != want {
			t.Errorf("default send destination %q = %v, want %v", dest, got, want)
		}
	}
	if got := x.classifyMessageDest("stranger@x", true); got != destUser {
		t.Errorf("arbitrary JID opt-in = %v, want destUser", got)
	}
}

func TestSendMessageArbitraryJidOptIn(t *testing.T) {
	payload := `{"action":"send_message","to":"stranger@x","text":"hello"}`
	denied := relayOutput(t, ResolvedAccount{Owner: "zach@x"}, payload)
	if !strings.Contains(denied, "not an allowed destination") {
		t.Fatalf("arbitrary peer should be denied by default, got %q", denied)
	}
	allowed := relayOutput(t, ResolvedAccount{Owner: "zach@x", AllowArbitraryJid: true}, payload)
	if !strings.Contains(allowed, "failed: no stanza was sent") || strings.Contains(allowed, "not an allowed destination") {
		t.Fatalf("opt-in should pass authorization and fail only at offline transport, got %q", allowed)
	}
}

func TestReadMessagesRequiresExplicitTarget(t *testing.T) {
	acct := ResolvedAccount{Owner: "zach@x", Name: "t", Rooms: []string{"team@muc.x"}}
	for _, payload := range []string{
		`{"action":"read_messages"}`,
		`{"action":"read_messages","target":"   "}`,
	} {
		out := relayOutput(t, acct, payload)
		if !strings.Contains(out, "target is required") {
			t.Errorf("read_messages without a target should fail explicitly, got %q", out)
		}
	}
}

func TestReadMessagesDefaultAllowlist(t *testing.T) {
	acct := ResolvedAccount{Owner: "zach@x", Name: "t", Rooms: []string{"team@muc.x"}}
	out := relayOutput(t, acct, `{"action":"read_messages","target":"stranger@x"}`)
	if !strings.Contains(out, "not an allowed target") {
		t.Fatalf("unexpected peer should be rejected by default, got %q", out)
	}
	legacy := relayOutput(t, acct, `{"action":"read_room","room":"team@muc.x"}`)
	if !strings.Contains(legacy, "unknown tool-relay action: read_room") {
		t.Fatalf("read_room unexpectedly remains an alias: %q", legacy)
	}
}

func TestArchivedDirectReplyTargetUsesConversationAndAuthor(t *testing.T) {
	acct := ResolvedAccount{JID: "pi@x", Owner: "zach@x", Name: "t"}
	b := newTestBridge(acct)
	b.xmpp.recordReadHistory([]InboundMessage{
		{ID: "own-stanza", From: "pi@x", Direct: true, Own: true, Body: "sent earlier"},
	}, "zach@x")
	entry, ok := b.xmpp.lookupReplyMessage("own-stanza")
	if !ok {
		t.Fatal("read_messages stanza ID was not retained")
	}
	if entry.FromJID != "pi@x" || entry.ConversationJID != "zach@x" {
		t.Fatalf("archived own message entry = %+v, want author pi@x in conversation zach@x", entry)
	}
	var buf bytes.Buffer
	b.rpc = &RPCClient{stdin: &nopClose{buf: &buf}, mu: sync.Mutex{}}
	b.handleSendMessageRelay("r1", "zach@x", "threaded reply", "own-stanza")
	if got := buf.String(); !strings.Contains(got, "failed: no stanza was sent") || strings.Contains(got, "belongs to") {
		t.Fatalf("own direct stanza should validate against conversation, then fail only on offline send: %q", got)
	}
}
