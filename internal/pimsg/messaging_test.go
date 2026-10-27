package pimsg

import (
	"bytes"
	"encoding/xml"
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
		"zach@x":        destUser,
		"team@muc.x":    destRoom,
		"alice@x":       destBlocked,
		"alice@x/phone": destBlocked,
		"stranger@x":    destBlocked,
	} {
		if got := x.classifyMessageDest(dest, false); got != want {
			t.Errorf("default send destination %q = %v, want %v", dest, got, want)
		}
	}
	if got := x.classifyMessageDest("stranger@x", true); got != destUser {
		t.Errorf("arbitrary JID opt-in = %v, want destUser", got)
	}
}

func TestMessageDestinationGuidanceSeparatesPermissionsAndMembers(t *testing.T) {
	x := NewXMPPBridge(
		ResolvedAccount{Rooms: []string{"team@muc.x", "other@muc.x", "team@muc.x"}, Owner: "zach@x/phone", ErrorRoom: "errors@muc.x"},
		func(InboundMessage) {}, func(string, string) {},
	)
	x.occupants["team@muc.x"] = map[string]string{
		"alice": "alice@x", "anonymous": "", "owner": "zach@x",
	}
	x.occupants["other@muc.x"] = map[string]string{"alice": "alice@x", "bob": "bob@x"}
	// Neither stale/unconfigured membership nor archive reads grant permission
	// or contribute names to the advertised room-scoped mention targets.
	x.occupants["errors@muc.x"] = map[string]string{"error-member": "error@x"}
	x.occupants["unconfigured@muc.x"] = map[string]string{"outsider": "outside@x"}
	x.recordReadHistory([]InboundMessage{
		{ID: "old", From: "archived@x", Direct: true, Body: "old message"},
	}, "archived@x")
	permissions := `Allowed 1:1 JIDs: ["zach@x"]. Allowed room JIDs: ["other@muc.x" "team@muc.x"]. `
	check := func(members string) {
		t.Helper()
		want := permissions + `Room-scoped members/mention names (NOT DM destinations): [` + members + `]. Send to the room JID and mention the member in text.`
		for range 20 { // Map iteration cannot change either category or nickname ordering.
			if got := x.messageDestinationGuidance(); got != want {
				t.Fatalf("destination guidance = %s, want %s", got, want)
			}
		}
		for dest, want := range map[string]destKind{
			"zach@x": destUser, "zach@x/phone": destUser,
			"other@muc.x": destRoom, "team@muc.x": destRoom,
			"alice@x": destBlocked, "alice@x/phone": destBlocked,
			"bob@x": destBlocked, "archived@x": destBlocked,
			"errors@muc.x": destBlocked, "unconfigured@muc.x": destBlocked,
		} {
			if got := x.classifyDest(dest); got != want {
				t.Errorf("classifyDest(%q) = %v, want %v", dest, got, want)
			}
		}
	}
	check(`"other@muc.x": ["alice" "bob"]; "team@muc.x": ["alice" "anonymous" "owner"]`)
	// Presence departure updates mention names, not permissions.
	start := xml.StartElement{Name: xml.Name{Local: "presence"}, Attr: []xml.Attr{
		{Name: xml.Name{Local: "from"}, Value: "other@muc.x/bob"},
		{Name: xml.Name{Local: "type"}, Value: "unavailable"},
	}}
	if err := x.handlePresence(&start, nil); err != nil {
		t.Fatal(err)
	}
	check(`"other@muc.x": ["alice"]; "team@muc.x": ["alice" "anonymous" "owner"]`)
	// Model serve's reconnect reset: only live member knowledge is discarded.
	x.occupants = make(map[string]map[string]string)
	check(`"other@muc.x": []; "team@muc.x": []`)
}

func TestAnonymousPresenceIsRoomScopedMentionTarget(t *testing.T) {
	x := NewXMPPBridge(ResolvedAccount{Owner: "owner@x", Rooms: []string{"team@muc.x"}, ErrorRoom: "errors@muc.x"}, func(InboundMessage) {}, func(string, string) {})
	for _, room := range []string{"team@muc.x", "errors@muc.x", "other@muc.x"} {
		start := xml.StartElement{Name: xml.Name{Local: "presence"}, Attr: []xml.Attr{
			{Name: xml.Name{Local: "from"}, Value: room + "/anonymous"},
		}}
		if err := x.handlePresence(&start, nil); err != nil {
			t.Fatal(err)
		}
	}
	if got := x.OccupantNicks("team@muc.x"); strings.Join(got, ",") != "anonymous" {
		t.Fatalf("anonymous nickname missing: %q", got)
	}
	if len(x.occupants) != 1 || x.occupantRealJID("team@muc.x", "anonymous") != "" {
		t.Fatalf("unexpected anonymous membership: %v", x.occupants)
	}
	if !strings.Contains(x.messageDestinationGuidance(), `"team@muc.x": ["anonymous"]`) {
		t.Fatal("anonymous member missing from room-scoped guidance")
	}
}

func TestNonMembershipPresenceDoesNotAdvertiseRoomMembers(t *testing.T) {
	for _, ptype := range []string{"error", "subscribe", "subscribed", "unsubscribe", "unsubscribed", "probe"} {
		t.Run(ptype, func(t *testing.T) {
			x := NewXMPPBridge(ResolvedAccount{Owner: "owner@x", Rooms: []string{"team@muc.x"}}, func(InboundMessage) {}, func(string, string) {})
			x.occupants["team@muc.x"] = map[string]string{"existing": "existing@x"}
			x.selfNick["team@muc.x"] = "confirmed"
			before := x.messageDestinationGuidance()
			for _, nick := range []string{"requested-nick", "existing"} {
				start := xml.StartElement{Name: xml.Name{Local: "presence"}, Attr: []xml.Attr{
					{Name: xml.Name{Local: "from"}, Value: "team@muc.x/" + nick},
					{Name: xml.Name{Local: "type"}, Value: ptype},
				}}
				// Even a self-presence code must not confirm a failed join.
				toks := []xml.Token{xml.StartElement{Name: xml.Name{Local: "status"}, Attr: []xml.Attr{
					{Name: xml.Name{Local: "code"}, Value: "110"},
				}}}
				if err := x.handlePresence(&start, toks); err != nil {
					t.Fatal(err)
				}
			}
			if got := x.messageDestinationGuidance(); got != before {
				t.Fatalf("non-membership presence changed guidance: %s", got)
			}
			if got := x.occupantRealJID("team@muc.x", "existing"); got != "existing@x" {
				t.Fatalf("non-membership presence replaced an existing identity: %q", got)
			}
			if got := x.ownNick("team@muc.x"); got != "confirmed" {
				t.Fatalf("non-membership presence changed our confirmed nick: %q", got)
			}
		})
	}
}

func TestSendMessageDeniedOwnerOnlyConfiguration(t *testing.T) {
	out := relayOutput(t, ResolvedAccount{Owner: "owner@example.com/phone"}, `{"action":"send_message","to":"stranger@x","text":"hello"}`)
	for _, want := range []string{`Allowed 1:1 JIDs: [\"owner@example.com\"]`, `Allowed room JIDs: []`, `Room-scoped members/mention names (NOT DM destinations): []`} {
		if !strings.Contains(out, want) {
			t.Fatalf("denial missing %q: %s", want, out)
		}
	}
}

func TestMessageDestinationGuidanceEmptyConfiguration(t *testing.T) {
	x := NewXMPPBridge(ResolvedAccount{}, func(InboundMessage) {}, func(string, string) {})
	want := `Allowed 1:1 JIDs: []. Allowed room JIDs: []. Room-scoped members/mention names (NOT DM destinations): []. Send to the room JID and mention the member in text.`
	if got := x.messageDestinationGuidance(); got != want {
		t.Fatalf("empty configuration guidance = %q, want %q", got, want)
	}
}

func TestSendMessageNoopAcceptsEmptyTextWithoutDelivery(t *testing.T) {
	b := newTestBridge(ResolvedAccount{Owner: "zach@x"})
	var buf bytes.Buffer
	b.rpc = &RPCClient{stdin: &nopClose{buf: &buf}, mu: sync.Mutex{}}
	b.handleSendMessageRelay("r1", "noop", "", "")
	if got := buf.String(); !strings.Contains(got, `"value":"noop"`) {
		t.Fatalf("noop response = %q, want successful noop result", got)
	}
	if b.replied() {
		t.Fatal("noop must not count as a delivered message")
	}
}

func TestSendMessageEmptyTextStillRejectedForRecipient(t *testing.T) {
	out := relayOutput(t, ResolvedAccount{Owner: "zach@x"}, `{"action":"send_message","to":"zach@x","text":""}`)
	if !strings.Contains(out, "send_message requires non-empty to and text") {
		t.Fatalf("empty recipient message should be rejected, got %q", out)
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

func TestMessagingContractSeparatesMembershipFromDestinations(t *testing.T) {
	for _, arbitrary := range []bool{false, true} {
		b := newTestBridge(ResolvedAccount{Owner: "owner@x", AllowArbitraryJid: arbitrary})
		got := b.messagingContract()
		for _, want := range []string{"send to the room JID and mention the member in text", "`sender:` identifies the author, not an authorized DM destination"} {
			if !strings.Contains(got, want) {
				t.Errorf("contract missing %q: %s", want, got)
			}
		}
		want := "By default, send_message and read_messages allow only the owner and configured rooms. Room members are mention targets within their room, NOT DM destinations."
		if arbitrary {
			want = "allowArbitraryJid permits other syntactically valid peer JIDs for both tools; unconfigured rooms remain unavailable. Room membership itself never grants DM permission."
		}
		if !strings.Contains(got, want) || strings.Contains(got, "use `sender:` to DM") {
			t.Errorf("incorrect destination policy: %s", got)
		}
	}
}

func TestRoomMembershipDoesNotAuthorizeFilesOrHistory(t *testing.T) {
	for _, arbitrary := range []bool{false, true} {
		for _, to := range []string{"alice@x", "alice@x/phone"} {
			b := newTestBridge(ResolvedAccount{Owner: "owner@x", Rooms: []string{"team@muc.x"}, AllowArbitraryJid: arbitrary})
			b.xmpp.occupants["team@muc.x"] = map[string]string{"alice": "alice@x"}
			var rpc bytes.Buffer
			b.rpc = &RPCClient{stdin: &nopClose{buf: &rpc}}
			b.handleToolRelay("r1", `{"action":"file","to":"`+to+`","path":"/nonexistent"}`)
			if !strings.Contains(rpc.String(), "not an allowed destination") || b.replied() || b.runDeliveries != 0 {
				t.Fatalf("room membership authorized file send (arbitrary=%v): %s", arbitrary, rpc.String())
			}
			if !arbitrary {
				rpc.Reset()
				b.handleToolRelay("r2", `{"action":"read_messages","target":"`+to+`"}`)
				if !strings.Contains(rpc.String(), "not an allowed target") {
					t.Fatalf("room membership authorized history read: %s", rpc.String())
				}
			}
		}
	}
}
