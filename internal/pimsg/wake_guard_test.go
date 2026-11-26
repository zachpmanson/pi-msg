package pimsg

import (
	"encoding/json"
	"encoding/xml"
	"fmt"
	"reflect"
	"strings"
	"sync"
	"testing"
)

// Feed actual decoded XML, including namespace handling, through the production
// presence handler rather than constructing the new cache directly.
func wakePresence(t *testing.T, x *XMPPBridge, from, real, show, node, ptype string) {
	t.Helper()
	body := ""
	if show != "" {
		body += "<show>" + show + "</show>"
	}
	if node != "" {
		body += "<c xmlns='" + capsNS + "' node='" + node + "' ver='test' hash='sha-1'/>"
	}
	if real != "" {
		body += "<x xmlns='http://jabber.org/protocol/muc#user'><item jid='" + real + "'/></x>"
	}
	toks := mamTokens(t, "<presence from='"+from+"' type='"+ptype+"'>"+body+"</presence>")
	start := toks[0].(xml.StartElement)
	if err := x.handlePresence(&start, toks[1:len(toks)-1]); err != nil {
		t.Fatal(err)
	}
}

func wakeRelay(t *testing.T, b *Bridge, to, text, reply string, flag any) {
	t.Helper()
	args := map[string]any{"action": "send_message", "to": to, "text": text, "replyTo": reply, "replyTimeoutSeconds": 30}
	if flag != nil {
		args["allowWake"] = flag
	}
	payload, err := json.Marshal(args)
	if err != nil {
		t.Fatal(err)
	}
	b.handleToolRelay("wake-test", string(payload))
}

func TestWakeGuardRoomTransport(t *testing.T) {
	for _, tt := range []struct {
		name, text, show, node, real string
		flag                         any
		blocked                      bool
	}{
		{name: "away", text: "@alice hello", show: "away", node: capsNode, blocked: true},
		{name: "xa", text: "@alice hello", show: "xa", node: capsNode, blocked: true},
		{name: "explicit false", text: "@alice hello", show: "away", node: capsNode, flag: false, blocked: true},
		{name: "override", text: "@alice hello", show: "away", node: capsNode, flag: true},
		{name: "available", text: "@alice hello", node: capsNode},
		{name: "busy", text: "@alice hello", show: "dnd", node: capsNode},
		{name: "unknown show", text: "@alice hello", show: "chat", node: capsNode},
		{name: "unknown identity", text: "@alice hello", show: "away"},
		{name: "human", text: "@alice hello", show: "away", node: "https://human-client.example"},
		{name: "owner", text: "@alice hello", show: "away", node: capsNode, real: "zach@x/phone"},
		{name: "self", text: "@alice hello", show: "away", node: capsNode, real: "pi@x/resource"},
		{name: "anonymous bot", text: "@alice hello", show: "away", node: capsNode, blocked: true},
		{name: "unaddressed", text: "claiming: Android lane", show: "away", node: capsNode},
		{name: "bare name", text: "Alice please check", show: "away", node: capsNode, blocked: true},
		{name: "leading colon", text: "ALICE: hello", show: "away", node: capsNode, blocked: true},
		{name: "leading comma", text: "alice, hello", show: "away", node: capsNode, blocked: true},
		{name: "inline colon", text: "hey alice: hello", show: "away", node: capsNode, blocked: true},
		{name: "bare name boundary", text: "alices malice", show: "away", node: capsNode},
		{name: "sigil prefix is not a match", text: "@alices", show: "away", node: capsNode},
		{name: "explicit tag overrides the bare name", text: "@other ask alice", show: "away", node: capsNode},
		{name: "user at host is not a match", text: "check alice@naboo or root@alice", show: "away", node: capsNode},
		{name: "punctuated explicit tag", text: "hello @alice.", show: "away", node: capsNode, blocked: true},
		{name: "inline code", text: "avoid `@alice`", show: "away", node: capsNode},
		{name: "quote", text: "> @alice hello", show: "away", node: capsNode},
		{name: "fence", text: "```\n@alice\n```", show: "away", node: capsNode},
		{name: "quoted then addressed", text: "> old text\n@alice hello", show: "away", node: capsNode, blocked: true},
		{name: "everyone", text: "@everyone lane claimed", show: "away", node: capsNode, blocked: true},
		{name: "all", text: "@all lane claimed", show: "xa", node: capsNode, blocked: true},
		{name: "here", text: "@here lane claimed", show: "away", node: capsNode, blocked: true},
		{name: "free", text: "@free lane claimed", show: "xa", node: capsNode, blocked: true},
		{name: "quoted broadcast", text: "`@everyone`", show: "away", node: capsNode},
		{name: "chunkable", text: "@alice " + strings.Repeat("large message ", 10000), show: "away", node: capsNode, blocked: true},
	} {
		t.Run(tt.name, func(t *testing.T) {
			b, wire, rpc := messageTransportBridge(t, false)
			wakePresence(t, b.xmpp, "team@muc.x/alice", tt.real, tt.show, tt.node, "")
			wakeRelay(t, b, "team@muc.x", tt.text, "", tt.flag)
			defer b.stopReplyWatches()
			if tt.blocked {
				if wire.Len() != 0 || !strings.Contains(rpc.String(), "No message was sent") || !strings.Contains(rpc.String(), "allow_wake=true") || !strings.Contains(rpc.String(), "alice ("+tt.show+")") {
					t.Fatalf("guard did not reject atomically: wire=%s rpc=%s", wire.String(), rpc.String())
				}
				if b.replied() || b.runDeliveries != 0 || len(b.replyWatches) != 0 || len(b.xmpp.msgHistory) != 0 || len(b.xmpp.spokeAt) != 0 || b.reactID != "" {
					t.Fatal("rejected send changed delivery/watch/history/participation/reaction state")
				}
			} else if !strings.Contains(rpc.String(), "sent:") || !strings.Contains(wire.String(), "<body>") {
				t.Fatalf("exempt send failed: wire=%s rpc=%s", wire.String(), rpc.String())
			}
		})
	}
}

func TestWakeGuardRoomTargetUnionAndReplies(t *testing.T) {
	for _, archived := range []bool{false, true} {
		for _, author := range []string{"alice", "bob", "unknown", "owner", "self"} {
			t.Run(fmt.Sprintf("archived=%v/%s", archived, author), func(t *testing.T) {
				b, wire, rpc := messageTransportBridge(t, false)
				wakePresence(t, b.xmpp, "team@muc.x/alice", "alice@x", "away", capsNode, "")
				wakePresence(t, b.xmpp, "team@muc.x/bob", "bob@x", "xa", capsNode, "")
				wakePresence(t, b.xmpp, "team@muc.x/owner", "zach@x", "away", capsNode, "")
				wakePresence(t, b.xmpp, "team@muc.x/self", "pi@x", "away", capsNode, "")
				entry := msgHistoryEntry{FromJID: "team@muc.x/" + author, ConversationJID: "team@muc.x"}
				if archived {
					b.xmpp.recordReadHistory([]InboundMessage{{ID: "quoted", From: entry.FromJID}}, "team@muc.x")
				} else {
					b.xmpp.msgHistory["quoted"] = entry
				}
				wakeRelay(t, b, "team@muc.x", "hello", "quoted", nil)
				defer b.stopReplyWatches()
				blocked := author == "alice" || author == "bob"
				if (wire.Len() == 0) != blocked {
					t.Fatalf("reply guard blocked=%v: wire=%s rpc=%s", blocked, wire.String(), rpc.String())
				}
			})
		}
	}
	b, wire, rpc := messageTransportBridge(t, false)
	wakePresence(t, b.xmpp, "team@muc.x/alice", "alice@x", "away", capsNode, "")
	wakePresence(t, b.xmpp, "team@muc.x/bob", "bob@x", "xa", capsNode, "")
	wakePresence(t, b.xmpp, "team@muc.x/awake", "awake@x", "", capsNode, "")
	wakeRelay(t, b, "team@muc.x", "@awake @bob @alice hello", "", nil)
	if wire.Len() != 0 || !strings.Contains(rpc.String(), "alice (away), bob (xa)") {
		t.Fatalf("mixed targets not rejected deterministically: %s %s", wire.String(), rpc.String())
	}
	b.xmpp.msgHistory["awake"] = msgHistoryEntry{FromJID: "team@muc.x/awake", ConversationJID: "team@muc.x"}
	rpc.Reset()
	wakeRelay(t, b, "team@muc.x", "@alice hello", "awake", nil)
	if wire.Len() != 0 || !strings.Contains(rpc.String(), "alice (away)") {
		t.Fatal("awake reply target bypassed sleeping body target")
	}
	// Permission/reply validation still takes priority and override grants neither.
	rpc.Reset()
	wakeRelay(t, b, "alice@x", "hello", "missing", true)
	if wire.Len() != 0 || !strings.Contains(rpc.String(), "not an allowed destination") {
		t.Fatal("allow_wake bypassed destination permission")
	}
	rpc.Reset()
	wakeRelay(t, b, "team@muc.x", "@alice hello", "missing", true)
	if wire.Len() != 0 || !strings.Contains(rpc.String(), "not in message history") {
		t.Fatal("allow_wake bypassed reply validation")
	}
}

func TestWakeGuardPresenceLifecycle(t *testing.T) {
	b, _, _ := messageTransportBridge(t, true)
	x := b.xmpp
	check := func(want bool) {
		t.Helper()
		got := x.sleepingMessageTargets("team@muc.x", "@alice", nil, true)
		if (len(got) > 0) != want {
			t.Fatalf("sleeping targets=%v want blocked=%v", got, want)
		}
	}
	wakePresence(t, x, "team@muc.x/alice", "alice@x", "away", capsNode, "")
	check(true)
	// Status updates need not repeat caps; irrelevant nested show must not win.
	wakePresence(t, x, "team@muc.x/alice", "alice@x", "xa", "", "")
	check(true)
	wakePresence(t, x, "team@muc.x/alice", "alice@x", "", capsNode, "")
	check(false)
	wakePresence(t, x, "team@muc.x/alice", "alice@x", "away", capsNode, "")
	for _, ptype := range []string{"probe", "subscribed", "unsubscribe"} {
		wakePresence(t, x, "team@muc.x/alice", "", "", "", ptype)
		check(true)
	}
	wakePresence(t, x, "team@muc.x/alice", "", "", "", "error")
	check(false)
	wakePresence(t, x, "team@muc.x/alice", "alice@x", "away", capsNode, "")
	wakePresence(t, x, "team@muc.x/alice", "", "", "", "unavailable")
	check(false)
	// A replacement occupant without pi-msg identity must not inherit the old bot.
	wakePresence(t, x, "team@muc.x/alice", "new-human@x", "away", "", "")
	check(false)
	wakePresence(t, x, "team@muc.x/alice", "alice@x", "away", capsNode, "")
	wakePresence(t, x, "team@muc.x/alice", "other@x", "away", "", "")
	check(false)
	wakePresence(t, x, "team@muc.x/alice", "alice@x", "away", capsNode, "")
	wakePresence(t, x, "team@muc.x/alice", "alice@x", "away", "other-client", "")
	check(false)
	wakePresence(t, x, "team@muc.x/alice", "alice@x", "away", capsNode, "")
	x.setOffline()
	check(false)
	if len(x.peerPresence) != 0 {
		t.Fatal("disconnect retained peer presence")
	}
	wakePresence(t, x, "team@muc.x/alice", "alice@x", "", capsNode, "")
	check(false)
}

func TestWakeGuardDirectResourceAggregation(t *testing.T) {
	b, wire, rpc := messageTransportBridge(t, true)
	x := b.xmpp
	wakePresence(t, x, "alice@x/one", "", "away", capsNode, "")
	wakePresence(t, x, "alice@x/two", "", "xa", capsNode, "")
	for _, to := range []string{"alice@x", "alice@x/one", "alice@x/two"} {
		wakeRelay(t, b, to, "hello", "", nil)
		if wire.Len() != 0 || !strings.Contains(rpc.String(), "No message was sent") {
			t.Fatalf("sleeping direct destination escaped guard: %s: %s %s", to, wire.String(), rpc.String())
		}
		rpc.Reset()
	}
	if got := x.sleepingMessageTargets("alice@x/unseen", "hello", nil, false); len(got) > 0 {
		t.Fatal("unknown full resource inherited sibling status")
	}
	wakePresence(t, x, "alice@x/two", "", "", capsNode, "")
	if got := x.sleepingMessageTargets("alice@x", "hello", nil, false); len(got) > 0 {
		t.Fatal("bare JID ignored available resource")
	}
	if got := x.sleepingMessageTargets("alice@x/one", "hello", nil, false); len(got) != 1 {
		t.Fatal("full resource ignored its own away status")
	}
	wakePresence(t, x, "alice@x/two", "", "", "", "unavailable")
	wakeRelay(t, b, "alice@x", "hello", "", true)
	if !strings.Contains(rpc.String(), "sent:") || !strings.Contains(wire.String(), "composing") || !strings.Contains(wire.String(), "<body>hello</body>") {
		t.Fatalf("override did not send normal composing/body: %s %s", wire.String(), rpc.String())
	}
	b.stopReplyWatches()
	wakePresence(t, x, "alice@x", "", "", "", "error")
	if got := x.sleepingMessageTargets("alice@x/one", "hello", nil, false); len(got) != 0 {
		t.Fatal("bare error retained resource evidence")
	}
	// A room witness can identify a bare destination, but cannot invent a full
	// direct resource. A known active witness in another room wins for bare DM.
	wakePresence(t, x, "team@muc.x/alice", "alice@x", "away", capsNode, "")
	if got := x.sleepingMessageTargets("alice@x", "hello", nil, false); len(got) != 1 {
		t.Fatal("room witness did not identify sleeping bare peer")
	}
	if got := x.sleepingMessageTargets("alice@x/unseen", "hello", nil, false); len(got) != 0 {
		t.Fatal("room witness invented direct resource")
	}
	x.roomBares["other@muc.x"] = true
	wakePresence(t, x, "other@muc.x/alias", "alice@x", "", capsNode, "")
	if got := x.sleepingMessageTargets("alice@x", "hello", nil, false); len(got) != 0 {
		t.Fatal("active room witness did not win for bare DM")
	}
	if got := x.sleepingMessageTargets("team@muc.x", "@alice", nil, true); len(got) != 1 {
		t.Fatal("another room overwrote exact room target")
	}
	for _, to := range []string{"zach@x", "zach@x/phone", "pi@x", "pi@x/self"} {
		wakePresence(t, x, to, "", "away", capsNode, "")
		if got := x.sleepingMessageTargets(to, "hello", nil, false); len(got) != 0 {
			t.Fatalf("owner/self was not exempt: %s", to)
		}
	}
}

func TestWakeGuardMalformedOverrideHasNoEffects(t *testing.T) {
	for _, value := range []string{`null`, `"true"`, `1`, `[]`, `{}`} {
		t.Run(value, func(t *testing.T) {
			b, wire, rpc := messageTransportBridge(t, true)
			b.handleToolRelay("bad", `{"action":"send_message","to":"zach@x","text":"hello","allowWake":`+value+`}`)
			if wire.Len() != 0 || b.replied() || !strings.Contains(rpc.String(), "allow_wake must be a boolean") {
				t.Fatalf("malformed override accepted: %s %s", wire.String(), rpc.String())
			}
		})
	}
}

func TestWakeGuardDefersPeerTypingAcrossPresenceChange(t *testing.T) {
	for _, initial := range []string{"unknown", "available"} {
		t.Run(initial, func(t *testing.T) {
			b, wire, rpc := messageTransportBridge(t, true)
			if initial == "available" {
				wakePresence(t, b.xmpp, "alice@x/agent", "", "", capsNode, "")
			}
			partial := map[string]any{"content": []any{map[string]any{"type": "toolCall", "id": "call-1", "name": "send_message", "arguments": map[string]any{}}}}
			b.handleStreamDelta(streamDelta(map[string]any{"type": "toolcall_start", "contentIndex": float64(0), "partial": partial}))
			b.handleStreamDelta(streamDelta(map[string]any{"type": "toolcall_delta", "contentIndex": float64(0), "partial": partial, "delta": `{"to":"alice@x","text":"hello"}`}))
			if wire.Len() != 0 || b.streamToolCalls[0].started {
				t.Fatal("peer typing leaked before validation")
			}
			wakePresence(t, b.xmpp, "alice@x/agent", "", "xa", capsNode, "")
			wakeRelay(t, b, "alice@x", "hello", "", nil)
			b.clearAllStreamComposing()
			if wire.Len() != 0 || !strings.Contains(rpc.String(), "No message was sent") {
				t.Fatalf("presence transition leaked a stanza: %s %s", wire.String(), rpc.String())
			}
		})
	}
}

func TestWakeGuardConcurrentSnapshots(t *testing.T) {
	x := NewXMPPBridge(ResolvedAccount{JID: "pi@x", Owner: "zach@x", Rooms: []string{"team@muc.x"}}, func(InboundMessage) {}, nil)
	// Parse once; immutable token slices can be read by concurrent handlers.
	toks := mamTokens(t, `<presence from="team@muc.x/alice"><show>away</show><c xmlns="http://jabber.org/protocol/caps" node="http://pi-msg"/></presence>`)
	start := toks[0].(xml.StartElement)
	var wg sync.WaitGroup
	for range 4 {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for range 50 {
				if err := x.handlePresence(&start, toks[1:len(toks)-1]); err != nil {
					t.Error(err)
				}
				got := x.sleepingMessageTargets("team@muc.x", "@alice", nil, true)
				if !reflect.DeepEqual(got, []string{"alice (away)"}) {
					t.Errorf("snapshot=%v", got)
				}
			}
		}()
	}
	wg.Wait()
}

func TestWakeGuardDirectExemptTransport(t *testing.T) {
	for _, tt := range []struct{ name, to, show, node, unavailable string }{
		{name: "available", to: "alice@x/agent", node: capsNode},
		{name: "busy", to: "alice@x/agent", show: "dnd", node: capsNode},
		{name: "human", to: "alice@x/phone", show: "away", node: "https://human-client.example"},
		{name: "unknown", to: "alice@x/unseen"},
		{name: "offline", to: "alice@x/agent", show: "away", node: capsNode, unavailable: "unavailable"},
		{name: "owner", to: "zach@x/phone", show: "xa", node: capsNode},
		{name: "self", to: "pi@x/agent", show: "xa", node: capsNode},
	} {
		t.Run(tt.name, func(t *testing.T) {
			b, wire, rpc := messageTransportBridge(t, true)
			if tt.node != "" {
				wakePresence(t, b.xmpp, tt.to, "", tt.show, tt.node, "")
			}
			if tt.unavailable != "" {
				wakePresence(t, b.xmpp, tt.to, "", "", "", tt.unavailable)
			}
			wakeRelay(t, b, tt.to, "hello", "", nil)
			defer b.stopReplyWatches()
			if !strings.Contains(rpc.String(), "sent:") || !strings.Contains(wire.String(), "<body>hello</body>") {
				t.Fatalf("exempt direct send failed: %s %s", wire.String(), rpc.String())
			}
		})
	}
}

func TestWakeGuardRejectionPreservesExistingState(t *testing.T) {
	b, wire, rpc := messageTransportBridge(t, false)
	wakeRelay(t, b, "team@muc.x", "a prior message", "", nil)
	defer b.stopReplyWatches()
	watch := b.replyWatches["team@muc.x"]
	oldHistory := fmt.Sprint(b.xmpp.msgHistory)
	oldSpoke := fmt.Sprint(b.xmpp.spokeAt)
	oldReactTo, oldReactID := b.reactTo, b.reactID
	oldDeliveryCount := b.runDeliveries
	wire.Reset()
	rpc.Reset()
	wakePresence(t, b.xmpp, "team@muc.x/alice", "alice@x", "xa", capsNode, "")
	wakeRelay(t, b, "team@muc.x", "@alice hello", "", false)
	if wire.Len() != 0 || !strings.Contains(rpc.String(), "No message was sent") {
		t.Fatalf("send not rejected: %s %s", wire.String(), rpc.String())
	}
	if b.replyWatches["team@muc.x"] != watch || fmt.Sprint(b.xmpp.msgHistory) != oldHistory || fmt.Sprint(b.xmpp.spokeAt) != oldSpoke || b.reactTo != oldReactTo || b.reactID != oldReactID || b.runDeliveries != oldDeliveryCount || !b.replied() {
		t.Fatal("rejection changed existing watch/history/participation/reaction/delivery state")
	}
}

func TestWakeGuardSelfNickAndRoomDeparture(t *testing.T) {
	b, _, _ := messageTransportBridge(t, true)
	x := b.xmpp
	x.acct.Nick = "original"
	// Real status-110 XML confirms a server-renamed anonymous self occupant.
	toks := mamTokens(t, `<presence from="team@muc.x/renamed"><show>xa</show><c xmlns="http://jabber.org/protocol/caps" node="http://pi-msg"/><x xmlns="http://jabber.org/protocol/muc#user"><status code="110"/></x></presence>`)
	start := toks[0].(xml.StartElement)
	if err := x.handlePresence(&start, toks[1:len(toks)-1]); err != nil {
		t.Fatal(err)
	}
	if got := x.sleepingMessageTargets("team@muc.x", "@renamed", nil, true); len(got) != 0 {
		t.Fatalf("anonymous self blocked: %v", got)
	}
	wakePresence(t, x, "team@muc.x/original", "other@x", "away", capsNode, "")
	if got := x.sleepingMessageTargets("team@muc.x", "@original", nil, true); len(got) != 1 {
		t.Fatal("replacement at old self nick was exempt")
	}
	wakePresence(t, x, "team@muc.x/renamed", "", "", "", "unavailable")
	if got := x.sleepingMessageTargets("team@muc.x", "@original", nil, true); len(got) != 0 {
		t.Fatal("room departure retained stale peer evidence")
	}
}

func TestWakeGuardPresenceIgnoresNestedMetadata(t *testing.T) {
	x := NewXMPPBridge(ResolvedAccount{JID: "pi@x", Owner: "zach@x", Rooms: []string{"team@muc.x"}}, func(InboundMessage) {}, nil)
	for _, body := range []string{
		`<show>away</show><x xmlns="other"><c xmlns="http://jabber.org/protocol/caps" node="http://pi-msg"/></x>`,
		`<x xmlns="other"><show>away</show></x><c xmlns="http://jabber.org/protocol/caps" node="http://pi-msg"/>`,
	} {
		toks := mamTokens(t, `<presence from="team@muc.x/alice">`+body+`</presence>`)
		start := toks[0].(xml.StartElement)
		if err := x.handlePresence(&start, toks[1:len(toks)-1]); err != nil {
			t.Fatal(err)
		}
		if got := x.sleepingMessageTargets("team@muc.x", "@alice", nil, true); len(got) != 0 {
			t.Fatalf("nested metadata established sleeping bot: %v", got)
		}
	}
}
