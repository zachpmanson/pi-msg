package pimsg

import (
	"fmt"
	"strings"
	"testing"
)

func TestWakeGuardCanonicalDirectJIDTransport(t *testing.T) {
	for _, presence := range []string{"alice@x/Agent", "ALICE@X/Agent"} {
		for _, to := range []string{"alice@x/Agent", "ALICE@X/Agent"} {
			for _, flag := range []any{nil, false, true} {
				t.Run(fmt.Sprintf("%s/to=%s/override=%v", presence, to, flag), func(t *testing.T) {
					b, wire, rpc := messageTransportBridge(t, true)
					wakePresence(t, b.xmpp, presence, "", "away", capsNode, "")
					wakeRelay(t, b, to, "hello", "", flag)
					defer b.stopReplyWatches()
					if flag == true {
						if !strings.Contains(rpc.String(), "sent:") || !strings.Contains(wire.String(), `<body>hello</body>`) || !strings.Contains(wire.String(), `to="alice@x/Agent"`) {
							t.Fatalf("override did not use canonical transport JID: %s %s", wire.String(), rpc.String())
						}
						return
					}
					if wire.Len() != 0 || !strings.Contains(rpc.String(), "No message was sent") || b.replied() || len(b.replyWatches) != 0 || len(b.xmpp.msgHistory) != 0 {
						t.Fatalf("equivalent full JID escaped zero-wire rejection: %s %s", wire.String(), rpc.String())
					}
					if got := b.xmpp.sleepingMessageTargets("alice@x/agent", "hello", nil, false); len(got) != 0 {
						t.Fatalf("resource case was folded: %v", got)
					}
				})
			}
		}
	}
}

func TestWakeGuardCanonicalPresenceInvalidation(t *testing.T) {
	for _, presence := range []string{"alice@x/Agent", "ALICE@X/Agent"} {
		for _, unavailable := range []string{"alice@x/Agent", "ALICE@X/Agent", "ALICE@X"} {
			for _, ptype := range []string{"unavailable", "error"} {
				t.Run(presence+"/"+unavailable+"/"+ptype, func(t *testing.T) {
					b, _, _ := messageTransportBridge(t, true)
					wakePresence(t, b.xmpp, presence, "", "xa", capsNode, "")
					if got := b.xmpp.sleepingMessageTargets("ALICE@X/Agent", "hello", nil, false); len(got) != 1 {
						t.Fatalf("canonical presence not recorded: %v", got)
					}
					wakePresence(t, b.xmpp, unavailable, "", "", "", ptype)
					if len(b.xmpp.peerPresence) != 0 {
						t.Fatalf("canonical unavailable/error retained presence: %v", b.xmpp.peerPresence)
					}
				})
			}
		}
	}
	b, _, _ := messageTransportBridge(t, true)
	wakePresence(t, b.xmpp, "TEAM@MUC.X/Alice", "ALICE@X/Agent", "away", capsNode, "")
	wakePresence(t, b.xmpp, "team@muc.x/Alice", "alice@x/Agent", "xa", "", "")
	if got := b.xmpp.sleepingMessageTargets("TEAM@MUC.X", "hello", &replyTarget{author: "TEAM@MUC.X/Alice", id: "old"}, true); len(got) != 1 {
		t.Fatalf("canonical room/reply identity not retained: %v", got)
	}
	wakePresence(t, b.xmpp, "TEAM@MUC.X/Alice", "", "", "", "unavailable")
	if len(b.xmpp.peerPresence) != 0 {
		t.Fatalf("canonical room departure retained presence: %v", b.xmpp.peerPresence)
	}
	for _, to := range []string{"ZACH@X/Phone", "PI@X/Self"} {
		wakePresence(t, b.xmpp, to, "", "xa", capsNode, "")
		if got := b.xmpp.sleepingMessageTargets(to, "hello", nil, false); len(got) != 0 {
			t.Fatalf("canonical owner/self not exempt: %v", got)
		}
	}
}

func TestWakeGuardPreflightsActualRoomChunks(t *testing.T) {
	for _, tt := range []struct {
		name, text string
		blocked    bool
	}{
		{"quoted mention exposed by split", "> " + strings.Repeat("x ", 25000) + "@alice", true},
		{"quoted broadcast exposed by split", "> " + strings.Repeat("x ", 25000) + "@everyone", true},
		{"fence remains quoted across chunks", "```\n" + strings.Repeat("x\n", 25000) + "@alice\n```", false},
		{"fenced broadcast remains quoted across chunks", "```\n" + strings.Repeat("x\n", 25000) + "@everyone\n```", false},
	} {
		for _, override := range []bool{false, true} {
			t.Run(fmt.Sprintf("%s/override=%v", tt.name, override), func(t *testing.T) {
				parts := chunk(tt.text, maxBody)
				if len(parts) < 2 {
					t.Fatal("fixture must produce multiple transport chunks")
				}
				if tt.blocked {
					if addressed, _ := matchTriggerName(tt.text, "alice"); addressed || containsBroadcast(stripUnquoted(tt.text)) {
						t.Fatal("fixture must be quoted before transport chunking")
					}
				}
				b, wire, rpc := messageTransportBridge(t, false)
				wakePresence(t, b.xmpp, "team@muc.x/alice", "alice@x", "xa", capsNode, "")
				wakeRelay(t, b, "team@muc.x", tt.text, "", override)
				defer b.stopReplyWatches()
				if tt.blocked && !override {
					if wire.Len() != 0 || !strings.Contains(rpc.String(), "No message was sent") || b.replied() || len(b.replyWatches) != 0 || len(b.xmpp.msgHistory) != 0 || len(b.xmpp.spokeAt) != 0 {
						t.Fatalf("later-chunk target was not rejected before first send: wire bytes=%d rpc=%s", wire.Len(), rpc.String())
					}
				} else if !strings.Contains(rpc.String(), "sent:") || strings.Count(wire.String(), "<body>") != len(parts) {
					t.Fatalf("allowed send did not deliver all chunks: bodies=%d want=%d rpc=%s", strings.Count(wire.String(), "<body>"), len(parts), rpc.String())
				}
			})
		}
	}
}

func TestWakeGuardChunkTargetUnion(t *testing.T) {
	b, wire, rpc := messageTransportBridge(t, false)
	wakePresence(t, b.xmpp, "team@muc.x/alice", "alice@x", "away", capsNode, "")
	wakePresence(t, b.xmpp, "team@muc.x/bob", "bob@x", "xa", capsNode, "")
	b.xmpp.msgHistory["reply"] = msgHistoryEntry{FromJID: "team@muc.x/bob", ConversationJID: "team@muc.x"}
	text := "> " + strings.Repeat("x ", 25000) + "@alice"
	wakeRelay(t, b, "team@muc.x", text, "reply", nil)
	if wire.Len() != 0 || !strings.Contains(rpc.String(), "alice (away), bob (xa)") {
		t.Fatalf("body chunks and first-chunk reply targets were not unioned: wire bytes=%d rpc=%s", wire.Len(), rpc.String())
	}
	// The override leaves transport's first-chunk-only reply stamping unchanged.
	wakeRelay(t, b, "team@muc.x", text, "reply", true)
	defer b.stopReplyWatches()
	if !strings.Contains(rpc.String(), "sent:") || strings.Count(wire.String(), "<reply ") != 1 || strings.Count(wire.String(), "<body>") != len(chunk(text, maxBody)) {
		t.Fatalf("override changed chunk reply semantics: wire bytes=%d rpc=%s", wire.Len(), rpc.String())
	}
}
