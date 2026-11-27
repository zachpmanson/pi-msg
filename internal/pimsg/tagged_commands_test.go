package pimsg

import (
	"context"
	"path/filepath"
	"regexp"
	"strings"
	"testing"
)

func TestTaggedRoomControls(t *testing.T) {
	for _, tc := range []struct {
		body, name, arg string
		busy, admitted  bool
	}{
		{body: "@all /new", name: "new", busy: true},
		{body: "@PI, @other !new", name: "new"},
		{body: "@other /name @pi", busy: true},
		{body: "@pi /name a session @other", name: "name", arg: "a session @other"},
		{body: "@free /new", name: "new"},
		{body: "@free /new", busy: true},
		{body: "@free &", name: "next", busy: true, admitted: true},
		{body: "@pi /next", name: "next"},
		{body: "@all /new didn't work"},
		{body: "@all /new\nHere is what happened"},
		{body: "@pi\n/new"},
		{body: "@all `/new`"},
		{body: "try @all /new"},
		{body: "@pi@example.org /new"},
		{body: "@all /unk"},
	} {
		b := roomBridge()
		if tc.busy {
			b.markRan()
		}
		m := InboundMessage{Room: "team@muc.x.com", Body: tc.body, FromOwner: true, Addressed: tc.admitted}
		name, arg, ok := boundaryCommand(m, b)
		if ok != (tc.name != "") || name != tc.name || arg != tc.arg {
			t.Errorf("%q (busy=%t, admitted=%t): got (%q, %q, %t), want (%q, %q, %t)", tc.body, tc.busy, tc.admitted, name, arg, ok, tc.name, tc.arg, tc.name != "")
		}
		m.FromOwner = false
		if _, _, ok := boundaryCommand(m, b); ok {
			t.Errorf("peer can execute %q", tc.body)
		}
	}
}

func TestTaggedHistoricalControlNeedsVerifiedOrigin(t *testing.T) {
	b := roomBridge()
	for _, tag := range []string{"@all", "@free", "@pi"} {
		m := InboundMessage{Room: "team@muc.x.com", Body: tag + " /new", Historical: true, FromOwner: true, RealJID: b.acct.Owner}
		if _, _, ok := boundaryCommand(b.recoveredRoomAuthor(m), b); ok {
			t.Errorf("current occupant map authorized historical %s", m.Body)
		}
		m.OriginJID = b.acct.Owner
		if !historicalBoundaryControl(b.recoveredRoomAuthor(m), b) {
			t.Errorf("verified %s lost boundary quarantine", m.Body)
		}
	}
}

func TestTaggedRecoveryDoesNotHoldOffTargetControl(t *testing.T) {
	f := nextSetup(t)
	f.incoming("/next", "current-boundary")
	f.b.deliverRecovered([]InboundMessage{{
		Body: "@another /new", Room: "team@muc.x", ID: "off-target",
		Historical: true, OriginJID: f.b.acct.Owner,
	}}, nil)
	if n := len(f.b.inbox.protectedPending()); n != 0 {
		t.Fatalf("off-target tagged control held %d protected entries", n)
	}
	if f.b.next.supersede != nil {
		t.Fatal("off-target tagged control requested supersession")
	}
}

func TestNewAcknowledgesExactOriginatingStanza(t *testing.T) {
	for _, tc := range []struct {
		name, roomID, sender, want string
		direct, reactions          bool
	}{
		{"room", "room-command-id", "team@muc.x/zach", `to="team@muc.x" type="groupchat"><reactions xmlns="urn:xmpp:reactions:0" id="room-command-id"><reaction>🆕</reaction>`, false, true},
		{"room-no-sender-ID", "room-command-id", "team@muc.x/zach", `to="team@muc.x" type="groupchat"><reactions xmlns="urn:xmpp:reactions:0" id="room-command-id"><reaction>🆕</reaction>`, false, true},
		{"DM", "", "zach@x/phone", `to="zach@x/phone" type="chat"><reactions xmlns="urn:xmpp:reactions:0" id="sender-command-id"><reaction>🆕</reaction>`, true, true},
		{"room-no-stable-ID", "", "team@muc.x/zach", `to="team@muc.x" type="groupchat"><body>🆕 new session ready`, false, true},
		{"room-disabled", "room-command-id", "team@muc.x/zach", `to="team@muc.x" type="groupchat"><body>🆕 new session ready`, false, false},
		{"DM-disabled", "", "zach@x/phone", `to="zach@x" type="chat"><body>🆕 new session ready`, true, false},
		{"DM-send-failure", "", "bad target@x/phone", `to="zach@x" type="chat"><body>🆕 new session ready`, true, true},
	} {
		t.Run(tc.name, func(t *testing.T) {
			b, wire, _ := messageTransportBridge(t, false)
			b.ctx = context.Background()
			b.acct.RoomTrigger = "pi"
			b.acct.Reactions = tc.direct && tc.reactions
			b.acct.RoomReactions = !tc.direct && tc.reactions
			b.rpc = NewRPCClient(fakePiRespond(t, `{"type":"response","id":"%s","success":true,"data":{"cancelled":false}}`), "", "", "", nil)
			if err := b.rpc.Start(); err != nil {
				t.Fatal(err)
			}
			defer b.rpc.Stop()
			m := InboundMessage{Body: "/new", From: tc.sender, FromOwner: true, ID: "sender-command-id", RoomStanzaID: tc.roomID, Direct: tc.direct}
			if tc.direct {
				b.handleCanonical(m.Body, m)
			} else {
				m.Body, m.Room = "@all /new", "team@muc.x"
				if tc.name == "room-no-sender-ID" {
					m.ID = ""
					b.inbox = newInbox(filepath.Join(t.TempDir(), "inbox.json"), b.log)
					b.onInbound(m)
					if n := len(b.inbox.pending()); n != 0 {
						t.Fatalf("handled command retained %d inbox entries", n)
					}
				} else {
					b.handleRoom(m)
				}
			}
			got := regexp.MustCompile(` id="[0-9a-f]+"><reactions`).ReplaceAllString(wire.String(), `><reactions`)
			if strings.Count(got, tc.want) != 1 || (strings.Contains(got, "<reactions") && strings.Contains(got, "new session ready")) {
				t.Fatalf("want only %s, got %s", tc.want, got)
			}
		})
	}
}
