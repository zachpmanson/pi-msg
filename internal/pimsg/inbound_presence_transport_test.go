package pimsg

import (
	"bytes"
	"context"
	"encoding/xml"
	"io"
	"testing"
	"time"

	"mellium.im/xmpp"
	"mellium.im/xmpp/jid"
)

// Exercise actual presence encoding to the roster and both joined rooms without
// connecting to a server or launching Pi. Ignored traffic must emit no stanza.
func TestInboundPresenceTransport(t *testing.T) {
	b, rpc := inboundPresenceBridge(t)
	wire := new(bytes.Buffer)
	session, err := xmpp.NewSession(context.Background(), jid.JID{}, jid.JID{}, wire, xmpp.Ready,
		xmpp.NewNegotiator(func(*xmpp.Session, *xmpp.StreamConfig) xmpp.StreamConfig { return xmpp.StreamConfig{} }))
	if err != nil {
		t.Fatal(err)
	}
	b.xmpp.session, b.xmpp.online = session, true
	for _, room := range b.acct.Rooms {
		b.xmpp.selfNick[room] = "pi"
	}
	oldIdle := time.Now().Add(-time.Hour).Truncate(time.Second)
	b.idleSince = oldIdle
	b.xmpp.SetIdleSince(oldIdle)
	b.xmpp.SetPresence("away", "idle activity")
	if wire.Len() == 0 {
		t.Fatal("initial away presence was not encoded")
	}
	wire.Reset()
	for _, room := range b.acct.Rooms {
		b.onInbound(InboundMessage{Room: room, Nick: "peer", From: room + "/peer", Body: "unrelated room chatter"})
	}
	if wire.Len() != 0 || rpc.Len() != 0 || b.inbox.len() != 0 {
		t.Fatalf("ignored chatter emitted stanza/RPC: %s / %s", wire.String(), rpc.String())
	}
	b.onInbound(InboundMessage{ID: "accepted", Room: b.acct.Rooms[0], Nick: "peer", From: b.acct.Rooms[0] + "/peer", Body: "@pi work"})
	if rpc.Len() == 0 {
		t.Fatal("accepted mention did not prompt Pi")
	}
	decoder := xml.NewDecoder(wire)
	seen := map[string]bool{}
	for {
		var stanza struct {
			XMLName xml.Name
			To      string `xml:"to,attr"`
			Show    string `xml:"show"`
			Idle    struct {
				Since string `xml:"since,attr"`
			} `xml:"urn:xmpp:idle:1 idle"`
		}
		if err := decoder.Decode(&stanza); err == io.EOF {
			break
		} else if err != nil {
			t.Fatal(err)
		}
		if stanza.XMLName.Local != "presence" {
			continue
		}
		if stanza.Show == "away" || stanza.Idle.Since == idleSinceISO(oldIdle) {
			t.Fatalf("accepted mention retained stale away/idle: %+v", stanza)
		}
		if stanza.Show == "dnd" {
			seen[stanza.To] = true
		}
	}
	for _, target := range []string{"", "testing@muc.x/pi", "agents@muc.x/pi"} {
		if !seen[target] {
			t.Errorf("missing encoded busy presence for %q", target)
		}
	}
}
