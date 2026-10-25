package pimsg

import (
	"encoding/xml"
	"io"
	"strings"
	"testing"
	"time"
)

// Exercise the read-loop handler and real session encoder, not just marshaling
// a response struct. The server has already authorized any forwarded IQ-get.
func handleActivityIQ(t *testing.T, b *XMPPBridge, input string) {
	t.Helper()
	d := xml.NewDecoder(strings.NewReader(input))
	tok, err := d.Token()
	if err != nil {
		t.Fatal(err)
	}
	start := tok.(xml.StartElement)
	stream := struct {
		xml.TokenReader
		*xml.Encoder
	}{d, xml.NewEncoder(io.Discard)}
	if err := b.handle(stream, &start); err != nil {
		t.Fatal(err)
	}
}

func TestLastActivityTransport(t *testing.T) {
	for _, tt := range []struct {
		name     string
		since    time.Time
		disabled bool
		typ      string
		wantType string
		wantErr  string
		min, max uint64
	}{
		{name: "active", typ: "get", wantType: "result"},
		{name: "idle", since: time.Now().Add(-123 * time.Second), typ: "get", wantType: "result", min: 123, max: 125},
		{name: "future clock clamped", since: time.Now().Add(time.Hour), typ: "get", wantType: "result"},
		{name: "disabled", since: time.Now().Add(-time.Hour), disabled: true, typ: "get", wantType: "error", wantErr: "service-unavailable"},
		{name: "set rejected", typ: "set", wantType: "error", wantErr: "bad-request"},
		{name: "result ignored", typ: "result"},
		{name: "error ignored", typ: "error"},
	} {
		t.Run(tt.name, func(t *testing.T) {
			b, wire, rpc := messageTransportBridge(t, false)
			b.xmpp.acct.LastActivity = !tt.disabled
			b.xmpp.SetIdleSince(tt.since)
			b.xmpp.presence = "private status must not leak"
			// Not on the messaging allowlist: only the server's presence
			// subscription check authorizes this forwarded full-JID query.
			handleActivityIQ(t, b.xmpp, `<iq xmlns='jabber:client' type='`+tt.typ+`' from='contact@x/phone' to='pi@x/pi-msg' id='last-123'><query xmlns='jabber:iq:last'/></iq>`)
			if !b.xmpp.idleSince.Equal(tt.since) {
				t.Fatal("IQ query reset the idle clock")
			}
			if rpc.Len() != 0 {
				t.Fatalf("IQ query reached agent: %s", rpc.String())
			}
			if tt.wantType == "" {
				if wire.Len() != 0 {
					t.Fatalf("unexpected reply: %s", wire.String())
				}
				return
			}
			var resp struct {
				XMLName xml.Name
				ID      string `xml:"id,attr"`
				To      string `xml:"to,attr"`
				Type    string `xml:"type,attr"`
				Query   struct {
					Seconds *uint64 `xml:"seconds,attr"`
					Text    string  `xml:",chardata"`
				} `xml:"jabber:iq:last query"`
				Error *struct {
					Type      string   `xml:"type,attr"`
					Condition xml.Name `xml:",any"`
				} `xml:"error"`
			}
			if err := xml.Unmarshal(wire.Bytes(), &resp); err != nil {
				t.Fatalf("invalid XML: %v; %s", err, wire.String())
			}
			if resp.XMLName.Local != "iq" || resp.ID != "last-123" || resp.To != "contact@x/phone" || resp.Type != tt.wantType {
				t.Fatalf("incorrect IQ envelope: %s", wire.String())
			}
			if resp.Query.Text != "" || strings.Contains(wire.String(), b.xmpp.presence) {
				t.Fatalf("online result leaked status text: %s", wire.String())
			}
			if tt.wantErr != "" {
				wantErrType := "cancel"
				if tt.wantErr == "bad-request" {
					wantErrType = "modify"
				}
				if resp.Error == nil || resp.Error.Type != wantErrType || resp.Error.Condition != (xml.Name{Space: "urn:ietf:params:xml:ns:xmpp-stanzas", Local: tt.wantErr}) || resp.Query.Seconds != nil {
					t.Fatalf("wrong error or leaked seconds: %s", wire.String())
				}
			} else if resp.Error != nil || resp.Query.Seconds == nil || *resp.Query.Seconds < tt.min || *resp.Query.Seconds > tt.max {
				t.Fatalf("invalid seconds or unexpected error: %s", wire.String())
			}
		})
	}
}

func TestLastActivityIgnoresOtherPayloads(t *testing.T) {
	for _, payload := range []string{
		`<query xmlns='other:namespace'/>`,
		`<other xmlns='other:namespace'><query xmlns='jabber:iq:last'/></other>`,
	} {
		b, wire, _ := messageTransportBridge(t, false)
		b.xmpp.acct.LastActivity = true
		handleActivityIQ(t, b.xmpp, `<iq type='get' from='zach@x/phone' id='q'>`+payload+`</iq>`)
		if wire.Len() != 0 {
			t.Fatalf("answered non-request: %s", wire.String())
		}
	}
}

func TestLastActivityTracksIdleTransitions(t *testing.T) {
	b, wire, _ := messageTransportBridge(t, false)
	b.xmpp.acct.LastActivity = true
	b.markIdle()
	if since := b.xmpp.idleSince; since.IsZero() || time.Since(since) > time.Second {
		t.Fatalf("idle transition did not start the XEP-0012/0319 clock: %v", since)
	}
	b.xmpp.SetIdleSince(time.Now().Add(-time.Hour))
	b.markActive()
	handleActivityIQ(t, b.xmpp, `<iq type='get' from='zach@x/phone' id='active'><query xmlns='jabber:iq:last'/></iq>`)
	if !strings.Contains(wire.String(), `seconds="0"`) {
		t.Fatalf("active transition left stale idle time: %s", wire.String())
	}
}

func TestLastActivityDiscovery(t *testing.T) {
	b, wire, _ := messageTransportBridge(t, false)
	handleActivityIQ(t, b.xmpp, `<iq type='get' from='contact@x/phone' id='disco'><query xmlns='http://jabber.org/protocol/disco#info'/></iq>`)
	if !strings.Contains(wire.String(), `var="jabber:iq:last"`) {
		t.Fatalf("missing last activity feature: %s", wire.String())
	}
	if capsVer != capsVerFor(capsIdentity, discoFeatures) {
		t.Fatal("capabilities hash does not match advertised feature set")
	}
}

func TestLastActivityConfig(t *testing.T) {
	for _, tt := range []struct {
		name    string
		setting *bool
		want    bool
	}{
		{"default", nil, true},
		{"enabled", boolPtr(true), true},
		{"disabled", boolPtr(false), false},
	} {
		t.Run(tt.name, func(t *testing.T) {
			path := writeConfig(t, Config{Accounts: map[string]Account{
				"default": {JID: "pi@x", Password: "pw", Owner: "zach@x", LastActivity: tt.setting},
			}})
			cfg, err := loadConfig(path)
			if err != nil {
				t.Fatal(err)
			}
			acct, err := resolveAccount(cfg, "")
			if err != nil {
				t.Fatal(err)
			}
			if acct.LastActivity != tt.want {
				t.Fatalf("lastActivity = %v, want %v", acct.LastActivity, tt.want)
			}
		})
	}
}
