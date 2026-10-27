package pimsg

import (
	"bytes"
	"context"
	"encoding/json"
	"encoding/xml"
	"io"
	"strings"
	"testing"

	"mellium.im/xmpp"
	"mellium.im/xmpp/jid"
)

// Capture real stanza encoding without opening a socket or contacting a server.
func messageTransportBridge(t *testing.T, arbitrary bool) (*Bridge, *bytes.Buffer, *bytes.Buffer) {
	t.Helper()
	b := newTestBridge(ResolvedAccount{
		JID: "pi@x", Owner: "zach@x", Rooms: []string{"team@muc.x"}, AllowArbitraryJid: arbitrary,
	})
	b.xmpp.occupants["team@muc.x"] = map[string]string{"alice": "alice@x"}
	wire := new(bytes.Buffer)
	session, err := xmpp.NewSession(context.Background(), jid.JID{}, jid.JID{}, wire, xmpp.Ready,
		xmpp.NewNegotiator(func(*xmpp.Session, *xmpp.StreamConfig) xmpp.StreamConfig {
			return xmpp.StreamConfig{}
		}))
	if err != nil {
		t.Fatal(err)
	}
	b.xmpp.session, b.xmpp.online = session, true
	rpc := new(bytes.Buffer)
	b.rpc = &RPCClient{stdin: &nopClose{buf: rpc}}
	return b, wire, rpc
}

func TestSendMessageDestinationTransport(t *testing.T) {
	for _, tt := range []struct {
		name, to, typ string
		arbitrary     bool
	}{
		{name: "owner", to: "zach@x", typ: "chat"},
		{name: "owner resource", to: "zach@x/phone", typ: "chat"},
		{name: "joined room", to: "team@muc.x", typ: "groupchat"},
		{name: "known occupant", to: "alice@x", typ: "chat"},
		{name: "opted in peer", to: "stranger@x", typ: "chat", arbitrary: true},
		{name: "opted in peer resource", to: "stranger@x/phone", typ: "chat", arbitrary: true},
		{name: "invalid JID with arbitrary access", to: "bad jid", arbitrary: true},
		{name: "unknown peer", to: "stranger@x"},
		{name: "unknown peer resource", to: "stranger@x/phone"},
		{name: "unconfigured room", to: "other@muc.x"},
		{name: "joined room resource", to: "team@muc.x/nick", typ: "groupchat"},
	} {
		t.Run(tt.name, func(t *testing.T) {
			b, wire, rpc := messageTransportBridge(t, tt.arbitrary)
			payload, err := json.Marshal(map[string]string{"action": "send_message", "to": tt.to, "text": "hello"})
			if err != nil {
				t.Fatal(err)
			}
			b.handleToolRelay("r1", string(payload))
			if tt.typ == "" {
				if wire.Len() != 0 {
					t.Fatalf("rejected destination received stanzas: %s", wire.String())
				}
				var response struct {
					Value string `json:"value"`
				}
				if err := json.Unmarshal(rpc.Bytes(), &response); err != nil {
					t.Fatal(err)
				}
				policy := "allowArbitraryJid=false: other destinations are not permitted."
				if tt.arbitrary {
					policy = "allowArbitraryJid=true: other syntactically valid peer JIDs are also permitted."
				}
				for _, hint := range []string{"not an allowed destination", `Allowed JIDs (configured owner, joined rooms, known room occupants): ["alice@x" "team@muc.x" "zach@x"].`, policy, "No message was sent."} {
					if !strings.Contains(response.Value, hint) {
						t.Errorf("rejection missing %q: %s", hint, response.Value)
					}
				}
				if b.replied() {
					t.Fatal("rejection counted as delivery")
				}
				return
			}
			if !strings.Contains(rpc.String(), `"value":"sent:`) || !b.replied() {
				t.Fatalf("allowed destination did not succeed: %s", rpc.String())
			}
			decoder := xml.NewDecoder(strings.NewReader(wire.String()))
			bodies := 0
			for {
				var msg struct {
					XMLName xml.Name
					To      string `xml:"to,attr"`
					Type    string `xml:"type,attr"`
					ID      string `xml:"id,attr"`
					Body    string `xml:"body"`
				}
				if err := decoder.Decode(&msg); err == io.EOF {
					break
				} else if err != nil {
					t.Fatalf("invalid outgoing XML: %v; %s", err, wire.String())
				}
				wantTo := tt.to
				if tt.typ == "groupchat" {
					wantTo = bareJid(wantTo)
				}
				if msg.XMLName.Local != "message" || msg.To != wantTo || msg.Type != tt.typ {
					t.Fatalf("unexpected outgoing stanza: %+v", msg)
				}
				if msg.Body != "" {
					bodies++
					if msg.Body != "hello" || msg.ID == "" || !strings.Contains(rpc.String(), "sent:"+msg.ID) {
						t.Fatalf("body or returned stanza ID mismatch: %+v; %s", msg, rpc.String())
					}
				}
			}
			if bodies != 1 {
				t.Fatalf("sent %d bodies, want 1; %s", bodies, wire.String())
			}
		})
	}
}

func TestBlockedSendMessageStreamDoesNotSendTyping(t *testing.T) {
	b, wire, rpc := messageTransportBridge(t, false)
	partial := map[string]any{"content": []any{map[string]any{
		"type": "toolCall", "id": "call-1", "name": "send_message", "arguments": map[string]any{},
	}}}
	b.handleStreamDelta(streamDelta(map[string]any{
		"type": "toolcall_start", "contentIndex": float64(0), "partial": partial,
	}))
	b.handleStreamDelta(streamDelta(map[string]any{
		"type": "toolcall_delta", "contentIndex": float64(0),
		"delta": `{"to":"stranger@x","text":"hello"}`, "partial": partial,
	}))
	if b.streamToolCalls[0].started || wire.Len() != 0 {
		t.Fatalf("blocked streamed destination triggered typing: %s", wire.String())
	}
	// Authorization must also precede reply lookup and any delivery side effects.
	b.handleToolRelay("r1", `{"action":"send_message","to":"stranger@x","text":"hello","replyTo":"missing-stanza"}`)
	if wire.Len() != 0 || b.replied() || !strings.Contains(rpc.String(), "not an allowed destination") {
		t.Fatalf("blocked relay attempted delivery or reply lookup: wire=%s rpc=%s", wire.String(), rpc.String())
	}
}
