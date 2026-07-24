package main

import (
	"encoding/xml"
	"strings"
	"testing"
)

func TestBareJid(t *testing.T) {
	cases := map[string]string{
		"zach@x.com/phone":    "zach@x.com",
		"zach@x.com":          "zach@x.com",
		"Zach@X.com/Res":      "zach@x.com",
		"room@muc.x.com/nick": "room@muc.x.com",
	}
	for in, want := range cases {
		if got := bareJid(in); got != want {
			t.Errorf("bareJid(%q) = %q, want %q", in, got, want)
		}
	}
}

func TestResourcepart(t *testing.T) {
	if got := resourcepart("room@muc.x.com/alice"); got != "alice" {
		t.Errorf("resourcepart = %q, want alice", got)
	}
	if got := resourcepart("zach@x.com"); got != "" {
		t.Errorf("resourcepart with no resource = %q, want empty", got)
	}
}

func TestChunkShort(t *testing.T) {
	if got := chunk("hello", maxBody); len(got) != 1 || got[0] != "hello" {
		t.Errorf("chunk short = %v, want [hello]", got)
	}
}

func TestChunkSplitsAndPreserves(t *testing.T) {
	// Build text well over the cap with spaces so it splits on word bounds.
	long := strings.Repeat("word ", 2000) // ~10000 bytes
	parts := chunk(long, maxBody)
	if len(parts) < 2 {
		t.Fatalf("expected multiple chunks, got %d", len(parts))
	}
	for i, p := range parts {
		if len(p) > maxBody {
			t.Errorf("chunk %d length %d exceeds cap %d", i, len(p), maxBody)
		}
	}
	// Rejoining with spaces should reconstruct the (space-trimmed) content.
	rejoined := strings.Join(parts, " ")
	if strings.Fields(rejoined)[0] != "word" {
		t.Errorf("content not preserved across chunks")
	}
}

func TestSeenDuplicate(t *testing.T) {
	b := NewXMPPBridge(ResolvedAccount{Owner: "o@x.com"}, func(InboundMessage) {}, nil)
	if b.seenDuplicate("a") {
		t.Error("first sighting of 'a' reported as duplicate")
	}
	if !b.seenDuplicate("a") {
		t.Error("second sighting of 'a' not reported as duplicate")
	}
	if b.seenDuplicate("b") {
		t.Error("first sighting of 'b' reported as duplicate")
	}
}

func TestTokenHelpers(t *testing.T) {
	// <body>hi</body> plus a delay element.
	toks := []xml.Token{
		xml.StartElement{Name: xml.Name{Local: "body"}},
		xml.CharData("hi there"),
		xml.EndElement{Name: xml.Name{Local: "body"}},
		xml.StartElement{Name: xml.Name{Space: "urn:xmpp:delay", Local: "delay"}},
		xml.EndElement{Name: xml.Name{Space: "urn:xmpp:delay", Local: "delay"}},
	}
	if got := childText(toks, "body"); got != "hi there" {
		t.Errorf("childText body = %q, want 'hi there'", got)
	}
	if _, ok := element(toks, "urn:xmpp:delay", "delay"); !ok {
		t.Error("delay element not found")
	}
	if _, ok := element(toks, "urn:xmpp:delay", "nope"); ok {
		t.Error("found nonexistent element")
	}
}

func TestReceiptAckMarshal(t *testing.T) {
	// The ack child shared by XEP-0184 receipts and XEP-0333 markers must emit
	// its namespace and the referenced message id.
	cases := []struct{ ns, local, id string }{
		{receiptsNS, "received", "msg-1"},
		{chatMarkersNS, "displayed", "msg-2"},
	}
	for _, c := range cases {
		ack := struct {
			XMLName xml.Name
			ID      string `xml:"id,attr"`
		}{XMLName: xml.Name{Space: c.ns, Local: c.local}, ID: c.id}
		out, err := xml.Marshal(ack)
		if err != nil {
			t.Fatalf("marshal %s: %v", c.local, err)
		}
		got := string(out)
		for _, want := range []string{"<" + c.local, `xmlns="` + c.ns + `"`, `id="` + c.id + `"`} {
			if !strings.Contains(got, want) {
				t.Errorf("%s ack = %q, missing %q", c.local, got, want)
			}
		}
	}
}
