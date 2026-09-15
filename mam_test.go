package main

import (
	"encoding/xml"
	"strings"
	"testing"
	"time"

	"mellium.im/xmlstream"
)

// mamTokens parses an XML fragment into a token slice the way handle() sees a
// stanza body: ReadAll after the outer start element has been consumed.
func mamTokens(t *testing.T, s string) []xml.Token {
	t.Helper()
	toks, err := xmlstream.ReadAll(xml.NewDecoder(strings.NewReader(s)))
	if err != nil {
		t.Fatalf("parsing %q: %v", s, err)
	}
	return toks
}

func newMAMTestBridge() *XMPPBridge {
	return &XMPPBridge{
		acct:       ResolvedAccount{JID: "slippy@chat.zachmanson.com", Owner: "zach@chat.zachmanson.com", Nick: "slippy"},
		ownerBare:  "zach@chat.zachmanson.com",
		msgHistory: make(map[string]msgHistoryEntry),
		mamPending: make(map[string]*mamCollector),
	}
}

// An archived owner 1:1 message lands in the collector with its archive stamp,
// is recorded for reaction targeting, and is tagged direct/canonical.
func TestCollectMAMResultDirect(t *testing.T) {
	b := newMAMTestBridge()
	col := &mamCollector{}
	b.mamPending["q1"] = col

	toks := mamTokens(t, `<result xmlns='urn:xmpp:mam:2' queryid='q1' id='a1'>`+
		`<forwarded xmlns='urn:xmpp:forward:0'><delay xmlns='urn:xmpp:delay' stamp='2026-09-15T01:02:03Z'/>`+
		`<message xmlns='jabber:client' from='zach@chat.zachmanson.com/phone' id='m1' type='chat'>`+
		`<body>hello there</body></message></forwarded></result>`)
	res, ok := element(toks, mamNS, "result")
	if !ok {
		t.Fatal("result element not detected")
	}
	b.collectMAMResult(toks, res)

	if len(col.out) != 1 {
		t.Fatalf("collected %d messages, want 1", len(col.out))
	}
	m := col.out[0]
	if m.Body != "hello there" || m.ID != "m1" {
		t.Errorf("message = %+v, want body/id from the archived stanza", m)
	}
	if !m.Direct || !m.FromOwner || m.Room != "" {
		t.Errorf("direct scope: got Direct=%v FromOwner=%v Room=%q", m.Direct, m.FromOwner, m.Room)
	}
	want := time.Date(2026, 9, 15, 1, 2, 3, 0, time.UTC)
	if !m.Stamp.Equal(want) {
		t.Errorf("stamp = %v, want %v", m.Stamp, want)
	}
	if got := b.lookupMessage("m1"); got != "zach@chat.zachmanson.com/phone" {
		t.Errorf("lookupMessage = %q, want the archived full JID", got)
	}
}

// A room-scoped archive result becomes a room message tagged with the room and
// the occupant nick; our own archived outbound is skipped.
func TestCollectMAMResultRoom(t *testing.T) {
	b := newMAMTestBridge()
	col := &mamCollector{room: "testing@muc.chat.zachmanson.com"}
	b.mamPending["q2"] = col

	for _, from := range []string{
		"testing@muc.chat.zachmanson.com/slippy",
		"testing@muc.chat.zachmanson.com/peppy",
	} {
		toks := mamTokens(t, `<result xmlns='urn:xmpp:mam:2' queryid='q2'>`+
			`<forwarded xmlns='urn:xmpp:forward:0'><delay xmlns='urn:xmpp:delay' stamp='2026-09-15T01:00:00Z'/>`+
			`<message xmlns='jabber:client' from='`+from+`' id='r-`+from+`' type='groupchat'>`+
			`<body>tick</body></message></forwarded></result>`)
		res, _ := element(toks, mamNS, "result")
		b.collectMAMResult(toks, res)
	}

	if len(col.out) != 1 {
		t.Fatalf("collected %d messages, want 1 (own outbound skipped)", len(col.out))
	}
	m := col.out[0]
	if m.Direct || m.Room != "testing@muc.chat.zachmanson.com" {
		t.Errorf("room scope: got Direct=%v Room=%q", m.Direct, m.Room)
	}
	if m.Nick != "peppy" {
		t.Errorf("nick = %q, want peppy", m.Nick)
	}
}

// A result for an unknown query id must be dropped, never dispatched as live
// input; an empty-body (chat-state) archive entry is dropped too.
func TestCollectMAMResultUnknownAndEmpty(t *testing.T) {
	b := newMAMTestBridge()
	b.mamPending["q1"] = &mamCollector{}

	unknown := mamTokens(t, `<result xmlns='urn:xmpp:mam:2' queryid='nope'>`+
		`<forwarded xmlns='urn:xmpp:forward:0'><message xmlns='jabber:client' from='zach@chat.zachmanson.com' id='x'><body>stray</body></message></forwarded></result>`)
	res, _ := element(unknown, mamNS, "result")
	b.collectMAMResult(unknown, res) // must not panic, must not collect

	empty := mamTokens(t, `<result xmlns='urn:xmpp:mam:2' queryid='q1'>`+
		`<forwarded xmlns='urn:xmpp:forward:0'><message xmlns='jabber:client' from='zach@chat.zachmanson.com' id='e'><body/></message></forwarded></result>`)
	res, _ = element(empty, mamNS, "result")
	b.collectMAMResult(empty, res)

	if n := len(b.mamPending["q1"].out); n != 0 {
		t.Fatalf("collected %d, want 0 (unknown id + empty body dropped)", n)
	}
}

// The MAM query payload must carry the query id, the FILTER form, the `start`
// time bound, the `with` filter and the RSM page cap. A missing `start` makes
// the server return the whole archive instead of the offline window.
func TestMAMQueryPayloadMarshal(t *testing.T) {
	since := time.Date(2026, 9, 15, 8, 15, 58, 0, time.UTC)
	p := newMAMQueryPayload("qid-1", "zach@chat.zachmanson.com", since, mamPageMax)

	raw, err := xml.Marshal(p)
	if err != nil {
		t.Fatalf("marshal: %v", err)
	}
	got := string(raw)
	for _, want := range []string{
		`<query xmlns="urn:xmpp:mam:2" queryid="qid-1">`,
		`<x xmlns="jabber:x:data" type="submit">`,
		`<field var="FORM_TYPE"><value>urn:xmpp:mam:2</value></field>`,
		`<field var="start"><value>2026-09-15T08:15:58Z</value></field>`,
		`<field var="with"><value>zach@chat.zachmanson.com</value></field>`,
		`<set xmlns="http://jabber.org/protocol/rsm"><max>200</max></set>`,
	} {
		if !strings.Contains(got, want) {
			t.Errorf("marshalled payload missing %q:\n%s", want, got)
		}
	}

	// A room query omits `with` (the room archive is addressed by JID) but still
	// carries the time bound.
	room := string(mustMarshal(t, newMAMQueryPayload("qid-2", "", since, 0)))
	if strings.Contains(room, `var="with"`) {
		t.Errorf("room payload should not carry a with filter:\n%s", room)
	}
	if !strings.Contains(room, `<field var="start">`) {
		t.Errorf("room payload missing start bound:\n%s", room)
	}
	if strings.Contains(room, `protocol/rsm`) {
		t.Errorf("max=0 should omit RSM:\n%s", room)
	}
}

func mustMarshal(t *testing.T, v any) []byte {
	t.Helper()
	raw, err := xml.Marshal(v)
	if err != nil {
		t.Fatalf("marshal: %v", err)
	}
	return raw
}

// Duplicate stanza ids must not enter the replay buffer twice: the same message
// can arrive as a server-pushed delayed stanza and again from MAM.
func TestReplayBufferDedupesStanzaID(t *testing.T) {
	b := &XMPPBridge{}
	b.bufferReplay(InboundMessage{Body: "once", ID: "dup"})
	b.bufferReplay(InboundMessage{Body: "twice", ID: "dup"})
	b.bufferReplay(InboundMessage{Body: "other", ID: "uniq"})

	if len(b.replayBuf) != 2 {
		t.Fatalf("buffer = %d entries, want 2", len(b.replayBuf))
	}
	if b.replayBuf[0].Body != "once" {
		t.Errorf("dedupe kept the wrong copy: %+v", b.replayBuf[0])
	}
}

// Delay-pushed and MAM-fetched entries interleave in arrival order; the drain
// sorts them chronologically so the catch-up reads as one timeline.
func TestReplayDrainOrdersByStamp(t *testing.T) {
	b := &XMPPBridge{}
	b.replayActive = true
	b.replayGraceEnd = time.Now()
	base := time.Date(2026, 9, 15, 0, 0, 0, 0, time.UTC)
	b.bufferReplay(InboundMessage{Body: "third", ID: "c", Stamp: base.Add(3 * time.Minute)})
	b.bufferReplay(InboundMessage{Body: "first", ID: "a", Stamp: base.Add(time.Minute)})
	b.bufferReplay(InboundMessage{Body: "second", ID: "b", Stamp: base.Add(2 * time.Minute)})

	got := b.DrainReplay(t.Context())
	if len(got) != 3 {
		t.Fatalf("drained %d, want 3", len(got))
	}
	for i, want := range []string{"first", "second", "third"} {
		if got[i].Body != want {
			t.Errorf("drain[%d] = %q, want %q", i, got[i].Body, want)
		}
	}
}
