package main

import (
	"strings"
	"testing"
)

func roomBridge() *Bridge {
	return NewBridge(ResolvedAccount{
		Owner:       "zach@x.com",
		Rooms:       []string{"team@muc.x.com"},
		Nick:        "pi",
		RoomTrigger: "pi",
	}, false)
}

func TestMatchTrigger(t *testing.T) {
	b := roomBridge()
	cases := []struct {
		in        string
		addressed bool
		stripped  string
	}{
		{"pi: do the thing", true, "do the thing"},
		{"pi, do the thing", true, "do the thing"},
		{"PI: caps", true, "caps"},
		{"pilot the ship", false, ""}, // no colon/comma → not addressing
		{"hey pi can you", false, ""}, // trigger not at start
		{"pi", false, ""},             // bare trigger, nothing after
		{"  pi: leading space", true, "leading space"},
	}
	for _, c := range cases {
		addressed, stripped := b.matchTrigger(c.in)
		if addressed != c.addressed || (addressed && stripped != c.stripped) {
			t.Errorf("matchTrigger(%q) = (%v,%q), want (%v,%q)", c.in, addressed, stripped, c.addressed, c.stripped)
		}
	}
}

func TestClassify(t *testing.T) {
	b := roomBridge()
	cases := []struct {
		m      InboundMessage
		action roomAction
		body   string
	}{
		{InboundMessage{Body: "just chatting", Nick: "alice", FromOwner: false}, actionAmbient, "just chatting"},
		{InboundMessage{Body: "pi: help alice", Nick: "alice", FromOwner: false}, actionCommentary, "help alice"},
		{InboundMessage{Body: "do it", Nick: "zach", FromOwner: true}, actionCanonical, "do it"},
		{InboundMessage{Body: "pi: do it", Nick: "zach", FromOwner: true}, actionCanonical, "do it"},
	}
	for _, c := range cases {
		action, body := b.classify(c.m)
		if action != c.action || body != c.body {
			t.Errorf("classify(%+v) = (%d,%q), want (%d,%q)", c.m, action, body, c.action, c.body)
		}
	}
}

func TestAmbientBufferAndDrain(t *testing.T) {
	b := roomBridge()
	if got := b.drainAmbient(); got != "" {
		t.Errorf("empty drain = %q, want empty", got)
	}
	b.bufferAmbient("alice", "the parser is flaky")
	b.bufferAmbient("bob", "+1")
	b.bufferAmbient("carol", "   ") // whitespace-only, ignored

	block := b.drainAmbient()
	if !strings.Contains(block, "non-canonical") {
		t.Errorf("block missing non-canonical label: %q", block)
	}
	if !strings.Contains(block, "alice: the parser is flaky") || !strings.Contains(block, "bob: +1") {
		t.Errorf("block missing buffered messages: %q", block)
	}
	if strings.Contains(block, "carol") {
		t.Errorf("whitespace-only message should have been ignored: %q", block)
	}
	// Drain clears the buffer.
	if got := b.drainAmbient(); got != "" {
		t.Errorf("second drain = %q, want empty (buffer should be cleared)", got)
	}
}

func TestAmbientCap(t *testing.T) {
	b := roomBridge()
	for i := 0; i < ambientCap+20; i++ {
		b.bufferAmbient("n", "m")
	}
	b.ambientMu.Lock()
	n := len(b.ambient)
	b.ambientMu.Unlock()
	if n != ambientCap {
		t.Errorf("ambient buffer len = %d, want capped at %d", n, ambientCap)
	}
}

func TestComposePrompt(t *testing.T) {
	b := roomBridge() // owner zach@x.com, room team@muc.x.com
	hint := b.routingHint()

	// Owner DM turn: "from:" is the owner, body follows directly (no sender
	// line), hint appended.
	got := b.composePrompt("hello", true, "", "zach@x.com", "")
	if !strings.HasPrefix(got, "from: zach@x.com\nhello") {
		t.Errorf("dm header wrong: %q", got)
	}
	if !strings.HasSuffix(got, hint) {
		t.Errorf("dm turn missing hint: %q", got)
	}

	// Room turn from the owner: from: is the room, sender: is the owner's jid.
	got = b.composePrompt("hi", true, "", "team@muc.x.com", "zach@x.com")
	if !strings.Contains(got, "from: team@muc.x.com\n") || !strings.Contains(got, "sender: zach@x.com\n") {
		t.Errorf("room header wrong: %q", got)
	}
	if !strings.HasSuffix(got, hint) {
		t.Errorf("room turn missing hint: %q", got)
	}

	// Commentary: wrapped as untrusted, includes nick + sender header.
	got = b.composePrompt("help", false, "alice", "team@muc.x.com", "alice@x.com")
	if !strings.Contains(got, "NON-OWNER") || !strings.Contains(got, "alice") ||
		!strings.Contains(got, "help") || !strings.Contains(got, "sender: alice@x.com") {
		t.Errorf("commentary framing wrong: %q", got)
	}

	// Ambient is prepended; the hint is still last.
	b.bufferAmbient("bob", "fyi")
	got = b.composePrompt("do it", true, "", "team@muc.x.com", "zach@x.com")
	if !strings.Contains(got, "room commentary") || !strings.Contains(got, "do it") || !strings.HasSuffix(got, hint) {
		t.Errorf("canonical+ambient wrong: %q", got)
	}
}

func TestRoutingHintNamesOwner(t *testing.T) {
	if h := roomBridge().routingHint(); !strings.Contains(h, "to:") || !strings.Contains(h, "zach@x.com") {
		t.Errorf("routingHint = %q, want it to mention to: and the owner jid", h)
	}
}
