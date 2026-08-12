package main

import (
	"fmt"
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

		// Inline "trig:" anywhere — addressed, body kept intact (#21).
		{"here's the draft\n\npi: fold this in", true, "here's the draft\n\npi: fold this in"},
		// Known miss: a bracket between the name and the colon breaks the form.
		// Rare enough that widening it is not worth the false-positive surface.
		{"worth a PRAGMA first (pi): adjust your query", false, ""},

		// Inline "@trig" anywhere — addressed, body kept intact.
		{"@pi how do I pull the logs", true, "@pi how do I pull the logs"},
		{"over to @pi for the exact path", true, "over to @pi for the exact path"},

		// Inline "trig," is NOT addressing: it occurs constantly in prose.
		{"roster shows pi, alice and bob as active", false, ""},
		{"tagged to pi, so I'll let them answer", false, ""},

		// Word boundaries still hold away from position 0.
		{"the pilot: reported in", false, ""},
		{"deploy happy: done", false, ""},

		// Quoted/fenced content must not address anyone.
		{"saved the log:\n```\npi: do the thing\n```", false, ""},
		{"they said:\n> pi: do the thing", false, ""},
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
	got := b.composePrompt("hello", true, "", "zach@x.com", "", "", "")
	if !strings.HasPrefix(got, "from: zach@x.com\nhello") {
		t.Errorf("dm header wrong: %q", got)
	}
	if !strings.HasSuffix(got, hint) {
		t.Errorf("dm turn missing hint: %q", got)
	}

	// Room turn from the owner: from: is the room, sender: is the owner's jid.
	got = b.composePrompt("hi", true, "", "team@muc.x.com", "zach@x.com", "", "")
	if !strings.Contains(got, "from: team@muc.x.com\n") || !strings.Contains(got, "sender: zach@x.com\n") {
		t.Errorf("room header wrong: %q", got)
	}
	if !strings.HasSuffix(got, hint) {
		t.Errorf("room turn missing hint: %q", got)
	}

	// Commentary: wrapped as untrusted, includes nick + sender header.
	got = b.composePrompt("help", false, "alice", "team@muc.x.com", "alice@x.com", "", "")
	if !strings.Contains(got, "NON-OWNER") || !strings.Contains(got, "alice") ||
		!strings.Contains(got, "help") || !strings.Contains(got, "sender: alice@x.com") {
		t.Errorf("commentary framing wrong: %q", got)
	}

	// Ambient is prepended; the hint is still last.
	b.bufferAmbient("bob", "fyi")
	got = b.composePrompt("do it", true, "", "team@muc.x.com", "zach@x.com", "", "")
	if !strings.Contains(got, "room commentary") || !strings.Contains(got, "do it") || !strings.HasSuffix(got, hint) {
		t.Errorf("canonical+ambient wrong: %q", got)
	}
}

func TestRoutingHintNamesOwner(t *testing.T) {
	if h := roomBridge().routingHint(); !strings.Contains(h, "to:") || !strings.Contains(h, "zach@x.com") {
		t.Errorf("routingHint = %q, want it to mention to: and the owner jid", h)
	}
}

func TestRouteLineNoop(t *testing.T) {
	cases := []struct {
		in     string
		dest   string
		inline string
		ok     bool
	}{
		{"to: noop", "noop", "", true},
		{"to: NOOP", "noop", "", true},
		{"  to: noop", "noop", "", true},
		{"to: noop nothing to add", "noop", "nothing to add", true},
		{"to: zach@x.com", "zach@x.com", "", true},
		{"to: be fair, that's prose", "", "", false}, // no @ and not "noop"
		{"to: nooperator", "", "", false},            // must be exactly "noop"
	}
	for _, c := range cases {
		dest, inline, ok := routeLine(c.in)
		if ok != c.ok || dest != c.dest || inline != c.inline {
			t.Errorf("routeLine(%q) = (%q,%q,%v), want (%q,%q,%v)", c.in, dest, inline, ok, c.dest, c.inline, c.ok)
		}
	}
}

// A noop reply must parse as a real segment, not fall through to the reject
// path — otherwise an attempt at silence is dumped to the error room and the
// agent is nudged to resend, producing the very turn it tried to avoid (#20).
func TestNoopIsNotRejected(t *testing.T) {
	segs, leading := splitReplySegments("to: noop")
	if leading != "" {
		t.Errorf("leading = %q, want empty", leading)
	}
	if len(segs) != 1 {
		t.Fatalf("got %d segments, want 1", len(segs))
	}
	if segs[0].dest != "noop" {
		t.Errorf("dest = %q, want noop", segs[0].dest)
	}
}

func TestCascadeCap(t *testing.T) {
	b := roomBridge()
	for i := 0; i < cascadeCap; i++ {
		ok, announce := b.spendCascade()
		if !ok {
			t.Fatalf("turn %d denied, want allowed (cap is %d)", i+1, cascadeCap)
		}
		if announce {
			t.Errorf("turn %d announced, want silent while under cap", i+1)
		}
	}
	// First refusal announces; later ones stay quiet so one stall produces one
	// notice rather than a stream of them.
	ok, announce := b.spendCascade()
	if ok || !announce {
		t.Errorf("first refusal = (ok %v, announce %v), want (false, true)", ok, announce)
	}
	ok, announce = b.spendCascade()
	if ok || announce {
		t.Errorf("second refusal = (ok %v, announce %v), want (false, false)", ok, announce)
	}
	// An owner message puts a human back in the loop and restores the budget,
	// including the right to announce again on a later episode.
	b.resetCascade()
	if ok, _ := b.spendCascade(); !ok {
		t.Errorf("turn denied after reset, want allowed")
	}
	for i := 1; i < cascadeCap; i++ {
		b.spendCascade()
	}
	if ok, announce := b.spendCascade(); ok || !announce {
		t.Errorf("post-reset refusal = (ok %v, announce %v), want (false, true)", ok, announce)
	}
}

// The cascade notice must never contain an "@mention", or reporting a cascade
// would itself trigger another agent and extend it.
func TestCascadeNoticeDoesNotAddress(t *testing.T) {
	b := roomBridge()
	notice := fmt.Sprintf(
		"⚠️ %s is no longer answering agent handoffs — %d consecutive agent-to-agent turns with no message from the owner. Further handoffs are being kept as context but not acted on. A message from the owner resumes normal operation.",
		b.acct.Nick, cascadeCap)
	if strings.Contains(notice, "@") {
		t.Errorf("cascade notice contains an @mention: %q", notice)
	}
	if addressed, _ := b.matchTrigger(notice); addressed {
		t.Errorf("cascade notice addresses an agent: %q", notice)
	}
}
