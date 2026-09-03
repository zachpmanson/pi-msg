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
	b := roomBridge()      // owner zach@x.com, room team@muc.x.com
	b.routingSeeded = true // this test exercises the non-seeding path

	// Owner DM turn: "from:" is the owner, body follows directly (no sender
	// line). No routing hint is appended (removed per issue #33).
	got := b.composePrompt("hello", true, "", "zach@x.com", "", "", "")
	if !strings.HasPrefix(got, "from: zach@x.com\nhello") {
		t.Errorf("dm header wrong: %q", got)
	}
	if strings.Contains(got, "to: ") {
		t.Errorf("dm turn should not contain a routing hint: %q", got)
	}

	// Room turn from the owner: from: is the room, sender: is the owner's jid.
	got = b.composePrompt("hi", true, "", "team@muc.x.com", "zach@x.com", "", "")
	if !strings.Contains(got, "from: team@muc.x.com\n") || !strings.Contains(got, "sender: zach@x.com\n") {
		t.Errorf("room header wrong: %q", got)
	}

	// Commentary: wrapped as untrusted, includes nick + sender header.
	got = b.composePrompt("help", false, "alice", "team@muc.x.com", "alice@x.com", "", "")
	if !strings.Contains(got, "NON-OWNER") || !strings.Contains(got, "alice") ||
		!strings.Contains(got, "help") || !strings.Contains(got, "sender: alice@x.com") {
		t.Errorf("commentary framing wrong: %q", got)
	}

	// Ambient is prepended.
	b.bufferAmbient("bob", "fyi")
	got = b.composePrompt("do it", true, "", "team@muc.x.com", "zach@x.com", "", "")
	if !strings.Contains(got, "room commentary") || !strings.Contains(got, "do it") {
		t.Errorf("canonical+ambient wrong: %q", got)
	}
}

func TestRoutingSeedOnce(t *testing.T) {
	b := roomBridge() // room-mode account
	// First prompt seeds the contract (once); room-mode only.
	got1 := b.composePrompt("go", true, "", "team@muc.x.com", "zach@x.com", "", "")
	if !strings.Contains(got1, "[pi-msg: routing:") {
		t.Errorf("first prompt should seed the routing contract: %q", got1)
	}
	// Subsequent prompts must NOT re-seed.
	got2 := b.composePrompt("again", true, "", "team@muc.x.com", "zach@x.com", "", "")
	if strings.Contains(got2, "[pi-msg: routing:") {
		t.Errorf("second prompt re-seeded the contract: %q", got2)
	}

	// A non-room (1:1) account never seeds.
	b1 := NewBridge(ResolvedAccount{Owner: "zach@x.com", Nick: "pi"}, false)
	got := b1.composePrompt("hi", true, "", "zach@x.com", "", "", "")
	if strings.Contains(got, "[pi-msg: routing:") {
		t.Errorf("1:1 account should not seed the routing contract: %q", got)
	}
}

// TestInitialPromptCompose verifies the invocation-time initial prompt path
// (pi-msg#35): the task text is composed through the normal prompt path, so a
// fresh room-mode on-demand spawn receives the routing contract seed followed
// by the task as a canonical owner message (its reply therefore routes `to:`
// the owner per the contract), while a 1:1 account gets the task verbatim.
func TestInitialPromptCompose(t *testing.T) {
	task := "resolve zachpmanson/pi-msg#35 and open a PR"

	// Fresh room-mode spawn: routingSeeded is false (an initial prompt forces a
	// fresh session), so the first prompt seeds the routing contract once.
	b := roomBridge()
	got := b.composePrompt(task, true, "", b.acct.Owner, "", "", "")
	if !strings.Contains(got, "[pi-msg: routing:") {
		t.Errorf("fresh room-mode initial prompt should seed the routing contract: %q", got)
	}
	if !strings.Contains(got, "from: zach@x.com") {
		t.Errorf("initial prompt should carry the from: owner header: %q", got)
	}
	if !strings.HasSuffix(got, task) {
		t.Errorf("initial prompt should end with the task text: %q", got)
	}

	// 1:1 account: the task is delivered verbatim, no routing contract.
	b1 := NewBridge(ResolvedAccount{Owner: "zach@x.com", Nick: "pi"}, false)
	if got := b1.composePrompt(task, true, "", "zach@x.com", "", "", ""); got != task {
		t.Errorf("1:1 initial prompt = %q, want plain %q", got, task)
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
		dest, replyTo, inline, ok := routeLine(c.in)
		if ok != c.ok || dest != c.dest || inline != c.inline {
			t.Errorf("routeLine(%q) = (%q,%q,%v), want (%q,%q,%v)", c.in, dest, inline, ok, c.dest, c.inline, c.ok)
		}
		if replyTo != "" {
			t.Errorf("routeLine(%q) set replyTo = %q, want empty", c.in, replyTo)
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

// A mistyped mention is inert -- it addresses nobody and reports nothing -- so
// the bridge has to spot it. Live evidence: "@zbeltino" was written 8 times
// against "@beltino" 5, i.e. most attempts to address that agent went nowhere.
func TestUnknownHandles(t *testing.T) {
	b := roomBridge()
	x := NewXMPPBridge(b.acct, func(InboundMessage) {}, func(_, _ string) {})
	x.occupants["team@muc.x.com"] = map[string]string{
		"beltino": "beltino@x.com",
		"peppy":   "peppy@x.com",
	}
	b.xmpp = x
	const room = "team@muc.x.com"

	cases := []struct {
		body string
		want []string
	}{
		{"@zbeltino picking Philippines", []string{"zbeltino"}},
		{"@beltino ok, yours", nil},
		{"@peppy and @zbeltino, sort it out", []string{"zbeltino"}},
		{"@zbeltino @zbeltino @zbeltino", []string{"zbeltino"}}, // deduped
		{"thanks @zach", nil},                                   // owner localpart
		{"mail me at bob@example.com", nil},                     // domain, not a mention
		{"ping beltino@x.com directly", nil},                    // bare JID
		{"```\n@nobody: do it\n```", nil},                       // fenced
		{"> @nobody said so", nil},                              // quoted
		{"no mentions at all", nil},
	}
	for _, c := range cases {
		got, valid := b.unknownHandles(room, c.body)
		if len(got) != len(c.want) {
			t.Errorf("unknownHandles(%q) = %v, want %v", c.body, got, c.want)
			continue
		}
		for i := range got {
			if got[i] != c.want[i] {
				t.Errorf("unknownHandles(%q) = %v, want %v", c.body, got, c.want)
			}
		}
		if len(got) > 0 && len(valid) == 0 {
			t.Errorf("unknownHandles(%q) reported unknowns with no valid list to suggest", c.body)
		}
	}

	// With no occupant roster we cannot tell a typo from a valid absent user, so
	// nothing is reported -- a wrong warning is worse than none.
	b2 := roomBridge()
	b2.xmpp = NewXMPPBridge(b2.acct, func(InboundMessage) {}, func(_, _ string) {})
	if got, _ := b2.unknownHandles(room, "@zbeltino hello"); got != nil {
		t.Errorf("empty roster produced warnings: %v", got)
	}
}

// Tagging yourself is inert: the bridge drops our own room echo before
// dispatch, so "@pi" written by pi notifies nobody. Observed live: slippy wrote
// "@slippy — good, Japan confirmed for you" twice where it meant another agent,
// so that agent never heard about the work handed to it. The unknown-handle
// check can't catch this, since our own nick IS a valid occupant handle.
func TestSelfTagHandle(t *testing.T) {
	b := roomBridge() // nick "pi", owner zach@x.com
	x := NewXMPPBridge(b.acct, func(InboundMessage) {}, func(_, _ string) {})
	x.occupants["team@muc.x.com"] = map[string]string{
		"pi":      "pi@x.com",
		"beltino": "beltino@x.com",
		"peppy":   "peppy@x.com",
	}
	b.xmpp = x
	const room = "team@muc.x.com"

	cases := []struct {
		body        string
		wantUnknown []string
		wantSelf    string
	}{
		{"@pi — good, Japan confirmed for you", nil, "pi"},
		{"@PI case-insensitive", nil, "PI"},
		{"@beltino ok, yours", nil, ""},
		{"@pi and @zbeltino both", []string{"zbeltino"}, "pi"},
		{"no mentions at all", nil, ""},
		{"```\n@pi do it\n```", nil, ""}, // fenced, not a real mention
	}
	for _, c := range cases {
		unknown, self, valid := b.handleIssues(room, c.body)
		if self != c.wantSelf {
			t.Errorf("handleIssues(%q) selfTag = %q, want %q", c.body, self, c.wantSelf)
		}
		if len(unknown) != len(c.wantUnknown) {
			t.Errorf("handleIssues(%q) unknown = %v, want %v", c.body, unknown, c.wantUnknown)
			continue
		}
		for i := range unknown {
			if unknown[i] != c.wantUnknown[i] {
				t.Errorf("handleIssues(%q) unknown = %v, want %v", c.body, unknown, c.wantUnknown)
			}
		}
		// Suggesting our own handle back to ourselves would re-teach the bug.
		for _, v := range valid {
			if strings.EqualFold(v, "pi") {
				t.Errorf("handleIssues(%q) offered our own handle %q as addressable", c.body, v)
			}
		}
		if len(valid) != 2 {
			t.Errorf("handleIssues(%q) valid = %v, want the two peers", c.body, valid)
		}
	}

	// A self-tag must never be reported as an unknown handle: it is a real
	// occupant handle, just an inert one to use on yourself.
	if got, _ := b.unknownHandles(room, "@pi hello"); got != nil {
		t.Errorf("self-tag reported as unknown handle: %v", got)
	}

	// No roster → no information, so neither problem is reported.
	b2 := roomBridge()
	b2.xmpp = NewXMPPBridge(b2.acct, func(InboundMessage) {}, func(_, _ string) {})
	unknown, self, valid := b2.handleIssues(room, "@pi and @zbeltino hello")
	if unknown != nil || self != "" || valid != nil {
		t.Errorf("empty roster produced %v / %q / %v, want nothing at all", unknown, self, valid)
	}
}

// The warning fires at most once per run, so a stubbornly-misspelling agent
// can't be nudged in a loop.
func TestHandleWarnOncePerRun(t *testing.T) {
	b := roomBridge()
	if b.handleWarned() {
		t.Fatal("fresh run already marked warned")
	}
	b.setHandleWarned(true)
	if !b.handleWarned() {
		t.Error("warned flag did not stick")
	}
	b.setHandleWarned(false)
	if b.handleWarned() {
		t.Error("warned flag did not clear at run start")
	}
}

// "@everyone" must wake the room. Agents reach for it unprompted, and without
// support the attempt is inert: a fleet leader opened an election with "Here's
// the structure I'll run, @everyone:", woke nobody, and the room sat silent for
// six minutes.
func TestBroadcastHandles(t *testing.T) {
	b := roomBridge() // trigger "pi"
	cases := []struct {
		in   string
		want bool
	}{
		{"@everyone stage 1 is open", true},
		{"here's the structure I'll run, @everyone:", true},
		{"@all please report", true},
		{"@here quick sync", true},
		{"@EVERYONE caps still counts", true},
		{"everyone should report in", false},  // no sigil — ordinary prose
		{"that's all from me", false},         // ditto
		{"we're all here", false},             // ditto
		{"@everyones opinion differs", false}, // handle must end at the word
		{"@allocate the budget", false},       // ditto
		{"```\n@everyone in a fence\n```", false},
		{"> @everyone in a quote", false},
	}
	for _, c := range cases {
		if got, _ := b.matchTrigger(c.in); got != c.want {
			t.Errorf("matchTrigger(%q) = %v, want %v", c.in, got, c.want)
		}
	}
}

// A broadcast handle is a real address, not a typo, so it must not be reported
// as unknown. And a self-mention only counts as a mis-addressed handoff when it
// is the FIRST mention: later ones are enumerations ("Tally board: @peppy ✅ ·
// @beltino ✅"), which are correct writing and must not burn a warning turn.
func TestHandleIssuesBroadcastAndEnumeration(t *testing.T) {
	b := roomBridge()
	x := NewXMPPBridge(b.acct, func(InboundMessage) {}, func(_, _ string) {})
	x.occupants["team@muc.x.com"] = map[string]string{
		"pi": "pi@x.com", "peppy": "peppy@x.com", "slippy": "slippy@x.com",
	}
	b.xmpp = x
	const room = "team@muc.x.com"

	if unknown, self, _ := b.handleIssues(room, "@everyone stage 1 is open"); len(unknown) != 0 || self != "" {
		t.Errorf("broadcast flagged: unknown=%v self=%q", unknown, self)
	}
	// Self first → a real mis-address.
	if _, self, _ := b.handleIssues(room, "@pi — good, Japan confirmed for you"); self != "pi" {
		t.Errorf("leading self-tag not caught: %q", self)
	}
	// Self later → an enumeration, not a handoff.
	if _, self, _ := b.handleIssues(room, "Tally board: @peppy ✅ · @slippy ✅ · @pi ✅ (officer)"); self != "" {
		t.Errorf("enumerated self-mention warned: %q", self)
	}
	// A genuine unknown handle is still caught alongside an enumeration.
	if unknown, self, _ := b.handleIssues(room, "@peppy and @zbeltino, sort it out"); len(unknown) != 1 || unknown[0] != "zbeltino" || self != "" {
		t.Errorf("unknown=%v self=%q, want [zbeltino] and no self-tag", unknown, self)
	}
}
