package main

import (
	"bytes"
	"path/filepath"
	"strings"
	"sync"
	"testing"
	"time"
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
		{"pilot the ship", false, ""},              // "pi" inside a word is not a mention
		{"hey pi can you", true, "hey pi can you"}, // bare mention (#106)
		{"pi", true, "pi"},                         // a lone trigger is still a mention
		{"  pi: leading space", true, "leading space"},

		// Inline "trig:" anywhere — addressed, body kept intact (#21).
		{"here's the draft\n\npi: fold this in", true, "here's the draft\n\npi: fold this in"},
		// The bracket form used to be a known miss; the bare mention (#106) now
		// catches it, since "pi" appears as a standalone word.
		{"worth a PRAGMA first (pi): adjust your query", true, "worth a PRAGMA first (pi): adjust your query"},

		// Inline "@trig" anywhere — addressed, body kept intact.
		{"@pi how do I pull the logs", true, "@pi how do I pull the logs"},
		{"over to @pi for the exact path", true, "over to @pi for the exact path"},

		// Bare mentions, the form added in #106. With no ambient buffer a missed
		// address means the message does not exist for the agent, so prose that
		// names it is deliberately accepted as addressing it.
		{"ask pi for the path", true, "ask pi for the path"},
		{"pi should own this one", true, "pi should own this one"},
		{"handing to pi, then", true, "handing to pi, then"},
		// Word boundaries hold for bare mentions too.
		{"the pilot: reported in", false, ""},
		{"a pi-rate ship", false, ""},

		// Quoted/fenced content must not address anyone.
		{"saved the log:\n```\npi: do the thing\n```", false, ""},
		{"they said:\n> pi: do the thing", false, ""},
		{"saved the log:\n```\nask pi for the path\n```", false, ""},
	}
	for _, c := range cases {
		addressed, stripped := b.matchTrigger("team@muc.x.com", c.in)
		if addressed != c.addressed || (addressed && stripped != c.stripped) {
			t.Errorf("matchTrigger(%q) = (%v,%q), want (%v,%q)", c.in, addressed, stripped, c.addressed, c.stripped)
		}
	}
}

// TestMatchTriggerPerRoom pins the per-room trigger override (#106): the same
// body addresses the agent in one room and not in another.
func TestMatchTriggerPerRoom(t *testing.T) {
	b := NewBridge(ResolvedAccount{
		Owner: "zach@x.com", Nick: "pi", RoomTrigger: "pi",
		Rooms:     []string{"team@muc.x.com", "other@muc.x.com"},
		RoomSpecs: []RoomSpec{{JID: "other@muc.x.com", Trigger: "robot", Reactions: true}},
	}, false)
	if got := b.acct.TriggerFor("team@muc.x.com"); got != "pi" {
		t.Errorf("TriggerFor(team) = %q, want pi", got)
	}
	if got := b.acct.TriggerFor("other@muc.x.com"); got != "robot" {
		t.Errorf("TriggerFor(other) = %q, want robot", got)
	}
	if addressed, _ := b.matchTrigger("other@muc.x.com", "robot: go"); !addressed {
		t.Error("the room override should be the trigger in that room")
	}
	if addressed, _ := b.matchTrigger("other@muc.x.com", "pi: go"); addressed {
		t.Error("the account trigger must not apply where the room overrides it")
	}
	// An unknown room (e.g. the error room) falls back to the account trigger.
	if got := b.acct.TriggerFor("errors@muc.x.com"); got != "pi" {
		t.Errorf("TriggerFor(unknown room) = %q, want the account default pi", got)
	}
}

func TestClassify(t *testing.T) {
	b := roomBridge()
	cases := []struct {
		m      InboundMessage
		action roomAction
		body   string
	}{
		{InboundMessage{Body: "just chatting", Nick: "alice", FromOwner: false}, actionNotOurs, "just chatting"},
		{InboundMessage{Body: "pi: help alice", Nick: "alice", FromOwner: false}, actionCommentary, "help alice"},
		{InboundMessage{Body: "ask pi about it", Nick: "alice", FromOwner: false}, actionCommentary, "ask pi about it"},
		{InboundMessage{Body: "do it", Nick: "zach", FromOwner: true}, actionCanonical, "do it"},
		{InboundMessage{Body: "pi: do it", Nick: "zach", FromOwner: true}, actionCanonical, "do it"},
	}
	for _, c := range cases {
		action, body, _ := b.classify(c.m)
		if action != c.action || body != c.body {
			t.Errorf("classify(%+v) = (%d,%q), want (%d,%q)", c.m, action, body, c.action, c.body)
		}
	}
}

// TestClassifyReplyToOwnMessage pins the anchored-reply rule (#106): an inbound
// XEP-0461 reply to a stanza we sent addresses us even when it names nobody.
// The id must be one WE sent — the history records both directions, and a room
// send records the room, so "ours" is a flag rather than an inferred JID.
func TestClassifyReplyToOwnMessage(t *testing.T) {
	b := roomBridge()
	b.xmpp = NewXMPPBridge(ResolvedAccount{Owner: "zach@x.com", Nick: "pi"}, func(InboundMessage) {}, b.log)

	// Not ours (nobody's, or someone else's): still dropped.
	m := InboundMessage{Body: "and another thing", Nick: "alice", Room: "team@muc.x.com", ReplyToID: "someone-elses-id"}
	if action, _, _ := b.classify(m); action != actionNotOurs {
		t.Errorf("reply to an unknown/foreign stanza = %d, want actionNotOurs", action)
	}

	// Ours: addressed.
	b.xmpp.recordSelfMessage("our-stanza-id", "team@muc.x.com", "the earlier thing")
	m.ReplyToID = "our-stanza-id"
	if action, _, _ := b.classify(m); action != actionCommentary {
		t.Errorf("reply to our own stanza = %d, want actionCommentary", action)
	}
	// The owner replying to our stanza stays canonical.
	m.FromOwner = true
	if action, _, _ := b.classify(m); action != actionCanonical {
		t.Errorf("owner reply to our own stanza = %d, want actionCanonical", action)
	}
}

// TestUnaddressedRoomMessageIsDropped replaces the old ambient-buffer tests
// (#106): an unaddressed room message must produce no turn and must not stay in
// the durable inbox. Reaching the buffer at all is now the bug.
func TestUnaddressedRoomMessageIsDropped(t *testing.T) {
	acct := ResolvedAccount{Owner: "zach@x", Name: "t", Rooms: []string{"team@muc.x"}, RoomTrigger: "pi"}
	b := newTestBridge(acct)
	b.rpc = &RPCClient{stdin: &nopClose{buf: &bytes.Buffer{}}, mu: sync.Mutex{}}
	b.inbox = newInbox(filepath.Join(t.TempDir(), "t.inbox.jsonl"), b.log)

	b.onInbound(InboundMessage{
		ID: "unaddressed-1", Nick: "alice", Room: "team@muc.x",
		From: "alice@x.com/alice", Body: "the parser is flaky",
	})

	if n := b.inbox.len(); n != 0 {
		t.Errorf("unaddressed room message left %d entr(y/ies) in the durable inbox, want 0", n)
	}
}

// TestAddressedRoomMessageIsRecorded is the other half: an addressed message is
// still durably recorded before it reaches pi (#96), so a run that dies first
// does not take the instruction with it.
func TestAddressedRoomMessageIsRecorded(t *testing.T) {
	acct := ResolvedAccount{Owner: "zach@x", Name: "t", Rooms: []string{"team@muc.x"}, RoomTrigger: "pi"}
	b := newTestBridge(acct)
	b.rpc = &RPCClient{stdin: &nopClose{buf: &bytes.Buffer{}}, mu: sync.Mutex{}}
	b.inbox = newInbox(filepath.Join(t.TempDir(), "t.inbox.jsonl"), b.log)

	b.onInbound(InboundMessage{
		ID: "addressed-1", Nick: "alice", Room: "team@muc.x",
		From: "alice@x.com/alice", Body: "pi: fold this in",
	})

	if n := b.inbox.len(); n != 1 {
		t.Errorf("addressed message left %d inbox entries, want 1", n)
	}
}

func TestComposePrompt(t *testing.T) {
	b := roomBridge()      // owner zach@x.com, room team@muc.x.com
	b.routingSeeded = true // this test exercises the non-seeding path

	// Owner DM turn: "from:" is the owner, body follows directly (no sender
	// line). No routing hint is appended (removed per issue #33).
	got := b.composePrompt("hello", true, "", "zach@x.com", "", "", "", "")
	if !strings.HasPrefix(got, "from: zach@x.com\nhello") {
		t.Errorf("dm header wrong: %q", got)
	}
	if strings.Contains(got, "to: ") {
		t.Errorf("dm turn should not contain a routing hint: %q", got)
	}

	// Room turn from the owner: from: is the room, sender: is the owner's jid.
	got = b.composePrompt("hi", true, "", "team@muc.x.com", "zach@x.com", "", "", "")
	if !strings.Contains(got, "from: team@muc.x.com\n") || !strings.Contains(got, "sender: zach@x.com\n") {
		t.Errorf("room header wrong: %q", got)
	}

	// Commentary: wrapped as untrusted, includes nick + sender header.
	got = b.composePrompt("help", false, "alice", "team@muc.x.com", "alice@x.com", "", "", "")
	if !strings.Contains(got, "NON-OWNER") || !strings.Contains(got, "alice") ||
		!strings.Contains(got, "help") || !strings.Contains(got, "sender: alice@x.com") {
		t.Errorf("commentary framing wrong: %q", got)
	}

	// No buffered room chatter is prepended any more (#106): the prompt is the
	// message, and nothing else.
	got = b.composePrompt("do it", true, "", "team@muc.x.com", "zach@x.com", "", "", "")
	if strings.Contains(got, "room commentary") {
		t.Errorf("composePrompt still prepends room commentary: %q", got)
	}
	if !strings.Contains(got, "do it") {
		t.Errorf("canonical room prompt wrong: %q", got)
	}
}

// TestRoomsSeedOnce: the room list and its delivery rule are seeded with the
// routing contract, once per session (#106). An agent that assumes silence
// means an empty room will miss handoffs it was not named in, so this is not
// optional context.
func TestRoomsSeedOnce(t *testing.T) {
	b := roomBridge()
	got1 := b.composePrompt("go", true, "", "team@muc.x.com", "zach@x.com", "", "", "")
	if !strings.Contains(got1, "[pi-msg: rooms:") || !strings.Contains(got1, "team@muc.x.com") {
		t.Errorf("first prompt should seed the room list: %q", got1)
	}
	if !strings.Contains(got1, "read_room") {
		t.Errorf("the room seed should point at read_room: %q", got1)
	}
	got2 := b.composePrompt("again", true, "", "team@muc.x.com", "zach@x.com", "", "", "")
	if strings.Contains(got2, "[pi-msg: rooms:") {
		t.Errorf("second prompt re-seeded the room list: %q", got2)
	}
	// A 1:1 account has no rooms to describe.
	b1 := NewBridge(ResolvedAccount{Owner: "zach@x.com", Nick: "pi"}, false)
	if got := b1.composePrompt("hi", true, "", "zach@x.com", "", "", "", ""); strings.Contains(got, "[pi-msg: rooms:") {
		t.Errorf("1:1 account should not seed a room list: %q", got)
	}
}

func TestRoutingSeedOnce(t *testing.T) {
	b := roomBridge() // room-mode account
	// First prompt seeds the contract (once); room-mode only.
	got1 := b.composePrompt("go", true, "", "team@muc.x.com", "zach@x.com", "", "", "")
	if !strings.Contains(got1, "[pi-msg: routing:") {
		t.Errorf("first prompt should seed the routing contract: %q", got1)
	}
	// Subsequent prompts must NOT re-seed.
	got2 := b.composePrompt("again", true, "", "team@muc.x.com", "zach@x.com", "", "", "")
	if strings.Contains(got2, "[pi-msg: routing:") {
		t.Errorf("second prompt re-seeded the contract: %q", got2)
	}

	// A non-room (1:1) account never seeds.
	b1 := NewBridge(ResolvedAccount{Owner: "zach@x.com", Nick: "pi"}, false)
	got := b1.composePrompt("hi", true, "", "zach@x.com", "", "", "", "")
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
	got := b.composePrompt(task, true, "", b.acct.Owner, "", "", "", "")
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
	if got := b1.composePrompt(task, true, "", "zach@x.com", "", "", "", ""); got != task {
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

// The cascade notice must name neither an @handle nor the agent's own nick, or
// reporting a cascade would itself address an agent (#106 widened addressing to
// bare names) and extend the cascade.
func TestCascadeNoticeDoesNotAddress(t *testing.T) {
	b := roomBridge()
	notice := cascadeStopNotice()
	if strings.Contains(notice, "@") {
		t.Errorf("cascade notice contains an @mention: %q", notice)
	}
	if addressed, _ := b.matchTrigger("team@muc.x.com", notice); addressed {
		t.Errorf("cascade notice addresses an agent: %q", notice)
	}
	// And under every nick the fleet actually uses, since bare-name mentions
	// (#106) mean any of them appearing as a word would re-address an agent.
	for _, trig := range []string{"peppy", "fox", "falco", "slippy", "beltino", "r2d2"} {
		if containsMention(notice, trig) {
			t.Errorf("cascade notice addresses %q: %q", trig, notice)
		}
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
		if got, _ := b.matchTrigger("team@muc.x.com", c.in); got != c.want {
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

// TestHandleRoomDropsOwnEcho is defence in depth (#29): even if the transport
// echo filter in dispatchRoom misses, a room message whose sender nick matches
// our own must be dropped at dispatch — not re-enter classify (where a
// self-addressed body would dispatch as commentary and prompt ourselves).
func TestHandleRoomDropsOwnEcho(t *testing.T) {
	b := roomBridge()
	b.rpc = &RPCClient{}
	b.inbox = newInbox(filepath.Join(t.TempDir(), "t.inbox.jsonl"), b.log)
	b.xmpp = &XMPPBridge{acct: ResolvedAccount{Nick: "pi"}, selfNick: map[string]string{"team@muc.x.com": "pi"}}

	// Own-echo with a body that would otherwise be unaddressed: dropped, and
	// nothing is recorded for it.
	b.handleRoom(InboundMessage{ID: "echo-1", Body: "just chatting", Nick: "PI", Room: "team@muc.x.com"})
	if n := b.inbox.len(); n != 0 {
		t.Fatalf("own-echo left %d inbox entries, want 0", n)
	}
	// Own-echo addressed to our own trigger: must not dispatch as commentary
	// (without the guard this hits dispatchCommentary and prompts ourselves).
	b.handleRoom(InboundMessage{ID: "echo-2", Body: "pi: status?", Nick: "pI", Room: "team@muc.x.com"})
	if n := b.inbox.len(); n != 0 {
		t.Fatalf("addressed own-echo left %d inbox entries, want 0", n)
	}
	if b.streaming() {
		t.Fatal("own-echo must not start a run")
	}
}

// TestUnaddressedMessageDropsNothingButItself: the durable queue must not be
// used as a parking space for messages that will never become a prompt (#104),
// including one that arrives addressed and then hits the cascade cap.
func TestCascadeCapDropsRatherThanBuffers(t *testing.T) {
	acct := ResolvedAccount{Owner: "zach@x", Name: "t", Rooms: []string{"team@muc.x"}, RoomTrigger: "pi"}
	b := newTestBridge(acct)
	b.rpc = &RPCClient{stdin: &nopClose{buf: &bytes.Buffer{}}, mu: sync.Mutex{}}
	b.inbox = newInbox(filepath.Join(t.TempDir(), "t.inbox.jsonl"), b.log)
	for i := 0; i < cascadeCap; i++ {
		b.spendCascade()
	}
	b.handleRoom(InboundMessage{
		ID: "capped-1", Nick: "alice", Room: "team@muc.x.com",
		From: "alice@x.com/alice", Body: "pi: one more thing",
	})
	if n := b.inbox.len(); n != 0 {
		t.Errorf("a cascade-capped message left %d inbox entries, want 0", n)
	}
	if b.streaming() {
		t.Error("a cascade-capped message must not start a run")
	}
}

// The read path is the only way an agent sees a room it was not addressed in
// (#106), so its failure modes matter: an unjoined room must be refused, and a
// readable one must render something the model can act on.
func TestReadRoomRelayRejectsUnjoinedRoom(t *testing.T) {
	acct := ResolvedAccount{Owner: "zach@x", Name: "t", Rooms: []string{"team@muc.x"}, RoomTrigger: "pi"}
	b := newTestBridge(acct)
	var buf bytes.Buffer
	b.rpc = &RPCClient{stdin: &nopClose{buf: &buf}, mu: sync.Mutex{}}

	// The error room is the interesting case: joined at the XMPP layer, but
	// write-only by construction, so it must not be readable.
	b.handleToolRelay("r1", `{"action":"read_room","room":"errors@muc.x"}`)
	out := buf.String()
	if !strings.Contains(out, "not a room this bridge has joined") {
		t.Errorf("unjoined room not refused: %q", out)
	}
	if strings.Contains(out, "team@muc.x") && strings.Contains(out, "archived message") {
		t.Errorf("read_room returned content for an unjoined room: %q", out)
	}
}

func TestFormatRoomRead(t *testing.T) {
	got := formatRoomRead("team@muc.x", nil, 30, true)
	// The extension rejects any result without this prefix, so an empty archive
	// (a successful read of nothing) must still carry it — otherwise a working
	// read is reported to the model as a failed tool call.
	if !strings.HasPrefix(got, "[pi-msg: read_room:") || !strings.Contains(got, "no archived messages") {
		t.Errorf("an empty archive must carry the header and say so: %q", got)
	}

	stamp := time.Now().Add(-3 * time.Minute)
	msgs := []InboundMessage{
		{Nick: "slippy", Body: "the parser   is flaky", Stamp: stamp},
		{Nick: "peppy", Body: "on it", Stamp: stamp, ReplyToID: "abc123"},
		{Nick: "zach", Body: "thanks", Stamp: stamp, FromOwner: true},
	}
	got = formatRoomRead("team@muc.x", msgs, 30, true)
	if !strings.HasPrefix(got, "[pi-msg: read_room:") {
		t.Errorf("read_room block must carry its header (the tool keys off it): %q", got)
	}
	if !strings.Contains(got, "slippy (3m ago): the parser is flaky") {
		t.Errorf("sender/age/body line wrong: %q", got)
	}
	if !strings.Contains(got, "owner (3m ago): thanks") {
		t.Errorf("the owner should render as owner: %q", got)
	}
	if !strings.Contains(got, "[in reply to abc123]") {
		t.Errorf("XEP-0461 stamp not surfaced: %q", got)
	}
	if strings.Contains(got, "older history") {
		t.Errorf("a complete window should not claim older history: %q", got)
	}

	// An incomplete result set means the server has more behind this page.
	got = formatRoomRead("team@muc.x", msgs, 3, false)
	if !strings.Contains(got, "older history exists") {
		t.Errorf("an incomplete window should warn: %q", got)
	}
}

// A message classified as addressed keeps that verdict through the durable
// queue. Re-deriving it after a restart cannot work for an anchored reply — the
// stanza history that proved the target was ours is gone — so a re-delivered
// message would be dropped as "not ours" and removed permanently (#106 review).
func TestRedeliveredAddressedMessageKeepsItsVerdict(t *testing.T) {
	acct := ResolvedAccount{Owner: "zach@x", Name: "t", Rooms: []string{"team@muc.x"}, RoomTrigger: "pi"}
	b := newTestBridge(acct)
	var buf bytes.Buffer
	b.rpc = &RPCClient{stdin: &nopClose{buf: &buf}, mu: sync.Mutex{}}
	b.inbox = newInbox(t.TempDir()+"/acct.inbox.jsonl", nil)
	// Fresh process: nothing is in the stanza history, so the reply target is
	// unresolvable now — exactly the situation after a restart.
	b.xmpp = NewXMPPBridge(acct, func(InboundMessage) {}, b.log)

	b.deliverInbox(inboxEntry{
		ID: "r1", Room: "team@muc.x", Nick: "peppy", From: "team@muc.x/peppy",
		Body: "and another thing", ReplyToID: "a-stanza-we-sent-before-the-restart",
		Addressed: true,
	})
	if !strings.Contains(buf.String(), "and another thing") {
		t.Errorf("a message addressed on arrival was not prompted after re-delivery: %q", buf.String())
	}

	// Without the recorded verdict the same entry is dropped: the behaviour the
	// flag exists to prevent.
	var buf2 bytes.Buffer
	b2 := newTestBridge(acct)
	b2.rpc = &RPCClient{stdin: &nopClose{buf: &buf2}, mu: sync.Mutex{}}
	b2.inbox = newInbox(t.TempDir()+"/acct.inbox.jsonl", nil)
	b2.xmpp = NewXMPPBridge(acct, func(InboundMessage) {}, b2.log)
	b2.deliverInbox(inboxEntry{
		ID: "r2", Room: "team@muc.x", Nick: "peppy", From: "team@muc.x/peppy",
		Body: "and another thing", ReplyToID: "a-stanza-we-sent-before-the-restart",
	})
	if strings.Contains(buf2.String(), "and another thing") {
		t.Errorf("an unclassified reply to an unknown stanza should not prompt: %q", buf2.String())
	}
}

// The bare-mention counter: bareMention isolates the case where a bare name is
// the ONLY reason a message addresses us, so the false-positive rate the
// bare-name rule trades for can be measured from the log (#106).
func TestBareMentionCounter(t *testing.T) {
	b := roomBridge() // trigger "pi"

	if got := b.bareMention("team@muc.x.com", "ask pi for the path"); got != "pi" {
		t.Errorf("bareMention = %q, want pi", got)
	}
	// Explicit addresses are not false positives.
	for _, body := range []string{"pi: do it", "pi, go", "@pi do it", "@everyone report", "unrelated chatter", "the pilot flew"} {
		if got := b.bareMention("team@muc.x.com", body); got != "" {
			t.Errorf("bareMention(%q) = %q, want empty", body, got)
		}
	}
	// A bare mention still counts when the trigger is only part of the reason we
	// are addressed... it is not: a broadcast or a handle elsewhere means this is
	// not a bare-mention case, and the counter must not inflate.
	if got := b.bareMention("team@muc.x.com", "@everyone ask pi about the path"); got != "" {
		t.Errorf("bareMention with a broadcast = %q, want empty", got)
	}
}

// The seeded routing contract must state the new rule. It is the only place the
// false-positive cost of bare mentions is disclosed to the agents living with
// it, and the old wording said the opposite (#106).
func TestRoutingContractStatesBareMentions(t *testing.T) {
	b := roomBridge()
	got := b.routingContract()
	if strings.Contains(got, "a name without @ does not reach") {
		t.Errorf("routing contract still denies bare mentions: %q", got)
	}
	if !strings.Contains(got, "a name without @ also reaches it") {
		t.Errorf("routing contract does not state the bare-mention rule: %q", got)
	}
}
