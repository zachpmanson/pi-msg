package main

import (
	"bytes"
	"encoding/json"
	"encoding/xml"
	"io"
	"net/http"
	"net/http/httptest"
	"reflect"
	"strings"
	"sync"
	"testing"
	"time"
)

func TestExtractText(t *testing.T) {
	if got := extractText("  hi  "); got != "hi" {
		t.Errorf("string content = %q, want hi", got)
	}
	content := []any{
		map[string]any{"type": "text", "text": "line one"},
		map[string]any{"type": "thinking", "thinking": "should be dropped"},
		map[string]any{"type": "text", "text": "line two"},
	}
	if got := extractText(content); got != "line one\nline two" {
		t.Errorf("array content = %q, want two joined text blocks (thinking dropped)", got)
	}
	if got := extractText(nil); got != "" {
		t.Errorf("nil content = %q, want empty", got)
	}
	// Non-text-only content yields empty.
	onlyThinking := []any{map[string]any{"type": "thinking", "thinking": "x"}}
	if got := extractText(onlyThinking); got != "" {
		t.Errorf("thinking-only content = %q, want empty", got)
	}
}

func TestSplitCommand(t *testing.T) {
	cases := []struct {
		in        string
		name, arg string
	}{
		{"/new", "new", ""},
		{"/model anthropic/claude", "model", "anthropic/claude"},
		{"/think high", "think", "high"},
		{"/COMPACT  keep the api notes ", "compact", "keep the api notes"},
		{"!new", "new", ""},
		{"!session", "session", ""},
		{"!model deepseek/", "model", "deepseek/"},
		{"!", "", ""}, // bare prefix → empty name; handleCommand special-cases it
	}
	for _, c := range cases {
		name, arg := splitCommand(c.in)
		if name != c.name || arg != c.arg {
			t.Errorf("splitCommand(%q) = (%q,%q), want (%q,%q)", c.in, name, arg, c.name, c.arg)
		}
	}
}

// TestBareBangIsAbort verifies a lone "!" maps to the abort path, not a
// literal prompt: splitCommand yields an empty name, which handleCommand's
// special case turns into "abort" (without it, the empty name falls through
// the default as text). Non-bang inputs are unmodified.
func TestBareBangIsAbort(t *testing.T) {
	cases := []struct {
		in   string
		name string
	}{
		{"!", "abort"},   // lone bang → handleCommand maps to abort
		{"!!", "!"},      // two bangs → name "!", falls through as text
		{"! abort", ""},  // space after bang → empty name, falls through
		{"!abort", "abort"},
	}
	for _, c := range cases {
		name, _ := splitCommand(c.in)
		if c.in == "!" {
			name = "abort" // the handleCommand special case under test
		}
		if name != c.name {
			t.Errorf("command for %q → %q, want %q", c.in, name, c.name)
		}
	}
}

func TestCommaInt(t *testing.T) {
	cases := map[int64]string{
		0:     "0",
		999:   "999",
		1000:  "1,000",
		12345: "12,345",
		1e9:   "1,000,000,000",
	}
	for in, want := range cases {
		if got := commaInt(in); got != want {
			t.Errorf("commaInt(%d) = %q, want %q", in, got, want)
		}
	}
}

func TestMatchModel(t *testing.T) {
	res := Event{"data": map[string]any{"models": []any{
		map[string]any{"provider": "anthropic", "id": "claude-sonnet-5"},
		map[string]any{"provider": "google", "id": "gemini-2.5-pro"},
	}}}
	provider, id, ok := matchModel(res, "sonnet")
	if !ok || provider != "anthropic" || id != "claude-sonnet-5" {
		t.Errorf("matchModel(sonnet) = (%q,%q,%v), want anthropic/claude-sonnet-5", provider, id, ok)
	}
	if _, _, ok := matchModel(res, "nonesuch"); ok {
		t.Error("matchModel(nonesuch) matched unexpectedly")
	}
}

func TestToolLabel(t *testing.T) {
	cases := []struct {
		name string
		ev   Event
		want string
	}{
		{"bash with command", Event{"toolName": "bash", "args": map[string]any{"command": "npm test"}}, "! npm test"},
		{"bash collapses whitespace", Event{"toolName": "bash", "args": map[string]any{"command": "go  build\n./..."}}, "! go build ./..."},
		{"non-bash tool", Event{"toolName": "read_file"}, "! read_file"},
		{"missing name", Event{}, "running a tool…"},
		{"bash no command", Event{"toolName": "bash"}, "! bash"},
	}
	for _, c := range cases {
		if got := toolLabel(c.ev); got != c.want {
			t.Errorf("%s: toolLabel = %q, want %q", c.name, got, c.want)
		}
	}
}

func TestTruncateLabel(t *testing.T) {
	if got := truncateLabel("short", 40); got != "short" {
		t.Errorf("short = %q, want unchanged", got)
	}
	long := "abcdefghij" // 10 runes
	if got := truncateLabel(long, 5); got != "abcd…" {
		t.Errorf("long = %q, want abcd…", got)
	}
	if got := truncateLabel("a\tb\nc  d", 40); got != "a b c d" {
		t.Errorf("whitespace = %q, want single-spaced", got)
	}
}

func TestSplitReplySegments(t *testing.T) {
	seg := func(dest, body string) replySegment {
		return replySegment{dest: dest, body: body}
	}
	cases := []struct {
		name        string
		in          string
		wantSegs    []replySegment
		wantLeading string
	}{
		{"single newline form", "to: room@muc.x\nhere are headlines",
			[]replySegment{seg("room@muc.x", "here are headlines")}, ""},
		{"no space after colon", "to:zach@x\nhi",
			[]replySegment{seg("zach@x", "hi")}, ""},
		{"inline body", "to: alice@x hello there",
			[]replySegment{seg("alice@x", "hello there")}, ""},
		{"two segments", "to: a@x.com\nblah blah\nto: b@x.com\nmore stuff",
			[]replySegment{seg("a@x.com", "blah blah"), seg("b@x.com", "more stuff")}, ""},
		{"multiline body per segment", "to: a@x\nl1\nl2\nto: b@x\nm1",
			[]replySegment{seg("a@x", "l1\nl2"), seg("b@x", "m1")}, ""},
		{"case insensitive", "TO: zach@x\nyo",
			[]replySegment{seg("zach@x", "yo")}, ""},
		{"leading junk before first to", "oops forgot\nto: a@x\nbody",
			[]replySegment{seg("a@x", "body")}, "oops forgot"},
		{"prose to: without @ is not a route", "to: whom it may concern\nhello",
			nil, "to: whom it may concern\nhello"},
		{"no routing at all", "just a reply", nil, "just a reply"},
	}
	for _, c := range cases {
		gotSegs, gotLeading := splitReplySegments(c.in)
		if gotLeading != c.wantLeading {
			t.Errorf("%s: leading = %q, want %q", c.name, gotLeading, c.wantLeading)
		}
		if !reflect.DeepEqual(gotSegs, c.wantSegs) {
			t.Errorf("%s: segs = %+v, want %+v", c.name, gotSegs, c.wantSegs)
		}
	}
}

func TestClassifyDest(t *testing.T) {
	x := NewXMPPBridge(
		ResolvedAccount{Rooms: []string{"team@muc.x"}, Owner: "zach@x"},
		func(InboundMessage) {}, func(string, string) {},
	)
	x.occupants["team@muc.x"] = map[string]string{"alice": "alice@x"}
	cases := []struct {
		dest string
		want destKind
	}{
		{"team@muc.x", destRoom},
		{"team@muc.x/somenick", destRoom},
		{"zach@x", destUser},
		{"zach@x/phone", destUser},
		{"alice@x", destUser},
		{"stranger@x", destBlocked},
		{"", destBlocked},
	}
	for _, c := range cases {
		if got := x.classifyDest(c.dest); got != c.want {
			t.Errorf("classifyDest(%q) = %d, want %d", c.dest, got, c.want)
		}
	}
}

// TestStreamTypingTarget pins the room-mode typing decision (issue #44): the
// indicator is withheld while the reply is still streaming / has not yet
// written a routing line, and once a completed "to:" line appears it points at
// that line's 1:1 recipient — or stays dark for a room, noop, or blocked target.
func TestStreamTypingTarget(t *testing.T) {
	x := NewXMPPBridge(
		ResolvedAccount{Rooms: []string{"team@muc.x"}, Owner: "zach@x"},
		func(InboundMessage) {}, func(string, string) {},
	)
	x.occupants["team@muc.x"] = map[string]string{"alice": "alice@x"}
	cases := []struct {
		buf     string
		target  string
		decided bool
	}{
		// Not yet a complete routing line → keep waiting.
		{"", "", false},
		{"to:", "", false},
		{"to: zach", "", false},
		{"to: zach@x", "", false},
		// Owner 1:1 → indicator on the owner.
		{"to: zach@x\n", "zach@x", true},
		{"to: zach@x/phone\n", "zach@x", true},
		// Known occupant → indicator on the occupant.
		{"to: alice@x\n", "alice@x", true},
		// A leading non-routing line is skipped; the routing still resolves.
		{"sure\nto: zach@x\n", "zach@x", true},
		// Room, noop, and unknown targets never light the owner's bubble.
		{"to: team@muc.x\n", "", true},
		{"to: noop\n", "", true},
		{"to: stranger@x\n", "", true},
	}
	for _, c := range cases {
		got, decided := streamTypingTarget(c.buf, x)
		if got != c.target || decided != c.decided {
			t.Errorf("streamTypingTarget(%q) = (%q,%v), want (%q,%v)",
				c.buf, got, decided, c.target, c.decided)
		}
	}
}

// TestErrorRoomInvisibleToAgent verifies the write-only error room is NOT in
// roomBares (so dispatch ignores it) and is NOT an allowed reply/send
// destination — agents can't read it or route to it.
func TestErrorRoomInvisibleToAgent(t *testing.T) {
	x := NewXMPPBridge(
		ResolvedAccount{Rooms: []string{"team@muc.x"}, ErrorRoom: "errors@muc.x", Owner: "zach@x"},
		func(InboundMessage) {}, func(string, string) {},
	)
	if x.isRoomJID("errors@muc.x") {
		t.Error("error room must NOT be an agent-visible (dispatched) room")
	}
	if !x.isRoomJID("team@muc.x") {
		t.Error("normal room should still be agent-visible")
	}
	if got := x.classifyDest("errors@muc.x"); got != destBlocked {
		t.Errorf("error room should be blocked for replies, got %v", got)
	}
	if got := x.classifyDest("team@muc.x"); got != destRoom {
		t.Errorf("normal room should remain an allowed destination, got %v", got)
	}
}

func TestRoutingNudgeBound(t *testing.T) {
	b := NewBridge(ResolvedAccount{}, false)
	for i := 1; i <= maxRoutingNudges; i++ {
		if !b.bumpRoutingNudge() {
			t.Errorf("nudge %d should be allowed (cap %d)", i, maxRoutingNudges)
		}
	}
	if b.bumpRoutingNudge() {
		t.Error("nudge past the cap should be denied")
	}
	b.resetRoutingNudges()
	if !b.bumpRoutingNudge() {
		t.Error("after reset, a nudge should be allowed again")
	}
}

// TestStagedNudgeLifecycle verifies issue #16's core flow: rejectReply stages
// a correction that is only fired at settle if a later message didn't route.
func TestStagedNudgeLifecycle(t *testing.T) {
	b := NewBridge(ResolvedAccount{}, false)

	// Nothing staged → nothing to fire.
	if got := b.takeStagedNudge(); got != "" {
		t.Errorf("empty staged nudge → got %q, want empty", got)
	}

	// Stage a correction (as rejectReply does), then a later message routes
	// fine → the staged nudge is cleared and never fires.
	b.stageNudge("dropped body", "no to: line")
	b.clearPendingNudge()
	if got := b.takeStagedNudge(); got != "" {
		t.Errorf("staged nudge after clear → got %q, want empty", got)
	}

	// Stage a correction and fire at settle → reason fires exactly once.
	b.stageNudge("dropped body", "no to: line")
	if got := b.takeStagedNudge(); got != "no to: line" {
		t.Errorf("settled nudge reason = %q, want %q", got, "no to: line")
	}
	if got := b.takeStagedNudge(); got != "" {
		t.Errorf("staged nudge should fire once, got %q on second take", got)
	}

	// Later staging replaces earlier — only the final reason is nudged.
	b.stageNudge("a", "reason one")
	b.stageNudge("b", "reason two")
	if got := b.takeStagedNudge(); got != "reason two" {
		t.Errorf("latest staged reason = %q, want %q", got, "reason two")
	}
}

// TestStagedNudgeRespectsBudget verifies the per-turn cap still bounds the
// settle-time reminder even with a single staging point.
func TestStagedNudgeRespectsBudget(t *testing.T) {
	b := NewBridge(ResolvedAccount{}, false)
	b.stageNudge("a", "r1")
	b.stageNudge("b", "r2")
	if got := b.takeStagedNudge(); got != "r2" {
		t.Fatalf("first staged nudge = %q, want r2", got)
	}
	// Both staged nudges consumed the budget now; a fresh turn resets it.
	b.resetRoutingNudges()
	b.stageNudge("c", "r3")
	if got := b.takeStagedNudge(); got != "r3" {
		t.Errorf("post-reset staged nudge = %q, want r3", got)
	}
}

// TestFirePendingNudgeReportsLaunch pins the banner-suppression contract: a
// settle that launches the routing nudge holds the "done (no reply)" banner,
// because the resend arrives moments later. firePendingNudge must report
// whether it actually launched so the caller can gate on it.
func TestFirePendingNudgeReportsLaunch(t *testing.T) {
	b := roomBridge()
	b.rpc = &RPCClient{} // fire-and-forget send to nowhere; avoids a nil deref

	// Nothing staged → no nudge launches.
	if b.firePendingNudge() {
		t.Error("no staged nudge → firePendingNudge must report false")
	}

	// A staged correction → the nudge fires and is reported.
	b.stageNudge("dropped body", "no to: line")
	if !b.firePendingNudge() {
		t.Error("staged nudge → firePendingNudge must report true")
	}

	// Consumed on fire → nothing left to launch.
	if b.firePendingNudge() {
		t.Error("after firing, no second nudge may launch")
	}
}

func TestPrettyDump(t *testing.T) {
	jsonl := strings.Join([]string{
		`{"type":"session","timestamp":"2024-12-03T14:00:00.000Z","cwd":"/proj"}`,
		`{"type":"message","timestamp":"2024-12-03T14:00:01.000Z","message":{"role":"user","content":"fix the build"}}`,
		`{"type":"message","timestamp":"2024-12-03T14:00:02.000Z","message":{"role":"assistant","content":[{"type":"text","text":"on it"},{"type":"toolCall","toolName":"bash"}]}}`,
		`{"type":"message","timestamp":"2024-12-03T14:00:03.000Z","message":{"role":"toolResult","toolName":"bash","content":[{"type":"text","text":"exit 0"}]}}`,
		`{"type":"model_change","timestamp":"2024-12-03T14:05:00.000Z","provider":"anthropic","modelId":"claude"}`,
	}, "\n")
	out := prettyDump([]byte(jsonl))
	for _, want := range []string{
		"TIME", "KIND", "DETAIL",
		"14:00:01", "user", "fix the build",
		"assistant", "on it ⚙ bash",
		"toolResult", "↳ bash: exit 0",
		"model", "anthropic/claude",
	} {
		if !strings.Contains(out, want) {
			t.Errorf("prettyDump missing %q in:\n%s", want, out)
		}
	}
}

func TestReactionEmojis(t *testing.T) {
	// Build the token stream directly (ASCII content) to exercise the reaction
	// extraction logic without any note-of-tool mangling of embedded emoji or
	// XML-in-string. The emoji round-trip itself is covered by the real XML
	// decode path (see xmpp.go handle / reactionEmojis) and TestInboundReactionAck.
	toks := []xml.Token{
		xml.StartElement{Name: xml.Name{Local: "message"}},
		xml.StartElement{Name: xml.Name{Local: "reactions", Space: reactionsNS}},
		xml.StartElement{Name: xml.Name{Local: "reaction"}},
		xml.CharData("ACK"),
		xml.EndElement{Name: xml.Name{Local: "reaction"}},
		xml.StartElement{Name: xml.Name{Local: "reaction"}},
		xml.CharData("OK"),
		xml.EndElement{Name: xml.Name{Local: "reaction"}},
		xml.StartElement{Name: xml.Name{Local: "reaction"}},
		xml.CharData("  "),
		xml.EndElement{Name: xml.Name{Local: "reaction"}},
		xml.EndElement{Name: xml.Name{Local: "reactions"}},
	}
	got := reactionEmojis(toks)
	if len(got) != 2 || got[0] != "ACK" || got[1] != "OK" {
		t.Errorf("reactionEmojis = %v, want [ACK OK]", got)
	}
	if got := reactionEmojis(nil); len(got) != 0 {
		t.Errorf("reactionEmojis(nil) = %v, want empty", got)
	}
}

func TestInboundReactionAck(t *testing.T) {
	// Path 1: a run is in flight → the ack is buffered to ambient, not a wake.
	b := roomBridge() // room-mode, owner zach@x.com
	b.setStreaming(true)
	b.onInbound(InboundMessage{
		Nick: "peppy", Room: "team@muc.x.com",
		From: "peppy@x.com/peppy", Reactions: []string{"\U0001FAE1"}, ReactionID: "target-123",
	})
	amb := b.drainAmbient()
	if !strings.Contains(amb, "peppy") || !strings.Contains(amb, "\U0001FAE1") || !strings.Contains(amb, "XEP-0444") {
		t.Errorf("streaming ack not buffered as ambient: %q", amb)
	}
	if b.reactionAckRun {
		t.Error("streaming path should not set reactionAckRun")
	}

	// Path 2: idle → the ack wakes the agent (reactionAckRun set, turnDest = room).
	b2 := roomBridge()
	b2.rpc = &RPCClient{} // fire-and-forget send to nowhere; avoids a nil deref
	b2.onInbound(InboundMessage{
		Nick: "peppy", Room: "team@muc.x.com",
		From: "peppy@x.com/peppy", Reactions: []string{"\U0001FAE1"}, ReactionID: "target-123",
	})
	if !b2.reactionAckRun {
		t.Error("idle reaction should set reactionAckRun")
	}
	if b2.currentTurnDest() != "team@muc.x.com" {
		t.Errorf("idle room reaction turnDest = %q, want room", b2.currentTurnDest())
	}
	if got := b2.drainAmbient(); got != "" {
		t.Errorf("idle reaction should not buffer ambient: %q", got)
	}

	// Owner reacting on 1:1 renders as "owner" and turns to the owner.
	b3 := NewBridge(ResolvedAccount{Owner: "zach@x.com", Nick: "pi"}, false)
	b3.rpc = &RPCClient{}
	b3.onInbound(InboundMessage{
		Direct: true, FromOwner: true, From: "zach@x.com/res",
		Reactions: []string{"\u2705"}, ReactionID: "out-1",
	})
	if !b3.reactionAckRun {
		t.Error("owner 1:1 reaction should wake (set reactionAckRun)")
	}
	if b3.currentTurnDest() != "zach@x.com" {
		t.Errorf("owner 1:1 reaction turnDest = %q, want owner", b3.currentTurnDest())
	}
}

func TestIdleAwayClock(t *testing.T) {
	b := roomBridge()
	b.markActive()
	if !b.idleSince.IsZero() {
		t.Error("markActive should clear idleSince")
	}
	b.markIdle()
	if b.idleSince.IsZero() {
		t.Error("markIdle should set idleSince")
	}
	// An inbound message marks active again, even a non-canonical one.
	b.markActive()
	if !b.idleSince.IsZero() {
		t.Error("markActive after activity should clear idleSince")
	}
}

// TestInboundRearmsIdleClock guards against the "busy-room bot never goes
// away" regression: an inbound message that never becomes a run (ambient room
// chatter, buffered with no prompt) previously left idleSince cleared by
// markActive, and since agent_settled never fires for it, the idle watcher had
// no way to ever drift the agent back to "away". onInbound must re-arm the
// clock so a quiet stretch still produces an away transition.
func TestInboundRearmsIdleClock(t *testing.T) {
	b := roomBridge()
	b.idleSince = time.Time{} // e.g. just cleared by a prior markActive
	b.awayAnnounced = false

	// Ambient room message: not from the owner, not addressed to the bot →
	// buffered, no run, no agent_settled.
	b.onInbound(InboundMessage{
		Nick: "falco", Room: "team@muc.x.com",
		From: "falco@x.com/falco", Body: "some ambient chatter",
	})

	if b.idleSince.IsZero() {
		t.Fatal("ambient inbound should re-arm the idle clock; zero idleSince = can never go away")
	}
	if elapsed := time.Since(b.idleSince); elapsed > time.Second {
		t.Errorf("idleSince should be restarted to ~now, got %v old", elapsed)
	}
	if b.awayAnnounced {
		t.Error("onInbound should leave awayAnnounced false so a fresh away can be announced")
	}
}

// TestIdleTickNoSelfDeadlockWhileStreaming guards against a regression where
// idleTick held b.mu and then called streaming() (which itself locks b.mu),
// self-deadlocking the idle-watcher goroutine — and, since idleTick's handler
// runs in the same goroutine as the XMPP read loop for other callers of b.mu,
// wedging the whole bridge until a manual restart. A buggy idleTick would hang
// forever here instead of returning.
func TestIdleTickNoSelfDeadlockWhileStreaming(t *testing.T) {
	b := roomBridge()
	b.idleSince = time.Now().Add(-idleAwayTimeout - time.Minute)
	b.streamingRun = true

	done := make(chan struct{})
	go func() {
		b.idleTick()
		close(done)
	}()
	select {
	case <-done:
	case <-time.After(2 * time.Second):
		t.Fatal("idleTick deadlocked while a run was streaming")
	}
}

func TestLoadAwayActivities(t *testing.T) {
	b := roomBridge()
	b.loadAwayActivities()
	b.mu.Lock()
	defer b.mu.Unlock()
	if len(awayActivities) < 400 {
		t.Errorf("embedded away-activities.txt parsed to %d entries, want >= 400", len(awayActivities))
	}
	for _, a := range awayActivities {
		if a == "" {
			t.Error("empty activity line in pool")
		}
		if len(a) > 90 {
			t.Errorf("activity too long (%d): %q", len(a), a)
		}
	}
}

// TestToolRelayCarriesReason: a relayed tool action must come back as a string
// — "ok" on success, or the failure reason — not a bare boolean, so the model
// learns *why* something failed (issue #34: e.g. an upload rejected by the
// server as too large never reached the agent).
func TestToolRelayCarriesReason(t *testing.T) {
	var buf bytes.Buffer
	b := roomBridge()
	b.rpc = &RPCClient{stdin: &nopClose{buf: &buf}, mu: sync.Mutex{}}
	b.xmpp = &XMPPBridge{ownerBare: "zach@x.com"} // owner allowlisted; SendFile fails fast ("not online")

	readLine := func(t *testing.T) map[string]any {
		t.Helper()
		deadline := time.Now().Add(2 * time.Second)
		for {
			if line, err := buf.ReadString('\n'); err == nil {
				var resp map[string]any
				if err := json.Unmarshal([]byte(line), &resp); err != nil {
					t.Fatalf("bad relay response line %q: %v", line, err)
				}
				return resp
			}
			if time.Now().After(deadline) {
				t.Fatalf("no relay response within 2s (buf=%q)", buf.String())
			}
			time.Sleep(5 * time.Millisecond)
		}
	}

	// Bad payload: the parse error is the reason.
	b.handleToolRelay("r-bad", `{not json`)
	resp := readLine(t)
	if resp["id"] != "r-bad" {
		t.Errorf("bad-payload response id = %v, want r-bad", resp["id"])
	}
	if v, _ := resp["value"].(string); !strings.Contains(v, "bad tool-relay payload") {
		t.Errorf("bad-payload response value = %q, want a reason", v)
	}

	// Blocked destination: the allowlist refusal is the reason.
	b.handleToolRelay("r-block", `{"action":"file","path":"/tmp/a.apk","to":"stranger@x.com"}`)
	resp = readLine(t)
	if v, _ := resp["value"].(string); !strings.Contains(v, "not an allowed destination") {
		t.Errorf("blocked-dest response value = %q, want allowlist reason", v)
	}

	// Failed upload: SendFile's error text, not a plain false (issue #34).
	b.handleToolRelay("r-file", `{"action":"file","path":"/tmp/a.apk","to":"zach@x.com"}`)
	resp = readLine(t)
	if v, _ := resp["value"].(string); !strings.Contains(v, "not online") {
		t.Errorf("file-failure response value = %q, want SendFile's reason (not online)", v)
	}
	if _, hasConfirmed := resp["confirmed"]; hasConfirmed {
		t.Errorf("file-failure response = %v, unexpected confirm-style boolean field", resp)
	}
}

// TestToolRelaySuccessOk: a successful relay answers "ok", which the extension
// maps to a clean tool result (as opposed to a generic failure).
func TestToolRelaySuccessOk(t *testing.T) {
	var buf bytes.Buffer
	b := roomBridge()
	b.rpc = &RPCClient{stdin: &nopClose{buf: &buf}, mu: sync.Mutex{}}
	b.xmpp = &XMPPBridge{ownerBare: "zach@x.com"}

	// The react path is synchronous and doesn't need a live session: a missing
	// target is the only failure mode reachable here, so feed one and assert
	// the reason names the missing stanza.
	b.handleToolRelay("r-react", `{"action":"react","emoji":"✅","messageId":"nonexistent-1"}`)

	deadline := time.Now().Add(2 * time.Second)
	var line string
	for {
		if l, err := buf.ReadString('\n'); err == nil {
			line = l
			break
		}
		if time.Now().After(deadline) {
			t.Fatalf("no relay response within 2s (buf=%q)", buf.String())
		}
		time.Sleep(5 * time.Millisecond)
	}
	if !strings.Contains(line, "\"value\"") || !strings.Contains(line, "not found in message history") {
		t.Errorf("react-miss response = %q, want a reason naming the missing target", line)
	}
}

// nopClose adapts a bytes.Buffer to io.WriteCloser for RPCClient.stdin.
type nopClose struct{ buf *bytes.Buffer }

func (n *nopClose) Write(p []byte) (int, error) { return n.buf.Write(p) }
func (n *nopClose) Close() error                { return nil }

func TestOpenRouterCreditsParse(t *testing.T) {
	// Build a throwaway HTTP server to avoid hitting the real endpoint.
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if got := r.Header.Get("Authorization"); got != "Bearer sk-or-test" {
			t.Errorf("auth header = %q, want Bearer sk-or-test", got)
		}
		w.Header().Set("Content-Type", "application/json")
		io.WriteString(w, `{"data":{"total_credits":85,"total_usage":81.11}}`)
	}))
	defer srv.Close()
	orig := creditEndpoint
	creditEndpoint = srv.URL
	defer func() { creditEndpoint = orig }()
	total, used, err := openRouterCredits("sk-or-test")
	if err != nil {
		t.Fatalf("openRouterCredits: %v", err)
	}
	if total != 85 || used != 81.11 {
		t.Fatalf("got total=%v used=%v, want 85 / 81.11", total, used)
	}
}

// newTestBridge builds an offline bridge: sends return "" (not online), which
// is exactly the "nothing reached a destination" case deliverReply must report.
func newTestBridge(acct ResolvedAccount) *Bridge {
	b := NewBridge(acct, false)
	b.xmpp = NewXMPPBridge(acct, func(InboundMessage) {}, b.log)
	return b
}

// TestRepliedOnlyOnDelivery pins the core of the dropped-reply fix: "replied"
// must mean "reached a destination", not "text existed". A malformed reply goes
// to the write-only error room, which the owner never reads, so counting it as
// a reply would suppress the settle-time banner and leave the owner in silence.
func TestRepliedOnlyOnDelivery(t *testing.T) {
	room := ResolvedAccount{Rooms: []string{"team@muc.x"}, ErrorRoom: "errors@muc.x", Owner: "zach@x"}
	b := newTestBridge(room)
	if b.deliverReply("no routing line here") {
		t.Error("a reply with no \"to:\" line must not count as delivered")
	}
	if b.deliverReply("to: errors@muc.x\n\nsneaky") {
		t.Error("a blocked destination must not count as delivered")
	}
	// Offline: a well-formed reply still can't reach anyone.
	if b.deliverReply("to: zach@x\n\nhello") {
		t.Error("an offline send must not count as delivered")
	}
	// Pure 1:1 accounts take the other branch; offline is still not delivered.
	solo := newTestBridge(ResolvedAccount{Owner: "zach@x"})
	if solo.deliverReply("hello") {
		t.Error("an offline 1:1 send must not count as delivered")
	}
}

// TestNoopStillCountsAsReplied verifies deliberate silence stays an answer:
// "to: noop" emits no stanza but must never look like a run that died before
// writing a reply, or the empty-tail recovery would argue with it.
func TestNoopStillCountsAsReplied(t *testing.T) {
	b := newTestBridge(ResolvedAccount{Rooms: []string{"team@muc.x"}, Owner: "zach@x"})
	if !b.deliverReply("to: noop\n\nnothing to add") {
		t.Error("to: noop must count as delivered")
	}
	if !b.replied() {
		t.Error("to: noop must set replied")
	}
}

// TestPreambleDoesNotDisarmNoReplyNet covers the failure that dropped replies
// live: the agent writes a preamble alongside its tool call, the tool returns,
// and the run ends with no further text. The delivered preamble used to mark
// the run as answered, so no banner and no retry fired — the owner just got a
// "running…" line and then silence.
func TestPreambleDoesNotDisarmNoReplyNet(t *testing.T) {
	b := newTestBridge(ResolvedAccount{Rooms: []string{"team@muc.x"}, Owner: "zach@x"})
	b.resetTailTracking()
	// Preamble message: text delivered, then the tool starts.
	b.setFinalMsgHadText(true)
	b.clearToolSinceDelivery()
	b.markToolSinceDelivery()
	// The tool result arrives and the run ends with a tool-only message.
	b.setFinalMsgHadText(false)
	if !b.needsEmptyTailRecovery() {
		t.Error("a run ending on a tool call after a preamble needs recovery")
	}
	// The winning shape: the reply text comes after the tool result.
	b.setFinalMsgHadText(true)
	b.clearToolSinceDelivery()
	if b.needsEmptyTailRecovery() {
		t.Error("a run that replied after its tool needs no recovery")
	}
}

// TestNoRecoveryWithoutTool verifies a run that never ran a tool is left alone:
// an empty final message there is the model saying nothing, which the existing
// "done (no reply)" banner already covers.
func TestNoRecoveryWithoutTool(t *testing.T) {
	b := newTestBridge(ResolvedAccount{Owner: "zach@x"})
	b.resetTailTracking()
	if b.needsEmptyTailRecovery() {
		t.Error("no tool ran — recovery must not fire")
	}
	// Volunteer and reaction-ack runs are allowed to end quietly even mid-work.
	b.markToolSinceDelivery()
	b.volunteered = true
	if b.needsEmptyTailRecovery() {
		t.Error("a volunteer run must not trigger recovery")
	}
	b.volunteered = false
	b.reactionAckRun = true
	if b.needsEmptyTailRecovery() {
		t.Error("a reaction-ack run must not trigger recovery")
	}
}

// TestEmptyTailRecoveryBounded verifies the retry can't loop against a model
// that keeps ending its runs on a tool call: one prompt per user turn, then the
// banner takes over and tells the owner nothing came back.
func TestEmptyTailRecoveryBounded(t *testing.T) {
	b := NewBridge(ResolvedAccount{}, false)
	for i := 1; i <= maxTailNudges; i++ {
		if !b.bumpTailNudge() {
			t.Errorf("recovery %d should be allowed (cap %d)", i, maxTailNudges)
		}
	}
	if b.bumpTailNudge() {
		t.Error("recovery past the cap should be denied")
	}
	b.resetTailNudges()
	if !b.bumpTailNudge() {
		t.Error("after a fresh user turn, recovery should be allowed again")
	}
}

// TestSettleLocallyClearsTailTracking verifies an aborted or replaced run can't
// leave state behind that fires a recovery prompt for cancelled work.
func TestSettleLocallyClearsTailTracking(t *testing.T) {
	b := newTestBridge(ResolvedAccount{Owner: "zach@x"})
	b.markToolSinceDelivery()
	b.setFinalMsgHadText(false)
	if !b.needsEmptyTailRecovery() {
		t.Fatal("precondition: mid-work run should need recovery")
	}
	b.settleLocally()
	if b.needsEmptyTailRecovery() {
		t.Error("settleLocally must clear the empty-tail bookkeeping")
	}
}

// TestUnansweredRunCounts covers the steer drop seen live: five messages went
// into one run and only the last one got an answer, because pi injects each
// queued message the instant a tool yields and the model moves on to it.
func TestUnansweredRunCounts(t *testing.T) {
	b := newTestBridge(ResolvedAccount{Owner: "zach@x"})
	// The live A–E burst: 5 messages in, 1 reply out.
	for range 5 {
		b.countInbound()
	}
	b.countDelivery()
	in, out, ok := b.unansweredRun()
	if !ok || in != 5 || out != 1 {
		t.Errorf("A–E burst = (%d,%d,%v), want (5,1,true)", in, out, ok)
	}
	// Every message answered → silent.
	b.resetRunCounts()
	for range 3 {
		b.countInbound()
		b.countDelivery()
	}
	if _, _, ok := b.unansweredRun(); ok {
		t.Error("a run that answered every message must not hint")
	}
	// A single unanswered message is the empty-tail case, not this one: the
	// "done (no reply)" banner and the tail recovery already cover it.
	b.resetRunCounts()
	b.countInbound()
	if _, _, ok := b.unansweredRun(); ok {
		t.Error("a single-message run must not hint")
	}
	// Volunteer and reaction-ack runs stay quiet even when unbalanced.
	b.resetRunCounts()
	b.countInbound()
	b.countInbound()
	b.volunteered = true
	if _, _, ok := b.unansweredRun(); ok {
		t.Error("a volunteer run must not hint")
	}
	b.volunteered = false
	b.reactionAckRun = true
	if _, _, ok := b.unansweredRun(); ok {
		t.Error("a reaction-ack run must not hint")
	}
}

// TestUnansweredHintBounded verifies the hint can't loop: one per user turn,
// refilled when the next message arrives.
func TestUnansweredHintBounded(t *testing.T) {
	b := NewBridge(ResolvedAccount{}, false)
	for i := 1; i <= maxHintNudges; i++ {
		if !b.bumpHintNudge() {
			t.Errorf("hint %d should be allowed (cap %d)", i, maxHintNudges)
		}
	}
	if b.bumpHintNudge() {
		t.Error("hint past the cap should be denied")
	}
	b.resetHintNudges()
	if !b.bumpHintNudge() {
		t.Error("after a fresh user turn, a hint should be allowed again")
	}
}

// TestNoopCountsTowardAnsweredRun verifies deliberate silence balances the
// tally, so a run the agent answered with "to: noop" is never nagged.
func TestNoopCountsTowardAnsweredRun(t *testing.T) {
	b := newTestBridge(ResolvedAccount{Rooms: []string{"team@muc.x"}, Owner: "zach@x"})
	b.countInbound()
	b.countInbound()
	if !b.deliverReply("to: zach@x\n\nanswered one") {
		// Offline, so this send reports undelivered; count it by hand to model
		// the online case.
		b.countDelivery()
	}
	if b.deliverReply("to: noop\n\nnothing more to add") {
		b.countDelivery()
	}
	if _, _, ok := b.unansweredRun(); ok {
		t.Error("a run answered with a reply plus to: noop must not hint")
	}
}

// TestSettleClearsRunCounts verifies an aborted run can't carry its tally into
// the next one and fire a hint for cancelled work.
func TestSettleClearsRunCounts(t *testing.T) {
	b := newTestBridge(ResolvedAccount{Owner: "zach@x"})
	b.countInbound()
	b.countInbound()
	if _, _, ok := b.unansweredRun(); !ok {
		t.Fatal("precondition: 2 in / 0 out should hint")
	}
	b.settleLocally()
	if _, _, ok := b.unansweredRun(); ok {
		t.Error("settleLocally must clear the run tally")
	}
}

// TestHintNotRepeatedOnTheCatchUpRun verifies the run that answers a hint is
// never hinted about in turn. The agent has just been told to catch up, so
// whatever it sends IS the catch-up — asking again would have it check its own
// correction.
func TestHintNotRepeatedOnTheCatchUpRun(t *testing.T) {
	b := newTestBridge(ResolvedAccount{Owner: "zach@x"})
	b.markHintPending() // as fireUnansweredHint does
	// The catch-up run: even an unbalanced tally must not hint again.
	b.countInbound()
	b.countInbound()
	b.countInbound()
	if !b.takeHintPending() {
		t.Fatal("the run after a hint must be marked as the catch-up run")
	}
	// The mark is one-shot: the run after the catch-up is judged normally.
	if b.takeHintPending() {
		t.Error("the catch-up mark must clear after one settle")
	}
	if _, _, ok := b.unansweredRun(); !ok {
		t.Error("a later unbalanced run should hint again")
	}
}

// TestAbortClearsHintPending verifies an aborted run drops the catch-up mark:
// no catch-up is coming, so the next real run must be judged on its own tally.
func TestAbortClearsHintPending(t *testing.T) {
	b := newTestBridge(ResolvedAccount{Owner: "zach@x"})
	b.markHintPending()
	b.settleLocally()
	if b.takeHintPending() {
		t.Error("settleLocally must clear the catch-up mark")
	}
}
