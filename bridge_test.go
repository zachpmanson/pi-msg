package main

import (
	"encoding/xml"
	"reflect"
	"strings"
	"testing"
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
	}
	for _, c := range cases {
		name, arg := splitCommand(c.in)
		if name != c.name || arg != c.arg {
			t.Errorf("splitCommand(%q) = (%q,%q), want (%q,%q)", c.in, name, arg, c.name, c.arg)
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
