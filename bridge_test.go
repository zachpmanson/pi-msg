package main

import "testing"

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
		{"bash with command", Event{"toolName": "bash", "args": map[string]any{"command": "npm test"}}, "running: npm test"},
		{"bash collapses whitespace", Event{"toolName": "bash", "args": map[string]any{"command": "go  build\n./..."}}, "running: go build ./..."},
		{"non-bash tool", Event{"toolName": "read_file"}, "running read_file"},
		{"missing name", Event{}, "running a tool…"},
		{"bash no command", Event{"toolName": "bash"}, "running bash"},
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
	seg := func(dest, body string) replySegment { return replySegment{dest: dest, body: body} }
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
		if len(gotSegs) != len(c.wantSegs) {
			t.Errorf("%s: got %d segs, want %d (%+v)", c.name, len(gotSegs), len(c.wantSegs), gotSegs)
			continue
		}
		for i := range gotSegs {
			if gotSegs[i] != c.wantSegs[i] {
				t.Errorf("%s: seg %d = %+v, want %+v", c.name, i, gotSegs[i], c.wantSegs[i])
			}
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
