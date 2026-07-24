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

func TestReplyDirective(t *testing.T) {
	room := NewBridge(ResolvedAccount{Rooms: []string{"team@muc.x"}, Owner: "zach@x"}, false)
	solo := NewBridge(ResolvedAccount{Owner: "zach@x"}, false) // 1:1, no room
	cases := []struct {
		name              string
		b                 *Bridge
		in                string
		wantDest, wantOut string
	}{
		{"dm alias → owner", room, "@dm here are headlines", "zach@x", "here are headlines"},
		{"owner alias", room, "@owner hi", "zach@x", "hi"},
		{"room alias → room jid", room, "@room broadcast", "team@muc.x", "broadcast"},
		{"to explicit user jid", room, "@to:alice@x hello", "alice@x", "hello"},
		{"to explicit room jid", room, "@to:team@muc.x hey", "team@muc.x", "hey"},
		{"newline after directive", room, "@dm\nline1\nline2", "zach@x", "line1\nline2"},
		{"case insensitive", room, "@DM yo", "zach@x", "yo"},
		{"leading whitespace", room, "  @room x", "team@muc.x", "x"},
		{"no directive", room, "just a reply", "", "just a reply"},
		{"mid-text not matched", room, "reply @dm inline", "", "reply @dm inline"},
		{"ignored in 1:1 mode", solo, "@dm hi", "", "@dm hi"},
	}
	for _, c := range cases {
		gotDest, gotOut := c.b.replyDirective(c.in)
		if gotDest != c.wantDest || gotOut != c.wantOut {
			t.Errorf("%s: replyDirective(%q) = (%q, %q), want (%q, %q)", c.name, c.in, gotDest, gotOut, c.wantDest, c.wantOut)
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
