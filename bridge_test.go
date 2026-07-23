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
