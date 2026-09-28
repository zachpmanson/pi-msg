package main

import "testing"

func TestFixToolCallXML_Noop(t *testing.T) {
	input := "Hello, world!"
	got := FixToolCallXML(input)
	if got != input {
		t.Errorf("noop: got %q, want %q", got, input)
	}
}

func TestFixToolCallXML_AlreadyClean(t *testing.T) {
	input := `<tool_calls>
<invoke name="edit">
<parameter name="edits" string="false">[{"newText":"a","oldText":"b"}]</parameter>
</invoke>
</tool_calls>`
	got := FixToolCallXML(input)
	if got != input {
		t.Errorf("clean: got %q, want %q", got, input)
	}
}

func TestFixToolCallXML_BrokenClose(t *testing.T) {
	// Model omits closing tags, writes broken close with zero-width space
	zwsp := "\u200B"
	input := "text before\n" +
		"<tool_calls>\n" +
		"<invoke name=\"edit\">\n" +
		"<parameter name=\"edits\" string=\"false\">[{\"newText\":\"a\",\"oldText\":\"b\"}]</" + zwsp + "\n" +
		"<tool_calls>\n" +
		"<invoke name=\"edit\">\n" +
		"<parameter name=\"edits\" string=\"false\">[{\"newText\":\"c\",\"oldText\":\"d\"}]</parameter>\n" +
		"</invoke>\n" +
		"</tool_calls>"

	want := "text before\n" +
		"<tool_calls>\n" +
		"<invoke name=\"edit\">\n" +
		"<parameter name=\"edits\" string=\"false\">[{\"newText\":\"a\",\"oldText\":\"b\"}]</parameter></invoke></tool_calls>\n" +
		"<tool_calls>\n" +
		"<invoke name=\"edit\">\n" +
		"<parameter name=\"edits\" string=\"false\">[{\"newText\":\"c\",\"oldText\":\"d\"}]</parameter>\n" +
		"</invoke>\n" +
		"</tool_calls>"

	got := FixToolCallXML(input)
	if got != want {
		t.Errorf("broken close:\n got: %q\nwant: %q", got, want)
	}
}

func TestFixToolCallXML_WhitespaceClose(t *testing.T) {
	// Model writes ]</  \n<tool_calls> instead of proper close
	input := "prefix\n" +
		"<tool_calls>\n" +
		"<invoke name=\"edit\">\n" +
		"<parameter name=\"edits\" string=\"false\">[{\"newText\":\"a\",\"oldText\":\"b\"}]</  \n" +
		"<tool_calls>\n" +
		"<invoke name=\"edit\">\n" +
		"<parameter name=\"edits\" string=\"false\">[{\"newText\":\"c\",\"oldText\":\"d\"}]</parameter>\n" +
		"</invoke>\n" +
		"</tool_calls>"

	want := "prefix\n" +
		"<tool_calls>\n" +
		"<invoke name=\"edit\">\n" +
		"<parameter name=\"edits\" string=\"false\">[{\"newText\":\"a\",\"oldText\":\"b\"}]</parameter></invoke></tool_calls>\n" +
		"<tool_calls>\n" +
		"<invoke name=\"edit\">\n" +
		"<parameter name=\"edits\" string=\"false\">[{\"newText\":\"c\",\"oldText\":\"d\"}]</parameter>\n" +
		"</invoke>\n" +
		"</tool_calls>"

	got := FixToolCallXML(input)
	if got != want {
		t.Errorf("whitespace close:\n got: %q\nwant: %q", got, want)
	}
}

func TestFixToolCallXML_UnclosedTag(t *testing.T) {
	// Model writes </tagname<tool_calls> (no close, no space)
	input := "prefix\n" +
		"<tool_calls>\n" +
		"<invoke name=\"edit\">\n" +
		"<parameter name=\"edits\" string=\"false\">[{\"newText\":\"a\",\"oldText\":\"b\"}]</tagname\n" +
		"<tool_calls>\n" +
		"<invoke name=\"edit\">\n" +
		"<parameter name=\"edits\" string=\"false\">[{\"newText\":\"c\",\"oldText\":\"d\"}]</parameter>\n" +
		"</invoke>\n" +
		"</tool_calls>"

	want := "prefix\n" +
		"<tool_calls>\n" +
		"<invoke name=\"edit\">\n" +
		"<parameter name=\"edits\" string=\"false\">[{\"newText\":\"a\",\"oldText\":\"b\"}]</parameter></invoke></tool_calls>\n" +
		"<tool_calls>\n" +
		"<invoke name=\"edit\">\n" +
		"<parameter name=\"edits\" string=\"false\">[{\"newText\":\"c\",\"oldText\":\"d\"}]</parameter>\n" +
		"</invoke>\n" +
		"</tool_calls>"

	got := FixToolCallXML(input)
	if got != want {
		t.Errorf("unclosed tag:\n got: %q\nwant: %q", got, want)
	}
}
