package pimsg

import (
	"bytes"
	"encoding/json"
	"strings"
	"sync"
	"testing"
)

// markerTestBridge is a 1:1-mode bridge wired for the deferred read-marker
// tests: a fire-and-forget RPC sink capturing the prompts handed to pi, a
// durable inbox, and a markerSender that records every XEP-0333 "displayed"
// marker the bridge decides to send (so the test needs no live XMPP session).
func markerTestBridge(t *testing.T, acct ResolvedAccount) (*Bridge, *bytes.Buffer, *[]string) {
	t.Helper()
	b := newTestBridge(acct)
	b.ctx = t.Context()
	buf := &bytes.Buffer{}
	b.rpc = &RPCClient{stdin: &nopClose{buf: buf}, mu: sync.Mutex{}}
	b.inbox = newInbox(t.TempDir()+"/acct.inbox.jsonl", nil)
	sent := &[]string{}
	b.markerSender = func(to, id string) error {
		*sent = append(*sent, to+"|"+id)
		return nil
	}
	return b, buf, sent
}

// lastPromptText decodes the most recent prompt command written to pi's stdin.
func lastPromptText(t *testing.T, buf *bytes.Buffer) string {
	t.Helper()
	lines := strings.Split(strings.TrimSpace(buf.String()), "\n")
	if len(lines) == 0 || lines[len(lines)-1] == "" {
		t.Fatalf("no prompt was written to pi: %q", buf.String())
	}
	var cmd map[string]any
	if err := json.Unmarshal([]byte(lines[len(lines)-1]), &cmd); err != nil {
		t.Fatalf("prompt is not a JSON command: %v (%q)", err, lines[len(lines)-1])
	}
	msg, _ := cmd["message"].(string)
	if msg == "" {
		t.Fatalf("prompt command carried no message: %q", lines[len(lines)-1])
	}
	return msg
}

// userStart is the RPC event pi emits when it begins the user message whose
// text is content. Pi always sends a user message as a text-block array (both a
// fresh prompt and a queued steer), so the helper mirrors that wire shape.
func userStart(content string) Event {
	return Event{"type": "message_start", "message": map[string]any{
		"role":    "user",
		"content": []any{map[string]any{"type": "text", "text": content}},
	}}
}

// A markable owner message that is merely accepted (and possibly queued by pi)
// must not be acknowledged: the old path sent the marker at accept time, which
// read as "the agent has seen this" before pi had started it at all (#73).
func TestNoMarkerWhileMessageIsOnlyQueued(t *testing.T) {
	acct := ResolvedAccount{Owner: "zach@x", Name: "t"}
	b, buf, sent := markerTestBridge(t, acct)

	b.onInbound(InboundMessage{ID: "m1", From: "zach@x/phone", Body: "hello", Direct: true, FromOwner: true, Markable: true})
	if !strings.Contains(buf.String(), "hello") {
		t.Fatalf("no prompt went out: %q", buf.String())
	}
	if len(*sent) != 0 {
		t.Fatalf("a marker was sent on accept: %v", *sent)
	}
	// A run starts but pi has not begun the user message yet.
	b.handleRPCEvent(Event{"type": "agent_start"})
	if len(*sent) != 0 {
		t.Fatalf("a marker was sent before the message started: %v", *sent)
	}
}

// The marker fires exactly when pi starts the matching user message, and points
// at the originating resource and stanza id.
func TestMarkerAfterMatchingUserMessageStart(t *testing.T) {
	acct := ResolvedAccount{Owner: "zach@x", Name: "t"}
	b, buf, sent := markerTestBridge(t, acct)

	b.onInbound(InboundMessage{ID: "m1", From: "zach@x/phone", Body: "hello", Direct: true, FromOwner: true, Markable: true})
	prompt := lastPromptText(t, buf)

	b.handleRPCEvent(userStart(prompt))
	if len(*sent) != 1 || (*sent)[0] != "zach@x/phone|m1" {
		t.Fatalf("markers sent = %v, want [zach@x/phone|m1]", *sent)
	}
	// The marker is consumed: a repeated start for the same text must not send
	// a second one.
	b.handleRPCEvent(userStart(prompt))
	if len(*sent) != 1 {
		t.Fatalf("the marker was sent twice: %v", *sent)
	}
}

// An unrelated user start (another run, another message) must never acknowledge
// a pending marker, and an assistant message start is never a user message.
func TestUnrelatedStartDoesNotTriggerMarker(t *testing.T) {
	acct := ResolvedAccount{Owner: "zach@x", Name: "t"}
	b, buf, sent := markerTestBridge(t, acct)

	b.onInbound(InboundMessage{ID: "m1", From: "zach@x/phone", Body: "hello", Direct: true, FromOwner: true, Markable: true})
	prompt := lastPromptText(t, buf)

	b.handleRPCEvent(userStart("a different owner message entirely"))
	b.handleRPCEvent(Event{"type": "message_start", "message": map[string]any{"role": "assistant", "content": prompt}})
	if len(*sent) != 0 {
		t.Fatalf("an unrelated start fired the marker: %v", *sent)
	}
	// The real one still fires.
	b.handleRPCEvent(userStart(prompt))
	if len(*sent) != 1 {
		t.Fatalf("the marker never fired for the matching start: %v", *sent)
	}
}

// A run that ends before the message starts (a steer pi never yielded) leaves
// the marker pending: it must survive the settle and fire in the run that
// finally reads the message.
func TestRunEndingBeforeMessageStartLeavesMarkerPending(t *testing.T) {
	acct := ResolvedAccount{Owner: "zach@x", Name: "t"}
	b, buf, sent := markerTestBridge(t, acct)

	b.onInbound(InboundMessage{ID: "m1", From: "zach@x/phone", Body: "hello", Direct: true, FromOwner: true, Markable: true})
	prompt := lastPromptText(t, buf)

	b.handleRPCEvent(Event{"type": "agent_start"})
	b.handleRPCEvent(Event{"type": "agent_settled"})
	if len(*sent) != 0 {
		t.Fatalf("the settle sent a marker for a message that never started: %v", *sent)
	}
	if len(b.pendingMarkers) != 1 {
		t.Fatalf("pending markers = %d after the settle, want 1", len(b.pendingMarkers))
	}
	// The next run finally injects the steer and pi starts the message.
	b.handleRPCEvent(Event{"type": "agent_start"})
	b.handleRPCEvent(userStart(prompt))
	if len(*sent) != 1 || (*sent)[0] != "zach@x/phone|m1" {
		t.Fatalf("markers sent = %v, want [zach@x/phone|m1]", *sent)
	}
	if len(b.pendingMarkers) != 0 {
		t.Fatalf("pending markers = %d after firing, want 0", len(b.pendingMarkers))
	}
}

// A message with no markable flag (the common case, and every room message)
// never registers a marker, so nothing is deferred and nothing is sent.
func TestUnmarkableMessageRegistersNoMarker(t *testing.T) {
	acct := ResolvedAccount{Owner: "zach@x", Name: "t"}
	b, buf, sent := markerTestBridge(t, acct)

	b.onInbound(InboundMessage{ID: "m1", From: "zach@x/phone", Body: "hello", Direct: true, FromOwner: true, Markable: false})
	prompt := lastPromptText(t, buf)
	b.handleRPCEvent(userStart(prompt))

	if len(*sent) != 0 {
		t.Fatalf("an unmarkable message produced a marker: %v", *sent)
	}
	if len(b.pendingMarkers) != 0 {
		t.Fatalf("an unmarkable message queued a pending marker: %v", b.pendingMarkers)
	}
}

// A markable message with no usable stanza id cannot be acknowledged on the
// wire, so no marker is queued (the previous policy, preserved).
func TestMarkableMessageWithoutStanzaIDRegistersNoMarker(t *testing.T) {
	acct := ResolvedAccount{Owner: "zach@x", Name: "t"}
	b, buf, _ := markerTestBridge(t, acct)

	b.onInbound(InboundMessage{ID: "", From: "zach@x/phone", Body: "hello", Direct: true, FromOwner: true, Markable: true})
	prompt := lastPromptText(t, buf)
	b.handleRPCEvent(userStart(prompt))

	if len(b.pendingMarkers) != 0 {
		t.Fatalf("a message with no stanza id queued a marker: %v", b.pendingMarkers)
	}
}
