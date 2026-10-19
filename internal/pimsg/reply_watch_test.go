package pimsg

import (
	"context"
	"encoding/json"
	"strings"
	"testing"
	"time"
)

func TestParseReplyTimeout(t *testing.T) {
	for _, raw := range []string{"0", "-1", "null", `"2"`, "true", "{}", "1e1000", "1e20", "1e-12", "NaN", "Infinity"} {
		if _, _, err := parseReplyTimeout(json.RawMessage(raw)); err == nil {
			t.Errorf("accepted %s", raw)
		}
	}
	if d, n, err := parseReplyTimeout(nil); err != nil || d != 0 || n != 0 {
		t.Fatalf("omission: %v %v %v", d, n, err)
	}
	if d, n, err := parseReplyTimeout(json.RawMessage("0.25")); err != nil || d != 250*time.Millisecond || n != .25 {
		t.Fatalf("fraction: %v %v %v", d, n, err)
	}
}

// Long real timers are stopped immediately; tests explicitly drive expiry.
func testReplyWatch(t *testing.T, b *Bridge, to, id string, room bool) *replyWatch {
	t.Helper()
	b.replyWatchMu.Lock()
	b.armReplyWatch(to, id, "question?", room, time.Now(), time.Hour, 3600)
	w := b.replyWatches[bareJid(to)]
	w.timer.Stop()
	b.replyWatchMu.Unlock()
	t.Cleanup(b.stopReplyWatches)
	return w
}

func TestReplyWatchOnlyQualifyingRepliesCancel(t *testing.T) {
	for _, tt := range []struct {
		name   string
		room   bool
		m      InboundMessage
		cancel bool
	}{
		{name: "peer resource", m: InboundMessage{From: "zach@x/phone", Body: "answer"}, cancel: true},
		{name: "other peer", m: InboundMessage{From: "other@x", Body: "answer"}},
		{name: "empty receipt", m: InboundMessage{From: "zach@x"}},
		{name: "own", m: InboundMessage{From: "zach@x", Body: "echo", Own: true}},
		{name: "delayed", m: InboundMessage{From: "zach@x", Body: "old", Stamp: time.Now()}},
		{name: "room chatter", room: true, m: InboundMessage{Room: "team@muc.x", Body: "answer"}},
		{name: "room broadcast", room: true, m: InboundMessage{Room: "team@muc.x", Body: "@everyone answer"}},
		{name: "free broadcast", room: true, m: InboundMessage{Room: "team@muc.x", Body: "@free answer"}},
		{name: "wrong thread", room: true, m: InboundMessage{Room: "team@muc.x", Body: "answer", ReplyToID: "other"}},
		{name: "watched thread", room: true, m: InboundMessage{Room: "team@muc.x", Body: "answer", ReplyToID: "sent-1"}, cancel: true},
	} {
		t.Run(tt.name, func(t *testing.T) {
			b := newTestBridge(ResolvedAccount{Owner: "zach@x", Rooms: []string{"team@muc.x"}})
			to := "zach@x"
			if tt.room {
				to = "team@muc.x"
			}
			testReplyWatch(t, b, to, "sent-1", tt.room)
			b.observeReplyMessage(tt.m)
			if got := b.replyWatches[to] == nil; got != tt.cancel {
				t.Fatalf("cancelled=%v want %v", got, tt.cancel)
			}
		})
	}
}

func TestReplyWatchQueuedExpiryCancellationAndReplacement(t *testing.T) {
	b, _, rpc := messageTransportBridge(t, false)
	b.setStreaming(true)
	old := testReplyWatch(t, b, "zach@x", "old", false)
	b.expireReplyWatch(old)
	<-b.replyWatchWake
	b.flushReplyWatches()
	if strings.Contains(rpc.String(), "reply-timeout:") {
		t.Fatal("prompted while busy")
	}
	replacement := testReplyWatch(t, b, "zach@x", "new", false)
	b.expireReplyWatch(old)
	if replacement.expired {
		t.Fatal("stale expiry affected replacement")
	}
	b.expireReplyWatch(replacement)
	b.observeReplyMessage(InboundMessage{From: "zach@x", Body: "answer"})
	b.setStreaming(false)
	b.flushReplyWatches()
	if strings.Contains(rpc.String(), "reply-timeout:") {
		t.Fatal("cancelled queued timeout delivered")
	}
}

func TestReplyWatchInterruptDrainsQueuedExpiry(t *testing.T) {
	b, _, rpc := messageTransportBridge(t, false)
	b.setStreaming(true)
	w := testReplyWatch(t, b, "zach@x", "first", false)
	b.expireReplyWatch(w)
	<-b.replyWatchWake
	b.flushReplyWatches() // consume original wake while busy
	if !b.handleCommand("!") {
		t.Fatal("interrupt not handled")
	}
	select {
	case <-b.replyWatchWake:
	default:
		t.Fatal("local interrupt did not reschedule expiry")
	}
	b.flushReplyWatches()
	if n := strings.Count(rpc.String(), "reply-timeout:"); n != 1 {
		t.Fatalf("timeout count=%d: %s", n, rpc.String())
	}
	if !b.heartbeatRun {
		t.Fatal("timeout not marked synthetic")
	}
	// Aborting the synthetic run must not leave heartbeatRun blocking later watches.
	if !b.handleCommand("!") {
		t.Fatal("second interrupt not handled")
	}
	if b.heartbeatRun || b.volunteered || b.streaming() {
		t.Fatal("terminated run flags retained")
	}
	second := testReplyWatch(t, b, "zach@x", "second", false)
	b.expireReplyWatch(second)
	b.flushReplyWatches()
	if n := strings.Count(rpc.String(), "reply-timeout:"); n != 2 {
		t.Fatalf("later timeout starved: %s", rpc.String())
	}
	b.flushReplyWatches()
	if n := strings.Count(rpc.String(), "reply-timeout:"); n != 2 {
		t.Fatalf("duplicate timeout: %d", n)
	}
}

func TestReplyWatchFreshSessionSeed(t *testing.T) {
	b, _, rpc := messageTransportBridge(t, false)
	b.messagingSeeded = false // /new resets this after completing its transition
	w := testReplyWatch(t, b, "zach@x", "seed", false)
	b.expireReplyWatch(w)
	b.flushReplyWatches()
	if !strings.Contains(rpc.String(), "pi-msg: messaging:") || !b.messagingSeeded {
		t.Fatalf("missing new-session contract: %s", rpc.String())
	}
}

func TestReplyWatchRelayValidationBeforeSend(t *testing.T) {
	for _, raw := range []string{"0", "-2", "null", "1e20", "1e-12"} {
		t.Run(raw, func(t *testing.T) {
			b, wire, _ := messageTransportBridge(t, false)
			b.handleSendMessageRelay("r", "zach@x", "hello", "", json.RawMessage(raw))
			if wire.Len() != 0 || len(b.replyWatches) != 0 {
				t.Fatal("invalid timeout sent or armed")
			}
		})
	}
	for _, to := range []string{"zach@x", "noop"} {
		t.Run(to, func(t *testing.T) {
			b, _, rpc := messageTransportBridge(t, false)
			b.handleSendMessageRelay("r", to, "hello", "", json.RawMessage("3600"))
			t.Cleanup(b.stopReplyWatches)
			if to == "noop" {
				if len(b.replyWatches) != 0 {
					t.Fatal("noop armed")
				}
				return
			}
			if len(b.replyWatches) != 1 || !strings.Contains(rpc.String(), "sent:") {
				t.Fatalf("watch not armed on success: %s", rpc.String())
			}
		})
	}
}

func TestReplyWatchTransportObservation(t *testing.T) {
	b := newTestBridge(ResolvedAccount{JID: "pi@x", Owner: "owner@x", Nick: "pi", Rooms: []string{"team@muc.x"}})
	b.xmpp.onReplyMessage = b.observeReplyMessage
	// Observation precedes owner-only DM dispatch, allowing a watched non-owner.
	testReplyWatch(t, b, "peer@x", "dm", false)
	b.xmpp.dispatchDirect(incomingMsg{typ: "chat", from: "peer@x/phone", body: "answer", id: "fresh"})
	if len(b.replyWatches) != 0 {
		t.Fatal("non-owner reply not observed")
	}
	testReplyWatch(t, b, "peer@x", "dm2", false)
	b.xmpp.dispatchDirect(incomingMsg{typ: "chat", from: "peer@x/phone", body: "answer", id: "fresh"})
	if len(b.replyWatches) != 1 {
		t.Fatal("duplicate cancelled new watch")
	}
	for _, body := range []string{"@pi answer", "pi: answer"} {
		testReplyWatch(t, b, "team@muc.x", "room", true)
		b.observeReplyMessage(InboundMessage{Room: "team@muc.x", From: "team@muc.x/alice", Body: body})
		if b.replyWatches["team@muc.x"] != nil {
			t.Fatalf("direct address %q did not cancel", body)
		}
	}
}

func TestReplyWatchNotArmedOnFailureOrOmission(t *testing.T) {
	for _, mode := range []string{"omitted", "offline", "denied"} {
		t.Run(mode, func(t *testing.T) {
			b, _, _ := messageTransportBridge(t, false)
			raw := json.RawMessage("3600")
			to := "zach@x"
			switch mode {
			case "omitted":
				raw = nil
			case "offline":
				b.xmpp.online = false
			case "denied":
				to = "unknown@x"
			}
			b.handleSendMessageRelay("r", to, "hello", "", raw)
			t.Cleanup(b.stopReplyWatches)
			if len(b.replyWatches) != 0 {
				t.Fatal("unexpected watch")
			}
		})
	}
}

func TestReplyWatchShutdownDiscardsQueuedExpiry(t *testing.T) {
	b, _, rpc := messageTransportBridge(t, false)
	w := testReplyWatch(t, b, "zach@x", "gone", false)
	b.expireReplyWatch(w)
	b.stopReplyWatches()
	b.expireReplyWatch(w)
	b.flushReplyWatches()
	if len(b.replyWatches) != 0 || strings.Contains(rpc.String(), "reply-timeout:") {
		t.Fatal("shutdown left reminder")
	}
}

func TestReplyWatchSessionTransitionSerializesPrompt(t *testing.T) {
	b, _, rpc := messageTransportBridge(t, false)
	w := testReplyWatch(t, b, "zach@x", "transition", false)
	b.expireReplyWatch(w)
	b.sessionTransitionMu.Lock()
	done := make(chan struct{})
	go func() { b.flushReplyWatches(); close(done) }()
	// While holding the session transition lock, a fresh-session contract reset
	// must happen before the timeout prompt, regardless of goroutine scheduling.
	b.messagingSeeded = false
	b.sessionTransitionMu.Unlock()
	select {
	case <-done:
	case <-time.After(time.Second):
		t.Fatal("flush blocked after transition")
	}
	if !b.messagingSeeded || !strings.Contains(rpc.String(), "pi-msg: messaging:") {
		t.Fatal("timeout raced ahead of session seed reset")
	}
}

func TestReplyWatchAbortCommandsDrainQueuedExpiry(t *testing.T) {
	for _, command := range []string{"/abort", "/stop", "!abort"} {
		t.Run(command, func(t *testing.T) {
			b, wire, rpc := messageTransportBridge(t, false)
			// Fail the optional clear_queue request immediately; abort must still
			// locally settle and wake pending watches even without an RPC response.
			ctx, cancel := context.WithCancel(context.Background())
			cancel()
			b.ctx = ctx
			b.rpc.pending = make(map[string]chan Event)
			b.setStreaming(true)
			b.heartbeatRun = true
			w := testReplyWatch(t, b, "zach@x", "abort", false)
			b.expireReplyWatch(w)
			<-b.replyWatchWake
			b.flushReplyWatches()
			if !b.handleCommand(command) {
				t.Fatal("command not handled")
			}
			if b.streaming() || b.heartbeatRun || b.volunteered {
				t.Fatal("abort retained run flags")
			}
			select {
			case <-b.replyWatchWake:
			default:
				t.Fatal("abort did not wake pending expiry")
			}
			sent := strings.Count(wire.String(), "<message")
			b.flushReplyWatches()
			if !strings.Contains(rpc.String(), "reply-timeout:") {
				t.Fatalf("expiry starved after abort: %s", rpc.String())
			}
			if strings.Count(wire.String(), "<message") != sent {
				t.Fatal("expiry sent an automatic chat message")
			}
		})
	}
}
