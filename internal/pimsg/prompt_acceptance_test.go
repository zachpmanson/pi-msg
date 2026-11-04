package pimsg

import (
	"errors"
	"io"
	"path/filepath"
	"strings"
	"testing"
)

// Fake Pi deliberately withholds agent_start and prompt acceptance. Deliver
// acceptance separately, as the production RPC event loop does (not Request).
func acceptPromptResponses(b *Bridge, success bool) {
	b.mu.Lock()
	ids := make([]string, 0, len(b.pendingPrompts))
	for id := range b.pendingPrompts {
		ids = append(ids, id)
	}
	b.mu.Unlock()
	for _, id := range ids {
		b.handleRPCEvent(Event{"type": "response", "command": "prompt", "id": id, "success": success, "error": "preflight rejected"})
	}
}

func pendingPromptCount(b *Bridge) int {
	b.mu.Lock()
	defer b.mu.Unlock()
	return len(b.pendingPrompts)
}

func TestNextRefusesPendingPromptAcceptance(t *testing.T) {
	for _, source := range []string{"canonical", "commentary", "reaction"} {
		t.Run(source, func(t *testing.T) {
			f := nextSetup(t)
			switch source {
			case "canonical":
				f.command("ordinary work", f.b.acct.Owner)
			case "commentary":
				f.b.dispatchCommentary("peer work", "peer", "team@muc.x", "peer@x", "", "", "", nil)
			case "reaction":
				f.b.handleReaction(InboundMessage{Direct: true, FromOwner: true, Reactions: []string{"✅"}})
			}
			awaitNext(t, "pending prompt received by Pi", func() bool {
				return strings.Contains(fileText(filepath.Join(f.dir, "commands")), `"type":"prompt"`)
			})
			if pendingPromptCount(f.b) != 1 || f.b.streaming() {
				t.Fatal("pending acceptance must not invent actual streaming")
			}
			f.command("/next", f.b.acct.Owner)
			if len(f.jobs(t)) != 0 || strings.Contains(fileText(filepath.Join(f.dir, "commands")), `"type":"new_session"`) {
				t.Fatal("/next reset while ordinary prompt preflight was pending")
			}
			acceptPromptResponses(f.b, true) // handled without an LLM turn, e.g. an extension command
			f.command("/next", f.b.acct.Owner)
			if len(f.jobs(t)) != 1 {
				t.Fatal("accepted non-streaming prompt left false activity")
			}
		})
	}
}

func submitSyntheticFixture(t *testing.T, f *nextFixture, kind string) {
	t.Helper()
	switch kind {
	case "resume":
		f.b.fireResumeTurn()
	case "recovery":
		f.b.fireSendRecovery()
	case "unanswered":
		f.b.fireUnansweredHint(2, 1, nil)
	case "heartbeat":
		f.b.fireHeartbeat("process report")
	case "reply-timeout":
		w := testReplyWatch(t, f.b, f.b.acct.Owner, "first", false)
		f.b.expireReplyWatch(w)
		f.b.flushReplyWatches()
	}
}

func TestNextRejectedSyntheticPromptClearsAcceptance(t *testing.T) {
	for _, kind := range []string{"resume", "recovery", "unanswered", "heartbeat", "reply-timeout"} {
		t.Run(kind, func(t *testing.T) {
			f := nextSetup(t)
			submitSyntheticFixture(t, f, kind)
			if pendingPromptCount(f.b) != 1 || f.b.streaming() {
				t.Fatal("synthetic preflight must be pending, not streaming")
			}
			acceptPromptResponses(f.b, false) // missing auth/compaction can reject without lifecycle events
			if pendingPromptCount(f.b) != 0 || f.b.streaming() || f.b.heartbeatRun || f.b.volunteered || f.b.hintPending {
				t.Fatal("rejected prompt left false activity or poisoned one-shot flags")
			}
			f.command("&", f.b.acct.Owner)
			if len(f.jobs(t)) != 1 {
				t.Fatal("/next remained blocked after prompt rejection")
			}
		})
	}
}

type nextFailingWriter struct{ io.WriteCloser }

func (nextFailingWriter) Write([]byte) (int, error) { return 0, errors.New("fixture write failure") }

func TestNextSyntheticWriteFailureClearsAcceptance(t *testing.T) {
	for _, kind := range []string{"resume", "recovery", "unanswered", "heartbeat", "reply-timeout"} {
		t.Run(kind, func(t *testing.T) {
			f := nextSetup(t)
			original := f.b.rpc.stdin
			f.b.rpc.stdin = nextFailingWriter{original}
			submitSyntheticFixture(t, f, kind)
			f.b.rpc.stdin = original
			if pendingPromptCount(f.b) != 0 || f.b.streaming() || f.b.heartbeatRun || f.b.volunteered || f.b.hintPending {
				t.Fatal("failed pipe write left false activity or poisoned one-shot flags")
			}
			f.command("/next", f.b.acct.Owner)
			if len(f.jobs(t)) != 1 {
				t.Fatal("/next remained blocked after synchronous prompt write failure")
			}
		})
	}
}

func TestPromptAcceptancePreservesStreamingAndIgnoresUnrelatedResponse(t *testing.T) {
	f := nextSetup(t)
	f.b.submitPrompt("work", "")
	f.b.handleRPCEvent(Event{"type": "response", "id": "unrelated", "success": false})
	if pendingPromptCount(f.b) != 1 {
		t.Fatal("unrelated response released prompt activity")
	}
	f.b.handleRPCEvent(Event{"type": "agent_start"})
	acceptPromptResponses(f.b, true)
	if pendingPromptCount(f.b) != 0 || !f.b.streaming() {
		t.Fatal("acceptance must not overwrite real lifecycle activity")
	}
	f.command("/next", f.b.acct.Owner)
	if len(f.jobs(t)) != 0 {
		t.Fatal("/next reset an active accepted prompt")
	}
}
