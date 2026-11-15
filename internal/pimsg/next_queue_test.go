package pimsg

import (
	"context"
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"
)

// Observations use the real reader/correlation/event path, not timing sleeps.
func (f *nextFixture) observe(t *testing.T) {
	t.Helper()
	done := make(chan error, 1)
	go func() { _, err := f.b.rpc.observedState(context.Background()); done <- err }()
	timer := time.NewTimer(5 * time.Second)
	defer timer.Stop()
	for {
		select {
		case err := <-done:
			if err != nil {
				t.Fatal(err)
			}
			return
		case ev := <-f.b.rpc.Events():
			f.b.handleRPCEvent(ev)
		case <-timer.C:
			t.Fatal("ordered observation blocked")
		}
	}
}
func (f *nextFixture) incoming(body, id string) {
	f.b.onInbound(InboundMessage{Body: body, ID: id, From: f.b.acct.Owner + "/phone", Direct: true, FromOwner: true, Markable: true})
}
func (f *nextFixture) boundary(t *testing.T) *nextBoundaryRecord {
	t.Helper()
	r, err := loadNextBoundary(f.b.nextPath())
	if err != nil {
		t.Fatal(err)
	}
	return r
}

func TestQueuedNextBusyFIFOAndIdentity(t *testing.T) {
	f := nextSetup(t)
	touchNext(t, filepath.Join(f.dir, "prompt-normal"))
	f.incoming("old work", "old")
	f.observe(t)
	if !f.b.streaming() {
		t.Fatal("fake did not emit normal agent_start")
	}
	oldDest := "old-room@muc.x"
	f.b.setTurnDest(oldDest, false)
	before := fileText(filepath.Join(f.dir, "commands"))
	f.incoming("/next", "next") // no executor/RPC round trip in admission
	r := f.boundary(t)
	if r == nil || r.Phase != nextBoundaryWaiting || !strings.Contains(f.wire.String(), r.ID) {
		t.Fatal("missing durable queued ACK")
	}
	if f.b.currentTurnDest() != oldDest || before != fileText(filepath.Join(f.dir, "commands")) {
		t.Fatal("admission changed old destination or performed RPC")
	}
	f.incoming("one held", "one")
	f.incoming("two held", "two")
	for _, alias := range []string{"!next", "&", "/next"} {
		f.incoming(alias, alias)
	}
	if f.boundary(t).ID != r.ID || len(f.b.inbox.protectedPending()) != 2 {
		t.Fatal("duplicate boundary or lost held input")
	}
	for _, control := range []string{"/abort", "/stop", "!", "/compact", "/session"} {
		f.incoming(control, control)
	}
	if len(f.b.inbox.protectedPending()) != 2 || f.b.currentTurnDest() != oldDest {
		t.Fatal("native control redirected old turn or changed queue")
	}
	f.step()
	if len(f.jobs(t)) != 0 || strings.Contains(fileText(filepath.Join(f.dir, "commands")), "one held") {
		t.Fatal("snapshot/reset/delivery while busy")
	}
	// Prove an old settle, even an aged one, cannot acknowledge held input.
	f.b.inbox.mu.Lock()
	for i := range f.b.inbox.entries {
		if f.b.inbox.entries[i].protected() {
			f.b.inbox.entries[i].At = time.Now().Add(-time.Hour)
		}
	}
	f.b.inbox.mu.Unlock()
	f.b.setReplied(true)
	f.b.mu.Lock()
	f.b.runDeliveries = 1
	f.b.mu.Unlock()
	f.b.noteRunActivity() // prove the old ordinary prompt was actually consumed
	f.b.rpc.Send(map[string]any{"type": "test_settle"})
	f.observe(t)
	if len(f.b.inbox.protectedPending()) != 2 {
		t.Fatal("old settle acked held input")
	}
	f.step()
	f.observe(t) // drain first target prompt acceptance
	f.step()     // only now may the successor enter asynchronous preflight
	f.observe(t)
	commands := fileText(filepath.Join(f.dir, "commands"))
	reset := strings.Index(commands, `"type":"new_session"`)
	one, two := strings.Index(commands, "one held"), strings.Index(commands, "two held")
	if reset < 0 || one < reset || two < one || strings.Count(commands, "\nreset\n") != 1 || len(f.jobs(t)) != 1 {
		t.Fatalf("not one-reset FIFO in fresh primary: %s", commands)
	}
	if strings.Count(commands, "prompt-session-1") != 2 {
		t.Fatalf("held prompts not both fresh: %s", commands)
	}
	r = f.boundary(t)
	if r.FreshSessionID != "id:s1" || loadSessionState("test") != f.sources[1] {
		t.Fatal("target/resume proof not durable")
	}
	for _, e := range f.b.inbox.protectedPending() {
		if e.TargetSessionID != r.FreshSessionID || e.Key == "" {
			t.Fatal("missing exact-target tags/key")
		}
	}
	// A buffered old automatic relay cannot change counts or wake the new context.
	prior := fileText(filepath.Join(f.dir, "commands"))
	f.b.handleToolRelay("late", `{"action":"subagent_count","count":9,"sessionIdentity":"id:s0"}`)
	f.b.handleToolRelay("late-hb", `{"action":"process_heartbeat","sessionIdentity":"id:s0","processes":[{"id":"late"}]}`)
	f.b.mu.Lock()
	count := f.b.bgSubagents
	f.b.mu.Unlock()
	if count != 0 {
		t.Fatal("old relay changed fresh counts")
	}
	f.observe(t)
	after := fileText(filepath.Join(f.dir, "commands"))
	if strings.Count(after, `"type":"prompt"`) != strings.Count(prior, `"type":"prompt"`) {
		t.Fatal("stale heartbeat woke target")
	}
	// Displayed evidence is correlated to fresh user start, never admission.
	if len(f.b.pendingMarkers) != 3 {
		t.Fatalf("marker registration count %d", len(f.b.pendingMarkers))
	}
}

func TestQueuedNextFailureRecoveryAndRestart(t *testing.T) {
	for _, kind := range []string{"reset-error", "reset-cancel"} {
		t.Run(kind, func(t *testing.T) {
			f := nextSetup(t)
			touchNext(t, filepath.Join(f.dir, kind))
			f.incoming("/next", "next")
			f.incoming("retained", "held")
			f.step()
			r := f.boundary(t)
			if r.Phase != nextBoundaryBlocked || len(f.b.inbox.protectedPending()) != 1 {
				t.Fatal("failed reset did not block/retain")
			}
			if strings.Contains(fileText(filepath.Join(f.dir, "commands")), "retained") {
				t.Fatal("failed reset submitted held input")
			}
			jobs := f.jobs(t)
			if len(jobs) != 1 {
				t.Fatal("missing prepared job")
			}
			if _, err := os.Stat(filepath.Join(jobs[0], "reset-authorized")); !os.IsNotExist(err) {
				t.Fatal("failed reset authorized effects")
			}
			before := strings.Count(fileText(filepath.Join(f.dir, "commands")), `"type":"new_session"`)
			f.incoming("!next", "duplicate")
			f.step()
			if strings.Count(fileText(filepath.Join(f.dir, "commands")), `"type":"new_session"`) != before {
				t.Fatal("automatic retry")
			}
			if err := os.Remove(filepath.Join(f.dir, kind)); err != nil {
				t.Fatal(err)
			}
			f.incoming("/new", "recover")
			f.step()
			if f.boundary(t).FreshSessionID != "id:s1" || len(f.jobs(t)) != 1 {
				t.Fatal("lossless /new did not supersede without new job")
			}
			f.incoming("/new", "retarget")
			if !strings.Contains(f.wire.String(), "already target-bound") {
				t.Fatal("retarget refusal missing")
			}
			// Exact target restart resumes at least once, but never resets or reopens gate.
			f.b.next.record = nil
			f.b.next.submitted = nil
			f.b.inbox = newInbox(inboxPath("test"), f.b.log)
			if err := f.b.loadNextQueue(); err != nil {
				t.Fatal(err)
			}
			f.b.mu.Lock()
			f.b.pendingPrompts = nil
			f.b.protectedPrompts = nil
			f.b.mu.Unlock()
			before = strings.Count(fileText(filepath.Join(f.dir, "commands")), `"type":"new_session"`)
			f.step()
			f.observe(t)
			if strings.Count(fileText(filepath.Join(f.dir, "commands")), `"type":"new_session"`) != before || strings.Count(fileText(filepath.Join(f.dir, "commands")), "retained") != 2 {
				t.Fatal("target resume reset/replayed effects or missed at-least-once input")
			}
		})
	}
}

func TestQueuedNextStartupBlocksPreResetAndOversize(t *testing.T) {
	f := nextSetup(t)
	f.b.setStreaming(true)
	f.incoming("/next", "next")
	f.incoming("held", "held")
	f.b.next.record = nil
	if err := f.b.loadNextQueue(); err != nil {
		t.Fatal(err)
	}
	f.b.setStreaming(false)
	f.step()
	if f.boundary(t).Phase != nextBoundaryBlocked || len(f.jobs(t)) != 0 {
		t.Fatal("startup idle incorrectly completed pre-reset boundary")
	}
	f.incoming(strings.Repeat("x", maxInboxRecordBytes), "oversize")
	if len(f.b.inbox.protectedPending()) != 1 || !strings.Contains(f.wire.String(), "NOT accepted durably") {
		t.Fatal("oversized append lost silently or went old")
	}
}

func TestQueuedNextGuardsAndPeerAuthority(t *testing.T) {
	for _, guard := range []string{"settling", "synthetic", "prompt", "heartbeat", "process", "subagent", "compaction", "pending"} {
		t.Run(guard, func(t *testing.T) {
			f := nextSetup(t)
			switch guard {
			case "settling":
				f.b.settlingRun = true
			case "synthetic":
				f.b.syntheticPrompts = 1
			case "prompt":
				f.b.pendingPrompts = map[string]struct{}{"p": {}}
			case "heartbeat":
				f.b.pendingHeartbeats = []string{"alarm"}
			case "process":
				f.b.bgProcesses = 1
			case "subagent":
				f.b.bgSubagents = 1
			case "compaction", "pending":
				raw := fileText(filepath.Join(f.dir, "state-0.json"))
				if guard == "compaction" {
					raw = strings.Replace(raw, `"isCompacting":false`, `"isCompacting":true`, 1)
				} else {
					raw = strings.Replace(raw, `"pendingMessageCount":0`, `"pendingMessageCount":1`, 1)
				}
				if err := os.WriteFile(filepath.Join(f.dir, "state-0.json"), []byte(raw), 0600); err != nil {
					t.Fatal(err)
				}
			}
			f.incoming("/next", "next")
			peer := InboundMessage{Body: "pi: /new", Room: "team@muc.x", Nick: "peer", RealJID: "peer@x", From: "team@muc.x/peer", ID: "peer", ReplyToID: "parent"}
			f.b.onInbound(peer)
			f.step()
			es := f.b.inbox.protectedPending()
			if len(f.jobs(t)) != 0 || len(es) != 1 || es[0].FromOwner || es[0].Body != peer.Body || es[0].ReplyToID != "parent" || es[0].Room != peer.Room {
				t.Fatal("guard bypass or peer authority/metadata changed")
			}
		})
	}
}

func TestQueuedNextRejectedProtectedPromptHeld(t *testing.T) {
	f := nextSetup(t)
	f.incoming("/next", "next")
	f.incoming("held", "held")
	f.step()
	f.b.mu.Lock()
	id := ""
	for k := range f.b.protectedPrompts {
		id = k
	}
	f.b.mu.Unlock()
	if id == "" {
		t.Fatal("no correlated protected submission")
	}
	f.b.handleRPCEvent(Event{"type": "response", "id": id, "success": false, "error": "rejected"})
	f.step()
	if f.boundary(t).Phase != nextBoundaryBlocked || len(f.b.inbox.protectedPending()) != 1 {
		t.Fatal("rejected protected prompt retired or retried")
	}
	if !f.b.inbox.protectedPending()[0].deliveredAt.IsZero() {
		t.Fatal("rejection invented consumption evidence")
	}
}

func TestQueuedNextOrderedObservationSaturation(t *testing.T) {
	f := nextSetup(t)
	// Saturate the actual event queue before a correlated response. The reader
	// must not skip the older events, and the observation must not complete early.
	for n := 0; n < cap(f.b.rpc.events); n++ {
		f.b.rpc.events <- Event{"type": "test_prior", "n": fmt.Sprint(n)}
	}
	done := make(chan error, 1)
	go func() { _, err := f.b.rpc.observedState(context.Background()); done <- err }()
	seen := 0
	deadline := time.After(5 * time.Second)
	for {
		select {
		case ev := <-f.b.rpc.Events():
			if ev.Type() == "test_prior" {
				seen++
			} else {
				if seen != cap(f.b.rpc.events) {
					t.Fatal("observation overtook buffered events")
				}
				f.b.handleRPCEvent(ev)
			}
		case err := <-done:
			if err != nil {
				t.Fatal(err)
			}
			if seen != cap(f.b.rpc.events) {
				t.Fatal("response bypassed ordered observation")
			}
			return
		case <-deadline:
			t.Fatal("saturated event observation deadlocked")
		}
	}
}

func TestQueuedNextPeerReleaseAndDisplayedMarker(t *testing.T) {
	f := nextSetup(t)
	f.b.setStreaming(true)
	f.incoming("/next", "next")
	markers := 0
	f.b.markerSender = func(to, id string) error {
		if to != f.b.acct.Owner+"/phone" || id != "dm" {
			t.Errorf("wrong marker target %s %s", to, id)
		}
		markers++
		return nil
	}
	f.incoming("owner held", "dm")
	f.b.onInbound(InboundMessage{Body: "pi: /new", Room: "team@muc.x", Nick: "peer", RealJID: "peer@x", From: "team@muc.x/peer", ID: "peer", ReplyToID: "parent"})
	if markers != 0 || len(f.b.pendingMarkers) != 0 {
		t.Fatal("held admission marked read")
	}
	f.b.setStreaming(false)
	touchNext(t, filepath.Join(f.dir, "prompt-normal"))
	f.step()
	f.observe(t)
	f.step() // accepted first preflight releases the peer successor
	f.observe(t)
	if markers != 0 || len(f.b.pendingMarkers) != 1 {
		t.Fatal("submission incorrectly marked read")
	}
	prompt := f.b.pendingMarkers[0].prompt
	f.b.handleRPCEvent(Event{"type": "message_start", "message": map[string]any{"role": "user", "content": prompt}})
	if markers != 1 {
		t.Fatal("exact fresh user start did not mark read")
	}
	if !f.b.peerRun || f.b.currentTurnDest() != "team@muc.x" {
		t.Fatal("peer release lost authority/destination")
	}
	commands := fileText(filepath.Join(f.dir, "commands"))
	if strings.Count(commands, `"type":"new_session"`) != 1 || !strings.Contains(commands, "peer@x") {
		t.Fatal("peer apparent control executed or sender lost")
	}
}
