package pimsg

import (
	"context"
	"encoding/json"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"
)

func protectedID(t *testing.T, b *Bridge) string {
	t.Helper()
	b.mu.Lock()
	defer b.mu.Unlock()
	if len(b.protectedPrompts) != 1 {
		t.Fatalf("want exactly one preflight, got %d", len(b.protectedPrompts))
	}
	for id := range b.protectedPrompts {
		return id
	}
	return ""
}
func acceptProtected(t *testing.T, f *nextFixture, success bool) {
	t.Helper()
	f.b.handleRPCEvent(Event{"type": "response", "id": protectedID(t, f.b), "success": success})
}

func TestQueuedNextProductionWakeLoopNoObservationFeedback(t *testing.T) {
	for _, mode := range []string{"compacting", "pending", "releasing"} {
		t.Run(mode, func(t *testing.T) {
			f := nextSetup(t)
			ctx, cancel := context.WithCancel(context.Background())
			f.b.ctx = ctx
			defer cancel()
			f.incoming("/next", "next")
			if mode == "releasing" {
				f.incoming("held", "held")
				f.step() // first preflight deliberately never completes
			} else {
				path := filepath.Join(f.dir, "state-0.json")
				raw := fileText(path)
				if mode == "compacting" {
					raw = strings.Replace(raw, `"isCompacting":false`, `"isCompacting":true`, 1)
				} else {
					raw = strings.Replace(raw, `"pendingMessageCount":0`, `"pendingMessageCount":1`, 1)
				}
				if err := os.WriteFile(path, []byte(raw), 0600); err != nil {
					t.Fatal(err)
				}
			}
			f.b.startNextQueue()
			// A bounded negative observation window below the production one-second poll:
			// no external event/tick is supplied, so any second observation is feedback.
			deadline := time.NewTimer(5 * time.Second)
			defer deadline.Stop()
			observations := 0
			for {
				select {
				case ev := <-f.b.rpc.Events():
					if ev.Type() == "bridge_observation" {
						observations++
						if observations == 1 {
							deadline.Reset(150 * time.Millisecond)
						}
					}
					f.b.handleRPCEvent(ev)
					if observations > 1 {
						t.Fatal("executor self-woke on its own observation")
					}
				case <-deadline.C:
					if observations != 1 {
						t.Fatalf("production loop did not observe: %d", observations)
					}
					cancel()
					// Synchronize worker exit/iteration before fixture teardown.
					awaitNext(t, "executor idle", func() bool { f.b.next.mu.Lock(); defer f.b.next.mu.Unlock(); return !f.b.next.executing })
					return
				}
			}
		})
	}
}

func TestQueuedNextDelayedAndRejectedFirstPreflight(t *testing.T) {
	for _, success := range []bool{false, true} {
		t.Run(map[bool]string{false: "rejected", true: "accepted"}[success], func(t *testing.T) {
			f := nextSetup(t)
			f.incoming("/next", "next")
			f.incoming("first held", "one")
			f.incoming("second held", "two")
			f.step()
			first := protectedID(t, f.b)
			for n := 0; n < 3; n++ {
				f.step()
			}
			if protectedID(t, f.b) != first || strings.Contains(fileText(filepath.Join(f.dir, "commands")), "second held") {
				t.Fatal("successor submitted before first preflight")
			}
			acceptProtected(t, f, success)
			f.step()
			f.observe(t)
			commands := fileText(filepath.Join(f.dir, "commands"))
			if success {
				if !strings.Contains(commands, "second held") {
					t.Fatal("accepted first did not release successor")
				}
				protectedID(t, f.b)
			} else if strings.Contains(commands, "second held") || f.boundary(t).Phase != nextBoundaryBlocked {
				t.Fatal("rejected first released successor")
			}
		})
	}
}

func TestQueuedNextReplyWatchResetSeal(t *testing.T) {
	f := nextSetup(t)
	w := &replyWatch{conversation: "watch@x", stanzaID: "out", expired: true, seconds: 1}
	f.b.replyWatches = map[string]*replyWatch{w.conversation: w}
	f.b.setTurnDest("original@x", false)
	f.b.mu.Lock()
	f.b.nextTransition = true
	f.b.mu.Unlock()
	seeded := f.b.messagingSeeded
	before := fileText(filepath.Join(f.dir, "commands"))
	f.b.flushReplyWatches()
	if f.b.replyWatches[w.conversation] != w || f.b.heartbeatRun || f.b.currentTurnDest() != "original@x" || f.b.messagingSeeded != seeded || f.b.syntheticPrompts != 0 || fileText(filepath.Join(f.dir, "commands")) != before {
		t.Fatal("sealed watch delivery mutated state or lost watch")
	}
	f.b.replyWatches = nil // synthetic fixture has no timer to stop
}

func TestQueuedNextOldUnreadSurvivesGraceAndBlocksReset(t *testing.T) {
	for _, delivered := range []bool{false, true} {
		t.Run(map[bool]string{false: "undelivered", true: "unread-delivered"}[delivered], func(t *testing.T) {
			f := nextSetup(t)
			e := inboxEntry{ID: "old", Body: "old unresolved", From: f.b.acct.Owner, Direct: true, At: time.Now().Add(-time.Hour)}
			f.b.inbox.append(e)
			if delivered {
				f.b.inbox.markDelivered(e.ID, "", "", time.Now())
			}
			f.incoming("/next", "next")
			f.incoming("future", "held")
			f.b.ackInboxSettled()
			f.step()
			if f.b.inbox.len() != 2 || len(f.jobs(t)) != 0 || strings.Contains(fileText(filepath.Join(f.dir, "commands")), `"type":"new_session"`) {
				t.Fatal("unread old entry lost or reset proceeded")
			}
			// Only actual old-session consumption is allowed through the fence.
			f.b.inbox.markDelivered(e.ID, "", "", time.Now().Add(-time.Second))
			f.b.noteRunActivity()
			f.b.ackInboxSettled()
			f.step()
			if len(f.jobs(t)) != 1 || len(f.b.inbox.protectedPending()) != 1 {
				t.Fatal("proven old consumption did not unblock safely")
			}
		})
	}
}

func TestQueuedNextRecoveredOriginalCannotBeLostToMigration(t *testing.T) {
	f := nextSetup(t)
	e := inboxEntry{ID: "old", From: f.b.acct.Owner, Direct: true, At: time.Now().Add(-time.Hour)}
	raw, _ := json.Marshal(e)
	e.Body = strings.Repeat("x", maxInboxRecordBytes-len(raw)-1)
	f.b.inbox.append(e)
	original := fileText(inboxPath("test"))
	if len(strings.TrimSpace(original)) > maxInboxRecordBytes {
		t.Fatal("invalid fixture")
	}
	f.incoming("/next", "next")
	// This is the deterministic reviewed failure: protected metadata cannot fit.
	candidate := e
	candidate.NextBoundaryID = f.boundary(t).ID
	if _, err := f.b.inbox.appendProtected(candidate); err == nil {
		t.Fatal("fixture must exceed protected encoded bound")
	}
	f.b.deliverInbox(e) // conservative pre-boundary quarantine; no migration/drop
	f.b.ackInboxSettled()
	f.step()
	if fileText(inboxPath("test")) != original {
		t.Fatal("original durable bytes changed after failed/avoided migration")
	}
	reloaded := newInbox(inboxPath("test"), f.b.log)
	if reloaded.loadError() != nil || reloaded.len() != 1 || reloaded.pending()[0].Body != e.Body {
		t.Fatal("original not exactly recoverable")
	}
	if len(f.jobs(t)) != 0 {
		t.Fatal("reset despite quarantined old input")
	}
}

func TestQueuedNextHistoricalDestructiveReplayQuarantined(t *testing.T) {
	for _, body := range []string{"/next", "/new", "/abort", "/stop", "!"} {
		t.Run(body, func(t *testing.T) {
			f := nextSetup(t)
			e := inboxEntry{ID: "control", Body: body, From: f.b.acct.Owner, Direct: true, At: time.Now().Add(-time.Hour)}
			f.b.inbox.append(e)
			if err := f.b.loadNextQueue(); err != nil {
				t.Fatal(err)
			}
			f.b.deliverInbox(e)
			f.b.ackInboxSettled()
			f.step()
			if f.b.inbox.len() != 1 || f.boundary(t) != nil || len(f.jobs(t)) != 0 {
				t.Fatal("historical control executed or lost")
			}
		})
	}
}

func TestQueuedNextRetirementSecondBoundaryAndExactTargetStartup(t *testing.T) {
	f := nextSetup(t)
	f.incoming("/next", "first-boundary")
	first := f.boundary(t).ID
	f.incoming("fresh long task", "held")
	f.step()
	acceptProtected(t, f, true)
	f.b.setStreaming(true) // accepted but long-running, deliberately NOT settled
	f.step()
	if f.boundary(t) != nil || f.b.nextPending() || len(f.b.inbox.protectedPending()) != 1 {
		t.Fatal("admission fence waited for settlement")
	}
	// Restart state reconstruction without .next must be replay-only exact target.
	f.b.next.record = nil
	f.b.next.submitted = nil
	f.b.inbox = newInbox(inboxPath("test"), f.b.log)
	if err := f.b.loadNextQueue(); err != nil {
		t.Fatal(err)
	}
	before := strings.Count(fileText(filepath.Join(f.dir, "commands")), `"type":"new_session"`)
	f.step()
	acceptProtected(t, f, true)
	f.step()
	if f.boundary(t) != nil || strings.Count(fileText(filepath.Join(f.dir, "commands")), `"type":"new_session"`) != before || len(f.jobs(t)) != 1 {
		t.Fatal("recovery reset or failed retirement")
	}
	f.incoming("&", "second-boundary")
	second := f.boundary(t).ID
	if second == first {
		t.Fatal("later & collapsed into successful boundary")
	}
	f.incoming("second target input", "later")
	f.step()
	if f.boundary(t).Phase != nextBoundaryWaiting || strings.Count(fileText(filepath.Join(f.dir, "commands")), `"type":"new_session"`) != before {
		t.Fatal("new reset crossed unresolved old exact-target input")
	}
	es := f.b.inbox.protectedPending()
	if len(es) != 2 || es[0].NextBoundaryID != first || es[0].TargetSessionID != "id:s1" || es[1].NextBoundaryID != second || es[1].TargetSessionID != "" {
		t.Fatal("older input reclassified/retargeted")
	}
	f.b.noteRunActivity()
	f.b.ackInboxSettled() // old target consumed; second boundary input is unassigned
	f.b.setStreaming(false)
	f.step()
	if strings.Count(fileText(filepath.Join(f.dir, "commands")), `"type":"new_session"`) != before+1 || len(f.jobs(t)) != 2 {
		t.Fatal("resolved older target did not permit exactly one second reset/job")
	}
	es = f.b.inbox.protectedPending()
	if len(es) != 1 || es[0].NextBoundaryID != second || es[0].TargetSessionID != "id:s2" {
		t.Fatal("second boundary lost exact target isolation")
	}
}

func TestQueuedNextOrphanStartupFailsClosed(t *testing.T) {
	for _, mode := range []string{"unassigned", "mixed", "missing-pointer", "foreign-active"} {
		t.Run(mode, func(t *testing.T) {
			f := nextSetup(t)
			target := "id:s0"
			if mode == "unassigned" {
				target = ""
			}
			_, err := f.b.inbox.appendProtected(inboxEntry{NextBoundaryID: "old", TargetSessionID: target, Body: "held", At: time.Now()})
			if err != nil {
				t.Fatal(err)
			}
			if mode != "missing-pointer" {
				if err := durableWritePrivate(sessionStatePath("test"), []byte(f.sources[0]+"\n")); err != nil {
					t.Fatal(err)
				}
			}
			if mode == "mixed" {
				_, err = f.b.inbox.appendProtected(inboxEntry{NextBoundaryID: "other", TargetSessionID: "id:s1", Body: "other", At: time.Now()})
				if err != nil {
					t.Fatal(err)
				}
			}
			if mode == "foreign-active" {
				// Saved target says s0 but actual ordered RPC state says s1.
				raw := fileText(filepath.Join(f.dir, "state-1.json"))
				if err := os.WriteFile(filepath.Join(f.dir, "state-0.json"), []byte(raw), 0600); err != nil {
					t.Fatal(err)
				}
				if err := f.b.loadNextQueue(); err != nil {
					t.Fatal(err)
				}
				f.step()
				if f.boundary(t).Phase != nextBoundaryBlocked {
					t.Fatal("foreign identity dispatched")
				}
			} else if err := f.b.loadNextQueue(); err == nil {
				t.Fatal("unsafe orphan accepted")
			}
			if len(f.jobs(t)) != 0 || strings.Contains(fileText(filepath.Join(f.dir, "commands")), `"type":"prompt"`) {
				t.Fatal("orphan recovery caused effects")
			}
		})
	}
}

func TestQueuedNextDedupeConversationSenderStanza(t *testing.T) {
	f := nextSetup(t)
	f.incoming("/next", "next")
	a := InboundMessage{Body: "pi: held", ID: "shared", Room: "one@muc.x", Nick: "sender", RealJID: "peer@x", Addressed: true}
	f.b.onInbound(a)
	f.b.onInbound(a)
	a.Room = "two@muc.x"
	f.b.onInbound(a)
	a.Nick = "other"
	a.RealJID = "other@x"
	f.b.onInbound(a)
	if len(f.b.inbox.protectedPending()) != 3 {
		t.Fatalf("dedupe crossed conversation/sender: %d", len(f.b.inbox.protectedPending()))
	}
}

func TestQueuedNextExplicitNewClearsOnlyOldPrimary(t *testing.T) {
	f := nextSetup(t)
	f.b.inbox.append(inboxEntry{ID: "old", Body: "old primary", Direct: true, From: f.b.acct.Owner, At: time.Now()})
	f.incoming("/next", "next")
	f.incoming("future retained", "held")
	f.incoming("/new", "new")
	f.step()
	es := f.b.inbox.pending()
	if len(es) != 1 || !es[0].protected() || es[0].Body != "future retained" || !strings.Contains(f.wire.String(), "discarded 1 unresolved OLD") || len(f.jobs(t)) != 0 {
		t.Fatal("explicit recovery discarded post-boundary input or failed to report old work")
	}
	acceptProtected(t, f, true)
	f.step()
	before := strings.Count(fileText(filepath.Join(f.dir, "commands")), `"type":"new_session"`)
	f.incoming("/new", "unsafe-new")
	f.observe(t)
	if strings.Count(fileText(filepath.Join(f.dir, "commands")), `"type":"new_session"`) != before || len(f.b.inbox.protectedPending()) != 1 {
		t.Fatal("legacy /new reset unresolved exact-target data")
	}
}
