package pimsg

import (
	"encoding/json"
	"os"
	"path/filepath"
	"reflect"
	"strings"
	"testing"
	"time"
)

func restartNextFixture(t *testing.T, f *nextFixture) error {
	t.Helper()
	f.b.next.record = nil
	f.b.next.submitted = nil
	f.b.inbox = newInbox(inboxPath("test"), f.b.log)
	f.b.mu.Lock()
	f.b.pendingPrompts = nil
	f.b.protectedPrompts = nil
	f.b.protectedPromptError = ""
	f.b.mu.Unlock()
	return f.b.loadNextQueue()
}

func TestQueuedNextWaitsForDurableAcceptedTarget(t *testing.T) {
	for _, supersede := range []bool{false, true} {
		t.Run(map[bool]string{false: "next", true: "superseded-new"}[supersede], func(t *testing.T) {
			f := nextSetup(t)
			target, err := os.ReadFile(f.sources[1])
			if err != nil {
				t.Fatal(err)
			}
			if err := os.Remove(f.sources[1]); err != nil {
				t.Fatal(err)
			}
			f.incoming("/next", "boundary")
			f.incoming("first held", "one")
			f.incoming("second held", "two")
			if supersede {
				f.incoming("/new", "supersede")
			}
			f.step()
			acceptProtected(t, f, true)
			f.step()
			acceptProtected(t, f, true)
			f.step()
			before := fileText(filepath.Join(f.dir, "commands"))
			for n := 0; n < 3; n++ {
				f.step()
			}
			r := f.boundary(t)
			if r == nil || r.Phase != nextBoundaryReleasing || r.FreshSessionID != "id:s1" || !f.b.next.fenced.Load() {
				t.Fatal("accepted but unmaterialized target did not retain release fence")
			}
			after := fileText(filepath.Join(f.dir, "commands"))
			if strings.Count(after, `"type":"prompt"`) != 2 || strings.Count(after, `"type":"new_session"`) != 1 || strings.Count(before, `"type":"prompt"`) != 2 {
				t.Fatal("durability polling resubmitted accepted input or reset")
			}
			f.incoming("/new", "unsafe-retarget")
			if !strings.Contains(f.wire.String(), "already target-bound") {
				t.Fatal("waiting target could be superseded")
			}
			// New admissions during the wait still follow the same target FIFO.
			f.incoming("third held", "three")
			f.step()
			acceptProtected(t, f, true)
			f.step()
			for _, e := range f.b.inbox.protectedPending() {
				if e.TargetSessionID != r.FreshSessionID || e.NextBoundaryID != r.ID {
					t.Fatal("waiting input retargeted")
				}
			}
			if err := os.WriteFile(f.sources[1], target, 0600); err != nil {
				t.Fatal(err)
			}
			f.step()
			f.observe(t)
			commands := fileText(filepath.Join(f.dir, "commands"))
			wantJobs := 1
			if supersede {
				wantJobs = 0
			}
			if f.boundary(t) != nil || f.b.next.fenced.Load() || len(f.b.inbox.protectedPending()) != 3 || len(f.jobs(t)) != wantJobs || strings.Count(commands, `"type":"new_session"`) != 1 || strings.Count(commands, `"type":"prompt"`) != 3 {
				t.Fatal("materialized exact target did not retire without replay/reset/reconciliation")
			}
		})
	}
}

func TestQueuedNextDurabilityWaitPointerFailureBlocks(t *testing.T) {
	for _, mode := range []string{"missing-pointer", "wrong-pointer"} {
		t.Run(mode, func(t *testing.T) {
			f := nextSetup(t)
			if err := os.Remove(f.sources[1]); err != nil {
				t.Fatal(err)
			}
			f.incoming("/next", "boundary")
			f.incoming("held", "held")
			f.incoming("/new", "supersede")
			f.step()
			acceptProtected(t, f, true)
			f.step()
			alterNextResumeEvidence(t, f, mode)
			f.step()
			if f.boundary(t).Phase != nextBoundaryBlocked || !f.b.next.fenced.Load() {
				t.Fatal("bad pointer treated as transient materialization delay")
			}
			f.incoming("/new", "unsafe-retarget")
			f.step()
			f.observe(t)
			commands := fileText(filepath.Join(f.dir, "commands"))
			if strings.Count(commands, `"type":"prompt"`) != 1 || strings.Count(commands, `"type":"new_session"`) != 1 || len(f.b.inbox.protectedPending()) != 1 {
				t.Fatal("pointer failure reset, resubmitted, or lost input")
			}
		})
	}
}

func TestQueuedNextDurabilityWaitRestart(t *testing.T) {
	for _, mode := range []string{"exact", "wrong-id", "wrong-file", "missing-pointer", "wrong-pointer", "missing-file", "empty-file"} {
		t.Run(mode, func(t *testing.T) {
			f := nextSetup(t)
			target, err := os.ReadFile(f.sources[1])
			if err != nil {
				t.Fatal(err)
			}
			if err := os.Remove(f.sources[1]); err != nil {
				t.Fatal(err)
			}
			f.incoming("/next", "boundary")
			f.incoming("held across crash", "held")
			f.incoming("/new", "supersede") // no reconciliation job to recover/reopen
			f.step()
			acceptProtected(t, f, true)
			f.step()
			if f.boundary(t).Phase != nextBoundaryReleasing {
				t.Fatal("not waiting durably before simulated crash")
			}
			if mode != "missing-file" {
				if mode == "empty-file" {
					target = nil
				}
				if err := os.WriteFile(f.sources[1], target, 0600); err != nil {
					t.Fatal(err)
				}
			}
			alterNextResumeEvidence(t, f, mode)
			before := fileText(filepath.Join(f.dir, "commands"))
			err = restartNextFixture(t, f)
			unusable := mode == "missing-pointer" || mode == "wrong-pointer" || mode == "missing-file" || mode == "empty-file"
			if unusable {
				if err == nil {
					t.Fatal("restart accepted an unusable exact resume pointer")
				}
			} else {
				if err != nil {
					t.Fatal(err)
				}
				f.step()
				if mode == "exact" {
					acceptProtected(t, f, true)
					f.step()
					if f.boundary(t) != nil {
						t.Fatal("exact restart did not release")
					}
				} else if f.boundary(t).Phase != nextBoundaryBlocked {
					t.Fatal("wrong active target was not blocked")
				}
			}
			f.observe(t)
			after := fileText(filepath.Join(f.dir, "commands"))
			wantPrompts := 1
			if mode == "exact" {
				wantPrompts = 2 // documented at-least-once exact-target replay
			}
			if strings.Count(after, `"type":"prompt"`) != wantPrompts || strings.Count(after, `"type":"new_session"`) != strings.Count(before, `"type":"new_session"`) || len(f.jobs(t)) != 0 || len(f.b.inbox.protectedPending()) != 1 {
				t.Fatal("restart lost input, reset, reconciled, or replayed to wrong target")
			}
		})
	}
}

// Alter only fake state or temp-directory resume evidence, never live state.
func alterNextResumeEvidence(t *testing.T, f *nextFixture, mode string) {
	t.Helper()
	switch mode {
	case "wrong-id", "wrong-file":
		path := filepath.Join(f.dir, "state-1.json")
		raw := fileText(path)
		if mode == "wrong-id" {
			raw = strings.Replace(raw, `"sessionId":"s1"`, `"sessionId":"foreign"`, 1)
		} else {
			raw = strings.Replace(raw, f.sources[1], f.sources[2], 1)
		}
		if err := os.WriteFile(path, []byte(raw), 0600); err != nil {
			t.Fatal(err)
		}
	case "missing-pointer":
		if err := os.Remove(sessionStatePath("test")); err != nil {
			t.Fatal(err)
		}
	case "wrong-pointer":
		if err := durableWritePrivate(sessionStatePath("test"), []byte(f.sources[2]+"\n")); err != nil {
			t.Fatal(err)
		}
	}
}

// Compare the durable identity/content projection, not process-local delivery
// timestamps. Check both memory and a fresh disk load to catch loss/substitution.
func assertNextProtectedEntries(t *testing.T, f *nextFixture, want []inboxEntry) {
	t.Helper()
	for source, entries := range map[string][]inboxEntry{
		"memory": f.b.inbox.protectedPending(),
		"disk":   newInbox(inboxPath("test"), f.b.log).protectedPending(),
	} {
		if len(entries) != len(want) {
			t.Fatalf("%s protected entries: got %d, want %d", source, len(entries), len(want))
		}
		seen := make(map[string]bool)
		for i, got := range entries {
			w := want[i]
			if got.Key == "" || seen[got.Key] || got.Key != w.Key || got.Body != w.Body || got.NextBoundaryID != w.NextBoundaryID || got.TargetSessionID != w.TargetSessionID {
				t.Fatalf("%s protected entry %d: got %+v, want %+v", source, i, got, w)
			}
			seen[got.Key] = true
		}
	}
}

func nextPromptCommands(t *testing.T, f *nextFixture) []Event {
	t.Helper()
	var prompts []Event
	for _, line := range strings.Split(fileText(filepath.Join(f.dir, "commands")), "\n") {
		if !strings.HasPrefix(line, "{") {
			continue
		}
		var ev Event
		if err := json.Unmarshal([]byte(line), &ev); err != nil {
			t.Fatal(err)
		}
		if ev.Type() == "prompt" {
			prompts = append(prompts, ev)
		}
	}
	return prompts
}

func assertNextPromptBodies(t *testing.T, f *nextFixture, want []string) {
	t.Helper()
	var got []string
	for _, ev := range nextPromptCommands(t, f) {
		got = append(got, ev.Str("message"))
	}
	if !reflect.DeepEqual(got, want) {
		t.Fatalf("ordered prompt bodies: got %q, want %q", got, want)
	}
}

func TestQueuedNextProtectedSteersWithIdleLocalFlags(t *testing.T) {
	f := nextSetup(t)
	f.incoming("/next", "boundary")
	f.incoming("first held", "one")
	f.incoming("second held", "two")
	assertIdle := func() {
		t.Helper()
		f.b.mu.Lock()
		defer f.b.mu.Unlock()
		if f.b.streamingRun || len(f.b.pendingPrompts) != 0 || len(f.b.protectedPrompts) != 0 {
			t.Fatal("regression requires idle local flags without agent_start")
		}
	}
	assertIdle()
	f.step()
	f.observe(t) // drain the pipe without emitting agent_start or acceptance
	prompts := nextPromptCommands(t, f)
	if len(prompts) != 1 || prompts[0].Str("streamingBehavior") != "steer" || !strings.HasSuffix(prompts[0].Str("message"), "first held") {
		t.Fatalf("first idle protected submission must explicitly steer: %+v", prompts)
	}
	acceptProtected(t, f, true)
	assertIdle() // acceptance cleared pendingPrompts, but Pi can already be busy
	f.step()
	f.observe(t)
	prompts = nextPromptCommands(t, f)
	if len(prompts) != 2 || prompts[1].Str("streamingBehavior") != "steer" || !strings.HasSuffix(prompts[1].Str("message"), "second held") {
		t.Fatalf("FIFO successor must steer despite lagging local flags: %+v", prompts)
	}
	acceptProtected(t, f, true)
	f.step()
	if f.boundary(t) != nil {
		t.Fatal("accepted FIFO did not retire")
	}
}

func TestQueuedNextOrphanReplayAdmissionCrashBeforeAssignment(t *testing.T) {
	f := nextSetup(t)
	f.incoming("/next", "boundary")
	f.incoming("orphan first", "")
	f.incoming("orphan second", "")
	f.incoming("/new", "supersede") // no reconciliation authority to recover
	f.step()
	acceptProtected(t, f, true)
	f.step()
	acceptProtected(t, f, true)
	f.step()
	if f.boundary(t) != nil {
		t.Fatal("fixture must start with retired, assigned orphan input")
	}
	want := f.b.inbox.protectedPending()
	if err := restartNextFixture(t, f); err != nil {
		t.Fatal(err)
	}
	r := f.boundary(t)
	if r == nil || r.Command != "exact-target-replay" || r.Phase != nextBoundaryReleasing || r.ID != want[0].NextBoundaryID || r.FreshSessionID != "id:s1" || r.FreshSessionFile != f.sources[1] {
		t.Fatalf("reconstructed replay fence not durably exact: %+v", r)
	}
	assertNextProtectedEntries(t, f, want)
	before := fileText(filepath.Join(f.dir, "commands"))
	f.incoming("admitted before crash", "") // no release step/target assignment
	entries := f.b.inbox.protectedPending()
	if len(entries) != 3 || entries[2].NextBoundaryID != r.ID || entries[2].TargetSessionID != "" {
		t.Fatal("new admission must remain unassigned at crash point")
	}
	want = append(want, entries[2])
	if err := restartNextFixture(t, f); err != nil {
		t.Fatal(err)
	}
	assertNextProtectedEntries(t, f, want)
	// A durable replay record, not mixed orphan inference, now carries authority.
	f.b.messagingSeeded = true
	f.step()
	for i := range want {
		want[i].TargetSessionID = "id:s1"
	}
	assertNextProtectedEntries(t, f, want)
	for range want {
		acceptProtected(t, f, true)
		f.step()
	}
	f.observe(t)
	prompts := nextPromptCommands(t, f)
	// Original acceptance plus documented at-least-once replay in exact order.
	if len(prompts) != 5 {
		t.Fatalf("prompt count: got %d, want 5", len(prompts))
	}
	var replayBodies []string
	for _, ev := range prompts[2:] {
		replayBodies = append(replayBodies, ev.Str("message"))
	}
	prefix := "from: " + f.b.acct.Owner + "\n"
	if !reflect.DeepEqual(replayBodies, []string{prefix + "orphan first", prefix + "orphan second", prefix + "admitted before crash"}) {
		t.Fatalf("replay lost, duplicated or substituted input: %q", replayBodies)
	}
	after := fileText(filepath.Join(f.dir, "commands"))
	if f.boundary(t) != nil || strings.Count(after, `"type":"new_session"`) != strings.Count(before, `"type":"new_session"`) || len(f.jobs(t)) != 0 {
		t.Fatal("replay failed retirement, reset, or reopened reconciliation")
	}
	assertNextProtectedEntries(t, f, want)
}

func TestQueuedNextHistoricalDurabilityBlockRecovery(t *testing.T) {
	for _, assignment := range []string{"unassigned", "assigned", "accepted-then-unassigned"} {
		for _, mode := range []string{"exact", "wrong-id", "wrong-file", "missing-pointer", "wrong-pointer", "missing-file", "empty-file", "rejected", "reset-failed", "write-uncertain"} {
			t.Run(assignment+"/"+mode, func(t *testing.T) {
				f := nextSetup(t)
				// Establish the same target as explicit /new, then reconstruct the
				// historical blocked artifact with assigned and/or later unassigned input.
				f.incoming("/next", "boundary")
				f.incoming("/new", "supersede")
				f.step()
				r, err := newNextBoundaryRecord(f.b.acct.Owner, "&", time.Now())
				if err != nil {
					t.Fatal(err)
				}
				r.Phase, r.LastError = nextBoundaryBlocked, nextAcceptedTargetNotDurable
				r.FreshSessionID, r.FreshSessionFile = "id:s1", f.sources[1]
				switch mode {
				case "rejected":
					r.LastError = "protected prompt rejected; retained for exact target"
				case "reset-failed":
					r.LastError = "reset failed/cancelled/uncertain"
				case "write-uncertain":
					r.LastError += "; persist blocked state: uncertain"
				case "missing-file":
					if err := os.Remove(f.sources[1]); err != nil {
						t.Fatal(err)
					}
				case "empty-file":
					if err := os.WriteFile(f.sources[1], nil, 0600); err != nil {
						t.Fatal(err)
					}
				}
				if err := saveNextBoundary(f.b.nextPath(), r); err != nil {
					t.Fatal(err)
				}
				for i, body := range []string{"historical first", "historical second"} {
					target := ""
					if assignment == "assigned" || assignment == "accepted-then-unassigned" && i == 0 {
						target = r.FreshSessionID
					}
					if _, err := f.b.inbox.appendProtected(inboxEntry{NextBoundaryID: r.ID, TargetSessionID: target, Body: body, Direct: true, From: f.b.acct.Owner}); err != nil {
						t.Fatal(err)
					}
				}
				want := f.b.inbox.protectedPending()
				assertNextProtectedEntries(t, f, want)
				alterNextResumeEvidence(t, f, mode)
				before := fileText(filepath.Join(f.dir, "commands"))
				if err := restartNextFixture(t, f); err != nil {
					t.Fatal(err)
				}
				assertNextProtectedEntries(t, f, want)
				f.b.messagingSeeded = true // isolate actual ordered bodies from contracts
				f.step()
				var wantBodies []string
				if mode == "exact" {
					acceptProtected(t, f, true)
					f.step()
					acceptProtected(t, f, true)
					f.step()
					if f.boundary(t) != nil {
						t.Fatal("historical durability block did not recover")
					}
					for i := range want {
						want[i].TargetSessionID = r.FreshSessionID
					}
					prefix := "from: " + f.b.acct.Owner + "\n"
					wantBodies = []string{prefix + "historical first", prefix + "historical second"}
				} else if f.boundary(t).Phase != nextBoundaryBlocked {
					t.Fatal("unproven or genuinely failed boundary recovered")
				}
				assertNextProtectedEntries(t, f, want)
				f.incoming("/new", "unsafe-retarget")
				f.step()
				f.observe(t)
				assertNextPromptBodies(t, f, wantBodies)
				assertNextProtectedEntries(t, f, want)
				after := fileText(filepath.Join(f.dir, "commands"))
				if strings.Count(after, `"type":"new_session"`) != strings.Count(before, `"type":"new_session"`) || len(f.jobs(t)) != 0 {
					t.Fatal("historical recovery reset or reopened reconciliation")
				}
			})
		}
	}
}
