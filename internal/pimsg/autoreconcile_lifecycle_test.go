package pimsg

import (
	"context"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"testing"
	"time"
)

func autoIteration(t *testing.T, b *Bridge, whileRunning func()) {
	t.Helper()
	done := make(chan struct{})
	go func() { b.executeAuto(); close(done) }()
	deadline := time.NewTimer(10 * time.Second)
	defer deadline.Stop()
	tick := time.NewTicker(10 * time.Millisecond)
	defer tick.Stop()
	for {
		select {
		case <-done:
			return
		case ev := <-b.rpc.Events():
			b.handleRPCEvent(ev)
		case <-tick.C:
			if whileRunning != nil {
				whileRunning()
			}
		case <-deadline.C:
			t.Fatal("automatic iteration blocked")
		}
	}
}
func configureAutoFixture(f *nextFixture) {
	f.b.acct.JID = "pi@x"
	f.b.acct.AutoreconcileTimeout = time.Millisecond
	f.b.acct.AutoreconcileHistoryMessages = 10
	f.b.acct.AutoreconcileOwnerHistoryMessages = 10
	f.b.xmpp.onAcceptedSent = f.b.captureAutoSent
	f.b.mu.Lock()
	f.b.idleSince = time.Now().Add(-time.Hour)
	f.b.mu.Unlock()
}
func TestAutoreconcileWorkerPreResetInputInvalidationAndSelfResult(t *testing.T) {
	for _, fail := range []bool{false, true} {
		t.Run(map[bool]string{false: "completed with ordinary skips", true: "hard failure"}[fail], func(t *testing.T) {
			f := nextSetup(t)
			configureAutoFixture(f)
			script := fileText(f.pi)
			script = strings.Replace(script, `if ! grep -q reset "$base/commands"; then exit 9; fi`, `if [ ! -f "$job/idle-authorized" ] || [ -f "$job/reset-authorized" ]; then exit 9; fi`, 1)
			if err := os.WriteFile(f.pi, []byte(script), 0700); err != nil {
				t.Fatal(err)
			}
			if fail {
				touchNext(t, filepath.Join(f.dir, "worker-fail"))
			}
			admitted := false
			autoIteration(t, f.b, func() {
				if admitted {
					return
				}
				for _, job := range f.jobs(t) {
					if _, err := os.Stat(filepath.Join(job, "started")); err == nil {
						// No lifetime transition lock: foreground input must remain responsive.
						if !f.b.sessionTransitionMu.TryLock() {
							t.Fatal("worker holds foreground lock")
						}
						f.b.sessionTransitionMu.Unlock()
						f.incoming("new foreground work", "new-input")
						admitted = true
						touchNext(t, filepath.Join(job, "release"))
						return
					}
				}
			})
			if !admitted {
				t.Fatal("restricted worker did not start before reset")
			}
			a := f.b.auto.state.Attempt
			if a.Phase != "blocked" || !strings.Contains(fileText(filepath.Join(f.dir, "commands")), "new foreground work") || strings.Contains(fileText(filepath.Join(f.dir, "commands")), `"type":"new_session"`) {
				t.Fatal("intervening input lost or unsafe reset", a)
			}
			if !strings.Contains(f.wire.String(), `to="pi@x"`) || !strings.Contains(f.wire.String(), "autoreconcile") || strings.Contains(f.wire.String(), `to="owner@x"`) {
				t.Fatal("automatic result not self addressed", f.wire.String())
			}
			if fail && !strings.Contains(f.wire.String(), "failed") {
				t.Fatal("hard failure result not reported")
			}
			if !fail && !strings.Contains(f.wire.String(), "wiki unchanged") {
				t.Fatal("ordinary skips discarded")
			}
			jobs := f.jobs(t)
			if len(jobs) != 1 {
				t.Fatal("unexpected jobs", jobs)
			}
			if _, err := os.Stat(filepath.Join(jobs[0], "reset-authorized")); !os.IsNotExist(err) {
				t.Fatal("fake reset authorization")
			}
			if len(f.b.auto.state.History.Recent) == 0 || !strings.Contains(f.b.auto.state.History.Recent[len(f.b.auto.state.History.Recent)-1].Body, "autoreconcile") {
				t.Fatal("accepted receipt absent from recap")
			}
			prior := fileText(filepath.Join(jobs[0], "start-count"))
			f.b.mu.Lock()
			f.b.pendingPrompts = nil
			f.b.idleSince = time.Now().Add(-time.Hour)
			f.b.mu.Unlock()
			autoIteration(t, f.b, nil)
			if fileText(filepath.Join(jobs[0], "start-count")) != prior || len(f.jobs(t)) != 1 {
				t.Fatal("unchanged snapshot reran worker effects")
			}
		})
	}
}
func TestAutoreconcilePendingReportRetryAndRecoveryNoWorkerReplay(t *testing.T) {
	f := nextSetup(t)
	configureAutoFixture(f)
	a := &autoAttempt{Token: "token", Phase: "notice-pending", Generation: 0, SourceID: "id:s0", SourceFile: f.sources[0], ReportID: "stable-report-id", ReportText: "completed with limitations; no wiki edits"}
	x := f.b.xmpp
	f.b.xmpp = nil
	if f.b.sendAutoNotice(a) {
		t.Fatal("offline report accepted")
	}
	if f.b.auto.state.Attempt.Phase != "notice-pending" {
		t.Fatal("lost durable pending report")
	}
	restarted := NewBridge(f.b.acct, false)
	restarted.ctx = context.Background()
	restarted.xmpp = x
	x.onAcceptedSent = restarted.captureAutoSent
	if err := restarted.loadAuto(); err != nil {
		t.Fatal(err)
	}
	pending := *restarted.auto.state.Attempt
	restarted.resumeAutoNotice(&pending)
	if restarted.auto.state.Attempt.Phase != "blocked" || !strings.Contains(f.wire.String(), "stable-report-id") || len(restarted.auto.state.History.Recent) != 1 {
		t.Fatal("recovery failed to report-only", restarted.auto.state.Attempt, f.wire.String())
	}
	if len(f.jobs(t)) != 0 || strings.Contains(fileText(filepath.Join(f.dir, "commands")), "new_session") {
		t.Fatal("report retry replayed effects/reset")
	}
	// Stable ID deduplicates another transport acceptance after an uncertain send.
	if err := x.sendRecordedChat("pi@x", pending.ReportID, pending.ReportText); err != nil {
		t.Fatal(err)
	}
	if len(restarted.auto.state.History.Recent) != 1 {
		t.Fatal("retry duplicated eligible self receipt")
	}
}
func TestAutoreconcileNoSkillMissingNativeCommandPreservesSource(t *testing.T) {
	f := nextSetup(t)
	configureAutoFixture(f)
	f.b.acct.ReconcileSkill = ""
	// The fake primary has no private command. Give get_commands a correlated
	// negative response instead of letting an unrecognized command become a turn.
	// This state-only case stops earlier using an already-invalidated generation.
	source := fileText(f.sources[0])
	f.b.auto.state.Generation = 1
	a := &autoAttempt{Token: "stale", Generation: 0, SourceID: "id:s0", SourceFile: f.sources[0]}
	if err := f.b.autoSave(a, "prepared"); err != nil {
		t.Fatal(err)
	}
	done := make(chan struct{})
	go func() { f.b.finishAuto(a); close(done) }()
	for {
		select {
		case <-done:
			if fileText(f.sources[0]) != source || strings.Contains(fileText(filepath.Join(f.dir, "commands")), "new_session") {
				t.Fatal("invalidated no-skill cleanup reset source")
			}
			return
		case ev := <-f.b.rpc.Events():
			f.b.handleRPCEvent(ev)
		case <-time.After(5 * time.Second):
			t.Fatal("state observation blocked")
		}
	}
}
func TestAutoreconcileNative(t *testing.T) {
	pkg := os.Getenv("PI_MSG_PI_PACKAGE")
	if pkg == "" {
		t.Skip("set PI_MSG_PI_PACKAGE for isolated real native Pi reset/rehydrate/restart proof")
	}
	ctx, cancel := context.WithTimeout(context.Background(), 45*time.Second)
	defer cancel()
	cmd := exec.CommandContext(ctx, "node", "piext/autoreconcile-native.mjs", pkg)
	raw, err := cmd.CombinedOutput()
	if err != nil {
		t.Fatalf("native proof: %v\n%s", err, raw)
	}
	t.Log(string(raw))
}
