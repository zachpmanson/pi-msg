package pimsg

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"syscall"
	"testing"
	"time"

	"mellium.im/xmpp"
	"mellium.im/xmpp/jid"
)

// The production executable dispatches this before config/bridge startup. The
// test executable does the same, so tests exercise the real runner, not Pi.
func init() {
	if len(os.Args) == 3 && os.Args[1] == "--internal-reconcile" {
		timeout := reconcileTimeout
		if os.Getenv("TEST_RECONCILE_TIMEOUT") == "short" {
			timeout = 300 * time.Millisecond
		}
		if err := runReconcileFor(os.Args[2], timeout); err != nil {
			fmt.Fprintln(os.Stderr, err)
			os.Exit(1)
		}
		os.Exit(0)
	}
}

type nextFixture struct {
	b       *Bridge
	wire    *bytes.Buffer
	dir, pi string
	sources []string
}

func nextSetup(t *testing.T) *nextFixture {
	t.Helper()
	dir := t.TempDir()
	t.Setenv("HOME", dir)
	t.Setenv("PI_MSG_CONFIG", filepath.Join(dir, "config", "config.json"))
	b, wire, _ := messageTransportBridge(t, false)
	b.acct.Name = "test"
	b.acct.Nick = "pi"
	b.acct.RoomTrigger = "pi"
	b.ctx = context.Background()
	skill := filepath.Join(dir, "skills", "reconcile", "SKILL.md")
	for name, content := range map[string]string{skill: "---\nname: reconcile\ndescription: reconcile\n---\nReconcile issues then /learn.", filepath.Join(dir, "skills", "learn", "SKILL.md"): "---\nname: learn\ndescription: learn\n---\nLearn."} {
		if err := os.MkdirAll(filepath.Dir(name), 0700); err != nil {
			t.Fatal(err)
		}
		if err := os.WriteFile(name, []byte(content), 0600); err != nil {
			t.Fatal(err)
		}
	}
	b.acct.ReconcileSkill = skill
	f := &nextFixture{b: b, wire: wire, dir: dir}
	for n := 0; n < 5; n++ {
		source := filepath.Join(dir, fmt.Sprintf("source-%d.jsonl", n))
		raw := fmt.Sprintf("{\"type\":\"session\",\"version\":3,\"id\":\"s%d\",\"cwd\":%q}\n{\"type\":\"message\",\"id\":\"m%d\",\"parentId\":null,\"message\":{\"role\":\"user\",\"content\":\"old task, not authorization to continue\"}}\n", n, dir, n)
		if err := os.WriteFile(source, []byte(raw), 0600); err != nil {
			t.Fatal(err)
		}
		f.sources = append(f.sources, source)
		state := map[string]any{"sessionFile": source, "isStreaming": false, "isCompacting": false, "pendingMessageCount": 0, "model": map[string]string{"provider": "fake", "id": "model"}, "thinkingLevel": "high"}
		data, _ := json.Marshal(state)
		if err := os.WriteFile(filepath.Join(dir, fmt.Sprintf("state-%d.json", n)), data, 0600); err != nil {
			t.Fatal(err)
		}
	}
	f.pi = filepath.Join(dir, "pi")
	// The fake worker cannot access a provider, gh or a wiki. It records arguments,
	// forks only into its private directory, and blocks until explicitly released.
	script := `#!/bin/sh
base=` + fmt.Sprintf("%q", dir) + `
if [ "$2" = json ]; then
  fork=; sessions=; prev=
  for arg in "$@"; do
    if [ "$prev" = --fork ]; then fork=$arg; fi
    if [ "$prev" = --session-dir ]; then sessions=$arg; fi
    prev=$arg
  done
  job=$(dirname "$fork")
  printf '%s\n' "$@" > "$job/args"
  if ! grep -q reset "$base/commands"; then exit 9; fi
  cp "$fork" "$sessions/worker.jsonl"
  printf '{"worker":"private write"}\n' >> "$sessions/worker.jsonl"
  echo "$$" > "$job/worker-pid"
  echo start >> "$job/start-count"
  touch "$job/started"
  trap 'exit 143' TERM INT
  while [ ! -f "$base/release" ] && [ ! -f "$job/release" ]; do sleep 0.02; done
  if [ -f "$base/worker-fail" ]; then echo 'worker failed' >&2; exit 7; fi
  if [ -f "$base/provider-fail" ]; then
    printf '{"type":"message_end","message":{"role":"assistant","stopReason":"error","errorMessage":"provider failed","content":[{"type":"text","text":"partial"}]}}\n'
  else
    printf '{"type":"message_end","message":{"role":"assistant","stopReason":"stop","content":[{"type":"text","text":"issues reconciled; wiki unchanged"}]}}\n'
  fi
  exit 0
fi
n=0
while IFS= read -r line; do
  printf '%s\n' "$line" >> "$base/commands"
  id=$(printf '%s' "$line" | sed -n 's/.*"id":"\([^"]*\)".*/\1/p')
  case "$line" in
    *'"type":"get_state"'*)
      printf '{"type":"response","id":"%s","success":true,"data":%s}\n' "$id" "$(cat "$base/state-$n.json")";;
    *'"type":"new_session"'*)
      if [ -f "$base/reset-exit" ]; then exit 1
      elif [ -f "$base/reset-error" ]; then
        printf '{"type":"response","id":"%s","success":false,"error":"reset failed"}\n' "$id"
      elif [ -f "$base/reset-cancel" ]; then
        printf '{"type":"response","id":"%s","success":true,"data":{"cancelled":true}}\n' "$id"
      else
        # Preparation and runner launch must have preceded reset; no worker yet.
        find "$base/config/test.reconcile" -name job.json > "$base/registered" 2>/dev/null
        printf 'reset\n' >> "$base/commands"
        n=$((n+1))
        printf '{"type":"response","id":"%s","success":true,"data":{"cancelled":false}}\n' "$id"
      fi;;
  esac
done
`
	if err := os.WriteFile(f.pi, []byte(script), 0700); err != nil {
		t.Fatal(err)
	}
	b.rpc = NewRPCClient(f.pi, "", dir, "", nil)
	if err := b.rpc.Start(); err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() {
		_ = os.WriteFile(filepath.Join(dir, "release"), nil, 0600)
		b.rpc.Stop()
		select {
		case <-b.rpc.Done():
		case <-time.After(5 * time.Second):
			t.Error("primary fake Pi failed to exit")
		}
		// Reap all job supervisors before the fixture directory disappears.
		deadline := time.Now().Add(8 * time.Second)
		for time.Now().Before(deadline) {
			live := false
			entries, _ := os.ReadDir(reconcileRoot(b.acct.Name))
			for _, e := range entries {
				dir := filepath.Join(reconcileRoot(b.acct.Name), e.Name())
				lock, err := lockFile(filepath.Join(dir, "runner.lock"))
				if err != nil {
					live = true
					continue
				}
				lock.Close()
			}
			if !live {
				return
			}
			time.Sleep(20 * time.Millisecond)
		}
		t.Error("job supervisor remained alive")
	})
	return f
}

func awaitNext(t *testing.T, what string, fn func() bool) {
	t.Helper()
	deadline := time.Now().Add(8 * time.Second)
	for time.Now().Before(deadline) {
		if fn() {
			return
		}
		time.Sleep(20 * time.Millisecond)
	}
	t.Fatalf("timed out waiting for %s", what)
}
func (f *nextFixture) jobs(t *testing.T) []string {
	t.Helper()
	entries, err := os.ReadDir(reconcileRoot(f.b.acct.Name))
	if os.IsNotExist(err) {
		return nil
	}
	if err != nil {
		t.Fatal(err)
	}
	var out []string
	for _, e := range entries {
		if e.IsDir() {
			out = append(out, filepath.Join(reconcileRoot(f.b.acct.Name), e.Name()))
		}
	}
	return out
}
func (f *nextFixture) command(body, dest string) {
	if dest != f.b.acct.Owner {
		body = "pi: " + body
	}
	f.b.onInbound(InboundMessage{Body: body, Direct: dest == f.b.acct.Owner, FromOwner: true, Room: dest, From: f.b.acct.Owner, ID: fmt.Sprint(time.Now().UnixNano())})
}
func fileText(path string) string { raw, _ := os.ReadFile(path); return string(raw) }
func touchNext(t *testing.T, path string) {
	t.Helper()
	if err := os.WriteFile(path, nil, 0600); err != nil {
		t.Fatal(err)
	}
}

func TestNextAndAmpersandBackgroundTransport(t *testing.T) {
	for _, command := range []string{"/next", "!next", "  &  "} {
		t.Run(command, func(t *testing.T) {
			f := nextSetup(t)
			before := fileText(f.sources[0])
			f.b.messagingSeeded = true
			f.b.ranSinceStart = true
			f.command(command, "team@muc.x")
			jobs := f.jobs(t)
			if len(jobs) != 1 {
				t.Fatalf("jobs: %v; wire %s", jobs, f.wire.String())
			}
			dir := jobs[0]
			if !strings.Contains(f.wire.String(), `to="team@muc.x"`) || !strings.Contains(f.wire.String(), "queued in background") || !strings.Contains(f.wire.String(), "new session ready") {
				t.Fatalf("missing real queued stanza: %s", f.wire.String())
			}
			if f.b.messagingSeeded || f.b.hasRun() || loadSessionState("test") != f.sources[1] {
				t.Fatalf("fresh state/resume not updated")
			}
			if !strings.Contains(fileText(filepath.Join(f.dir, "registered")), dir) {
				t.Fatal("job not registered before reset")
			}
			awaitNext(t, "actual worker start", func() bool {
				j, e := loadReconcile(dir)
				return e == nil && j.State == "running" && fileText(filepath.Join(dir, "args")) != ""
			})
			f.b.setTurnDest(f.b.acct.Owner, false)
			f.b.pollReconciles()
			if !strings.Contains(f.wire.String(), "reconciliation running in background") {
				t.Fatalf("missing real running stanza: %s", f.wire.String())
			}
			args := fileText(filepath.Join(dir, "args"))
			for _, s := range []string{"--no-extensions", "--no-context-files", "--no-skills", "--skill", filepath.Join(dir, "learn", "SKILL.md"), "--fork", filepath.Join(dir, "snapshot.jsonl"), "--session-dir", "fake/model", "--thinking\nhigh", "ONLY reconcile GitHub issues", "do not edit application source"} {
				if !strings.Contains(args, s) {
					t.Errorf("args missing %q: %s", s, args)
				}
			}
			// Fresh primary prompts remain usable while worker is blocked.
			f.command("new question", f.b.acct.Owner)
			touchNext(t, filepath.Join(f.dir, "release"))
			awaitNext(t, "worker terminal", func() bool { j, e := loadReconcile(dir); return e == nil && j.State == "completed" })
			f.b.pollReconciles()
			wire := f.wire.String()
			if strings.Contains(wire, `to="zach@x"`) || !strings.Contains(wire, "issues reconciled; wiki unchanged") || !strings.Contains(wire, `type="groupchat"`) {
				t.Fatalf("completion destination changed: %s", wire)
			}
			if got := fileText(f.sources[0]); got != before {
				t.Fatal("source changed")
			}
			if got := fileText(filepath.Join(dir, "snapshot.jsonl")); got != before {
				t.Fatal("snapshot changed")
			}
			if !strings.Contains(fileText(filepath.Join(dir, "sessions", "worker.jsonl")), "private write") {
				t.Fatal("fake fork not written")
			}
			if strings.Contains(fileText(filepath.Join(f.dir, "commands")), "/skill:reconcile") {
				t.Fatal("old context injected into primary")
			}
			f.b.pollReconciles()
			if f.wire.String() != wire {
				t.Fatal("duplicate terminal report")
			}
		})
	}
}

func TestNextPreparationRefusesWithoutReset(t *testing.T) {
	for _, kind := range []string{"unset", "relative", "missing", "directory", "fifo", "unreadable", "empty", "learn unreadable", "snapshot missing", "bad snapshot", "missing model", "arguments", "streaming", "background shell", "background subagent", "rpc streaming", "rpc compacting", "rpc pending", "shared lock failure", "storage failure"} {
		t.Run(kind, func(t *testing.T) {
			f := nextSetup(t)
			switch kind {
			case "unset":
				f.b.acct.ReconcileSkill = ""
			case "relative":
				f.b.acct.ReconcileSkill = "skills/reconcile/SKILL.md"
			case "missing":
				f.b.acct.ReconcileSkill = filepath.Join(f.dir, "nope")
			case "directory":
				f.b.acct.ReconcileSkill = f.dir
			case "fifo":
				fifo := filepath.Join(f.dir, "fifo")
				if err := syscall.Mkfifo(fifo, 0600); err != nil {
					t.Fatal(err)
				}
				f.b.acct.ReconcileSkill = fifo
			case "unreadable":
				_ = os.Chmod(f.b.acct.ReconcileSkill, 0000)
			case "empty":
				touchNext(t, f.b.acct.ReconcileSkill)
			case "learn unreadable":
				_ = os.Chmod(filepath.Join(f.dir, "skills", "learn", "SKILL.md"), 0000)
			case "snapshot missing":
				_ = os.Remove(f.sources[0])
			case "bad snapshot":
				_ = os.WriteFile(f.sources[0], []byte("not JSON"), 0600)
			case "missing model", "rpc streaming", "rpc compacting", "rpc pending":
				var state map[string]any
				_ = json.Unmarshal([]byte(fileText(filepath.Join(f.dir, "state-0.json"))), &state)
				switch kind {
				case "missing model":
					delete(state, "model")
				case "rpc streaming":
					state["isStreaming"] = true
				case "rpc compacting":
					state["isCompacting"] = true
				case "rpc pending":
					state["pendingMessageCount"] = 1
				}
				raw, _ := json.Marshal(state)
				_ = os.WriteFile(filepath.Join(f.dir, "state-0.json"), raw, 0600)
			case "streaming":
				f.b.streamingRun = true
			case "background shell":
				f.b.bgProcesses = 1
			case "background subagent":
				f.b.bgSubagents = 1
			case "shared lock failure":
				if err := os.MkdirAll(filepath.Join(f.dir, ".local", "state", "pi-msg", "reconcile.lock"), 0700); err != nil {
					t.Fatal(err)
				}
			case "storage failure":
				_ = os.MkdirAll(filepath.Dir(reconcileRoot(f.b.acct.Name)), 0700)
				touchNext(t, reconcileRoot(f.b.acct.Name))
			}
			command := "/next"
			if kind == "arguments" {
				command += " arbitrary"
			}
			f.command(command, f.b.acct.Owner)
			if strings.Contains(fileText(filepath.Join(f.dir, "commands")), "new_session") {
				t.Fatal("unsafe reset")
			}
			if f.wire.Len() == 0 {
				t.Fatal("no actionable error stanza")
			}
		})
	}
}

func TestNextResetFailureAndCancellation(t *testing.T) {
	for _, kind := range []string{"reset-error", "reset-cancel", "reset-exit"} {
		t.Run(kind, func(t *testing.T) {
			f := nextSetup(t)
			touchNext(t, filepath.Join(f.dir, kind))
			f.b.messagingSeeded = true
			saveSessionState(f.b.log, "test", f.sources[0])
			f.command("/next", f.b.acct.Owner)
			jobs := f.jobs(t)
			if len(jobs) != 1 {
				t.Fatalf("jobs %v", jobs)
			}
			awaitNext(t, "cancelled job", func() bool { j, e := loadReconcile(jobs[0]); return e == nil && j.State == "cancelled" })
			if _, err := os.Stat(filepath.Join(jobs[0], "started")); !os.IsNotExist(err) {
				t.Fatal("worker started after failed reset")
			}
			if !f.b.messagingSeeded || loadSessionState("test") != f.sources[0] {
				t.Fatal("failed reset changed fresh state")
			}
			if strings.Contains(f.wire.String(), "new session ready") {
				t.Fatal("false reset success")
			}
			if _, err := os.Stat(filepath.Join(jobs[0], "reset-authorized")); !os.IsNotExist(err) {
				t.Fatal("failed reset persisted success authority")
			}
			replacement, wire, _ := messageTransportBridge(t, false)
			replacement.acct = f.b.acct
			awaitNext(t, "failed reset reported", func() bool {
				replacement.pollReconciles()
				_, err := os.Stat(filepath.Join(jobs[0], "reported"))
				return err == nil
			})
			if strings.Contains(wire.String(), "new session ready") || strings.Contains(wire.String(), "queued in background") {
				t.Fatal("recovery falsely claimed reset success")
			}
		})
	}
}

func TestNextMultipleJobsNewAndSharedLock(t *testing.T) {
	f := nextSetup(t)
	f.command("/next", f.b.acct.Owner)
	first := f.jobs(t)[0]
	awaitNext(t, "first running", func() bool { j, _ := loadReconcile(first); return j != nil && j.State == "running" })
	f.command("/next", "team@muc.x")
	jobs := f.jobs(t)
	if len(jobs) != 2 {
		t.Fatalf("jobs %v", jobs)
	}
	second := jobs[0]
	if second == first {
		second = jobs[1]
	}
	awaitNext(t, "second queued", func() bool { j, _ := loadReconcile(second); return j != nil && j.State == "queued" })
	if _, err := os.Stat(filepath.Join(second, "started")); !os.IsNotExist(err) {
		t.Fatal("concurrent wiki writers")
	}
	f.command("/new", f.b.acct.Owner)
	if loadSessionState("test") != f.sources[3] {
		t.Fatal("/new blocked or lost primary pointer")
	}
	// Re-open as a different persona but retain the user's shared wiki lock.
	acct := f.b.acct
	acct.Name = "other"
	state := Event{"sessionFile": f.sources[4], "model": map[string]any{"provider": "fake", "id": "model"}}
	dir, _, err := prepareReconcile(acct, state, acct.Owner, f.pi)
	if err != nil {
		t.Fatal(err)
	}
	gate, err := startReconcile(dir)
	if err != nil {
		t.Fatal(err)
	}
	if err := authorizeReconcile(dir); err != nil {
		t.Fatal(err)
	}
	_, _ = gate.Write([]byte("run\n"))
	gate.Close()
	awaitNext(t, "other persona queued", func() bool { j, _ := loadReconcile(dir); return j != nil && j.State == "queued" })
	if _, err := os.Stat(filepath.Join(dir, "started")); !os.IsNotExist(err) {
		t.Fatal("cross persona lock not shared")
	}
	touchNext(t, filepath.Join(f.dir, "release"))
	for _, d := range []string{first, second, dir} {
		awaitNext(t, "all jobs completed", func() bool { j, _ := loadReconcile(d); return j != nil && j.State == "completed" })
	}
}

func TestNextWorkerFailureAndRecovery(t *testing.T) {
	for _, kind := range []string{"worker-fail", "provider-fail"} {
		t.Run(kind, func(t *testing.T) {
			f := nextSetup(t)
			touchNext(t, filepath.Join(f.dir, kind))
			touchNext(t, filepath.Join(f.dir, "release"))
			f.command("&", "team@muc.x")
			dir := f.jobs(t)[0]
			awaitNext(t, "worker failure", func() bool { j, _ := loadReconcile(dir); return j != nil && j.State == "failed" })
			// A replacement bridge recovers/reporting without any session-scoped tracking.
			replacement, wire, _ := messageTransportBridge(t, false)
			replacement.acct = f.b.acct
			// Terminal metadata is durable before the runner releases its lock;
			// poll only reports completion once liveness is definitively gone.
			awaitNext(t, "failure reported after runner exits", func() bool {
				replacement.pollReconciles()
				_, err := os.Stat(filepath.Join(dir, "reported"))
				return err == nil
			})
			if !strings.Contains(wire.String(), "failed") || !strings.Contains(wire.String(), `to="team@muc.x"`) {
				t.Fatalf("failure missing/wrong destination: %s", wire.String())
			}
			if strings.Contains(wire.String(), "completed") {
				t.Fatal("false success")
			}
			if !strings.Contains(wire.String(), "reconciliation ran in background") {
				t.Fatalf("fast worker start confirmation missing: %s", wire.String())
			}
		})
	}
}

func TestNextInterruptedRecoveryNeverReplays(t *testing.T) {
	f := nextSetup(t)
	state := Event{"sessionFile": f.sources[0], "model": map[string]any{"provider": "fake", "id": "model"}}
	dir, j, err := prepareReconcile(f.b.acct, state, "team@muc.x", f.pi)
	if err != nil {
		t.Fatal(err)
	}
	j.State = "running"
	if err := saveReconcile(dir, j); err != nil {
		t.Fatal(err)
	}
	f.b.pollReconciles()
	j, err = loadReconcile(dir)
	if err != nil || j.State != "interrupted" {
		t.Fatalf("state %v %v", j, err)
	}
	if !strings.Contains(f.wire.String(), "never automatically replayed") {
		t.Fatal("missing interrupted warning")
	}
	if _, err := os.Stat(filepath.Join(dir, "started")); !os.IsNotExist(err) {
		t.Fatal("replayed interrupted effects")
	}
}

func TestNextRunnerGateLossTimeoutAndCancellation(t *testing.T) {
	for _, kind := range []string{"gate loss", "timeout", "signal"} {
		t.Run(kind, func(t *testing.T) {
			f := nextSetup(t)
			if kind == "timeout" {
				t.Setenv("TEST_RECONCILE_TIMEOUT", "short")
			}
			state := Event{"sessionFile": f.sources[0], "model": map[string]any{"provider": "fake", "id": "model"}}
			dir, _, err := prepareReconcile(f.b.acct, state, f.b.acct.Owner, f.pi)
			if err != nil {
				t.Fatal(err)
			}
			gate, err := startReconcile(dir)
			if err != nil {
				t.Fatal(err)
			}
			if kind != "gate loss" {
				touchNext(t, filepath.Join(f.dir, "commands"))
				_ = os.WriteFile(filepath.Join(f.dir, "commands"), []byte("reset\n"), 0600)
				if err := authorizeReconcile(dir); err != nil {
					t.Fatal(err)
				}
				_, _ = gate.Write([]byte("run\n"))
			}
			gate.Close()
			if kind == "signal" {
				awaitNext(t, "runner start", func() bool { j, _ := loadReconcile(dir); return j != nil && j.State == "running" })
				j, _ := loadReconcile(dir)
				p, _ := os.FindProcess(j.RunnerPID)
				_ = p.Signal(syscall.SIGTERM)
			}
			want := "cancelled"
			if kind == "gate loss" {
				want = "interrupted"
			}
			awaitNext(t, want, func() bool { j, _ := loadReconcile(dir); return j != nil && j.State == want })
			if kind == "gate loss" {
				if _, err := os.Stat(filepath.Join(dir, "started")); !os.IsNotExist(err) {
					t.Fatal("worker effects before reset authorization")
				}
			}
		})
	}
}

func TestAmpersandOnlyOwnerControl(t *testing.T) {
	f := nextSetup(t)
	for _, body := range []string{"& stuff", "foo &", "&&"} {
		f.command(body, f.b.acct.Owner)
	}
	f.b.onInbound(InboundMessage{Body: "@pi &", FromOwner: false, Room: "team@muc.x", Nick: "peer", From: "peer@x", ID: "peer-command"})
	awaitNext(t, "ordinary prompts", func() bool { return strings.Count(fileText(filepath.Join(f.dir, "commands")), `"type":"prompt"`) >= 4 })
	if strings.Contains(fileText(filepath.Join(f.dir, "commands")), "new_session") || len(f.jobs(t)) != 0 {
		t.Fatal("noncanonical ampersand executed /next")
	}
}

func TestReconcileEnvNoMessagingOrParentSession(t *testing.T) {
	env := strings.Join(reconcileEnv([]string{"PI_MSG_TOOLS=messaging", "PI_MSG_BEFORE_AGENT_START_TEXT=launch children", "PI_SUBAGENT_ID=parent", "PI_SESSION_FILE=old", "PI_CODING_AGENT_SESSION_DIR=parent", "PI_CODING_AGENT_DIR=/auth", "PATH=/bin"}), "\n")
	if strings.Contains(env, "messaging") || strings.Contains(env, "parent") || !strings.Contains(env, "PI_CODING_AGENT_DIR=/auth") {
		t.Fatal(env)
	}
}

func TestNextCustomSkillAndFastCompletion(t *testing.T) {
	f := nextSetup(t)
	custom := filepath.Join(f.dir, "custom", "custom-task.md")
	if err := os.MkdirAll(filepath.Dir(custom), 0700); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(custom, []byte("---\nname: custom-finalize\ndescription: custom unattended reconciliation\n---\nReconcile issues, no nested skills."), 0600); err != nil {
		t.Fatal(err)
	}
	f.b.acct.ReconcileSkill = custom
	touchNext(t, filepath.Join(f.dir, "release"))
	f.command("&", f.b.acct.Owner)
	dir := f.jobs(t)[0]
	awaitNext(t, "fast completion", func() bool { j, _ := loadReconcile(dir); return j != nil && j.State == "completed" })
	f.b.pollReconciles()
	if !strings.Contains(f.wire.String(), "reconciliation ran in background") || !strings.Contains(f.wire.String(), "completed") {
		t.Fatalf("missing real fast worker notices: %s", f.wire.String())
	}
	args := fileText(filepath.Join(dir, "args"))
	if strings.Contains(args, "/skill:reconcile") || strings.Contains(args, filepath.Join(dir, "learn", "SKILL.md")) || !strings.Contains(args, "Read and follow the snapshotted skill at "+filepath.Join(dir, "reconcile", "SKILL.md")) {
		t.Fatal(args)
	}
	for _, path := range []string{dir, filepath.Join(dir, "snapshot.jsonl"), filepath.Join(dir, "job.json"), filepath.Join(dir, "events.jsonl"), filepath.Join(dir, "sessions")} {
		st, err := os.Stat(path)
		if err != nil {
			t.Fatal(err)
		}
		if st.Mode().Perm()&0077 != 0 {
			t.Fatalf("nonprivate artifact %s: %v", path, st.Mode())
		}
	}
}

func TestNextLiveRecoveryAndWorkerStartFailure(t *testing.T) {
	t.Run("live recovery", func(t *testing.T) {
		f := nextSetup(t)
		f.command("/next", "team@muc.x")
		dir := f.jobs(t)[0]
		awaitNext(t, "running", func() bool { j, _ := loadReconcile(dir); return j != nil && j.State == "running" })
		replacement, wire, _ := messageTransportBridge(t, false)
		replacement.acct = f.b.acct
		replacement.pollReconciles()
		j, _ := loadReconcile(dir)
		if j.State != "running" || !strings.Contains(wire.String(), "running in background") {
			t.Fatalf("live job not recovered: %+v %s", j, wire.String())
		}
		touchNext(t, filepath.Join(f.dir, "release"))
		awaitNext(t, "terminal", func() bool { j, _ := loadReconcile(dir); return j != nil && j.State == "completed" })
		awaitNext(t, "runner unlock", func() bool {
			lock, e := lockFile(filepath.Join(dir, "runner.lock"))
			if e != nil {
				return false
			}
			lock.Close()
			return true
		})
		replacement.pollReconciles()
		if !strings.Contains(wire.String(), "completed") {
			t.Fatal("live recovery lost completion")
		}
	})
	t.Run("worker exec failure", func(t *testing.T) {
		f := nextSetup(t)
		state := Event{"sessionFile": f.sources[0], "model": map[string]any{"provider": "fake", "id": "model"}}
		dir, j, err := prepareReconcile(f.b.acct, state, f.b.acct.Owner, f.pi)
		if err != nil {
			t.Fatal(err)
		}
		j.Pi = filepath.Join(f.dir, "missing-pi")
		if err := saveReconcile(dir, j); err != nil {
			t.Fatal(err)
		}
		gate, err := startReconcile(dir)
		if err != nil {
			t.Fatal(err)
		}
		if err := authorizeReconcile(dir); err != nil {
			t.Fatal(err)
		}
		_, _ = gate.Write([]byte("run\n"))
		gate.Close()
		awaitNext(t, "failed exec", func() bool { j, _ := loadReconcile(dir); return j != nil && j.State == "failed" })
		f.b.pollReconciles()
		if strings.Contains(f.wire.String(), "running in background") || strings.Contains(f.wire.String(), "ran in background") {
			t.Fatal("claimed worker started after exec failure")
		}
	})
	t.Run("readiness failure", func(t *testing.T) {
		f := nextSetup(t)
		state := Event{"sessionFile": f.sources[0], "model": map[string]any{"provider": "fake", "id": "model"}}
		dir, _, err := prepareReconcile(f.b.acct, state, f.b.acct.Owner, f.pi)
		if err != nil {
			t.Fatal(err)
		}
		if err := os.WriteFile(filepath.Join(dir, "job.json"), []byte("broken"), 0600); err != nil {
			t.Fatal(err)
		}
		if gate, err := startReconcile(dir); err == nil {
			gate.Close()
			t.Fatal("runner readiness incorrectly succeeded")
		}
		if strings.Contains(fileText(filepath.Join(f.dir, "commands")), "new_session") {
			t.Fatal("reset during failed preparation")
		}
	})
}

func TestReconcileSkillConfigResolved(t *testing.T) {
	cfg := Config{Accounts: map[string]Account{"default": {JID: "pi@x", Password: "pw", Owner: "owner@x", ReconcileSkill: "/skills/custom.md"}}}
	loaded, err := loadConfig(writeConfig(t, cfg))
	if err != nil {
		t.Fatal(err)
	}
	acct, err := resolveAccount(loaded, "")
	if err != nil {
		t.Fatal(err)
	}
	if acct.ReconcileSkill != "/skills/custom.md" {
		t.Fatalf("skill path lost: %+v", acct)
	}
}

// Blocks one real transport write without holding Bridge.mu. Tests can stop
// settlement after streaming=false, exactly where the old /next check raced.
type nextBlockedWriter struct {
	io.Writer
	entered     chan struct{}
	release     chan struct{}
	once        sync.Once
	releaseOnce sync.Once
}

func (w *nextBlockedWriter) unblock() { w.releaseOnce.Do(func() { close(w.release) }) }

func (w *nextBlockedWriter) Read([]byte) (int, error) { return 0, io.EOF }

func (w *nextBlockedWriter) Write(p []byte) (int, error) {
	w.once.Do(func() { close(w.entered); <-w.release })
	return w.Writer.Write(p)
}

func waitNextSignal(t *testing.T, ch <-chan struct{}) {
	t.Helper()
	select {
	case <-ch:
	case <-time.After(5 * time.Second):
		t.Fatal("fixture failed to reach synchronization point")
	}
}

func TestNextRefusesThroughoutSettlement(t *testing.T) {
	for _, kind := range []string{"recovery", "unanswered", "heartbeat"} {
		t.Run(kind, func(t *testing.T) {
			f := nextSetup(t)
			w := &nextBlockedWriter{Writer: f.wire, entered: make(chan struct{}), release: make(chan struct{})}
			t.Cleanup(w.unblock)
			session, err := xmpp.NewSession(context.Background(), jid.JID{}, jid.JID{}, w, xmpp.Ready,
				xmpp.NewNegotiator(func(*xmpp.Session, *xmpp.StreamConfig) xmpp.StreamConfig { return xmpp.StreamConfig{} }))
			if err != nil {
				t.Fatal(err)
			}
			f.b.xmpp.session = session
			f.b.setStreaming(true)
			switch kind {
			case "recovery":
				f.b.runInbound = 1
			case "unanswered":
				f.b.runInbound, f.b.runDeliveries, f.b.repliedThisRun = 2, 1, true
			case "heartbeat":
				f.b.pendingHeartbeats = []string{"old process heartbeat"}
			}
			done := make(chan struct{})
			go func() { f.b.handleRPCEvent(Event{"type": "agent_settled"}); close(done) }()
			waitNextSignal(t, w.entered)
			if f.b.streaming() {
				t.Fatal("fixture must pause after streaming=false")
			}
			// Make failure reporting nonblocking while the presence write is paused.
			f.b.xmpp.mu.Lock()
			f.b.xmpp.online = false
			f.b.xmpp.mu.Unlock()
			f.command("/next", f.b.acct.Owner)
			if len(f.jobs(t)) != 0 || strings.Contains(fileText(filepath.Join(f.dir, "commands")), "new_session") {
				t.Fatal("/next crossed an in-progress old-session settlement")
			}
			w.unblock()
			waitNextSignal(t, done)
			awaitNext(t, "old-generation synthetic prompt", func() bool { return strings.Contains(fileText(filepath.Join(f.dir, "commands")), `"type":"prompt"`) })
			// The atomic submission reservation also covers the gap before Pi's
			// agent_start. The fake deliberately emits no such lifecycle event.
			f.command("/next", f.b.acct.Owner)
			if len(f.jobs(t)) != 0 {
				t.Fatal("reset before synthetic run settled")
			}
			f.b.settleLocally()
			acceptPromptResponses(f.b, true)
			f.command("/next", f.b.acct.Owner)
			if len(f.jobs(t)) != 1 {
				t.Fatal("/next did not recover after safe settlement")
			}
			commands := fileText(filepath.Join(f.dir, "commands"))
			if strings.LastIndex(commands, `"type":"prompt"`) > strings.Index(commands, `"type":"new_session"`) {
				t.Fatal("old prompt entered fresh primary")
			}
		})
	}
}

func TestNextTransitionRejectsSyntheticAndLateSettle(t *testing.T) {
	f := nextSetup(t)
	// Hold get_state on the real RPC pipe after /next has reserved the idle
	// generation. Events must return immediately, not wait on the session mutex.
	original := f.b.rpc.stdin
	w := &nextBlockedWriter{Writer: original, entered: make(chan struct{}), release: make(chan struct{})}
	t.Cleanup(w.unblock)
	f.b.rpc.stdin = &nextBlockedCloser{nextBlockedWriter: w, Closer: original}
	done := make(chan struct{})
	go func() { f.command("/next", f.b.acct.Owner); close(done) }()
	waitNextSignal(t, w.entered)
	f.b.handleRPCEvent(Event{"type": "agent_settled"})
	if f.b.fireSendRecovery() || f.b.fireUnansweredHint(2, 1, nil) {
		t.Fatal("synthetic prompt reserved during reset")
	}
	f.b.fireHeartbeat("old heartbeat")
	w.unblock()
	waitNextSignal(t, done)
	if len(f.jobs(t)) != 1 {
		t.Fatal("reset failed")
	}
	if strings.Contains(fileText(filepath.Join(f.dir, "commands")), `"type":"prompt"`) {
		t.Fatal("old prompt crossed transition")
	}
}

type nextBlockedCloser struct {
	*nextBlockedWriter
	io.Closer
}

func TestNextQueuedNoticeRetriesAfterReconnect(t *testing.T) {
	f := nextSetup(t)
	// Hold the production cross-persona lock before /next starts its runner.
	lockPath := filepath.Join(f.dir, ".local", "state", "pi-msg", "reconcile.lock")
	if err := os.MkdirAll(filepath.Dir(lockPath), 0700); err != nil {
		t.Fatal(err)
	}
	shared, err := lockFile(lockPath)
	if err != nil {
		t.Fatal(err)
	}
	defer shared.Close()
	f.b.xmpp.mu.Lock()
	f.b.xmpp.online = false
	f.b.xmpp.mu.Unlock()
	f.command("/next", "team@muc.x")
	dir := f.jobs(t)[0]
	awaitNext(t, "waiting shared lock", func() bool { j, _ := loadReconcile(dir); return j != nil && j.State == "queued" })
	f.b.pollReconciles()
	if strings.Contains(f.wire.String(), "new session ready") {
		t.Fatal("offline send claimed success")
	}
	if _, err := os.Stat(filepath.Join(dir, "queued-reported")); !os.IsNotExist(err) {
		t.Fatal("offline notice marked delivered")
	}
	// Replace the bridge, exercising the persisted intent rather than in-memory retry.
	replacement, wire, _ := messageTransportBridge(t, false)
	replacement.acct = f.b.acct
	replacement.setTurnDest(f.b.acct.Owner, false)
	replacement.pollReconciles()
	if !strings.Contains(wire.String(), "new session ready") || !strings.Contains(wire.String(), "queued in background") || !strings.Contains(wire.String(), `to="team@muc.x"`) {
		t.Fatalf("queued notice not recovered: %s", wire.String())
	}
	queued := wire.String()
	replacement.pollReconciles()
	if wire.String() != queued {
		t.Fatal("duplicate queued notice")
	}
	shared.Close()
	awaitNext(t, "worker running", func() bool { j, _ := loadReconcile(dir); return j != nil && j.State == "running" })
	replacement.pollReconciles()
	touchNext(t, filepath.Join(f.dir, "release"))
	awaitNext(t, "completed", func() bool { j, _ := loadReconcile(dir); return j != nil && j.State == "completed" })
	awaitNext(t, "reported", func() bool {
		replacement.pollReconciles()
		_, err := os.Stat(filepath.Join(dir, "reported"))
		return err == nil
	})
	text := wire.String()
	if strings.Index(text, "queued in background") >= strings.Index(text, "running in background") || strings.Index(text, "running in background") >= strings.Index(text, "] completed") {
		t.Fatalf("notice ordering: %s", text)
	}
}

func TestNextKilledRunnerSurvivorRetainsLocks(t *testing.T) {
	f := nextSetup(t)
	f.command("/next", "team@muc.x")
	first := f.jobs(t)[0]
	awaitNext(t, "first worker", func() bool {
		j, _ := loadReconcile(first)
		return j != nil && j.State == "running" && fileText(filepath.Join(first, "worker-pid")) != ""
	})
	j, _ := loadReconcile(first)
	if err := syscall.Kill(j.RunnerPID, syscall.SIGKILL); err != nil {
		t.Fatal(err)
	}
	awaitNext(t, "runner reaped", func() bool { return syscall.Kill(j.RunnerPID, 0) == syscall.ESRCH })
	f.command("/next", f.b.acct.Owner)
	var second string
	for _, dir := range f.jobs(t) {
		if dir != first {
			second = dir
		}
	}
	awaitNext(t, "second queued", func() bool { j, _ := loadReconcile(second); return j != nil && j.State == "queued" })
	for n := 0; n < 3; n++ {
		f.b.pollReconciles()
		live, _ := loadReconcile(first)
		if live.State != "running" {
			t.Fatalf("surviving worker prematurely interrupted: %+v", live)
		}
		if _, err := os.Stat(filepath.Join(first, "reported")); !os.IsNotExist(err) {
			t.Fatal("terminal notice while worker survives")
		}
		if _, err := os.Stat(filepath.Join(second, "started")); !os.IsNotExist(err) {
			t.Fatal("second worker overlapped survivor")
		}
	}
	// Exit just the survivor; second must acquire the inherited shared lock.
	touchNext(t, filepath.Join(first, "release"))
	awaitNext(t, "second worker starts", func() bool { j, _ := loadReconcile(second); return j != nil && j.State == "running" })
	f.b.pollReconciles()
	dead, _ := loadReconcile(first)
	if dead.State != "interrupted" || !strings.Contains(dead.Summary, "effects may be partial") {
		t.Fatalf("dead runner outcome: %+v", dead)
	}
	f.b.pollReconciles()
	if fileText(filepath.Join(first, "start-count")) != "start\n" {
		t.Fatal("dead first job replayed")
	}
	if !strings.Contains(f.wire.String(), "never automatically replayed") {
		t.Fatal("missing effects-unknown recovery warning")
	}
}

func TestNextRefusesReservedSyntheticSubmission(t *testing.T) {
	for _, kind := range []string{"recovery", "unanswered", "heartbeat"} {
		t.Run(kind, func(t *testing.T) {
			f := nextSetup(t)
			original := f.b.rpc.stdin
			w := &nextBlockedWriter{Writer: original, entered: make(chan struct{}), release: make(chan struct{})}
			t.Cleanup(w.unblock)
			f.b.rpc.stdin = &nextBlockedCloser{nextBlockedWriter: w, Closer: original}
			done := make(chan struct{})
			go func() {
				switch kind {
				case "recovery":
					f.b.fireSendRecovery()
				case "unanswered":
					f.b.fireUnansweredHint(2, 1, nil)
				case "heartbeat":
					f.b.fireHeartbeat("old heartbeat")
				}
				close(done)
			}()
			waitNextSignal(t, w.entered)
			f.command("/next", f.b.acct.Owner)
			if len(f.jobs(t)) != 0 {
				t.Fatal("reset crossed a reserved synthetic submission")
			}
			w.unblock()
			waitNextSignal(t, done)
			f.command("/next", f.b.acct.Owner)
			if len(f.jobs(t)) != 0 {
				t.Fatal("reset crossed the pre-agent_start gap")
			}
		})
	}
}

func TestNextResetAuthorityCrashGaps(t *testing.T) {
	for _, kind := range []string{"before authority", "after authority before gate", "run without authority"} {
		t.Run(kind, func(t *testing.T) {
			f := nextSetup(t)
			state := Event{"sessionFile": f.sources[0], "model": map[string]any{"provider": "fake", "id": "model"}}
			dir, _, err := prepareReconcile(f.b.acct, state, "team@muc.x", f.pi)
			if err != nil {
				t.Fatal(err)
			}
			gate, err := startReconcile(dir)
			if err != nil {
				t.Fatal(err)
			}
			if kind == "after authority before gate" {
				if err := authorizeReconcile(dir); err != nil {
					t.Fatal(err)
				}
			}
			if kind == "run without authority" {
				_, _ = gate.Write([]byte("run\n"))
			}
			gate.Close()
			awaitNext(t, "interrupted authorization gap", func() bool { j, _ := loadReconcile(dir); return j != nil && j.State == "interrupted" })
			awaitNext(t, "reported gap", func() bool {
				f.b.pollReconciles()
				_, err := os.Stat(filepath.Join(dir, "reported"))
				return err == nil
			})
			if got := strings.Contains(f.wire.String(), "new session ready"); got != (kind == "after authority before gate") {
				t.Fatalf("false reset claim or lost persisted intent: %s", f.wire.String())
			}
			if _, err := os.Stat(filepath.Join(dir, "started")); !os.IsNotExist(err) {
				t.Fatal("worker ran across authorization gap")
			}
			f.b.pollReconciles()
		})
	}
}
