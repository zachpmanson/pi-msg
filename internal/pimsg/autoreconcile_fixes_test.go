package pimsg

import (
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"
	"reflect"
	"strings"
	"testing"
	"time"
)

func TestAutoreconcileReloadReducedQuotasPreservesAttemptSeed(t *testing.T) {
	for _, tc := range []struct{ old, recent, owner int }{{10, 0, 0}, {100, 1, 2}} {
		t.Run(fmt.Sprint(tc), func(t *testing.T) {
			b := autoTestBridge(t)
			b.acct.AutoreconcileHistoryMessages = tc.old
			b.acct.AutoreconcileOwnerHistoryMessages = tc.old
			for n := 0; n < tc.old; n++ {
				b.captureAutoInbound(InboundMessage{Direct: true, From: "owner@x/phone", FromOwner: true, ID: fmt.Sprint(n), Body: fmt.Sprint("owner-", n), Stamp: time.Unix(int64(n+1), 0)})
			}
			for n := 0; n < tc.old; n++ {
				b.captureAutoInbound(InboundMessage{Room: "room@muc.x", From: "room@muc.x/peer", RealJID: "peer@x", ID: fmt.Sprint(n), Body: fmt.Sprint("peer-", n), Stamp: time.Unix(int64(n+1000), 0)})
			}
			seed := b.auto.state.History.seed("/source", "/job")
			a := &autoAttempt{Token: "immutable", Seed: seed}
			if err := b.autoSave(a, "worker"); err != nil {
				t.Fatal(err)
			}
			b.acct.AutoreconcileHistoryMessages = tc.recent
			b.acct.AutoreconcileOwnerHistoryMessages = tc.owner
			r := NewBridge(b.acct, false)
			if err := r.loadAuto(); err != nil {
				t.Fatal(err)
			}
			if len(r.auto.state.History.Recent) != tc.recent || len(r.auto.state.History.Owner) != tc.owner || r.auto.state.Attempt.Seed != seed {
				t.Fatal("quotas/immutable seed", r.auto.state)
			}
			var disk autoState
			if err := json.Unmarshal([]byte(fileText(r.autoPath())), &disk); err != nil {
				t.Fatal(err)
			}
			if !reflect.DeepEqual(disk.History, r.auto.state.History) || disk.Attempt.Seed != seed {
				t.Fatal("trim not durable")
			}
			// No owner arrives after restart: peer-only activity must never revive the old floor.
			r.captureAutoInbound(InboundMessage{Room: "room@muc.x", From: "room@muc.x/peer", RealJID: "peer@x", ID: "post-restart", Body: "new peer", Stamp: time.Now()})
			selected := r.auto.state.History.selected()
			if len(selected) != tc.recent+tc.owner {
				t.Fatal("wrong reduced union", selected)
			}
			for _, e := range selected {
				if e.Owner && e.Body != "owner-98" && e.Body != "owner-99" {
					t.Fatal("wrong retained owner", e)
				}
			}
			if tc.recent > 0 && selected[len(selected)-1].Body != "new peer" {
				t.Fatal("new peer not selected")
			}
		})
	}
}

func TestAutoreconcileUnsaturatedDedupPreservesFullRecords(t *testing.T) {
	b := autoTestBridge(t)
	m := InboundMessage{Direct: true, From: "owner@x/phone", FromOwner: true, ID: "stable", Body: "original", Stamp: time.Unix(1, 0)}
	b.captureAutoInbound(m)
	before := b.auto.state.History.selected()
	disk := fileText(b.autoPath())
	m.Body = "changed duplicate"
	m.Stamp = time.Now()
	b.captureAutoInbound(m)
	if !reflect.DeepEqual(before, b.auto.state.History.selected()) || disk != fileText(b.autoPath()) {
		t.Fatal("duplicate replaced original full key/body/time")
	}
}

func writeAutoTargetReceipt(t *testing.T, b *Bridge, a *autoAttempt, file, id string) {
	t.Helper()
	raw := fmt.Sprintf("{\"type\":\"session\",\"version\":3,\"id\":%q,\"cwd\":%q}\n", strings.TrimPrefix(id, "id:"), filepath.Dir(file))
	seed, _ := json.Marshal(Event{"type": "custom_message", "customType": "pi-msg-idle-seed", "content": a.Seed, "details": map[string]any{"token": a.Token}})
	if err := durableWritePrivate(file, append([]byte(raw), append(seed, '\n')...)); err != nil {
		t.Fatal(err)
	}
	receipt, _ := json.Marshal(Event{"token": a.Token, "sourceID": a.SourceID, "status": "success", "id": id, "file": file, "seedHash": autoHash([]byte(a.Seed))})
	if err := durableWritePrivate(b.autoRequestPath()+".receipt", receipt); err != nil {
		t.Fatal(err)
	}
}

func TestAutoreconcileRetiredFenceCrashRecovery(t *testing.T) {
	for _, mode := range []string{"unlink", "unlink-sync-failure", "missing-pointer", "wrong-pointer", "wrong-receipt", "missing-receipt", "wrong-header", "wrong-seed", "unbound-orphan", "wrong-target-orphan", "bound-orphan"} {
		t.Run(mode, func(t *testing.T) {
			b := autoTestBridge(t)
			a := &autoAttempt{Token: "retired", SourceID: "id:source", SourceFile: "/source", Seed: "immutable history"}
			if err := b.beginAutoBoundary(a); err != nil {
				t.Fatal(err)
			}
			if err := b.autoSave(a, "resetting"); err != nil {
				t.Fatal(err)
			}
			target := filepath.Join(t.TempDir(), "native.jsonl")
			writeAutoTargetReceipt(t, b, a, target, "id:target")
			if err := durableWritePrivate(sessionStatePath(b.acct.Name), []byte(target+"\n")); err != nil {
				t.Fatal(err)
			}
			if mode == "unlink-sync-failure" {
				err := durableRemoveWithSync(b.nextPath(), func(string) error { return os.ErrPermission })
				if err == nil {
					t.Fatal("expected post-unlink sync failure")
				}
				b.autoUncertain(a, err)
			} else if err := b.clearAutoBoundary(a); err != nil {
				t.Fatal(err)
			}
			if _, err := os.Stat(b.nextPath()); !os.IsNotExist(err) {
				t.Fatal("actual unlink did not happen", err)
			}
			switch mode {
			case "missing-pointer":
				if err := os.Remove(sessionStatePath(b.acct.Name)); err != nil {
					t.Fatal(err)
				}
			case "wrong-pointer":
				if err := durableWritePrivate(sessionStatePath(b.acct.Name), []byte("/other\n")); err != nil {
					t.Fatal(err)
				}
			case "wrong-receipt":
				a.Token = "other"
				writeAutoTargetReceipt(t, b, a, target, "id:target")
			case "missing-receipt":
				if err := os.Remove(b.autoRequestPath() + ".receipt"); err != nil {
					t.Fatal(err)
				}
			case "wrong-header", "wrong-seed":
				old, replacement := `"id":"target"`, `"id":"other"`
				if mode == "wrong-seed" {
					old, replacement = a.Seed, "changed history"
				}
				if err := durableWritePrivate(target, []byte(strings.Replace(fileText(target), old, replacement, 1))); err != nil {
					t.Fatal(err)
				}
			case "unbound-orphan", "wrong-target-orphan", "bound-orphan":
				id := "id:target"
				if mode == "unbound-orphan" {
					id = ""
				}
				if mode == "wrong-target-orphan" {
					id = "id:other"
				}
				if _, err := b.inbox.appendProtected(inboxEntry{ID: "held", Body: "held", NextBoundaryID: a.Token, TargetSessionID: id, At: time.Now()}); err != nil {
					t.Fatal(err)
				}
			}
			r := NewBridge(b.acct, false)
			err := r.loadAuto()
			badProof := mode == "missing-pointer" || mode == "wrong-pointer" || mode == "wrong-receipt" || mode == "missing-receipt" || mode == "wrong-header" || mode == "wrong-seed"
			if badProof {
				if err == nil {
					t.Fatal("missing fence accepted without exact proof")
				}
				return
			}
			if err != nil {
				t.Fatal(err)
			}
			got := r.auto.state.Attempt
			if got.Phase != "blocked" || got.TargetID != "id:target" || got.TargetFile != target || got.Seed != "immutable history" || r.presenceShow() == "xa" {
				t.Fatal("wrong recovered association", got)
			}
			r.inbox = newInbox(inboxPath(r.acct.Name), r.log)
			err = r.loadNextQueue()
			badOrphan := mode == "unbound-orphan" || mode == "wrong-target-orphan"
			if badOrphan {
				if err == nil {
					t.Fatal("unsafe orphan accepted")
				}
				return
			}
			if err != nil {
				t.Fatal(err)
			}
			if mode == "bound-orphan" && (r.next.record == nil || r.next.record.FreshSessionID != "id:target") {
				t.Fatal("exact orphan fence not reconstructed")
			}
			before := fileText(r.autoPath())
			if err := r.loadAuto(); err != nil {
				t.Fatal(err)
			}
			if before != fileText(r.autoPath()) {
				t.Fatal("recovery not idempotent")
			}
		})
	}
}

func TestAutoreconcileLateOutcomeExactReleaseAndRestart(t *testing.T) {
	for _, source := range []bool{false, true} {
		t.Run(fmt.Sprint("source=", source), func(t *testing.T) {
			f := nextSetup(t)
			configureAutoFixture(f)
			a := &autoAttempt{Token: "late", SourceID: "id:s0", SourceFile: f.sources[0], Seed: "history"}
			if err := f.b.beginAutoBoundary(a); err != nil {
				t.Fatal(err)
			}
			f.b.autoUncertain(a, os.ErrDeadlineExceeded)
			f.incoming("protected late input", "held-late")
			f.step()
			if strings.Contains(fileText(f.dir+"/commands"), `"type":"prompt"`) {
				t.Fatal("unknown outcome submitted input")
			}
			id, file := "id:s1", f.sources[1]
			if source {
				id, file = a.SourceID, a.SourceFile
				raw, _ := json.Marshal(Event{"token": a.Token, "sourceID": a.SourceID, "sourceAuthoritative": true, "status": "cancelled"})
				if err := durableWritePrivate(f.b.autoRequestPath()+".receipt", raw); err != nil {
					t.Fatal(err)
				}
			} else {
				writeAutoTargetReceipt(t, f.b, a, file, id)
				if res, err := f.b.rpc.NewSession(f.b.ctx); err != nil || !res.success() {
					t.Fatal(err, res)
				}
			}
			autoIteration(t, f.b, nil)
			if f.b.auto.state.Attempt.Phase != "blocked" || f.b.presenceShow() == "xa" {
				t.Fatal("late outcome reauthorized cleanup")
			}
			f.step()
			acceptProtected(t, f, true)
			f.step()
			held := f.b.inbox.protectedPending()
			if len(held) != 1 || held[0].TargetSessionID != id || f.boundary(t) != nil || loadSessionState(f.b.acct.Name) != file {
				t.Fatal("late exact release/retirement failed", held)
			}
			before := fileText(f.dir + "/commands")
			// Retired source outcomes are already terminal; receipt is not replay authority.
			if err := f.b.loadAuto(); err != nil {
				t.Fatal(err)
			}
			if err := restartNextFixture(t, f); err != nil {
				t.Fatal(err)
			}
			f.step()
			acceptProtected(t, f, true)
			f.step()
			after := fileText(f.dir + "/commands")
			if strings.Count(after, `"type":"new_session"`) != strings.Count(before, `"type":"new_session"`) || len(f.jobs(t)) != 0 || !strings.Contains(after, "prompt-session-"+strings.TrimPrefix(id, "id:s")) {
				t.Fatal("recovery reset/replayed worker or lost target", after)
			}
			for _, e := range f.b.inbox.protectedPending() {
				if e.TargetSessionID != id {
					t.Fatal("retargeted", e)
				}
			}
		})
	}
}

// An old recovered auto attempt cannot become a permanent authority over later
// independently accepted manual /next targets and their unconsumed input.
func TestAutoreconcileRetiredRecoveryThenManualNextOrphanRestart(t *testing.T) {
	f := nextSetup(t)
	configureAutoFixture(f)
	a := &autoAttempt{Token: "old-retired-auto", SourceID: "id:s0", SourceFile: f.sources[0], Seed: "original seed"}
	if err := f.b.beginAutoBoundary(a); err != nil {
		t.Fatal(err)
	}
	if err := f.b.autoSave(a, "resetting"); err != nil {
		t.Fatal(err)
	}
	writeAutoTargetReceipt(t, f.b, a, f.sources[1], "id:s1")
	if res, err := f.b.rpc.NewSession(f.b.ctx); err != nil || !res.success() {
		t.Fatal(err, res)
	}
	if err := durableWritePrivate(sessionStatePath(f.b.acct.Name), []byte(f.sources[1]+"\n")); err != nil {
		t.Fatal(err)
	}
	if err := f.b.clearAutoBoundary(a); err != nil {
		t.Fatal(err)
	}
	if err := f.b.loadAuto(); err != nil {
		t.Fatal(err)
	}
	if f.b.auto.state.Attempt.Error != autoRetiredFenceRecovered {
		t.Fatal("fixture did not recover retired auto attempt")
	}
	f.incoming("/next", "later-manual")
	manual := f.boundary(t)
	if manual == nil || manual.ID == a.Token {
		t.Fatal("new manual boundary missing")
	}
	f.incoming("later manual held input", "later-held")
	f.step()
	acceptProtected(t, f, true)
	f.step()
	held := f.b.inbox.protectedPending()
	if f.boundary(t) != nil || len(held) != 1 || held[0].NextBoundaryID != manual.ID || held[0].TargetSessionID != "id:s2" {
		t.Fatal("manual target did not retire with accepted unconsumed input", held)
	}
	before := fileText(f.dir + "/commands")
	jobs := len(f.jobs(t))
	if err := f.b.loadAuto(); err != nil {
		t.Fatal(err)
	}
	if err := restartNextFixture(t, f); err != nil {
		t.Fatal("historical auto attempt hijacked newer manual orphan", err)
	}
	recovered := f.boundary(t)
	if recovered == nil || recovered.ID != manual.ID || recovered.FreshSessionID != "id:s2" || recovered.FreshSessionFile != f.sources[2] {
		t.Fatal("manual exact-target recovery changed", recovered)
	}
	f.step()
	acceptProtected(t, f, true)
	f.step()
	after := fileText(f.dir + "/commands")
	if f.boundary(t) != nil || strings.Count(after, `"type":"new_session"`) != strings.Count(before, `"type":"new_session"`) || len(f.jobs(t)) != jobs {
		t.Fatal("manual orphan recovery replayed reset/worker")
	}
	held = f.b.inbox.protectedPending()
	if len(held) != 1 || held[0].TargetSessionID != "id:s2" || held[0].NextBoundaryID != manual.ID {
		t.Fatal("manual input retargeted/lost", held)
	}
}
