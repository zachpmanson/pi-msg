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

func TestNextBoundaryStoreRoundTrip(t *testing.T) {
	path := filepath.Join(t.TempDir(), "private", "pending.json")
	if r, err := loadNextBoundary(path); err != nil || r != nil {
		t.Fatalf("missing = %+v, %v", r, err)
	}
	r, err := newNextBoundaryRecord("owner@example", "/next", time.Unix(123, 0).UTC())
	if err != nil {
		t.Fatal(err)
	}
	other, err := newNextBoundaryRecord("room@example", "/next", r.AcceptedAt)
	if err != nil || other.ID == r.ID || len(r.ID) != 32 {
		t.Fatalf("random identity: %+v %v", other, err)
	}
	r.OldSessionID, r.OldSessionFile = "old", "/old.jsonl"
	r.FreshSessionID, r.FreshSessionFile = "fresh", "/fresh.jsonl"
	r.JobDir, r.LastError = "/job", "diagnostic"
	for _, phase := range []nextBoundaryPhase{nextBoundaryWaiting, nextBoundaryPreparing, nextBoundaryResetting, nextBoundaryResetConfirmed, nextBoundaryReleasing, nextBoundaryBlocked} {
		r.Phase = phase
		if err := saveNextBoundary(path, r); err != nil {
			t.Fatal(err)
		}
		got, err := loadNextBoundary(path)
		if err != nil || !reflect.DeepEqual(got, r) {
			t.Fatalf("roundtrip = %+v, %v", got, err)
		}
	}
	for p, want := range map[string]os.FileMode{path: 0o600, filepath.Dir(path): 0o700} {
		st, err := os.Stat(p)
		if err != nil {
			t.Fatal(err)
		}
		if st.Mode().Perm() != want {
			t.Fatalf("%s mode = %o", p, st.Mode().Perm())
		}
	}
	if err := removeNextBoundary(path); err != nil {
		t.Fatal(err)
	}
	if err := removeNextBoundary(path); err != nil {
		t.Fatal(err)
	}
	if got, err := loadNextBoundary(path); err != nil || got != nil {
		t.Fatalf("removed = %+v %v", got, err)
	}
}

func TestNextBoundaryStoreRejectsMalformed(t *testing.T) {
	path := filepath.Join(t.TempDir(), "pending.json")
	good, err := newNextBoundaryRecord("owner", "/next", time.Unix(1, 0))
	if err != nil {
		t.Fatal(err)
	}
	for _, mutate := range []func(*nextBoundaryRecord){
		func(r *nextBoundaryRecord) { r.Version++ },
		func(r *nextBoundaryRecord) { r.ID = "" },
		func(r *nextBoundaryRecord) { r.Phase = "done" },
		func(r *nextBoundaryRecord) { r.Destination = "" },
		func(r *nextBoundaryRecord) { r.Command = "" },
		func(r *nextBoundaryRecord) { r.AcceptedAt = time.Time{} },
	} {
		r := *good
		mutate(&r)
		if err := saveNextBoundary(path, &r); err == nil {
			t.Fatalf("saved invalid %+v", r)
		}
		b, _ := json.Marshal(r)
		if err := os.WriteFile(path, b, 0o600); err != nil {
			t.Fatal(err)
		}
		if _, err := loadNextBoundary(path); err == nil {
			t.Fatalf("loaded invalid %+v", r)
		}
	}
	for _, b := range []string{"{", "null", "{}", `{"version":1,"unknown":true}`, `{} {}`} {
		if err := os.WriteFile(path, []byte(b), 0o600); err != nil {
			t.Fatal(err)
		}
		if _, err := loadNextBoundary(path); err == nil {
			t.Fatalf("loaded %q", b)
		}
	}
	if _, err := loadNextBoundary(filepath.Dir(path)); err == nil {
		t.Fatal("loaded directory")
	}
}

func TestNextBoundaryStoreFailurePreservesState(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "pending.json")
	r, err := newNextBoundaryRecord("owner", "/next", time.Unix(1, 0))
	if err != nil {
		t.Fatal(err)
	}
	if err := saveNextBoundary(path, r); err != nil {
		t.Fatal(err)
	}
	before, err := os.ReadFile(path)
	if err != nil {
		t.Fatal(err)
	}
	r.AcceptedAt = time.Date(10000, 1, 1, 0, 0, 0, 0, time.UTC)
	if err := saveNextBoundary(path, r); err == nil {
		t.Fatal("invalid time saved")
	}
	after, _ := os.ReadFile(path)
	if string(before) != string(after) {
		t.Fatal("failed save changed previous record")
	}
	r.AcceptedAt = time.Unix(1, 0)
	blocked := filepath.Join(dir, "blocked")
	if err := os.Mkdir(blocked, 0o700); err != nil {
		t.Fatal(err)
	}
	marker := filepath.Join(blocked, "keep")
	if err := os.WriteFile(marker, []byte("prior"), 0o600); err != nil {
		t.Fatal(err)
	}
	if err := saveNextBoundary(blocked, r); err == nil || !strings.Contains(err.Error(), "rename") {
		t.Fatalf("rename failure: %v", err)
	}
	if b, err := os.ReadFile(marker); err != nil || string(b) != "prior" {
		t.Fatalf("rename changed destination: %q %v", b, err)
	}
	if err := removeNextBoundary(blocked); err == nil {
		t.Fatal("removed nonempty directory")
	}
	temps, _ := filepath.Glob(filepath.Join(dir, ".blocked-*"))
	if len(temps) != 0 {
		t.Fatalf("leaked temps: %v", temps)
	}
}
