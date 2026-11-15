package pimsg

import (
	"bytes"
	"encoding/json"
	"errors"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"
)

func TestInboxCorruptLoadCannotBeOverwrittenByOrdinaryMutation(t *testing.T) {
	at := time.Unix(1, 0).UTC()
	legacy, err := json.Marshal(inboxEntry{ID: "ordinary", Body: "old", At: at})
	if err != nil {
		t.Fatal(err)
	}
	for name, bad := range map[string]string{
		"protected decode": `{"key":"held","nextBoundaryID":"b","body":"retain","at":"invalid"}`,
		"scanner limit":    strings.Repeat("x", maxInboxRecordBytes+2),
	} {
		for _, operation := range []string{"append", "drop", "ack"} {
			t.Run(name+"/"+operation, func(t *testing.T) {
				path := filepath.Join(t.TempDir(), "inbox.jsonl")
				original := append(append(append([]byte(nil), legacy...), '\n'), []byte(bad+"\n")...)
				if err := os.WriteFile(path, original, 0600); err != nil {
					t.Fatal(err)
				}
				in := newInbox(path, nil)
				if in.loadError() == nil || in.len() != 1 {
					t.Fatalf("expected retained diagnostic and readable legacy entry: %v, %d", in.loadError(), in.len())
				}
				switch operation {
				case "append":
					in.append(inboxEntry{ID: "new", Body: "new", At: at})
				case "drop":
					if n := in.drop("ordinary", "", ""); n != 0 {
						t.Fatalf("reported removal after failed load: %d", n)
					}
				case "ack":
					if n, err := in.ackSettledErr(ackPolicy{Now: at.Add(time.Hour)}); err == nil || n != 0 {
						t.Fatalf("acknowledged damaged storage: n=%d err=%v", n, err)
					}
				}
				got, err := os.ReadFile(path)
				if err != nil || !bytes.Equal(got, original) || in.len() != 1 {
					t.Fatalf("mutation changed damaged source or memory: err=%v entries=%d", err, in.len())
				}
			})
		}
	}
}

func TestInboxProtectedEncodingBound(t *testing.T) {
	for name, body := range map[string]string{
		"large body": strings.Repeat("x", 1<<20),
		"escaping":   strings.Repeat("\"", 1<<19),
	} {
		t.Run(name, func(t *testing.T) {
			path := filepath.Join(t.TempDir(), "inbox.jsonl")
			in := newInbox(path, nil)
			if _, err := in.appendProtected(inboxEntry{Key: "kept", NextBoundaryID: "b", Body: "keep"}); err != nil {
				t.Fatal(err)
			}
			before, err := os.ReadFile(path)
			if err != nil {
				t.Fatal(err)
			}
			if _, err := in.appendProtected(inboxEntry{Key: "oversize", NextBoundaryID: "b", Body: body}); err == nil {
				t.Fatal("accepted record that exceeds restart scanner bound")
			}
			after, err := os.ReadFile(path)
			if err != nil || !bytes.Equal(before, after) || in.len() != 1 {
				t.Fatalf("failed admission changed authoritative state: %v", err)
			}
		})
	}
	t.Run("exact accepted limit roundtrips", func(t *testing.T) {
		path := filepath.Join(t.TempDir(), "inbox.jsonl")
		e := inboxEntry{Key: "near-limit", NextBoundaryID: "b", At: time.Unix(1, 0).UTC()}
		base, err := json.Marshal(e)
		if err != nil {
			t.Fatal(err)
		}
		e.Body = strings.Repeat("x", maxInboxRecordBytes-len(base))
		in := newInbox(path, nil)
		if _, err := in.appendProtected(e); err != nil {
			t.Fatal(err)
		}
		reloaded := newInbox(path, nil)
		got := reloaded.protectedPending()
		if reloaded.loadError() != nil || len(got) != 1 || got[0].Body != e.Body {
			t.Fatalf("accepted limit did not reload: err=%v entries=%d", reloaded.loadError(), len(got))
		}
	})
}

func TestNextBoundaryStoreRemovalRetrySyncsAbsentPath(t *testing.T) {
	path := filepath.Join(t.TempDir(), "pending.json")
	if err := os.WriteFile(path, []byte("old"), 0600); err != nil {
		t.Fatal(err)
	}
	injected := errors.New("directory sync failed")
	if err := durableRemoveWithSync(path, func(string) error { return injected }); !errors.Is(err, injected) {
		t.Fatalf("missing uncertain-removal failure: %v", err)
	}
	if _, err := os.Stat(path); !os.IsNotExist(err) {
		t.Fatalf("unlink did not precede injected sync failure: %v", err)
	}
	calls := 0
	if err := durableRemoveWithSync(path, func(dir string) error {
		calls++
		return syncDirectory(dir)
	}); err != nil || calls == 0 {
		t.Fatalf("absent-path retry did not establish durable absence: calls=%d err=%v", calls, err)
	}
}

func TestNextBoundaryStoreCreatedDirectoryLinksAndRetry(t *testing.T) {
	base := t.TempDir()
	private := filepath.Join(base, "private")
	leaf := filepath.Join(private, "nested")
	path := filepath.Join(leaf, "pending.json")
	injected := errors.New("created-link sync failed")
	if err := durableWritePrivateWithSync(path, []byte("value"), func(dir string) error {
		if dir == private {
			return injected
		}
		return syncDirectory(dir)
	}); !errors.Is(err, injected) {
		t.Fatalf("accepted uncommitted directory link: %v", err)
	}
	if _, err := os.Stat(leaf); err != nil {
		t.Fatal("test must leave uncertain directories for retry")
	}
	var synced []string
	if err := durableWritePrivateWithSync(path, []byte("value"), func(dir string) error {
		synced = append(synced, dir)
		return syncDirectory(dir)
	}); err != nil {
		t.Fatal(err)
	}
	position := func(dir string) int {
		for i, got := range synced {
			if got == dir {
				return i
			}
		}
		return -1
	}
	if position(leaf) != 0 || position(private) <= position(leaf) || position(base) <= position(private) || position(filepath.Dir(base)) <= position(base) {
		t.Fatalf("retry assumed uncertain links were already durable: %v", synced)
	}
	got, err := os.ReadFile(path)
	if err != nil || string(got) != "value" {
		t.Fatalf("successful retry missing data: %q %v", got, err)
	}
}
