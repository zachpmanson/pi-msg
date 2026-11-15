package pimsg

// Storage only: none of these helpers admits input, resets a session, or infers
// that a reset succeeded. The caller must supply any proven session identity.
import (
	"crypto/rand"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"strings"
	"time"
)

const nextBoundaryVersion = 1

type nextBoundaryPhase string

const (
	nextBoundaryWaiting        nextBoundaryPhase = "waiting"
	nextBoundaryPreparing      nextBoundaryPhase = "preparing"
	nextBoundaryResetting      nextBoundaryPhase = "resetting"
	nextBoundaryResetConfirmed nextBoundaryPhase = "reset-confirmed"
	nextBoundaryReleasing      nextBoundaryPhase = "releasing"
	nextBoundaryBlocked        nextBoundaryPhase = "blocked"
)

type nextBoundaryRecord struct {
	Version          int               `json:"version"`
	ID               string            `json:"id"`
	Phase            nextBoundaryPhase `json:"phase"`
	Destination      string            `json:"destination"`
	Command          string            `json:"command"`
	AcceptedAt       time.Time         `json:"acceptedAt"`
	OldSessionID     string            `json:"oldSessionID,omitempty"`
	OldSessionFile   string            `json:"oldSessionFile,omitempty"`
	FreshSessionID   string            `json:"freshSessionID,omitempty"`
	FreshSessionFile string            `json:"freshSessionFile,omitempty"`
	JobDir           string            `json:"jobDir,omitempty"`
	LastError        string            `json:"lastError,omitempty"`
}

func randomLocalKey() (string, error) {
	var b [16]byte
	if _, err := rand.Read(b[:]); err != nil {
		return "", fmt.Errorf("generate local identity: %w", err)
	}
	return hex.EncodeToString(b[:]), nil
}

func newNextBoundaryRecord(destination, command string, at time.Time) (*nextBoundaryRecord, error) {
	id, err := randomLocalKey()
	if err != nil {
		return nil, err
	}
	r := &nextBoundaryRecord{Version: nextBoundaryVersion, ID: id, Phase: nextBoundaryWaiting, Destination: destination, Command: command, AcceptedAt: at}
	if err := r.validate(); err != nil {
		return nil, err
	}
	return r, nil
}

func (r *nextBoundaryRecord) validate() error {
	if r == nil {
		return fmt.Errorf("nil pending boundary")
	}
	if r.Version != nextBoundaryVersion {
		return fmt.Errorf("unsupported pending boundary version %d", r.Version)
	}
	if strings.TrimSpace(r.ID) == "" || strings.TrimSpace(r.Destination) == "" || strings.TrimSpace(r.Command) == "" || r.AcceptedAt.IsZero() {
		return fmt.Errorf("pending boundary requires identity, destination, command and accepted timestamp")
	}
	switch r.Phase {
	case nextBoundaryWaiting, nextBoundaryPreparing, nextBoundaryResetting, nextBoundaryResetConfirmed, nextBoundaryReleasing, nextBoundaryBlocked:
	default:
		return fmt.Errorf("invalid pending boundary phase %q", r.Phase)
	}
	return nil
}

func loadNextBoundary(path string) (*nextBoundaryRecord, error) {
	b, err := os.ReadFile(path)
	if os.IsNotExist(err) {
		return nil, nil
	}
	if err != nil {
		return nil, fmt.Errorf("load pending boundary: %w", err)
	}
	var r nextBoundaryRecord
	d := json.NewDecoder(strings.NewReader(string(b)))
	d.DisallowUnknownFields()
	if err := d.Decode(&r); err != nil {
		return nil, fmt.Errorf("decode pending boundary: %w", err)
	}
	if err := d.Decode(new(any)); err != io.EOF {
		return nil, fmt.Errorf("pending boundary has trailing data")
	}
	if err := r.validate(); err != nil {
		return nil, err
	}
	return &r, nil
}

func saveNextBoundary(path string, r *nextBoundaryRecord) error {
	if err := r.validate(); err != nil {
		return err
	}
	b, err := json.Marshal(r)
	if err != nil {
		return err
	}
	return durableWritePrivate(path, append(b, '\n'))
}

func removeNextBoundary(path string) error { return durableRemove(path) }

// A directory-sync error after rename/removal means durability is uncertain;
// callers must not report durable success or proceed with boundary effects.
func syncDirectory(path string) error {
	d, err := os.Open(path)
	if err != nil {
		return err
	}
	defer d.Close()
	return d.Sync()
}

// Sync every existing ancestor: a previous failed attempt may have created a
// directory whose link is still uncertain. Merely observing it on retry does
// not establish durability. Missing directories on removal are skipped until
// their nearest existing parent, whose sync commits their absence.
func syncDirectoryAncestry(dir string, syncDir func(string) error) error {
	current, err := filepath.Abs(dir)
	if err != nil {
		return err
	}
	for {
		if _, err := os.Stat(current); err == nil {
			if err := syncDir(current); err != nil {
				return fmt.Errorf("sync directory %s: %w", current, err)
			}
		} else if !os.IsNotExist(err) {
			return err
		}
		parent := filepath.Dir(current)
		if parent == current {
			return nil
		}
		current = parent
	}
}

func durableWritePrivate(path string, data []byte) error {
	return durableWritePrivateWithSync(path, data, syncDirectory)
}

func durableWritePrivateWithSync(path string, data []byte, syncDir func(string) error) error {
	if err := os.MkdirAll(filepath.Dir(path), 0o700); err != nil {
		return fmt.Errorf("create private directory: %w", err)
	}
	if err := syncDirectoryAncestry(filepath.Dir(path), syncDir); err != nil {
		return fmt.Errorf("persist private directory links: %w", err)
	}
	f, err := os.CreateTemp(filepath.Dir(path), "."+filepath.Base(path)+"-*")
	if err != nil {
		return fmt.Errorf("create durable temp: %w", err)
	}
	tmp := f.Name()
	defer os.Remove(tmp)
	defer f.Close()
	if err := f.Chmod(0o600); err != nil {
		return fmt.Errorf("chmod durable temp: %w", err)
	}
	if _, err := f.Write(data); err != nil {
		return fmt.Errorf("write durable temp: %w", err)
	}
	if err := f.Sync(); err != nil {
		return fmt.Errorf("sync durable temp: %w", err)
	}
	if err := f.Close(); err != nil {
		return fmt.Errorf("close durable temp: %w", err)
	}
	if err := os.Rename(tmp, path); err != nil {
		return fmt.Errorf("rename durable temp: %w", err)
	}
	if err := syncDir(filepath.Dir(path)); err != nil {
		return fmt.Errorf("sync directory after rename (durability uncertain): %w", err)
	}
	return nil
}

func durableRemove(path string) error {
	return durableRemoveWithSync(path, syncDirectory)
}

func durableRemoveWithSync(path string, syncDir func(string) error) error {
	if err := os.Remove(path); err != nil && !os.IsNotExist(err) {
		return fmt.Errorf("remove durable record: %w", err)
	}
	// ENOENT may mean a prior unlink succeeded but its directory sync failed.
	// Retrying must establish durable absence, not just observe local absence.
	if err := syncDirectoryAncestry(filepath.Dir(path), syncDir); err != nil {
		return fmt.Errorf("sync directory after removal (durability uncertain): %w", err)
	}
	return nil
}
