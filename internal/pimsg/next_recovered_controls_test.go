package pimsg

import (
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"
)

func TestQueuedNextRecoveredArchiveControlOverlapQuarantined(t *testing.T) {
	for _, command := range []string{"/new", "/abort", "/stop", "!", "/next", "!next", "&"} {
		for _, pending := range []bool{false, true} {
			for _, includeInbox := range []bool{false, true} {
				name := command
				if pending {
					name += "/pending"
				}
				if includeInbox {
					name += "/with-inbox-drain"
				} else {
					name += "/archive-only-backfill"
				}
				t.Run(name, func(t *testing.T) {
					f := nextSetup(t)
					if pending {
						f.incoming("/next", "current-boundary")
					}
					before := f.boundary(t)
					e := inboxEntry{ID: "historical", From: f.b.acct.Owner + "/phone", Body: command, Direct: true, FromOwner: true, At: time.Now().Add(-time.Hour)}
					f.b.inbox.append(e)
					var originals []inboxEntry
					if includeInbox {
						originals = f.b.inboxPending()
					}
					m := e.message()
					m.Stamp = e.At
					f.b.deliverRecovered([]InboundMessage{m}, originals)
					f.observe(t) // all previous pipe writes must precede this observation
					commands := fileText(filepath.Join(f.dir, "commands"))
					for _, forbidden := range []string{"new_session", "abort", "clear_queue", "prompt"} {
						if strings.Contains(commands, `"type":"`+forbidden+`"`) {
							t.Fatalf("historical control escaped quarantine (%s): %s", forbidden, commands)
						}
					}
					after := f.boundary(t)
					if (before == nil) != (after == nil) || (before != nil && before.ID != after.ID) {
						t.Fatal("historical copy created/replaced a boundary")
					}
					f.b.next.mu.Lock()
					supersede := f.b.next.supersedeTo != ""
					f.b.next.mu.Unlock()
					if supersede || !f.b.next.quarantined.Load() || len(f.jobs(t)) != 0 {
						t.Fatal("historical copy requested supersession/effects or lost quarantine")
					}
					f.b.ackInboxSettled()
					reloaded := newInbox(inboxPath(f.b.acct.Name), nil)
					entries := reloaded.pending()
					if reloaded.loadError() != nil || len(entries) != 1 || entries[0].Body != command || entries[0].protected() {
						t.Fatalf("original was changed/lost: %+v, %v", entries, reloaded.loadError())
					}
					if _, err := os.Stat(inboxPath(f.b.acct.Name)); err != nil {
						t.Fatal(err)
					}
				})
			}
		}
	}
}

func TestQueuedNextRecoveredOrdinaryOverlapDeliveredOnce(t *testing.T) {
	f := nextSetup(t)
	e := inboxEntry{ID: "ordinary-overlap", From: f.b.acct.Owner + "/phone", Body: "one durable ordinary input", Direct: true, FromOwner: true, At: time.Now()}
	f.b.inbox.append(e)
	f.b.deliverRecovered([]InboundMessage{e.message()}, f.b.inboxPending())
	f.observe(t)
	commands := fileText(filepath.Join(f.dir, "commands"))
	if strings.Count(commands, `"type":"prompt"`) != 1 || !strings.Contains(commands, "re-delivered after a restart") {
		t.Fatalf("archive bypassed original or duplicated it: %s", commands)
	}
}
