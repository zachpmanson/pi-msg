package pimsg

import (
	"os"
	"strings"
	"testing"
)

func TestAutoreconcileUnknownOutcomeDurablyFencesInputAndRestart(t *testing.T) {
	f := nextSetup(t)
	configureAutoFixture(f)
	a := &autoAttempt{Token: "unknown-switch", SourceID: "id:s0", SourceFile: f.sources[0], Seed: "historical source", Generation: 0}
	if err := f.b.beginAutoBoundary(a); err != nil {
		t.Fatal(err)
	}
	f.b.autoUncertain(a, os.ErrDeadlineExceeded)
	before := fileText(f.dir + "/commands")
	f.incoming("retain through unknown native switch", "held-unknown")
	held := f.b.inbox.protectedPending()
	if len(held) != 1 || held[0].NextBoundaryID != a.Token || held[0].TargetSessionID != "" {
		t.Fatal("unknown outcome did not protect input", held)
	}
	if fileText(f.dir+"/commands") != before {
		t.Fatal("unknown switch delivered into an unproven primary")
	}
	f.incoming("/new", "no-blind-reset")
	if f.b.next.supersedeTo != "" {
		t.Fatal("unknown native switch allowed blind /new supersession")
	}
	restarted := NewBridge(f.b.acct, false)
	if err := restarted.loadAuto(); err == nil || !strings.Contains(err.Error(), "input remains fenced") {
		t.Fatal("unknown restart allowed blind fallback", err)
	}
	reloaded := newInbox(inboxPath(f.b.acct.Name), f.b.log)
	if len(reloaded.protectedPending()) != 1 || reloaded.protectedPending()[0].Body != "retain through unknown native switch" {
		t.Fatal("restart lost protected input")
	}
}
