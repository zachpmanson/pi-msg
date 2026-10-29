package pimsg

import (
	"bytes"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"
)

func TestSubagentPresenceAndBusyMarker(t *testing.T) {
	t.Setenv("PI_MSG_CONFIG", filepath.Join(t.TempDir(), "config.json"))
	b := bgBridge()
	marker := busyMarkerPath(b.acct.Name)
	assertState := func(show, status string, busy bool) {
		t.Helper()
		if b.xmpp.show != show || b.xmpp.presence != status {
			t.Fatalf("presence = %q/%q, want %q/%q", b.xmpp.show, b.xmpp.presence, show, status)
		}
		_, err := os.Stat(marker)
		if (err == nil) != busy {
			t.Fatalf("busy marker stat = %v, want busy=%v", err, busy)
		}
	}

	b.setStreaming(true)
	b.xmpp.SetPresence("dnd", "! bash")
	b.setBgSubagents(2)
	assertState("dnd", "! bash", true) // don't overwrite the parent's tool label
	b.setStreaming(false)              // parent returns, children keep working
	b.markIdle()
	b.announceSettledPresence()
	assertState("dnd", "2 subagents", true)
	if !b.idleSince.IsZero() {
		t.Fatal("background work advertised an idle timestamp")
	}
	b.setBgSubagents(1) // one child finishing must not clear the other
	assertState("dnd", "1 subagent", true)
	b.setBgProcesses(1)
	assertState("dnd", "1 subagent; 1 shell", true)
	b.setBgSubagents(0)
	assertState("dnd", "1 shell", true)
	b.setBgProcesses(0)
	assertState("", "listening", false)
	if b.idleSince.IsZero() {
		t.Fatal("last process after subagent work did not restore the idle clock")
	}

	b.setBgSubagents(1)
	b.setStreaming(true)
	b.xmpp.SetPresence("dnd", "thinking…")
	b.setBgSubagents(0) // last child stops, parent is still working
	assertState("dnd", "thinking…", true)
	b.setStreaming(false)
	b.announceSettledPresence()
	assertState("", "listening", false)
	b.setBgSubagents(-1)
	assertState("", "listening", false)
}

func TestSubagentsIdleAndReconnect(t *testing.T) {
	t.Setenv("PI_MSG_CONFIG", filepath.Join(t.TempDir(), "config.json"))
	b := bgBridge()
	b.setBgSubagents(1)
	b.idleSince = time.Now().Add(-idleAwayTimeout - time.Minute)
	b.idleTick()
	if b.awayAnnounced || b.xmpp.show != "dnd" {
		t.Fatal("subagent work drifted away")
	}
	show, status := reconnectPresence(b.xmpp.show, b.xmpp.presence, "starting")
	if show != "dnd" || status != "1 subagent" {
		t.Fatalf("reconnect lost background presence: %q/%q", show, status)
	}
	b.setBgSubagents(0)
	b.idleTick()
	if b.awayAnnounced { // final child completion starts a fresh idle period
		t.Fatal("child completion inherited stale idle clock")
	}
	b.idleSince = time.Now().Add(-idleAwayTimeout - time.Minute)
	b.idleTick()
	if !b.awayAnnounced || b.xmpp.show != "away" {
		t.Fatal("away did not return after child completion")
	}
}

func TestSubagentCountRelay(t *testing.T) {
	t.Setenv("PI_MSG_CONFIG", filepath.Join(t.TempDir(), "config.json"))
	var buf bytes.Buffer
	b := bgBridge()
	b.rpc = &RPCClient{stdin: &nopClose{buf: &buf}}
	b.handleToolRelay("children", `{"action":"subagent_count","count":3}`)
	if b.bgSubagents != 3 || b.xmpp.presence != "3 subagents" {
		t.Fatalf("relay did not update subagents: %d/%q", b.bgSubagents, b.xmpp.presence)
	}
	if !strings.Contains(buf.String(), `"value":"ok"`) {
		t.Fatalf("relay reply = %q", buf.String())
	}
}
