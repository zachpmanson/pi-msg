package pimsg

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"
	"reflect"
	"strings"
	"testing"
	"time"
)

func TestAutoreconcileConfig(t *testing.T) {
	ptr := func(n int) *int { return &n }
	for _, tc := range []struct {
		name, timeout string
		recent, owner *int
		away, bad     bool
		duration      time.Duration
		r, o          int
	}{
		{name: "absent", r: 10, o: 10}, {name: "zero", timeout: "0", r: 10, o: 10},
		{name: "counts alone", recent: ptr(99), owner: ptr(0), r: 99, o: 0},
		{name: "enabled optional skill", timeout: "30m", duration: 30 * time.Minute, r: 10, o: 10},
		{name: "independent zeros", timeout: "1h", recent: ptr(0), owner: ptr(7), duration: time.Hour, r: 0, o: 7},
		{name: "no bodies", timeout: "1s", recent: ptr(0), owner: ptr(0), duration: time.Second},
		{name: "bad unit", timeout: "30", bad: true}, {name: "negative", timeout: "-1m", bad: true},
		{name: "negative count", recent: ptr(-1), bad: true}, {name: "bounded", owner: ptr(101), bad: true},
		{name: "reset conflict", timeout: "30m", away: true, bad: true}, {name: "legacy away", away: true, r: 10, o: 10},
	} {
		t.Run(tc.name, func(t *testing.T) {
			a := Account{JID: "pi@x", Owner: "owner@x", Password: "pw", AutoreconcileTimeout: tc.timeout, AutoreconcileHistoryMessages: tc.recent, AutoreconcileOwnerHistoryMessages: tc.owner, ResetSessionOnAway: tc.away}
			got, err := resolveAccount(&Config{Accounts: map[string]Account{"test": a}}, "test")
			if (err != nil) != tc.bad {
				t.Fatalf("error=%v", err)
			}
			if tc.bad {
				return
			}
			if got.AutoreconcileTimeout != tc.duration || got.AutoreconcileHistoryMessages != tc.r || got.AutoreconcileOwnerHistoryMessages != tc.o {
				t.Fatalf("unexpected config: %+v", got)
			}
		})
	}
}
func autoTestBridge(t *testing.T) *Bridge {
	t.Helper()
	t.Setenv("PI_MSG_CONFIG", filepath.Join(t.TempDir(), "config.json"))
	b := NewBridge(ResolvedAccount{Name: "auto", JID: "pi@x", Owner: "owner@x", Rooms: []string{"room@muc.x"}, AutoreconcileTimeout: time.Second, AutoreconcileHistoryMessages: 10, AutoreconcileOwnerHistoryMessages: 10}, false)
	b.ctx = context.Background()
	b.xmpp = NewXMPPBridge(b.acct, b.onInbound, b.log)
	b.xmpp.onAcceptedSent = b.captureAutoSent
	b.inbox = newInbox(inboxPath(b.acct.Name), b.log)
	return b
}
func TestAutoreconcileHistoryFloorUnionChronologyAndBounds(t *testing.T) {
	b := autoTestBridge(t)
	at := time.Unix(100, 0)
	for n := 0; n < 10; n++ {
		b.captureAutoInbound(InboundMessage{Direct: true, From: "owner@x/phone", RealJID: "owner@x", FromOwner: true, ID: fmt.Sprint(n), Body: strings.Repeat("é", 4000), Stamp: at.Add(time.Duration(n) * time.Second)})
	}
	for n := 0; n < 150; n++ {
		b.captureAutoInbound(InboundMessage{Room: "room@muc.x", From: "room@muc.x/peer", RealJID: "peer@x", ID: fmt.Sprint(n), Body: "peer", Stamp: at.Add(time.Duration(n+20) * time.Second)})
	}
	selected := b.auto.state.History.selected()
	if len(selected) != 20 || len(b.auto.state.History.Owner) != 10 {
		t.Fatal("peer flood evicted owner floor", len(selected))
	}
	for n, e := range selected {
		if n < 10 && (!e.Owner || !strings.Contains(e.Body, "[truncated]")) {
			t.Fatal("owner selection/truncation lost")
		}
		if n > 0 && e.At.Before(selected[n-1].At) {
			t.Fatal("not chronological")
		}
	}
	// Stable duplicate and archive reads cannot add eligible history.
	before := append([]autoChat(nil), selected...)
	b.captureAutoInbound(InboundMessage{Room: "room@muc.x", From: "room@muc.x/peer", RealJID: "peer@x", ID: "149", Body: "duplicate", Stamp: at})
	b.xmpp.recordReadHistory([]InboundMessage{{ID: "archive-only", Body: "not eligible", From: "owner@x", FromOwner: true}}, "owner@x")
	if !reflect.DeepEqual(b.auto.state.History.selected(), before) {
		t.Fatal("duplicate/archive changed recap keys, bodies, order or identity")
	}
	b.captureAutoSent("pi@x", "report", "final report")
	b.onInbound(InboundMessage{Direct: true, From: "pi@x/other", ID: "report", Body: "/new"})
	selected = b.auto.state.History.selected()
	if selected[len(selected)-1].Body != "final report" {
		t.Fatal("accepted self-report not last")
	}
	raw, err := os.ReadFile(b.autoPath())
	if err != nil || len(raw) > 100000 {
		t.Fatalf("unbounded or missing index: %d %v", len(raw), err)
	}
	info, _ := os.Stat(b.autoPath())
	if info.Mode().Perm() != 0600 {
		t.Fatal("nonprivate history")
	}
	seed := b.auto.state.History.seed("/old/session", "/job")
	for _, want := range []string{"NOT new requests", "read_messages(target=...)", "/old/session", "/job", "final report"} {
		if !strings.Contains(seed, want) {
			t.Fatal("seed missing", want)
		}
	}
	// Overlap is deduplicated and independent explicit zero windows work.
	var h autoHistory
	for n := 0; n < 4; n++ {
		h.add(autoChat{Key: fmt.Sprint(n), Owner: true, At: at.Add(time.Duration(n))}, 10, 10)
	}
	if len(h.selected()) != 4 {
		t.Fatal("union duplicated overlap")
	}
	var empty autoHistory
	empty.add(autoChat{Key: "x", Owner: true}, 0, 0)
	if len(empty.selected()) != 0 || !strings.Contains(empty.seed("old", ""), "read_messages") {
		t.Fatal("zero counts lost guidance")
	}
}
func TestAutoreconcileOwnerIdentityNotNickOrFlagAlone(t *testing.T) {
	b := autoTestBridge(t)
	for n, m := range []InboundMessage{
		{Room: "room@muc.x", From: "room@muc.x/owner", Nick: "owner", RealJID: "peer@x", FromOwner: true},
		{Room: "room@muc.x", From: "room@muc.x/owner", Nick: "owner", FromOwner: true},
		{Direct: true, From: "peer@x", FromOwner: true},
		{Room: "room@muc.x", From: "room@muc.x/verified", RealJID: "owner@x", FromOwner: true},
	} {
		m.ID = fmt.Sprint(n)
		m.Body = "context"
		b.captureAutoInbound(m)
	}
	if len(b.auto.state.History.Owner) != 1 {
		t.Fatal("unverified owner quota", b.auto.state.History.Owner)
	}
}
func TestAutoreconcileDisabledNoopAndSelfPassive(t *testing.T) {
	b := autoTestBridge(t)
	b.acct.AutoreconcileTimeout = 0
	b.captureAutoInbound(InboundMessage{Direct: true, From: "owner@x", ID: "in", Body: "hi"})
	b.captureAutoSent("owner@x", "out", "reply")
	b.autoActivity()
	b.autoTick()
	if b.auto.running || b.auto.state.Generation != 0 || len(b.auto.state.History.selected()) != 0 {
		t.Fatal("disabled feature active")
	}
	if _, err := os.Stat(b.autoPath()); !os.IsNotExist(err) {
		t.Fatal("disabled feature wrote storage")
	}
	for _, m := range []InboundMessage{{Direct: true, From: "pi@x/resource", Body: "/new"}, {Direct: true, Own: true, From: "owner@x", Body: "/next"}, {Room: "room@muc.x", Own: true, Body: "@pi /new"}} {
		b.onInbound(m)
	}
	if b.inbox.len() != 0 || b.nextPending() || !b.idleSince.IsZero() {
		t.Fatal("self copies woke or controlled primary")
	}
	for _, env := range rpcEnv(b.acct) {
		if strings.HasPrefix(env, "PI_MSG_IDLE_REQUEST=") {
			t.Fatal("disabled private command enabled")
		}
	}
}
func TestAutoreconcileEligibilityEveryLocalGuard(t *testing.T) {
	for _, name := range []string{"streaming", "settling", "synthetic", "prompt", "heartbeat", "process", "subagent", "transition", "shutdown", "marker", "watch", "inbox", "next", "quarantine", "not-idle", "too-soon"} {
		t.Run(name, func(t *testing.T) {
			b := autoTestBridge(t)
			b.rpc = &RPCClient{}
			b.idleSince = time.Now().Add(-time.Hour)
			if !b.autoEligible() {
				t.Fatal("fixture not eligible")
			}
			switch name {
			case "streaming":
				b.streamingRun = true
			case "settling":
				b.settlingRun = true
			case "synthetic":
				b.syntheticPrompts = 1
			case "prompt":
				b.pendingPrompts = map[string]struct{}{"x": {}}
			case "heartbeat":
				b.pendingHeartbeats = []string{"x"}
			case "process":
				b.bgProcesses = 1
			case "subagent":
				b.bgSubagents = 1
			case "transition":
				b.nextTransition = true
			case "shutdown":
				b.shuttingDown = true
			case "marker":
				b.pendingMarkers = []pendingMarker{{id: "x"}}
			case "watch":
				b.replyWatches = map[string]*replyWatch{"x": {}}
			case "inbox":
				b.inbox.append(inboxEntry{ID: "x", Body: "pending"})
			case "next":
				b.next.fenced.Store(true)
			case "quarantine":
				b.next.quarantined.Store(true)
			case "not-idle":
				b.idleSince = time.Time{}
			case "too-soon":
				b.idleSince = time.Now()
			}
			if b.autoEligible() {
				t.Fatal("unsafe idle", name)
			}
		})
	}
	for _, s := range []Event{{"sessionId": "a", "sessionFile": "/old", "isStreaming": true}, {"sessionId": "a", "sessionFile": "/old", "isCompacting": true}, {"sessionId": "a", "sessionFile": "/old", "pendingMessageCount": float64(1)}, {"sessionFile": "relative"}} {
		if autoIdleState(s) {
			t.Fatal("native busy accepted", s)
		}
	}
}
func TestAutoreconcileImmutableAttemptGenerationAndRecovery(t *testing.T) {
	b := autoTestBridge(t)
	a := &autoAttempt{Token: "nonce", Fingerprint: "unchanged", SourceID: "id:old", SourceFile: "/old", Generation: 0}
	if err := b.autoSave(a, "prepared"); err != nil {
		t.Fatal(err)
	}
	a.Error = "local change"
	if b.auto.state.Attempt.Error != "" {
		t.Fatal("attempt not immutable copy")
	}
	if !b.autoCurrent(a) {
		t.Fatal("missing initial authority")
	}
	b.autoActivity()
	if b.autoCurrent(a) {
		t.Fatal("new activity retained reset authority")
	}
	restarted := NewBridge(b.acct, false)
	if err := restarted.loadAuto(); err != nil {
		t.Fatal(err)
	}
	if restarted.auto.state.Attempt.Phase != "blocked" {
		t.Fatal("interrupted snapshot not blocked")
	}
	before, _ := os.ReadFile(b.autoPath())
	if err := restarted.loadAuto(); err != nil {
		t.Fatal(err)
	}
	after, _ := os.ReadFile(b.autoPath())
	if !bytes.Equal(before, after) {
		t.Fatal("terminal recovery rewritten/replayed")
	}
}
func TestAutoreconcilePrivateCommandProvenance(t *testing.T) {
	res := func(path, source, name string) Event {
		return Event{"data": map[string]any{"commands": []any{map[string]any{"sourceInfo": map[string]any{"path": path, "source": "cli"}, "source": source, "name": name}}}}
	}
	if !autoCommandPresent(res("/owned.ts", "extension", "pi-msg-idle-reset"), "/owned.ts") {
		t.Fatal("valid command missing")
	}
	for _, r := range []Event{res("/other.ts", "extension", "pi-msg-idle-reset"), res("/owned.ts", "skill", "pi-msg-idle-reset"), res("/owned.ts", "extension", "pi-msg-idle-reset:1")} {
		if autoCommandPresent(r, "/owned.ts") {
			t.Fatal("wrong command provenance")
		}
	}
}
func TestSelfSendReadPermissionIndependentOfAutomaticMode(t *testing.T) {
	x := NewXMPPBridge(ResolvedAccount{JID: "pi@x", Owner: "owner@x"}, func(InboundMessage) {}, func(string, string) {})
	for _, to := range []string{"pi@x", "PI@X/resource"} {
		if x.classifyMessageDest(to, false) != destUser {
			t.Fatal("self send denied", to)
		}
		if x.classifyDest(to) != destBlocked {
			t.Fatal("unrelated file permission expanded")
		}
	}
	if x.classifyMessageDest("other@x", false) != destBlocked {
		t.Fatal("other account allowed")
	}
	b := newTestBridge(ResolvedAccount{JID: "pi@x", Owner: "owner@x"})
	var buf bytes.Buffer
	written := make(chan struct{})
	b.rpc = &RPCClient{stdin: rpcNotifyingWriter{WriteCloser: &nopClose{buf: &buf}, written: written}}
	b.handleReadMessagesRelay("r1", "pi@x/resource", 10, "", "")
	select {
	case <-written:
	case <-time.After(time.Second):
		t.Fatal("self read did not complete")
	}
	out := buf.String()
	if strings.Contains(out, "not an allowed target") {
		t.Fatal(out)
	}
}
func TestAutoreconcileReceiptFailsClosed(t *testing.T) {
	b := autoTestBridge(t)
	a := &autoAttempt{Token: "a", SourceID: "id:old", SourceFile: "/old", Seed: "seed"}
	for _, status := range []string{"failed", "cancelled", "success"} {
		raw, _ := json.Marshal(Event{"token": "wrong", "status": status, "id": "id:new", "file": "/new"})
		if err := durableWritePrivate(b.autoRequestPath()+".receipt", raw); err != nil {
			t.Fatal(err)
		}
		if _, err := b.readAutoReceipt(a); err == nil {
			t.Fatal("uncertain receipt authorized reset")
		}
	}
}
