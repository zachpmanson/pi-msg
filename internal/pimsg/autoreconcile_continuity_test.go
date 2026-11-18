package pimsg

import (
	"encoding/json"
	"fmt"
	"os"
	"reflect"
	"strings"
	"testing"
	"time"
)

func TestAutoreconcileReplyTailIndependentAndPruned(t *testing.T) {
	var h autoHistory
	at := func(n int) time.Time { return time.Unix(int64(n), 0) }
	add := func(key string, n int, direction, kind, conv, sender string, owner bool) {
		h.add(autoChat{Key: key, At: at(n), Direction: direction, Kind: kind, Conversation: conv, Sender: sender, Owner: owner, Body: key}, 0, 2, "owner@x", "pi@x")
	}
	add("owner1", 10, "in", "dm", "owner@x", "owner@x", true)
	add("before", 9, "out", "dm", "owner@x", "pi@x", false)
	for n := 0; n < 20; n++ {
		add(fmt.Sprint("reply", n), 10+n, "out", "dm", "owner@x", "pi@x", false)
	}
	for _, e := range []autoChat{
		{Key: "room", Kind: "room", Conversation: "owner@x", Sender: "pi@x", Direction: "out"},
		{Key: "self", Kind: "self", Conversation: "pi@x", Sender: "pi@x", Direction: "out"},
		{Key: "peer", Kind: "dm", Conversation: "peer@x", Sender: "pi@x", Direction: "out"},
		{Key: "fake", Kind: "dm", Conversation: "owner@x", Sender: "peer@x", Direction: "out"},
	} {
		e.At = at(30)
		h.add(e, 0, 2, "owner@x", "pi@x")
	}
	for n := 0; n < 50; n++ {
		add(fmt.Sprint("flood", n), 100+n, "in", "room", "room@muc.x", "peer@x", false)
	}
	if len(h.Replies) != 20 || len(h.Owner) != 1 || len(h.Recent) != 0 || len(h.selected()) != 21 {
		t.Fatalf("independent tail: %+v", h)
	}
	before := h.selected()
	if h.add(autoChat{Key: "reply0", At: at(999), Body: "duplicate"}, 0, 2, "owner@x", "pi@x") || !reflect.DeepEqual(before, h.selected()) {
		t.Fatal("reply-only duplicate replaced original")
	}
	add("owner2", 20, "in", "dm", "owner@x", "owner@x", true)
	add("late", 5, "in", "dm", "owner@x", "owner@x", true)
	if len(h.Replies) != 20 || !h.Owner[0].At.Equal(at(10)) {
		t.Fatal("late owner displaced newest N")
	}
	add("owner3", 25, "in", "dm", "owner@x", "owner@x", true)
	if len(h.Replies) != 10 || h.Replies[0].At != at(20) {
		t.Fatal("inclusive advanced floor", h.Replies)
	}
	h.normalize(0, 1, "owner@x", "pi@x")
	if len(h.Replies) != 5 || h.Replies[0].At != at(25) {
		t.Fatal("quota reduction did not prune")
	}
	h.normalize(0, 0, "owner@x", "pi@x")
	if len(h.selected()) != 0 {
		t.Fatal("zero retained floor")
	}
	// A late arrival can move an unsaturated floor earlier, but cannot recover
	// replies already pruned when no earlier owner was retained.
	add("new", 40, "in", "dm", "owner@x", "owner@x", true)
	add("older", 1, "in", "dm", "owner@x", "owner@x", true)
	if h.Owner[0].At != at(1) || len(h.Replies) != 0 {
		t.Fatal("late floor fabricated replies")
	}
}

func TestAutoreconcileReplyMigrationBeforeTrimAndDurableReduction(t *testing.T) {
	b := autoTestBridge(t)
	b.acct.AutoreconcileHistoryMessages = 0
	b.acct.AutoreconcileOwnerHistoryMessages = 1
	owner := autoChat{Key: "persisted-owner", Kind: "dm", Direction: "in", Sender: "owner@x", Conversation: "owner@x", Owner: true, At: time.Unix(10, 0)}
	reply := autoChat{Key: "persisted-reply", Kind: "dm", Direction: "out", Sender: "pi@x", Conversation: "owner@x", At: time.Unix(10, 0), Body: "legacy reply"}
	b.auto.state = autoState{Version: 1, History: autoHistory{Owner: []autoChat{owner}, Recent: []autoChat{reply, reply}}, Attempt: &autoAttempt{Phase: "worker", Seed: "immutable old seed"}}
	if err := b.saveAutoLocked(); err != nil {
		t.Fatal(err)
	}
	r := NewBridge(b.acct, false)
	if err := r.loadAuto(); err != nil {
		t.Fatal(err)
	}
	h := r.auto.state.History
	if len(h.Recent) != 0 || len(h.Replies) != 1 || h.Replies[0].Key != reply.Key || len(h.selected()) != 2 || r.auto.state.Attempt.Seed != "immutable old seed" {
		t.Fatal("migration", r.auto.state)
	}
	disk := fileText(r.autoPath())
	r2 := NewBridge(b.acct, false)
	if err := r2.loadAuto(); err != nil {
		t.Fatal(err)
	}
	if disk != fileText(r.autoPath()) || !reflect.DeepEqual(h, r2.auto.state.History) {
		t.Fatal("reload changed normalized history")
	}
	// Recent may retain old owner records, but may never rebuild the floor.
	r2.auto.state.History.Recent = []autoChat{owner, reply}
	r2.acct.AutoreconcileHistoryMessages = 10
	r2.acct.AutoreconcileOwnerHistoryMessages = 0
	if err := r2.saveAutoLocked(); err != nil {
		t.Fatal(err)
	}
	r3 := NewBridge(r2.acct, false)
	if err := r3.loadAuto(); err != nil {
		t.Fatal(err)
	}
	r3.acct.AutoreconcileOwnerHistoryMessages = 10
	r3.captureAutoInbound(InboundMessage{Direct: true, From: "peer@x", ID: "newpeer", Body: "peer"})
	if len(r3.auto.state.History.Owner) != 0 || len(r3.auto.state.History.Replies) != 0 {
		t.Fatal("retired floor resurrected")
	}
}

func TestAutoreconcileReplyCaptureClassification(t *testing.T) {
	b := autoTestBridge(t)
	b.acct.AutoreconcileHistoryMessages = 0
	b.acct.AutoreconcileOwnerHistoryMessages = 1
	b.captureAutoInbound(InboundMessage{Direct: true, From: "owner@x/resource", FromOwner: true, ID: "in", Body: "owner", Stamp: time.Unix(1, 0)})
	for n, to := range []string{"owner@x/resource", "peer@x", "pi@x", "room@muc.x"} {
		b.captureAutoSent(to, fmt.Sprint(n), to)
	}
	if h := b.auto.state.History; len(h.Owner) != 1 || len(h.Replies) != 1 || h.Replies[0].Conversation != "owner@x" || h.Replies[0].Owner {
		t.Fatal("capture widened scope", h)
	}
	// Existing empty-ID key semantics are unchanged: retained identical keys dedup.
	b.captureAutoSent("owner@x", "", "first")
	b.captureAutoSent("owner@x", "", "second")
	if len(b.auto.state.History.Replies) != 2 || b.auto.state.History.Replies[1].Body != "first" {
		t.Fatal("empty-ID dedup changed")
	}
}

func TestAutoreconcileOverflowStickyPrivateAndForegroundCompatible(t *testing.T) {
	b := autoTestBridge(t)
	b.auto.state.Attempt = &autoAttempt{Phase: "worker", Seed: "immutable", Token: "old"}
	if err := b.saveAutoLocked(); err != nil {
		t.Fatal(err)
	}
	good := fileText(b.autoPath())
	// Total cap includes the immutable seed, not just history.
	b.auto.state.Attempt.Seed = strings.Repeat("x", 2<<20)
	if err := b.saveAutoLocked(); err == nil || !strings.Contains(err.Error(), "2 MiB") {
		t.Fatal("overflow accepted", err)
	}
	if good != fileText(b.autoPath()) {
		t.Fatal("overflow replaced last good state")
	}
	info, err := os.Stat(b.autoDisabledPath())
	if err != nil || info.Mode().Perm() != 0600 || fileText(b.autoDisabledPath()) != autoOverflowDiagnostic+"\n" {
		t.Fatal("private constant marker", info, err)
	}
	r := NewBridge(b.acct, false)
	if err := r.loadAuto(); err != nil || r.auto.err == nil {
		t.Fatal("marker killed foreground or failed to disable", err, r.auto.err)
	}
	if r.auto.state.Attempt.Phase != "recovering" || r.auto.state.Attempt.Seed != "immutable" {
		t.Fatal("marker prevented attempt recovery")
	}
	r.captureAutoInbound(InboundMessage{Direct: true, From: "peer@x", ID: "foreground", Body: "still usable"})
	if len(r.auto.state.History.Recent) != 1 || r.auto.err == nil {
		t.Fatal("foreground history unavailable or marker auto-cleared")
	}
	again := NewBridge(b.acct, false)
	if err := again.loadAuto(); err != nil || again.auto.err == nil {
		t.Fatal("successful save cleared sticky diagnostic", err)
	}
	b.acct.AutoreconcileTimeout = 0
	off := NewBridge(b.acct, false)
	if err := off.loadAuto(); err != nil || off.auto.err != nil || len(off.auto.state.History.selected()) != 0 {
		t.Fatal("zero mode read marker/index", err)
	}
}

func TestAutoreconcileOverflowMarkerFailurePreservesIndex(t *testing.T) {
	b := autoTestBridge(t)
	if err := b.saveAutoLocked(); err != nil {
		t.Fatal(err)
	}
	good := fileText(b.autoPath())
	if err := os.Mkdir(b.autoDisabledPath(), 0700); err != nil {
		t.Fatal(err)
	}
	b.auto.state.Attempt = &autoAttempt{Seed: strings.Repeat("x", 2<<20)}
	if err := b.saveAutoLocked(); err == nil || !strings.Contains(err.Error(), "cannot persist disable marker") {
		t.Fatal("missing marker failure diagnostic", err)
	}
	if fileText(b.autoPath()) != good {
		t.Fatal("marker failure overwrote good index")
	}
	var state autoState
	if err := json.Unmarshal([]byte(good), &state); err != nil {
		t.Fatal(err)
	}
}

func TestAutoreconcileMigrationOverflowStillRecovers(t *testing.T) {
	b := autoTestBridge(t)
	b.acct.AutoreconcileHistoryMessages = 100
	b.auto.state.History.Owner = []autoChat{{Key: "owner", Owner: true, Direction: "in", At: time.Unix(1, 0)}}
	for n := 0; n < 100; n++ {
		b.auto.state.History.Recent = append(b.auto.state.History.Recent, autoChat{Key: fmt.Sprint(n), Direction: "out", Kind: "dm", Sender: "pi@x", Conversation: "owner@x", At: time.Unix(2, 0), Body: strings.Repeat("x", autoBodyBytes)})
	}
	b.auto.state.Attempt = &autoAttempt{Phase: "worker", Seed: strings.Repeat("s", 1800<<10)}
	if err := b.saveAutoLocked(); err != nil {
		t.Fatal("legacy fixture must fit", err)
	}
	good := fileText(b.autoPath())
	r := NewBridge(b.acct, false)
	if err := r.loadAuto(); err != nil || r.auto.err == nil {
		t.Fatal("migration overflow killed foreground", err, r.auto.err)
	}
	if r.auto.state.Attempt.Phase != "recovering" || len(r.auto.state.History.Replies) != 100 || fileText(b.autoPath()) != good {
		t.Fatal("migration overflow skipped recovery or dropped replies/last-good")
	}
	if _, err := os.Stat(b.autoDisabledPath()); err != nil {
		t.Fatal(err)
	}
}

func TestAutoreconcileReplyFloorDoesNotPruneIndependentRecent(t *testing.T) {
	var h autoHistory
	for _, e := range []autoChat{
		{Key: "owner1", Direction: "in", Owner: true, At: time.Unix(1, 0)},
		{Key: "reply", Direction: "out", Kind: "dm", Conversation: "owner@x", Sender: "pi@x", At: time.Unix(2, 0)},
		{Key: "owner2", Direction: "in", Owner: true, At: time.Unix(3, 0)},
	} {
		h.add(e, 10, 1, "owner@x", "pi@x")
	}
	if len(h.Replies) != 0 || len(h.Recent) != 3 || len(h.selected()) != 3 {
		t.Fatal("floor advancement pruned Recent", h)
	}
	h.normalize(10, 0, "owner@x", "pi@x")
	if len(h.Owner) != 0 || len(h.Replies) != 0 || len(h.Recent) != 3 {
		t.Fatal("zero owner pruned Recent", h)
	}
}
