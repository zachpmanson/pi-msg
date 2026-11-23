package pimsg

import (
	"encoding/json"
	"reflect"
	"strings"
	"testing"
	"time"
	"unicode/utf8"
)

func TestAutoreconcileTextTruncationIdempotent(t *testing.T) {
	const marker = " [truncated]"
	checkTagged := func(label, tagged string, limit int) {
		t.Helper()
		bounded := autoText(tagged, limit)
		if len(bounded) > limit+len(marker) || !strings.HasSuffix(bounded, marker) || autoText(bounded, limit) != bounded {
			t.Fatalf("%s limit %d: oversized tagged input escaped stable bounding", label, limit)
		}
	}
	for _, limit := range []int{512, autoBodyBytes} {
		checkTagged("ASCII complete marker", strings.Repeat("x", limit+1)+marker, limit)
		checkTagged("ASCII incomplete marker", strings.Repeat("x", limit+len(marker))+" [truncated", limit)
		for _, runeText := range []string{"é", "界", "😀"} {
			source := strings.Repeat(runeText, 3000)
			want := source[:limit/len(runeText)*len(runeText)] + marker
			got := autoText(source, limit)
			if got != want || !utf8.ValidString(got) || len(got) > limit+len(marker) {
				t.Fatalf("%q limit %d: source prefix or complete marker changed", runeText, limit)
			}
			for n := 0; n < 3; n++ {
				got = autoText(got, limit)
				if got != want {
					t.Fatalf("%q limit %d: normalization %d changed bounded text", runeText, limit, n)
				}
			}
			checkTagged(runeText, source+marker, limit)
		}
	}
}

func TestAutoreconcileTruncatedRowStableAcrossActivityAndReload(t *testing.T) {
	b := autoTestBridge(t)
	h := &b.auto.state.History
	label := strings.Repeat("界", 1000)
	h.add(autoChat{
		Key: "owner", Kind: "dm", Direction: "in", Owner: true,
		At: time.Unix(10, 0).UTC(), Body: label,
		Sender: label, Conversation: label, OriginFrom: label, ID: label,
	}, 10, 10, b.acct.Owner, b.acct.JID)
	want := h.Owner[0]
	if want.Body != strings.Repeat("界", 682)+" [truncated]" || want.Sender != strings.Repeat("界", 170)+" [truncated]" {
		t.Fatal("fixture must retain the full original source prefix")
	}
	assertRow := func(history autoHistory) {
		t.Helper()
		if len(history.Owner) != 1 || !reflect.DeepEqual(history.Owner[0], want) || !reflect.DeepEqual(history.Recent[0], want) {
			t.Fatal("retained entire row changed")
		}
	}
	if err := b.saveAutoLocked(); err != nil {
		t.Fatal(err)
	}
	for n := 0; n < 3; n++ {
		disk := fileText(b.autoPath())
		if h.normalize(10, 10, b.acct.Owner, b.acct.JID) {
			t.Fatal("normalization reported a change to bounded history")
		}
		assertRow(*h)
		if err := b.saveAutoLocked(); err != nil {
			t.Fatal(err)
		}
		if disk != fileText(b.autoPath()) {
			t.Fatal("normalization changed durable file")
		}
	}
	b.captureAutoInbound(InboundMessage{Direct: true, From: "peer@x", ID: "unrelated", Body: "peer", Stamp: time.Unix(20, 0).UTC()})
	assertRow(*h)
	disk := fileText(b.autoPath())
	var persisted autoState
	if err := json.Unmarshal([]byte(disk), &persisted); err != nil {
		t.Fatal(err)
	}
	assertRow(persisted.History)
	for n := 0; n < 3; n++ {
		r := NewBridge(b.acct, false)
		if err := r.loadAuto(); err != nil {
			t.Fatal(err)
		}
		assertRow(r.auto.state.History)
		if !reflect.DeepEqual(*h, r.auto.state.History) || disk != fileText(b.autoPath()) {
			t.Fatal("reload changed entire history or durable file")
		}
	}
}
