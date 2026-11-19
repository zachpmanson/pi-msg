package pimsg

import (
	"context"
	"encoding/xml"
	"fmt"
	"sync"
	"testing"
	"time"
)

func TestFindRoomReplyParent(t *testing.T) {
	const room = "team@muc.x"
	alice := InboundMessage{Room: room, From: room + "/Alice", ID: "shared", ArchiveID: "older-1", RoomStanzaID: "room-1"}
	bob := InboundMessage{Room: room, From: room + "/Bob", ID: "shared", ArchiveID: "newer-2", RoomStanzaID: "room-2"}
	foreign := InboundMessage{Room: "other@muc.x", From: "other@muc.x/Alice", ID: "foreign", ArchiveID: "newer-1"}
	for _, tc := range []struct{ name, target, id, want string }{
		{"room ID from earlier page", room, "room-1", alice.From},
		{"archive ID", room, "older-1", alice.From},
		{"bare-room legacy collision", room, "shared", ""},
		{"legacy collision disambiguated by author", alice.From, "shared", alice.From},
		{"wrong author", bob.From, "room-1", ""},
		{"wrong archived room", room, "foreign", ""},
		{"wrong target room", "other@muc.x", "room-1", ""},
	} {
		t.Run(tc.name, func(t *testing.T) {
			got, err := findRoomReplyParent(room, tc.id, tc.target, func(before string) ([]InboundMessage, bool, error) {
				switch before {
				case "":
					return []InboundMessage{foreign, bob}, false, nil
				case foreign.ArchiveID:
					return []InboundMessage{alice}, true, nil
				default:
					t.Fatalf("did not use archive cursor: %q", before)
					return nil, true, nil
				}
			})
			if err != nil || got.From != tc.want {
				t.Fatalf("parent author = %q, %v; want %q", got.From, err, tc.want)
			}
		})
	}
}

func TestArchivedParentUsesOccupantIDNotNickname(t *testing.T) {
	const room = "team@muc.x"
	b := NewXMPPBridge(ResolvedAccount{JID: "pi@x", Nick: "pi", Rooms: []string{room}}, nil, func(string, string) {})
	presence := xml.StartElement{Attr: []xml.Attr{{Name: xml.Name{Local: "from"}, Value: room + "/pi"}}}
	toks := mamTokens(t, `<x xmlns="http://jabber.org/protocol/muc#user"><status code="110"/></x>`+
		`<occupant-id xmlns="urn:xmpp:occupant-id:0" id="self-room-id"/>`)
	if err := b.handlePresence(&presence, toks); err != nil {
		t.Fatal(err)
	}
	for _, tc := range []struct {
		nick, occupant string
		want           bool
	}{{"previous-nickname", "self-room-id", true}, {"pi", "another-occupant", false}, {"pi", "", false}} {
		parent := InboundMessage{From: room + "/" + tc.nick, OccupantID: tc.occupant}
		if got := b.archivedParentIsSelf(room, parent); got != tc.want {
			t.Errorf("nick=%s occupant=%s self=%v; want %v", tc.nick, tc.occupant, got, tc.want)
		}
	}
	if b.archivedParentIsSelf("other@muc.x", InboundMessage{OccupantID: "self-room-id"}) {
		t.Fatal("occupant ID leaked across room scope")
	}
}

func TestArchiveIngressKeepsTrafficWhenQueueFills(t *testing.T) {
	seen := make(chan string, archiveIngressLimit+2)
	b := NewXMPPBridge(ResolvedAccount{}, func(m InboundMessage) { seen <- m.ID }, func(string, string) {})
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	b.ingressSpace = sync.NewCond(&b.ingressMu)
	b.ingressCancel = cancel
	for i := range archiveIngressLimit {
		b.dispatchReplyAware(InboundMessage{Direct: true, ID: fmt.Sprint(i)})
	}
	go func() {
		<-ctx.Done() // the overflowing stanza cancels lookup before waiting
		b.drainReplyIngress(ctx, cancel, InboundMessage{ID: "first"})
	}()
	go b.dispatchReplyAware(InboundMessage{Direct: true, ID: "overflow"})
	deadline := time.After(time.Second)
	for i := range archiveIngressLimit + 2 {
		want := fmt.Sprint(i - 1)
		if i == 0 {
			want = "first"
		} else if i == archiveIngressLimit+1 {
			want = "overflow"
		}
		select {
		case got := <-seen:
			if got != want {
				t.Fatalf("delivery %d = %q; want %q", i, got, want)
			}
		case <-deadline:
			t.Fatalf("timed out waiting for %s", want)
		}
	}
}

func TestRoomMAMResultRequiresArchiveSender(t *testing.T) {
	const room = "team@muc.x"
	b := newMAMTestBridge()
	b.mamPending["lookup"] = &mamCollector{room: room}
	toks := mamTokens(t, `<result xmlns="urn:xmpp:mam:2" queryid="lookup"/>`)
	res, _ := element(toks, mamNS, "result")
	for _, from := range []string{room + "/Alice", "alice@x", "other@muc.x", ""} {
		if b.mamResultSourceOK(from, res) {
			t.Errorf("accepted a room MAM result from %q", from)
		}
	}
	if !b.mamResultSourceOK(room, res) {
		t.Fatal("rejected a correlated result from the queried room")
	}
}
