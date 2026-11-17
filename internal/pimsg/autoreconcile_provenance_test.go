package pimsg

import (
	"fmt"
	"path/filepath"
	"reflect"
	"testing"
	"time"
)

func autoMAMLine(t *testing.T, x *XMPPBridge, from, id, body string, record bool) []InboundMessage {
	t.Helper()
	col := &mamCollector{record: record, room: bareJid(from)}
	if x.mamPending == nil {
		x.mamPending = make(map[string]*mamCollector)
	}
	x.mamPending["proof-query"] = col
	toks := mamTokens(t, fmt.Sprintf(`<result xmlns='urn:xmpp:mam:2' queryid='proof-query'><forwarded xmlns='urn:xmpp:forward:0'><delay xmlns='urn:xmpp:delay' stamp='2026-09-15T01:00:00Z'/><message xmlns='jabber:client' from='%s' id='%s' type='groupchat'><body>%s</body></message></forwarded></result>`, from, id, body))
	res, _ := element(toks, mamNS, "result")
	x.collectMAMResult(toks, res)
	return col.out
}

func TestAutoreconcileHistoricalRoomIdentityNotCurrentNick(t *testing.T) {
	for _, proven := range []bool{false, true} {
		t.Run(fmt.Sprint("proven=", proven), func(t *testing.T) {
			b := autoTestBridge(t)
			x := b.xmpp
			x.roomOrigin = b.autoRoomOrigin
			from := "room@muc.x/reused"
			if proven {
				b.captureAutoInbound(InboundMessage{Room: "room@muc.x", From: from, RealJID: "peer@x", ID: "historical", Body: "peer text"})
			}
			x.occupants["room@muc.x"] = map[string]string{"reused": "owner@x"}
			lines := autoMAMLine(t, x, from, "historical", "peer text", true)
			if len(lines) != 1 {
				t.Fatal("permitted peer replay was disabled", lines)
			}
			b.captureAutoInbound(lines[0])
			if len(b.auto.state.History.Owner) != 0 {
				t.Fatal("current nickname reuse established historical owner", b.auto.state.History)
			}
			for _, e := range b.auto.state.History.selected() {
				if e.Owner || e.Sender == "owner@x" {
					t.Fatal("false historical owner label", e)
				}
			}
			// Delayed replay follows the same provenance rule, without disabling replay.
			x.replayStart = time.Now().Add(-time.Minute)
			x.replayActive = true
			x.replayGraceEnd = time.Now().Add(time.Minute)
			x.dispatchRoom(incomingMsg{typ: "groupchat", from: from, id: "delayed", body: "other peer text", delay: true, delayStamp: time.Now()})
			if len(x.replayBuf) != 1 || !x.replayBuf[0].Historical || x.replayBuf[0].OriginJID != "" {
				t.Fatal("delayed origin not separated", x.replayBuf)
			}
			b.captureAutoInbound(x.replayBuf[0])
			if len(b.auto.state.History.Owner) != 0 {
				t.Fatal("delayed nickname reuse entered owner floor")
			}
		})
	}
	b := autoTestBridge(t)
	x := b.xmpp
	x.roomOrigin = b.autoRoomOrigin
	b.captureAutoInbound(InboundMessage{Room: "room@muc.x", From: "room@muc.x/oldowner", RealJID: "owner@x", FromOwner: true, ID: "owner-original", Body: "owner original"})
	x.occupants["room@muc.x"] = map[string]string{"oldowner": "peer@x"}
	lines := autoMAMLine(t, x, "room@muc.x/oldowner", "owner-original", "owner original", true)
	if len(lines) != 1 || lines[0].OriginJID != "owner@x" {
		t.Fatal("verified original delivery proof lost", lines)
	}
	if x.roomOrigin("other@muc.x", "room@muc.x/oldowner", "owner-original", "owner original") != "" {
		t.Fatal("provenance crossed conversations")
	}
	// A direct archive remains verified by its actual bare-JID sender.
	b.captureAutoInbound(InboundMessage{Direct: true, Historical: true, From: "owner@x/phone", FromOwner: true, ID: "direct", Body: "direct archive"})
	if len(b.auto.state.History.Owner) != 2 {
		t.Fatal("direct archive lost owner proof")
	}
}

func TestAutoreconcileOwnOldNickAcceptedSendProvenanceAndVerifiedLiveSelf(t *testing.T) {
	t.Setenv("PI_MSG_CONFIG", filepath.Join(t.TempDir(), "config.json"))
	b, _, _ := messageTransportBridge(t, false)
	b.acct.Name = "provenance"
	b.acct.AutoreconcileTimeout = time.Hour
	b.acct.AutoreconcileHistoryMessages = 10
	b.acct.AutoreconcileOwnerHistoryMessages = 10
	b.xmpp.onAcceptedSent = b.captureAutoSent
	b.xmpp.roomOrigin = b.autoRoomOrigin
	body := "@everyone accepted outgoing"
	id := b.xmpp.SendRoomTo("team@muc.x", body)
	if id == "" {
		t.Fatal("room transport did not accept outgoing stanza")
	}
	before := b.auto.state.History.selected()
	// Restart erases transient transport history; bounded auto history is sufficient.
	r := NewBridge(b.acct, false)
	if err := r.loadAuto(); err != nil {
		t.Fatal(err)
	}
	x := NewXMPPBridge(r.acct, r.onInbound, r.log)
	r.xmpp = x
	x.roomOrigin = r.autoRoomOrigin
	replies, arrivals := 0, 0
	x.onReplyMessage = func(InboundMessage) { replies++ }
	x.onMsg = func(InboundMessage) { arrivals++ }
	x.occupants["team@muc.x"] = map[string]string{"another-resource": "pi@x", "oldnick": "peer@x"}
	generation := r.auto.state.Generation
	lines := autoMAMLine(t, x, "team@muc.x/oldnick", id, body, true)
	if len(lines) != 0 || !x.isSelfMessage(id) {
		t.Fatal("old-nick accepted outgoing admitted as inbound", lines)
	}
	read := autoMAMLine(t, x, "team@muc.x/oldnick", id, body, false)
	if len(read) != 1 || !read[0].Own {
		t.Fatal("read did not label own archived line", read)
	}
	x.dispatchRoom(incomingMsg{typ: "groupchat", from: "team@muc.x/another-resource", id: "live-own", body: "@everyone own alternate nick"})
	x.replayStart = time.Now().Add(-time.Minute)
	x.replayActive = true
	x.replayGraceEnd = time.Now().Add(time.Minute)
	x.dispatchRoom(incomingMsg{typ: "groupchat", from: "team@muc.x/oldnick", id: id, body: body, delay: true, delayStamp: time.Now()})
	if replies != 0 || arrivals != 0 || len(x.replayBuf) != 0 || r.auto.state.Generation != generation || !reflect.DeepEqual(before, r.auto.state.History.selected()) {
		t.Fatal("own copies caused observation/admission/activity/history")
	}
	// Stable IDs are conversation-bound, body-bound, and archive reads alone add no proof.
	for _, line := range []struct{ from, id, body string }{{"team@muc.x/oldnick", "unknown-own", "@everyone unknown historical text"}, {"other@muc.x/oldnick", id, body}, {"team@muc.x/oldnick", id, "different body"}} {
		got := autoMAMLine(t, x, line.from, line.id, line.body, false)
		if len(got) != 1 || got[0].Own {
			t.Fatal("unknown/cross-room/changed copy falsely proved own", got)
		}
	}
}

func TestAutoreconcileLegacyRoomHistoryCannotSupplyOriginProof(t *testing.T) {
	b := autoTestBridge(t)
	e := autoChat{Key: "legacy", Conversation: "room@muc.x", Kind: "room", Sender: "owner@x", Direction: "in", Owner: true, ID: "old", Body: "unproven"}
	b.auto.state.History = autoHistory{Recent: []autoChat{e}, Owner: []autoChat{e}}
	if err := b.saveAutoLocked(); err != nil {
		t.Fatal(err)
	}
	if err := b.loadAuto(); err != nil {
		t.Fatal(err)
	}
	if len(b.auto.state.History.Owner) != 0 || b.auto.state.History.Recent[0].Owner || b.auto.state.History.Recent[0].Sender == "owner@x" || b.autoRoomOrigin("room@muc.x", "room@muc.x/reused", "old", "unproven") != "" {
		t.Fatal("legacy unverified historical identity trusted")
	}
}
