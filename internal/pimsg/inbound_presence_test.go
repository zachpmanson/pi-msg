package pimsg

import (
	"bytes"
	"path/filepath"
	"testing"
	"time"
)

func inboundPresenceBridge(t *testing.T) (*Bridge, *bytes.Buffer) {
	t.Helper()
	t.Setenv("XDG_CONFIG_HOME", t.TempDir())
	b := newTestBridge(ResolvedAccount{
		Name: "presence-test", Owner: "owner@x", Nick: "pi", RoomTrigger: "pi",
		Rooms: []string{"testing@muc.x", "agents@muc.x"},
	})
	for _, room := range b.acct.Rooms {
		b.xmpp.occupants[room] = map[string]string{"pi": "pi@x", "peer": "peer@x"}
	}
	var rpc bytes.Buffer
	b.rpc = &RPCClient{stdin: &nopClose{buf: &rpc}}
	b.inbox = newInbox(filepath.Join(t.TempDir(), "inbox.jsonl"), b.log)
	b.markRan()
	return b, &rpc
}

func TestDiscardedInboundPreservesPresence(t *testing.T) {
	cases := []struct {
		name    string
		message InboundMessage
	}{
		{"peer chatter", InboundMessage{Body: "unrelated chatter"}},
		{"unnamed owner without opt-in", InboundMessage{Body: "do this", FromOwner: true}},
		{"owner explicitly tags someone else", InboundMessage{Body: "@peer do this", FromOwner: true}},
		{"Unix username is not a tag", InboundMessage{Body: "check pi@naboo", FromOwner: true}},
		{"owner replies to someone else", InboundMessage{Body: "do this", FromOwner: true, ReplyToID: "other"}},
		{"unknown peer reaction", InboundMessage{Reactions: []string{"OK"}, ReactionID: "unknown"}},
		{"owner reaction to peer", InboundMessage{FromOwner: true, Reactions: []string{"OK"}, ReactionID: "other"}},
		{"own echo", InboundMessage{Nick: "PI", Body: "@pi do this"}},
		{"own reaction echo", InboundMessage{Nick: "PI", Reactions: []string{"OK"}, ReactionID: "ours"}},
	}
	for _, room := range []string{"testing@muc.x", "agents@muc.x"} {
		for _, state := range []string{"away", "listening", "streaming", "shell", "subagent"} {
			for _, c := range cases {
				t.Run(room+"/"+state+"/"+c.name, func(t *testing.T) {
					b, rpc := inboundPresenceBridge(t)
					show, status := "away", "idle activity"
					switch state {
					case "listening":
						show, status = "", "listening"
					case "streaming":
						show, status = "dnd", "drafting…"
						b.setStreaming(true)
					case "shell":
						show, status = "dnd", "1 shell"
						b.bgProcesses = 1
					case "subagent":
						show, status = "dnd", "1 subagent"
						b.bgSubagents = 1
					}
					b.xmpp.SetPresence(show, status)
					since := time.Now().Add(-idleAwayTimeout - time.Minute)
					b.idleSince = since
					b.xmpp.SetIdleSince(since)
					b.awayAnnounced = true
					b.lastAwayStatus = "idle activity"
					b.sendRecoveryNudges, b.hintNudges = 1, 1
					b.setTurnDest("old-room@muc.x", false)
					b.xmpp.recordInboundMessage("other", room+"/peer", "other's message", false)
					b.xmpp.recordSelfMessage("ours", room, "our message")
					m := c.message
					m.ID, m.Room, m.From = "discarded", room, room+"/peer"
					if m.Nick == "" {
						m.Nick = "peer"
					}
					b.onInbound(m)
					if gotShow, gotStatus := b.xmpp.currentPresence(); gotShow != show || gotStatus != status {
						t.Fatalf("discarded traffic changed presence to %q/%q; want %q/%q", gotShow, gotStatus, show, status)
					}
					if !b.idleSince.Equal(since) || !b.xmpp.idleSince.Equal(since) || !b.awayAnnounced || b.lastAwayStatus != "idle activity" {
						t.Fatal("discarded traffic changed idle/away state")
					}
					if b.sendRecoveryNudges != 1 || b.hintNudges != 1 || b.currentTurnDest() != "old-room@muc.x" || b.reactionAckRun || b.streaming() != (state == "streaming") {
						t.Fatal("discarded traffic changed turn state")
					}
					if b.inbox.len() != 0 || rpc.Len() != 0 || len(b.pendingPrompts) != 0 {
						t.Fatalf("discarded traffic queued inbox=%d, RPC=%s", b.inbox.len(), rpc.String())
					}
				})
			}
		}
	}
}

// The first classification must use the original away/participation state, and
// the admitted verdict must survive activity changes and the later dispatch.
func TestAddressedInboundActivityAndPreActivityVerdict(t *testing.T) {
	cases := []struct {
		name, body, show                              string
		owner, alreadyAddressed, participating, fresh bool
		untaggedOwnerToFree                           bool
	}{
		{name: "bare mention", body: "ask pi for this", show: "away"},
		{name: "explicit mention", body: "@pi do this", show: "away"},
		{name: "everyone broadcast", body: "@everyone do this", show: "away"},
		{name: "all broadcast", body: "@all do this", show: "away"},
		{name: "peer free", body: "@free do this", show: "away"},
		{name: "owner free", body: "@free do this", show: "away", owner: true},
		{name: "owner untagged away opted in", body: "do this", show: "away", owner: true, untaggedOwnerToFree: true},
		{name: "owner recent participant", body: "do this", owner: true, participating: true},
		{name: "owner bare name still reaches other participants", body: "peer do this", owner: true, participating: true},
		{name: "owner Unix username still reaches other participants", body: "check peer@naboo", owner: true, participating: true},
		{name: "owner fresh opted in", body: "do this", owner: true, fresh: true, untaggedOwnerToFree: true},
		{name: "peer admitted replay", body: "do this", alreadyAddressed: true},
		{name: "owner admitted replay", body: "do this", owner: true, alreadyAddressed: true},
	}
	for _, room := range []string{"testing@muc.x", "agents@muc.x"} {
		for _, c := range cases {
			t.Run(room+"/"+c.name, func(t *testing.T) {
				b, rpc := inboundPresenceBridge(t)
				b.acct.UntaggedOwnerToFree = c.untaggedOwnerToFree
				setShow(b, c.show, !c.fresh)
				if c.participating {
					spokeAgo(b, room, time.Minute)
				}
				b.idleSince = time.Now().Add(-idleAwayTimeout - time.Minute)
				b.awayAnnounced = true
				b.sendRecoveryNudges, b.hintNudges = 1, 1
				b.onInbound(InboundMessage{ID: "accepted", Room: room, Nick: "peer", From: room + "/peer", Body: c.body, FromOwner: c.owner, Addressed: c.alreadyAddressed})
				if b.inbox.len() != 1 || rpc.Len() == 0 {
					t.Fatalf("addressed traffic lost: inbox=%d RPC=%s", b.inbox.len(), rpc.String())
				}
				if !b.inbox.pending()[0].Addressed {
					t.Fatal("original addressing verdict not persisted")
				}
				if b.awayAnnounced || b.sendRecoveryNudges != 0 || b.hintNudges != 0 || time.Since(b.idleSince) > time.Second {
					t.Fatal("accepted traffic did not reset activity")
				}
				if show, _ := b.xmpp.currentPresence(); show != "dnd" {
					t.Fatalf("accepted prompt presence=%q, want dnd", show)
				}
			})
		}
	}
}

func TestIneligibleBroadcastPreservesPresence(t *testing.T) {
	for _, owner := range []bool{false, true} {
		for _, body := range []string{"@free do this", "do this"} {
			t.Run(body+"/"+map[bool]string{false: "peer", true: "owner"}[owner], func(t *testing.T) {
				b, rpc := inboundPresenceBridge(t)
				b.xmpp.SetPresence("", "listening")
				since := time.Now().Add(-time.Minute)
				b.idleSince = since
				// Recent participation allows owner chatter, but never widens @free.
				if body == "@free do this" {
					spokeAgo(b, b.acct.Rooms[0], time.Minute)
				}
				b.onInbound(InboundMessage{Room: b.acct.Rooms[0], Nick: "peer", Body: body, FromOwner: owner})
				if !b.idleSince.Equal(since) || b.inbox.len() != 0 || rpc.Len() != 0 {
					t.Fatal("ineligible broadcast counted as activity or prompt")
				}
			})
		}
	}
}

func TestReactionActivityOnlyAfterAcceptance(t *testing.T) {
	for _, busy := range []bool{false, true} {
		for _, direct := range []bool{false, true} {
			t.Run(map[bool]string{false: "idle", true: "busy"}[busy]+"/"+map[bool]string{false: "room", true: "direct"}[direct], func(t *testing.T) {
				b, rpc := inboundPresenceBridge(t)
				room := b.acct.Rooms[0]
				b.xmpp.recordSelfMessage("ours", room, "our message")
				show, status := "away", "idle activity"
				if busy {
					show, status = "dnd", "drafting…"
					b.setStreaming(true)
				}
				b.xmpp.SetPresence(show, status)
				since := time.Now().Add(-time.Minute)
				b.idleSince = since
				b.xmpp.SetIdleSince(since)
				b.awayAnnounced = true
				b.lastAwayStatus = "idle activity"
				b.sendRecoveryNudges, b.hintNudges = 1, 1
				b.setTurnDest("old-room@muc.x", false)
				b.onInbound(InboundMessage{Room: room, Nick: "peer", Direct: direct, FromOwner: direct, Reactions: []string{"OK"}, ReactionID: "ours"})
				if busy {
					gotShow, gotStatus := b.xmpp.currentPresence()
					if !b.idleSince.Equal(since) || !b.xmpp.idleSince.Equal(since) || !b.awayAnnounced || b.lastAwayStatus != "idle activity" || gotShow != show || gotStatus != status || rpc.Len() != 0 || b.reactionAckRun {
						t.Fatal("dropped busy reaction changed activity")
					}
					if b.sendRecoveryNudges != 1 || b.hintNudges != 1 || b.currentTurnDest() != "old-room@muc.x" || b.inbox.len() != 0 || len(b.pendingPrompts) != 0 || !b.streaming() {
						t.Fatal("dropped busy reaction changed turn state")
					}
				} else if !b.reactionAckRun || rpc.Len() == 0 || b.awayAnnounced || time.Since(b.idleSince) > time.Second {
					t.Fatal("relevant idle reaction did not count as activity")
				}
			})
		}
	}
}

func TestQueuedNextAdmissionPreservesOldPresence(t *testing.T) {
	f := nextSetup(t)
	f.incoming("/next", "boundary")
	room := "team@muc.x"
	setShow(f.b, "away", true)
	since := time.Now().Add(-time.Minute)
	f.b.idleSince = since
	f.b.awayAnnounced = true
	before := fileText(filepath.Join(f.dir, "commands"))
	f.b.onInbound(InboundMessage{ID: "held-free", Room: room, Nick: "peer", From: room + "/peer", Body: "@free next work"})
	es := f.b.inbox.protectedPending()
	if len(es) != 1 || !es[0].Addressed || es[0].ID != "held-free" {
		t.Fatalf("pre-activity @free verdict not durably held: %+v", es)
	}
	if !f.b.idleSince.Equal(since) || !f.b.awayAnnounced || f.b.presenceShow() != "away" || before != fileText(filepath.Join(f.dir, "commands")) {
		t.Fatal("held traffic changed old-session activity or RPC")
	}
	f.b.onInbound(InboundMessage{ID: "ignored", Room: room, Nick: "peer", From: room + "/peer", Body: "unrelated chatter"})
	if len(f.b.inbox.protectedPending()) != 1 || !f.b.idleSince.Equal(since) || f.b.presenceShow() != "away" {
		t.Fatal("irrelevant traffic entered boundary or changed presence")
	}
}
