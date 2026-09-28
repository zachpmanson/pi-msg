package main

import (
	"strings"
	"testing"
)

// roomWith builds a room bridge (nick "pi") whose roster holds the given nicks
// alongside us, and returns it with the room JID.
func roomWith(nicks ...string) (*Bridge, string) {
	b := roomBridge()
	x := NewXMPPBridge(b.acct, func(InboundMessage) {}, func(_, _ string) {})
	occ := map[string]string{"pi": "pi@x.com"}
	for _, n := range nicks {
		occ[n] = n + "@x.com"
	}
	x.occupants["team@muc.x.com"] = occ
	b.xmpp = x
	return b, "team@muc.x.com"
}

// An outbound room message only reaches anyone if it says who it is for. The
// check has to accept every form #106 makes an address — an "@handle", a
// broadcast, a bare name — and reject the forms that reach nobody: no mention at
// all, a handle nobody answers to, our own handle, and a mention written inside
// a fence, a quote or an inline span.
func TestAddressesRoom(t *testing.T) {
	b, room := roomWith("peppy", "slippy")
	cases := []struct {
		body string
		want bool
	}{
		{"@peppy the path is /srv/x", true},
		{"peppy, the path is /srv/x", true},
		{"@everyone report in", true},
		{"@ALL report in", true},
		{"the path is /srv/x", false},
		{"everyone should report in", false}, // bare broadcast word is prose
		{"that's all from me", false},        // ditto
		{"we're all here", false},            // ditto
		{"@nosuchhandle the path is /srv/x", false},
		{"@pi the path is /srv/x", false}, // our own handle addresses nobody
		{"```\n@peppy in a fence\n```", false},
		{"> @peppy in a quote", false},
		{"the flag is `@peppy` quoted", false},
		{"zach, the report is up", true}, // the owner is addressable by name
		{"@zach the report is up", true},
		{"mail me at bob@example.com", false}, // domain, not a mention
		// A bare JID still names the occupant in prose, and inbound dispatch
		// treats that as addressing them (#106), so it is addressed here too.
		{"ping peppy@x.com directly", true},
		{"Thanks Peppy — done!", true}, // bare mention, any case
		{"peppytest is a different word", false},
	}
	for _, c := range cases {
		if got := b.addressesRoom(room, c.body); got != c.want {
			t.Errorf("addressesRoom(%q) = %v, want %v", c.body, got, c.want)
		}
	}

	// No roster → we cannot tell a bare name from ordinary prose, so the check
	// never claims a message addressed nobody. A wrong warning is worse than
	// none, the rule handleIssues already follows.
	empty := roomBridge()
	empty.xmpp = NewXMPPBridge(empty.acct, func(InboundMessage) {}, func(_, _ string) {})
	if !empty.addressesRoom(room, "nothing addressed here") {
		t.Error("empty roster must count as addressed, so nothing is warned about")
	}
}

// Mentions must be sliced from the SCANNED text. Stripping an inline span
// shortens the body, so indices from the scan applied to the raw body slice the
// wrong characters: the first live symptom would be an agent warned about a
// handle it never typed, on a message that addressed its peer correctly.
func TestMentionsSlicedFromScan(t *testing.T) {
	body := "the flag is `@peppy` then @slippy takes it"
	got := mentionsIn(body)
	if len(got) != 1 || got[0] != "slippy" {
		t.Fatalf("mentionsIn(%q) = %v, want [slippy]", body, got)
	}

	b, room := roomWith("peppy", "slippy")
	unknown, _, _ := b.handleIssues(room, body)
	if len(unknown) != 0 {
		t.Errorf("handleIssues(%q) reported unknown handles %v, want none", body, unknown)
	}
	if !b.addressesRoom(room, body) {
		t.Errorf("addressesRoom(%q) = false, want true (slippy is named)", body)
	}
}

// The untagged warning is for a peer's handoff, once per run, and only when the
// message really addressed nobody. br.rpc is nil here, which also pins that the
// warning never touches rpc unless it has something to say.
func TestUntaggedWarningGates(t *testing.T) {
	b, room := roomWith("peppy", "slippy")

	// Owner-triggered run: a tagless report is correct writing, so no warning.
	b.setTurnDest(room, false)
	b.warnUntaggedRoomReply(room, "the report is up")
	if b.untaggedWarned() {
		t.Error("owner-triggered run must not warn about an untagged reply")
	}

	// Peer-triggered run, untagged reply: warn, once.
	b.setTurnDest(room, true)
	b.warnUntaggedRoomReply(room, "the report is up")
	if !b.untaggedWarned() {
		t.Error("peer-triggered untagged reply must warn")
	}
	// Already warned this run: the flag is what bounds the nudge.
	b.setUntaggedWarned(false)

	// Peer-triggered, addressed: nothing to correct.
	b.setTurnDest(room, true)
	b.warnUntaggedRoomReply(room, "@peppy the report is up")
	if b.untaggedWarned() {
		t.Error("an addressed reply must not warn")
	}

	// The peer flag is per turn: a later owner message must clear it, or a
	// report written for the owner would be warned about for the rest of the
	// session.
	b.setTurnDest(room, true)
	b.setTurnDest(b.acct.Owner, false)
	if b.peerTriggered() {
		t.Error("peer flag survived a turn that named a different trigger")
	}
}

// The wording has to be usable from the inside: it must name the consequence
// (nobody was delivered it), list the handles that would work, and NOT offer
// "to: noop" — a fleet trained to prefer silence takes the cheap out, and this
// message has already reached the room.
func TestUntaggedRoomNotice(t *testing.T) {
	got := untaggedRoomNotice([]string{"peppy", "slippy"})
	for _, want := range []string{"tagged nobody", "@peppy", "@slippy", "@everyone"} {
		if !strings.Contains(got, want) {
			t.Errorf("notice %q missing %q", got, want)
		}
	}
	if strings.Contains(got, "noop") {
		t.Errorf("notice offers the silence out: %q", got)
	}
	if bare := untaggedRoomNotice(nil); !strings.Contains(bare, "No other handle") {
		t.Errorf("empty roster wording = %q, want the no-handle form", bare)
	}
}
