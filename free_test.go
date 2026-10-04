package main

import "testing"

// setShow puts the bridge's own presence into the given show state — the one
// fact the @free gate reads (#130).
func setShow(b *Bridge, show string) {
	if show == "" {
		// A fresh bridge is already available with the start label; nothing to do.
		return
	}
	b.xmpp.SetPresence(show, "test")
}

// @free is a room-wide handle like @everyone, and it carries the same
// requirements: the sigil is mandatory ("free" alone is ordinary prose) and the
// handle must end at a word boundary. It is also the ONE form the classification
// gate applies to, so freeOnlyBroadcast must not claim a body that also carries
// an unconditional handle — @all is the forcing function and always wins.
func TestFreeHandleSyntax(t *testing.T) {
	b := roomBridge()
	cases := []struct {
		in        string
		addressed bool
		freeOnly  bool
	}{
		{"@free status please", true, true},
		{"@FREE caps count too", true, true},
		{"@free, and @everyone if you are busy", true, false},
		{"@everyone and @free", true, false},
		{"free to take this one", false, false}, // no sigil — ordinary prose
		{"@freely available", false, false},     // handle must end at the word
		{"feel @free to jump in", true, true},   // sigil is what counts
		{"```\n@free in a fence\n```", false, false},
		{"> @free in a quote", false, false},
		{"the flag is `@free` quoted", false, false},
	}
	for _, c := range cases {
		if got, _ := b.matchTrigger("team@muc.x.com", c.in); got != c.addressed {
			t.Errorf("matchTrigger(%q) = %v, want %v", c.in, got, c.addressed)
		}
		if got := freeOnlyBroadcast(c.in); got != c.freeOnly {
			t.Errorf("freeOnlyBroadcast(%q) = %v, want %v", c.in, got, c.freeOnly)
		}
	}
}

// The gate, in one table: @free and an untagged owner message reach an away
// agent and nobody else, while every aimed form — a name, @everyone — is
// delivered whatever the presence says.
func TestFreeBroadcastGating(t *testing.T) {
	cases := []struct {
		name      string
		body      string
		fromOwner bool
		show      string
		addressed bool // m.Addressed: already admitted on first arrival
		want      roomAction
	}{
		{"owner untagged, away", "status please", true, "away", false, actionCanonical},
		{"owner untagged, listening", "status please", true, "", false, actionNotOurs},
		{"owner untagged, dnd", "status please", true, "dnd", false, actionNotOurs},
		{"owner @free, away", "@free status please", true, "away", false, actionCanonical},
		{"owner @free, listening", "@free status please", true, "", false, actionNotOurs},
		{"owner @all, listening", "@all status please", true, "", false, actionCanonical},
		{"owner @everyone, dnd", "@everyone status please", true, "dnd", false, actionCanonical},
		{"owner names us, dnd", "pi: take this", true, "dnd", false, actionCanonical},
		{"owner names a peer, away", "@peppy take this", true, "away", false, actionNotOurs},
		{"owner untagged replayed, listening", "status please", true, "", true, actionCanonical},
		{"peer @free, away", "@free status please", false, "away", false, actionCommentary},
		{"peer @free, listening", "@free status please", false, "", false, actionNotOurs},
		{"peer @free, dnd", "@free status please", false, "dnd", false, actionNotOurs},
		{"peer @all, dnd", "@all report in", false, "dnd", false, actionCommentary},
		{"peer names us, listening", "pi: report in", false, "", false, actionCommentary},
		{"peer prose naming us, dnd", "ask pi for the path", false, "dnd", false, actionCommentary},
		{"peer untagged, away", "just chatting", false, "away", false, actionNotOurs},
	}
	for _, c := range cases {
		b, room := roomWith("peppy")
		setShow(b, c.show)
		action, _, _ := b.classify(InboundMessage{
			Room: room, Nick: "peppy", Body: c.body,
			FromOwner: c.fromOwner, Addressed: c.addressed,
		})
		if action != c.want {
			t.Errorf("%s: classify(%q, owner=%v, show=%q) = %d, want %d",
				c.name, c.body, c.fromOwner, c.show, action, c.want)
		}
	}
}

// A gate keyed on the wire presence is only safe if the presence survives a
// reconnect: awayAnnounced stops the idle watcher re-announcing, so a reconnect
// that reset show to "" would drop an idle agent out of @free until its next
// work cycle.
func TestReconnectPresenceKeepsAway(t *testing.T) {
	cases := []struct {
		name                 string
		show, status, start  string
		wantShow, wantStatus string
	}{
		{"away survives the reconnect", "away", "reading obscure RFCs", "awake", "away", "reading obscure RFCs"},
		{"working resets to the start label", "dnd", "thinking…", "resumed", "", "resumed"},
		{"listening resets to the start label", "", "listening", "awake", "", "awake"},
		{"a fresh bridge announces the start label", "", "awake", "awake", "", "awake"},
	}
	for _, c := range cases {
		show, status := reconnectPresence(c.show, c.status, c.start)
		if show != c.wantShow || status != c.wantStatus {
			t.Errorf("%s: reconnectPresence(%q, %q, %q) = (%q, %q), want (%q, %q)",
				c.name, c.show, c.status, c.start, show, status, c.wantShow, c.wantStatus)
		}
	}
}
