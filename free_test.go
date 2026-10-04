package main

import "testing"

// setRan forces the has-run flag. markRan only ever sets it, so a table that
// revisits both sides of the gate needs the explicit setter.
func setRan(b *Bridge, ran bool) {
	b.mu.Lock()
	b.ranSinceStart = ran
	b.mu.Unlock()
}

// setShow puts the bridge's own presence into the given show state, and ran says
// whether a run has already happened on it — the two facts freeForSummons reads
// (#130). It sets the show explicitly rather than only when non-empty, so a table
// that visits "away" before "" lands on the available state it asked for.
func setShow(b *Bridge, show string, ran bool) {
	setRan(b, ran)
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

// freeForSummons is the gate itself: away, or a bridge nothing has been asked of.
// The fresh half is what keeps a just-deployed fleet summonable; the settled half
// is what keeps a peer from waking an agent that has worked and has deliberately
// been left alone since.
func TestFreeForSummons(t *testing.T) {
	cases := []struct {
		name string
		show string
		ran  bool
		want bool
	}{
		{"fresh bridge, available", "", false, true},
		{"fresh bridge, visibly working", "dnd", false, false},
		{"settled bridge, available", "", true, false},
		{"settled bridge, working", "dnd", true, false},
		{"settled bridge, away", "away", true, true},
		{"fresh bridge, away", "away", false, true},
	}
	for _, c := range cases {
		b, _ := roomWith("peppy")
		setShow(b, c.show, c.ran)
		if got := b.freeForSummons(); got != c.want {
			t.Errorf("%s (show=%q, ran=%v): freeForSummons = %v, want %v", c.name, c.show, c.ran, got, c.want)
		}
	}
}

// The gate, in one table: @free and an untagged owner message reach a free agent
// and nobody else, while every aimed form — a name, @everyone — is delivered
// whatever the state says.
func TestFreeBroadcastGating(t *testing.T) {
	cases := []struct {
		name      string
		body      string
		fromOwner bool
		show      string
		ran       bool
		addressed bool // m.Addressed: already admitted on first arrival
		want      roomAction
	}{
		{"owner untagged, fresh", "status please", true, "", false, false, actionCanonical},
		{"owner untagged, settled", "status please", true, "", true, false, actionNotOurs},
		{"owner untagged, working", "status please", true, "dnd", true, false, actionNotOurs},
		{"owner untagged, away", "status please", true, "away", true, false, actionCanonical},
		{"owner @free, fresh", "@free status please", true, "", false, false, actionCanonical},
		{"owner @free, settled", "@free status please", true, "", true, false, actionNotOurs},
		{"owner @all, settled", "@all status please", true, "", true, false, actionCanonical},
		{"owner @everyone, working", "@everyone status please", true, "dnd", true, false, actionCanonical},
		{"owner names us, working", "pi: take this", true, "dnd", true, false, actionCanonical},
		{"owner names a peer, away", "@peppy take this", true, "away", true, false, actionNotOurs},
		{"owner untagged replayed, settled", "status please", true, "", true, true, actionCanonical},
		{"peer @free, fresh", "@free status please", false, "", false, false, actionCommentary},
		{"peer @free, settled", "@free status please", false, "", true, false, actionNotOurs},
		{"peer @free, working", "@free status please", false, "dnd", true, false, actionNotOurs},
		{"peer @all, working", "@all report in", false, "dnd", true, false, actionCommentary},
		{"peer names us, settled", "pi: report in", false, "", true, false, actionCommentary},
		{"peer prose naming us, working", "ask pi for the path", false, "dnd", true, false, actionCommentary},
		{"peer untagged, away", "just chatting", false, "away", true, false, actionNotOurs},
	}
	for _, c := range cases {
		b, room := roomWith("peppy")
		setShow(b, c.show, c.ran)
		action, _, _ := b.classify(InboundMessage{
			Room: room, Nick: "peppy", Body: c.body,
			FromOwner: c.fromOwner, Addressed: c.addressed,
		})
		if action != c.want {
			t.Errorf("%s: classify(%q, owner=%v, show=%q, ran=%v) = %d, want %d",
				c.name, c.body, c.fromOwner, c.show, c.ran, action, c.want)
		}
	}
}

// A /new is a fresh start in-process: the agent has a blank session and nothing
// asked of it, so @free must reach it again rather than leaving it in the settled
// window for idleAwayTimeout. The dnd guard still holds — /new does not kill
// background processes.
func TestNewMakesAnAgentFree(t *testing.T) {
	b, _ := roomWith("peppy")

	// Settled after work: a run has happened and we have not drifted away.
	setShow(b, "", true)
	if b.freeForSummons() {
		t.Fatal("a settled bridge must not be free")
	}

	// /new drops the agent back to the fresh state, so an untagged owner
	// message reaches it again immediately.
	b.markFresh()
	if !b.freeForSummons() {
		t.Error("a /new bridge must be free again")
	}
	action, _, _ := b.classify(InboundMessage{Room: "team@muc.x.com", Nick: "zach", Body: "status please", FromOwner: true})
	if action != actionCanonical {
		t.Errorf("untagged owner message after /new = %d, want actionCanonical", action)
	}

	// Work started after the /new puts it back out of reach.
	b.markRan()
	if b.freeForSummons() {
		t.Error("a bridge that has run since the /new must not be free")
	}

	// A /new that settles into a background process is not free either: dnd
	// outranks fresh.
	b.markFresh()
	setShow(b, "dnd", false)
	if b.freeForSummons() {
		t.Error("a /new'd bridge waiting on a process must not be free")
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
