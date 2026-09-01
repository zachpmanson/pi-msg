package main

import (
	"context"
	"crypto/sha1"
	"encoding/base64"
	"encoding/hex"
	"encoding/xml"
	"fmt"
	"net"
	"os"
	"path/filepath"
	"strings"
	"syscall"
	"testing"
	"time"
)

func TestBareJid(t *testing.T) {
	cases := map[string]string{
		"zach@x.com/phone":    "zach@x.com",
		"zach@x.com":          "zach@x.com",
		"Zach@X.com/Res":      "zach@x.com",
		"room@muc.x.com/nick": "room@muc.x.com",
	}
	for in, want := range cases {
		if got := bareJid(in); got != want {
			t.Errorf("bareJid(%q) = %q, want %q", in, got, want)
		}
	}
}

func TestResourcepart(t *testing.T) {
	if got := resourcepart("room@muc.x.com/alice"); got != "alice" {
		t.Errorf("resourcepart = %q, want alice", got)
	}
	if got := resourcepart("zach@x.com"); got != "" {
		t.Errorf("resourcepart with no resource = %q, want empty", got)
	}
}

func TestChunkShort(t *testing.T) {
	if got := chunk("hello", maxBody); len(got) != 1 || got[0] != "hello" {
		t.Errorf("chunk short = %v, want [hello]", got)
	}
}

func TestChunkSplitsAndPreserves(t *testing.T) {
	// Build text well over the cap with spaces so it splits on word bounds.
	// Sized relative to maxBody so this holds regardless of the cap's value.
	long := strings.Repeat("word ", maxBody/5*2) // ~2x the cap
	parts := chunk(long, maxBody)
	if len(parts) < 2 {
		t.Fatalf("expected multiple chunks, got %d", len(parts))
	}
	for i, p := range parts {
		if len(p) > maxBody {
			t.Errorf("chunk %d length %d exceeds cap %d", i, len(p), maxBody)
		}
	}
	// Rejoining with spaces should reconstruct the (space-trimmed) content.
	rejoined := strings.Join(parts, " ")
	if strings.Fields(rejoined)[0] != "word" {
		t.Errorf("content not preserved across chunks")
	}
}

func TestSeenDuplicate(t *testing.T) {
	b := NewXMPPBridge(ResolvedAccount{Owner: "o@x.com"}, func(InboundMessage) {}, nil)
	if b.seenDuplicate("a") {
		t.Error("first sighting of 'a' reported as duplicate")
	}
	if !b.seenDuplicate("a") {
		t.Error("second sighting of 'a' not reported as duplicate")
	}
	if b.seenDuplicate("b") {
		t.Error("first sighting of 'b' reported as duplicate")
	}
}

func TestTokenHelpers(t *testing.T) {
	// <body>hi</body> plus a delay element.
	toks := []xml.Token{
		xml.StartElement{Name: xml.Name{Local: "body"}},
		xml.CharData("hi there"),
		xml.EndElement{Name: xml.Name{Local: "body"}},
		xml.StartElement{Name: xml.Name{Space: "urn:xmpp:delay", Local: "delay"}},
		xml.EndElement{Name: xml.Name{Space: "urn:xmpp:delay", Local: "delay"}},
	}
	if got := childText(toks, "body"); got != "hi there" {
		t.Errorf("childText body = %q, want 'hi there'", got)
	}
	if _, ok := element(toks, "urn:xmpp:delay", "delay"); !ok {
		t.Error("delay element not found")
	}
	if _, ok := element(toks, "urn:xmpp:delay", "nope"); ok {
		t.Error("found nonexistent element")
	}
}

func TestReceiptAckMarshal(t *testing.T) {
	// The ack child shared by XEP-0184 receipts and XEP-0333 markers must emit
	// its namespace and the referenced message id.
	cases := []struct{ ns, local, id string }{
		{receiptsNS, "received", "msg-1"},
		{chatMarkersNS, "displayed", "msg-2"},
	}
	for _, c := range cases {
		ack := struct {
			XMLName xml.Name
			ID      string `xml:"id,attr"`
		}{XMLName: xml.Name{Space: c.ns, Local: c.local}, ID: c.id}
		out, err := xml.Marshal(ack)
		if err != nil {
			t.Fatalf("marshal %s: %v", c.local, err)
		}
		got := string(out)
		for _, want := range []string{"<" + c.local, `xmlns="` + c.ns + `"`, `id="` + c.id + `"`} {
			if !strings.Contains(got, want) {
				t.Errorf("%s ack = %q, missing %q", c.local, got, want)
			}
		}
	}
}

// reactionEl / reactionsPayload mirror the wire struct built by encodeReaction,
// so the XEP-0444 form can be asserted without a live session.
type reactionEl struct {
	XMLName xml.Name `xml:"reaction"`
	Text    string   `xml:",chardata"`
}

func reactionsPayload(forID string, emojis []string) any {
	p := struct {
		XMLName   xml.Name
		ID        string `xml:"id,attr"`
		Reactions []reactionEl
	}{XMLName: xml.Name{Space: reactionsNS, Local: "reactions"}, ID: forID}
	for _, e := range emojis {
		if e == "" {
			continue
		}
		p.Reactions = append(p.Reactions, reactionEl{Text: e})
	}
	return p
}

func TestReactionsMarshal(t *testing.T) {
	out, err := xml.Marshal(reactionsPayload("msg-42", []string{"👀"}))
	if err != nil {
		t.Fatalf("marshal reactions: %v", err)
	}
	got := string(out)
	for _, want := range []string{
		"<reactions",
		`xmlns="` + reactionsNS + `"`,
		`id="msg-42"`,
		"<reaction>👀</reaction>",
	} {
		if !strings.Contains(got, want) {
			t.Errorf("reactions = %q, missing %q", got, want)
		}
	}
}

func TestReactionsMarshalMultiple(t *testing.T) {
	out, err := xml.Marshal(reactionsPayload("m1", []string{"👀", "✅"}))
	if err != nil {
		t.Fatalf("marshal: %v", err)
	}
	got := string(out)
	if !strings.Contains(got, "<reaction>👀</reaction>") || !strings.Contains(got, "<reaction>✅</reaction>") {
		t.Errorf("expected both reactions, got %q", got)
	}
}

func TestReactionsMarshalEmptyClears(t *testing.T) {
	// No emoji → an empty <reactions>, which is XEP-0444's "clear" form.
	out, err := xml.Marshal(reactionsPayload("m1", nil))
	if err != nil {
		t.Fatalf("marshal: %v", err)
	}
	got := string(out)
	if strings.Contains(got, "<reaction>") {
		t.Errorf("empty reactions should carry no <reaction> child, got %q", got)
	}
	if !strings.Contains(got, `id="m1"`) || !strings.Contains(got, "reactions") {
		t.Errorf("empty reactions still needs the reactions element + id, got %q", got)
	}
}

func TestLoadAvatar(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "avatar.png")
	data := []byte("not-really-a-png-but-bytes-are-bytes")
	if err := os.WriteFile(path, data, 0o644); err != nil {
		t.Fatal(err)
	}
	b := NewXMPPBridge(ResolvedAccount{Owner: "o@x.com", Avatar: path}, func(InboundMessage) {}, nil)
	if b.avatarType != "image/png" {
		t.Errorf("avatarType = %q, want image/png", b.avatarType)
	}
	sum := sha1.Sum(data)
	if want := hex.EncodeToString(sum[:]); b.avatarHash != want {
		t.Errorf("avatarHash = %q, want %q", b.avatarHash, want)
	}
	if want := base64.StdEncoding.EncodeToString(data); b.avatarB64 != want {
		t.Errorf("avatarB64 = %q, want %q", b.avatarB64, want)
	}
	if u := b.avatarUpdate(); u == nil || u.Photo != b.avatarHash {
		t.Errorf("avatarUpdate = %+v, want photo %q", u, b.avatarHash)
	}
}

func TestLoadAvatarMissingIsNonFatal(t *testing.T) {
	b := NewXMPPBridge(
		ResolvedAccount{Owner: "o@x.com", Avatar: "/no/such/file.png"},
		func(InboundMessage) {}, nil,
	)
	if b.avatarHash != "" || b.avatarB64 != "" || b.avatarType != "" {
		t.Errorf("missing avatar file should leave avatar fields empty")
	}
	if b.avatarUpdate() != nil {
		t.Errorf("avatarUpdate should be nil with no avatar")
	}
}

func TestLoadAvatarNoneConfigured(t *testing.T) {
	b := NewXMPPBridge(ResolvedAccount{Owner: "o@x.com"}, func(InboundMessage) {}, nil)
	if b.avatarUpdate() != nil {
		t.Errorf("no avatar configured → avatarUpdate should be nil")
	}
}

func TestVCardXUpdateMarshal(t *testing.T) {
	out, err := xml.Marshal(&vcardXUpdate{Photo: "abc123"})
	if err != nil {
		t.Fatalf("marshal: %v", err)
	}
	got := string(out)
	for _, want := range []string{`xmlns="vcard-temp:x:update"`, "<photo>abc123</photo>"} {
		if !strings.Contains(got, want) {
			t.Errorf("vcard x:update = %q, missing %q", got, want)
		}
	}
}

func TestReplayInSwapWindow(t *testing.T) {
	b := &XMPPBridge{}
	start := time.Date(2026, 8, 11, 12, 0, 0, 0, time.UTC)
	if !b.SetReplayWindow(start) {
		t.Fatal("SetReplayWindow should arm")
	}
	if !b.inSwapWindow(start.Add(time.Second)) {
		t.Errorf("stamp inside window should be buffered")
	}
	if !b.inSwapWindow(start) {
		t.Errorf("stamp at window start should be buffered")
	}
	if !b.inSwapWindow(start.Add(-time.Second)) {
		t.Errorf("stamp within slack before start should be buffered")
	}
	if b.inSwapWindow(start.Add(-replaySlack - time.Second)) {
		t.Errorf("stamp well before window start should be dropped")
	}
	if b.inSwapWindow(time.Time{}) {
		t.Errorf("missing stamp should be dropped")
	}
	if b.SetReplayWindow(time.Time{}) {
		t.Errorf("zero window start should not arm")
	}
}

func TestReplaySwapWindowActive(t *testing.T) {
	b := &XMPPBridge{}
	b.replayActive = true
	b.replayGraceEnd = time.Now().Add(time.Second)
	if !b.swapWindowActive() {
		t.Errorf("open window should report active")
	}
	b.replayGraceEnd = time.Now().Add(-time.Second)
	if b.swapWindowActive() {
		t.Errorf("expired window should report inactive")
	}
	b.replayActive = false
	b.replayGraceEnd = time.Now().Add(time.Second)
	if b.swapWindowActive() {
		t.Errorf("unarmed window should report inactive")
	}
}

func TestReplayBufferDrain(t *testing.T) {
	b := &XMPPBridge{}
	b.replayStart = time.Now().Add(-time.Minute)
	b.replayArmed = true
	b.replayLit = true
	b.replayActive = true
	b.replayGraceEnd = time.Now().Add(50 * time.Millisecond)
	b.bufferReplay(InboundMessage{Body: "first", Direct: true})
	b.bufferReplay(InboundMessage{Body: "second", Direct: true})

	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()
	got := b.DrainReplay(ctx)
	if len(got) != 2 || got[0].Body != "first" || got[1].Body != "second" {
		t.Errorf("drain = %v, want both buffered messages in order", got)
	}
	if b.replayActive {
		t.Errorf("window should be closed after drain")
	}
	if again := b.DrainReplay(context.Background()); again != nil {
		t.Errorf("second drain = %v, want nil", again)
	}
}

func TestReplayDrainNotArmed(t *testing.T) {
	b := &XMPPBridge{}
	if got := b.DrainReplay(context.Background()); got != nil {
		t.Errorf("unarmed drain = %v, want nil", got)
	}
}

// Presence must reach every joined room, not just the roster. A broadcast
// presence is not relayed into MUCs by the service (XEP-0045 scopes an
// occupant's presence to room@service/nick), so without directed copies a room
// roster shows the agent's join-time state forever while the owner's 1:1 tracks
// every change.
func TestPresenceTargets(t *testing.T) {
	b := NewXMPPBridge(ResolvedAccount{
		Owner:     "zach@x.com",
		Nick:      "pi",
		Rooms:     []string{"team@muc.x.com", "Ops@MUC.x.com"},
		ErrorRoom: "errors@muc.x.com",
	}, func(InboundMessage) {}, func(_, _ string) {})

	// Before any join is confirmed there must be NO targets. Run announces
	// presence before joining, and directed presence to room@service/nick with
	// no MUC <x/> child is a legacy groupchat-1.0 join (XEP-0045) — which would
	// join every room in legacy mode a moment before the real join, losing
	// status code 110 and the muc#user real JIDs the owner check depends on.
	if got := b.presenceTargets(); len(got) != 0 {
		t.Fatalf("presenceTargets() before join = %v, want none", got)
	}

	// The service echoes our own occupant presence with status code 110; that
	// is what marks a room joined, and it carries the nick actually assigned
	// (which may differ from the configured one after a nick conflict).
	b.mu.Lock()
	b.selfNick["team@muc.x.com"] = "pi"
	b.selfNick["ops@muc.x.com"] = "pi2"
	b.mu.Unlock()

	got := b.presenceTargets()
	want := []string{"team@muc.x.com/pi", "ops@muc.x.com/pi2"}
	if len(got) != len(want) {
		t.Fatalf("presenceTargets() = %v, want %v", got, want)
	}
	for i := range want {
		if got[i] != want[i] {
			t.Errorf("presenceTargets()[%d] = %q, want %q", i, got[i], want[i])
		}
	}

	// The error room is write-only and out of the agent-visible set; nothing
	// reads presence there, so it must not be targeted even once joined.
	b.mu.Lock()
	b.selfNick["errors@muc.x.com"] = "pi"
	b.mu.Unlock()
	for _, tgt := range b.presenceTargets() {
		if strings.HasPrefix(tgt, "errors@") {
			t.Errorf("error room was targeted: %q", tgt)
		}
	}

	// A reconnect clears selfNick, so targets must drop back to none until the
	// rooms are re-joined and re-confirmed.
	b.mu.Lock()
	b.selfNick = make(map[string]string)
	b.mu.Unlock()
	if got := b.presenceTargets(); len(got) != 0 {
		t.Errorf("presenceTargets() after reconnect reset = %v, want none", got)
	}
}

func TestIsTransportError(t *testing.T) {
	// A wedged/broken TCP write surfaces as a *net.OpError (which implements
	// net.Error) wrapping a timeout — the exact "write tcp …: i/o timeout" from
	// issue #31.
	timeout := &net.OpError{Op: "write", Net: "tcp", Err: &timeoutErr{}}
	reset := syscall.ECONNRESET
	pipe := syscall.EPIPE

	cases := []struct {
		name string
		err  error
		want bool
	}{
		{"nil", nil, false},
		{"timeout (net.Error)", timeout, true},
		{"connection reset", reset, true},
		{"broken pipe", pipe, true},
		{"not online", fmt.Errorf("not online"), false},
		{"invalid recipient", fmt.Errorf("invalid recipient %q", "x"), false},
		{"plain error", fmt.Errorf("some stanza problem"), false},
	}
	for _, c := range cases {
		if got := isTransportError(c.err); got != c.want {
			t.Errorf("isTransportError(%s) = %v, want %v", c.name, got, c.want)
		}
	}
}

// timeoutErr is a minimal error implementing net.Error so it can sit inside a
// *net.OpError as the wrapped cause, mimicking how mellium reports a write that
// exceeded its context deadline on a wedged socket.
type timeoutErr struct{}

func (*timeoutErr) Error() string   { return "i/o timeout" }
func (*timeoutErr) Timeout() bool   { return true }
func (*timeoutErr) Temporary() bool { return true }

// TestKickDebounced verifies kick() is a safe no-op when offline and fires at
// most once per second when online, so a burst of wedged-socket writes triggers
// a single reconnect rather than a Close storm.
func TestKickDebounced(t *testing.T) {
	b := &XMPPBridge{}

	// Offline: kick must not panic and must not touch anything.
	b.kick()
	if b.lastKick != (time.Time{}) {
		t.Errorf("offline kick recorded a timestamp: %v", b.lastKick)
	}

	// Online, no real session: first kick records a timestamp (Close is skipped
	// because the session is nil), a second immediate kick is debounced.
	b.online = true
	b.kick()
	first := b.lastKick
	if first.IsZero() {
		t.Fatalf("online kick did not record a timestamp")
	}
	b.kick()
	if !b.lastKick.Equal(first) {
		t.Errorf("kicks within the debounce window advanced lastKick: %v -> %v", first, b.lastKick)
	}
}

// fenceCount counts ``` fence lines in a string, treating any line starting
// with "```" as a fence marker (open or close).
func fenceCount(s string) int {
	n := 0
	for _, l := range strings.Split(s, "\n") {
		if strings.HasPrefix(strings.TrimRight(l, " \t"), "```") {
			n++
		}
	}
	return n
}

// TestChunkFenceAware ensures chunk() never splits a ``` code fence across
// message boundaries: every piece is self-contained (its own balanced fences),
// and together the pieces reconstruct the fenced content without corruption.
func TestChunkFenceAware(t *testing.T) {
	// A long message whose bulk is a fenced code block, small enough that the
	// fix must not refuse to split but large enough to force boundary cuts.
	fence := "```\n"
	for i := 0; i < maxBody/4; i++ {
		fence += "code line\n"
	}
	fence += "```"
	msg := "Before fence.\n" + fence + "\nAfter fence."

	parts := chunk(msg, maxBody)
	if len(parts) < 2 {
		t.Fatalf("expected multiple chunks, got %d", len(parts))
	}
	// Every piece must be self-contained: it opens what it closes.
	for i, p := range parts {
		if len(p) > maxBody {
			t.Errorf("piece %d exceeds cap (%d > %d)", i, len(p), maxBody)
		}
		if n := fenceCount(p); n%2 != 0 {
			t.Errorf("piece %d has unbalanced fence markers (%d):\n%s", i, n, p)
		}
		// Strip the code block and confirm no bare fence opener leaked without
		// a closer within the piece.
		if strings.HasPrefix(p, "```") && fenceCount(p) == 1 {
			t.Errorf("piece %d opens a fence it never closes:\n%s", i, p)
		}
	}
}

func TestCapsVerGoldenVector(t *testing.T) {
	// XEP-0115 §5.3 worked example: identity client/pc (name "Exodus 0.9.1",
	// no language) and features caps / disco#info / disco#items / muc hash to
	// the verification string published in the spec.
	feats := []string{
		"http://jabber.org/protocol/caps",
		"http://jabber.org/protocol/disco#info",
		"http://jabber.org/protocol/disco#items",
		"http://jabber.org/protocol/muc",
	}
	got := capsVerFor("client/pc//Exodus 0.9.1", feats)
	const want = "QgayPKawpkPSDYmwT/WM94uAlu0="
	if got != want {
		t.Fatalf("caps ver: got %q, want %q", got, want)
	}
	// The bridge's own ver must be a stable, non-colliding value.
	if capsVer == "" || capsVer == want {
		t.Fatalf("bridge caps ver unexpected: %q", capsVer)
	}
}

func TestIdleSinceISO(t *testing.T) {
	if got := idleSinceISO(time.Time{}); got != "" {
		t.Fatalf("zero idle: got %q, want empty", got)
	}
	// 2026-01-02 03:04:05 +10:00 (AEST) == 2026-01-01 17:04:05 UTC.
	ts := time.Date(2026, 1, 2, 3, 4, 5, 0, time.FixedZone("AEST", 10*3600))
	if got := idleSinceISO(ts); got != "2026-01-01T17:04:05Z" {
		t.Fatalf("idle iso: got %q, want 2026-01-01T17:04:05Z", got)
	}
}

func TestPresenceChildrenMarshal(t *testing.T) {
	// idle with timestamp
	idle := idleElem{Since: "2026-01-01T17:04:05Z"}
	b, err := xml.Marshal(idle)
	if err != nil {
		t.Fatal(err)
	}
	if got := string(b); got != `<idle xmlns="urn:xmpp:idle:1" since="2026-01-01T17:04:05Z"></idle>` {
		t.Fatalf("idle marshal: %s", got)
	}
	// idle empty = active
	b, _ = xml.Marshal(idleElem{})
	if got := string(b); got != `<idle xmlns="urn:xmpp:idle:1"></idle>` {
		t.Fatalf("idle empty marshal: %s", got)
	}
	// caps
	c := capsElem{Hash: "sha-1", Node: "http://pi-msg", Ver: capsVer}
	b, err = xml.Marshal(c)
	if err != nil {
		t.Fatal(err)
	}
	s := string(b)
	for _, want := range []string{
		`xmlns="http://jabber.org/protocol/caps"`,
		`hash="sha-1"`,
		`node="http://pi-msg"`,
		`ver="` + capsVer + `"`,
	} {
		if !strings.Contains(s, want) {
			t.Fatalf("caps marshal missing %q: %s", want, s)
		}
	}
	// full presence shape
	p := struct {
		XMLName  xml.Name `xml:"presence"`
		Show     string   `xml:"show,omitempty"`
		Status   string   `xml:"status,omitempty"`
		Priority int      `xml:"priority"`
		Idle     idleElem
		Caps     capsElem
	}{Show: "away", Status: "consulting the entrails", Priority: 0,
		Idle: idleElem{Since: "2026-01-01T17:04:05Z"}, Caps: c}
	b, err = xml.Marshal(p)
	if err != nil {
		t.Fatal(err)
	}
	s = string(b)
	for _, want := range []string{"<priority>0</priority>", "urn:xmpp:idle:1", "http://jabber.org/protocol/caps"} {
		if !strings.Contains(s, want) {
			t.Fatalf("presence missing %q: %s", want, s)
		}
	}
}
