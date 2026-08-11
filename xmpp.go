package main

import (
	"context"
	"crypto/rand"
	"crypto/sha1"
	"crypto/tls"
	"encoding/base64"
	"encoding/hex"
	"encoding/xml"
	"fmt"
	"io"
	"mime"
	"net"
	"net/http"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"time"

	"mellium.im/sasl"
	"mellium.im/xmlstream"
	"mellium.im/xmpp"
	"mellium.im/xmpp/disco"
	"mellium.im/xmpp/jid"
	"mellium.im/xmpp/ping"
	"mellium.im/xmpp/stanza"
	"mellium.im/xmpp/upload"
)

// maxBody is a soft cap for a single outgoing message body; longer text is
// split on newline / word boundaries so servers don't reject oversized
// stanzas.
const maxBody = 50000

// Restart-gap inbound replay: messages the server replayed (offline storage /
// MUC history) that carry a delay stamp inside the swap window are buffered and
// handed to the resumed session instead of being dropped as stale backfill.
const (
	// replayGracePeriod is how long after (re)connect the bridge accepts
	// server-replayed (delayed) messages as belonging to the restart gap. The
	// offline/MUC backlog lands within a moment of connect; this lets it all
	// arrive while keeping the window tight.
	replayGracePeriod = 3 * time.Second
	// replaySlack tolerates clock skew when matching a message's delay stamp to
	// the swap window.
	replaySlack = 2 * time.Second
)

const chatStatesNS = "http://jabber.org/protocol/chatstates"

// Receipt namespaces: XEP-0184 message delivery receipts and XEP-0333 chat
// markers. The bridge honors whichever an incoming owner message requests.
const (
	receiptsNS    = "urn:xmpp:receipts"
	chatMarkersNS = "urn:xmpp:chat-markers:0"
)

// reactionsNS is XEP-0444 message reactions: the agent reacts to an owner
// message with emoji (e.g. 👀 picked up, ✅ done, ⛔ aborted).
const reactionsNS = "urn:xmpp:reactions:0"

// newStanzaID generates a random stanza id.
func newStanzaID() string {
	b := make([]byte, 8)
	_, _ = rand.Read(b)
	return hex.EncodeToString(b)
}

// bareJid returns the bare (localpart@domain) form of a JID, lowercased.
func bareJid(full string) string {
	if slash := strings.IndexByte(full, '/'); slash >= 0 {
		full = full[:slash]
	}
	return strings.ToLower(full)
}

// resourcepart returns the part of a full JID after '/', or "".
func resourcepart(full string) string {
	if slash := strings.IndexByte(full, '/'); slash >= 0 {
		return full[slash+1:]
	}
	return ""
}

// chunk splits text into pieces no longer than max, preferring newline then
// word boundaries.
func chunk(text string, max int) []string {
	if len(text) <= max {
		return []string{text}
	}
	var chunks []string
	rest := text
	for len(rest) > max {
		cut := strings.LastIndexByte(rest[:max], '\n')
		if cut < max/2 {
			cut = strings.LastIndexByte(rest[:max], ' ')
		}
		if cut < max/2 {
			cut = max
		}
		chunks = append(chunks, rest[:cut])
		rest = strings.TrimLeft(rest[cut:], " \t\r\n")
	}
	if rest != "" {
		chunks = append(chunks, rest)
	}
	return chunks
}

// InboundMessage is a received message the bridge should act on, after
// transport-level guards. In 1:1 mode it is always the owner. In room mode it
// may be any occupant; classification (canonical/commentary/ambient) is left
// to the bridge.
type InboundMessage struct {
	Body      string // message text
	Nick      string // occupant nick (room mode), or "" for 1:1
	RealJID   string // sender's bare real JID if known, else ""
	FromOwner bool   // sender is the configured owner
	Direct    bool   // arrived as a 1:1 chat, not groupchat (reply goes back 1:1)
	Room      string // source room bare JID (room mode); "" for 1:1
	ID        string // stanza id (used as the XEP-0444 reaction target)
	From      string // full from-JID, so a reaction routes back to that resource
}

// XMPPBridge owns a single account's XMPP connection: it maintains a
// (reconnecting) session, delivers relevant incoming messages via onMsg, and
// exposes send/presence/chat-state helpers the bridge calls from other
// goroutines.
type XMPPBridge struct {
	acct      ResolvedAccount
	ownerBare string
	roomBares map[string]bool // bare JIDs of the joined rooms
	onMsg     func(InboundMessage)
	logf      func(level, msg string)

	mu       sync.Mutex
	session  *xmpp.Session
	online   bool
	show     string // presence <show>: "" (available) or "dnd"/"away"/… (availability axis)
	presence string // presence <status> free text (activity axis)

	// startStatus is the presence <status> announced on (re)connect, before any
	// activity occurs. The bridge sets it to distinguish a fresh start ("awake")
	// from a resumed continuation ("resumed"); it falls back to "awake".
	startStatus string

	seen      map[string]struct{}
	seenOrder []string

	// MUC occupant tracking (room mode), keyed by room bare JID.
	occupants map[string]map[string]string // roomBare -> nick -> bare real JID
	selfNick  map[string]string            // roomBare -> our nick (per status code 110)

	uploadMu  sync.Mutex
	uploadSvc string // resolved XEP-0363 upload component JID (cached)

	// XEP-0153 vCard avatar, loaded once from acct.Avatar. Empty when no avatar
	// is configured or the file couldn't be read.
	avatarType string // image MIME type, e.g. "image/png"
	avatarB64  string // base64 of the raw image bytes (vCard <BINVAL>)
	avatarHash string // lowercase hex SHA-1 of the raw bytes (presence photo hash)

	// msgHistory maps stanza IDs to their source JID (inbound and outbound) so
	// send_reaction can target arbitrary messages by ID. Capped at 500 entries;
	// oldest is evicted when full.
	msgHistory map[string]msgHistoryEntry

	// Restart-gap replay state. replayStart is the swap-window start (when the
	// account went offline); replayArmed is set once at startup when a window
	// marker exists; on the first successful connect the window is "lit" and
	// delayed messages stamped within it are buffered into replayBuf until
	// replayGraceEnd. DrainReplay hands the buffer to the resumed session.
	replayStart    time.Time
	replayArmed    bool
	replayLit      bool // replay window armed on the first connect only
	replayActive   bool // currently collecting swap-window messages
	replayGraceEnd time.Time
	replayBuf      []InboundMessage
}

// NewXMPPBridge constructs a bridge. onMsg is called for each message that
// should drive the agent; logf receives diagnostics.
func NewXMPPBridge(acct ResolvedAccount, onMsg func(InboundMessage), logf func(level, msg string)) *XMPPBridge {
	roomBares := make(map[string]bool, len(acct.Rooms))
	for _, room := range acct.Rooms {
		roomBares[bareJid(room)] = true
	}
	b := &XMPPBridge{
		acct:        acct,
		ownerBare:   bareJid(acct.Owner),
		roomBares:   roomBares,
		onMsg:       onMsg,
		logf:        logf,
		presence:    "awake (" + nowStamp() + ")",
		startStatus: "awake",
		seen:        make(map[string]struct{}),
		occupants:   make(map[string]map[string]string),
		selfNick:    make(map[string]string),
		msgHistory:  make(map[string]msgHistoryEntry),
	}
	b.loadAvatar()
	return b
}

// loadAvatar reads the configured XEP-0153 avatar image and precomputes the
// vCard payload (base64 + MIME type) and the presence photo hash (SHA-1). A
// missing path or unreadable file is a logged warning, not fatal — the bridge
// just runs without an avatar.
func (b *XMPPBridge) loadAvatar() {
	if b.acct.Avatar == "" {
		return
	}
	data, err := os.ReadFile(b.acct.Avatar)
	if err != nil {
		b.log("warning", "avatar not loaded: "+err.Error())
		return
	}
	if len(data) == 0 {
		b.log("warning", "avatar not loaded: file is empty: "+b.acct.Avatar)
		return
	}
	ctype := mime.TypeByExtension(filepath.Ext(b.acct.Avatar))
	if ctype == "" {
		ctype = http.DetectContentType(data)
	}
	if i := strings.IndexByte(ctype, ';'); i >= 0 { // drop any "; charset=…"
		ctype = strings.TrimSpace(ctype[:i])
	}
	sum := sha1.Sum(data)
	b.avatarType = ctype
	b.avatarB64 = base64.StdEncoding.EncodeToString(data)
	b.avatarHash = hex.EncodeToString(sum[:])
	b.log("info", fmt.Sprintf("avatar loaded (%s, %d bytes, sha1 %s)", ctype, len(data), b.avatarHash))
}

func (b *XMPPBridge) log(level, msg string) {
	if b.logf != nil {
		b.logf(level, msg)
	}
}

// Run connects and serves in a loop with exponential backoff until ctx is
// canceled. onConnected (may be nil) is invoked after each successful connect,
// once presence has been announced.
func (b *XMPPBridge) Run(ctx context.Context, onConnected func()) {
	backoff := time.Second
	for {
		err := b.serve(ctx, onConnected)
		if ctx.Err() != nil {
			return
		}
		b.log("warning", fmt.Sprintf("connection lost: %v; reconnecting in %s", err, backoff))
		select {
		case <-ctx.Done():
			return
		case <-time.After(backoff):
		}
		if backoff < 30*time.Second {
			backoff *= 2
		}
	}
}

// serve establishes one session and runs its read loop until it drops.
func (b *XMPPBridge) serve(ctx context.Context, onConnected func()) error {
	session, err := b.connect(ctx)
	if err != nil {
		return fmt.Errorf("connect: %w", err)
	}
	defer session.Close()

	b.mu.Lock()
	b.session = session
	b.online = true
	// Re-assert the startup status on every (re)connect so the roster shows the
	// correct label (fresh start "awake" vs resumed "resumed") rather than a
	// stale idle label from a previous session.
	b.show = ""
	b.presence = b.startStatus + " (" + nowStamp() + ")"
	show, status := b.show, b.presence
	// Reset occupant state for this fresh connection; a re-join repopulates it.
	b.occupants = make(map[string]map[string]string)
	b.selfNick = make(map[string]string)
	b.mu.Unlock()

	// Announce presence so the server routes messages to this resource and the
	// owner's roster shows the bot online.
	if err := b.encodePresence(show, status); err != nil {
		b.setOffline()
		return fmt.Errorf("presence: %w", err)
	}
	for _, room := range b.acct.Rooms {
		if err := b.joinRoom(room); err != nil {
			b.setOffline()
			return fmt.Errorf("join room %s: %w", room, err)
		}
		b.log("info", fmt.Sprintf("joined room %s as %s", room, b.acct.Nick))
	}
	// The error room is joined at the XMPP layer (so groupchat sends are
	// accepted and keepalive covers it) but it is deliberately NOT added to
	// roomBares, so it stays invisible to the agent: no dispatch, no occupants,
	// no allowlist. Write-only by construction.
	if b.acct.ErrorRoom != "" {
		if err := b.joinRoom(b.acct.ErrorRoom); err != nil {
			b.setOffline()
			return fmt.Errorf("join error room %s: %w", b.acct.ErrorRoom, err)
		}
		b.log("info", fmt.Sprintf("joined write-only error room %s as %s", b.acct.ErrorRoom, b.acct.Nick))
	}
	b.log("info", fmt.Sprintf("online as %s, relaying to %s", b.acct.JID, b.ownerBare))
	// Open the restart-gap replay window on this (the first) connection. The
	// offline/MUC backlog is delivered right after presence/join, so any delayed
	// message stamped within the swap window gets buffered until the grace
	// window closes.
	b.mu.Lock()
	if b.replayArmed && !b.replayLit {
		b.replayLit = true
		b.replayActive = true
		b.replayGraceEnd = time.Now().Add(replayGracePeriod)
	}
	b.mu.Unlock()
	if onConnected != nil {
		onConnected()
	}

	// Keepalive: XEP-0199 server pings (and XEP-0410 MUC self-pings) surface a
	// silently-dropped connection or a silent MUC eviction. It runs until the
	// read loop below returns, at which point keepaliveCtx is canceled.
	keepaliveCtx, stopKeepalive := context.WithCancel(ctx)
	defer stopKeepalive()
	go b.keepalive(keepaliveCtx, session)

	// Publish the vCard avatar (XEP-0153) once the read loop below is up to
	// route the IQ result. The presence broadcast above already carried the
	// photo hash, so clients will fetch the vCard as soon as it lands.
	if b.avatarB64 != "" {
		go func() {
			if err := b.publishAvatar(); err != nil {
				b.log("warning", "avatar vCard publish failed: "+err.Error())
			} else {
				b.log("info", "avatar vCard published")
			}
		}()
	}

	serveErr := session.Serve(xmpp.HandlerFunc(b.handle))
	b.setOffline()
	if ctx.Err() != nil {
		return ctx.Err()
	}
	if serveErr != nil {
		return serveErr
	}
	return fmt.Errorf("stream closed")
}

func (b *XMPPBridge) setOffline() {
	b.mu.Lock()
	b.online = false
	b.session = nil
	b.mu.Unlock()
}

// SetReplayWindow arms inbound replay for the restart gap. Call before Run;
// start is the swap-window start (the time the account went offline). Returns
// true when a window was armed.
func (b *XMPPBridge) SetReplayWindow(start time.Time) bool {
	if start.IsZero() {
		return false
	}
	b.mu.Lock()
	b.replayStart = start
	b.replayArmed = true
	b.mu.Unlock()
	return true
}

// swapWindowActive reports whether the replay window is currently open (armed,
// lit on the first connect, and still inside its grace period).
func (b *XMPPBridge) swapWindowActive() bool {
	b.mu.Lock()
	defer b.mu.Unlock()
	return b.replayActive && time.Now().Before(b.replayGraceEnd)
}

// inSwapWindow reports whether a delayed message's stamp falls within the swap
// window (allowing clock skew).
func (b *XMPPBridge) inSwapWindow(stamp time.Time) bool {
	if stamp.IsZero() {
		return false
	}
	b.mu.Lock()
	start := b.replayStart
	b.mu.Unlock()
	return !stamp.Add(replaySlack).Before(start)
}

// bufferReplay appends a swap-window message to the restart replay buffer,
// preserving delivery order.
func (b *XMPPBridge) bufferReplay(m InboundMessage) {
	b.mu.Lock()
	defer b.mu.Unlock()
	b.replayBuf = append(b.replayBuf, m)
}

// DrainReplay blocks until the replay-window grace period has elapsed after the
// first connection, then returns (and clears) the buffered swap-window messages.
// It returns nil immediately if no window was armed, and nil on ctx cancel.
func (b *XMPPBridge) DrainReplay(ctx context.Context) []InboundMessage {
	b.mu.Lock()
	active, end := b.replayActive, b.replayGraceEnd
	b.mu.Unlock()
	if !active {
		return nil
	}
	if d := time.Until(end); d > 0 {
		select {
		case <-ctx.Done():
			return nil
		case <-time.After(d):
		}
	}
	b.mu.Lock()
	buf := b.replayBuf
	b.replayBuf = nil
	b.replayActive = false
	b.mu.Unlock()
	return buf
}

// markOutbound updates the persistent last-outbound floor so an ungraceful
// crash can still bound its replay window. Called whenever a chat message is
// actually sent.
func (b *XMPPBridge) markOutbound() {
	markLastOut(b.logf, b.acct.Name, time.Now())
}

// pingTimeout bounds each keepalive ping's round trip.
const pingTimeout = 15 * time.Second

// keepalive periodically pings the server (XEP-0199) to detect a
// silently-dropped connection, and in room mode self-pings each joined room
// (XEP-0410) to detect a silent eviction. A failed server ping tears the
// session down so Run reconnects; a failed self-ping re-joins just that room.
// It returns when ctx is canceled (the read loop ended) or the interval is
// non-positive (keepalive disabled).
func (b *XMPPBridge) keepalive(ctx context.Context, session *xmpp.Session) {
	if b.acct.PingInterval <= 0 {
		return // disabled
	}
	server, err := jid.Parse(domainOf(b.acct.JID))
	if err != nil {
		b.log("warning", "keepalive disabled: bad server jid: "+err.Error())
		return
	}
	ticker := time.NewTicker(b.acct.PingInterval)
	defer ticker.Stop()
	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
		}
		if err := b.pingOnce(ctx, session, server); err != nil {
			b.log("warning", "keepalive ping failed; forcing reconnect: "+err.Error())
			// Closing unblocks session.Serve, so serve() returns and Run
			// reconnects. serve()'s deferred Close makes the double-close a
			// harmless no-op.
			session.Close()
			return
		}
		for _, room := range b.acct.Rooms {
			b.selfPing(ctx, session, room)
		}
		if errRoom := b.acct.ErrorRoom; errRoom != "" {
			b.selfPing(ctx, session, errRoom)
		}
	}
}

// pingOnce sends a single XEP-0199 ping to `to` bounded by pingTimeout.
func (b *XMPPBridge) pingOnce(ctx context.Context, session *xmpp.Session, to jid.JID) error {
	pingCtx, cancel := context.WithTimeout(ctx, pingTimeout)
	defer cancel()
	return ping.Send(pingCtx, session, to)
}

// selfPing performs an XEP-0410 MUC self-ping to confirm we're still joined to
// room; on error it re-joins. ping.Send treats a service-unavailable reply as
// success (we exist at the occupant JID but don't answer pings), which is
// exactly the "still joined" signal — any other error means we've been
// desynced/ejected.
func (b *XMPPBridge) selfPing(ctx context.Context, session *xmpp.Session, room string) {
	roomBare := bareJid(room)
	occupant, err := jid.Parse(roomBare + "/" + b.ownNick(roomBare))
	if err != nil {
		return
	}
	if err := b.pingOnce(ctx, session, occupant); err != nil {
		b.log("warning", fmt.Sprintf("self-ping to %s failed; re-joining: %v", roomBare, err))
		if err := b.joinRoom(room); err != nil {
			b.log("warning", fmt.Sprintf("re-join %s failed: %v", roomBare, err))
		}
	}
}

// connect dials and negotiates a client session for the account.
func (b *XMPPBridge) connect(ctx context.Context) (*xmpp.Session, error) {
	addr := b.acct.JID
	if b.acct.Resource != "" {
		addr = b.acct.JID + "/" + b.acct.Resource
	}
	j, err := jid.Parse(addr)
	if err != nil {
		return nil, fmt.Errorf("invalid jid %q: %w", b.acct.JID, err)
	}

	target := strings.TrimPrefix(b.acct.Service, "xmpp://")
	if target == "" {
		target = j.Domain().String() + ":5222"
	}

	dialCtx, cancel := context.WithTimeout(ctx, 30*time.Second)
	defer cancel()
	var d net.Dialer
	conn, err := d.DialContext(dialCtx, "tcp", target)
	if err != nil {
		return nil, fmt.Errorf("dialing %s: %w", target, err)
	}

	features := []xmpp.StreamFeature{
		xmpp.StartTLS(&tls.Config{ServerName: j.Domain().String()}),
		// SCRAM-SHA-256 first (works against ejabberd via mellium, unlike the
		// @xmpp/client SCRAM-SHA-1 the TS build had to disable), PLAIN last.
		xmpp.SASL("", b.acct.Password, sasl.ScramSha256Plus, sasl.ScramSha256, sasl.ScramSha1Plus, sasl.ScramSha1, sasl.Plain),
		xmpp.BindResource(),
	}
	session, err := xmpp.NewClientSession(ctx, j, conn, features...)
	if err != nil {
		conn.Close()
		return nil, err
	}
	return session, nil
}

// incomingMsg is a received message stanza reduced to the fields the bridge
// cares about.
type incomingMsg struct {
	from        string
	typ         string
	body        string
	id          string
	delay       bool      // carried a XEP-0203 <delay/> (server-replayed history)
	delayStamp  time.Time // the <delay stamp> attr, zero if absent/unparsable
	wantReceipt bool      // carried a XEP-0184 <request/> (delivery receipt)
	markable    bool      // carried a XEP-0333 <markable/> (chat marker)
}

// handle is the mellium read-loop callback for one inbound stanza.
func (b *XMPPBridge) handle(t xmlstream.TokenReadEncoder, start *xml.StartElement) error {
	switch start.Name.Local {
	case "message":
		toks, err := xmlstream.ReadAll(t)
		if err != nil {
			return err
		}
		m := incomingMsg{
			from: attr(start.Attr, "from"),
			typ:  attr(start.Attr, "type"),
			id:   attr(start.Attr, "id"),
			body: childText(toks, "body"),
		}
		if d, ok := element(toks, "urn:xmpp:delay", "delay"); ok {
			m.delay = true
			if s := attr(d.Attr, "stamp"); s != "" {
				if t, err := time.Parse(time.RFC3339, s); err == nil {
					m.delayStamp = t
				}
			}
		}
		_, m.wantReceipt = element(toks, receiptsNS, "request")
		_, m.markable = element(toks, chatMarkersNS, "markable")
		b.dispatch(m)
		return nil
	case "presence":
		toks, err := xmlstream.ReadAll(t)
		if err != nil {
			return err
		}
		return b.handlePresence(start, toks)
	case "iq":
		toks, err := xmlstream.ReadAll(t)
		if err != nil {
			return err
		}
		// Answer XEP-0199 ping requests so a server/peer keepalive sees us as
		// alive. (Responses to our own pings are correlated by the session
		// before reaching this handler, so they never arrive here.)
		if attr(start.Attr, "type") == "get" {
			if _, ok := element(toks, ping.NS, "ping"); ok {
				return b.encodePong(attr(start.Attr, "from"), attr(start.Attr, "id"))
			}
		}
		return nil
	default:
		// Anything else: drain so the stream advances.
		_, err := xmlstream.Copy(xmlstream.Discard(), t)
		return err
	}
}

// encodePong replies to an XEP-0199 ping with an empty result IQ echoing the
// request id back to its sender.
func (b *XMPPBridge) encodePong(to, id string) error {
	session := b.currentSession()
	if session == nil {
		return fmt.Errorf("not online")
	}
	resp := stanza.IQ{ID: id, Type: stanza.ResultIQ}
	if to != "" {
		toJID, err := jid.Parse(to)
		if err != nil {
			return fmt.Errorf("invalid ping sender %q: %w", to, err)
		}
		resp.To = toJID
	}
	ctx, cancel := context.WithTimeout(context.Background(), pingTimeout)
	defer cancel()
	return session.Encode(ctx, resp)
}

// dispatch applies delivery policy and forwards a message to onMsg. Routing is
// by stanza type, not mode: groupchat goes to the room path (room mode only),
// while 1:1 chat is always handled — so even in room mode the owner can DM the
// bot and get a 1:1 reply.
func (b *XMPPBridge) dispatch(m incomingMsg) {
	if m.typ == "groupchat" {
		if b.acct.RoomMode() {
			b.dispatchRoom(m)
		}
		return // stray groupchat outside room mode: ignore
	}
	b.dispatchDirect(m)
}

// dispatchDirect forwards a 1:1 chat message from the owner. Works in both 1:1
// and room mode.
func (b *XMPPBridge) dispatchDirect(m incomingMsg) {
	// Only direct chat (or type-less) messages from the owner.
	if m.typ != "" && m.typ != "chat" && m.typ != "normal" {
		return
	}
	if bareJid(m.from) != b.ownerBare {
		return
	}
	if strings.TrimSpace(m.body) == "" {
		return // chat-states, receipts, empty
	}
	// Drop server-replayed history (offline / MAM catch-up on reconnect) unless
	// it falls inside the restart swap window — then buffer it for the resumed
	// session instead of silently dropping it.
	if m.delay {
		if b.swapWindowActive() && b.inSwapWindow(m.delayStamp) {
			b.bufferReplay(InboundMessage{
				Body: m.body, RealJID: b.ownerBare, FromOwner: true,
				Direct: true, ID: m.id, From: m.from,
			})
		}
		return
	}
	if m.id != "" && b.seenDuplicate(m.id) {
		return
	}
	// Record the inbound message in history so send_reaction can target it by ID.
	if m.id != "" {
		b.recordMessage(m.id, m.from)
	}
	// The agent is about to take this in — acknowledge it as read/delivered.
	b.sendReceipts(m)
	b.onMsg(InboundMessage{Body: m.body, RealJID: b.ownerBare, FromOwner: true, Direct: true, ID: m.id, From: m.from})
}

// dispatchRoom applies groupchat guards and forwards room messages to onMsg,
// tagging each with the room it arrived from so replies route back to it.
func (b *XMPPBridge) dispatchRoom(m incomingMsg) {
	if m.typ != "groupchat" {
		return // ignore 1:1 DMs to the bot in room mode (v1)
	}
	room := bareJid(m.from)
	if !b.isRoomJID(room) {
		return
	}
	nick := resourcepart(m.from)
	if nick == "" {
		return // room-level stanza (e.g. subject with no occupant)
	}
	if nick == b.ownNick(room) {
		return // our own echo
	}
	if m.delay {
		// Replayed MUC history: buffer only what falls inside the restart swap
		// window for the resumed session; drop the rest as stale backfill.
		if b.swapWindowActive() && b.inSwapWindow(m.delayStamp) {
			real := b.occupantRealJID(room, nick)
			b.bufferReplay(InboundMessage{
				Body:      m.body,
				Nick:      nick,
				RealJID:   real,
				FromOwner: real != "" && real == b.ownerBare,
				Room:      room,
				ID:        m.id,
				From:      m.from,
			})
		}
		return
	}
	if strings.TrimSpace(m.body) == "" {
		return // subject-only, chat-states, empty
	}
	if m.id != "" && b.seenDuplicate(m.id) {
		return
	}
	// Record the inbound room message in history so send_reaction can target it by ID.
	if m.id != "" {
		b.recordMessage(m.id, m.from)
	}
	real := b.occupantRealJID(room, nick)
	b.onMsg(InboundMessage{
		Body:      m.body,
		Nick:      nick,
		RealJID:   real,
		FromOwner: real != "" && real == b.ownerBare,
		Room:      room,
		ID:        m.id,
		From:      m.from,
	})
}

// handlePresence maintains the MUC occupant map (room mode) and auto-approves
// roster subscription requests (1:1).
func (b *XMPPBridge) handlePresence(start *xml.StartElement, toks []xml.Token) error {
	from := attr(start.Attr, "from")
	ptype := attr(start.Attr, "type")

	if room := bareJid(from); b.isRoomJID(room) {
		nick := resourcepart(from)
		if nick == "" {
			return nil
		}
		// Our own occupant presence carries status code 110.
		if hasStatusCode(toks, "110") {
			b.mu.Lock()
			b.selfNick[room] = nick
			b.mu.Unlock()
		}
		real := ""
		if item, ok := element(toks, "http://jabber.org/protocol/muc#user", "item"); ok {
			real = bareJid(attr(item.Attr, "jid"))
		}
		b.mu.Lock()
		if b.occupants[room] == nil {
			b.occupants[room] = make(map[string]string)
		}
		if ptype == "unavailable" {
			delete(b.occupants[room], nick)
		} else if real != "" {
			b.occupants[room][nick] = real
		}
		b.mu.Unlock()
		return nil
	}

	// 1:1: auto-approve subscription requests so the owner sees accurate
	// presence without manual approval.
	if ptype == string(stanza.SubscribePresence) && from != "" {
		return b.approveSubscription(from)
	}
	return nil
}

// ownNick returns our occupant nick in room (server-confirmed via 110 if known,
// else the configured nick).
func (b *XMPPBridge) ownNick(room string) string {
	b.mu.Lock()
	defer b.mu.Unlock()
	if n := b.selfNick[room]; n != "" {
		return n
	}
	return b.acct.Nick
}

// occupantRealJID returns the bare real JID mapped to nick in room, or "".
func (b *XMPPBridge) occupantRealJID(room, nick string) string {
	b.mu.Lock()
	defer b.mu.Unlock()
	if m := b.occupants[room]; m != nil {
		return m[nick]
	}
	return ""
}

// seenDuplicate reports whether id was already handled, recording it if not.
// Bounded to the most recent 500 ids.
func (b *XMPPBridge) seenDuplicate(id string) bool {
	b.mu.Lock()
	defer b.mu.Unlock()
	if _, ok := b.seen[id]; ok {
		return true
	}
	b.seen[id] = struct{}{}
	b.seenOrder = append(b.seenOrder, id)
	if len(b.seenOrder) > 500 {
		evicted := b.seenOrder[0]
		b.seenOrder = b.seenOrder[1:]
		delete(b.seen, evicted)
	}
	return false
}

// Send delivers a chat message to the owner, splitting long text across
// stanzas.
func (b *XMPPBridge) Send(text string) string { return b.SendChatTo(b.acct.Owner, text) }

// SendChatTo posts a 1:1 chat message to an arbitrary JID, splitting long text.
// Returns the stanza ID of the last chunk sent, or "" if nothing was sent.
func (b *XMPPBridge) SendChatTo(to, text string) string {
	if b.currentSession() == nil {
		b.log("warning", "send skipped: not online")
		return ""
	}
	var lastID string
	for _, part := range chunk(text, maxBody) {
		id, err := b.encodeChat(to, part, stanza.ChatMessage)
		if err != nil {
			b.log("error", "send failed: "+err.Error())
			break
		}
		lastID = id
	}
	if lastID != "" {
		b.markOutbound()
	}
	return lastID
}

// destKind classifies an agent-chosen reply destination for delivery policy.
type destKind int

const (
	destBlocked destKind = iota // not permitted (unknown JID)
	destRoom                    // a joined MUC → groupchat
	destUser                    // owner or a known room occupant → 1:1 chat
)

// classifyDest decides how (and whether) to deliver a reply the agent addressed
// to an explicit JID. Rooms the bridge has joined get groupchat; the owner and
// real JIDs currently seen in a room get 1:1 chat; anything else is refused, so
// the agent can't message arbitrary users on the server.
func (b *XMPPBridge) classifyDest(dest string) destKind {
	bare := bareJid(dest)
	switch {
	case bare == "":
		return destBlocked
	case b.isRoomJID(bare):
		return destRoom
	case bare == b.ownerBare, b.isOccupant(bare):
		return destUser
	default:
		return destBlocked
	}
}

// isRoomJID reports whether bare is one of the rooms the bridge has joined.
func (b *XMPPBridge) isRoomJID(bare string) bool {
	return b.roomBares[bare]
}

// isOccupant reports whether bare is a real JID currently tracked in any room.
func (b *XMPPBridge) isOccupant(bare string) bool {
	b.mu.Lock()
	defer b.mu.Unlock()
	for _, occ := range b.occupants {
		for _, real := range occ {
			if real == bare {
				return true
			}
		}
	}
	return false
}

// SetStartupStatus sets the presence <status> label announced on (re)connect,
// before any activity run. Call it before Run to distinguish a fresh start
// ("awake") from a resumed continuation ("resumed"). It falls back to "awake".
func (b *XMPPBridge) SetStartupStatus(status string) {
	if status == "" {
		status = "awake"
	}
	b.mu.Lock()
	b.startStatus = status
	b.mu.Unlock()
}

// SetPresence announces presence with a show (availability axis: "" = available,
// "dnd" = busy, …) and a status label (activity axis), remembering both for
// re-assertion on reconnect. Redundant no-change calls are dropped so streaming
// deltas don't spray identical presence stanzas.
func (b *XMPPBridge) SetPresence(show, status string) {
	b.mu.Lock()
	if show == b.show && status == b.presence {
		b.mu.Unlock()
		return // unchanged; skip the stanza
	}
	b.show = show
	b.presence = status
	online := b.online
	b.mu.Unlock()
	if !online {
		return
	}
	if err := b.encodePresence(show, status); err != nil {
		b.log("warning", "presence failed: "+err.Error())
	}
}

// GoOffline broadcasts an unavailable presence so the owner's roster stops
// showing the bot online, carrying an optional status describing why (e.g.
// "session ended — …"). Safe to call when already offline (no-op).
func (b *XMPPBridge) GoOffline(status string) {
	if err := b.encodeUnavailable(status); err != nil {
		b.log("warning", "offline presence failed: "+err.Error())
	}
}

// ChatState sends an XEP-0085 chat-state notification to the owner (the
// "typing…" indicator). "composing" shows typing; "active" clears it.
func (b *XMPPBridge) ChatState(state string) {
	if b.currentSession() == nil {
		return
	}
	if err := b.encodeChatState(b.acct.Owner, state, stanza.ChatMessage); err != nil {
		b.log("warning", "chatstate failed: "+err.Error())
	}
}

func (b *XMPPBridge) currentSession() *xmpp.Session {
	b.mu.Lock()
	defer b.mu.Unlock()
	if !b.online {
		return nil
	}
	return b.session
}

// --- stanza encoders ---

func (b *XMPPBridge) encodeChat(to, body string, typ stanza.MessageType) (string, error) {
	session := b.currentSession()
	if session == nil {
		return "", fmt.Errorf("not online")
	}
	toJID, err := jid.Parse(to)
	if err != nil {
		return "", fmt.Errorf("invalid recipient %q: %w", to, err)
	}
	id := newStanzaID()
	msg := struct {
		stanza.Message
		Body string `xml:"body"`
	}{
		Message: stanza.Message{ID: id, To: toJID, Type: typ},
		Body:    body,
	}
	ctx, cancel := context.WithTimeout(context.Background(), 15*time.Second)
	defer cancel()
	b.recordMessage(id, to)
	return id, session.Encode(ctx, msg)
}

func (b *XMPPBridge) encodeChatState(to, state string, typ stanza.MessageType) error {
	session := b.currentSession()
	if session == nil {
		return fmt.Errorf("not online")
	}
	toJID, err := jid.Parse(to)
	if err != nil {
		return fmt.Errorf("invalid recipient %q: %w", to, err)
	}
	msg := struct {
		stanza.Message
		State struct {
			XMLName xml.Name
		}
	}{
		Message: stanza.Message{To: toJID, Type: typ},
	}
	msg.State.XMLName = xml.Name{Space: chatStatesNS, Local: state}
	ctx, cancel := context.WithTimeout(context.Background(), 15*time.Second)
	defer cancel()
	return session.Encode(ctx, msg)
}

// sendReceipts acknowledges an accepted 1:1 owner message: a XEP-0184 delivery
// receipt if the sender requested one, and a XEP-0333 "displayed" chat marker
// if the message was markable — a genuine read receipt, since the agent is
// about to act on it. Sent to the message's full from-JID so it routes back to
// the originating resource. Best-effort; failures are logged, not fatal.
func (b *XMPPBridge) sendReceipts(m incomingMsg) {
	if m.id == "" || m.from == "" {
		return
	}
	if m.wantReceipt {
		if err := b.encodeReceipt(m.from, receiptsNS, "received", m.id); err != nil {
			b.log("warning", "delivery receipt failed: "+err.Error())
		}
	}
	if m.markable {
		if err := b.encodeReceipt(m.from, chatMarkersNS, "displayed", m.id); err != nil {
			b.log("warning", "chat marker failed: "+err.Error())
		}
	}
}

// encodeReceipt sends a bodyless message to `to` carrying a single ack element
// (namespace ns, local name) whose `id` attribute references the acknowledged
// message forID — the wire form shared by XEP-0184 receipts and XEP-0333
// markers.
func (b *XMPPBridge) encodeReceipt(to, ns, local, forID string) error {
	session := b.currentSession()
	if session == nil {
		return fmt.Errorf("not online")
	}
	toJID, err := jid.Parse(to)
	if err != nil {
		return fmt.Errorf("invalid recipient %q: %w", to, err)
	}
	msg := struct {
		stanza.Message
		Ack struct {
			XMLName xml.Name
			ID      string `xml:"id,attr"`
		}
	}{
		Message: stanza.Message{To: toJID, Type: stanza.ChatMessage},
	}
	msg.Ack.XMLName = xml.Name{Space: ns, Local: local}
	msg.Ack.ID = forID
	ctx, cancel := context.WithTimeout(context.Background(), 15*time.Second)
	defer cancel()
	return session.Encode(ctx, msg)
}

// msgHistoryEntry records an inbound or outbound message stanza in the
// history ring buffer, so the bridge can resolve a stanza ID to its source
// JID without the agent having to remember it.
type msgHistoryEntry struct {
	FromJID   string
	Timestamp time.Time
}

// msgHistoryCap is the maximum number of stanza IDs retained in history.
const msgHistoryCap = 500

// recordMessage records a stanza ID -> JID mapping in the history ring
// buffer, evicting the oldest entry if the buffer is full.
func (b *XMPPBridge) recordMessage(id, fromJID string) {
	if id == "" {
		return
	}
	b.mu.Lock()
	defer b.mu.Unlock()
	if _, exists := b.msgHistory[id]; exists {
		b.msgHistory[id] = msgHistoryEntry{FromJID: fromJID, Timestamp: time.Now()}
		return
	}
	if len(b.msgHistory) >= msgHistoryCap {
		// Evict the oldest entry.
		var oldestKey string
		var oldestTime time.Time
		for k, v := range b.msgHistory {
			if oldestKey == "" || v.Timestamp.Before(oldestTime) {
				oldestKey, oldestTime = k, v.Timestamp
			}
		}
		delete(b.msgHistory, oldestKey)
	}
	b.msgHistory[id] = msgHistoryEntry{FromJID: fromJID, Timestamp: time.Now()}
}

// lookupMessage returns the from-JID for a recorded stanza ID, or "" if not found.
func (b *XMPPBridge) lookupMessage(id string) string {
	if id == "" {
		return ""
	}
	b.mu.Lock()
	defer b.mu.Unlock()
	if e, ok := b.msgHistory[id]; ok {
		return e.FromJID
	}
	return ""
}

// SendReaction reacts to message forID (authored by `to`) with the given emoji,
// per XEP-0444. Each stanza carries the full reaction set for the
// (agent, message) pair, so a later call replaces an earlier one; calling with
// no emoji sends an empty <reactions>, clearing any prior reaction.
// Best-effort: a missing target or an encode failure is logged, not fatal.
func (b *XMPPBridge) SendReaction(to, forID string, emojis ...string) {
	if to == "" || forID == "" {
		return
	}
	if err := b.encodeReaction(to, forID, emojis); err != nil {
		b.log("warning", "reaction failed: "+err.Error())
	}
}

// SendReactionTo reacts to message forID (authored by `to`) with a single
// emoji, taking explicit target parameters. Unlike SendReaction, it accepts a
// single required emoji string. Calling with an empty emoji clears the reaction.
func (b *XMPPBridge) SendReactionTo(to, forID, emoji string) {
	b.SendReaction(to, forID, emoji)
}

// encodeReaction sends a bodyless message to `to` carrying an XEP-0444
// <reactions id='forID'> element with one <reaction> child per emoji. An empty
// emojis slice yields an empty <reactions>, which clears the reaction set.
// When the target is a known room, the stanza is sent as groupchat so clients
// display the reaction within the room context.
func (b *XMPPBridge) encodeReaction(to, forID string, emojis []string) error {
	session := b.currentSession()
	if session == nil {
		return fmt.Errorf("not online")
	}
	toJID, err := jid.Parse(to)
	if err != nil {
		return fmt.Errorf("invalid recipient %q: %w", to, err)
	}
	msgType := stanza.ChatMessage
	if b.isRoomJID(to) {
		msgType = stanza.GroupChatMessage
	}
	type reaction struct {
		XMLName xml.Name `xml:"reaction"`
		Text    string   `xml:",chardata"`
	}
	msg := struct {
		stanza.Message
		Reactions struct {
			XMLName   xml.Name
			ID        string `xml:"id,attr"`
			Reactions []reaction
		}
	}{
		Message: stanza.Message{To: toJID, Type: msgType},
	}
	msg.Reactions.XMLName = xml.Name{Space: reactionsNS, Local: "reactions"}
	msg.Reactions.ID = forID
	for _, e := range emojis {
		if e == "" {
			continue
		}
		msg.Reactions.Reactions = append(msg.Reactions.Reactions, reaction{Text: e})
	}
	ctx, cancel := context.WithTimeout(context.Background(), 15*time.Second)
	defer cancel()
	return session.Encode(ctx, msg)
}

// vcardXUpdate is the XEP-0153 <x xmlns='vcard-temp:x:update'> presence child
// that advertises the SHA-1 hash of the account's vCard avatar, so clients know
// to (re)fetch it.
type vcardXUpdate struct {
	XMLName xml.Name `xml:"vcard-temp:x:update x"`
	Photo   string   `xml:"photo"`
}

// avatarUpdate returns the vcard-temp:x:update element to attach to a presence
// stanza, or nil when no avatar is configured. As a pointer it is omitted from
// the marshaled presence entirely when nil.
func (b *XMPPBridge) avatarUpdate() *vcardXUpdate {
	if b.avatarHash == "" {
		return nil
	}
	return &vcardXUpdate{Photo: b.avatarHash}
}

// encodePresence announces presence with an optional show and status, carrying
// the XEP-0153 avatar hash when one is configured. An empty "to" broadcasts
// (roster-wide) presence.
func (b *XMPPBridge) encodePresence(show, status string) error {
	session := b.currentSession()
	if session == nil {
		return fmt.Errorf("not online")
	}
	p := struct {
		XMLName xml.Name `xml:"presence"`
		Show    string   `xml:"show,omitempty"`
		Status  string   `xml:"status,omitempty"`
		VCard   *vcardXUpdate
	}{Show: show, Status: status, VCard: b.avatarUpdate()}
	ctx, cancel := context.WithTimeout(context.Background(), 15*time.Second)
	defer cancel()
	return session.Encode(ctx, p)
}

// encodeUnavailable broadcasts a roster-wide unavailable presence, marking the
// bot offline, with an optional <status> line describing why.
func (b *XMPPBridge) encodeUnavailable(status string) error {
	session := b.currentSession()
	if session == nil {
		return fmt.Errorf("not online")
	}
	p := struct {
		XMLName xml.Name `xml:"presence"`
		Type    string   `xml:"type,attr"`
		Status  string   `xml:"status,omitempty"`
	}{Type: "unavailable", Status: status}
	ctx, cancel := context.WithTimeout(context.Background(), 15*time.Second)
	defer cancel()
	return session.Encode(ctx, p)
}

// publishAvatar stores the configured image in the account's vCard via an
// XEP-0153 IQ-set (vcard-temp <PHOTO>). No-op when no avatar is configured.
// Must run while the read loop (Serve) is active, since EncodeIQ blocks on the
// IQ result — so the bridge calls it from a goroutine.
func (b *XMPPBridge) publishAvatar() error {
	if b.avatarB64 == "" {
		return nil
	}
	session := b.currentSession()
	if session == nil {
		return fmt.Errorf("not online")
	}
	iq := struct {
		stanza.IQ
		VCard struct {
			XMLName xml.Name `xml:"vcard-temp vCard"`
			Photo   struct {
				XMLName xml.Name `xml:"PHOTO"`
				Type    string   `xml:"TYPE"`
				BinVal  string   `xml:"BINVAL"`
			}
		}
		// jid.JID implements xml.MarshalerAttr, so encoding/xml's `omitempty`
		// on stanza.IQ.To never applies (isEmptyValue doesn't special-case
		// structs) — a zero-value To always marshals to `to=""`, which
		// ejabberd rejects as "Bad value of attribute 'to'". Set it to our
		// own bare JID (the standard self-addressed form for vcard-temp) so
		// the attribute is always well-formed.
	}{IQ: stanza.IQ{Type: stanza.SetIQ, To: session.LocalAddr().Bare()}}
	iq.VCard.Photo.Type = b.avatarType
	iq.VCard.Photo.BinVal = b.avatarB64
	ctx, cancel := context.WithTimeout(context.Background(), 15*time.Second)
	defer cancel()
	resp, err := session.EncodeIQ(ctx, iq)
	if err != nil {
		return err
	}
	// Manually check the IQ response for errors instead of using
	// session.UnmarshalIQ with nil — the mellium library panics on nil
	// interface type assertions when the server responds with an error.
	tok, err := resp.Token()
	if err != nil {
		return err
	}
	start, ok := tok.(xml.StartElement)
	if !ok {
		return fmt.Errorf("publish avatar: expected IQ start element")
	}
	_, err = stanza.UnmarshalIQError(resp, start)
	if err != nil {
		return fmt.Errorf("publish avatar: %w", err)
	}
	resp.Close()
	return nil
}

// SendRoomTo posts a groupchat message to a room JID, splitting long text.
// Returns the stanza ID of the last chunk sent, or "" if nothing was sent.
func (b *XMPPBridge) SendRoomTo(room, text string) string {
	if b.currentSession() == nil {
		b.log("warning", "room send skipped: not online")
		return ""
	}
	var lastID string
	for _, part := range chunk(text, maxBody) {
		id, err := b.encodeChat(room, part, stanza.GroupChatMessage)
		if err != nil {
			b.log("error", "room send failed: "+err.Error())
			break
		}
		lastID = id
	}
	if lastID != "" {
		b.markOutbound()
	}
	return lastID
}

// SendFile uploads a local file via XEP-0363 and sends its URL to `to` as an
// XEP-0066 out-of-band message (groupchat if `to` is a joined room, else 1:1),
// so the recipient's client shows it as a downloadable file.
func (b *XMPPBridge) SendFile(to, path string) error {
	session := b.currentSession()
	if session == nil {
		return fmt.Errorf("not online")
	}
	fi, err := os.Stat(path)
	if err != nil {
		return fmt.Errorf("stat %s: %w", path, err)
	}
	if fi.IsDir() {
		return fmt.Errorf("%s is a directory", path)
	}
	ctx, cancel := context.WithTimeout(context.Background(), 120*time.Second)
	defer cancel()

	svc, err := b.uploadService(ctx)
	if err != nil {
		return err
	}
	svcJID, err := jid.Parse(svc)
	if err != nil {
		return fmt.Errorf("invalid upload service %q: %w", svc, err)
	}
	name := filepath.Base(path)
	ctype := mime.TypeByExtension(filepath.Ext(name))
	if ctype == "" {
		ctype = "application/octet-stream"
	}
	slot, err := upload.GetSlot(ctx, upload.File{Name: name, Size: int(fi.Size()), Type: ctype}, svcJID, session)
	if err != nil {
		return fmt.Errorf("requesting upload slot: %w", err)
	}
	f, err := os.Open(path)
	if err != nil {
		return err
	}
	defer f.Close()
	req, err := slot.Put(ctx, f)
	if err != nil {
		return err
	}
	req.ContentLength = fi.Size()
	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		return fmt.Errorf("uploading file: %w", err)
	}
	defer resp.Body.Close()
	_, _ = io.Copy(io.Discard, resp.Body)
	if resp.StatusCode != http.StatusOK && resp.StatusCode != http.StatusCreated {
		return fmt.Errorf("upload rejected (HTTP %d)", resp.StatusCode)
	}

	typ := stanza.ChatMessage
	if b.isRoomJID(bareJid(to)) {
		typ = stanza.GroupChatMessage
	}
	return b.encodeOOB(to, slot.GetURL.String(), typ)
}

// uploadService resolves (and caches) the XEP-0363 upload component JID: the
// configured one, else the first of upload.<domain> / httpupload.<domain> that
// advertises the upload feature via disco#info.
func (b *XMPPBridge) uploadService(ctx context.Context) (string, error) {
	b.uploadMu.Lock()
	cached := b.uploadSvc
	b.uploadMu.Unlock()
	if cached != "" {
		return cached, nil
	}
	session := b.currentSession()
	if session == nil {
		return "", fmt.Errorf("not online")
	}
	candidates := []string{b.acct.UploadService}
	if b.acct.UploadService == "" {
		domain := domainOf(b.acct.JID)
		candidates = []string{"upload." + domain, "httpupload." + domain}
	}
	for _, c := range candidates {
		toJID, err := jid.Parse(c)
		if err != nil {
			continue
		}
		info, err := disco.GetInfo(ctx, "", toJID, session)
		if err != nil {
			continue
		}
		for _, f := range info.Features {
			if f.Var == upload.NS {
				b.uploadMu.Lock()
				b.uploadSvc = c
				b.uploadMu.Unlock()
				return c, nil
			}
		}
	}
	return "", fmt.Errorf("no XEP-0363 upload service found (set uploadService in config)")
}

// encodeOOB sends a message whose body is url plus an XEP-0066 <x> payload, so
// clients render it as a file/link rather than plain text.
func (b *XMPPBridge) encodeOOB(to, url string, typ stanza.MessageType) error {
	session := b.currentSession()
	if session == nil {
		return fmt.Errorf("not online")
	}
	toJID, err := jid.Parse(to)
	if err != nil {
		return fmt.Errorf("invalid recipient %q: %w", to, err)
	}
	msg := struct {
		stanza.Message
		Body string `xml:"body"`
		X    struct {
			XMLName xml.Name `xml:"jabber:x:oob x"`
			URL     string   `xml:"url"`
		}
	}{
		Message: stanza.Message{ID: newStanzaID(), To: toJID, Type: typ},
		Body:    url,
	}
	msg.X.URL = url
	ctx, cancel := context.WithTimeout(context.Background(), 15*time.Second)
	defer cancel()
	return session.Encode(ctx, msg)
}

// domainOf returns the domain part of a JID (after '@', before '/').
func domainOf(j string) string {
	if at := strings.IndexByte(j, '@'); at >= 0 {
		j = j[at+1:]
	}
	if slash := strings.IndexByte(j, '/'); slash >= 0 {
		j = j[:slash]
	}
	return j
}

// joinRoom sends MUC join presence to room/nick, suppressing history replay
// (maxstanzas=0) so past room chatter isn't reprocessed as new ambient.
func (b *XMPPBridge) joinRoom(room string) error {
	session := b.currentSession()
	if session == nil {
		return fmt.Errorf("not online")
	}
	occupant := room + "/" + b.acct.Nick
	join := struct {
		XMLName xml.Name `xml:"presence"`
		To      string   `xml:"to,attr"`
		Status  string   `xml:"status,omitempty"`
		X       struct {
			XMLName xml.Name `xml:"http://jabber.org/protocol/muc x"`
			History struct {
				XMLName    xml.Name `xml:"history"`
				MaxStanzas int      `xml:"maxstanzas,attr"`
			} `xml:"history"`
		} `xml:"x"`
		VCard *vcardXUpdate // XEP-0153 avatar hash, so it shows in the room roster
	}{To: occupant, Status: b.presence, VCard: b.avatarUpdate()}
	ctx, cancel := context.WithTimeout(context.Background(), 15*time.Second)
	defer cancel()
	return session.Encode(ctx, join)
}

// approveSubscription auto-accepts a presence subscription request.
func (b *XMPPBridge) approveSubscription(from string) error {
	session := b.currentSession()
	if session == nil {
		return fmt.Errorf("not online")
	}
	fromJID, err := jid.Parse(from)
	if err != nil {
		return fmt.Errorf("invalid subscriber %q: %w", from, err)
	}
	ctx, cancel := context.WithTimeout(context.Background(), 15*time.Second)
	defer cancel()
	return session.Encode(ctx, stanza.Presence{To: fromJID, Type: stanza.SubscribedPresence})
}

// --- token helpers ---

// hasStatusCode reports whether toks contain a MUC <status code="code"/>
// element (in the muc#user namespace).
func hasStatusCode(toks []xml.Token, code string) bool {
	for _, tok := range toks {
		se, ok := tok.(xml.StartElement)
		if !ok || se.Name.Local != "status" {
			continue
		}
		if attr(se.Attr, "code") == code {
			return true
		}
	}
	return false
}

// attr returns the value of the first attribute named local, or "".
func attr(attrs []xml.Attr, local string) string {
	for _, a := range attrs {
		if a.Name.Local == local {
			return a.Value
		}
	}
	return ""
}

// element returns the first child start-element among toks matching space and
// local name.
func element(toks []xml.Token, space, local string) (xml.StartElement, bool) {
	for _, tok := range toks {
		if se, ok := tok.(xml.StartElement); ok && se.Name.Local == local && se.Name.Space == space {
			return se, true
		}
	}
	return xml.StartElement{}, false
}

// childText returns the character data immediately following the first start
// element with the given local name, or "".
func childText(toks []xml.Token, local string) string {
	for i, tok := range toks {
		se, ok := tok.(xml.StartElement)
		if !ok || se.Name.Local != local {
			continue
		}
		if i+1 < len(toks) {
			if cd, ok := toks[i+1].(xml.CharData); ok {
				return string(cd)
			}
		}
	}
	return ""
}
