package main

import (
	"context"
	"crypto/rand"
	"crypto/tls"
	"encoding/hex"
	"encoding/xml"
	"fmt"
	"net"
	"strings"
	"sync"
	"time"

	"mellium.im/sasl"
	"mellium.im/xmlstream"
	"mellium.im/xmpp"
	"mellium.im/xmpp/jid"
	"mellium.im/xmpp/stanza"
)

// maxBody is a soft cap for a single outgoing message body; longer text is
// split on newline / word boundaries so servers don't reject oversized
// stanzas.
const maxBody = 3000

const chatStatesNS = "http://jabber.org/protocol/chatstates"

// Receipt namespaces: XEP-0184 message delivery receipts and XEP-0333 chat
// markers. The bridge honors whichever an incoming owner message requests.
const (
	receiptsNS    = "urn:xmpp:receipts"
	chatMarkersNS = "urn:xmpp:chat-markers:0"
)

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
}

// XMPPBridge owns a single account's XMPP connection: it maintains a
// (reconnecting) session, delivers relevant incoming messages via onMsg, and
// exposes send/presence/chat-state helpers the bridge calls from other
// goroutines.
type XMPPBridge struct {
	acct      ResolvedAccount
	ownerBare string
	roomBare  string
	onMsg     func(InboundMessage)
	logf      func(level, msg string)

	mu       sync.Mutex
	session  *xmpp.Session
	online   bool
	show     string // presence <show>: "" (available) or "dnd"/"away"/… (availability axis)
	presence string // presence <status> free text (activity axis)

	seen      map[string]struct{}
	seenOrder []string

	// MUC occupant tracking (room mode).
	occupants map[string]string // nick -> bare real JID (non-anon rooms)
	selfNick  string            // our own occupant nick, per status code 110
}

// NewXMPPBridge constructs a bridge. onMsg is called for each message that
// should drive the agent; logf receives diagnostics.
func NewXMPPBridge(acct ResolvedAccount, onMsg func(InboundMessage), logf func(level, msg string)) *XMPPBridge {
	return &XMPPBridge{
		acct:      acct,
		ownerBare: bareJid(acct.Owner),
		roomBare:  bareJid(acct.Room),
		onMsg:     onMsg,
		logf:      logf,
		presence:  "listening",
		seen:      make(map[string]struct{}),
		occupants: make(map[string]string),
	}
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
	show, status := b.show, b.presence
	// Reset occupant state for this fresh connection; a re-join repopulates it.
	b.occupants = make(map[string]string)
	b.selfNick = ""
	b.mu.Unlock()

	// Announce presence so the server routes messages to this resource and the
	// owner's roster shows the bot online.
	if err := b.encodePresence(show, status); err != nil {
		b.setOffline()
		return fmt.Errorf("presence: %w", err)
	}
	if b.acct.RoomMode() {
		if err := b.joinRoom(); err != nil {
			b.setOffline()
			return fmt.Errorf("join room %s: %w", b.acct.Room, err)
		}
		b.log("info", fmt.Sprintf("joined room %s as %s", b.acct.Room, b.acct.Nick))
	}
	b.log("info", fmt.Sprintf("online as %s, relaying to %s", b.acct.JID, b.ownerBare))
	if onConnected != nil {
		onConnected()
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
	delay       bool // carried an XEP-0203 <delay/> (server-replayed history)
	wantReceipt bool // carried a XEP-0184 <request/> (delivery receipt)
	markable    bool // carried a XEP-0333 <markable/> (chat marker)
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
		_, m.delay = element(toks, "urn:xmpp:delay", "delay")
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
	default:
		// IQ, etc.: drain so the stream advances.
		_, err := xmlstream.Copy(xmlstream.Discard(), t)
		return err
	}
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
	// Drop server-replayed history (offline / MAM catch-up on reconnect) so a
	// blip doesn't reprocess old messages.
	if m.delay {
		return
	}
	if m.id != "" && b.seenDuplicate(m.id) {
		return
	}
	// The agent is about to take this in — acknowledge it as read/delivered.
	b.sendReceipts(m)
	b.onMsg(InboundMessage{Body: m.body, RealJID: b.ownerBare, FromOwner: true, Direct: true})
}

// dispatchRoom applies groupchat guards and forwards room messages to onMsg.
func (b *XMPPBridge) dispatchRoom(m incomingMsg) {
	if m.typ != "groupchat" {
		return // ignore 1:1 DMs to the bot in room mode (v1)
	}
	if bareJid(m.from) != b.roomBare {
		return
	}
	nick := resourcepart(m.from)
	if nick == "" {
		return // room-level stanza (e.g. subject with no occupant)
	}
	if nick == b.ownNick() {
		return // our own echo
	}
	if m.delay {
		return // replayed history
	}
	if strings.TrimSpace(m.body) == "" {
		return // subject-only, chat-states, empty
	}
	if m.id != "" && b.seenDuplicate(m.id) {
		return
	}
	real := b.occupantRealJID(nick)
	b.onMsg(InboundMessage{
		Body:      m.body,
		Nick:      nick,
		RealJID:   real,
		FromOwner: real != "" && real == b.ownerBare,
	})
}

// handlePresence maintains the MUC occupant map (room mode) and auto-approves
// roster subscription requests (1:1).
func (b *XMPPBridge) handlePresence(start *xml.StartElement, toks []xml.Token) error {
	from := attr(start.Attr, "from")
	ptype := attr(start.Attr, "type")

	if b.acct.RoomMode() && bareJid(from) == b.roomBare {
		nick := resourcepart(from)
		if nick == "" {
			return nil
		}
		// Our own occupant presence carries status code 110.
		if hasStatusCode(toks, "110") {
			b.mu.Lock()
			b.selfNick = nick
			b.mu.Unlock()
		}
		real := ""
		if item, ok := element(toks, "http://jabber.org/protocol/muc#user", "item"); ok {
			real = bareJid(attr(item.Attr, "jid"))
		}
		b.mu.Lock()
		if ptype == "unavailable" {
			delete(b.occupants, nick)
		} else if real != "" {
			b.occupants[nick] = real
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

// ownNick returns our occupant nick (server-confirmed via 110 if known, else
// the configured nick).
func (b *XMPPBridge) ownNick() string {
	b.mu.Lock()
	defer b.mu.Unlock()
	if b.selfNick != "" {
		return b.selfNick
	}
	return b.acct.Nick
}

// occupantRealJID returns the bare real JID mapped to nick, or "".
func (b *XMPPBridge) occupantRealJID(nick string) string {
	b.mu.Lock()
	defer b.mu.Unlock()
	return b.occupants[nick]
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
func (b *XMPPBridge) Send(text string) {
	if b.currentSession() == nil {
		b.log("warning", "send skipped: not online")
		return
	}
	for _, part := range chunk(text, maxBody) {
		if err := b.encodeChat(b.acct.Owner, part, stanza.ChatMessage); err != nil {
			b.log("error", "send failed: "+err.Error())
			break
		}
	}
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
// showing the bot online. Safe to call when already offline (no-op).
func (b *XMPPBridge) GoOffline() {
	if err := b.encodeUnavailable(); err != nil {
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

func (b *XMPPBridge) encodeChat(to, body string, typ stanza.MessageType) error {
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
	}{
		Message: stanza.Message{ID: newStanzaID(), To: toJID, Type: typ},
		Body:    body,
	}
	ctx, cancel := context.WithTimeout(context.Background(), 15*time.Second)
	defer cancel()
	return session.Encode(ctx, msg)
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

// encodePresence announces presence with an optional show and status. An empty
// "to" broadcasts (roster-wide) presence.
func (b *XMPPBridge) encodePresence(show, status string) error {
	session := b.currentSession()
	if session == nil {
		return fmt.Errorf("not online")
	}
	p := struct {
		XMLName xml.Name `xml:"presence"`
		Show    string   `xml:"show,omitempty"`
		Status  string   `xml:"status,omitempty"`
	}{Show: show, Status: status}
	ctx, cancel := context.WithTimeout(context.Background(), 15*time.Second)
	defer cancel()
	return session.Encode(ctx, p)
}

// encodeUnavailable broadcasts a roster-wide unavailable presence, marking the
// bot offline.
func (b *XMPPBridge) encodeUnavailable() error {
	session := b.currentSession()
	if session == nil {
		return fmt.Errorf("not online")
	}
	p := struct {
		XMLName xml.Name `xml:"presence"`
		Type    string   `xml:"type,attr"`
	}{Type: "unavailable"}
	ctx, cancel := context.WithTimeout(context.Background(), 15*time.Second)
	defer cancel()
	return session.Encode(ctx, p)
}

// SendRoom posts a groupchat message to the room, splitting long text.
func (b *XMPPBridge) SendRoom(text string) {
	if b.currentSession() == nil {
		b.log("warning", "room send skipped: not online")
		return
	}
	for _, part := range chunk(text, maxBody) {
		if err := b.encodeChat(b.acct.Room, part, stanza.GroupChatMessage); err != nil {
			b.log("error", "room send failed: "+err.Error())
			break
		}
	}
}

// joinRoom sends MUC join presence to room/nick, suppressing history replay
// (maxstanzas=0) so past room chatter isn't reprocessed as new ambient.
func (b *XMPPBridge) joinRoom() error {
	session := b.currentSession()
	if session == nil {
		return fmt.Errorf("not online")
	}
	occupant := b.acct.Room + "/" + b.acct.Nick
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
	}{To: occupant, Status: b.presence}
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
