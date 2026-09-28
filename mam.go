package main

import (
	"context"
	"encoding/xml"
	"errors"
	"fmt"
	"strings"
	"time"

	"mellium.im/xmlstream"
	"mellium.im/xmpp/jid"
	"mellium.im/xmpp/stanza"
)

// XEP-0313 Message Archive Management — offline backfill for messages the
// server never pushed to us (pi-msg issue #84).
//
// The bridge already recovers the *restart swap window* from the server's
// best-effort offline delivery (XEP-0203 delayed stanzas), but that only covers
// 1:1, only whatever the server chose to store, and nothing older than the
// window. A MAM query against the account's own archive (and each joined room's
// archive) recovers the rest: room backlog is suppressed at join
// (`<history maxstanzas="0">`) and is otherwise never fetched at all.
//
// Results are folded into the existing restart-replay buffer so the resumed
// session receives them through exactly one path (see XMPPBridge.DrainReplay).
const (
	mamNS   = "urn:xmpp:mam:2"
	rsmNS   = "http://jabber.org/protocol/rsm"
	delayNS = "urn:xmpp:delay"

	// mamPageMax caps a single MAM page. A window holding more than this is
	// truncated and logged rather than paged — see issue #84 for RSM paging.
	mamPageMax = 200
	// mamTimeout bounds the whole backfill across every scope (owner + rooms).
	mamTimeout = 20 * time.Second
)

// mamFormField is one field of the MAM query's data form.
type mamFormField struct {
	Var   string `xml:"var,attr"`
	Value string `xml:"value"`
}

// mamQueryPayload is the XEP-0313 query: a data form selecting the archive
// (optionally filtered by `with`) plus an RSM <set> capping the page size and
// optionally selecting the last page.
type mamQueryPayload struct {
	XMLName xml.Name `xml:"urn:xmpp:mam:2 query"`
	QueryID string   `xml:"queryid,attr"`
	X       struct {
		Type  string         `xml:"type,attr"`
		Field []mamFormField `xml:"field"`
	} `xml:"jabber:x:data x"`
	Set *mamRSMSet `xml:"http://jabber.org/protocol/rsm set,omitempty"`
}

// mamRSMSet is the RSM element of a MAM query: a page size and an optional
// cursor. The pointer distinguishes the three states XEP-0313 needs: nil omits
// <before> entirely (first page / backfill), a pointer to "" selects the LAST
// page (§4.3.3), and a pointer to a stanza id pages BACKWARDS from that id
// (§4.3.2) — read_room's `before` argument. With no cursor at all the server
// returns the first page, which is the opposite of what an on-demand read wants.
type mamRSMSet struct {
	Max    int     `xml:"max"`
	Before *string `xml:"before,omitempty"`
}

// mamCollector gathers the archived messages for one in-flight query. The
// read-loop callback appends; FetchMAM reads after the terminating IQ result.
type mamCollector struct {
	room string // bare room JID for a MUC-scoped query, "" for the owner 1:1
	out  []InboundMessage
	// record controls whether the fetched stanzas enter the stanza history.
	// Backfill needs that (a later reply resolves through it); an on-demand read
	// must not, because recording clears the "we sent this" flag on our own
	// archived lines and can evict live ids that `to: <stanza-id>` routing
	// depends on (#106 review).
	record bool
}

// FetchMAM queries a XEP-0313 archive for messages at or after since and
// returns them in delivery order, plus whether the archive reported the result
// set complete (<fin complete='true'/>). room selects the scope: a bare MUC JID
// queries that room's archive, "" queries the account's own (owner 1:1) archive
// filtered by `with`.
//
// The archived messages themselves arrive as <message> stanzas in the read
// loop and are intercepted by collectMAMResult; the returned IQ result only
// terminates the query. That ordering is guaranteed by the server (every result
// precedes the IQ result), so by the time EncodeIQElement returns the collector
// holds them all.
func (b *XMPPBridge) FetchMAM(ctx context.Context, room, with string, since time.Time, max int) ([]InboundMessage, bool, error) {
	return b.fetchMAM(ctx, room, with, since, max, "", false, true)
}

// FetchMAMLastPage queries the most recent `max` archived messages of a room —
// the XEP-0313 last page (RSM <before/>, no start bound). This is what an
// on-demand read wants: a query with no RSM cursor returns the archive's FIRST
// page, so read_room would hand the agent the room's oldest messages while
// claiming they were the latest (#106 review).
//
// Fetched stanzas are deliberately NOT recorded in the stanza history: a read
// must not perturb routing state (see mamCollector.record).
func (b *XMPPBridge) FetchMAMLastPage(ctx context.Context, room string, max int) ([]InboundMessage, bool, error) {
	return b.FetchMAMRoomWindow(ctx, room, time.Time{}, "", max)
}

// FetchMAMRoomWindow queries a room's archive for an explicit window: `since`
// (zero means no lower bound) and `before` (a stanza id cursor; "" means the
// newest page). It is read_room's paging path — a cursor walks the archive
// backwards, so repeated calls reach history older than the newest-N window.
// Fetched stanzas are deliberately NOT recorded in the stanza history: a read
// must not perturb routing state (see mamCollector.record).
//
// A cursor read returns the messages the server pages to — everything strictly
// OLDER than the cursor — and an unrecognised cursor is reported as an error.
func (b *XMPPBridge) FetchMAMRoomWindow(ctx context.Context, room string, since time.Time, before string, max int) ([]InboundMessage, bool, error) {
	if before == "" {
		return b.fetchMAM(ctx, room, "", since, max, "", true, false)
	}
	if max <= 0 {
		max = roomReadDefaultLimit
	}
	msgs, complete, err := b.fetchMAM(ctx, room, "", since, max, before, false, false)
	if err != nil {
		return nil, false, err
	}
	// An unknown or expired cursor is NOT an error on the server side: ejabberd
	// answers it with the newest page instead (measured live 2026-09-28 — a
	// fabricated cursor returned the 27 newest messages under a `before stanza
	// <garbage>` label). That is the one failure a caller cannot detect from the
	// page itself, so check it here: a cursor page can never contain the room's
	// newest message, because the cursor is newer than everything on the page.
	// Equality is therefore the signature of the fallback.
	if len(msgs) > 0 {
		newest, _, err := b.fetchMAM(ctx, room, "", since, 1, "", true, false)
		if err != nil {
			return nil, false, err
		}
		if cursorFallbackPage(msgs, newest) {
			return nil, false, fmt.Errorf("cursor %q is not in this room's archive (unknown or expired stanza id)", before)
		}
		return msgs, complete, nil
	}
	// An empty page is ambiguous, and the two meanings need different answers. A
	// cursor at (or below) the room's oldest archived message is a successful read
	// of an empty window; an id the archive does not hold at all pages to nothing
	// and must not be rendered as “no messages” — measured live 2026-09-28: a
	// cursor carrying the message id instead of the archive id quietly returned an
	// empty window. The room's oldest archive id settles it: if the oldest stanza
	// is not the cursor, an empty page cannot be the archive's beginning.
	oldest, _, err := b.fetchMAM(ctx, room, "", time.Time{}, 1, "", false, false)
	if err != nil {
		return nil, false, err
	}
	if cursorEmptyPageUnknown(oldest, before) {
		return nil, false, fmt.Errorf("cursor %q is not in this room's archive (unknown or expired stanza id)", before)
	}
	return msgs, complete, nil
}

// cursorEmptyPageUnknown reports whether an empty cursor page means the archive
// does not hold the cursor: the room's oldest archived message is the cursor
// when the page is genuinely the archive's beginning, so any other oldest id
// means the server answered an unknown cursor with nothing.
func cursorEmptyPageUnknown(oldest []InboundMessage, cursor string) bool {
	return len(oldest) > 0 && oldest[0].ArchiveID != cursor
}

// cursorFallbackPage reports whether `page` is the newest page the server
// substitutes for a cursor it does not recognise. A genuine cursor page holds
// only messages strictly older than the cursor, so its newest message is the one
// immediately before the cursor — never the room's newest. The substitution
// always ends at the room's newest, which makes that equality the signature.
func cursorFallbackPage(page, newest []InboundMessage) bool {
	return len(page) > 0 && len(newest) > 0 && page[len(page)-1].ArchiveID == newest[len(newest)-1].ArchiveID
}

// fetchMAM is the shared XEP-0313 query. `before` is an optional stanza id
// cursor ("" = none) and lastPage selects the final page via an empty RSM
// <before/>; record controls whether the fetched ids enter the stanza history.
func (b *XMPPBridge) fetchMAM(ctx context.Context, room, with string, since time.Time, max int, before string, lastPage, record bool) ([]InboundMessage, bool, error) {
	session := b.currentSession()
	if session == nil {
		return nil, false, errors.New("not online")
	}
	qid := newStanzaID()
	col := &mamCollector{room: room, record: record}
	b.mamMu.Lock()
	if b.mamPending == nil {
		b.mamPending = make(map[string]*mamCollector)
	}
	b.mamPending[qid] = col
	b.mamMu.Unlock()
	defer func() {
		b.mamMu.Lock()
		delete(b.mamPending, qid)
		b.mamMu.Unlock()
	}()

	payload := newMAMQueryPayload(qid, with, since, max, before, lastPage)
	if payload.Set == nil {
		// The cursor lives inside <set>; ask for a page even when the caller
		// passed no max, so the request still means "the newest N" — or, with an
		// explicit cursor, "the N before this id".
		payload.Set = &mamRSMSet{Max: mamPageMax}
		if before != "" {
			payload.Set.Before = &before
		} else {
			payload.Set.Before = new(string)
		}
	}

	iq := stanza.IQ{ID: qid, Type: stanza.SetIQ}
	if room != "" {
		to, err := jid.Parse(room)
		if err != nil {
			return nil, false, fmt.Errorf("invalid room jid %q: %w", room, err)
		}
		iq.To = to.Bare()
	} else {
		// Address the query at our own bare JID (the standard MAM form).
		iq.To = session.LocalAddr().Bare()
	}

	resp, err := session.EncodeIQElement(ctx, payload, iq)
	if err != nil {
		return nil, false, fmt.Errorf("mam query: %w", err)
	}
	defer resp.Close()
	toks, err := xmlstream.ReadAll(resp)
	if err != nil {
		return nil, false, fmt.Errorf("mam response: %w", err)
	}
	for _, tok := range toks {
		se, ok := tok.(xml.StartElement)
		if !ok {
			continue
		}
		if se.Name.Local == "iq" && attr(se.Attr, "type") == "error" {
			return nil, false, fmt.Errorf("mam query rejected: %s", elementText(toks, "error"))
		}
	}
	complete := true
	if fin, ok := element(toks, mamNS, "fin"); ok {
		if attr(fin.Attr, "complete") == "false" {
			complete = false
		}
	}
	b.mamMu.Lock()
	out := col.out
	col.out = nil
	b.mamMu.Unlock()
	return out, complete, nil
}

// newMAMQueryPayload builds the XEP-0313 query. The time bound is a data-form
// `start` field, NOT RSM: without it the query returns the whole archive from the
// beginning, so a backfill would replay ancient history instead of the offline
// window. RSM's <set> caps the page size and carries the cursor: an empty
// <before/> selects the last page rather than the first, while a
// <before>id</before> pages backwards from that archived stanza.
func newMAMQueryPayload(qid, with string, since time.Time, max int, before string, lastPage bool) mamQueryPayload {
	p := mamQueryPayload{QueryID: qid}
	p.X.Type = "submit"
	p.X.Field = []mamFormField{{Var: "FORM_TYPE", Value: mamNS}}
	if !since.IsZero() {
		p.X.Field = append(p.X.Field, mamFormField{Var: "start", Value: since.UTC().Format(time.RFC3339)})
	}
	if with != "" {
		p.X.Field = append(p.X.Field, mamFormField{Var: "with", Value: with})
	}
	if max > 0 {
		p.Set = &mamRSMSet{Max: max}
		switch {
		case before != "":
			// An explicit cursor wins over the last-page flag: page backwards
			// from this stanza id rather than asking for the newest page
			// (XEP-0313 §4.3.2).
			p.Set.Before = &before
		case lastPage:
			// No cursor plus <before/> = the final page (XEP-0313 §4.3.3).
			p.Set.Before = new(string)
		}
	}
	return p
}

// mamSinceFor resolves the archive lower bound for a backfill: the later of the
// restart window start and the last completed backfill marker. Taking the
// earlier one is a bug — it re-delivers messages the running bridge already
// handled live (observed: a replayed `!new` resetting the session). Returns
// ok=false when there is no downtime window at all (first launch), meaning no
// backfill should run.
func mamSinceFor(windowStart time.Time, windowOK bool, seen time.Time, seenOK bool) (time.Time, bool) {
	if !windowOK {
		return time.Time{}, false
	}
	if seenOK && seen.After(windowStart) {
		return seen, true
	}
	return windowStart, true
}

// mamReconnectWindow bounds how far back a mid-session reconnect backfill may
// reach. The restart path (mamSinceFor) is what recovers long downtime; the
// reconnect path only has to cover the gap since the connection dropped, and
// clamping keeps a session that has been idle for hours from dragging a large
// slice of archive into a catch-up (#94).
const mamReconnectWindow = 30 * time.Minute

// reconnectSince resolves the archive lower bound for a mid-session reconnect
// backfill (#94): the later of the last inbound the running bridge handled live
// (`lastin`) and the last completed backfill (`mamseen`), clamped to at most
// mamReconnectWindow behind now. Returns ok=false when neither cursor exists.
func reconnectSince(lastIn time.Time, lastInOK bool, seen time.Time, seenOK bool, now time.Time) (time.Time, bool) {
	cursor := time.Time{}
	ok := false
	if lastInOK {
		cursor, ok = lastIn, true
	}
	if seenOK && (!ok || seen.After(cursor)) {
		cursor, ok = seen, true
	}
	if !ok {
		return time.Time{}, false
	}
	if floor := now.Add(-mamReconnectWindow); cursor.Before(floor) {
		return floor, true
	}
	return cursor, true
}

// collectMAMResult consumes a XEP-0313 archived-message <result> from the read
// loop, appending it to the matching in-flight collector. Unknown query ids are
// dropped: a MAM result must never fall through to live dispatch, or an old
// archived message would start a turn as if it had just arrived.
func (b *XMPPBridge) collectMAMResult(toks []xml.Token, res xml.StartElement) {
	qid := attr(res.Attr, "queryid")
	b.mamMu.Lock()
	col := b.mamPending[qid]
	b.mamMu.Unlock()
	if col == nil {
		return
	}
	// The archived stanza is the first <message> nested in the result's
	// <forwarded>; toks begins just inside the outer <message> we are handling.
	archIdx := -1
	var archStart xml.StartElement
	for i, tok := range toks {
		se, ok := tok.(xml.StartElement)
		if !ok || se.Name.Local != "message" {
			continue
		}
		archIdx, archStart = i, se
		break
	}
	if archIdx < 0 {
		return
	}
	// Slice from just inside the archived <message> so rest holds that
	// message's own children: childText only matches a direct child of the
	// stanza it is given (#95), and the archived element's open is still present
	// at archIdx.
	rest := toks[archIdx+1:]
	body := childText(rest, "body")
	if strings.TrimSpace(body) == "" {
		return // chat-state / receipt / empty archive entry
	}
	from := attr(archStart.Attr, "from")
	id := attr(archStart.Attr, "id")
	// Own-message detection comes BEFORE the history write: an archived copy of
	// one of our own lines must be recorded as ours (so a peer's later
	// unaddressed XEP-0461 reply to it still resolves), never as a plain inbound
	// stanza that would clear that flag (#106 review).
	ownLine := false
	nick := ""
	if col.room == "" {
		ownLine = bareJid(from) == bareJid(b.acct.JID)
	} else {
		nick = resourcepart(from)
		if self := b.ownNick(col.room); self != "" && strings.EqualFold(nick, self) {
			ownLine = true
		}
	}
	owner := false
	if col.room == "" {
		owner = bareJid(from) == b.ownerBare
	} else if real := b.occupantRealJID(col.room, nick); real != "" {
		owner = real == b.ownerBare
	}
	if id != "" && col.record {
		if ownLine {
			b.recordSelfMessage(id, from, body)
		} else {
			b.recordInboundMessage(id, from, body, owner)
		}
	}
	if ownLine {
		return // our own message, archived against the other side's stream
	}
	var stamp time.Time
	if d, ok := element(toks, delayNS, "delay"); ok {
		if t, err := time.Parse(time.RFC3339, attr(d.Attr, "stamp")); err == nil {
			stamp = t
		}
	}
	// The <result> element's own id is the ARCHIVE id (what RSM cursors address);
	// the message's id attribute is the sender's. Keeping both lets read_room
	// print a usable cursor without changing routing, which keys off the message
	// id (#117).
	m := InboundMessage{Body: body, ID: id, ArchiveID: attr(res.Attr, "id"), From: from, Stamp: stamp}
	// A recovered message can itself be a XEP-0461 reply; keep the stamp so the
	// prompt names what it answers (#95).
	if re, ok := element(rest, replyNS, "reply"); ok {
		m.ReplyToID = attr(re.Attr, "id")
		m.ReplyToJID = attr(re.Attr, "to")
	}
	if col.room != "" {
		real := b.occupantRealJID(col.room, nick)
		m.Room = col.room
		m.Nick = nick
		m.RealJID = real
		m.FromOwner = real != "" && real == b.ownerBare
	} else {
		m.Direct = true
		m.RealJID = bareJid(from)
		m.FromOwner = true
	}
	b.mamMu.Lock()
	col.out = append(col.out, m)
	b.mamMu.Unlock()
}

// elementText returns the character data of the first child element with the
// given local name, for surfacing an IQ <error> text.
func elementText(toks []xml.Token, local string) string {
	for i, tok := range toks {
		se, ok := tok.(xml.StartElement)
		if !ok || se.Name.Local != local {
			continue
		}
		for _, next := range toks[i+1:] {
			switch v := next.(type) {
			case xml.CharData:
				if s := strings.TrimSpace(string(v)); s != "" {
					return s
				}
			case xml.StartElement, xml.EndElement:
				return local
			}
		}
	}
	return local
}
