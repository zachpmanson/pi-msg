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
// (optionally filtered by `with`) plus an RSM <set> capping the page size.
type mamQueryPayload struct {
	XMLName xml.Name `xml:"urn:xmpp:mam:2 query"`
	QueryID string   `xml:"queryid,attr"`
	X       struct {
		Type  string         `xml:"type,attr"`
		Field []mamFormField `xml:"field"`
	} `xml:"jabber:x:data x"`
	Set *struct {
		Max int `xml:"max"`
	} `xml:"http://jabber.org/protocol/rsm set,omitempty"`
}

// mamCollector gathers the archived messages for one in-flight query. The
// read-loop callback appends; FetchMAM reads after the terminating IQ result.
type mamCollector struct {
	room string // bare room JID for a MUC-scoped query, "" for the owner 1:1
	out  []InboundMessage
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
	session := b.currentSession()
	if session == nil {
		return nil, false, errors.New("not online")
	}
	qid := newStanzaID()
	col := &mamCollector{room: room}
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

	payload := newMAMQueryPayload(qid, with, since, max)

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
// `start` field, NOT RSM: without it the query returns the account's whole
// archive (newest page first), so a backfill would replay ancient history
// instead of the offline window. RSM's <set> only caps the page size.
func newMAMQueryPayload(qid, with string, since time.Time, max int) mamQueryPayload {
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
		p.Set = &struct {
			Max int `xml:"max"`
		}{Max: max}
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
	if col.room == "" && bareJid(from) == bareJid(b.acct.JID) {
		return // our own outbound, archived with the counterparty's stream
	}
	var stamp time.Time
	if d, ok := element(toks, delayNS, "delay"); ok {
		if t, err := time.Parse(time.RFC3339, attr(d.Attr, "stamp")); err == nil {
			stamp = t
		}
	}
	if id != "" {
		b.recordMessageBody(id, from, body)
	}
	m := InboundMessage{Body: body, ID: id, From: from, Stamp: stamp}
	// A recovered message can itself be a XEP-0461 reply; keep the stamp so the
	// prompt names what it answers (#95).
	if re, ok := element(rest, replyNS, "reply"); ok {
		m.ReplyToID = attr(re.Attr, "id")
		m.ReplyToJID = attr(re.Attr, "to")
	}
	if col.room != "" {
		nick := resourcepart(from)
		if self := b.ownNick(col.room); self != "" && strings.EqualFold(nick, self) {
			return // our own groupchat line: the archive stores it as room/our-nick
		}
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
